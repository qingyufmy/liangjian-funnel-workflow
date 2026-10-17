"""Approved A2 role -> frozen global scores -> original partition/hard gates.

Explicit research adapter, composed at the natural research boundary. No new
fact source, model-picked score membership or executable nominations.
"""
from collections.abc import Mapping
from contextlib import closing
from copy import deepcopy
from dataclasses import replace
from datetime import datetime
import time
from zoneinfo import ZoneInfo

from ..runtime.team_v2_eod import _response
from .team_v2_a2_schema import validate_a2_response
from .team_v2_a2_score import freeze_theme_scores,canonicalize_team_scores
from .team_v2_contract import TeamValidation
from .team_v2_model import team_call_binding
from .team_v2_a1_research import _hash


def prepare_a2_snapshot(adapter,snapshot,symbols):
    from .research import common as legacy
    from .team_v2_nomination_source import prepare_nomination_source
    config=adapter.a2_config
    if (not isinstance(config,Mapping) or snapshot.data.get('THEME_SCORE_WEIGHTS')!=config.get('score_weights')
            or snapshot.data.get('PENALTY_RULES')!=config.get('penalty_rules')):
        raise ValueError('TEAM_A2_CONFIG_SOURCE_MISMATCH')
    existing=snapshot.data.get('_TEAM_A2_SCORE_FREEZE')
    if existing is not None:
        if not set(symbols)<=set(existing['review_symbols']):raise ValueError('TEAM_A2_GLOBAL_DOMAIN_MISMATCH')
        expected=freeze_theme_scores(legacy._jsonable(snapshot.data),review_symbols=set(existing['review_symbols']),
            weights=config['score_weights'],policy=config['team_theme_score_policy'],source_snapshot_sha256=snapshot.snapshot_hash)
        if expected!=existing:raise ValueError('TEAM_A2_FROZEN_SOURCE_MISMATCH')
        return prepare_nomination_source(snapshot,set(existing['review_symbols']))
    # The caller supplies the complete quantitative review domain before any
    # transport split. Membership is not reconstructed from model FOCUS.
    frozen=freeze_theme_scores(legacy._jsonable(snapshot.data),review_symbols=set(symbols),
        weights=config['score_weights'],policy=config['team_theme_score_policy'],source_snapshot_sha256=snapshot.snapshot_hash)
    return prepare_nomination_source(replace(snapshot,data={**snapshot.data,'_TEAM_A2_SCORE_FREEZE':frozen}),set(symbols))


def _post_policy(raw,snapshot,upstream_output,symbols,lane_id,model):
    from .research import common as legacy
    output=legacy._expand_a2_compact_output(raw,snapshot.data,symbols)
    for theme,review in zip(output['active_themes'],raw['theme_reviews'],strict=True):
        theme.update(team_penalty_codes=list(review['penalty_codes']),supporting_evidence=list(review['supporting_refs']),
            contradicting_evidence=list(review['opposing_refs']),what_would_change_my_mind=list(review['what_would_change_my_mind']))
    output['envelope']={**legacy._required_envelope(snapshot,lane_id,model,'A2'),'prompt_version':'agent-2/3.0.5','status':'OK'}
    output.update(handoff_notes=deepcopy(raw['handoff_notes']),what_would_change_my_mind=raw['what_would_change_my_mind'],
        model_nominations=deepcopy(raw['model_nominations']),market_read=deepcopy(raw['market_read']))
    output,_=legacy._canonicalize_stage_lineage(output,'A2',upstream_output,snapshot.data)
    output,_=legacy._demote_a2_llm_rejects(output,snapshot.data)
    output,_=canonicalize_team_scores(output,snapshot.data)
    output,_=legacy._canonicalize_a2_bottleneck_scorecards(output,snapshot.data)
    output,_=legacy._apply_stage_threshold_policy(output,'A2',snapshot.data)
    if snapshot.data.get('STRICT_AGENT_RULES') is True:
        output,_=legacy._apply_a2_lineage_policy(output,upstream_output,snapshot.data)
    output,_=legacy._canonicalize_stage_lineage(output,'A2',upstream_output,snapshot.data)
    output,_=legacy._enrich_a2_decision_facts(output,snapshot.data)
    output=legacy._refresh_analysis_counts(legacy._annotate_a2_pool_target(output,snapshot.data),'A2')
    reasons=legacy._validate_output(output,stage='A2',model=model,snapshot_id=snapshot.snapshot_id,
        upstream_symbols=symbols,snapshot_data=snapshot.data)
    return output,reasons


def run_a2_stage(adapter,pipeline,*,lane_id,model,snapshot,upstream_output,upstream_symbols,bundle,run_id,
        projection_symbols=None):
    from .research import common as legacy
    def blocked(code,diagnostics=None):
        return replace(pipeline._blocked_stage(lane_id,model,'A2',snapshot.snapshot_id,code),diagnostics=diagnostics)
    if bundle is None:return blocked('PROMPT_REPOSITORY_BLOCKED')
    source=snapshot.as_of
    if not isinstance(source,datetime) or source.utcoffset() is None:return blocked('TEAM_RESEARCH_SOURCE_TIME_REQUIRED')
    source=source.astimezone(ZoneInfo('Asia/Shanghai'))
    if adapter.calendar.is_trading_day(source.date()) is not True or source.hour<15:
        return blocked('TEAM_RESEARCH_CLOSED_SOURCE_REQUIRED')
    symbols=set(upstream_symbols if projection_symbols is None else projection_symbols)
    try:
        snapshot=prepare_a2_snapshot(adapter,snapshot,set(upstream_symbols))
        frozen=snapshot.data['_TEAM_A2_SCORE_FREEZE']
        values=legacy._prompt_replacements(bundle,'A2',snapshot,upstream_output,projection_symbols=symbols)
        for key in set(adapter.repository.document('A2').placeholders)-set(values):
            found,value=legacy._lookup_field(snapshot.data,key);values[key]=legacy._jsonable(value) if found else None
        projected=legacy._project_a2_bottleneck_context(snapshot.data['A2_BOTTLENECK_CONTEXT'],symbols)
        values['A2_BOTTLENECK_CONTEXT']=projected
        sample=values.get('A2_QUANT_FILTERED_SAMPLE')
        if sample is None:sample=[]
        if (not isinstance(sample,list) or len(sample)>30 or any(not isinstance(r,Mapping)
                or not isinstance(r.get('symbol'),str) or not isinstance(r.get('quant_filter_reason'),str) for r in sample)):
            return blocked('TEAM_A2_NOMINATION_SOURCE_INVALID')
        # One bounded global sample, one reviewing batch. It never expands
        # A3 scope; a later batch cannot consume another five nominations.
        if not symbols or min(frozen['review_symbols']) not in symbols:sample=[]
        if len({r['symbol'] for r in sample})!=len(sample) or set(r['symbol'] for r in sample)&set(frozen['review_symbols']):
            return blocked('TEAM_A2_NOMINATION_SOURCE_INVALID')
        values['A2_QUANT_FILTERED_SAMPLE']=sample
        values['PRIOR_OUTCOME_FEEDBACK']=adapter.method_feedback('A2',source)
        values['PENALTY_RULES']=dict(penalty_mode=frozen['policy']['penalty_mode'],
            penalty_points=frozen['policy']['penalty_points'],minimum_penalty_points=-30)
        # Metadata, not a second unscoped copy of all snapshot/legacy feedback.
        values['SNAPSHOT_MANIFEST']=dict(snapshot_id=snapshot.snapshot_id,snapshot_hash=snapshot.snapshot_hash,
            as_of=source.isoformat(),source_fields=sorted(k for k in snapshot.data if not k.startswith('_TEAM_')))
        values['EVIDENCE_CATALOG']=dict(categories=['MARKET','QUANT','NEWS'],items=[
            dict(id='A2:MARKET',category='MARKET',sha256=_hash(values.get('MARKET_REGIME_SNAPSHOT'))),
            dict(id='A2:NEWS',category='NEWS',sha256=_hash(values.get('NEWS_HEAT_SNAPSHOT'))),
            *[dict(id='A2:QUANT:'+s,category='QUANT',sha256=_hash(projected[s])) for s in sorted(symbols)],
            *[dict(id='A2:NOMINATION:'+r['symbol'],category='QUANT',sha256=_hash(r)) for r in sample]])
        # Missing source is not evidence. Keep the absent catalog entry out.
        if values.get('NEWS_HEAT_SNAPSHOT') is None:
            values['EVIDENCE_CATALOG']['items']=[r for r in values['EVIDENCE_CATALOG']['items'] if r['id']!='A2:NEWS']
        if values.get('MARKET_REGIME_SNAPSHOT') is None:
            values['EVIDENCE_CATALOG']['items']=[r for r in values['EVIDENCE_CATALOG']['items'] if r['id']!='A2:MARKET']
        values['EVIDENCE_CATALOG']['categories']=sorted({r['category'] for r in values['EVIDENCE_CATALOG']['items']})
        market=values.get('MARKET_REGIME_SNAPSHOT')
        market=market if isinstance(market,Mapping) else {}
        envelope=dict(schema_version='team-research-envelope/1',run_id=run_id,stage_id='A2',
            source_trade_date=source.date().isoformat(),execution_trade_date=adapter.calendar.next_trading_day(source.date()).isoformat(),
            as_of=source.isoformat(),knowledge_cutoff=source.isoformat(),input_snapshot_ids=[snapshot.snapshot_id],
            config_version=adapter.config_version,config_hash=adapter.config_hash,prompt_version='agent-2/3.0.5',
            model_name=model,market_regime=market.get('regime','UNKNOWN'),status='OK',reason_codes=[])
        domain={s:snapshot.data['A2_BOTTLENECK_CONTEXT'][s]['theme_id'] for s in symbols}
        directions={d:set(ss) for d,ss in projected['_rotation_review_scope'].items()}
        def validate(raw):
            result=validate_a2_response(raw,values['EVIDENCE_CATALOG'],required_envelope=envelope,
                candidate_ids=symbols,nomination_scope={r['symbol'] for r in sample},symbol_themes=domain,
                allowed_theme_ids=set(projected['_theme_review_scope']),direction_symbols=directions)
            issues=list(result.issues)
            if result.valid:
                if raw['market_read']['regime']!=envelope['market_regime']:issues.append('A2_MARKET_REGIME_BINDING_MISMATCH')
                known=frozen['policy']['penalty_points']
                if any(set(r['penalty_codes'])-set(known) for r in raw['theme_reviews']):issues.append('A2_SCORE_PENALTY_CODE_UNKNOWN')
                by_symbol={r['symbol']:r for r in sample}
                for r in raw['model_nominations']:
                    if r['quant_filter_reason']!=by_symbol[r['symbol']]['quant_filter_reason']:
                        issues.append('A2_NOMINATION_FILTER_REASON_MISMATCH')
                    if r['theme_id']!=by_symbol[r['symbol']]['theme_id']:
                        issues.append('A2_NOMINATION_THEME_BINDING_MISMATCH')
                    if 'A2:NOMINATION:'+r['symbol'] not in r['supporting_refs']:
                        issues.append('A2_NOMINATION_ORIGINAL_FACT_REFERENCE_REQUIRED')
            return TeamValidation(not issues,tuple(dict.fromkeys(issues)),result.judgments,result.sentinel_count)
        history=[];ids=[];output=None
        for attempt in range(2):
            request=adapter.repository.prepare_messages('A2',values,required_envelope=envelope,session='CLOSE_REVIEW')
            tokens=legacy._estimate_message_tokens(request.messages)
            if tokens>pipeline.settings.model_max_input_tokens:return blocked('MODEL_PROMPT_TOO_LARGE')
            timeout=float(pipeline.settings.model_timeout_seconds)
            if pipeline._deadline_started_monotonic is not None:
                timeout=min(timeout,pipeline.settings.research_close_deadline_seconds-(time.monotonic()-pipeline._deadline_started_monotonic))
            if timeout<=0:return blocked('RESEARCH_DEADLINE_EXCEEDED')
            identity=_hash(dict(run_id=run_id,lane=lane_id,model=model,attempt=attempt,input_hash=request.input_hash,
                snapshot_hash=snapshot.snapshot_hash,score_hash=frozen['score_snapshot_sha256'],config_hash=adapter.config_hash))
            call_id='A2:CLOSE_REVIEW:'+identity;ids.append(call_id)
            budget=adapter.store.freeze_control_context('a2-research-budget:'+identity,source_sha256=identity,
                body=dict(timeout_seconds=timeout,max_output_tokens=legacy._stage_model_output_limit('A2',len(symbols),
                    configured_limit=pipeline.settings.model_max_output_tokens)))
            expected=dict(run_id=run_id,role='A2',session='CLOSE_REVIEW',source_trade_date=envelope['source_trade_date'],
                execution_trade_date=envelope['execution_trade_date'],model=model,prompt_hash=request.prompt.sha256,
                input_hash=request.input_hash,catalog_hash=request.evidence_catalog_hash)
            if budget['timeout_seconds']>timeout:
                if (adapter.journal.load_captured(call_id,expected=expected) is None and adapter.journal.load_failed(call_id,
                        request_binding=team_call_binding(request,run_id=run_id,model=model,session='CLOSE_REVIEW',**budget)) is None):
                    return blocked('TEAM_RESEARCH_REMAINING_BUDGET_INSUFFICIENT')
            adapter.store.freeze_control_context('a2-research-source:'+identity,source_sha256=request.input_hash,
                body=dict(values=legacy._jsonable(values),envelope=envelope,score_freeze=frozen))
            raw,valid,issues=_response(request=request,client=pipeline.model_client,journal=adapter.journal,call_id=call_id,
                run_id=run_id,model=model,source_trade_date=envelope['source_trade_date'],trade_date=envelope['execution_trade_date'],
                catalog=values['EVIDENCE_CATALOG'],clock=pipeline.now,session='CLOSE_REVIEW',validator=validate,**budget)
            output=None
            if valid:
                output,issues=_post_policy(raw,snapshot,upstream_output,symbols,lane_id,model)
                valid=not issues
                adapter.store.freeze_control_context('a2-research-postpolicy:'+identity,source_sha256=request.input_hash,
                    body=dict(source_call_id=call_id,output=output,reason_codes=list(issues)))
            history.append(list(issues))
            if valid:break
            if not issues or not all(r.startswith('A2_ROTATION_') for r in issues):break
            projected['_rotation_review_contract']['retry_feedback']=dict(validation_errors=list(issues),
                unchanged_focus_symbols=[r['symbol'] for r in raw['focus_decisions']],
                required_no_focus_if_focus_unchanged={d:sorted(ss) for d,ss in directions.items()
                    if not ss.intersection(r['symbol'] for r in raw['focus_decisions'])})
            if output is not None:
                feedback=legacy._a2_rotation_retry_feedback(output,snapshot.data,symbols)
                feedback['server_demoted_focus_symbols']=sorted(set(r['symbol'] for r in raw['focus_decisions'])-
                    set(r['symbol'] for r in output['focus_pool']))
                feedback['theme_score_status']={theme:record['status'] for theme,record in frozen['themes'].items()}
                projected['_rotation_review_contract']['retry_feedback']=feedback
        metrics=[]
        with closing(adapter.journal._connect()) as db:
            for cid in ids:
                row=db.execute('SELECT latency_ms,provider_attempts FROM model_calls WHERE call_id=?',(cid,)).fetchone()
                if row:metrics.append(row)
        diagnostics=dict(team_call_ids=ids,legacy_checkpoint_reused=False,validation_history=history,
            original_rotation_domain={d:sorted(ss) for d,ss in sorted(directions.items())},
            original_theme_review_domain=sorted(projected['_theme_review_scope']),
            score_snapshot_sha256=frozen['score_snapshot_sha256'],
            nomination_source=snapshot.data['_TEAM_A2_NOMINATION_FREEZE'])
        if not valid:
            return legacy.StageAudit(lane_id,model,'A2','BLOCKED',snapshot.snapshot_id,request.prompt.sha256,request.input_hash,
                None,sum(r[0] or 0 for r in metrics),sum(r[1] for r in metrics),'team_v2',(),tuple(issues),diagnostics=diagnostics)
        return legacy.StageAudit(lane_id,model,'A2','VALIDATED',snapshot.snapshot_id,
            request.prompt.sha256,request.input_hash,_hash(output),sum(r[0] or 0 for r in metrics),sum(r[1] for r in metrics),
            'team_v2',tuple(sorted(legacy._approved_symbols(output,'A2'))),(),output=output,diagnostics=diagnostics)
    except ValueError as exc:
        safe=str(exc) if str(exc).startswith(('TEAM_','A2_','MODEL_','CONTROL_')) else 'TEAM_A2_INPUT_INVALID'
        return blocked(safe)
    except (TypeError,KeyError):return blocked('TEAM_A2_SOURCE_SHAPE_INVALID')
