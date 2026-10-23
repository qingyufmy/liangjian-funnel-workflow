"""Approved A2 role -> frozen global scores -> original partition/hard gates.

Explicit research adapter, composed at the natural research boundary. No new
fact source, model-picked score membership or executable nominations.
"""
from collections.abc import Mapping
from contextlib import closing
from copy import deepcopy
from dataclasses import replace
import time

from ..runtime.team_v2_eod import _response
from .team_v2_a2_schema import validate_a2_response,a2_output_contract
from .team_v2_a2_score import freeze_theme_scores,canonicalize_team_scores
from .team_v2_contract import TeamValidation
from .team_v2_model import team_call_binding
from .team_v2_a1_research import _hash
from .team_v2_research_time import research_times
from .team_v2_a2_recovery import bind_a2_retry_feedback, drop_focused_rotation, validation_feedback
from .team_v2_prompts import _VERSIONS as PROMPT_VERSIONS
from .a2_theme_research import (add_unscored_theme_hypotheses, independent_theme_reviews,
    hot100_research_intake, attention_theme_hypotheses, ATTENTION_SCHEMA)


def directory_reference_feedback(raw,catalog,issues,*,nomination_sample=()):
    """0308: one existing second round for concrete foreign IDs, not bad shapes.

    This extends the Team runtime's former rotation-only feedback eligibility.
    The original validator and captured first response remain unchanged.
    """
    from .team_v2_contract import validate_team_output
    rows={row['path']:row for row in validate_team_output('A2',raw,catalog).judgments}
    known={row['id'] for row in catalog['items']};invalid=set()
    for issue in issues:
        if issue.startswith('A2_ROTATION_'):continue
        prefix,_,path=issue.partition(':')
        field={'TEAM_SUPPORTING_REFS_INVALID':'supporting_refs',
            'TEAM_OPPOSING_REFS_INVALID':'opposing_refs'}.get(prefix)
        if field is None:return None
        refs=rows.get(path,{}).get(field)
        if (not isinstance(refs,list) or not 1<=len(refs)<=3
                or any(not isinstance(ref,str) or not ref.strip() for ref in refs)):
            return None
        foreign=set(refs)-known
        if not foreign:return None
        invalid.update(foreign)
    if not invalid:return None
    from .team_v2_a2_references import same_fact_namespace_suggestions
    return dict(invalid_reference_ids=sorted(invalid),catalog_categories=sorted(catalog['categories']),
        same_fact_namespace_suggestions=same_fact_namespace_suggestions(raw,catalog,issues,nomination_sample))


def _fair_research_slots(nomination, attention, *, maximum=30):
    """Round robin original queues, old path first; one symbol owns one slot."""
    queues={'NOMINATION':nomination,'HOT100':attention}
    positions={route:0 for route in queues};owners={};slots=[]
    while len(slots)<maximum and any(positions[r]<len(q) for r,q in queues.items()):
        for route,queue in queues.items():
            if positions[route]>=len(queue):continue
            row=queue[positions[route]];positions[route]+=1
            symbol=row['symbol']
            if symbol in owners:continue
            owners[symbol]=route
            slots.append(dict(slot=len(slots)+1,symbol=symbol,owner_route=route,queue_position=positions[route]))
            if len(slots)==maximum:break
    return slots,owners


def _attention_plan(snapshot, upstream_output, formal_symbols):
    """Fairly share the existing global 30-row side sample, no new calls.

    This plan never creates a batch. It is consumed by the first already
    scheduled formal A2 batch; absent such a batch every lead stays deferred.
    """
    from .research import common as legacy
    supplied=upstream_output.get('independent_research_intake') if isinstance(upstream_output,Mapping) else None
    if supplied is None:return None
    expected=hot100_research_intake(upstream_output,snapshot.data,legacy._extract_g0(snapshot.data),
        hard_risks=legacy._daily_emotion_hard_risks(snapshot.data))
    if (not isinstance(supplied,Mapping) or supplied.get('schema')!=ATTENTION_SCHEMA
            or supplied.get('candidates')!=expected['candidates']):
        raise ValueError('TEAM_A2_ATTENTION_SOURCE_MISMATCH')
    from .team_v2_nomination_source import nomination_source_candidates
    nomination=nomination_source_candidates(snapshot,formal_symbols)
    slots,owners=_fair_research_slots(nomination['candidates'],expected['candidates'],maximum=30 if formal_symbols else 0)
    selected_evidence=[r for r in expected['candidates'] if r['symbol'] in owners]
    attention_by_symbol={r['symbol']:r for r in selected_evidence}
    nomination_symbols={r['symbol'] for r in nomination['candidates']}
    attention_symbols={r['symbol'] for r in expected['candidates']}
    # A shared symbol has one physical transport row. The nomination row keeps
    # its old qualification and embeds the independently verified attention
    # proof; all its exact themes remain mandatory, regardless of slot owner.
    selected_nomination=[]
    for original in nomination['candidates']:
        if original['symbol'] not in owners:continue
        row=deepcopy(original)
        if row['symbol'] in attention_by_symbol:
            row['independent_attention_research']=deepcopy(attention_by_symbol[row['symbol']])
        selected_nomination.append(row)
    selected=[r for r in selected_evidence if r['symbol'] not in nomination_symbols]
    if len(selected_nomination)+len(selected)!=len(slots):
        raise ValueError('TEAM_A2_FAIR_UNIQUE_SLOT_MISMATCH')
    selected_ids={r['candidate_id'] for r in selected_evidence}
    for slot in slots:
        symbol=slot['symbol']
        slot.update(evidence_routes=[r for r,pool in (('NOMINATION',nomination_symbols),('HOT100',attention_symbols)) if symbol in pool],
            transport_representation='NOMINATION_ROW_WITH_HOT100_PROOF' if symbol in nomination_symbols & attention_symbols
                else 'NOMINATION_ROW' if symbol in nomination_symbols else 'HOT100_THEME_RESEARCH_ROW')
    dispositions=[]
    for row in expected['dispositions']:
        item={k:row[k] for k in ('candidate_id','symbol','hot100_rank','original_a1_partition','reason_codes')}
        item.update(execution_authority=False,formal_a1_member=False)
        item['status']=('PLANNED_EXISTING_A2_CALL' if row['candidate_id'] in selected_ids else
            'INELIGIBLE' if row['status']=='INELIGIBLE' else
            'DEFERRED_NO_EXISTING_A2_CALL' if not formal_symbols else 'DEFERRED_A2_SIDE_SAMPLE_CAPACITY')
        item['selected_owner_route']=owners.get(row['symbol'])
        item['shared_slot']=row['symbol'] in nomination_symbols & attention_symbols
        dispositions.append(item)
    nomination_dispositions=[]
    for position,row in enumerate(nomination['candidates'],1):
        owner=owners.get(row['symbol'])
        nomination_dispositions.append(dict(symbol=row['symbol'],queue_position=position,
            original_context_sha256=row['original_context_sha256'],selected_owner_route=owner,
            status='PLANNED_EXISTING_A2_CALL' if owner is not None else
                'DEFERRED_NO_EXISTING_A2_CALL' if not formal_symbols else 'DEFERRED_A2_SIDE_SAMPLE_CAPACITY',
            shared_slot=row['symbol'] in attention_symbols,execution_authority=False))
    body=dict(schema=ATTENTION_SCHEMA,source_snapshot_sha256=snapshot.snapshot_hash,
        candidates=selected,attention_evidence_candidates=selected_evidence,
        hypotheses=attention_theme_hypotheses(selected_evidence),dispositions=dispositions,
        nomination_candidates=selected_nomination,nomination_dispositions=nomination_dispositions,
        nomination_source=dict(original_candidate_count=len(nomination['candidates']),
            full_queue_sha256=_hash(nomination['candidates']),source_context_sha256=nomination['source_context_sha256']),
        attention_original_candidate_count=len(expected['candidates']),attention_full_queue_sha256=_hash(expected['candidates']),
        selected_slots=slots,duplicate_symbols=[dict(symbol=s,owner_route=owners.get(s),
            available_evidence_routes=['NOMINATION','HOT100'],
            disposition='SELECTED_ONCE_BOTH_EVIDENCE_ROUTES_RETAINED' if s in owners else 'DEFERRED_SHARED')
            for s in sorted(nomination_symbols & attention_symbols)],
        first_formal_symbol=min(formal_symbols) if formal_symbols else None,
        maximum_side_sample_size=30,existing_nomination_sample_size=len(snapshot.data['A2_QUANT_FILTERED_SAMPLE']),
        selected_nomination_count=sum(r=='NOMINATION' for r in owners.values()),
        selected_attention_count=sum(r=='HOT100' for r in owners.values()),
        transport_nomination_count=len(selected_nomination),transport_attention_only_count=len(selected),
        selected_attention_evidence_count=len(selected_evidence),selected_unique_count=len(slots),
        count_basis='SELECTED_ROUTE_COUNTS_ARE_SLOT_OWNERS;TRANSPORT_COUNTS_ARE_PHYSICAL_UNIQUE_ROWS;SHARED_ATTENTION_EVIDENCE_REMAINS_MANDATORY',
        selection='ROUND_ROBIN_NOMINATION_FIRST;EACH_ORIGINAL_ORDER;UNIQUE_SYMBOL_OWNER_FIRST_SELECTED;EMPTY_QUEUE_FILL',
        formal_candidate_ids_unchanged=True,new_model_calls_authorized=False,execution_authority=False)
    body['plan_sha256']=_hash(body)
    return body


def _sampling_audit(plan):
    return {key:deepcopy(plan[key]) for key in ('plan_sha256','source_snapshot_sha256',
        'nomination_source','attention_original_candidate_count','attention_full_queue_sha256',
        'selected_slots','duplicate_symbols','maximum_side_sample_size','selected_nomination_count',
        'selected_attention_count','transport_nomination_count','transport_attention_only_count',
        'selected_attention_evidence_count','selected_unique_count','selection','formal_candidate_ids_unchanged','new_model_calls_authorized',
        'count_basis','execution_authority')}


def prepare_a2_snapshot(adapter,snapshot,symbols,*,upstream_output=None):
    from .research import common as legacy
    from .team_v2_nomination_source import prepare_nomination_source
    config=adapter.a2_config
    if (not isinstance(config,Mapping) or snapshot.data.get('THEME_SCORE_WEIGHTS')!=config.get('score_weights')
            or snapshot.data.get('PENALTY_RULES')!=config.get('penalty_rules')):
        raise ValueError('TEAM_A2_CONFIG_SOURCE_MISMATCH')
    existing=snapshot.data.get('_TEAM_A2_SCORE_FREEZE')
    domain=set(existing['review_symbols']) if existing is not None else set(symbols)
    if not set(symbols)<=domain:raise ValueError('TEAM_A2_GLOBAL_DOMAIN_MISMATCH')
    base=freeze_theme_scores(legacy._jsonable(snapshot.data),review_symbols=domain,
        weights=config['score_weights'],policy=config['team_theme_score_policy'],source_snapshot_sha256=snapshot.snapshot_hash)
    old_attention=snapshot.data.get('_TEAM_A2_ATTENTION_RESEARCH_FREEZE')
    if existing is not None:
        original=add_unscored_theme_hypotheses(base,snapshot.data['A2_BOTTLENECK_CONTEXT'],
            additional_hypotheses=old_attention['hypotheses'] if old_attention else None)
        if original!=existing:raise ValueError('TEAM_A2_FROZEN_SOURCE_MISMATCH')
    snapshot=prepare_nomination_source(snapshot,domain)
    attention=_attention_plan(snapshot,upstream_output,domain)
    # The caller supplies the complete quantitative review domain before any
    # transport split. Membership is not reconstructed from model FOCUS.
    frozen=add_unscored_theme_hypotheses(base,snapshot.data['A2_BOTTLENECK_CONTEXT'],
        additional_hypotheses=attention['hypotheses'] if attention else None)
    if existing is not None and frozen!=existing:raise ValueError('TEAM_A2_FROZEN_SOURCE_MISMATCH')
    if old_attention is not None and old_attention!=attention:raise ValueError('TEAM_A2_ATTENTION_SOURCE_MISMATCH')
    data={**snapshot.data,'_TEAM_A2_SCORE_FREEZE':frozen}
    if attention is not None:data['_TEAM_A2_ATTENTION_RESEARCH_FREEZE']=attention
    return replace(snapshot,data=data)


def _post_policy(raw,snapshot,upstream_output,symbols,lane_id,model,*,policy_audit=None,attention_hypotheses=None):
    from .research import common as legacy
    output=legacy._expand_a2_compact_output(raw,snapshot.data,symbols)
    for theme,review in zip(output['active_themes'],raw['theme_reviews'],strict=True):
        theme.update(team_penalty_codes=list(review['penalty_codes']),supporting_evidence=list(review['supporting_refs']),
            contradicting_evidence=list(review['opposing_refs']),what_would_change_my_mind=list(review['what_would_change_my_mind']))
    output['envelope']={**legacy._required_envelope(snapshot,lane_id,model,'A2'),'prompt_version':PROMPT_VERSIONS['A2'],'status':'OK'}
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
    output['independent_theme_reviews']=independent_theme_reviews(output,snapshot.data['A2_BOTTLENECK_CONTEXT'],symbols,
        additional_hypotheses=attention_hypotheses)
    from .team_v2_a2_normalization import complete_server_policy_demotions
    directions=legacy._project_a2_bottleneck_context(snapshot.data['A2_BOTTLENECK_CONTEXT'],symbols)['_rotation_review_scope']
    completion=complete_server_policy_demotions(output,provider_raw=raw,direction_members=directions)
    if completion is not None:
        output,audit=completion
        if policy_audit is not None:policy_audit.update(audit)
    reasons=legacy._validate_output(output,stage='A2',model=model,snapshot_id=snapshot.snapshot_id,
        upstream_symbols=symbols,snapshot_data=snapshot.data)
    return output,reasons


def run_a2_stage(adapter,pipeline,*,lane_id,model,snapshot,upstream_output,upstream_symbols,bundle,run_id,
        projection_symbols=None):
    from .research import common as legacy
    attention_state=None
    ids=[]
    def blocked(code,diagnostics=None):
        if attention_state is not None:
            rows=deepcopy(attention_state['dispositions'])
            nominations=deepcopy(attention_state['nomination_dispositions'])
            for item in rows+nominations:
                if item['status']=='PLANNED_EXISTING_A2_CALL':
                    item.update(status='BLOCKED_A2_RESEARCH_AUDIT_INCOMPLETE' if ids else 'DEFERRED_A2_CALL_NOT_EXECUTED',
                        reason_codes=[code],team_call_ids=list(ids))
            diagnostics={**(diagnostics or {}),'independent_attention_research':dict(
                plan_sha256=attention_state['plan_sha256'],dispositions=rows,
                nomination_dispositions=nominations,sampling_plan=_sampling_audit(attention_state),
                new_model_calls_authorized=False,formal_candidate_ids_unchanged=True)}
        return replace(pipeline._blocked_stage(lane_id,model,'A2',snapshot.snapshot_id,code),diagnostics=diagnostics)
    if bundle is None:return blocked('PROMPT_REPOSITORY_BLOCKED')
    try:
        source,market_source=research_times(snapshot,adapter.calendar)
    except ValueError as exc:
        return blocked(str(exc))
    symbols=set(upstream_symbols if projection_symbols is None else projection_symbols)
    try:
        snapshot=prepare_a2_snapshot(adapter,snapshot,set(upstream_symbols),upstream_output=upstream_output)
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
        attention=snapshot.data.get('_TEAM_A2_ATTENTION_RESEARCH_FREEZE')
        first_attention_batch=bool(attention and attention['first_formal_symbol'] in symbols)
        attention_state=attention if first_attention_batch else None
        if attention is not None:
            sample=attention['nomination_candidates'] if first_attention_batch else []
            values['A2_QUANT_FILTERED_SAMPLE']=sample
        attention_hypotheses=attention['hypotheses'] if first_attention_batch else {}
        if first_attention_batch:
            for symbol,rows in attention_hypotheses.items():
                for row in rows:
                    projected.setdefault('_theme_review_scope',{}).setdefault(row['theme_id'],[]).append(symbol)
            projected['_hot100_independent_research']={
                'schema':ATTENTION_SCHEMA,'candidates':attention['candidates'],
                'shared_nomination_symbols':[r['symbol'] for r in sample if 'independent_attention_research' in r],
                'hypotheses':attention_hypotheses,'plan_sha256':attention['plan_sha256'],
                'contract':'Review every exact theme hypothesis, including Hot100 proof embedded in a shared nomination row. Each shared symbol occupies one slot, not two. All these symbols are outside formal A1: never put them in focus_decisions, reject_decisions or the rotation symbol domain. Only the original selected nomination scope may appear in model_nominations. Membership is not a catalyst/stage/entry proof. Missing theme-specific facts remain DATA_LIMITED.',
                'execution_authority':False,'formal_candidate_ids_unchanged':True}
        # Do not send the unbounded queue in the upstream pool projection.
        if isinstance(values.get('UPSTREAM_ACTIVE_POOL'),Mapping):
            values['UPSTREAM_ACTIVE_POOL']={k:v for k,v in values['UPSTREAM_ACTIVE_POOL'].items()
                if k!='independent_research_intake'}
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
            *([dict(id='A2:ATTENTION:'+r['symbol'],category='QUANT',sha256=_hash(r))
                for r in attention['attention_evidence_candidates']] if first_attention_batch else []),
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
            source_trade_date=market_source.date().isoformat(),execution_trade_date=adapter.calendar.next_trading_day(market_source.date()).isoformat(),
            as_of=source.isoformat(),knowledge_cutoff=source.isoformat(),input_snapshot_ids=[snapshot.snapshot_id],
            config_version=adapter.config_version,config_hash=adapter.config_hash,prompt_version=PROMPT_VERSIONS['A2'],
            model_name=model,market_regime=market.get('regime','UNKNOWN'),status='OK',reason_codes=[])
        domain={s:snapshot.data['A2_BOTTLENECK_CONTEXT'][s]['theme_id'] for s in symbols}
        directions={d:set(ss) for d,ss in projected['_rotation_review_scope'].items()}
        emotion=snapshot.data.get('MARKET_EMOTION_SNAPSHOT')
        engine_emotion=emotion.get('emotion_cycle_stage') if isinstance(emotion,Mapping) else None
        from .team_v2_a2_score import penalty_code_domain
        legal_penalties=penalty_code_domain(frozen['policy'],snapshot.data['PENALTY_RULES'])
        output_contract=a2_output_contract(required_envelope=envelope,candidate_ids=symbols,
            nomination_scope={r['symbol'] for r in sample},symbol_themes=domain,
            allowed_theme_ids=set(projected['_theme_review_scope']),direction_symbols=directions,
            catalog=values['EVIDENCE_CATALOG'],engine_emotion_cycle=engine_emotion,
            penalty_codes=legal_penalties,nomination_facts=sample)
        policy_previews={}
        def policy_preview(raw):
            if not isinstance(raw,Mapping):return None
            try:
                key=_hash(raw)
                if key not in policy_previews:
                    policy_previews[key]=_post_policy(raw,snapshot,upstream_output,symbols,lane_id,model,
                        attention_hypotheses=attention_hypotheses)[0]
                return policy_previews[key]
            except (TypeError,KeyError,ValueError,AttributeError):return None
        def validate(raw):
            result=validate_a2_response(raw,values['EVIDENCE_CATALOG'],required_envelope=envelope,
                candidate_ids=symbols,nomination_scope={r['symbol'] for r in sample},symbol_themes=domain,
                allowed_theme_ids=set(projected['_theme_review_scope']),direction_symbols=directions,
                engine_emotion_cycle=engine_emotion,penalty_codes=legal_penalties,nomination_facts=sample)
            issues=list(result.issues)
            if result.valid:
                if raw['market_read']['regime']!=envelope['market_regime']:issues.append('A2_MARKET_REGIME_BINDING_MISMATCH')
                known=legal_penalties
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
        history=[];provider_history=[];canonical_history=[];symbol_audits=[];canonical_audits=[];ids=[];output=None;reference_feedback_rounds=0
        normalization=None;normalizations=[];budget_stop=None
        input_projections=[]
        for attempt in range(3):
            from .team_v2_a2_projection import prepare_a2_transport_values
            from .team_v2_a2_references import prepare_nomination_evidence_values
            model_values,nomination_evidence=prepare_nomination_evidence_values(values)
            transport_values,input_projection=prepare_a2_transport_values(model_values)
            input_projection['nomination_evidence_projection']=nomination_evidence
            request=adapter.repository.prepare_messages('A2',transport_values,required_envelope=envelope,session='CLOSE_REVIEW')
            from .team_v2_prompts import with_runtime_contract
            request=with_runtime_contract(request,output_contract)
            if attempt:
                request=bind_a2_retry_feedback(request,projected['_rotation_review_contract'].get('retry_feedback'),
                    correction_attempt=attempt+1,engine_emotion_cycle=engine_emotion)
            tokens=legacy._estimate_message_tokens(request.messages)
            input_projection.update(semantic_attempt=attempt+1,input_hash=request.input_hash,
                actual_message_characters=sum(len(message['content']) for message in request.messages),
                actual_estimated_tokens=tokens)
            input_projections.append(input_projection)
            if tokens>pipeline.settings.model_max_input_tokens:return blocked('MODEL_PROMPT_TOO_LARGE')
            # Each semantic answer has its own bounded timeout; the original
            # research deadline remains the absolute, shared budget.
            timeout=min(600.0,float(pipeline.settings.model_timeout_seconds))
            remaining=None
            if pipeline._deadline_started_monotonic is not None:
                remaining=pipeline.settings.research_close_deadline_seconds-(time.monotonic()-pipeline._deadline_started_monotonic)
                timeout=min(timeout,remaining)
            if timeout<=0:
                if not ids:return blocked('RESEARCH_DEADLINE_EXCEEDED')
                issues=('RESEARCH_DEADLINE_EXCEEDED',);valid=False
                budget_stop=dict(reason_code=issues[0],remaining_seconds=remaining)
                break
            identity=_hash(dict(run_id=run_id,lane=lane_id,model=model,attempt=attempt,input_hash=request.input_hash,
                snapshot_hash=snapshot.snapshot_hash,score_hash=frozen['score_snapshot_sha256'],config_hash=adapter.config_hash))
            call_id='A2:CLOSE_REVIEW:'+identity
            expected=dict(run_id=run_id,role='A2',session='CLOSE_REVIEW',source_trade_date=envelope['source_trade_date'],
                execution_trade_date=envelope['execution_trade_date'],model=model,prompt_hash=request.prompt.sha256,
                input_hash=request.input_hash,catalog_hash=request.evidence_catalog_hash)
            # Replaying a captured answer is not a new model attempt.  A new
            # HTTP request is forbidden below 120s remaining, even on retry.
            if remaining is not None and remaining<120 and adapter.journal.load_captured(call_id,expected=expected) is None:
                if not ids:
                    return blocked('TEAM_RESEARCH_REMAINING_BUDGET_INSUFFICIENT',
                        dict(team_call_ids=ids,validation_history=history,remaining_timeout_seconds=timeout))
                issues=('TEAM_RESEARCH_REMAINING_BUDGET_INSUFFICIENT',);valid=False
                budget_stop=dict(reason_code=issues[0],remaining_seconds=remaining)
                break
            ids.append(call_id)
            budget=adapter.store.freeze_control_context('a2-research-budget:'+identity,source_sha256=identity,
                body=dict(timeout_seconds=timeout,max_output_tokens=legacy._stage_model_output_limit('A2',len(symbols),
                    configured_limit=pipeline.settings.model_max_output_tokens)))
            if budget['timeout_seconds']>timeout:
                if (adapter.journal.load_captured(call_id,expected=expected) is None and adapter.journal.load_failed(call_id,
                        request_binding=team_call_binding(request,run_id=run_id,model=model,session='CLOSE_REVIEW',**budget)) is None):
                    return blocked('TEAM_RESEARCH_REMAINING_BUDGET_INSUFFICIENT')
            adapter.store.freeze_control_context('a2-research-source:'+identity,source_sha256=request.input_hash,
                body=dict(values=legacy._jsonable(values),envelope=envelope,score_freeze=frozen,
                    request_transport_projection=dict(schema='a2-retry-user-feedback/1',semantic_attempt=attempt+1,
                        input_hash=request.input_hash,prompt_sha256=request.prompt.sha256,
                        user_message=request.messages[1]['content'],input_projection=input_projection)))
            raw,valid,issues=_response(request=request,client=pipeline.model_client,journal=adapter.journal,call_id=call_id,
                run_id=run_id,model=model,source_trade_date=envelope['source_trade_date'],trade_date=envelope['execution_trade_date'],
                catalog=values['EVIDENCE_CATALOG'],clock=pipeline.now,session='CLOSE_REVIEW',validator=validate,**budget)
            provider_history.append(list(issues))
            from .team_v2_symbol_suffix import canonicalize_a2_symbols
            effective_raw,symbol_actions=canonicalize_a2_symbols(raw,candidate_ids=symbols,
                nomination_scope={r['symbol'] for r in sample},direction_symbols=directions,catalog=values['EVIDENCE_CATALOG'])
            if symbol_actions:
                canonical_checked=validate(effective_raw)
                valid=canonical_checked.valid;issues=canonical_checked.issues
                key='a2-symbol-suffix:'+identity
                audit=dict(rule_id='SYMBOL_SUFFIX_CANONICAL',authority='0498',actions=symbol_actions,
                    source_call_id=call_id,provider_raw_sha256=_hash(raw),canonical_sha256=_hash(effective_raw),
                    strict_raw_issues=provider_history[-1],canonical_issues=list(issues),audit_context_key=key,
                    provider_capture_unchanged=True,canonical_raw=effective_raw)
                adapter.store.freeze_control_context(key,source_sha256=request.input_hash,body=audit)
                symbol_audits.append(audit)
            # 0501: spelling first, safe contradictory explanation removal
            # second, then strict validation of the complete effective reply.
            # Neither step changes or invents any provider FOCUS selection.
            effective_focus={r.get('symbol') for r in effective_raw.get('focus_decisions',[]) if isinstance(r,Mapping) and isinstance(r.get('symbol'),str) and r['symbol'] in symbols} if isinstance(effective_raw,Mapping) else set()
            projection=drop_focused_rotation(effective_raw,values['EVIDENCE_CATALOG'],directions,
                effective_focus_symbols=effective_focus)
            normalization=None
            if projection is not None:
                effective_raw,audit=projection
                checked=validate(effective_raw)
                valid=checked.valid;issues=checked.issues
            canonical_history.append(list(issues))
            canonical_actions=symbol_actions+([] if projection is None else audit['actions'])
            if canonical_actions:
                key='a2-canonicalization:'+identity
                canonical_audit=dict(rule_id='A2_IDENTITY_AND_CONTRADICTION_CANONICAL',authority='0501',
                    component_authorities=['0498','0501'],actions=canonical_actions,source_call_id=call_id,
                    provider_raw_sha256=_hash(raw),canonical_sha256=_hash(effective_raw),
                    strict_raw_issues=provider_history[-1],canonical_issues=list(issues),audit_context_key=key,
                    provider_capture_unchanged=True,canonical_raw=effective_raw)
                adapter.store.freeze_control_context(key,source_sha256=request.input_hash,body=canonical_audit)
                canonical_audits.append(canonical_audit)
            output=None
            if valid:
                policy_audit={}
                output,issues=_post_policy(effective_raw,snapshot,upstream_output,symbols,lane_id,model,
                    policy_audit=policy_audit,attention_hypotheses=attention_hypotheses)
                if policy_audit:
                    key='a2-research-policy-demotion:'+identity
                    policy_audit.update(source_call_id=call_id,audit_context_key=key)
                    adapter.store.freeze_control_context(key,source_sha256=request.input_hash,body=policy_audit)
                    normalizations.append(policy_audit)
                valid=not issues
                adapter.store.freeze_control_context('a2-research-postpolicy:'+identity,source_sha256=request.input_hash,
                    body=dict(source_call_id=call_id,output=output,reason_codes=list(issues)))
            if projection is not None:
                normalization_key='a2-research-focused-drop:'+identity
                normalization=dict(**audit,original_issues=provider_history[-1],
                    status='VALIDATED' if valid else 'BLOCKED',source_call_id=call_id,
                    provider_raw_sha256=_hash(raw),normalized_raw_sha256=_hash(effective_raw),
                    normalized_validation_issues=list(checked.issues),postpolicy_reason_codes=list(issues),
                    audit_context_key=normalization_key)
                adapter.store.freeze_control_context(normalization_key,source_sha256=request.input_hash,
                    body=dict(**normalization,normalized_raw=effective_raw,postpolicy_output=output))
                normalizations.append(normalization)
            history.append(list(issues))
            if valid:break
            if attempt==2:break
            reference_feedback=directory_reference_feedback(effective_raw,values['EVIDENCE_CATALOG'],issues,
                nomination_sample=sample) if issues else None
            # Transport failures have no provider answer to correct.  Every
            # actual validation failure, not only rotation, gets feedback.
            if not issues or raw is None and set(issues)<={'TEAM_MODEL_CALL_FAILED','TEAM_MODEL_RESULT_INVALID'}:break
            feedback=validation_feedback(effective_raw,values['EVIDENCE_CATALOG'],issues,envelope=envelope,
                symbols=symbols,themes=set(projected['_theme_review_scope']),directions=directions,
                nomination_scope={r['symbol'] for r in sample},engine_emotion_cycle=engine_emotion,
                effective_focus_symbols=effective_focus,output_contract=output_contract)
            if canonical_actions:feedback['field_level_canonicalization_applied']=canonical_actions
            if reference_feedback is not None:
                reference_feedback_rounds+=1
                feedback['reference_feedback']=reference_feedback
            preview=output if output is not None else policy_preview(effective_raw)
            if preview is not None:
                feedback['rotation_scope_boundary']='0488: provider corrects coverage against its own focus_decisions intersect exact members, never same-theme labels. Server alone explains its later policy demotions; do not predict them.'
                feedback['server_demoted_focus_symbols']=sorted(({r['symbol'] for r in effective_raw['focus_decisions']} & symbols)-
                    set(r['symbol'] for r in preview['focus_pool']))
                feedback['server_policy_demotions_are_not_provider_omissions']=True
                feedback['theme_score_status']={theme:record['status'] for theme,record in frozen['themes'].items()}
            projected['_rotation_review_contract']['retry_feedback']=feedback
        metrics=[]
        with closing(adapter.journal._connect()) as db:
            for cid in ids:
                row=db.execute('SELECT latency_ms,provider_attempts FROM model_calls WHERE call_id=?',(cid,)).fetchone()
                if row:metrics.append(row)
        diagnostics=dict(team_call_ids=ids,legacy_checkpoint_reused=False,validation_history=history,
            provider_validation_history=provider_history,canonical_validation_history=canonical_history,
            symbol_suffix_audits=symbol_audits,canonicalization_audits=canonical_audits,
            first_round_strict_raw_pass=bool(provider_history and not provider_history[0]),
            first_round_canonical_pass=bool(canonical_history and not canonical_history[0]),engine_emotion_cycle=engine_emotion,
            first_round_failed=bool(history and history[0]),reference_feedback_rounds=reference_feedback_rounds,
            reference_retry_authority='0361_REPLY_B_ALL_VALIDATION_FEEDBACK_MAX_THREE',
            server_normalization=normalization,
            server_normalizations=normalizations,
            input_projections=input_projections,
            budget_stop=budget_stop,
            original_rotation_domain={d:sorted(ss) for d,ss in sorted(directions.items())},
            rotation_output_contract=output_contract,
            original_theme_review_domain=sorted(projected['_theme_review_scope']),
            score_snapshot_sha256=frozen['score_snapshot_sha256'],
            nomination_source=snapshot.data['_TEAM_A2_NOMINATION_FREEZE'])
        if first_attention_batch:
            dispositions=deepcopy(attention['dispositions'])
            nominations=deepcopy(attention['nomination_dispositions'])
            for item in dispositions:
                if item['status']=='PLANNED_EXISTING_A2_CALL':
                    item.update(status='VALIDATED_THEME_RESEARCH' if valid else 'BLOCKED_THEME_RESEARCH',
                        team_call_ids=list(ids),input_hash=request.input_hash,
                        reason_codes=[] if valid else list(issues))
            nominated={r['symbol'] for r in output.get('model_nominations',())} if valid else set()
            for item in nominations:
                if item['status']=='PLANNED_EXISTING_A2_CALL':
                    item.update(status='DELIVERED_IN_VALIDATED_A2_CALL' if valid else 'BLOCKED_NOMINATION_RESEARCH',
                        team_call_ids=list(ids),input_hash=request.input_hash,model_nominated=item['symbol'] in nominated,
                        reason_codes=[] if valid else list(issues))
            diagnostics['independent_attention_research']=dict(plan_sha256=attention['plan_sha256'],
                dispositions=dispositions,nomination_dispositions=nominations,sampling_plan=_sampling_audit(attention),
                new_model_calls_authorized=False,formal_candidate_ids_unchanged=True)
            if valid:
                output['independent_attention_research_dispositions']=dispositions
                output['independent_nomination_research_dispositions']=nominations
                output['independent_research_sampling_plan']=_sampling_audit(attention)
        from .early_discovery import finalize_a2_discovery_review
        discovery_audit=finalize_a2_discovery_review(snapshot.data,upstream_output,symbols=symbols,
            status='VALIDATED' if valid else 'BLOCKED',call_ids=ids,input_hash=request.input_hash)
        if discovery_audit.get('records'):
            diagnostics['early_discovery']=discovery_audit
            if valid:output['early_discovery']=discovery_audit
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
