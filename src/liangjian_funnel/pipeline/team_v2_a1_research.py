"""A1 role/owned journal -> unchanged original research post-policy validator.

Explicit adapter only. A shallow per-request pipeline view avoids changing
shared clients/flags across company batches. No legacy prompt is sent, and a
legacy semantic repair cannot silently recall a new Team model request.
"""
from collections.abc import Mapping
from contextlib import closing
from copy import copy,deepcopy
from dataclasses import replace
from datetime import datetime
import hashlib
import time
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from ..runtime.team_v2_eod import _response
from ..runtime.team_v2_store import canonical
from .model_client import ModelCallResult,ModelClientError
from .team_v2_a1_schema import validate_a1_response
from .team_v2_model import team_call_binding


def _hash(value):return hashlib.sha256(canonical(value).encode()).hexdigest()


def _catalog(values):
    """Only refs in actually supplied projected facts, not every raw source.

    Source IDs remain original. Categories are server-owned source families;
    duplicate refs never create fictitious independent evidence categories.
    """
    refs={}
    def visit(value,path,category):
        if isinstance(value,Mapping):
            # 0319: use explicit original source-family/document-category
            # values verbatim. Never infer a family from paths, URLs or prose.
            family=next(((key,value[key]) for key in ('source_family','source_type','source_category','official_category')
                if isinstance(value.get(key),str) and value[key].strip()),None)
            if family is not None:category=family[1]
            for key,item in value.items():
                if key in {'fact_id','source_ref','source_refs','source_url','source_urls'}:
                    for ref in item if isinstance(item,(tuple,list)) else [item]:
                        if isinstance(ref,str) and ref.strip():
                            refs.setdefault(ref,dict(id=ref,category=category,source_field=path+'.'+key))
                else:visit(item,path+'.'+str(key),category)
        elif isinstance(value,(list,tuple)):
            for i,row in enumerate(value):visit(row,path+'['+str(i)+']',category)
    for key,category in [('A1_RESEARCH_PACKET','RESEARCH_PACKET'),('MACRO_POLICY_FEED','POLICY'),
            ('INDUSTRY_PROFIT_DATA','FUNDAMENTAL'),('INDUSTRY_ACTIVITY_DATA','FUNDAMENTAL'),
            ('MAIN_BUSINESS_EVIDENCE','FUNDAMENTAL'),('COMPANY_FUNDAMENTALS','FUNDAMENTAL'),
            ('DISCLOSURE_EVENTS','DISCLOSURE'),('RISK_EVENTS','RISK'),('RESEARCH_CONSENSUS','RESEARCH'),
            ('BROKER_RESEARCH_CONSENSUS','RESEARCH'),('EXISTING_CHAIN_GRAPH','CHAIN'),('A1_BATCH_CONTEXT','CONTEXT')]:
        visit(values.get(key),key,category)
    return dict(categories=sorted({row['category'] for row in refs.values()}),items=[refs[k] for k in sorted(refs)])


def _server_quality(output,context):
    from .deterministic import local_active_items
    from .research.common import _first_symbol
    result=deepcopy(dict(output));candidates=context.get('local_candidates',{})
    for pool in ('active_research_pool','monitor_pool'):
        for row in result.get(pool,[]):
            symbol=_first_symbol(row['symbol']);row['symbol']=symbol
            original=candidates.get(symbol,{})
            # Reuse the existing deterministic projection's quality/confidence
            # computation, not the model echo or a newly invented formula.
            values=local_active_items(SimpleNamespace(decisions=[dict(original,
                status='LOCAL_ACTIVE_CANDIDATE')])) if original else []
            projected=values[0] if values else {}
            row['data_quality_score']=projected.get('data_quality_score')
            row['evidence_confidence']=projected.get('evidence_confidence')
    return result


def run_a1_stage(adapter,pipeline,*,lane_id,model,snapshot,upstream_output,upstream_symbols,bundle,run_id,
        projection_symbols=None,a1_discovery_context=None):
    from .research import common as legacy
    stage='A1';context=dict(a1_discovery_context or {});mode=context.get('mode')
    def blocked(code):return pipeline._blocked_stage(lane_id,model,stage,snapshot.snapshot_id,code)
    if bundle is None:return blocked('PROMPT_REPOSITORY_BLOCKED')
    if mode not in {'POLICY_MACRO_DISCOVERY','COMPANY_MAPPING'}:return blocked('TEAM_A1_SOURCE_MODE_REQUIRED')
    symbols=set(upstream_symbols if projection_symbols is None else projection_symbols)
    if mode=='POLICY_MACRO_DISCOVERY' and symbols:return blocked('TEAM_A1_DISCOVERY_STOCK_DOMAIN_FORBIDDEN')
    source=snapshot.as_of
    if not isinstance(source,datetime) or source.utcoffset() is None:return blocked('TEAM_RESEARCH_SOURCE_TIME_REQUIRED')
    source=source.astimezone(ZoneInfo('Asia/Shanghai'))
    if adapter.calendar.is_trading_day(source.date()) is not True:return blocked('TEAM_RESEARCH_SOURCE_DATE_INVALID')
    try:
        values=legacy._prompt_replacements(bundle,stage,snapshot,upstream_output,projection_symbols=projection_symbols,
            a1_discovery_context=context)
        needed=set(adapter.repository.document('A1').placeholders)
        for key in needed-set(values):
            found,value=legacy._lookup_field(snapshot.data,key);values[key]=value if found else None
        # The old manifest projection contains full source blocks, including
        # arbitrary legacy feedback. This role explicitly requests metadata,
        # not a second unguarded copy of every input beneath a manifest key.
        values['SNAPSHOT_MANIFEST']=dict(snapshot_id=snapshot.snapshot_id,snapshot_sha256=snapshot.snapshot_hash,
            as_of=source.isoformat(),source_fields=sorted(snapshot.data),
            data_quality_summary=snapshot.data.get('DATA_QUALITY_SUMMARY'))
        if mode=='POLICY_MACRO_DISCOVERY':
            # The authored role refers to these limits in A1_BATCH_CONTEXT.
            # Discovery intentionally omits company A1_POOL_TARGETS, so carry
            # the original validator's one authority explicitly, not a quota
            # inferred from the current batch or a caller-supplied override.
            from .a1_contract import A1_CONTRACT_VERSION,A1_THEME_TARGET,A1_NODE_TARGET
            values['A1_BATCH_CONTEXT']={**values['A1_BATCH_CONTEXT'],
                'monthly_coverage_contract':dict(contract_version=A1_CONTRACT_VERSION,
                    minimum_structural_themes=A1_THEME_TARGET[0],maximum_structural_themes=A1_THEME_TARGET[1],
                    minimum_industry_chain_nodes=A1_NODE_TARGET[0],maximum_industry_chain_nodes=A1_NODE_TARGET[1])}
            # Same packet/snapshot intersection as the old request builder;
            # do not trust an inherited or caller-expanded citation allowlist.
            context['authorized_discovery_source_refs']=tuple(values['A1_BATCH_CONTEXT']['allowed_primary_source_refs'])
        # Only owned, mature, as-of-bound A5 method aggregates; no arbitrary
        # READY/stock-return blob from a legacy snapshot or model prose.
        values['PRIOR_OUTCOME_FEEDBACK']=adapter.method_feedback('A1',source)
        values['EVIDENCE_CATALOG']=_catalog(values)
        envelope=dict(schema_version='team-research-envelope/1',run_id=run_id,stage_id='A1',
            source_trade_date=source.date().isoformat(),execution_trade_date=adapter.calendar.next_trading_day(source.date()).isoformat(),
            as_of=source.isoformat(),knowledge_cutoff=source.isoformat(),input_snapshot_ids=[snapshot.snapshot_id],
            config_version=adapter.config_version,config_hash=adapter.config_hash,prompt_version='agent-1/3.0.5',model_name=model,
            market_regime=legacy._required_envelope(snapshot,lane_id,model,stage)['market_regime'],status='OK',reason_codes=[])
        request=adapter.repository.prepare_messages('A1',values,required_envelope=envelope,session=mode)
        tokens=legacy._estimate_message_tokens(request.messages)
        if tokens>pipeline.settings.model_max_input_tokens:return blocked('MODEL_PROMPT_TOO_LARGE')
        timeout=float(pipeline.settings.model_timeout_seconds)
        if pipeline._deadline_started_monotonic is not None:
            timeout=min(timeout,pipeline.settings.research_close_deadline_seconds-(time.monotonic()-pipeline._deadline_started_monotonic))
        if timeout<=0:return blocked('RESEARCH_DEADLINE_EXCEEDED')
        identity=_hash(dict(run_id=run_id,lane=lane_id,model=model,mode=mode,snapshot_hash=snapshot.snapshot_hash,
            input_hash=request.input_hash,symbols=sorted(symbols),config_hash=adapter.config_hash))
        original_budget=adapter.store.freeze_control_context('a1-research-budget:'+identity,source_sha256=identity,
            body=dict(timeout_seconds=timeout,max_output_tokens=legacy._stage_model_output_limit(stage,len(symbols),
                configured_limit=pipeline.settings.model_max_output_tokens)))
        expected=dict(run_id=run_id,role=stage,session=mode,source_trade_date=envelope['source_trade_date'],
            execution_trade_date=envelope['execution_trade_date'],model=model,prompt_hash=request.prompt.sha256,
            input_hash=request.input_hash,catalog_hash=request.evidence_catalog_hash)
        call_id='A1:'+mode+':'+identity
        if original_budget['timeout_seconds']>timeout:
            captured=adapter.journal.load_captured(call_id,expected=expected)
            failed=adapter.journal.load_failed(call_id,request_binding=team_call_binding(request,run_id=run_id,model=model,session=mode,**original_budget))
            if captured is None and failed is None:return blocked('TEAM_RESEARCH_REMAINING_BUDGET_INSUFFICIENT')
        adapter.store.freeze_control_context('a1-research-source:'+identity,source_sha256=request.input_hash,
            body=dict(snapshot_hash=snapshot.snapshot_hash,context=context,values=values,envelope=envelope))
        output,valid,issues=_response(request=request,client=pipeline.model_client,journal=adapter.journal,call_id=call_id,
            run_id=run_id,model=model,source_trade_date=envelope['source_trade_date'],trade_date=envelope['execution_trade_date'],
            catalog=values['EVIDENCE_CATALOG'],clock=pipeline.now,session=mode,**original_budget,
            validator=lambda raw:validate_a1_response(raw,values['EVIDENCE_CATALOG'],required_envelope=envelope,
                mode=mode,candidate_symbols=symbols))
        with closing(adapter.journal._connect()) as db:
            metrics=db.execute('SELECT latency_ms,provider_attempts FROM model_calls WHERE call_id=?',(call_id,)).fetchone()
        if not valid:
            return legacy.StageAudit(lane_id,model,stage,'BLOCKED',snapshot.snapshot_id,request.prompt.sha256,
                request.input_hash,None,metrics[0] if metrics else None,metrics[1] if metrics else 0,'team_v2',(),
                tuple(issues),diagnostics={'team_call_id':call_id,'legacy_checkpoint_reused':False})
        normalized=_server_quality(output,context) if mode=='COMPANY_MAPPING' else deepcopy(output)
        response=ModelCallResult(model=model,output=normalized,prompt_hash=request.prompt.sha256,input_hash=request.input_hash,
            latency_ms=metrics[0],attempts=metrics[1],thinking_variant='team_v2_captured')
        class CapturedOnce:
            used=False
            def complete(self,selected,messages,**metadata):
                if self.used:raise ModelClientError('TEAM_A1_POST_POLICY_REJECTED_NO_IMPLICIT_RECALL')
                if (selected!=model or list(messages)!=[dict(m) for m in request.messages]
                        or metadata.get('prompt_hash')!=request.prompt.sha256 or metadata.get('input_hash')!=request.input_hash):
                    raise ModelClientError('TEAM_A1_VALIDATOR_INPUT_CHANGED')
                self.used=True;return response
        prepared=legacy._PreparedStageRequest(request.prompt.sha256,request.input_hash,tuple(dict(m) for m in request.messages),
            sum(len(m['content']) for m in request.messages),tokens,pipeline.settings.model_max_input_tokens,{})
        view=copy(pipeline);view.team_research_adapter=None;view.model_client=CapturedOnce()
        audit=view._run_stage(lane_id=lane_id,model=model,stage=stage,snapshot=snapshot,upstream_output=upstream_output,
            upstream_symbols=upstream_symbols,bundle=bundle,run_id=run_id,projection_symbols=projection_symbols,
            a1_discovery_context=context,prepared_request=prepared)
        result_output=deepcopy(audit.output) if audit.output is not None else None
        if result_output is not None:
            from .team_v2_catalyst_gaps import enforce_catalyst_gaps
            result_output=enforce_catalyst_gaps(result_output,context)
        if result_output is not None:result_output['envelope']['prompt_version']='agent-1/3.0.5'
        return replace(audit,output=result_output,output_hash=_hash(result_output) if result_output is not None else audit.output_hash,
            diagnostics={**(audit.diagnostics or {}),'team_call_id':call_id,'legacy_checkpoint_reused':False,
                'team_model_prompt_hash':request.prompt.sha256,'method_feedback_status':values['PRIOR_OUTCOME_FEEDBACK']['status']})
    except ValueError as exc:
        safe=str(exc) if str(exc).startswith(('A1_','TEAM_','MODEL_','CONTROL_')) else 'TEAM_A1_INPUT_INVALID'
        return blocked(safe)
    except (TypeError,KeyError):
        return blocked('TEAM_A1_SOURCE_SHAPE_INVALID')
    except legacy.ResearchPipelineError as exc:
        return replace(blocked(exc.reason_code),diagnostics=exc.diagnostics)
