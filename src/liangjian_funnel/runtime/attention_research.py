"""Explicit outside-ACTIVE A3 research entry, never a close/publication lane.

OPS owns human approval and cumulative HTTP reservation at transport level.
This entry requires a typed, exact-input binding plus an explicit per-batch
guard. It returns isolated case results; it cannot publish or activate plans.
"""
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
import re

from ..pipeline.feature_store import content_hash
from ..pipeline.a3_attention_research import (
    build_attention_a3_domain, evaluate_attention_a3_domain, _case_technical_input,
)


@dataclass(frozen=True)
class AttentionResearchBinding:
    run_id: str
    source_trade_date: str
    snapshot_id: str
    snapshot_hash: str
    snapshot_data_sha256: str
    a2_output_hash: str
    domain_hash: str
    technical_hash: str
    deadline: datetime
    maximum_first_pass_batches: int
    authority_sha256: str
    technical_snapshot_id: str | None = None
    technical_snapshot_hash: str | None = None
    technical_snapshot_data_sha256: str | None = None
    purpose: str = 'ATTENTION_A3_RESEARCH_ONLY'


def _require(value, code):
    if not value: raise ValueError('ATTENTION_RESEARCH_'+code)


def prepare_attention_research(snapshot, a1_output, a2_audit, *, source_trade_date, technical_snapshot=None):
    domain=build_attention_a3_domain(snapshot.data,a1_output,a2_audit,source_trade_date=source_trade_date)
    technical_snapshot=technical_snapshot or snapshot
    _require(technical_snapshot.data.get('g0_symbols')==snapshot.data.get('g0_symbols'),'TECHNICAL_G0_CHANGED')
    if technical_snapshot is not snapshot:
        from ..pipeline.research.common import _snapshot_proof
        proof=_snapshot_proof(technical_snapshot)
        _require(proof['status']=='CAPTURED' and any(
            (node.get('parent_snapshot_id'),node.get('parent_snapshot_hash'))==(snapshot.snapshot_id,snapshot.snapshot_hash)
            for node in proof['chain']),'TECHNICAL_PARENT_SEED_UNPROVEN')
    technical=evaluate_attention_a3_domain(technical_snapshot.data,a1_output,domain)
    return dict(status='TECHNICAL_DRY_RUN_NOT_MODEL_REVIEWED', domain=domain, technical=technical,
        new_model_http=0, new_source_http=0, execution_authority=False)


def run_attention_research(pipeline, *, snapshot, a1_output, a2_audit, binding,
                          assert_transport_budget, assert_current_inputs, lane_id, model, bundle,
                          technical_snapshot=None):
    """Use ordinary Team A3 review only for independently qualified cases.

    No snapshot enrichment/network is implicit. Caller must first supply the
    properly bound enriched snapshot, then bind this exact technical dry run.
    The transport guard must reserve every real request including retries;
    this batch counter is deliberately not presented as an HTTP ledger.
    """
    from ..pipeline import deterministic as d
    from ..pipeline.research.common import FrozenInputSnapshot, _capture_snapshot_recipe
    from ..pipeline.research import _with_a3_deterministic_context
    _require(type(binding) is AttentionResearchBinding and binding.purpose=='ATTENTION_A3_RESEARCH_ONLY','TYPED_BINDING_REQUIRED')
    _require(re.fullmatch(r'[A-Za-z0-9_.-]{1,120}',binding.run_id) is not None,'RUN_ID_INVALID')
    _require(all(isinstance(v,str) and re.fullmatch('[0-9a-f]{64}',v) for v in (
        binding.snapshot_hash,binding.snapshot_data_sha256,binding.a2_output_hash,
        binding.domain_hash,binding.technical_hash,binding.authority_sha256)),'BOUND_HASH_REQUIRED')
    _require(type(binding.maximum_first_pass_batches) is int and binding.maximum_first_pass_batches>=0,'BATCH_CAP_INVALID')
    _require(callable(assert_transport_budget) and callable(assert_current_inputs),'ACTUAL_INPUT_AND_TRANSPORT_GUARDS_REQUIRED')
    now=pipeline.now()
    _require(now.tzinfo is not None and binding.deadline.tzinfo is not None and now<binding.deadline,'DEADLINE_EXCEEDED')
    _require((snapshot.snapshot_id,snapshot.snapshot_hash,content_hash(dict(snapshot.data)))==
        (binding.snapshot_id,binding.snapshot_hash,binding.snapshot_data_sha256),'SNAPSHOT_CHANGED')
    technical_snapshot=technical_snapshot or snapshot
    expected_technical=(binding.technical_snapshot_id,binding.technical_snapshot_hash,binding.technical_snapshot_data_sha256)
    from .closed_session_capture import aware
    _require(aware(snapshot.as_of)<=now and aware(technical_snapshot.as_of)<=now,'FUTURE_INPUT')
    if expected_technical==(None,None,None):
        _require(technical_snapshot is snapshot,'EXACT_TECHNICAL_BINDING_REQUIRED')
    else:
        _require(expected_technical==(technical_snapshot.snapshot_id,technical_snapshot.snapshot_hash,
            content_hash(dict(technical_snapshot.data))),'TECHNICAL_SNAPSHOT_CHANGED')
    prepared=prepare_attention_research(snapshot,a1_output,a2_audit,source_trade_date=binding.source_trade_date,
        technical_snapshot=technical_snapshot)
    domain,technical=prepared['domain'],prepared['technical']
    _require((domain['a1_partitions_unchanged'],domain['a2_output_hash'],domain['content_hash'],technical['content_hash'])==
        (True,binding.a2_output_hash,binding.domain_hash,binding.technical_hash),'DRY_RUN_CHANGED')
    adapter=getattr(pipeline,'team_research_adapter',None)
    _require(adapter is not None and adapter.supports('A3'),'ORIGINAL_TEAM_A3_ADAPTER_REQUIRED')
    candidates=d._candidate_map(technical_snapshot.data)
    local=d._build_a2_local_market_factors(technical_snapshot.data,candidates=candidates,
        industry_membership=d._membership_map(technical_snapshot.data.get('THS_INDUSTRY_MEMBERSHIP'),taxonomy='INDUSTRY'))
    cases={r['case_id']:r for r in domain['records']}
    results={r['case_id']:dict(r) for r in technical['records']};audits=[];stop=None
    for index,batch in enumerate(technical['model_batches']):
        if index>=binding.maximum_first_pass_batches:
            stop='FIRST_PASS_BATCH_CAP_REACHED';break
        if pipeline.now()>=binding.deadline:
            stop='DEADLINE_EXCEEDED';break
        assert_current_inputs(binding)
        assert_transport_budget(binding,batch)  # raises before any stage call
        contexts={};upstream_rows=[];theme_reviews=[]
        for case_id in batch['case_ids']:
            case=cases[case_id]
            scoped,upstream,_,overlay=_case_technical_input(technical_snapshot.data,a1_output,case,local)
            contexts.update(overlay['A2_BOTTLENECK_CONTEXT']);upstream_rows+=upstream['focus_pool']
            if not theme_reviews:theme_reviews=upstream['active_themes']
            else:_require(content_hash(theme_reviews)==content_hash(upstream['active_themes']),'BATCH_THEME_REVIEW_CONFLICT')
        symbols=set(contexts)
        _require(len(symbols)==len(batch['case_ids'])<=16,'BATCH_EXACT_CASE_SCOPE_REQUIRED')
        overlay=dict(A2_BOTTLENECK_CONTEXT=contexts,A3_CANDIDATE_ORIGIN={s:'WATCH_ONLY' for s in symbols},
            A3_CANDIDATE_SCOPE=sorted(symbols),A3_ATTENTION_CASE_IDS=list(batch['case_ids']))
        recipe=dict(base_snapshot_hash=technical_snapshot.snapshot_hash,stage='A3',overlay=overlay)
        derived=FrozenInputSnapshot(technical_snapshot.snapshot_id+':a3:'+content_hash(recipe)[:12],
            {**technical_snapshot.data,**overlay},content_hash(recipe),technical_snapshot.as_of)
        derived=_capture_snapshot_recipe(technical_snapshot,derived,recipe_kind='STAGE_OVERLAY',
            hash_recipe=recipe,data_recipe=dict(operation='SHALLOW_OVERLAY',overlay=overlay))
        upstream=dict(focus_pool=upstream_rows,watch_only_pool=[],active_themes=theme_reviews,
            independent_theme_reviews=[],execution_authority=False)
        gate=d.screen_a3(derived.data,upstream)
        _require(set(gate.review_symbols)==symbols,'TECHNICAL_GATE_CHANGED_BEFORE_MODEL')
        derived=_with_a3_deterministic_context(derived,gate)
        audit=pipeline._run_v2_downstream_review(lane_id=lane_id,model=model,stage='A3',snapshot=derived,
            upstream_output=upstream,full_upstream_symbols=symbols,gate=gate,bundle=bundle,
            run_id=binding.run_id+'.attention.'+content_hash(batch)[:16])
        original=audit.as_dict();audits.append(original)
        assert_current_inputs(binding)
        calls=list((audit.diagnostics or {}).get('team_call_ids') or [])
        output=audit.output if isinstance(audit.output,Mapping) else {}
        rows={r['symbol']:(pool,r) for pool in ('core_watch_pool','secondary_watch_pool','rejected_candidates')
            for r in output.get(pool,[]) if isinstance(r,Mapping)}
        for cid in batch['case_ids']:
            row=results[cid];match=rows.get(row['symbol'])
            row.update(model_state='MODEL_REVIEWED' if calls else 'NOT_MODEL_REVIEWED_CALL_UNPROVEN',
                sent_to_llm=bool(calls),team_call_ids=calls,audit_status=audit.status,
                audit_input_hash=audit.input_hash,audit_output_hash=audit.output_hash,
                model_result_pool=match[0] if match else None,
                model_result={**match[1],'execution_permission':'BLOCKED','risk_unit':'NO_ENTRY',
                    'original_review_row_sha256':content_hash(match[1])} if match else None,
                execution_permission='BLOCKED',risk_unit='NO_ENTRY')
        if audit.status not in {'VALIDATED','VALIDATED_NO_SETUP','VALIDATED_NO_ACTION'}:
            stop='ORDINARY_A3_REVIEW_BLOCKED';break
        if not calls:
            stop='ACTUAL_A3_CALL_REFERENCE_UNPROVEN';break
    for row in results.values():
        if row['model_state']=='PENDING_AUTHORIZED_MODEL_REVIEW':
            row['model_state']='NOT_MODEL_REVIEWED_BUDGET_OR_DEADLINE_DEFERRED'
    result=dict(schema='attention-a3-research-result/1',run_id=binding.run_id,
        status='RESEARCH_CASES_COMPLETED' if stop is None else 'RESEARCH_CASES_INCOMPLETE',
        stop_reason=stop,domain_hash=domain['content_hash'],technical_hash=technical['content_hash'],
        records=list(results.values()),domain_dispositions=domain['dispositions'],original_a3_audits=audits,
        technical_case_count=len(results),model_reviewed_count=sum(r['sent_to_llm'] for r in results.values()),
        not_model_reviewed_count=sum(not r['sent_to_llm'] for r in results.values()),
        first_pass_batches_attempted=len(audits),actual_http_count_basis='EXTERNAL_CUMULATIVE_TRANSPORT_LEDGER_REQUIRED',
        source_trade_date=binding.source_trade_date,execution_authority=False,
        execution_plan_write_requested=False,formal_a1_partitions_unchanged=True,formal_a3_lane_unchanged=True,
        protected_production_state_comparison='REQUIRED_FROM_BOUND_CALLER_NOT_OBSERVED_BY_THIS_PURE_ENTRY')
    result['content_hash']=content_hash(result)
    return result
