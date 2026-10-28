from copy import deepcopy
from pathlib import Path
import json

import pytest
from liangjian_funnel.pipeline.team_v2_effective_risk import digest
from liangjian_funnel.runtime.a6_effective_review import effective_review, validate_review
from liangjian_funnel.runtime.regeneration_data_limits import a6_gap_requirements
from liangjian_funnel.runtime.team_v2_morning import _envelopes
from liangjian_funnel.runtime.team_v2_risk_meeting import apply_premarket_review


def original():
    value = json.loads((Path(__file__).parent/'fixtures/postclose_20261027/a6_capacity_0929_original.json').read_bytes())
    raw = value['raw']; fields = value['fields']
    assert digest(raw) == value['raw_sha256']
    envelopes = _envelopes(fields['ENGINE_ENVELOPES'])
    context = fields['RISK_LIMITS']['server_cluster_capacity']
    arguments = dict(required_envelope=raw['envelope'], envelopes=envelopes,
        engine_utilization_cap=fields['RISK_LIMITS']['engine_upper_bounds']['engine_utilization_cap'],
        capacity_slots=context['engine_slots'], cluster_caps=context['cluster_caps'],
        unready_plan_ids={e.plan_id for e in envelopes if not e.eligible},
        cluster_plan_counts=context['cluster_plan_counts'])
    return value, dict(catalog=fields['EVIDENCE_CATALOG'],arguments=arguments,
        requirements=a6_gap_requirements(fields['EVIDENCE_CATALOG'],envelopes),
        quota_context=context,captured_issues=value['issues'])


def test_real_capacity6_reply_reduces_to_exact4_and_preserves_original():
    value, args = original(); raw = value['raw']; before = deepcopy(raw)
    assert not validate_review(raw,args['catalog'],args['arguments'],args['requirements']).valid
    effective, audit = effective_review(raw,**args)
    assert raw == before and digest(raw) == value['raw_sha256']
    assert audit['candidate_count'] == 7
    assert audit['candidate_eligible_count'] == 4 and audit['capacity_trimmed_count'] == 3
    symbols = {x['value']['plan_id']: x['value']['symbol']
        for x in args['catalog']['items'] if x['category']=='PLAN'}
    assert {symbols[p] for p in audit['executable_plan_ids']} == {'000858.SZ','600628.SH','002966.SZ','601077.SH'}
    assert {symbols[x['plan_id']] for x in audit['trimmed']} == {'600694.SH','601825.SH','603101.SH'}
    correction = audit['corrections']
    assert len(correction)==1 and symbols[correction[0]['plan_id']]=='601077.SH'
    assert correction[0]['code']=='NONEXISTENT_SOURCE_GAP_REFERENCE'
    assert correction[0]['after']['adjustment']=='KEEP'
    assert len(correction[0]['after']['what_would_change_my_mind'])==1
    meeting = apply_premarket_review(args['arguments']['envelopes'],effective,
        engine_utilization_cap=args['arguments']['engine_utilization_cap'],validated=True,previous_failures=0)
    assert meeting.status=='READY' and sum(e.eligible for e in meeting.envelopes)==4
    assert sum('CAPACITY_TRIMMED' in e.blocked_reasons for e in meeting.envelopes)==3


@pytest.mark.parametrize('issue',['A6_ENVELOPE_BINDING_MISMATCH','A6_DATA_LIMITED_REVIEW_REQUIRED:any',
                                 'A6_ADJUSTMENT_INVALID','TEAM_MODEL_CALL_FAILED'])
def test_unapproved_issues_cannot_be_normalized(issue):
    value,args=original();args['captured_issues']=[*args['captured_issues'],issue]
    assert effective_review(value['raw'],**args) is None


def test_missing_real_gap_review_and_bad_domain_still_block():
    value,args=original();raw=deepcopy(value['raw'])
    raw['envelope_adjustments'][0]['what_would_change_my_mind']=['仅技术条件']
    assert effective_review(raw,**args) is None
    raw=deepcopy(value['raw']);raw['envelope_adjustments'][0]['plan_id']='unbound'
    assert effective_review(raw,**args) is None


def test_original_suspend_is_never_revived_or_reordered():
    value,args=original();raw=deepcopy(value['raw']);raw['envelope_adjustments'][0]['adjustment']='SUSPEND'
    effective,audit=effective_review(raw,**args)
    assert effective['envelope_adjustments'][0]['adjustment']=='SUSPEND'
    assert [r['plan_id'] for r in raw['envelope_adjustments']]==[r['plan_id'] for r in effective['envelope_adjustments']]


def test_real_stream_raw_invalid_is_accepted_only_by_recomputed_cas_evidence(tmp_path,monkeypatch):
    import sqlite3
    from test_reviewed_batch_evidence_0546 import prepared
    from liangjian_funnel.runtime.reviewed_batch_evidence import build_reviewed_batch_evidence
    settings,arguments=prepared(tmp_path,monkeypatch,a6_capacity_override=99)
    body=build_reviewed_batch_evidence(**arguments)
    with sqlite3.connect(settings.root/'state/team_v2/model.db') as db:
        row=db.execute("SELECT validated,output_json FROM model_calls WHERE role='A6'").fetchone()
    assert row[0]==0 and json.loads(row[1])['capacity_for_tomorrow']['plans_acceptable']==99
    assert body['evidence']['a6']['effective_review']['original_validated'] is False
    assert body['evidence']['a6']['effective_review']['candidate_eligible_count']==1
    with sqlite3.connect(settings.root/'state/team_v2/team.db') as db:
        key='a6-effective-review:review-scope:risk'
        raw=db.execute('SELECT body_json FROM control_minute_context WHERE decision_id=?',(key,)).fetchone()[0]
        changed=json.loads(raw);changed['candidate_eligible_count']=99
        db.execute('UPDATE control_minute_context SET body_json=? WHERE decision_id=?',(json.dumps(changed),key))
    with pytest.raises(ValueError,match='PROOF_RECOMPUTATION_CONFLICT'):
        build_reviewed_batch_evidence(**arguments)


def test_original522_does_not_create_any_effective_reply(tmp_path):
    import httpx,sqlite3
    from test_team_v2_risk_session import quota_fixture
    from liangjian_funnel.runtime.team_v2_risk_session import run_risk_premarket
    from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient
    kwargs,_,_=quota_fixture(tmp_path)
    reached=[]
    def handler(request):
        reached.append(request)
        return httpx.Response(522,text='controlled replay of original 522 status',request=request)
    kwargs['client']=OpenAICompatibleModelClient(kwargs['client'].settings,
        transport=httpx.MockTransport(handler),max_attempts=1)
    assert run_risk_premarket(**kwargs).status=='DEGRADED'
    assert len(reached)==1
    with sqlite3.connect(tmp_path/'team.db') as db:
        assert db.execute("SELECT count(*) FROM control_minute_context WHERE decision_id LIKE 'a6-effective-review:%'").fetchone()[0]==0


def test_trimmed_three_remain_blocked_after_fresh_execution_envelope_and_buy_vote():
    from datetime import datetime
    from decimal import Decimal as D
    from liangjian_funnel.runtime.risk_engine import PlanRisk,AccountRisk,RiskLimits,adjudicate_signal
    from liangjian_funnel.runtime.team_v2_envelope_refresh import refresh_execution_envelopes
    value,args=original();raw,audit=effective_review(value['raw'],**args)
    morning=apply_premarket_review(args['arguments']['envelopes'],raw,
        engine_utilization_cap=args['arguments']['engine_utilization_cap'],validated=True,previous_failures=0)
    now=datetime.fromisoformat('2026-10-28T09:40:00+08:00');plans=[]
    for row in args['catalog']['items']:
        if row['category']!='PLAN':continue
        source=row['value'];facts=source['publication_facts']
        plans.append(PlanRisk(source['plan_id'],source['symbol'],source['cluster'],'ACTIVE_TODAY',
            now.replace(minute=32),datetime.fromisoformat(source['expires_at']),facts['reference_price'],
            facts['stop_level'],facts['trigger_low'],facts['trigger_high'],facts['no_chase'],risk_unit='PROBE'))
    account=AccountRisk(D('1000000'),D('1000000'),D('1000000'),D('1000000'));limits=RiskLimits()
    envelopes=refresh_execution_envelopes(plans,morning_envelopes=morning.envelopes,account=account,
        positions=(),limits=limits,now=now,regime=value['raw']['envelope']['market_regime'])
    byid={p.plan_id:p for p in plans};trimmed={r['plan_id'] for r in audit['trimmed']}
    for envelope in envelopes:
        if envelope.plan_id not in trimmed:continue
        assert not envelope.eligible and envelope.max_shares_cap==0 and 'CAPACITY_TRIMMED' in envelope.blocked_reasons
        decision=adjudicate_signal('CONTROLLED', 'BUY_SIGNAL',byid[envelope.plan_id],envelope,account,(),limits,
            now=now,regime=value['raw']['envelope']['market_regime'])
        assert decision.status=='REJECTED'


def test_model_cannot_claim_server_capacity_trimmed_reason():
    value,args=original();raw=deepcopy(value['raw'])
    raw['envelope_adjustments'][0].update(adjustment='SUSPEND',reason_code='CAPACITY_TRIMMED')
    assert effective_review(raw,**args) is None


def test_capacity_customer_snapshots_use_separate7_4_3_counts(tmp_path,monkeypatch):
    from datetime import datetime
    from liangjian_funnel.runtime.lark_notifications import WorkflowLarkPublisher
    from liangjian_funnel.runtime.state import RuntimeStore
    import liangjian_funnel.runtime.a6_effective_review as module
    from test_d8_real_outputs_0837 import CapturingNotifier,text
    value,args=original();raw,audit=effective_review(value['raw'],**args)
    summary=dict(status='AVAILABLE',candidate_count=7,candidate_eligible_count=4,capacity_trimmed_count=3,
        trimmed_plan_ids=[x['plan_id'] for x in audit['trimmed']],eligible_plan_ids=audit['executable_plan_ids'])
    monkeypatch.setattr(module,'read_capacity_summary',lambda *a,**k:summary)
    publisher=WorkflowLarkPublisher(RuntimeStore(tmp_path/'workflow.sqlite3'),None)
    notifier=CapturingNotifier();publisher.notifier=notifier;now=datetime.fromisoformat('2026-10-28T09:26:00+08:00')
    publisher.publish_batch_switch_status(trade_date='2026-10-28',scope_id='CONTROLLED_LJ099',
        plan_count=7,body_sha256='c'*64,observed_at=now,capacity_review=summary)
    plans=[dict(plan_id=r['value']['plan_id'],symbol=r['value']['symbol'],lane_id=r['value']['lane_id'],
        status='PENDING_MORNING_REVIEW',payload_json=json.dumps(r['value']['publication_facts']))
        for r in args['catalog']['items'] if r['category']=='PLAN']
    publisher.publish_premarket(plans,reviewed_at=now,evidence={})
    for card in notifier.cards:assert '候选 7 / 可执行 4 / 容量截取 3' in text(card)
    assert sum(text(card).count('容量截取，仅观察，禁止新入场') for card in notifier.cards)==3
    out=Path('D:/dev_A股/liangjian_team_v2_20261016/artifacts/a4-monitor-20261021/D8_LJ099_CONTROLLED_20261028')
    out.mkdir(exist_ok=True)
    (out/'CAPACITY_SNAPSHOTS.json').write_text(json.dumps(dict(scope='CONTROLLED_RENDER_NOT_TODAY_PUBLICATION',
        raw_sha256=value['raw_sha256'],projection_sha256=digest(audit),cards=notifier.cards,
        notification_http=0,model_http=0),ensure_ascii=False,indent=2),encoding='utf-8')


def test_real1027_valid_qualifications_and_six_primary_six_backup_are_unchanged():
    from dataclasses import asdict
    from decimal import Decimal
    from liangjian_funnel.pipeline.team_v2_cluster_quota import build_cluster_quota_context
    from liangjian_funnel.runtime.team_v2_pm_policy import priority_order
    from liangjian_funnel.runtime.team_v2_sources import meeting_plan_facts
    from liangjian_funnel.pipeline.team_v2_pm_schema import project_pm_compact
    from liangjian_funnel.runtime.team_v2_store import canonical
    x=json.loads((Path(__file__).parent/'fixtures/postclose_20261027/a6_valid_1027_original.json').read_bytes())
    original_a6=next(r for r in x['calls'] if r['role']=='A6');raw=json.loads(original_a6['output_json'])
    assert original_a6['validated']==1 and digest(raw)==original_a6['output_hash']
    engine=json.loads(x['engine']['body_json']);inp=json.loads(x['risk']['input_json'])
    envelopes=_envelopes(inp['envelopes']);catalog=json.loads(original_a6['catalog']['catalog_json'])
    clusters={p['plan_id']:p['cluster'] for p in engine['source']['plans']}
    context=build_cluster_quota_context(engine_slots=engine['capacity_slots'],cluster_caps=engine['cluster_caps'],
        plan_clusters=clusters,plan_priority_order=priority_order(meeting_plan_facts(engine['source']['plan_rows'])))
    arguments=dict(required_envelope=raw['envelope'],envelopes=envelopes,
        engine_utilization_cap=inp['engine_utilization_cap'],capacity_slots=engine['capacity_slots'],
        cluster_caps=engine['cluster_caps'],unready_plan_ids={e.plan_id for e in envelopes if not e.eligible},
        cluster_plan_counts=context['cluster_plan_counts'])
    requirements=a6_gap_requirements(catalog,envelopes)
    assert validate_review(raw,catalog,arguments,requirements).valid
    assert effective_review(raw,catalog=catalog,arguments=arguments,requirements=requirements,quota_context=context) is None
    risk=apply_premarket_review(envelopes,raw,engine_utilization_cap=inp['engine_utilization_cap'],validated=True,previous_failures=0)
    assert json.loads(canonical(asdict(risk)))==json.loads(x['risk']['result_json'])
    assert sum(e.eligible for e in risk.envelopes)==14
    original_pm=next(r for r in x['calls'] if r['session']=='PM_PREMARKET')
    pmraw=json.loads(original_pm['output_json']);pmcatalog=json.loads(original_pm['catalog']['catalog_json'])
    assert original_pm['validated']==1 and digest(pmraw)==original_pm['output_hash']
    refs={r['id']:r['value']['plan_id'] for r in pmcatalog['items'] if r['category']=='PLAN'}
    projected,validation=project_pm_compact(pmraw,pmcatalog,plan_refs=refs,
        required_envelope=pmraw['envelope'],envelopes=risk.envelopes,plan_clusters=clusters,
        free_slots=6,baseline_range=tuple(Decimal(str(v)) for v in engine['baseline_range']),
        utilization_cap=risk.utilization_cap,high_conviction_ids=set())
    assert validation.valid,validation.issues
    expected=json.loads(x['pm']['result_json'])
    assert projected['slot_plan']==expected['slot_plan']
    assert projected['plan_allocations']==expected['plan_allocations']
    assert len(projected['slot_plan'])==6 and sum(bool(r['backup_plan_id']) for r in projected['slot_plan'])==6


def test_false_gap_label_only_never_trims_otherwise_valid_primary_backup_pool():
    value,args=original();raw=deepcopy(value['raw'])
    raw['capacity_for_tomorrow']['plans_acceptable']=4
    args['captured_issues']=[]
    effective,proof=effective_review(raw,**args)
    assert proof['projection_kind']=='LABEL_ONLY' and not proof['capacity_arithmetic_invalid']
    assert proof['capacity_trimmed_count']==0 and len(proof['executable_plan_ids'])==7
    assert effective['capacity_for_tomorrow']==raw['capacity_for_tomorrow']
    assert [r['adjustment'] for r in effective['envelope_adjustments']]==[r['adjustment'] for r in raw['envelope_adjustments']]
