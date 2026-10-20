"""F1–F4: immutable publication cap survives all downstream STANDARD suggestions."""
from dataclasses import asdict, replace
from decimal import Decimal as D
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace
import json
import sqlite3

import pytest

from liangjian_funnel.runtime.risk_engine import (
    AccountRisk, PlanRisk, RiskEnvelope, RiskLimits, adjudicate_signal, build_planning_envelope,
    build_risk_envelope, cap_risk_envelope, effective_risk_unit, plan_risk_fields,
)
from liangjian_funnel.runtime.team_v2_sources import decode_plan, read_planning_scope, read_execution_scope
from liangjian_funnel.runtime.team_v2_risk_meeting import apply_premarket_review
from liangjian_funnel.runtime.team_v2_envelope_refresh import refresh_execution_envelopes
from liangjian_funnel.runtime.team_v2_allocation import enforce_pm_allocation
from liangjian_funnel.runtime.team_v2_execution import admit_trader_signal
from test_team_v2_risk_engine import NOW, plan, account, envelope
from test_team_v2_execution import fixture, policy

NINE=json.loads((Path(__file__).parent/'fixtures/team_v2_nine_real_probe_plans_20261020.json').read_text(encoding='utf-8'))


@pytest.mark.parametrize('tag', ['A2_UNAVAILABLE','A2_REVIEW_DEGRADED','PATTERN_B','SERVER_FALLBACK'])
def test_every_publication_degradation_tag_is_hard_probe_even_with_standard_top_and_hints(tag):
    p=plan(**plan_risk_fields(dict(risk_unit='STANDARD', **{tag:True})))
    assert effective_risk_unit(p,'STANDARD','STANDARD','STANDARD')=='PROBE'
    e=build_risk_envelope(p,account(),(),RiskLimits(),now=NOW,regime='TREND_MAINLINE',high_conviction=True)
    assert e.risk_unit=='PROBE' and e.risk_budget==D('15000')
    assert 'CLAMPED_TO_PROBE' in e.risk_cap_audit and 'plan.'+tag in e.risk_cap_audit


def test_single_cap_uses_minimum_not_order_and_never_promotes_none():
    p=plan()
    for index in range(3):
        hints=['STANDARD']*3;hints[index]='PROBE'
        assert effective_risk_unit(p,*hints)=='PROBE'
    assert effective_risk_unit(p,'NONE','PROBE','STANDARD')=='NONE'
    assert effective_risk_unit(p,'STANDARD','STANDARD','STANDARD')=='STANDARD'
    with pytest.raises(ValueError,match='CAP_INVALID'):effective_risk_unit(p,'UNRECOGNIZED')
    blocked=cap_risk_envelope(envelope(),plan=p,a6='NONE')
    assert not blocked.eligible and blocked.risk_unit=='NONE' and blocked.max_shares_cap==0
    retained=cap_risk_envelope(replace(envelope(),plan_risk_unit='PROBE'),plan=p,a4='STANDARD')
    assert retained.risk_unit=='PROBE' and 'envelope.plan_risk_unit' in retained.risk_cap_audit


def test_top_level_probe_not_nested_scenario_and_serialization_remains_backward_compatible():
    raw=dict(risk_unit='PROBE',A2_UNAVAILABLE=True,scenarios=[dict(risk_unit='STANDARD')])
    p=plan(**plan_risk_fields(raw))
    serialized=json.loads(json.dumps(asdict(p),default=str))
    assert decode_plan(serialized)==p
    assert p.risk_unit=='PROBE' and p.degradation_tags==('A2_UNAVAILABLE',)
    for key in ('risk_unit','degradation_tags'):serialized.pop(key)
    assert decode_plan(serialized).risk_unit=='STANDARD'  # Existing nondegraded archived plans.
    with pytest.raises(ValueError,match='CAP_INVALID'):plan(risk_unit='BOGUS')


def test_a6_keep_pm_standard_a4_standard_and_final_quantity_cannot_erase_plan_probe():
    p=plan(risk_unit='PROBE',degradation_tags=('A2_UNAVAILABLE',))
    morning=replace(envelope(),purpose='PLANNING',plan_risk_unit='PROBE',degradation_tags=p.degradation_tags)
    a6=apply_premarket_review((morning,),dict(session='PREMARKET',utilization_cap=dict(cap_pct=75),
        envelope_adjustments=[dict(plan_id=p.plan_id,adjustment='KEEP')]),
        engine_utilization_cap=D(75),validated=True,previous_failures=0).envelopes[0]
    assert a6.risk_unit=='PROBE' and a6.risk_budget==D(15000)
    fresh=refresh_execution_envelopes((p,),morning_envelopes=(a6,),account=account(),positions=(),
        limits=RiskLimits(),now=NOW,regime='TREND_MAINLINE')[0]
    pm,reason=enforce_pm_allocation(policy({'p1':p}),plan=p,envelope=fresh,trade_date='2026-10-19',
        snapshot=dict(positions=[],open_orders=[]))
    assert reason is None and pm.risk_unit=='PROBE'
    a4=cap_risk_envelope(pm,plan=p,a4='STANDARD')
    final=adjudicate_signal('f','BUY_SIGNAL',p,a4,account(),(),RiskLimits(),now=NOW,regime='TREND_MAINLINE')
    assert final.risk_unit=='PROBE' and D(final.shares)*(p.price-p.stop_price)<=D(15000)
    assert 'CLAMPED_TO_PROBE' in final.reason_codes
    assert cap_risk_envelope(a4,plan=p,a4='STANDARD')==a4
    restored=RiskEnvelope(**json.loads(json.dumps(asdict(a4),default=str)))
    restored=replace(restored,risk_budget=D(restored.risk_budget))
    assert restored==a4 and cap_risk_envelope(restored,plan=p)==a4


def test_final_quantity_rechecks_original_plan_even_if_all_upstream_envelopes_are_standard():
    p=plan(risk_unit='PROBE')
    result=adjudicate_signal('final','BUY_SIGNAL',p,envelope(),account(),(),RiskLimits(),now=NOW,
        regime='TREND_MAINLINE')
    assert result.risk_unit=='PROBE' and result.shares<=15000
    assert 'CLAMPED_TO_PROBE' in result.reason_codes and 'plan.risk_unit' in result.reason_codes
    assert envelope().risk_unit=='STANDARD'


def test_non_degraded_standard_and_existing_risk_limits_are_unchanged():
    limits=RiskLimits()
    assert (limits.max_cluster_signals,limits.max_new_entries,limits.max_position_pct)==(2,3,D('.20'))
    assert cap_risk_envelope(envelope(),plan=plan(),pm='STANDARD',a4='STANDARD')==envelope()
    assert adjudicate_signal('std','BUY_SIGNAL',plan(),envelope(),account(),(),limits,now=NOW,
        regime='TREND_MAINLINE').shares==20000
    p=plan(risk_unit='PROBE')
    assert 'DAILY_ENTRY_CAP' in build_risk_envelope(p,account(new_entries_today=3),(),limits,
        now=NOW,regime='TREND_MAINLINE').blocked_reasons
    assert 'CLUSTER_SIGNAL_CAP' in build_risk_envelope(p,account(),(),limits,
        now=NOW,regime='TREND_MAINLINE',cluster_signals_today=2).blocked_reasons


def test_configured_multiplier_and_high_conviction_do_not_double_apply_or_exceed_standard_probe():
    limits=RiskLimits(probe_multiplier=D('.4'))
    p=plan(risk_unit='PROBE')
    fresh=build_risk_envelope(p,account(),(),limits,now=NOW,regime='TREND_MAINLINE',high_conviction=True)
    assert fresh.risk_budget==D(12000)
    assert cap_risk_envelope(fresh,plan=p,limits=limits)==fresh
    forged=build_risk_envelope(plan(),account(),(),limits,now=NOW,regime='TREND_MAINLINE',high_conviction=True)
    capped=cap_risk_envelope(forged,plan=p,limits=limits)
    assert capped.risk_budget==D(12000) and cap_risk_envelope(capped,plan=p,limits=limits)==capped


def test_atomic_paper_journal_retains_clamp_and_original_source_and_idempotency(tmp_path):
    broker,args=fixture(tmp_path)
    p=plan(risk_unit='PROBE',degradation_tags=('A2_UNAVAILABLE',))
    args.update(plan=p,all_plans={'p1':p},pm_allocation=policy({'p1':p}))
    result=admit_trader_signal(**args)
    assert result['dispatched'] and result['judgment']['risk_unit']=='PROBE'
    assert 'CLAMPED_TO_PROBE' in result['judgment']['reason_codes']
    assert 'plan.A2_UNAVAILABLE' in result['judgment']['reason_codes']
    assert result['judgment']['shares']<=15000
    assert admit_trader_signal(**args)==result and len(broker.get_orders('2026-10-19'))==1
    with sqlite3.connect(broker.path) as db:
        text='\n'.join(str(row) for row in db.execute('SELECT * FROM paper_admissions'))
    assert 'CLAMPED_TO_PROBE' in text and 'A2_UNAVAILABLE' in text


def test_readonly_planning_and_execution_source_both_preserve_exact_top_cap_without_payload_edit(tmp_path):
    from test_team_v2_morning import fixture as source_fixture,CAL,advance,source_bytes
    settings,clock=source_fixture(tmp_path)
    with sqlite3.connect(settings.state_db_path) as db:
        raw=json.loads(db.execute('SELECT payload_json FROM execution_plans').fetchone()[0])
        raw.update(risk_unit='PROBE',A2_UNAVAILABLE=True,A2_REVIEW_DEGRADED=True,
            scenarios=[dict(risk_unit='STANDARD')])
        text=json.dumps(raw)
        db.execute('UPDATE execution_plans SET payload_json=?',(text,))
    before=source_bytes(settings)
    morning=read_planning_scope(settings,now=clock.now,calendar=CAL)
    p=decode_plan(morning['plans'][0])
    assert p.risk_unit=='PROBE' and set(p.degradation_tags)=={'A2_UNAVAILABLE','A2_REVIEW_DEGRADED'}
    assert source_bytes(settings)==before
    with sqlite3.connect(settings.state_db_path) as db:
        db.execute("UPDATE execution_plans SET status='ACTIVE_TODAY',valid_from='2026-10-19T09:32:00+08:00'")
    advance(clock,10,1);before=source_bytes(settings)
    execution=read_execution_scope(settings,now=clock.now,calendar=CAL,morning_source=morning,positions=[])
    active=decode_plan(execution['plans'][0])
    assert active.risk_unit==p.risk_unit and active.degradation_tags==p.degradation_tags
    assert execution['plan_rows'][0]['payload_json']==text and source_bytes(settings)==before


@pytest.mark.parametrize('original', NINE['rows'], ids=lambda row:row['symbol'])
def test_nine_real_publication_projections_reach_final_quantity_only_as_probe(tmp_path,original):
    """Original plan facts + explicitly synthetic activation/quote/model suggestions, no I/O provider."""
    from test_team_v2_morning import fixture as source_fixture,CAL,source_bytes
    from liangjian_funnel.runtime.team_v2_allocation import compile_pm_allocation
    from test_team_v2_execution import pm_result
    assert original['status']=='PENDING_MORNING_REVIEW' and original['valid_from'] is None
    assert len(original['original_payload_sha256'])==64
    raw=original['payload_projection']
    assert raw['risk_unit']=='PROBE' and raw['A2_UNAVAILABLE'] is True
    assert raw['source_run_id']==NINE['source_run_id']
    settings,_=source_fixture(tmp_path)
    keys=('plan_id','lane_id','symbol','status','valid_from','expires_at')
    with sqlite3.connect(settings.state_db_path) as db:
        db.execute('DELETE FROM execution_plans')
        db.execute('INSERT INTO execution_plans VALUES (?,?,?,?,?,?,?)',
            (*[original[k] for k in keys],json.dumps(raw)))
    morning_time=datetime.fromisoformat('2026-10-21T08:33:00+08:00')
    calendar=SimpleNamespace(is_trading_day=lambda day:day==date(2026,10,21),
        previous_trading_day=lambda day:date(2026,10,20))
    before=source_bytes(settings)
    source=read_planning_scope(settings,now=morning_time,calendar=calendar)
    assert source['status']=='COMPLETE',source['reason_codes']
    assert source_bytes(settings)==before
    planning_plan=decode_plan(source['plans'][0])
    engine=build_planning_envelope(planning_plan,account(),(),RiskLimits(),now=morning_time,
        regime=raw['market_regime'],high_conviction=True)
    assert engine.eligible and engine.risk_unit=='PROBE' and engine.risk_budget==D(15000)
    a6=apply_premarket_review((engine,),dict(session='PREMARKET',utilization_cap=dict(cap_pct=75),
        envelope_adjustments=[dict(plan_id=original['plan_id'],adjustment='KEEP')]),
        engine_utilization_cap=D(75),validated=True,previous_failures=0).envelopes[0]
    # Synthetic control activates the unchanged publication and supplies a price inside its original interval.
    activated='2026-10-21T09:32:00+08:00'
    with sqlite3.connect(settings.state_db_path) as db:
        db.execute("UPDATE execution_plans SET status='ACTIVE_TODAY',valid_from=?",(activated,))
    now=datetime.fromisoformat('2026-10-21T10:00:00+08:00')
    before=source_bytes(settings)
    current=read_execution_scope(settings,now=now,calendar=calendar,morning_source=source,positions=[])
    assert current['active_plan_ids']==[original['plan_id']] and source_bytes(settings)==before
    p=decode_plan(current['plans'][0])
    p=replace(p,price=(D(str(raw['trigger_low']))+D(str(raw['trigger_high'])))/2,
        tradable=True,data_ready=True)
    a4=refresh_execution_envelopes((p,),morning_envelopes=(a6,),account=account(),positions=(),
        limits=RiskLimits(),now=now,regime=raw['market_regime'])[0]
    pm_raw=pm_result({p.plan_id:p});pm_raw['trade_date']='2026-10-21'
    allocation=compile_pm_allocation(pm_raw,account_id='paper',trade_date='2026-10-21',
        meeting_id='synthetic-standard-pm',input_sha256='b'*64)
    permitted,reason=enforce_pm_allocation(allocation,plan=p,envelope=a4,trade_date='2026-10-21',
        snapshot=dict(positions=[],open_orders=[]))
    assert reason is None
    final_envelope=cap_risk_envelope(permitted,plan=p,a4='STANDARD')
    result=adjudicate_signal('synthetic:'+p.symbol,'BUY_SIGNAL',p,final_envelope,account(),(),RiskLimits(),
        now=now,regime=raw['market_regime'])
    assert result.status in {'APPROVED','REDUCED'} and result.risk_unit=='PROBE' and result.shares>0
    assert D(result.shares)*(p.price-p.stop_price)<=D(15000)
    assert D(result.shares)*p.price<=D(200000)
    assert 'CLAMPED_TO_PROBE' in result.reason_codes and 'plan.A2_UNAVAILABLE' in result.reason_codes
    assert effective_risk_unit(p,a6,'STANDARD','STANDARD')=='PROBE'
    assert cap_risk_envelope(final_envelope,plan=p,a4='STANDARD')==final_envelope


@pytest.mark.parametrize('payload', [dict(A2_UNAVAILABLE='true'),dict(SERVER_FALLBACK=1),
    dict(degradation_tags='A2_UNAVAILABLE'),dict(degradation_tags=['UNKNOWN_CAP']),dict(degradation_tags=[{}]),
    dict(reason_codes='A2_UNAVAILABLE'),dict(reason_codes=[True]),dict(reason_codes=None)])
def test_malformed_top_cap_metadata_is_not_silently_dropped(payload):
    with pytest.raises(ValueError,match='CAP_INVALID'):plan_risk_fields(payload)


def test_unrelated_reason_codes_are_not_new_risk_errors_or_probe_permissions():
    assert plan_risk_fields(dict(reason_codes=['A3_WATCH_ONLY_TECHNICALLY_QUALIFIED_PROBE',
        'ANY_UNRELATED_REASON']))==dict(risk_unit='STANDARD',degradation_tags=())
    assert plan_risk_fields(dict(reason_codes=['A2_UNAVAILABLE']))['degradation_tags']==('A2_UNAVAILABLE',)


def test_none_unit_with_malformed_positive_envelope_is_rejected_at_final_quantity_and_admission(tmp_path):
    forged=replace(envelope(),risk_unit='NONE',eligible=True,risk_budget=D(30000),max_shares_cap=20000)
    capped=cap_risk_envelope(forged,plan=plan())
    assert not capped.eligible and capped.risk_unit=='NONE' and capped.risk_budget==0 and capped.max_shares_cap==0
    assert cap_risk_envelope(capped,plan=plan())==capped
    decision=adjudicate_signal('none-final','BUY_SIGNAL',plan(),forged,account(),(),RiskLimits(),
        now=NOW,regime='TREND_MAINLINE')
    assert decision.status=='REJECTED' and decision.shares==0 and decision.risk_unit=='NONE'
    broker,args=fixture(tmp_path)
    args.update(envelope=forged,signal_id='none-admission')
    admitted=admit_trader_signal(**args)
    assert not admitted['dispatched'] and admitted['judgment']['shares']==0
    assert admitted['judgment']['risk_unit']=='NONE' and broker.get_orders('2026-10-19')==()
    assert admit_trader_signal(**args)==admitted


def test_old_persisted_pm_probe_vote_retains_one_batch_and_original_quantity_tightening(tmp_path):
    from test_team_v2_execution import pm_result
    from liangjian_funnel.runtime.team_v2_allocation import compile_pm_allocation
    p=plan(max_batches=2);e=replace(envelope(),max_batches=2)
    raw=pm_result({'p1':p});raw['plan_allocations'][0].update(intended_risk_unit='PROBE',max_batches=2)
    allocation=compile_pm_allocation(raw,account_id='paper',trade_date='2026-10-19',
        meeting_id='old-pm',input_sha256='b'*64)
    capped,reason=enforce_pm_allocation(allocation,plan=p,envelope=e,trade_date='2026-10-19',
        snapshot=dict(positions=[],open_orders=[]))
    assert reason is None and capped.risk_unit=='PROBE' and capped.max_batches==1
    assert capped.risk_budget==D(15000) and capped.max_shares_cap==10000
    broker,args=fixture(tmp_path)
    args.update(plan=p,all_plans={'p1':p},envelope=e,pm_allocation=allocation)
    assert admit_trader_signal(**args)['dispatched']
    second=admit_trader_signal(**(args|dict(signal_id='old-pm-second')))
    assert not second['dispatched'] and 'TRADER_BATCH_CAP' in second['judgment']['reason_codes']
    assert len(broker.get_orders('2026-10-19'))==1
