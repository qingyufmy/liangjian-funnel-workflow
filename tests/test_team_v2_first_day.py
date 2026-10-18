from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
import hashlib
import json
import sqlite3

import pytest

from test_team_v2_monitor_source import prepared as source_prepared
from liangjian_funnel.runtime.risk_engine import RiskLimits,RiskEnvelope
from liangjian_funnel.runtime.team_v2_first_day import (
    POLICY,validate_first_day_policy,first_day_limits,probe_envelope,observe_first_day_packet,forced_record)
from liangjian_funnel.runtime.team_v2_monitor_source import collect_monitor_packet
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore,canonical


def prepared(tmp_path):
    return source_prepared(tmp_path,trade_date=POLICY['activation_trade_date'])


def packet(original,*,now,count,blocked):
    result=deepcopy(original);template=next(iter(result['sources'].values()));result['sources']={}
    for i in range(1,count+1):
        symbol=f'{i:06}.SZ';row=deepcopy(template)
        row['quote']['symbol']=symbol;row['quote']['quote']['symbol']=symbol
        row['quote']['quote']['quote_time']=(now-timedelta(seconds=120) if i<=blocked else now).isoformat()
        result['sources'][symbol]=row
    result['binding']['symbols']=sorted(result['sources']);result['binding']['minute']=now.replace(second=0,microsecond=0).isoformat()
    result['captured_at']=now.isoformat()
    result['packet_sha256']=hashlib.sha256(canonical({k:v for k,v in result.items() if k!='packet_sha256'}).encode()).hexdigest()
    return result


def test_first_day_limits_probe_preserve_engine_ownership_no_double_halving():
    limits=RiskLimits();from datetime import datetime
    now=datetime.fromisoformat('2026-10-20T09:36:00+08:00')
    assert first_day_limits(limits,POLICY,now=now).max_new_entries==2 and limits.max_new_entries==3
    assert first_day_limits(limits,POLICY,now=now+timedelta(days=1))==limits
    original=RiskEnvelope('p','000001.SZ','2026-10-20',True,'STANDARD',Decimal(30000),2,1000,(),(),high_conviction=True,purpose='PLANNING')
    result=probe_envelope(original,POLICY,now=now,probe_multiplier=limits.probe_multiplier)
    assert result.risk_unit=='PROBE' and result.risk_budget==15000 and result.max_shares_cap==500
    assert not result.a6_tightened and not result.high_conviction and result.purpose=='PLANNING'
    assert probe_envelope(result,POLICY,now=now,probe_multiplier=limits.probe_multiplier)==result
    assert probe_envelope(original,POLICY,now=now+timedelta(days=1),probe_multiplier=limits.probe_multiplier)==original


@pytest.mark.parametrize('key,value',[('max_new_entries',3),('probe_only',False),('maximum_freshness_data_block_pct',21),('activation_trade_date','2026-10-19')])
def test_unapproved_first_day_policy_cannot_silently_change_thresholds(key,value):
    with pytest.raises(ValueError,match='CONTROL_FIRST_DAY_POLICY_INVALID'):validate_first_day_policy(dict(POLICY,**{key:value}))


def test_quote_denominator_deduplicated_exact20_not_trip_then_gt20_durable_record(tmp_path):
    settings,clock,scope,calls,args=prepared(tmp_path);store=TeamMeetingStore(settings.root/'state/team_v2/team.db')
    original=collect_monitor_packet(settings,scope,**args);first=packet(original,now=clock.now,count=5,blocked=1)
    observe=lambda p:observe_first_day_packet(store,POLICY,account_id='local-paper',packet=p,now=clock.now)
    result=observe(first);assert not result['record_required'] and result['checked_quotes']==5 and result['blocked_quotes']==1
    assert observe(first)==result
    clock.now+=timedelta(minutes=1);second=packet(original,now=clock.now,count=1,blocked=1)
    result=observe(second)
    assert result['record_required'] and result['latch']['checked_quotes']==6 and result['latch']['blocked_quotes']==2
    # A restarted process and later clean data cannot release the same-day latch.
    store=TeamMeetingStore(store.path);clock.now=clock.now.replace(hour=13)
    result2=observe_first_day_packet(store,POLICY,account_id='local-paper',packet=first,now=clock.now)
    assert result2['latch']==result['latch']
    assert forced_record(settings,account_id='local-paper',now=clock.now)==result['latch']
    assert forced_record(settings,account_id='local-paper',now=clock.now+timedelta(days=1)) is None
    with sqlite3.connect(store.path) as db:
        assert db.execute('SELECT count(*) FROM team_notifications').fetchone()[0]==1
        assert db.execute('SELECT status FROM team_notifications').fetchone()[0]=='PENDING'


def test_changed_same_minute_quote_cannot_rewrite_denominator(tmp_path):
    settings,clock,scope,calls,args=prepared(tmp_path);store=TeamMeetingStore(settings.root/'state/team_v2/team.db')
    original=collect_monitor_packet(settings,scope,**args)
    observe_first_day_packet(store,POLICY,account_id='local-paper',packet=original,now=clock.now)
    changed=packet(original,now=clock.now,count=1,blocked=1)
    with pytest.raises(ValueError,match='CONTROL_FIRST_DAY_OBSERVATION_CHANGED'):
        observe_first_day_packet(store,POLICY,account_id='local-paper',packet=changed,now=clock.now)


def test_guard_anomaly_latches_without_turning_normal_policy_rejection_into_anomaly(tmp_path):
    settings,clock,scope,calls,args=prepared(tmp_path);store=TeamMeetingStore(settings.root/'state/team_v2/team.db')
    original=collect_monitor_packet(settings,scope,**args)
    result=observe_first_day_packet(store,POLICY,account_id='local-paper',packet=original,now=clock.now)
    assert not result['record_required']
    result=observe_first_day_packet(store,POLICY,account_id='local-paper',packet=original,now=clock.now,guard_anomaly=True)
    assert result['record_required'] and result['latch']['reason_code']=='CONTROL_FIRST_DAY_GUARD_ANOMALY'


def test_daily_cap_two_is_checked_inside_actual_reservation_transaction(tmp_path):
    from test_team_v2_execution import fixture,policy
    from test_team_v2_risk_engine import plan,envelope
    from liangjian_funnel.runtime.team_v2_execution import admit_trader_signal
    broker,args=fixture(tmp_path);plans={}
    for i in range(1,4):plans['p'+str(i)]=replace(plan(),plan_id='p'+str(i),symbol=f'{i:06}.SZ',cluster='cluster'+str(i))
    # This low-level reservation fixture deliberately uses its original day;
    # the current production activation date is asserted independently.
    day_policy=dict(POLICY,activation_trade_date=args['now'].date().isoformat())
    outcomes=[];limits=first_day_limits(args['limits'],day_policy,now=args['now'])
    for pid,p in plans.items():
        cap=probe_envelope(replace(envelope(),plan_id=pid,symbol=p.symbol),day_policy,now=args['now'],probe_multiplier=limits.probe_multiplier)
        outcomes.append(admit_trader_signal(**(args|dict(signal_id='first-day:'+pid,plan=p,envelope=cap,limits=limits,
            all_plans=plans,pm_allocation=policy(plans)))))
    assert [r['dispatched'] for r in outcomes]==[True,True,False]
    assert 'DAILY_ENTRY_CAP' in outcomes[2]['judgment']['reason_codes']
    assert len(broker.get_orders('2026-10-19'))==2


def test_actual_stale_quote_record_latch_keeps_legacy_then_owned_protection(tmp_path,monkeypatch):
    from test_team_v2_monitor_dispatch import prepared as dispatch_prepared
    from liangjian_funnel.runtime.team_v2_monitor import run_monitor_control
    from liangjian_funnel.workflow import WorkflowApplication
    app,clock,seen,api,proof=dispatch_prepared(tmp_path,monkeypatch,trade_date='2026-10-20');old=app._fetch_live_quote
    def stale(*args,**kwargs):
        value=old(*args,**kwargs)
        return value.model_copy(update={'quote':value.quote.model_copy(update={'quote_time':clock.now-timedelta(seconds=120)})})
    app._fetch_live_quote=stale
    result=run_monitor_control(app,now=clock.now,clock=lambda:clock.now,client=api,tradability_loader=proof)
    assert result['status']=='CONTROL_RECORD_REQUIRED' and not seen
    with sqlite3.connect(app.settings.root/'state/team_v2/paper.db') as db:
        assert db.execute('SELECT count(*) FROM paper_orders').fetchone()[0]==0
    legacy=[];app.monitor_once=lambda **kw:legacy.append('legacy') or {'status':'LEGACY_RECORD'}
    clock.now+=timedelta(minutes=1)
    import liangjian_funnel.runtime.team_v2_monitor as monitor
    monkeypatch.setattr(monitor,'run_monitor_control',lambda app,**kw:run_monitor_control(
        app,**kw,clock=lambda:clock.now,tradability_loader=proof))
    next_tick=WorkflowApplication.monitor_dispatch(app,now=clock.now)
    assert legacy==['legacy'] and next_tick['mode']=='record'
    assert next_tick['status']=='LEGACY_RECORD'
    assert next_tick['team']['status']=='RECORD_FALLBACK_ACTIVE' and next_tick['team']['model_calls']==0
    assert next_tick['team_control_fallback']['reason_code']=='CONTROL_FIRST_DAY_QUOTE_FRESHNESS_GT20PCT'


def test_alert_enqueue_failure_does_not_release_record_and_internal_projector_repairs_it(tmp_path,monkeypatch):
    from liangjian_funnel.runtime.team_v2_internal import InternalOutbox
    from liangjian_funnel.runtime.team_v2_internal_events import InternalEventProjector
    settings,clock,scope,calls,args=prepared(tmp_path);store=TeamMeetingStore(settings.root/'state/team_v2/team.db')
    original=collect_monitor_packet(settings,scope,**args);enqueue=InternalOutbox.enqueue
    def failure(*args,**kwargs):raise RuntimeError('PRIVATE_NOT_LOGGED')
    monkeypatch.setattr(InternalOutbox,'enqueue',failure)
    result=observe_first_day_packet(store,POLICY,account_id='local-paper',packet=original,now=clock.now,guard_anomaly=True)
    assert result['record_required'] and result['internal_alert']=='PENDING_ENQUEUE_REPAIR'
    assert forced_record(settings,account_id='local-paper',now=clock.now)
    monkeypatch.setattr(InternalOutbox,'enqueue',enqueue)
    projector=InternalEventProjector(InternalOutbox(store))
    projector._team('local-paper','2026-10-20',clock.now)
    projector._team('local-paper','2026-10-20',clock.now)
    with sqlite3.connect(store.path) as db:
        assert db.execute("SELECT count(*) FROM team_notifications WHERE source_id='CONTROL_FIRST_DAY_RECORD'").fetchone()[0]==1
