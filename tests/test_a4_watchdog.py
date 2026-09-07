from datetime import datetime,timedelta
from types import SimpleNamespace

import json
from liangjian_funnel.runtime.watchdog import WatchdogLedger,assess,expected_minutes,gap_ranges,completed_minutes_from_log


def at(clock):return datetime.fromisoformat('2026-09-07T'+clock+':00+08:00')


def healthy(now):
    mins=expected_minutes(now)
    return {'reachable':True,'app_healthy':True,'minutes':mins,'last_completed_minute':mins[-1] if mins else None,
            'leases':[{'lease_name':'scheduler:'+x,'state':'COMPLETED','last_dispatch_key':'2026-09-07'}
                      for x in ['premarket_0830','morning_0925','a5_midday_1135','a5_post_close_1600','close_1510']]}


class Notifier:
    def __init__(self,ok=True):self.sent=[];self.ok=ok
    def send(self,*args):
        self.sent.append(args)
        return SimpleNamespace(ok=self.ok,reason_code='OK' if self.ok else 'FAILED')


def test_calendar_lunch_and_completion_grace():
    assert expected_minutes(at('09:32'))==[]
    assert len(expected_minutes(at('12:00')))==120
    assert len(expected_minutes(at('13:01')))==120
    assert len(expected_minutes(at('15:02')))==240
    assert not assess(at('12:00'),healthy(at('12:00')),trading_day=True)['problems']
    assert assess(at('10:00'),{},trading_day=False)['skipped']


def test_live_health_does_not_hide_stalled_business():
    now=at('10:00');snap=healthy(now);snap['last_completed_minute']=at('09:40').isoformat()
    result=assess(now,snap,trading_day=True)
    assert 'MONITOR_STALLED' in result['problems']


def test_outage_and_recovery_once_and_durable(tmp_path):
    path=tmp_path/'watch.sqlite3';n=Notifier();now=at('10:00')
    bad=assess(now,{},trading_day=True)
    ledger=WatchdogLedger(path)
    ledger.process(bad,n,now);ledger.process(bad,n,now+timedelta(minutes=1))
    assert len(n.sent)==1
    ledger.db.close();ledger=WatchdogLedger(path)
    ledger.process(assess(now,healthy(now),trading_day=True),n,now+timedelta(minutes=2))
    ledger.process(assess(now,healthy(now),trading_day=True),n,now+timedelta(minutes=3))
    assert len(n.sent)==2


def test_unreachable_host_cannot_close_stall_incident(tmp_path):
    n=Notifier();l=WatchdogLedger(tmp_path/'db');now=at('10:00');snap=healthy(now);snap['last_completed_minute']=''
    l.process(assess(now,snap,trading_day=True),n,now)
    l.process(assess(now,{},trading_day=True),n,now+timedelta(minutes=1))
    assert l.db.execute("select active from incident where code='MONITOR_STALLED'").fetchone()[0]==1
    assert not any('恢复' in x[0] for x in n.sent)


def test_failed_outage_delivery_cannot_be_bypassed_by_recovery(tmp_path):
    n=Notifier(False);l=WatchdogLedger(tmp_path/'db');now=at('10:00')
    l.process(assess(now,{},trading_day=True),n,now)
    l.process(assess(now,{},trading_day=True),n,now+timedelta(minutes=1))
    n.ok=True
    good=assess(now,healthy(now),trading_day=True)
    l.process(good,n,now+timedelta(minutes=2))
    assert len(n.sent)==2  # fault is still in its two-minute backoff
    l.process(good,n,now+timedelta(minutes=3))
    assert len(n.sent)==4
    assert '告警' in n.sent[-2][0] and '恢复' in n.sent[-1][0]


def test_restart_gap_detection_without_prior_incident(tmp_path):
    now=at('13:20');snap=healthy(now)
    snap['minutes']=[m for m in snap['minutes'] if not 'T10:40'<=m[10:16]<='T11:30']
    # Explicit range construction also verifies lunch never enters the gap.
    missing=[m for m in expected_minutes(now) if '10:40'<=m[11:16]<='11:30']
    snap['minutes']=[m for m in snap['minutes'] if m not in missing]
    result=assess(now,snap,trading_day=True)
    assert sum(g['minutes'] for g in result['gaps'])==51
    n=Notifier();l=WatchdogLedger(tmp_path/'db')
    l.process(result,n,now);l.process(result,n,now+timedelta(minutes=1))
    assert len(n.sent)==1 and '缺口' in n.sent[0][0]


def test_close_minute_missing_is_detected_and_midday_job_missed():
    now=at('15:03');snap=healthy(now)
    snap['minutes']=snap['minutes'][:-1];snap['last_completed_minute']=snap['minutes'][-1];snap['leases']=[]
    result=assess(now,snap,trading_day=True)
    assert 'MONITOR_STALLED' in result['problems']
    assert 'MISSED_a5_midday_1135' in result['problems']
    assert result['gaps'][-1]['start']==at('15:00').isoformat()


def test_gap_ranges_splits_lunch():
    assert len(gap_ranges([at('11:30').isoformat(),at('13:01').isoformat()]))==2


def test_completed_log_requires_business_dispatch_and_success():
    rows=[]
    for run,dispatched,finished in [('ok',True,True),('noop',False,True),('crash',True,False)]:
        for stream,message in [('stdout','  "time": "2026-09-07T13:20:03+08:00"'),
                               ('stdout','"status": "DISPATCHED"' if dispatched else '"dispatch": []'),
                               ('node','任务结束 run-monitor status=succeeded exit=0' if finished else '开始执行')]:
            rows.append(json.dumps({'job':'monitor','runId':run,'stream':stream,'message':message}))
    assert completed_minutes_from_log('\x00'+'\n'.join(rows))==[at('13:20').isoformat()]
