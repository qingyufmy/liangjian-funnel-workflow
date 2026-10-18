"""Real owned paper commits -> bounded projection -> fake internal HTTP ACK."""
from datetime import datetime
import hashlib
import json
import sqlite3

from liangjian_funnel.runtime.team_v2_internal_events import InternalEventProjector
from liangjian_funnel.runtime.team_v2_internal import InternalOutbox
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore, canonical
from liangjian_paper_broker.matching import PaperRules
from test_team_v2_internal import sender, URL
from test_team_v2_paper_ledger import broker, place, drive, bar, stamp


def fixture(tmp_path):
    b=broker(tmp_path/'paper.db');store=TeamMeetingStore(tmp_path/'team.db')
    box=InternalOutbox(store)
    return b,box,InternalEventProjector(box)


def reject(b,key,mode='control'):
    return b.admit_signal(key,binding={'plan_id':'p1','symbol':'000001.SZ','trade_date':'2026-10-19'},
        decide=lambda snapshot:{'judgment':{'status':'REJECTED','shares':0,'reason_codes':['TEST_REJECTED']},'order':None},mode=mode)


def project(projector,b,now=None):
    return projector.project(paper_path=b.path,account_id='local-paper',now=now or stamp(tm='10:02:00'))


def messages(box):
    with sqlite3.connect(box.store.path) as db:
        return db.execute('SELECT kind,source_id,body_json,status FROM team_notifications ORDER BY kind,source_id').fetchall()


def test_actual_rejections_increment_once_and_threshold_alert_does_not_mutate_orders(tmp_path):
    b,box,p=fixture(tmp_path)
    for i in range(7):reject(b,'s'+str(i))
    before=hashlib.sha256(b.path.read_bytes()).hexdigest()
    assert project(p,b)['status']=='READY'
    first=messages(box)
    assert len([r for r in first if r[0]=='DECISION'])==7
    assert len([r for r in first if r[0]=='ALERT'])==1
    assert project(InternalEventProjector(box),b)['status']=='READY' and messages(box)==first
    assert hashlib.sha256(b.path.read_bytes()).hexdigest()==before
    send,seen=sender(tmp_path,env={'SHADOW_RESEARCH_WEBHOOK':URL})
    assert box.flush(send,now=stamp(tm='10:02:00'))['sent']==3
    assert 'TRADER_SIGNAL_BLOCKED_BY_GUARDRAIL' in json.loads(seen[0].content)['content']['text']


def test_record_mode_is_explicit_and_not_counted_as_live_guard_alert(tmp_path):
    b,box,p=fixture(tmp_path)
    for i in range(7):reject(b,str(i),mode='record')
    assert project(p,b)['status']=='READY'
    assert not [r for r in messages(box) if r[0]=='ALERT']
    assert all('RECORD' in r[2] for r in messages(box) if r[0]=='DECISION')


def test_committed_fill_is_separate_from_decision_and_basis_not_fabricated(tmp_path):
    b,box,p=fixture(tmp_path);place(b);drive(b,bar(),PaperRules('2026-10-19'))
    assert project(p,b)['status']=='READY'
    fill=[r for r in messages(box) if r[0]=='FILL'];assert len(fill)==1
    assert '000001.SZ' in fill[0][2] and 'REPORTED_VWAP' in fill[0][2]
    assert not [r for r in messages(box) if r[0]=='DECISION']
    assert b.get_account()['cash']=='989995.00' or b.get_account()['cash']=='989995'


def test_bounded_cursor_restart_and_replaced_anchor_fail_visible(tmp_path):
    b,box,p=fixture(tmp_path)
    for i in range(101):reject(b,'s'+str(i),mode='record')
    assert project(p,b)['status']=='READY'
    assert len([r for r in messages(box) if r[0]=='DECISION'])==100
    assert project(p,b)['status']=='READY'
    assert len([r for r in messages(box) if r[0]=='DECISION'])==101
    with sqlite3.connect(b.path) as db:
        db.execute("UPDATE paper_admissions SET result_json='{}' WHERE rowid=101")
    assert project(p,b)['reason_codes']==['INTERNAL_PAPER_PROJECTION_FAILED']
    assert len([r for r in messages(box) if r[0]=='DECISION'])==101


def test_missing_paper_does_not_create_account_or_prevent_degraded_morning(tmp_path):
    b,box,p=fixture(tmp_path);b.path.unlink()
    assert project(p,b)['status']=='DATA_LIMITED' and not b.path.exists()
    morning=[r for r in messages(box) if r[0]=='MORNING']
    assert len(morning)==1 and '缺失' in morning[0][2] and 'UNAVAILABLE' in morning[0][2]


def test_regime_ack_tracks_real_send_not_projection_and_same_day_risk_latch(tmp_path):
    b,box,p=fixture(tmp_path)
    evidence=dict(kind='emotion',previous='EMOTION_OFF',current='EMOTION_ON',tick=stamp().isoformat())
    with sqlite3.connect(box.store.path) as db:
        db.execute('INSERT INTO regime_transitions(transition_id,trade_date,tick,evidence_json) VALUES (?,?,?,?)',
            ('flip','2026-10-19',stamp().isoformat(),canonical(evidence)))
        db.execute('INSERT INTO risk_day_state VALUES (?,?,?,?,?)',('local-paper','2026-10-19',2,1,stamp().isoformat()))
    assert project(p,b)['status']=='READY'
    p.sync_acknowledgments(account_id='local-paper',trade_date='2026-10-19')
    with sqlite3.connect(box.store.path) as db:
        assert db.execute('SELECT notification_status FROM regime_transitions').fetchone()==('PENDING',)
    send,seen=sender(tmp_path,env={'SHADOW_RESEARCH_WEBHOOK':URL})
    box.flush(send,now=stamp(tm='10:02:00'));p.sync_acknowledgments(account_id='local-paper',trade_date='2026-10-19')
    with sqlite3.connect(box.store.path) as db:
        assert db.execute('SELECT notification_status FROM regime_transitions').fetchone()==('SENT',)
    assert any('A6_DEGRADED_HALT' in r[2] for r in messages(box))


def test_future_commits_not_announced_and_other_projection_still_runs(tmp_path):
    b,box,p=fixture(tmp_path);reject(b,'future')
    assert project(p,b,now=stamp(tm='09:59:00'))['status']=='DATA_LIMITED'
    assert not [r for r in messages(box) if r[0]=='DECISION']
    assert project(p,b)['status']=='READY'
    assert len([r for r in messages(box) if r[0]=='DECISION'])==1


def test_real_frozen_settlement_is_read_only_no_fake_nav_when_marks_missing(tmp_path):
    b,box,p=fixture(tmp_path);place(b);drive(b,bar(),PaperRules('2026-10-19'))
    b.clock=lambda:stamp(tm='15:30:00');report=b.settle('2026-10-19')
    assert report['status']=='DATA_LIMITED'
    before=b.snapshot_bytes()
    assert project(p,b,now=stamp(tm='15:31:00'))['status']=='READY'
    card=[r for r in messages(box) if r[0]=='SETTLEMENT'][0]
    assert '净值：资料不足' in json.loads(card[2])['content']['text']
    assert b.snapshot_bytes()==before


def test_enqueue_cursor_commit_crash_reuses_original_event(tmp_path):
    b,box,p=fixture(tmp_path);reject(b,'one')
    with sqlite3.connect(box.store.path) as db:
        db.execute("CREATE TRIGGER cursor_fault BEFORE INSERT ON team_notification_cursors BEGIN SELECT RAISE(ABORT,'FAULT'); END")
    assert project(p,b)['status']=='DATA_LIMITED'
    assert len([r for r in messages(box) if r[0]=='DECISION'])==1
    with sqlite3.connect(box.store.path) as db:db.execute('DROP TRIGGER cursor_fault')
    assert project(p,b)['status']=='READY'
    assert len([r for r in messages(box) if r[0]=='DECISION'])==1


def test_rule_alert_original_stays_frozen_only_real_ack_claimed(tmp_path):
    b,box,p=fixture(tmp_path);path=tmp_path/'2026-10-19.alert.json'
    raw=dict(trade_date='2026-10-19',audience='INTERNAL_ONLY',delivery_status='PENDING',
        reason_code='RULE_VERSION_CHANGED_NO_AUTO_ACCEPT',blocks_new_entries=True,receipt_sha256='a'*64)
    path.write_text(canonical(raw),encoding='utf-8');before=path.read_bytes()
    assert p.project(paper_path=b.path,account_id='local-paper',now=stamp(),rule_alert_path=path)['status']=='READY'
    assert len([r for r in messages(box) if r[0]=='ALERT'])==1
    send,seen=sender(tmp_path,env={'SHADOW_RESEARCH_WEBHOOK':URL});box.flush(send,now=stamp())
    assert path.read_bytes()==before
    raw['trade_date']='2026-10-18';path.write_text(canonical(raw),encoding='utf-8')
    assert p.project(paper_path=b.path,account_id='local-paper',now=stamp(),rule_alert_path=path)['reason_codes']==['INTERNAL_RULE_PROJECTION_FAILED']


def test_no_report_today_is_internal_fact_not_a_fabricated_a5_report(tmp_path):
    b,box,p=fixture(tmp_path)
    assert project(p,b,now=stamp(tm='17:10:00'))['status']=='READY'
    alerts=[r for r in messages(box) if r[0]=='ALERT']
    assert len(alerts)==1 and 'NO_REPORT_TODAY' in alerts[0][2]
    with sqlite3.connect(box.store.path) as db:
        assert not db.execute("SELECT name FROM sqlite_master WHERE name='team_a5_reviews'").fetchall()


def test_empty_current_day_advances_real_prior_anchor_without_importing_old_events(tmp_path):
    b,box,p=fixture(tmp_path);b.clock=lambda:stamp(day='16')
    for i in range(7):
        b.admit_signal('prior:'+str(i),binding={'plan_id':'old','symbol':'000001.SZ','trade_date':'2026-10-16'},
            decide=lambda _:dict(judgment=dict(status='REJECTED',shares=0,reason_codes=['OLD']),order=None),mode='record')
    b.clock=stamp
    assert project(p,b)['status']=='READY'
    assert not [r for r in messages(box) if r[0]=='DECISION']
    with sqlite3.connect(box.store.path) as db:
        assert db.execute("SELECT last_row FROM team_notification_cursors WHERE stream='paper_admissions'").fetchone()==(7,)
    reject(b,'today')
    assert project(p,b)['status']=='READY'
    assert len([r for r in messages(box) if r[0]=='DECISION'])==1
