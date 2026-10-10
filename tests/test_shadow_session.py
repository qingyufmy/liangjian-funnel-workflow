"""Standalone fixtures only; no RuntimeStore, provider, baseline call or model."""
from datetime import datetime, timedelta
import hashlib
import importlib
import json
import sqlite3
import uuid
import zlib
from pathlib import Path

import pytest

AT=datetime.fromisoformat('2026-10-12T10:00:00+08:00')
CUTOFF=AT-timedelta(minutes=1)


def module(): return importlib.import_module('liangjian_funnel.runtime.shadow_session')


def databases(tmp_path, count=1):
    state,minute=tmp_path/'state.db',tmp_path/'minute.db'
    with sqlite3.connect(state) as db:
        db.executescript('''CREATE TABLE execution_plans(plan_id TEXT PRIMARY KEY,lane_id TEXT,symbol TEXT,status TEXT,plan_version INTEGER,valid_from TEXT,expires_at TEXT,payload_json TEXT,created_at TEXT,updated_at TEXT);
        CREATE TABLE monitor_events(event_id TEXT PRIMARY KEY,event_key TEXT,lane_id TEXT,minute_end TEXT,action TEXT,reason_code TEXT,effective INTEGER,payload_json TEXT,created_at TEXT);
        CREATE TABLE virtual_positions(account_id TEXT,symbol TEXT,total_qty INTEGER);
        ''')
        for i in range(count):
            pid,symbol=f'p{i}',f'60000{i}.SH'
            plan={'plan_id':pid,'symbol':symbol,'strategy_profile':'TREND_MA5','strategy_facts':{}}
            db.execute('INSERT INTO execution_plans VALUES(?,?,?,?,?,?,?,?,?,?)',
                (pid,'lane_1',symbol,'ACTIVE_TODAY',1,AT.replace(hour=9,minute=30).isoformat(),AT.replace(hour=15).isoformat(),json.dumps(plan),AT.replace(hour=8).isoformat(),AT.replace(hour=9,minute=30).isoformat()))
            key=f'internal:lane_1:{pid}:{AT.isoformat()}:START_CONFIRMATION:A4_SESSION_WARMUP'
            strategy={'action':'DATA_BLOCK','state':'WARMUP','reason_codes':['NO_CLOSED_5M'],'as_of':CUTOFF.isoformat(),
                'execution_data':{'market_cutoff':CUTOFF.isoformat()},'market_gate':{'state_status':'READY',
                'as_of':AT.isoformat(),'trade_date':AT.date().isoformat(),'decision':'ALLOW'}}
            payload={'plan_id':pid,'symbol':symbol,'minute_snapshot_id':'snap-current','strategy':strategy}
            db.execute('INSERT INTO monitor_events VALUES(?,?,?,?,?,?,?,?,?)',
                (str(uuid.uuid5(uuid.NAMESPACE_URL,'liangjian-monitor:'+key)),key,'lane_1',AT.isoformat(),
                 'START_CONFIRMATION','A4_SESSION_WARMUP',0,json.dumps(payload),AT.isoformat()))
    with sqlite3.connect(minute) as db:
        db.execute('CREATE TABLE minute_decision_snapshots(snapshot_id TEXT,symbol TEXT,interval TEXT,decision_as_of TEXT,captured_at TEXT,payload_sha256 TEXT,payload_zlib BLOB,PRIMARY KEY(snapshot_id,symbol,interval))')
        for i in range(count):
            for interval in ('1m','5m'):
                bar={'symbol':f'60000{i}.SH','interval':interval,'bar_end':CUTOFF.isoformat(),
                     'open':10,'high':11,'low':9,'close':10,'volume':100,'amount':1000,
                     'source_id':'FIXTURE_ONLY','volume_unit':'shares','evidence_kind':'MARKET_BAR'}
                raw=json.dumps([bar]).encode()
                db.execute('INSERT INTO minute_decision_snapshots VALUES(?,?,?,?,?,?,?)',
                    ('snap-current',bar['symbol'],interval,AT.isoformat(),AT.isoformat(),hashlib.sha256(raw).hexdigest(),zlib.compress(raw)))
    return state,minute


def completion_payload(state,*,finished=AT):
    from liangjian_funnel.runtime.decision_observability import DecisionObservation,TimingSpan,project_decision_axes
    with sqlite3.connect(state) as db:
        db.row_factory=sqlite3.Row
        rows=db.execute('SELECT * FROM monitor_events ORDER BY event_id').fetchall()
    events=[]
    for row in rows:
        payload=json.loads(row['payload_json'])
        events.append({'plan_id':payload['plan_id'],'symbol':payload['symbol'],'lane_id':row['lane_id'],
            'minute_end':row['minute_end'],'action':row['action'],'reason_code':row['reason_code']})
    symbols=tuple(sorted(event['symbol'] for event in events))
    observation=DecisionObservation(run_id='fixture-only',decision_id='fixture-decision',lane_id='ALL',
        scheduled_at=AT,started_at=AT,deadline_at=AT+timedelta(seconds=47),snapshot_ids=('snap-current',),
        required_scope=symbols,ready_scope=symbols,blocked_scope=(),no_signal_scope=symbols,
        timing_spans=(TimingSpan('round_total',(finished-AT).total_seconds()*1000,AT,finished),),
        source_attempts=(),terminal_reason='A4_NO_SIGNAL',versions={},
        axes=project_decision_axes(job_status='SUCCEEDED',data_state='READY',opportunity_state='ABSENT',critical_data=True))
    return {'time':AT.isoformat(),'minute_snapshot_id':'snap-current','execution_cutoff':CUTOFF.isoformat(),
        'lanes':[{'lane_id':'lane_1','minute_snapshot_id':'snap-current','events':events}],
        'observability':observation.to_dict()}


def write_completion(path,state,*,finished=AT):
    from liangjian_funnel.reporting import atomic_write_json
    atomic_write_json(path,completion_payload(state,finished=finished))


class Engine:
    def __init__(self): self.calls=[]
    def evaluate_minute(self,items,**kw):
        self.calls.append((items,kw))
        return {'signals':[],'errors':[],'minute_summary':{'minute':kw['minute'].isoformat(),
            'status':'OK','evaluated_plan_count':len(items),'evaluated_variant_count':0,
            'shadow_budget_exceeded_count':0,'elapsed_ms':0}}
    def evaluate_tentative(self,items,**kw):
        from types import SimpleNamespace
        return SimpleNamespace(token='FIXTURE_ONLY',result=self.evaluate_minute(items,**kw))
    def commit_tentative(self,result,**kw): return {'ok':True,'status':'COMMITTED'}
    def discard_tentative(self,result): return {'ok':True,'status':'DISCARDED'}


class Ledger:
    def __init__(self,tmp_path):
        self.db_path=tmp_path/'shadow.db'; self.jsonl_path=tmp_path/'shadow.jsonl'; self.minutes=[]
    def record_minute(self,summary,**kw): self.minutes.append(summary); return {'ok':True}
    def seal_price_limit_evidence(self,*args,**kw): return {'ok':True}
    def advance_outcomes(self,*args,**kw): return {'ok':True}
    def record_signal(self,*args,**kw): return {'ok':True,'stored':True}


def session(tmp_path,count=1,**kw):
    state,minute=databases(tmp_path,count)
    engine,ledger=Engine(),Ledger(tmp_path)
    latest=tmp_path/'monitor-latest.json'; write_completion(latest,state)
    kw.setdefault('wall_clock',lambda:AT+timedelta(seconds=1))
    service=module().ShadowSession(module().ReadOnlyShadowSource(state,minute,lanes=('lane_1',),monitor_latest=latest),
        engine=engine,ledger=ledger,started_at=AT-timedelta(seconds=1),wait_seconds=0,**kw)
    return service,engine,ledger,state,minute


def test_complete_current_scope_passes_original_inner_and_outer_without_baseline_call(tmp_path,monkeypatch):
    from liangjian_funnel.runtime import strategies
    monkeypatch.setattr(strategies,'evaluate_strategy',lambda *a,**kw:(_ for _ in ()).throw(AssertionError('baseline called')))
    service,engine,ledger,state,minute=session(tmp_path)
    before=[hashlib.sha256(p.read_bytes()).hexdigest() for p in (state,minute)]
    result=service.poll(observed_at=AT+timedelta(seconds=1))
    assert result['scope_status']=='COMPLETE' and len(engine.calls)==1
    item=engine.calls[0][0][0]
    assert item['baseline']['action']=='DATA_BLOCK'
    assert item['actual_outer_baseline']['action']=='START_CONFIRMATION'
    assert item['baseline_event_sha256'] and item['source_binding']['minute_payload_sha256']['1m']
    assert engine.calls[0][1]['budget_seconds']<=5
    assert before==[hashlib.sha256(p.read_bytes()).hexdigest() for p in (state,minute)]
    assert result['pit_status']=='UNWIRED' and result['status']=='DATA_LIMITED'
    assert ledger.minutes


@pytest.mark.parametrize('case',['missing_plan_event','hash','future_bar','snapshot_time','wrong_snapshot',
    'identity','event_id','no_inner','expires','valid_from','invalidated','captured_future','cross_day'])
def test_invalid_or_partial_scope_never_calls_engine(case,tmp_path):
    service,engine,ledger,state,minute=session(tmp_path,count=2)
    with sqlite3.connect(state) as db:
        row=db.execute("SELECT payload_json FROM monitor_events ORDER BY event_key LIMIT 1").fetchone()
        payload=json.loads(row[0])
        if case=='missing_plan_event': db.execute("DELETE FROM monitor_events WHERE payload_json=?",(row[0],))
        elif case=='identity': payload['symbol']='000001.SZ'
        elif case=='event_id': db.execute("UPDATE monitor_events SET event_id='fake' WHERE payload_json=?",(row[0],))
        elif case=='no_inner': payload['strategy']=None
        elif case=='expires': db.execute("UPDATE execution_plans SET expires_at=?",((AT-timedelta(seconds=1)).isoformat(),))
        elif case=='valid_from': db.execute("UPDATE execution_plans SET valid_from=?",((AT+timedelta(seconds=1)).isoformat(),))
        elif case=='invalidated': db.execute("UPDATE execution_plans SET status='INVALIDATED'")
        elif case=='wrong_snapshot': payload['minute_snapshot_id']='absent'
        elif case=='cross_day': payload['strategy']['as_of']=(AT-timedelta(days=1)).isoformat()
        db.execute("UPDATE monitor_events SET payload_json=? WHERE payload_json=?",(json.dumps(payload),row[0]))
    with sqlite3.connect(minute) as db:
        if case=='hash': db.execute("UPDATE minute_decision_snapshots SET payload_sha256=?",('0'*64,))
        elif case=='snapshot_time': db.execute("UPDATE minute_decision_snapshots SET decision_as_of=?",((AT-timedelta(minutes=1)).isoformat(),))
        elif case=='captured_future': db.execute("UPDATE minute_decision_snapshots SET captured_at=?",((AT+timedelta(seconds=5)).isoformat(),))
        elif case=='future_bar':
            raw=json.loads(zlib.decompress(db.execute('SELECT payload_zlib FROM minute_decision_snapshots LIMIT 1').fetchone()[0]))
            raw[0]['bar_end']=(AT+timedelta(minutes=1)).isoformat(); encoded=json.dumps(raw).encode()
            db.execute('UPDATE minute_decision_snapshots SET payload_zlib=?,payload_sha256=?',(zlib.compress(encoded),hashlib.sha256(encoded).hexdigest()))
    result=service.poll(observed_at=AT+timedelta(seconds=1))
    assert result['status']=='DATA_LIMITED' and not engine.calls


def test_duplicate_minute_is_not_evaluated_or_written_twice(tmp_path):
    service,engine,ledger,*_=session(tmp_path)
    service.poll(observed_at=AT+timedelta(seconds=1))
    result=service.poll(observed_at=AT+timedelta(seconds=2))
    assert result['status']=='DUPLICATE' and len(engine.calls)==len(ledger.minutes)==1


def test_historical_startup_or_late_minute_cannot_trigger(tmp_path):
    service,engine,ledger,*_=session(tmp_path)
    service.started_at=AT+timedelta(seconds=1)
    result=service.poll(observed_at=AT+timedelta(seconds=2))
    assert result['status']=='DATA_LIMITED' and not engine.calls


def test_writer_failure_isolated_and_explicit(tmp_path):
    service,engine,ledger,*_=session(tmp_path)
    ledger.record_minute=lambda *a,**kw:(_ for _ in ()).throw(RuntimeError('secret fixture'))
    result=service.poll(observed_at=AT+timedelta(seconds=1))
    assert len(engine.calls)==1 and result['status']=='SHADOW_WRITE_FAILED'
    assert 'secret fixture' not in json.dumps(result)


def test_input_output_alias_is_refused(tmp_path):
    state,minute=databases(tmp_path)
    ledger=Ledger(tmp_path); ledger.db_path=state
    with pytest.raises(ValueError):
        module().ShadowSession(module().ReadOnlyShadowSource(state,minute,lanes=('lane_1',)),
            engine=Engine(),ledger=ledger,started_at=AT)


def test_completed_round_with_missing_baseline_is_not_treated_as_still_running(tmp_path):
    service,engine,ledger,state,_=session(tmp_path,count=2)
    with sqlite3.connect(state) as db: db.execute("DELETE FROM monitor_events WHERE payload_json LIKE '%p1%'")
    value=[0.0]; service.clock=lambda:value[0]; service.wait_seconds=2
    # Completion bytes already exist; missing rows are a real source gap,
    # not a fresh five-second waiting period or permission for partial scope.
    assert service.poll(observed_at=AT+timedelta(seconds=1))['status']=='DATA_LIMITED'
    value[0]=2
    assert service.poll(observed_at=AT+timedelta(seconds=3))['status']=='DUPLICATE'
    assert not engine.calls and not ledger.minutes


def test_insufficient_same_minute_evaluation_room_is_not_a_realtime_trigger(tmp_path):
    service,engine,ledger,*_=session(tmp_path)
    assert service.poll(observed_at=AT+timedelta(seconds=55))['status']=='DATA_LIMITED'
    assert not engine.calls and not ledger.minutes


def test_engine_failure_is_only_an_explicit_shadow_receipt(tmp_path):
    service,engine,ledger,*_=session(tmp_path)
    engine.evaluate_minute=lambda *a,**kw:(_ for _ in ()).throw(RuntimeError('private fixture text'))
    result=service.poll(observed_at=AT+timedelta(seconds=1))
    assert result['status']=='SHADOW_ENGINE_FAILED' and result['source_sha256']
    assert 'private fixture text' not in json.dumps(result)
    assert not ledger.minutes


def test_independent_poll_reentry_never_queues_work(tmp_path):
    service,engine,ledger,*_=session(tmp_path)
    service._lock.acquire()
    try: assert service.poll(observed_at=AT)['status']=='SHADOW_SESSION_BUSY'
    finally: service._lock.release()
    assert not engine.calls


def test_day_rollover_uses_new_current_scope_not_old_events(tmp_path):
    service,engine,ledger,*_=session(tmp_path)
    service.poll(observed_at=AT+timedelta(seconds=1))
    service.wall_clock=lambda:AT+timedelta(days=1,seconds=1)
    result=service.poll(observed_at=AT+timedelta(days=1,seconds=1))
    assert result['status']=='WAITING_PRODUCTION_COMPLETION' and len(engine.calls)==1
    assert result['minute'].startswith('2026-10-13')
    assert service.poll(observed_at=AT+timedelta(days=1,seconds=55))['status']=='DATA_LIMITED'


def test_real_engine_and_independent_ledger_do_not_recompute_baseline(tmp_path,monkeypatch):
    from liangjian_funnel.runtime.shadow_variants import ShadowVariantEngine
    from liangjian_funnel.runtime.shadow_evidence import ShadowEvidenceLedger,read_shadow_evidence
    from liangjian_funnel.runtime import strategies
    monkeypatch.setattr(strategies,'evaluate_strategy',lambda *a,**kw:(_ for _ in ()).throw(AssertionError('baseline')))
    state,minute=databases(tmp_path)
    ledger=ShadowEvidenceLedger(tmp_path/'shadow.db',tmp_path/'shadow.jsonl')
    latest=tmp_path/'latest.json'; write_completion(latest,state)
    service=module().ShadowSession(module().ReadOnlyShadowSource(state,minute,lanes=('lane_1',),monitor_latest=latest),
        engine=ShadowVariantEngine(),ledger=ledger,started_at=AT-timedelta(seconds=1),wait_seconds=0,
        wall_clock=lambda:AT+timedelta(seconds=1))
    result=service.poll(observed_at=AT+timedelta(seconds=1))
    assert result['engine_status']=='OK' and all(r['ok'] for r in result['write_receipts'])
    stored=read_shadow_evidence(ledger.db_path,ledger.jsonl_path)
    assert stored['ok'] and stored['hash_chain_status']=='MATCHED'
    assert len(stored['signals'])==6
    assert all(s['signal']['status']=='DATA_LIMITED' and s['outcome'] is None for s in stored['signals'])


def test_cli_help_and_missing_source_do_not_discover_or_create_production(tmp_path):
    import subprocess,sys
    script=Path(__file__).resolve().parents[1]/'scripts/run_shadow_session.py'
    help_result=subprocess.run([sys.executable,str(script),'--help'],capture_output=True,text=True)
    assert help_result.returncode==0 and '--state-db' in help_result.stdout
    result=subprocess.run([sys.executable,str(script),'--state-db',str(tmp_path/'missing.db'),
        '--minute-db',str(tmp_path/'missing-minute.db'),'--lane','lane_1',
        '--shadow-db',str(tmp_path/'shadow.db'),'--shadow-jsonl',str(tmp_path/'shadow.jsonl'),
        '--receipt-jsonl',str(tmp_path/'receipt.jsonl')],capture_output=True,text=True)
    assert result.returncode==2 and json.loads(result.stdout)['status']=='DATA_LIMITED'
    assert not list(tmp_path.iterdir())


def test_scope_census_retains_missing_ids_and_actual_outer_hashes(tmp_path):
    service,engine,ledger,state,_=session(tmp_path,count=2)
    with sqlite3.connect(state) as db: db.execute("DELETE FROM monitor_events WHERE payload_json LIKE '%p1%'")
    result=service.poll(observed_at=AT+timedelta(seconds=1))
    census=result['plan_window_census']
    assert census['expected_plan_ids']==['p0','p1'] and census['missing_plan_ids']==['p1']
    assert census['outer_baseline_records'][0]['action']=='START_CONFIRMATION'
    assert len(census['outer_baseline_records'][0]['event_sha256'])==64
    assert not engine.calls


def test_outer_event_action_cannot_conflict_with_real_event_key(tmp_path):
    service,engine,ledger,state,_=session(tmp_path)
    with sqlite3.connect(state) as db: db.execute("UPDATE monitor_events SET action='BUY_SIGNAL'")
    result=service.poll(observed_at=AT+timedelta(seconds=1))
    assert result['status']=='DATA_LIMITED' and not engine.calls


def test_injected_pit_provider_unknown_is_not_claimed_ready(tmp_path):
    service,engine,ledger,*_=session(tmp_path)
    service.identity_provider=lambda plan,now:{'status':'DATA_LIMITED','evidence':{},'observed_at':now}
    service.bar_arrival_provider=lambda now:[]
    result=service.poll(observed_at=AT+timedelta(seconds=1))
    assert result['status']=='DATA_LIMITED' and result['pit_status']=='DATA_LIMITED'


def test_cli_small_local_fixture_receipts_only_no_source_mutation(tmp_path):
    import subprocess,sys
    state,minute=databases(tmp_path)
    before=[hashlib.sha256(p.read_bytes()).hexdigest() for p in (state,minute)]
    script=Path(__file__).resolve().parents[1]/'scripts/run_shadow_session.py'
    result=subprocess.run([sys.executable,str(script),'--state-db',str(state),'--minute-db',str(minute),
        '--lane','lane_1','--shadow-db',str(tmp_path/'shadow.db'),'--shadow-jsonl',str(tmp_path/'shadow.jsonl'),
        '--receipt-jsonl',str(tmp_path/'receipt.jsonl'),'--duration-seconds','.1','--wait-seconds','0'],
        capture_output=True,text=True)
    assert result.returncode==0
    receipts=[json.loads(line) for line in (tmp_path/'receipt.jsonl').read_text(encoding='utf-8').splitlines()]
    assert receipts and receipts[0]['status']=='DATA_LIMITED'
    assert all(row['status'] in ('DATA_LIMITED','DUPLICATE') for row in receipts)
    assert before==[hashlib.sha256(p.read_bytes()).hexdigest() for p in (state,minute)]
