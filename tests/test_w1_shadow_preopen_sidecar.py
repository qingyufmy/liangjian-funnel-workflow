from copy import deepcopy
from datetime import datetime, timedelta
import hashlib
import importlib
import json
import sqlite3

import pytest

from test_w2_shadow_inputs_publication import fixture, TARGET, AT, TZ, SYMBOL


GENERATED = datetime(2026,10,12,8,0,tzinfo=TZ)


def module():
    return importlib.import_module('liangjian_funnel.runtime.shadow_preopen_sidecar')


def row():
    _, _, plan, _ = fixture()
    return {'plan_id':'p','lane_id':'lane_1','symbol':SYMBOL,'plan_version':1,
        'status':'ACTIVE_TODAY','created_at':AT.isoformat(),'updated_at':AT.isoformat(),
        'valid_from':GENERATED.replace(hour=9,minute=30).isoformat(),
        'expires_at':GENERATED.replace(hour=15).isoformat(),'payload_json':json.dumps(plan)}


def generate(plan_row=None, rows=None, **kw):
    original, _, _, dates = fixture()
    arguments=dict(plan_row=row() if plan_row is None else plan_row,
        rows=original if rows is None else rows,target_trade_date=TARGET,
        generated_at=GENERATED,source_as_of=GENERATED,
        bar_cutoff=datetime(2026,10,9,15,tzinfo=TZ),expected_close_dates=dates)
    arguments.update(kw)
    return module().generate_preopen_sidecar(**arguments)


def test_original_builder_morning_inputs_preserve_plan_and_bind_rows():
    plan_row=row(); original=deepcopy(plan_row)
    result=generate(plan_row)
    assert result['status']=='AVAILABLE' and result['inputs_origin']=='SHADOW_PREOPEN_SIDECAR'
    assert result['shadow_inputs']['version']=='raw-daily-ma5-wilder-tr14/1'
    assert result['shadow_inputs']['daily_as_of']==GENERATED.isoformat()
    assert result['shadow_inputs']['source_ref']['source_rows_count']==20
    assert plan_row==original
    assert result['plan_binding']['payload_json_sha256']==hashlib.sha256(plan_row['payload_json'].encode()).hexdigest()


@pytest.mark.parametrize('case',['future_bar','ma5','late0900','late0930','source_future','wrong_target'])
def test_bad_source_clock_or_geometry_is_limited(case):
    rows,_,_,_=fixture(); plan_row=row(); kw={}
    if case=='future_bar':
        from liangjian_funnel.pipeline.local_fact_cache import canonical_json_hash
        rows[-1]['payload']['date_ms']=int(GENERATED.replace(hour=15).timestamp()*1000)
        rows[-1]['timestamp']=GENERATED.replace(hour=15).isoformat()
        rows[-1]['content_hash']=canonical_json_hash(rows[-1]['payload'])
    elif case=='ma5':
        plan=json.loads(plan_row['payload_json']); plan['strategy_facts']['daily_ma']['ma5']=99
        plan_row['payload_json']=json.dumps(plan)
    elif case=='late0900': kw['generated_at']=GENERATED.replace(hour=9)
    elif case=='late0930': kw['generated_at']=GENERATED.replace(hour=9,minute=30)
    elif case=='source_future': kw['source_as_of']=GENERATED+timedelta(seconds=1)
    else: kw['target_trade_date']=TARGET+timedelta(days=1)
    result=generate(plan_row,rows,**kw)
    assert result['status']=='DATA_LIMITED' and result['shadow_inputs'] is None


@pytest.mark.parametrize('case',['plan_id','payload','target','record_hash','generated_at'])
def test_consumer_wrong_binding_or_late_generation_cannot_use_sidecar(tmp_path,case):
    ledger=module().PreopenInputsLedger(tmp_path/'sidecar.db')
    record=generate(); assert ledger.record(record)['ok']
    plan_row=row(); at=GENERATED.replace(hour=10); target=TARGET
    if case=='plan_id': plan_row['plan_id']='different'
    elif case=='payload': plan_row['payload_json']+=' '
    elif case=='target': target+=timedelta(days=1)
    else:
        with sqlite3.connect(ledger.path) as db:
            stored=json.loads(db.execute('SELECT record_json FROM shadow_preopen_inputs').fetchone()[0])
            stored[case]='0'*64 if case=='record_hash' else GENERATED.replace(hour=9,minute=30).isoformat()
            db.execute('UPDATE shadow_preopen_inputs SET record_json=?',(json.dumps(stored),))
    result=ledger.lookup(plan_row,target_trade_date=target,observed_at=at)
    assert result['status']=='DATA_LIMITED' and result['shadow_inputs'] is None


def test_sidecar_record_deduplication_and_original_sqlite_row_bytes_unchanged(tmp_path):
    state=tmp_path/'source.db'; plan_row=row()
    with sqlite3.connect(state) as db:
        db.execute('CREATE TABLE execution_plans(plan_id TEXT,status TEXT,payload_json TEXT)')
        db.execute('INSERT INTO execution_plans VALUES(?,?,?)',(plan_row['plan_id'],plan_row['status'],plan_row['payload_json']))
    before=hashlib.sha256(state.read_bytes()).hexdigest()
    ledger=module().PreopenInputsLedger(tmp_path/'sidecar.db')
    record=generate(plan_row)
    assert ledger.record(record)['stored']
    assert ledger.record(record)['duplicate']
    found=ledger.lookup(plan_row,target_trade_date=TARGET,observed_at=GENERATED.replace(hour=10))
    assert found['status']=='AVAILABLE' and found['inputs_origin']=='SHADOW_PREOPEN_SIDECAR'
    assert hashlib.sha256(state.read_bytes()).hexdigest()==before
    with sqlite3.connect(state) as db:
        assert db.execute('SELECT * FROM execution_plans').fetchone()==('p','ACTIVE_TODAY',plan_row['payload_json'])


def test_same_day_morning_source_valid_but_intraday_source_is_invalid():
    from liangjian_funnel.runtime.shadow_variants import prepare_shadow_plan
    record=generate(); plan=json.loads(row()['payload_json'])
    plan['strategy_facts']['shadow_inputs']=record['shadow_inputs']
    at=GENERATED.replace(hour=10)
    assert prepare_shadow_plan(plan,decision_time=at)[2] is None
    plan['strategy_facts']['shadow_inputs']['daily_as_of']=at.isoformat()
    assert prepare_shadow_plan(plan,decision_time=at)[2]=='DAILY_EVIDENCE_NOT_PREOPEN'


def test_consumer_utc_clock_and_original_plan_freeze_never_loosen_0930_boundary():
    from datetime import timezone
    from liangjian_funnel.runtime.shadow_variants import prepare_shadow_plan
    plan=json.loads(row()['payload_json']); plan['strategy_facts']['shadow_inputs']=generate()['shadow_inputs']
    at=GENERATED.replace(hour=10)
    assert prepare_shadow_plan(plan,decision_time=at.astimezone(timezone.utc),generated_at=GENERATED)[2] is None
    assert prepare_shadow_plan(plan,decision_time=at,generated_at=at)[2]=='SHADOW_INPUT_GENERATION_NOT_PREOPEN'
    plan['strategy_facts']['shadow_inputs']['daily_as_of']=at.astimezone(timezone.utc).isoformat()
    assert prepare_shadow_plan(plan,decision_time=at.astimezone(timezone.utc))[2]=='DAILY_EVIDENCE_NOT_PREOPEN'


def test_readonly_real_local_fact_cache_reuses_selection_without_init_or_write(tmp_path,monkeypatch):
    from liangjian_funnel.pipeline.local_fact_cache import LocalFactCache
    rows,_,_,_=fixture()
    source=tmp_path/'facts.db'; LocalFactCache(source).upsert_daily_bars(rows)
    before=hashlib.sha256(source.read_bytes()).hexdigest()
    monkeypatch.setattr(LocalFactCache,'_initialize',lambda *args:(_ for _ in ()).throw(AssertionError('writable init')))
    cache=module().ReadOnlyLocalFactCache(source)
    result=module().generate_from_local_cache(row(),cache=cache,target_trade_date=TARGET,generated_at=GENERATED)
    assert result['status']=='AVAILABLE'
    assert hashlib.sha256(source.read_bytes()).hexdigest()==before


def test_read_crossing_0900_cannot_backdate_generation(tmp_path):
    from liangjian_funnel.pipeline.local_fact_cache import LocalFactCache
    rows,_,_,_=fixture(); source=tmp_path/'facts.db'; LocalFactCache(source).upsert_daily_bars(rows)
    result=module().generate_from_local_cache(row(),cache=module().ReadOnlyLocalFactCache(source),
        target_trade_date=TARGET,generated_at=GENERATED.replace(hour=8,minute=59,second=59),
        clock=lambda:GENERATED.replace(hour=9))
    assert result['status']=='DATA_LIMITED' and result['shadow_inputs'] is None


def test_session_sidecar_only_engine_copy_not_original_plan_or_baseline(tmp_path):
    from test_shadow_session import session, AT as DISPATCH
    service,engine,ledger,state,_=session(tmp_path)
    plan_row=row()
    with sqlite3.connect(state) as db:
        db.row_factory=sqlite3.Row
        stored=dict(db.execute('SELECT * FROM execution_plans').fetchone())
        plan=json.loads(plan_row['payload_json']); plan['plan_id']=stored['plan_id']
        db.execute('UPDATE execution_plans SET payload_json=?',(json.dumps(plan),))
        stored=dict(db.execute('SELECT * FROM execution_plans').fetchone())
    before=hashlib.sha256(state.read_bytes()).hexdigest()
    sidecar=module().PreopenInputsLedger(tmp_path/'sidecar.db')
    assert sidecar.record(generate(stored))['ok']
    service.source.preopen_inputs_provider=sidecar.lookup
    recorded_plans=[]
    original=engine.evaluate_minute
    def buying(items,**kw):
        result=original(items,**kw)
        result['signals']=[{'plan_id':'p0','variant_id':'V1','status':'OK','variant_action':'BUY_SIGNAL'}]
        return result
    engine.evaluate_minute=buying
    signals=[]
    ledger.record_signal=lambda signal,plan,**kw:(signals.append(signal) or recorded_plans.append(plan) or {'ok':True,'stored':True})
    result=service.poll(observed_at=DISPATCH+timedelta(seconds=1))
    assert result['scope_status']=='COMPLETE' and engine.calls[0][0][0]['inputs_origin']=='SHADOW_PREOPEN_SIDECAR'
    assert engine.calls[0][0][0]['plan']['strategy_facts']['shadow_inputs']['status']=='AVAILABLE'
    assert 'shadow_inputs' not in recorded_plans[0]['strategy_facts']
    assert signals[0]['technical_trigger'] is True and signals[0]['technical_action']=='BUY_SIGNAL'
    assert signals[0]['status']=='DATA_LIMITED' and signals[0]['variant_action'] is None
    assert signals[0]['inputs_origin']=='SHADOW_PREOPEN_SIDECAR'
    assert ledger.minutes[0]['baseline_records'][0]['action']=='START_CONFIRMATION'
    assert hashlib.sha256(state.read_bytes()).hexdigest()==before


def test_cli_only_independent_sidecar_writes_original_sqlite_rows_unchanged(tmp_path):
    from pathlib import Path
    from liangjian_funnel.pipeline.local_fact_cache import LocalFactCache
    rows,_,_,_=fixture(); facts=tmp_path/'facts.db'; LocalFactCache(facts).upsert_daily_bars(rows)
    state=tmp_path/'state.db'; plan_row=row()
    with sqlite3.connect(state) as db:
        columns=','.join(k+' TEXT' for k in plan_row)
        db.execute('CREATE TABLE execution_plans('+columns+')')
        db.execute('INSERT INTO execution_plans VALUES('+','.join('?' for _ in plan_row)+')',tuple(plan_row.values()))
    before=hashlib.sha256(state.read_bytes()).hexdigest()
    spec=importlib.util.spec_from_file_location('_preopen_cli',Path(__file__).resolve().parents[1]/'scripts/run_shadow_preopen_sidecar.py')
    cli=importlib.util.module_from_spec(spec); spec.loader.exec_module(cli)
    args=['--state-db',str(state),'--fact-db',str(facts),'--preopen-inputs-db',str(tmp_path/'sidecar.db'),
        '--receipt-json',str(tmp_path/'receipt.json'),'--target-trade-date',TARGET.isoformat(),'--lane','lane_1']
    assert cli.main(args,clock=lambda:GENERATED)==0
    receipt=json.loads((tmp_path/'receipt.json').read_bytes())
    assert receipt['available_count']==1 and receipt['selected_plan_count']==1
    assert hashlib.sha256(state.read_bytes()).hexdigest()==before
