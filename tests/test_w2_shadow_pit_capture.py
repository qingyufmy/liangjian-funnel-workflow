"""Synthetic local source bytes: no real provider, Settings or production DB."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from copy import deepcopy
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.runtime.shadow_evidence import ShadowEvidenceLedger
from liangjian_funnel.runtime.shadow_pit_capture import PITSourceReceipt, capture_shadow_pit, build_plan_pit_capture

SH = ZoneInfo('Asia/Shanghai')


def at(clock='09:26', day='2026-10-12'):
    return datetime.fromisoformat(f'{day}T{clock}:00').replace(tzinfo=SH)


def plan(pid='p1', status='PENDING_MORNING_REVIEW', **kw):
    return dict(plan_id=pid, symbol='600001.SH', lane_id='lane_1', status=status,
                valid_from=None if status=='PENDING_MORNING_REVIEW' else at('09:32').isoformat(),
                expires_at=at('15:00').isoformat(), plan_version='1.5.0',
                payload_json=json.dumps({'symbol':'600001.SH','target_trade_date':'2026-10-12'})) | kw


def fields(**kw):
    return dict(symbol='600001.SH', trade_date='2026-10-12', observed_at=at('09:25').isoformat(),
                security_name='测试普通股', security_name_basis='SOURCE_QUOTE_NAME',
                preclose=10, preclose_basis='EXCHANGE_DISPLAYED_PRECLOSE',
                upper_limit=11, lower_limit=9, listing_date='2020-01-01',
                listing_date_basis='SOURCE_LISTING_DATE', listing_observed_at=at('09:25').isoformat(),
                board='SSE_MAIN', security_type='CASH_A_SHARE', security_status='ORDINARY',
                is_st=False, limit_regime='NORMAL', rule_effective_from='2026-07-06') | kw


def receipt(**kw):
    body=json.dumps(fields(**kw),ensure_ascii=False,separators=(',',':')).encode()
    return PITSourceReceipt('fixture:source-response',at(),True,raw_response=body,
                            byte_kind='ORIGINAL_HTTP_RESPONSE_BYTES')


@pytest.fixture
def ledger(tmp_path):
    return ShadowEvidenceLedger(tmp_path/'shadow.sqlite3',tmp_path/'shadow.jsonl')


def test_explicit_current_limits_priority_and_raw_hash_binding(ledger):
    src=receipt()
    result=capture_shadow_pit([plan()],observed_at=at(),source_receipts={'600001.SH':src},ledger=ledger)
    row=result['entries'][0]
    assert row['limits']['status']=='KNOWN'
    assert row['limits']['basis']=='EXPLICIT_FROZEN_SAME_DAY'
    assert row['evidence']['source_input_sha256']==hashlib.sha256(src.raw_response).hexdigest()
    assert row['source_lineage']['raw_http_bytes_available'] is True
    assert row['sealed_receipt']['ok']
    assert result['execution_publication']=='UNCHANGED' and result['customer_notification_calls']==0


def test_rule_review_date_not_extended_for_derivation(ledger):
    result=capture_shadow_pit([plan()],observed_at=at(),source_receipts={'600001.SH':receipt(upper_limit=None,lower_limit=None)},ledger=ledger)
    row=result['entries'][0]
    assert row['limits']['status']=='UNKNOWN'
    assert row['limits']['evidence_reason']=='RULE_DATE_UNPROVEN'
    assert row['status']=='DATA_LIMITED'


@pytest.mark.parametrize('kw,code',[
    ({'symbol':'600002.SH'},'QUOTE_SYMBOL_MISMATCH'),
    ({'trade_date':'2026-10-09'},'QUOTE_DATE_MISMATCH'),
    ({'observed_at':at('09:25','2026-10-09').isoformat()},'QUOTE_DATE_MISMATCH'),
    ({'observed_at':at('09:27').isoformat()},'QUOTE_FUTURE'),
    ({'observed_at':at('09:22').isoformat()},'QUOTE_AUCTION_EXPIRED')])
def test_wrong_or_unavailable_quote_not_sealed_as_current(ledger,kw,code):
    result=capture_shadow_pit([plan()],observed_at=at(),source_receipts={'600001.SH':receipt(**kw)},ledger=ledger)
    row=result['entries'][0]
    assert code in row['reason_codes']
    assert row['status']=='DATA_LIMITED' and row['limits']['upper'] is None
    assert ledger.snapshot()['price_limit_evidence']==[]


@pytest.mark.parametrize('time',['09:25','09:30','10:00'])
def test_capture_window_expired_never_fetches(time):
    calls=[]
    r=capture_shadow_pit([plan()],observed_at=at(time),source_fetcher=lambda *args:calls.append(args))
    assert r['status']=='DATA_LIMITED' and 'CAPTURE_WINDOW_MISSED' in r['reason_codes']
    assert calls==[]


def test_pending_and_active_are_not_activated_or_mixed(ledger):
    plans=[plan(),plan('p2','ACTIVE_TODAY')]
    original=deepcopy(plans)
    r=capture_shadow_pit(plans,observed_at=at(),source_receipts={'600001.SH':receipt()},ledger=ledger)
    assert [v['plan_status'] for v in r['entries']]==['PENDING_MORNING_REVIEW','ACTIVE_TODAY']
    assert r['pending_plan_count']==1 and r['active_plan_count']==1
    assert r['entries'][1]['entry_effective_now'] is False
    assert plans==original


def test_other_target_and_expired_plan_not_fetched():
    calls=[]
    future=plan(payload_json=json.dumps({'symbol':'600001.SH','target_trade_date':'2026-10-13'}))
    r=capture_shadow_pit([future,plan('p2',expires_at=at('09:25').isoformat())],observed_at=at(),source_fetcher=lambda *args:calls.append(args))
    assert r['selected_plan_count']==0 and len(r['excluded_plans'])==2
    assert calls==[]


def test_target_missing_not_inferred_from_expiry():
    p=plan(payload_json=json.dumps({'symbol':'600001.SH'}))
    r=capture_shadow_pit([p],observed_at=at(),source_receipts={'600001.SH':receipt()})
    assert r['selected_plan_count']==0
    assert r['excluded_plans'][0]['reason_code']=='TARGET_DAY_UNPROVEN'


def test_old_listing_record_and_no_normal_flags_not_backfilled():
    row=build_plan_pit_capture(plan(),receipt(listing_observed_at=at('09:25','2026-10-09').isoformat(),
                                            security_status=None,is_st=None,limit_regime=None),observed_at=at())
    assert row['evidence']['listing_date'] is None
    assert row['field_status']['listing_date']=='UNKNOWN'
    assert row['evidence']['is_st'] is None
    assert row['evidence']['security_status'] is None


def test_normalized_market_quote_not_claimed_http_raw():
    q={'symbol':'600001.SH','name':'测试','previous_close':10,'quote_time':at('09:25').isoformat(),
       'price':10.1,'source_id':'TENCENT:ifzq.gtimg.cn'}
    src=PITSourceReceipt('frozen:market-quote',at(),True,format='NORMALIZED_QUOTE',normalized=q,byte_kind='NORMALIZED_OBJECT')
    row=build_plan_pit_capture(plan(),src,observed_at=at())
    assert row['source_lineage']['raw_http_bytes_available'] is False
    assert row['source_lineage']['raw_response_sha256'] is None
    assert row['source_lineage']['normalized_object_sha256']
    assert row['evidence']['source_input_sha256'] is None
    assert row['status']=='DATA_LIMITED'
    assert row['evidence']['preclose_basis']!='EXCHANGE_DISPLAYED_PRECLOSE'


def test_partial_source_does_not_use_valid_looking_fields(ledger):
    good=receipt()
    src=PITSourceReceipt(good.source_ref,good.captured_at,False,raw_response=good.raw_response)
    row=capture_shadow_pit([plan()],observed_at=at(),source_receipts={'600001.SH':src},ledger=ledger)['entries'][0]
    assert 'SOURCE_PARTIAL' in row['reason_codes']
    assert row['limits']['upper'] is None
    assert ledger.snapshot()['price_limit_evidence']==[]


def test_same_stock_fetched_once_and_exception_is_bounded():
    calls=[]
    def fetch(symbol,observed_at):
        calls.append(symbol)
        raise RuntimeError('secret fixture forbidden in receipt')
    r=capture_shadow_pit([plan(),plan('p2')],observed_at=at(),source_fetcher=fetch)
    assert calls==['600001.SH']
    assert all('SOURCE_FETCH_FAILED' in row['reason_codes'] for row in r['entries'])
    assert 'secret' not in json.dumps(r)


def test_default_unwired_no_provider_or_notification(ledger):
    result=capture_shadow_pit([plan()],observed_at=at(),ledger=ledger)
    assert result['status']=='DATA_LIMITED'
    assert result['source_fetch_count']==0 and result['customer_notification_calls']==0
    assert result['entries'][0]['reason_codes']==['SOURCE_UNWIRED']


def test_duplicate_sealing_and_conflict_preserve_original(ledger):
    args=dict(observed_at=at(),ledger=ledger)
    one=capture_shadow_pit([plan()],source_receipts={'600001.SH':receipt()},**args)
    two=capture_shadow_pit([plan()],source_receipts={'600001.SH':receipt()},**args)
    assert two['entries'][0]['sealed_receipt']['duplicate']
    before=ledger.snapshot()['price_limit_evidence']
    conflict=capture_shadow_pit([plan()],source_receipts={'600001.SH':receipt(preclose=9)},**args)
    assert conflict['status']=='DATA_LIMITED'
    assert conflict['entries'][0]['sealed_receipt']['error_code']=='FROZEN_IDENTITY_CONFLICT'
    assert ledger.snapshot()['price_limit_evidence']==before
    assert one['entries'][0]['evidence']['preclose']==10


def test_raw_json_field_mapping_bound_to_actual_bytes():
    data={'data':fields()}
    src=PITSourceReceipt('fixture:original',at(),True,raw_response=json.dumps(data).encode(),
                        field_map={k:'/data/'+k for k in fields()})
    row=build_plan_pit_capture(plan(),src,observed_at=at())
    assert row['evidence']['upper_limit']==11
    assert row['source_lineage']['field_paths']['upper_limit']=='/data/upper_limit'


def test_duplicate_json_keys_are_not_accepted():
    src=PITSourceReceipt('fixture',at(),True,raw_response=b'{"symbol":"600001.SH","symbol":"600002.SH"}')
    row=build_plan_pit_capture(plan(),src,observed_at=at())
    assert 'SOURCE_JSON_DUPLICATE_KEY' in row['reason_codes']
    assert row['limits']['upper'] is None


def test_name_st_never_inferred_ordinary():
    row=build_plan_pit_capture(plan(),receipt(security_name='ST测试',is_st=False),observed_at=at())
    assert row['evidence']['is_st'] is True
    row=build_plan_pit_capture(plan(),receipt(security_name='普通测试',is_st=None),observed_at=at())
    assert row['evidence']['is_st'] is None


def test_listing_missing_and_preclose_missing_are_unknown_not_zero():
    row=build_plan_pit_capture(plan(),receipt(preclose=None,listing_date=None),observed_at=at())
    assert row['evidence']['preclose'] is None and row['evidence']['listing_date'] is None
    assert row['field_status']['preclose']=='UNKNOWN' and row['field_status']['listing_date']=='UNKNOWN'
    assert row['status']=='DATA_LIMITED'


def test_conflicting_explicit_and_derived_is_blocked_on_reviewed_date():
    p=plan(payload_json=json.dumps({'symbol':'600001.SH','target_trade_date':'2026-10-08'}),expires_at=at('15:00','2026-10-08').isoformat())
    e=fields(trade_date='2026-10-08',observed_at=at('09:25','2026-10-08').isoformat(),
             listing_observed_at=at('09:25','2026-10-08').isoformat(),upper_limit=12)
    src=PITSourceReceipt('fixture',at(day='2026-10-08'),True,raw_response=json.dumps(e).encode())
    row=build_plan_pit_capture(p,src,observed_at=at(day='2026-10-08'))
    assert row['limits']['status']=='CONFLICT'


def test_single_provider_cannot_bypass_auction_window():
    row=build_plan_pit_capture(plan(),receipt(observed_at=at('09:29').isoformat()),observed_at=at('09:30'))
    assert row['evidence'] is None
    assert row['reason_codes']==['CAPTURE_WINDOW_MISSED']


def test_listing_timestamp_is_not_a_listing_date():
    row=build_plan_pit_capture(plan(),receipt(listing_date='2020-01-01T12:00:00'),observed_at=at())
    assert row['evidence']['listing_date'] is None


def test_capture_arrival_clock_and_failed_writer_are_independent():
    class BrokenLedger:
        def seal_price_limit_evidence(self,*a,**kw):
            raise OSError('private failure')
    r=capture_shadow_pit([plan()],observed_at=at(),source_fetcher=lambda *a,**kw:receipt(),
                         clock=lambda:at('09:27'),ledger=BrokenLedger())
    assert r['entries'][0]['observed_at']==at('09:27').isoformat()
    assert r['entries'][0]['sealed_receipt']['error_code']=='SHADOW_EVIDENCE_WRITE_FAILED'
    assert r['execution_publication']=='UNCHANGED'
    r=capture_shadow_pit([plan()],observed_at=at(),source_fetcher=lambda *a,**kw:receipt(),
                         clock=lambda:at('09:30'))
    assert r['entries']==[] and r['reason_codes']==['CAPTURE_WINDOW_MISSED']


def test_duplicate_plan_ids_rejected_before_any_fetch():
    calls=[]
    r=capture_shadow_pit([plan(),plan()],observed_at=at(),source_fetcher=lambda *a,**kw:calls.append(a))
    assert r['reason_codes']==['DUPLICATE_PLAN_ID'] and calls==[]


def local_input(tmp_path, *, quote=True):
    p=tmp_path/'plans.json'; p.write_text(json.dumps([plan()]),encoding='utf-8')
    manifest={'schema_version':'shadow-pit-local-input/1',
              'plans':{'path':p.name,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()},'quotes':{}}
    if quote:
        q=tmp_path/'quote.json'; q.write_bytes(receipt().raw_response)
        manifest['quotes']['600001.SH']={'path':q.name,'sha256':hashlib.sha256(q.read_bytes()).hexdigest(),
           'source_ref':'fixture:bytes','captured_at':at().isoformat(),'complete':True,
           'format':'EXPLICIT_JSON_FIELDS','byte_kind':'CONSUMER_INPUT_BYTES'}
    f=tmp_path/'input.json'; f.write_text(json.dumps(manifest),encoding='utf-8')
    return f,manifest


def run_capture_cli(f, output):
    command=[sys.executable,'-B',str(Path(__file__).resolve().parents[1]/'scripts/capture_shadow_pit.py'),
        '--input',str(f),'--input-sha256',hashlib.sha256(f.read_bytes()).hexdigest(),
        '--observed-at',at().isoformat(),'--output-dir',str(output)]
    return subprocess.run(command,capture_output=True,text=True,encoding='utf-8',errors='replace')


def test_cli_pinned_bytes_and_independent_output(tmp_path):
    f,_=local_input(tmp_path); before={p.name:p.read_bytes() for p in tmp_path.iterdir()}
    out=tmp_path/'new-capture'; r=run_capture_cli(f,out)
    assert r.returncode==0, r.stdout+r.stderr
    report=json.loads((out/'capture-report.json').read_text(encoding='utf-8'))
    assert report['capture_mode']=='LOCAL_PINNED_PIT_INPUT'
    assert report['entries'][0]['source_lineage']['raw_http_bytes_available'] is False
    assert report['entries'][0]['sealed_receipt']['ok']
    binding=json.loads((out/'manifest.json').read_text(encoding='utf-8'))
    assert binding['input_manifest_sha256']==hashlib.sha256(f.read_bytes()).hexdigest()
    assert binding['native_exit_code']==0
    for name,body in before.items(): assert (tmp_path/name).read_bytes()==body
    assert run_capture_cli(f,out).returncode==2


def test_cli_unwired_is_limited_not_zero_or_success(tmp_path):
    f,_=local_input(tmp_path,quote=False); out=tmp_path/'unwired'
    r=run_capture_cli(f,out); assert r.returncode==2
    report=json.loads((out/'capture-report.json').read_text(encoding='utf-8'))
    assert report['entries'][0]['reason_codes']==['SOURCE_UNWIRED']
    assert report['entries'][0]['limits']['upper'] is None


@pytest.mark.parametrize('kind',['wrong_hash','escape','absolute'])
def test_cli_rejects_unbound_or_escaped_files_before_output(tmp_path,kind):
    f,m=local_input(tmp_path)
    if kind=='wrong_hash': m['plans']['sha256']='0'*64
    elif kind=='escape': m['plans']['path']='../plans.json'
    else: m['plans']['path']=str((tmp_path/'plans.json').resolve())
    f.write_text(json.dumps(m),encoding='utf-8'); out=tmp_path/'not-created'
    r=run_capture_cli(f,out)
    assert r.returncode==2 and not out.exists()
