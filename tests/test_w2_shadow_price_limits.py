"""Offline synthetic arrival clocks; approved bytes are never relabelled live."""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.runtime import shadow_price_limits as mod
from liangjian_funnel.runtime import shadow_rule_preflight as rule
from liangjian_funnel.runtime.shadow_pit_capture import FIELDS, PITSourceReceipt, build_plan_pit_capture
from liangjian_funnel.runtime.shadow_pit_sources import record_raw_response
from liangjian_funnel.evaluation.ablation import price_limits as original

SH=ZoneInfo('Asia/Shanghai')
ROOT=Path(__file__).parents[1]
NOW=datetime(2026,10,12,9,26,tzinfo=SH)
PRE=NOW.replace(hour=8,minute=31)


@pytest.fixture(scope='module')
def approved():
    return rule.load_approved_rule_archive(ROOT/'artifacts/wp5-20261010/official-rules-20261010')


@pytest.fixture(scope='module')
def confirmed(approved):
    rows=[record_raw_response(d.raw_response,source_ref='official-rule:'+d.document_id,endpoint=d.url,
        request_parameters={},request_started_at=PRE-timedelta(seconds=1),response_received_at=PRE,
        byte_kind='HTTP_RESPONSE_CONTENT_BYTES') for d in approved.documents]
    result=rule.confirm_rule_version(approved,rows,trade_date='2026-10-12',known_at=PRE)
    assert result['status']=='RULE_VERSION_CONFIRMED'
    return result


def inputs(**changed):
    fields=dict(symbol='600001.SH',trade_date='2026-10-12',observed_at=NOW.replace(minute=25).isoformat(),
        security_name='测试普通股',security_name_basis='SOURCE_QUOTE_NAME',preclose=10,
        preclose_basis='EXCHANGE_DISPLAYED_PRECLOSE',upper_limit=None,lower_limit=None,
        listing_date='2000-01-01',listing_date_basis='SOURCE_LISTING_DATE',listing_observed_at=NOW.replace(minute=25).isoformat(),
        board='SSE_MAIN',security_type='CASH_A_SHARE',security_status='ORDINARY',is_st=False,
        limit_regime='NORMAL',rule_effective_from='2026-07-06',prior_raw_close=None)
    fields.update(changed)
    raw=json.dumps(fields,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode()
    source=PITSourceReceipt('fixture:identity',NOW,True,raw_response=raw,byte_kind='CONSUMER_INPUT_BYTES')
    evidence=fields|{'source_ref':source.source_ref,'source_input_sha256':hashlib.sha256(raw).hexdigest()}
    return evidence,source


def derive(approved,confirmed,**changed):
    evidence,source=inputs(**changed)
    return mod.derive_shadow_price_limits(evidence,symbol='600001.SH',at=NOW,
        rule_receipt=confirmed,approved=approved,source=source)


def seal(receipt):
    receipt['receipt_sha256']=hashlib.sha256(rule.canonical_receipt_bytes(receipt)).hexdigest()
    return receipt


def test_new_day_wrapper_derives_but_original_api_remains_closed(approved,confirmed):
    evidence,source=inputs()
    old=original.resolve_price_limits({'symbol':'600001.SH','price_limit_evidence':evidence},symbol='600001.SH',at=NOW)
    assert old['status']=='UNKNOWN' and old['evidence_reason']=='RULE_DATE_UNPROVEN'
    result=derive(approved,confirmed)
    assert result['status']=='KNOWN' and (result['upper'],result['lower'])==(11,9)
    assert result['basis']=='DERIVED_FROM_PRECLOSE_AND_BOARD_RULE'
    assert result['rule_confirmation']['confirmed_trade_date']=='2026-10-12'
    assert result['rule_confirmation']['source_authenticated'] is False
    assert original.RULE_REVIEWED_THROUGH.isoformat()=='2026-10-10'
    assert old==original.resolve_price_limits({'symbol':'600001.SH','price_limit_evidence':evidence},symbol='600001.SH',at=NOW)


@pytest.mark.parametrize('mode',[
    'missing','forged_status','wrong_hash','changed_raw','changed_metadata','wrong_day','future_known',
    'late_known','missing_leg','duplicate_leg','wrong_formal_date','metadata_conflict_removed','expired',
    'unapproved_archive','unsafe_ref','extra_secret_field'])
def test_receipt_not_a_self_signed_authorization(approved,confirmed,mode):
    receipt=deepcopy(confirmed); archive=approved; at=NOW
    if mode=='missing':receipt=None
    elif mode=='forged_status':receipt={'status':'RULE_VERSION_CONFIRMED','confirmed_trade_date':'2026-10-12'}
    elif mode=='wrong_hash':receipt['receipt_sha256']='0'*64
    elif mode=='changed_raw':receipt['rows'][0]['raw_response_base64']='ZmFrZQ=='
    elif mode=='changed_metadata':receipt['rows'][0]['request_options']={'timeout_argument':99}
    elif mode=='wrong_day':receipt['confirmed_trade_date']='2026-10-09'
    elif mode=='future_known':receipt['known_at']=NOW.replace(minute=27).isoformat()
    elif mode=='late_known':receipt['known_at']=NOW.isoformat()
    elif mode=='missing_leg':receipt['rows'].pop()
    elif mode=='duplicate_leg':receipt['rows'][2]=receipt['rows'][0]
    elif mode=='wrong_formal_date':receipt['rows'][0]['formal_effective_date']='2026-10-13'
    elif mode=='metadata_conflict_removed':receipt['rows'][0]['metadata_conflicts']=[]
    elif mode=='expired':at=NOW.replace(day=19)
    elif mode=='unapproved_archive':archive=replace(approved,receipt_raw=b'fake')
    elif mode=='unsafe_ref':receipt['rows'][0]['source_ref']='token:secret'
    elif mode=='extra_secret_field':receipt['unexpected_private']='private-body'
    if isinstance(receipt,dict) and mode!='wrong_hash':seal(receipt)
    result=mod.validate_shadow_rule_receipt(archive,receipt,at=at)
    assert result['status']=='DATA_LIMITED'
    assert result['confirmed_trade_date'] is None
    assert 'private-body' not in json.dumps(result)


@pytest.mark.parametrize('changed',[
    {'symbol':'600002.SH'},{'trade_date':'2026-10-09'},{'board':'STAR'},
    {'security_status':'ST'},{'is_st':True},{'security_name':'*ST测试'},
    {'security_type':'ETF'},{'limit_regime':'IPO_NO_LIMIT'},
    {'listing_date':'2026-10-12'},{'listing_date':None},{'preclose':None},
    {'preclose':True},{'preclose':0},{'preclose':'10.001'},
    {'preclose_basis':'RAW_T1_CLOSE'},{'rule_effective_from':'2026-10-12'},
    {'observed_at':NOW.replace(minute=27).isoformat()},
    {'listing_observed_at':NOW.replace(day=9).isoformat()},
    {'upper_limit':12,'lower_limit':9},{'upper_limit':12},
])
def test_identity_and_source_gaps_never_derive(approved,confirmed,changed):
    result=derive(approved,confirmed,**changed)
    assert result['status'] in {'DATA_LIMITED','CONFLICT'}
    assert result['upper'] is None and result['lower'] is None


def test_exact_raw_binding_not_a_caller_hash_string(approved,confirmed):
    evidence,source=inputs()
    evidence['source_input_sha256']='0'*64
    result=mod.derive_shadow_price_limits(evidence,symbol='600001.SH',at=NOW,rule_receipt=confirmed,approved=approved,source=source)
    assert result['status']=='DATA_LIMITED'


def test_changed_raw_field_and_normalized_only_input_do_not_derive(approved,confirmed):
    evidence,source=inputs();evidence['preclose']=20
    result=mod.derive_shadow_price_limits(evidence,symbol='600001.SH',at=NOW,rule_receipt=confirmed,approved=approved,source=source)
    assert result['status']=='DATA_LIMITED'
    evidence,source=inputs()
    source=replace(source,normalized={'preclose':10},raw_response=None,format='NORMALIZED_QUOTE')
    result=mod.derive_shadow_price_limits(evidence,symbol='600001.SH',at=NOW,rule_receipt=confirmed,approved=approved,source=source)
    assert result['status']=='DATA_LIMITED'


def test_matching_partial_explicit_leg_does_not_change_original_derive_contract(approved,confirmed):
    # Full derivation is independently bound to the same source. Matching
    # partial explicit prices are not mixed with a second stock/day/source.
    result=derive(approved,confirmed,upper_limit=11)
    assert result['status']=='KNOWN' and (result['upper'],result['lower'])==(11,9)


def plan(**changed):
    return dict(plan_id='p1',symbol='600001.SH',status='PENDING_MORNING_REVIEW',valid_from=None,
        expires_at=NOW.replace(hour=15,minute=0).isoformat(),
        payload_json=json.dumps({'symbol':'600001.SH','target_trade_date':'2026-10-12'}))|changed


def test_new_pit_entry_replaces_only_rule_date_gap_without_mutating_inputs(approved,confirmed):
    _,source=inputs();p=plan();before=(deepcopy(p),deepcopy(confirmed),source)
    old=build_plan_pit_capture(p,source,observed_at=NOW)
    assert old['status']=='DATA_LIMITED' and old['reason_codes']==['RULE_DATE_UNPROVEN']
    result=mod.build_shadow_plan_pit_capture(p,source,observed_at=NOW,rule_receipt=confirmed,approved=approved)
    assert result['status']=='COMPLETE' and result['limits']['status']=='KNOWN'
    assert result['reason_codes']==[] and result['sealed_receipt'] is None
    assert (p,confirmed,source)==before


@pytest.mark.parametrize('changed',[
    {'expires_at':NOW.replace(minute=25).isoformat()},
    {'invalidated_at':NOW.replace(minute=25).isoformat()},
    {'payload_json':json.dumps({'symbol':'600001.SH','target_trade_date':'2026-10-09'})},
    {'status':'EXPIRED'},
])
def test_pit_plan_window_remains_closed(approved,confirmed,changed):
    _,source=inputs()
    result=mod.build_shadow_plan_pit_capture(plan(**changed),source,observed_at=NOW,rule_receipt=confirmed,approved=approved)
    assert result['status']=='DATA_LIMITED'
    assert result['limits']['upper'] is None


def test_explicit_pit_limits_do_not_require_rule_receipt_or_change_basis(approved):
    _,source=inputs(upper_limit=11,lower_limit=9)
    before=build_plan_pit_capture(plan(),source,observed_at=NOW)
    after=mod.build_shadow_plan_pit_capture(plan(),source,observed_at=NOW,rule_receipt=None,approved=None)
    assert before['status']=='COMPLETE'
    assert after==before


def test_formal_meta_conflict_preserved_not_a_rejection_by_meta_alone(approved,confirmed):
    result=mod.validate_shadow_rule_receipt(approved,confirmed,at=NOW)
    assert result['status']=='RULE_VERSION_CONFIRMED'
    assert result['metadata_conflicts']==['META_NOT_EFFECTIVE_VS_FORMAL_BODY']


@pytest.mark.parametrize('symbol,board',[
    ('600001.SH','SSE_MAIN'),('000001.SZ','SZSE_MAIN'),('300001.SZ','CHINEXT'),('688001.SH','STAR')])
@pytest.mark.parametrize('preclose',['0.01','0.02','1.05','10.05','100.01'])
def test_reviewed_day_formula_exactly_matches_frozen_original(symbol,board,preclose):
    # Formula comparison only: not an approval certificate for historical 10-09.
    at=NOW.replace(day=9)
    evidence,_=inputs(symbol=symbol,board=board,preclose=preclose,trade_date='2026-10-09',
        observed_at=at.replace(minute=25).isoformat())
    old,why=original._derive(evidence,symbol=symbol,at=at)
    new,why2=mod._derive_confirmed_day(evidence,symbol=symbol,at=at,confirmed_day=at.date())
    assert why==why2==None
    for key in ('upper','lower','board','rate','tick','rounding','rule_effective_from','rule_source','preclose'):
        assert new[key]==old[key]


@pytest.mark.parametrize('changed',[
    {'symbol':'600002.SH'},{'trade_date':'2026-10-12'},{'board':'STAR'},
    {'security_type':'ETF'},{'security_status':'ST'},{'is_st':True},{'is_st':None},
    {'limit_regime':'IPO_NO_LIMIT'},{'listing_date':'2026-10-09'},{'listing_date':None},
    {'rule_effective_from':'2026-10-12'},{'source_ref':None},
    {'source_input_sha256':'wrong'},{'preclose':None},{'preclose':True},
    {'preclose':0},{'preclose':'10.001'},{'preclose_basis':'RAW_T1_CLOSE'},
    {'observed_at':'2026-10-12T09:25:00+08:00'},
])
def test_reviewed_day_rejection_reasons_match_original(changed):
    at=NOW.replace(day=9)
    evidence,_=inputs(trade_date='2026-10-09',observed_at=at.replace(minute=25).isoformat())
    evidence.update(changed)
    old,why=original._derive(evidence,symbol='600001.SH',at=at)
    new,why2=mod._derive_confirmed_day(evidence,symbol='600001.SH',at=at,confirmed_day=at.date())
    assert old==new==None and why==why2


def test_rule_and_price_modules_original_source_pins():
    assert hashlib.sha256(Path(original.__file__).read_bytes()).hexdigest()=='ec1c395b452e8d6fa1e5b6016f05f88a2ccdb982296f1b8940b73ed0d8d4549c'
    assert hashlib.sha256(Path(rule.__file__).read_bytes()).hexdigest()=='efbb4d10067e4bc3e976c3e0f49a5530aa45e97b97b00c0f742e494ae4cd77fc'
