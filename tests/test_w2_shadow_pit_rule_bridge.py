"""Pure/local fixture chain only; no live daily rule or catalogue receipt."""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime,timedelta
import hashlib
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.runtime import shadow_pit_rule_bridge as mod
from liangjian_funnel.runtime import shadow_rule_preflight as rule
from liangjian_funnel.runtime.shadow_pit_capture import PITSourceReceipt
from liangjian_funnel.runtime.shadow_pit_sources import record_raw_response

ROOT=Path(__file__).parents[1]
ARCHIVE=ROOT/'artifacts/wp5-20261010/official-rules-20261010'
SH=ZoneInfo('Asia/Shanghai')
PRE=datetime(2026,10,12,8,31,tzinfo=SH)
NOW=PRE.replace(hour=9,minute=26)


@pytest.fixture(scope='module')
def confirmed():
    approved=rule.load_approved_rule_archive(ARCHIVE)
    rows=[record_raw_response(d.raw_response,source_ref='official-rule:'+d.document_id,endpoint=d.url,
        request_parameters={},request_started_at=PRE-timedelta(seconds=1),response_received_at=PRE,
        byte_kind='HTTP_RESPONSE_CONTENT_BYTES') for d in approved.documents]
    return rule.confirm_rule_version(approved,rows,trade_date='2026-10-12',known_at=PRE)


def write_package(tmp_path,confirmed,*,known_at=PRE,trade_date='2026-10-12',raw=None,sha=None):
    body=raw or json.dumps(confirmed,ensure_ascii=False,indent=2).encode()
    p=tmp_path/'daily-rule.json';p.write_bytes(body)
    return mod.load_daily_rule_package(receipt_path=p,receipt_file_sha256=sha or hashlib.sha256(body).hexdigest(),
        approved_archive_root=ARCHIVE,target_trade_date=trade_date,known_at=known_at)


@pytest.fixture
def package(tmp_path,confirmed):
    result=write_package(tmp_path,confirmed)
    assert result['status']=='RULE_VERSION_CONFIRMED'
    return result['package']


def plan(**changes):
    return dict(plan_id='p1',symbol='600001.SH',lane_id='lane_1',plan_version='1.5.0',
        status='PENDING_MORNING_REVIEW',valid_from=None,expires_at=NOW.replace(hour=15,minute=0).isoformat(),
        payload_json=' {"plan_id":"p1", "symbol":"600001.SH", "target_trade_date":"2026-10-12"} ')|changes


def source(**changes):
    fields=dict(symbol='600001.SH',trade_date='2026-10-12',observed_at=NOW.replace(minute=25).isoformat(),
        security_name='测试普通股',security_name_basis='SOURCE_QUOTE_NAME',preclose=10,
        preclose_basis='EXCHANGE_DISPLAYED_PRECLOSE',upper_limit=None,lower_limit=None,
        listing_date='2000-01-01',listing_date_basis='SOURCE_LISTING_DATE',listing_observed_at=NOW.replace(minute=25).isoformat(),
        board='SSE_MAIN',security_type='CASH_A_SHARE',security_status='ORDINARY',is_st=False,
        limit_regime='NORMAL',rule_effective_from='2026-07-06',prior_raw_close=None)
    fields.update(changes)
    raw=json.dumps(fields,ensure_ascii=False,separators=(',',':')).encode()
    return PITSourceReceipt('fixture:identity',NOW,True,raw_response=raw,byte_kind='CONSUMER_INPUT_BYTES')


def prepare(package,p=None,s=None,now=NOW):
    return mod.prepare_rule_bound_pit(plan() if p is None else p,source() if s is None else s,
        observed_at=now,rule_package=package)


def test_loader_binds_exact_file_bytes_and_distinct_canonical_receipt_sha(tmp_path,confirmed):
    result=write_package(tmp_path,confirmed)
    pkg=result['package']
    assert pkg.receipt_file_sha256==hashlib.sha256(pkg.receipt_raw).hexdigest()
    assert pkg.receipt_file_sha256!=confirmed['receipt_sha256']
    assert result['source_authenticated'] is False and result['http_requests']==0


@pytest.mark.parametrize('mode',[
    'missing_file','wrong_sha','wrong_day','old_day','late_load','naive_clock','missing_originals',
    'forged_status','bad_schema','duplicate_json_key','nonfinite','approval_missing'])
def test_loader_missing_or_forged_daily_package_is_limited(tmp_path,confirmed,mode):
    raw=json.dumps(confirmed).encode();sha=None;day='2026-10-12';known=PRE;archive=ARCHIVE
    if mode=='wrong_sha':sha='0'*64
    elif mode=='wrong_day':day='2026-10-13'
    elif mode=='old_day':known=PRE.replace(day=9);day='2026-10-09'
    elif mode=='late_load':known=NOW
    elif mode=='naive_clock':known=PRE.replace(tzinfo=None)
    elif mode=='missing_originals':raw=json.dumps(confirmed|{'rows':[]}).encode()
    elif mode=='forged_status':raw=b'{"status":"RULE_VERSION_CONFIRMED"}'
    elif mode=='bad_schema':raw=json.dumps(confirmed|{'schema_version':'fake/1'}).encode()
    elif mode=='duplicate_json_key':raw=b'{"status":"X","status":"RULE_VERSION_CONFIRMED"}'
    elif mode=='nonfinite':raw=b'{"status":NaN}'
    elif mode=='approval_missing':archive=tmp_path/'absent-approved'
    p=tmp_path/'daily.json'
    if mode!='missing_file':p.write_bytes(raw)
    result=mod.load_daily_rule_package(receipt_path=p,receipt_file_sha256=sha or hashlib.sha256(raw).hexdigest(),
        approved_archive_root=archive,target_trade_date=day,known_at=known)
    assert result['status']=='DATA_LIMITED' and result['package'] is None


def test_material_keeps_original_payload_string_and_capture_clock(package):
    p=plan();s=source();before=(deepcopy(p),s,package)
    result=prepare(package,p,s)
    assert result['status']=='PREPARED' and result['entry']['status']=='COMPLETE'
    material=result['material'];body=json.loads(material.canonical_bytes)
    assert material.sha256==hashlib.sha256(material.canonical_bytes).hexdigest()
    assert body['plan_binding']['payload_json_bytes_sha256']==hashlib.sha256(p['payload_json'].encode()).hexdigest()
    assert body['plan_binding']['payload_json_bytes_sha256']!=hashlib.sha256(json.dumps(json.loads(p['payload_json'])).encode()).hexdigest()
    assert body['captured_at']==NOW.isoformat()
    assert body['authority_kind']=='SHADOW_DAILY_RULE_DERIVED'
    assert body['limits']['basis']=='DERIVED_FROM_PRECLOSE_AND_BOARD_RULE'
    assert body['derived_outcome_consumption']=='UNWIRED'
    assert body['ledger_seal_status']=='NOT_WRITTEN'
    assert body['source_authenticated'] is False
    assert (p,s,package)==before
    later=mod.validate_rule_bound_material(material,plan=p,source=s,rule_package=package,at=NOW.replace(minute=32))
    assert later['status']=='VALIDATED_PREPARED_MATERIAL'
    assert later['captured_at']==NOW.isoformat() and later['provider_integration']=='UNWIRED'


@pytest.mark.parametrize('changed',[
    {'payload_json':None},{'payload_json':{'symbol':'600001.SH','target_trade_date':'2026-10-12'}},
    {'payload_json':'{"plan_id":"wrong","symbol":"600001.SH","target_trade_date":"2026-10-12"}'},
    {'payload_json':'{"symbol":"600002.SH","target_trade_date":"2026-10-12"}'},
    {'payload_json':'{"symbol":"600001.SH","target_trade_date":"2026-10-09"}'},
    {'payload_json':'{"symbol":"600001.SH","symbol":"600002.SH"}'},
    {'plan_id':None},{'symbol':'600002.SH'},{'lane_id':None},{'plan_version':None},
    {'status':'EXPIRED'},{'expires_at':NOW.replace(minute=25).isoformat()},
    {'invalidated_at':NOW.replace(minute=25).isoformat()},
])
def test_raw_plan_identity_binding_is_required(package,changed):
    result=prepare(package,plan(**changed))
    assert result['status']=='DATA_LIMITED' and result['material'] is None


@pytest.mark.parametrize('changed',[
    {'is_st':True},{'security_name':'ST测试'},{'listing_date':None},{'listing_date':'2026-10-12'},
    {'preclose':None},{'preclose_basis':'RAW_T1_CLOSE'},{'symbol':'600002.SH'},
    {'observed_at':NOW.replace(day=9).isoformat()},{'observed_at':NOW.replace(minute=27).isoformat()},
])
def test_rule_confirmed_does_not_fill_identity_or_raw_gaps(package,changed):
    result=prepare(package,s=source(**changed))
    assert result['status']=='DATA_LIMITED' and result['material'] is None


def test_default_package_and_source_unwired(package):
    no_rule=prepare(None)
    assert no_rule['status']=='DATA_LIMITED'
    no_source=mod.prepare_rule_bound_pit(plan(),None,observed_at=NOW,rule_package=package)
    assert no_source['status']=='DATA_LIMITED'


@pytest.mark.parametrize('mode',['package_hash','material_hash','material_body','wrong_day','future_capture','changed_payload','source_raw'])
def test_material_cannot_be_self_declared_or_rebound(package,mode):
    p=plan();s=source();material=prepare(package,p,s)['material'];pkg=package;at=NOW.replace(minute=32)
    if mode=='package_hash':pkg=replace(package,receipt_file_sha256='0'*64)
    elif mode=='material_hash':material=replace(material,sha256='0'*64)
    elif mode=='material_body':
        body=json.loads(material.canonical_bytes);body['authority_kind']='EXPLICIT';raw=json.dumps(body).encode()
        material=replace(material,canonical_bytes=raw,sha256=hashlib.sha256(raw).hexdigest())
    elif mode=='wrong_day':at=at.replace(day=13)
    elif mode=='future_capture':at=NOW.replace(minute=25)
    elif mode=='changed_payload':p['payload_json']=p['payload_json'].strip()
    elif mode=='source_raw':s=source(preclose=20)
    result=mod.validate_rule_bound_material(material,plan=p,source=s,rule_package=pkg,at=at)
    assert result['status']=='DATA_LIMITED'


def test_raw_model_extra_is_hashed_not_emitted(package):
    p=plan();body=json.loads(p['payload_json']);body['model_reasoning']='private-model-body'
    p['payload_json']=json.dumps(body)
    result=prepare(package,p)
    assert result['status']=='PREPARED'
    assert b'private-model-body' not in result['material'].canonical_bytes


def test_explicit_limits_keep_separate_authority(package):
    result=prepare(package,s=source(upper_limit=11,lower_limit=9))
    assert result['status']=='PREPARED'
    body=json.loads(result['material'].canonical_bytes)
    assert body['authority_kind']=='EXPLICIT_SOURCE_LIMITS'
    assert body['limits']['basis']=='EXPLICIT_FROZEN_SAME_DAY'


@pytest.mark.parametrize('key,value',[('lane_id','other_lane'),('plan_version','other_version'),
                                     ('model_number',float('inf'))])
def test_inner_identity_and_nested_numeric_input_cannot_conflict(package,key,value):
    p=plan();body=json.loads(p['payload_json']);body[key]=value
    p['payload_json']=json.dumps(body).replace('Infinity','1e400')
    assert prepare(package,p)['status']=='DATA_LIMITED'


def test_prepared_material_not_valid_after_original_window(package):
    p=plan();s=source();material=prepare(package,p,s)['material']
    result=mod.validate_rule_bound_material(material,plan=p,source=s,rule_package=package,
                                          at=NOW.replace(hour=15,minute=1))
    assert result['status']=='DATA_LIMITED'
