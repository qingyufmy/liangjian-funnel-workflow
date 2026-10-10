"""Numeric MA keys must survive the actual JSON persistence boundary."""
from copy import deepcopy
from datetime import datetime
import json
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.pipeline import close_scope as scope
from liangjian_funnel.pipeline.disclosure_maintenance import (
    build_maintenance_queue, seal_maintenance_queue, validate_queue,
)
from liangjian_funnel.pipeline.feature_store import content_hash
from liangjian_funnel.reporting import atomic_write_json
from test_wp5_disclosure_prefilter import build

NOW = datetime(2026,10,9,15,10,tzinfo=ZoneInfo('Asia/Shanghai'))
SYMBOLS = ['000000.SZ','000001.SZ','000002.SZ']


def args(tmp_path):
    return dict(root=tmp_path,run_id='original-numeric-key-run',research_as_of=NOW,
        market_data_as_of=NOW,a1_reference={'generation_id':'original','payload_hash':'f'*64},
        a1_symbols=SYMBOLS[:1],hot_payload={'records':[{'symbol':SYMBOLS[1]}]},
        discovery={'records':[{'symbol':SYMBOLS[2],'review_budget_selected':True,
            'evidence':{'ma':{5:10.,20:9.,60:8.},'previous_ma':{5:9.,20:8.,60:7.}}}]},
        g0_symbols=SYMBOLS,selected_symbols=SYMBOLS)


def payload(schema='close-scope-receipt/2'):
    options = args(None)
    return {'schema_version':schema,'recorded_at':NOW.isoformat(),'run_id':options['run_id'],
        'research_as_of':NOW.isoformat(),'market_data_as_of':NOW.isoformat(),
        'a1_reference':options['a1_reference'],'binding_status':'ORIGINAL_RUN_REFERENCE',
        'execution_authority':False,'discovery':options['discovery'],'hot100':options['hot_payload'],
        'scope':scope.build_scope_ledger(a1_symbols=SYMBOLS[:1],hot_symbols=SYMBOLS[1:2],
            discovery_symbols=SYMBOLS[2:],g0_symbols=SYMBOLS)}


def persisted_v2():
    return json.loads(json.dumps(scope.hash_scope_receipt(payload())))


def test_producer_atomic_json_validator_accepts_original_integer_ma_keys(tmp_path):
    options = args(tmp_path)
    original = deepcopy(options['discovery'])
    path = scope.seal_scope_receipt(**options)
    value = json.loads(path.read_text(encoding='utf-8'))
    scope.validate_scope_receipt(value)
    assert value['schema_version'] == 'close-scope-receipt/3'
    assert options['discovery'] == original
    assert set(value['discovery']['records'][0]['evidence']['ma']) == {'5','20','60'}


def test_retry_identical_scope_reuses_original_bytes_not_new_observation(tmp_path):
    options = args(tmp_path)
    first = scope.seal_scope_receipt(**options)
    before = first.read_bytes()
    assert scope.seal_scope_receipt(**options) == first
    assert first.read_bytes() == before
    assert len(list(tmp_path.glob('*.json'))) == 1


def test_maintenance_queue_passes_same_json_boundary_without_admission_change(tmp_path):
    source = json.loads(scope.seal_scope_receipt(**args(tmp_path)).read_text(encoding='utf-8'))
    prefilter = build(SYMBOLS,hot_symbols=SYMBOLS[1:2],discovery_symbols=SYMBOLS[2:])
    queue = build_maintenance_queue(source,prefilter)
    assert queue['source_receipt_hash'] == source['receipt_hash']
    assert queue['execution_authority'] is False and queue['changes_query_scope'] is False


def test_old_v2_only_confirmed_ma_key_restore_verifies_both_hashes_without_mutation():
    value = persisted_v2()
    before = deepcopy(value)
    scope.validate_scope_receipt(value)
    assert value == before


@pytest.mark.parametrize('fault',['receipt','observation','recorded_at','a1','ma_value','extra_ma_key'])
def test_old_v2_compatibility_never_ignores_a_changed_field(fault):
    value = persisted_v2()
    if fault == 'receipt': value['receipt_hash'] = '0'*64
    if fault == 'observation': value['observation_hash'] = '0'*64
    if fault == 'recorded_at': value['recorded_at'] = NOW.replace(hour=16).isoformat()
    if fault == 'a1': value['a1_reference']['generation_id'] = 'later'
    if fault == 'ma_value': value['discovery']['records'][0]['evidence']['ma']['5'] = 999
    if fault == 'extra_ma_key': value['discovery']['records'][0]['evidence']['ma']['7'] = 999
    with pytest.raises(ValueError): scope.validate_scope_receipt(value)


def test_non_ma_numeric_keys_are_not_guessed_for_old_v2():
    value = payload()
    value['unrelated'] = {5:'a',20:'b',60:'c'}
    value = json.loads(json.dumps(scope.hash_scope_receipt(value)))
    with pytest.raises(ValueError,match='CLOSE_SCOPE_RECEIPT_HASH_MISMATCH'):
        scope.validate_scope_receipt(value)


def test_schema3_detaches_real_json_shape_before_hash():
    value = payload('close-scope-receipt/3')
    frozen = scope.hash_scope_receipt(value)
    value['discovery']['records'][0]['evidence']['ma'][5] = 999
    assert frozen['discovery']['records'][0]['evidence']['ma']['5'] == 10
    scope.validate_scope_receipt(json.loads(json.dumps(frozen)))


@pytest.mark.parametrize('bad',[float('nan'),float('inf'),-float('inf')])
def test_schema3_nonfinite_values_are_rejected(bad):
    value = payload('close-scope-receipt/3')
    value['discovery']['records'][0]['evidence']['ma'][5] = bad
    with pytest.raises(ValueError): scope.hash_scope_receipt(value)


def test_schema3_stringified_key_collision_is_rejected_before_loss():
    value = payload('close-scope-receipt/3')
    value['discovery']['records'][0]['evidence']['ma']['5'] = 999
    with pytest.raises(ValueError,match='^RECEIPT_KEY_COLLISION$'):
        scope.hash_scope_receipt(value)


@pytest.mark.parametrize('field',['receipt_hash','observation_hash'])
def test_schema3_still_strictly_checks_both_hashes(field):
    value = scope.hash_scope_receipt(payload('close-scope-receipt/3'))
    value[field] = '0'*64
    with pytest.raises(ValueError): scope.validate_scope_receipt(value)


def test_schema3_validator_does_not_resanitize_and_hide_persisted_tampering():
    original = payload('close-scope-receipt/3')
    original['source'] = {'token':'fixture-not-a-real-secret'}
    frozen = scope.hash_scope_receipt(original)
    assert frozen['source']['token'] == '[REDACTED]'
    scope.validate_scope_receipt(frozen)
    frozen['source']['token'] = 'tampered-persisted-value'
    with pytest.raises(ValueError,match='CLOSE_SCOPE_RECEIPT_HASH_MISMATCH'):
        scope.validate_scope_receipt(frozen)


# Deterministic property-style corpus: finite accepted key families crossed
# with nested list/tuple containers, depth and insertion order. No dependency
# or random seed; not claimed as a proof for every arbitrary Python object.
KEY_FAMILIES = [
    (5,20,60),
    (2.5,10.5,-.125),
    (True,False),
    (None,),
    (2,2.5,False,None,'string'),
    ('unicode中文','control\nkey','quoted"key'),
    (1e20,1e-7,-1e20),
]


def generated_tree(keys,depth,reverse):
    keys=tuple(reversed(keys)) if reverse else keys
    value={key:{'value':index,'nil':None,'bool':bool(index%2),'text':'中文\n"'}
           for index,key in enumerate(keys)}
    for level in range(depth):
        value={'children':({level+100:value},),'flag':False,'nil':None}
    return value


@pytest.mark.parametrize('keys',KEY_FAMILIES)
@pytest.mark.parametrize('depth',[0,1,5])
@pytest.mark.parametrize('reverse',[False,True])
def test_schema3_generated_output_hash_invariant_and_actual_maintenance_writer(
        tmp_path,keys,depth,reverse):
    tree=generated_tree(keys,depth,reverse)
    options=args(tmp_path/'scopes')
    options['discovery']['records'][0]['evidence']['generated']=tree
    original=deepcopy(options['discovery'])
    source_path=scope.seal_scope_receipt(**options)
    persisted=json.loads(source_path.read_text(encoding='utf-8'))
    scope.validate_scope_receipt(persisted)
    assert options['discovery']==original
    signed=scope.hash_scope_receipt({**payload('close-scope-receipt/3'),
                                   'discovery':options['discovery'],
                                   'recorded_at':persisted['recorded_at']})
    assert signed==persisted
    roundtrip=json.loads(json.dumps(signed,ensure_ascii=False,allow_nan=False))
    assert scope._hash(signed)==scope._hash(roundtrip)
    assert scope.hash_scope_receipt(signed)==scope.hash_scope_receipt(roundtrip)
    actual_path=tmp_path/'actual-atomic-writer.json'
    atomic_write_json(actual_path,signed)
    actual=json.loads(actual_path.read_text(encoding='utf-8'))
    assert actual==signed and scope._hash(actual)==scope._hash(signed)
    scope.validate_scope_receipt(actual)
    prefilter=build(SYMBOLS,hot_symbols=SYMBOLS[1:2],discovery_symbols=SYMBOLS[2:])
    queue=build_maintenance_queue(actual,prefilter)
    queue_path=seal_maintenance_queue(tmp_path/'queues',actual,prefilter)
    read_queue=json.loads(queue_path.read_text(encoding='utf-8'))
    assert read_queue==queue
    assert content_hash(queue)==content_hash(read_queue)
    validate_queue(read_queue)
    assert read_queue['source_receipt_hash']==signed['receipt_hash']


COLLISION_PAIRS=[(1,'1'),(1.25,'1.25'),(True,'true'),(False,'false'),(None,'null')]


@pytest.mark.parametrize('pair',COLLISION_PAIRS)
@pytest.mark.parametrize('depth',[0,1,5])
@pytest.mark.parametrize('reverse',[False,True])
def test_generated_key_collision_exact_code_before_any_receipt_write(tmp_path,pair,depth,reverse):
    options=args(tmp_path/'not-written')
    options['discovery']['records'][0]['evidence']['generated']=generated_tree(pair,depth,reverse)
    original=deepcopy(options['discovery'])
    with pytest.raises(ValueError,match='^RECEIPT_KEY_COLLISION$'):
        scope.seal_scope_receipt(**options)
    assert options['discovery']==original
    assert not options['root'].exists()


def test_legacy_v1_validator_preserves_original_bytes_and_does_not_resign(tmp_path):
    value=payload('close-scope-receipt/1')
    value['discovery']['records'][0]['evidence']={'ma':{'5':10,'20':9,'60':8}}
    value['receipt_hash']=scope._hash(value)
    path=tmp_path/'original-v1.json'
    path.write_text(json.dumps(value,ensure_ascii=False,indent=3),encoding='utf-8')
    before=path.read_bytes(); loaded=json.loads(before)
    original=deepcopy(loaded)
    scope.validate_scope_receipt(loaded)
    assert loaded==original and path.read_bytes()==before

