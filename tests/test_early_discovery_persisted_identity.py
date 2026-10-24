from copy import deepcopy
import json

from liangjian_funnel.pipeline.early_discovery import verify_discovery_event_identity
from liangjian_funnel.pipeline.feature_store import content_hash
from test_selected_board_startup_nomination import inputs


def event():return inputs()[0].data['EARLY_DISCOVERY_SNAPSHOT']['records'][0]


def test_new_identity_survives_json_persistence_and_reordered_ma_keys():
    e=event();persisted=json.loads(json.dumps(e))
    assert verify_discovery_event_identity(persisted)
    persisted['evidence']['ma']=dict(reversed(list(persisted['evidence']['ma'].items())))
    assert verify_discovery_event_identity(persisted)


def test_exact_legacy_int_key_format_verifies_without_rewriting_old_id():
    e=event();e.pop('identity_version')
    e['evidence']['ma']={5:63.328,20:67.3515,60:65.0}
    e['evidence']['previous_ma']={5:62.0,20:67.0,60:64.0}
    e['decision_id']=content_hash(dict(version='early-discovery/1',symbol=e['symbol'],evidence=e['evidence']))
    persisted=json.loads(json.dumps(e));before=deepcopy(persisted)
    assert verify_discovery_event_identity(persisted) and persisted==before
    persisted['evidence']['ma']['5']=999
    assert not verify_discovery_event_identity(persisted)


def test_legacy_conflicting_integer_and_text_keys_are_not_resolved_by_iteration():
    e=event();e.pop('identity_version')
    e['evidence']['ma'][5]=e['evidence']['ma']['5']
    assert not verify_discovery_event_identity(e)


def test_unknown_identity_format_and_forged_event_never_fall_back():
    e=event();e['identity_version']='UNKNOWN'
    assert not verify_discovery_event_identity(e)
    e=event();e['decision_id']='0'*64
    assert not verify_discovery_event_identity(e)


def test_actual_scanner_emits_same_verifiable_id_before_and_after_persistence():
    from test_early_discovery import repair_bars,scan
    fresh=scan(repair_bars())
    persisted=json.loads(json.dumps(fresh,sort_keys=True))
    assert fresh==persisted
    assert all(verify_discovery_event_identity(r) for r in persisted['records'])
