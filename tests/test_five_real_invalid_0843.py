"""Original production replies + original typed references; never a new model call."""
from pathlib import Path
from copy import deepcopy
import hashlib
import json
import types
import pytest
from liangjian_funnel.pipeline.team_v2_trader_schema import validate_trader_output,project_trader_references

F=Path(__file__).parent/'fixtures/postclose_20261027'
ROWS=json.loads((F/'five_invalid_originals.json').read_bytes())['rows']

@pytest.mark.parametrize('row',ROWS,ids=lambda row:row['call_id'].split('T')[1][:4])
def test_actual_five_invalid_become_valid_without_changing_decisions(row):
    raw=json.loads(row['output_json']);catalog=json.loads(row['catalog']['catalog_json'])
    assert hashlib.sha256(row['output_json'].encode()).hexdigest()==row['output_hash']
    assert hashlib.sha256(row['catalog']['catalog_json'].encode()).hexdigest()==row['catalog']['catalog_sha256']
    plans={signal['plan_id']:signal['symbol'] for signal in raw['signals']}
    refs={r['plan_id']:{**{key:'MET' for key in json.loads(r['met'] or '[]')},
        **{key:'UNMET' for key in json.loads(r['unmet'] or '[]')}} for r in row['same_tick_original_flags']
        if r['plan_id'] in plans}
    assert set(refs)==set(plans)
    args=dict(required_envelope=raw['envelope'],decision_id=raw['decision_id'],
        minute_snapshot_id=raw['minute_snapshot_id'],plans=plans,held_symbols=set(),tech_reference=refs,
        candidate_actions={pid:'BUY_SIGNAL' for pid in plans},position_candidate_actions={})
    old=types.ModuleType('liangjian_funnel.pipeline.frozen_trader_schema_6eaaf75')
    old.__package__='liangjian_funnel.pipeline'
    exec(compile((F/'trader_schema_production_6eaaf75.py').read_bytes(),str(F/'trader_schema_production_6eaaf75.py'),'exec'),old.__dict__)
    assert not old.validate_trader_output(raw,catalog,**args).valid
    original=deepcopy(raw)
    checked=validate_trader_output(raw,catalog,**args)
    assert checked.valid,checked.issues
    assert raw==original
    projected,changes=project_trader_references(raw,refs)
    assert changes
    assert [r['action'] for r in projected['signals']]==[r['action'] for r in raw['signals']]
    assert all(r['action']=='NO_ACTION' for r in raw['signals'])
    # Correcting a reference echo never authorizes unknown IDs or new fields.
    broken=deepcopy(raw);broken['signals'][0]['tech_reference_review'][0]['item']='UNKNOWN_REAL_NEGATIVE'
    assert not validate_trader_output(broken,catalog,**args).valid
    broken=deepcopy(raw);broken['signals'][0]['extra_authority']='BUY'
    assert not validate_trader_output(broken,catalog,**args).valid
    broken=deepcopy(raw);broken['signals'][0]['tech_reference_review'][0]['status']='UNKNOWN'
    assert not validate_trader_output(broken,catalog,**args).valid
