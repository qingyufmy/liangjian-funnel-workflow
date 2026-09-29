import importlib.util
import json
from pathlib import Path
import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('field_sensitivity', ROOT / 'scripts/audit_a4_field_sensitivity.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def facts():
    path = ROOT / 'artifacts/a3-a5-20260929/post-close-853a8225ed2b-facts.json'
    if not path.exists():
        pytest.skip('Frozen 20260929 production facts unavailable')
    return json.loads(path.read_text(encoding='utf-8'))


def test_frozen_sensitivity_reproduces_baseline_without_mutating_facts():
    source = facts()
    before = json.dumps(source, sort_keys=True)
    result = module.audit(source)
    assert json.dumps(source, sort_keys=True) == before
    assert result['plans'][0]['baseline_minutes'] == 239
    assert result['plans'][0]['baseline_reasons_match']
    assert len(result['plans'][0]['scenarios']) == 3
    assert all(s['buy_count'] == s['action_changes'] == 0 and not s['differences']
               for s in result['plans'][0]['scenarios'])


def test_replacement_rejects_wrong_left_value():
    with pytest.raises(ValueError, match='FROZEN_LEFT_VALUE_NOT_MATCHED'):
        module.replace_field([{'bar_end': 't', 'high': 1}], {'bar_end': 't', 'left': 2, 'right': 3}, 'HIGH')


def test_replay_refuses_to_invent_position_lifecycle():
    source = facts()
    event = next(e for e in source['a4']['events'] if e.get('plan_id'))
    event.update(action='BUY_SIGNAL', effective=True)
    with pytest.raises(ValueError, match='LIFECYCLE_REPLAY_NOT_SUPPORTED'):
        module.audit(source)
