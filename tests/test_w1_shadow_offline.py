from copy import deepcopy
import importlib.util
import json
from pathlib import Path

import pytest

from test_w1_shadow_variants import item, fake_result
from liangjian_funnel.runtime.shadow_variants import ShadowVariantEngine, SHADOW_VARIANTS_V1
from liangjian_funnel.evaluation.ablation.dataset import digest

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/run_shadow_offline.py'
spec = importlib.util.spec_from_file_location('w1_offline', SCRIPT)
offline = importlib.util.module_from_spec(spec)
spec.loader.exec_module(offline)


def record():
    payload = item()['plan']
    return {'plan_id':'p', 'payload':payload, 'sha256':digest(payload)}


def test_existing_append_only_inputs_are_not_changed():
    source = record()
    before = deepcopy(source)
    plan, evidence = offline.adapt_frozen_plan(source, trade_date='2026-09-30', source_ref='local.json')
    assert evidence['status'] == 'ELIGIBLE'
    assert evidence['conversion'] == 'EXISTING_SHADOW_INPUTS'
    assert evidence['source_plan_sha256'] == source['sha256']
    assert plan == before['payload'] and source == before


def test_adapter_requires_actual_dates_asof_and_atr_hash_not_name_or_ma5():
    source = record()
    source['payload']['strategy_facts'].pop('shadow_inputs')
    source['sha256'] = digest(source['payload'])
    plan, evidence = offline.adapt_frozen_plan(source, trade_date='2026-09-30', source_ref='local.json')
    assert plan is None and evidence['status'] == 'DATA_LIMITED'
    assert evidence['field_presence']['atr14'] == 'PRESENT'
    assert evidence['field_presence']['previous_daily_closes'] == 'MISSING'
    assert evidence['reason'] == 'FROZEN_DAILY_SHADOW_FIELDS_INCOMPLETE'


def test_complete_original_fields_can_be_adapted_with_explicit_source_binding():
    source = record()
    fields = source['payload']['strategy_facts'].pop('shadow_inputs')
    source['payload']['daily_indicators'].update({key:value for key,value in fields.items()
        if key not in ('schema_version','daily_ma5')})
    source['sha256'] = digest(source['payload'])
    before = deepcopy(source)
    plan, evidence = offline.adapt_frozen_plan(source, trade_date='2026-09-30', source_ref='local.json')
    assert evidence['status'] == 'ELIGIBLE'
    assert evidence['conversion'] == 'FROZEN_DAILY_CONTEXT_TO_SHADOW_INPUTS/1'
    assert plan['strategy_facts']['shadow_inputs']['source_ref'] == 'local.json#plan:p'
    assert source == before


def test_missing_null_empty_are_distinct_and_bad_plan_hash_rejected():
    assert [offline.presence({},'x'), offline.presence({'x':None},'x'),
        offline.presence({'x':[]},'x')] == ['MISSING','NULL','EMPTY']
    source = record()
    source['sha256'] = 'b'*64
    with pytest.raises(ValueError, match='PLAN_HASH_MISMATCH'):
        offline.adapt_frozen_plan(source, trade_date='2026-09-30', source_ref='local.json')


def test_first_trigger_comparison_never_promotes_empty_or_missing_reference():
    assert offline.compare_first_triggers({}, {}, eligible_windows=0)['difference_count'] is None
    assert not offline.compare_first_triggers({}, {}, eligible_windows=0)['golden_pass']
    result = offline.compare_first_triggers({'p|V1':'09:35'}, {'p|V1':'09:36'}, eligible_windows=1)
    assert result['difference_count'] == 1 and not result['golden_pass']
    assert offline.compare_first_triggers({}, {}, eligible_windows=1)['golden_pass']
    assert not offline.compare_first_triggers({}, None, eligible_windows=1)['golden_pass']


def test_actual_nested_matrix_schema_is_read_not_invented_status(tmp_path, monkeypatch):
    data = item()
    at = data['now'].isoformat()
    projected, reason = offline._projection(data['baseline'])
    day = '2026-09-30'
    directory = tmp_path/day
    directory.mkdir()
    rows_path = directory/'rows.json'
    rows_path.write_text(json.dumps([{'plan_id':'p','scenario':variant.matrix_scenario,
        'minute':at,'action':'BUY_SIGNAL','ablation':{'status':'OK'}}
        for variant in SHADOW_VARIANTS_V1]),encoding='utf-8')
    monkeypatch.setattr(offline,'load_dataset',lambda _:dict(coverage={'status':'COMPLETE'},
        plans=[{'plan_id':'p','payload':data['plan']}],windows=[dict(plan_id='p',
        decision_time=at,observation_time=at,bars=data['bars'],market_context=data['market_context'],
        recorded_action=projected,recorded_reason=reason)]))
    monkeypatch.setattr(offline,'ShadowVariantEngine',lambda:ShadowVariantEngine(
        evaluator=lambda *a,**k:fake_result('BUY_SIGNAL')))
    info = {'rows_expected_sha256':offline.file_hash(rows_path)}
    result = offline.replay_day(tmp_path/'input.json',{'p':data['plan']},tmp_path,day,info)
    assert result['golden_pass'] and result['expected_first_trigger_count'] == 6
    assert result['actual_first_trigger_count'] == 6 and result['difference_count'] == 0
