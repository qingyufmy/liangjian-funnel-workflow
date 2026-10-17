import importlib.util
from pathlib import Path
from types import SimpleNamespace
import sys

import pytest


@pytest.fixture
def profile_module():
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root / 'scripts'))
    spec = importlib.util.spec_from_file_location('offline_a4_profile', root / 'scripts/profile_team_a4_frozen_input.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_profile_uses_exact_messages_and_reports_missing_bindings(profile_module):
    values = dict(BARS={'status':'UNAVAILABLE'}, TRADABILITY={'status':'UNAVAILABLE'},
        ACTIVE_PLANS=[{'plan_id':'p', 'symbol':'600000'}],
        EVIDENCE_CATALOG={'items':[
            {'id':'p', 'category':'PLAN', 'value':{'strategy_profile':'TREND_MA5'}},
            {'id':'t', 'category':'STOCK_TECH', 'value':{'closed_bars':{'1m':[{'close':1}]}}}]})
    request = SimpleNamespace(messages=({'role':'system','content':'中文ab'}, {'role':'user','content':'xx'}),
        input_hash='i', evidence_catalog_hash='c', prompt=SimpleNamespace(source_hashes={'a':'s'}, role_version='v'))
    result = profile_module.describe(values, request)
    assert result['exact_message_content_chars'] == 6
    assert result['exact_message_content_utf8_bytes'] == 10
    assert result['unavailable_placeholders'] == ['BARS','TRADABILITY']
    assert result['active_plan_fields'] == ['plan_id','symbol']
    assert result['stock_tech_field_value_chars']['closed_bars'] > 0


def test_capture_stops_after_real_renderer_before_any_caller_continuation(profile_module):
    request = object()
    class Repository:
        def prepare_messages(self, role, values, **kwargs):
            assert role == 'A4'
            return request
    repository = profile_module.CaptureRepository(Repository())
    values = {'BARS':{'status':'UNAVAILABLE'}}
    with pytest.raises(profile_module.ProfileComplete):
        repository.prepare_messages('A4', values, session='INTRADAY')
    assert repository.captured == (values, request)
