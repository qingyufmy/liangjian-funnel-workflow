from copy import deepcopy
import json
from pathlib import Path
import pytest
from liangjian_funnel.review.key_counts import reconcile_counts

F=Path(__file__).parent/'fixtures/postclose_20261027'


def original_day():
    day=json.loads((F/'full_day_runtime_rows.json').read_bytes())
    actual=dict(plans=len(day['plans']),monitor_rows=len(day['minute_events']),
        minute_rows=sum(bool(r['plan_id']) for r in day['minute_events']),signals=0,model_calls=12,fills=0)
    # The external original day audit independently proves the 12 attempts,
    # 11 captured calls, 13 raw opinions, no admissions/fills. Do not call the
    # latter three values the number of model attempts.
    facts=dict(a3={'plans':day['plans']},metrics=dict(a3_plan_count=14,
        a4_monitor_observation_count=actual['monitor_rows'],a4_trade_signal_count=0,
        a4_team_model_attempt_count=12,a4_team_fill_count=0,a4_stock_minute_count=3346))
    return facts,actual


def test_real_1027_stock_minutes_and_empty_scope_are_distinct():
    facts,actual=original_day()
    assert actual['minute_rows']==3346 and actual['monitor_rows']==3347
    receipt=reconcile_counts(facts,actual,source_sha256='a'*64,observed_at='2026-10-27T16:00:00+08:00')
    assert receipt['status']=='MATCH' and receipt['actual']['model_calls']==12
    assert facts['metrics']['a4_team_model_attempt_count']==12


@pytest.mark.parametrize('field',['a3_plan_count','a4_monitor_observation_count','a4_trade_signal_count',
    'a4_team_model_attempt_count','a4_team_fill_count','a4_stock_minute_count'])
def test_each_key_numeric_mismatch_refuses_publication(field):
    facts,actual=original_day()
    facts=deepcopy(facts);facts['metrics'][field]+=1
    with pytest.raises(ValueError,match='A5_KEY_COUNT_LEDGER_MISMATCH'):
        reconcile_counts(facts,actual,source_sha256='a'*64,observed_at='2026-10-27T16:00:00+08:00')
