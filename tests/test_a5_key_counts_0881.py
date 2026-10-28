from copy import deepcopy
import json
from pathlib import Path
import pytest
from liangjian_funnel.review.key_counts import reconcile_counts,technical_opportunities,review_attempt_count

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


def test_real_78_minute_echoes_are_18_stock_5m_opportunities_not_74_or_8():
    rows=json.loads((F/'technical_buy_closed5m_originals.json').read_bytes())['rows']
    full=technical_opportunities(rows)
    midday=technical_opportunities([row for row in rows if row['minute_end']<'2026-10-27T12'])
    assert len(rows)==78 and full['count']==18 and midday['count']==8
    assert not full['unavailable_event_ids']
    facts,actual=original_day();actual['technical_opportunities']=18
    facts['metrics']['a4_technical_opportunity_count']=18
    receipt=reconcile_counts(facts,actual,source_sha256='a'*64,observed_at='2026-10-27T16:00:00+08:00',technical_evidence=full)
    assert reconcile_counts(facts,receipt['actual'],source_sha256=receipt['source_sha256'],observed_at=receipt['observed_at'],technical_evidence=receipt['technical_opportunity_evidence'])==receipt
    facts['metrics']['a4_technical_opportunity_count']=74
    with pytest.raises(ValueError,match='A5_KEY_COUNT_LEDGER_MISMATCH'):
        reconcile_counts(facts,actual,source_sha256='a'*64,observed_at='2026-10-27T16:00:00+08:00',technical_evidence=full)


def test_missing_original_bar_or_technical_action_is_unavailable_not_zero():
    row=dict(event_id='original',plan_id='p',minute_end='2026-10-27T10:06:00+08:00',technical_action='BUY_SIGNAL')
    assert technical_opportunities([row])['count'] is None
    row['technical_action']=None
    assert technical_opportunities([row])['count'] is None
    assert technical_opportunities([])['count']==0


def test_midday_retry_cannot_count_later_afternoon_model_attempts():
    rows=[('team:20261027T1126:reviewed:bd_original',1),('team:20261027T1306:reviewed:bd_original',0)]
    assert review_attempt_count(rows,'2026-10-27T11:30:00+08:00')==1
    assert review_attempt_count(rows,'2026-10-27T15:00:00+08:00')==2
    assert review_attempt_count([('unknown-clock',1)],'2026-10-27T11:30:00+08:00') is None
