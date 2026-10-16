"""Actual SQLite state: restart-safe daily HALT, idempotency and isolation."""
from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
import hashlib
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from liangjian_funnel.runtime.risk_engine import RiskEnvelope
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
from liangjian_funnel.pipeline.team_v2_judgments import ModelJudgmentJournal


AT = datetime.fromisoformat('2026-10-19T08:35:00+08:00')
E = RiskEnvelope('p','000001.SZ','2026-10-19',True,'STANDARD',Decimal('30000'),2,2000,(),(),purpose='PLANNING')


def record(store, key, *, good=False, day='2026-10-19', account='paper:team', at=AT, **changes):
    output = {'session':'PREMARKET','envelope_adjustments':[{'plan_id':'p','adjustment':'KEEP'}],
              'utilization_cap':{'cap_pct':75}} if good else None
    kwargs = dict(meeting_id=key,source_call_id='source:'+key,account_id=account,trade_date=day,
        observed_at=at,envelopes=(replace(E,trade_date=day),),output=output,validated=good,
        engine_utilization_cap=Decimal('75'))
    return store.record_premarket(**{**kwargs,**changes})


def test_restart_cannot_clear_two_failures_and_later_success_cannot_unhalt_today(tmp_path):
    path=tmp_path/'team.sqlite3';store=TeamMeetingStore(path)
    assert record(store,'one').status=='DEGRADED'
    store=TeamMeetingStore(path)
    assert record(store,'two',at=AT+timedelta(seconds=20)).status=='HALT'
    assert record(TeamMeetingStore(path),'three',good=True,at=AT+timedelta(seconds=40)).status=='HALT'


def test_same_model_call_repolled_with_new_meeting_id_does_not_count_failure_twice(tmp_path):
    store=TeamMeetingStore(tmp_path/'team.sqlite3')
    first=record(store,'first',source_call_id='same-original-model-call')
    again=record(store,'poll-again',source_call_id='same-original-model-call',at=AT+timedelta(seconds=1))
    assert first==again and again.consecutive_failures==1


def test_same_meeting_reuse_checks_original_binding_and_preserves_evidence(tmp_path):
    store=TeamMeetingStore(tmp_path/'team.sqlite3');first=record(store,'first')
    assert record(store,'first',at=AT+timedelta(seconds=1))==first
    with pytest.raises(ValueError,match='BINDING_CONFLICT'):record(store,'first',good=True)


def test_account_and_next_trading_day_failures_are_separate(tmp_path):
    store=TeamMeetingStore(tmp_path/'team.sqlite3')
    record(store,'one');record(store,'two')
    assert record(store,'other',good=True,account='paper:other').status=='READY'
    assert record(store,'next',good=True,day='2026-10-20',at=AT+timedelta(days=1)).status=='READY'


def test_late_result_never_replaces_newer_meeting_state(tmp_path):
    store=TeamMeetingStore(tmp_path/'team.sqlite3');record(store,'now',at=AT+timedelta(seconds=2))
    with pytest.raises(ValueError,match='OUT_OF_ORDER'):record(store,'late')


def test_mid_transaction_failure_rolls_back_meeting_and_day_latch_together(tmp_path):
    store=TeamMeetingStore(tmp_path/'team.sqlite3')
    with sqlite3.connect(store.path) as db:
        db.execute("CREATE TRIGGER fail BEFORE INSERT ON risk_day_state BEGIN SELECT RAISE(ABORT,'LOCAL_FAULT'); END")
    with pytest.raises(sqlite3.IntegrityError):record(store,'first')
    with sqlite3.connect(store.path) as db:
        assert db.execute('SELECT count(*) FROM risk_meetings').fetchone()[0]==0
        assert db.execute('SELECT count(*) FROM risk_day_state').fetchone()[0]==0


@pytest.mark.parametrize('factory',[TeamMeetingStore,ModelJudgmentJournal])
def test_research_and_meeting_stores_refuse_a_production_shaped_database_without_any_write(tmp_path,factory):
    path=tmp_path/'legacy.sqlite3'
    with sqlite3.connect(path) as db:db.execute('CREATE TABLE execution_plans(plan_id TEXT PRIMARY KEY)')
    pin=hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(ValueError,match='DATABASE_SCOPE_INVALID'):factory(path)
    assert hashlib.sha256(path.read_bytes()).hexdigest()==pin


def test_source_envelopes_and_trade_date_must_match_without_creating_rows(tmp_path):
    store=TeamMeetingStore(tmp_path/'team.sqlite3')
    with pytest.raises(ValueError,match='ENVELOPE_DATE_MISMATCH'):
        record(store,'wrong',envelopes=(replace(E,trade_date='2026-10-18'),))
    with sqlite3.connect(store.path) as db:assert db.execute('SELECT count(*) FROM risk_meetings').fetchone()[0]==0


@pytest.mark.parametrize('same_call',[True,False])
def test_concurrent_results_neither_lose_failure_count_nor_double_count_one_call(tmp_path,same_call):
    store=TeamMeetingStore(tmp_path/'team.sqlite3');barrier=Barrier(2)
    def apply(index):
        barrier.wait()
        return record(store,'same' if same_call else str(index))
    with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(apply,range(2)))
    assert sorted(row.consecutive_failures for row in results)==([1,1] if same_call else [1,2])
    with sqlite3.connect(store.path) as db:
        assert db.execute('SELECT consecutive_failures,halted FROM risk_day_state').fetchone()==((1,0) if same_call else (2,1))
