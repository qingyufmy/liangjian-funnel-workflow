"""0222 R1/R2: isolated fixture databases; no historical/prod repair."""
import json
import sqlite3
from datetime import timedelta
from types import SimpleNamespace

import pytest

from test_h1_entry_settlement_invariants import at, seeded
from liangjian_funnel.data.cache import MinuteBarStore
from liangjian_funnel.data.session_windows import closed_window_ends
from liangjian_funnel.runtime.state import MonitorAction, PlanStatus
from liangjian_funnel.workflow import WorkflowApplication
from liangjian_funnel.review.daily import build_a5_fact_snapshot, A5ReviewKind


def label(store, plan, day):
    return store.record_outcome_label(dict(run_id="fixture", lane_id="lane_1",
        trade_date=day, stage="A4", symbol="600621.SH", decision="PASSED",
        snapshot_id="fixture", config_hash="fixture", metadata={"plan_id": plan}))


def test_r1_today_conflict_does_not_quarantine_historical_closed_trade(tmp_path):
    store, app, bar = seeded(tmp_path)
    yesterday = at("10:00:00") - timedelta(days=1)
    store.create_execution_plan("historic", "lane_1", "600621.SH", status=PlanStatus.INVALIDATED,
        valid_from=yesterday, expires_at=yesterday.replace(hour=15), payload={})
    store.record_monitor_event(event_key="historic-entry", lane_id="lane_1", minute_end=yesterday,
        action=MonitorAction.BUY_SIGNAL, effective=True, sync_a4_lifecycle=True,
        payload={"plan_id": "historic", "symbol": "600621.SH"})
    # Fixture-only historical completed projection, deliberately same account/symbol.
    with sqlite3.connect(store.path) as c:
        c.execute("UPDATE a4_signal_lifecycles SET status='CLOSED',entry_qty=100,exit_qty=100,remaining_qty=0 WHERE entry_event_key='historic-entry'")
        c.execute("CREATE TRIGGER fault BEFORE UPDATE ON a4_signal_lifecycles WHEN OLD.plan_id='p1' BEGIN SELECT RAISE(ABORT,'LOCAL_FAULT'); END")
    original = store.get_a4_signal_lifecycle("historic-entry")
    good_label = label(store, "historic", "2026-10-15")
    _, reason = WorkflowApplication._settle_prior_signals_isolated(app, "lane_1", "600621.SH", bar,
        observed_at=at("11:19:00"))
    assert reason == "STATE_CONFLICT_QUARANTINED"
    with sqlite3.connect(store.path) as c:
        assert c.execute("SELECT plan_id FROM a4_lifecycle_reconciliations").fetchall() == [("p1",)]
    assert store.get_a4_signal_lifecycle("historic-entry") == original
    assert store.list_outcome_labels(stage="A4") == (good_label,)
    facts = build_a5_fact_snapshot(store, tmp_path, trade_date=yesterday.date(),
        cutoff_at=yesterday.replace(hour=15), review_kind=A5ReviewKind.POST_CLOSE, lane_id="lane_1")
    historical = next(row for row in facts["a4"]["lifecycles"] if row["plan_id"] == "historic")
    assert historical["performance_eligible"] is True
    # Risk-side same-symbol quarantine remains deliberately stricter than statistics.
    assert store.get_a4_plan_quarantine("historic") is not None


def archive_day(tmp_path, bar, *, omit_capture=None, extra_missing=None, include_entry=False):
    minutes = MinuteBarStore(tmp_path / "minute")
    captures = [at("09:32:00") + timedelta(minutes=i) for i in range(119)]
    captures += [at("13:01:00") + timedelta(minutes=i) for i in range(120)]
    for clock in captures:
        if clock == omit_capture:
            continue
        bars = [bar.model_copy(update={"bar_end": end})
            for end in closed_window_ends(clock - timedelta(minutes=1), "1m")
            if (include_entry or end != bar.bar_end) and end != extra_missing]
        from unittest.mock import patch
        from liangjian_funnel.data.cache import _observed_at
        with patch("liangjian_funnel.data.cache._observed_at", side_effect=lambda value:
                (clock + timedelta(seconds=5)).isoformat() if value is None else _observed_at(value)):
            minutes.write_live(bars, as_of=clock, snapshot_id=f"minute-{clock.strftime('%Y%m%dT%H%M%S%z')}")
    return minutes


def finalize(store, minutes, now=None):
    from liangjian_funnel.runtime.entry_close import finalize_pending_entries
    return finalize_pending_entries(store, minutes, now=now or at("20:15:00"))


def test_r2_complete_capture_ledger_missing_entry_is_data_error_not_unfilled(tmp_path):
    store, _, bar = seeded(tmp_path)
    raw_label = label(store, "p1", "2026-10-16")
    minutes = archive_day(tmp_path, bar)
    before = store.get_account("paper:lane_1")["cash"]
    result = finalize(store, minutes)
    assert result["closed"] == 1 and result["unresolved"] == 0
    life = store.get_a4_signal_lifecycle("h1-entry")
    assert life["status"] == "DATA_ERROR" and life["exit_reason"] == "ENTRY_WINDOW_DATA_MISSING"
    with sqlite3.connect(store.path) as c:
        row = c.execute("SELECT result,reason_code,evidence_json FROM a4_entry_matching_attempts").fetchone()
    assert row[:2] == ("DATA_MISSING", "ENTRY_WINDOW_DATA_MISSING")
    assert json.loads(row[2])["capture_count"] == 239
    assert store.list_outcome_labels(stage="A4") == ()
    assert store.list_outcome_labels(stage="A4", include_quarantined=True) == (raw_label,)
    assert store.list_fills("paper:lane_1") == () and store.get_account("paper:lane_1")["cash"] == before
    facts = build_a5_fact_snapshot(store, tmp_path, trade_date=at("15:00:00").date(),
        cutoff_at=at("15:00:00"), review_kind=A5ReviewKind.POST_CLOSE, lane_id="lane_1")
    assert facts["a4"]["lifecycles"][0]["performance_eligible"] is False


def test_r2_closed_missing_entry_does_not_hang_or_reopen_after_restart_next_day(tmp_path):
    store, _, bar = seeded(tmp_path)
    minutes = archive_day(tmp_path, bar)
    finalize(store, minutes)
    from liangjian_funnel.runtime.state import RuntimeStore
    restarted = RuntimeStore(store.path)
    result = finalize(restarted, minutes, now=at("20:15:00") + timedelta(days=3))
    assert result["closed"] == 0
    assert restarted.get_a4_signal_lifecycle("h1-entry")["status"] == "DATA_ERROR"
    with sqlite3.connect(store.path) as c:
        assert c.execute("SELECT count(*) FROM a4_entry_matching_attempts").fetchone()[0] == 1


@pytest.mark.parametrize("case", ["capture_gap", "other_bar_gap", "entry_found", "late_archive", "hash_corrupt", "not_due"])
def test_r2_incomplete_or_positive_evidence_never_forces_terminal(tmp_path, capsys, case):
    store, _, bar = seeded(tmp_path)
    minutes = archive_day(tmp_path, bar, omit_capture=at("13:01:00") if case == "capture_gap" else None,
        extra_missing=at("10:00:00") if case == "other_bar_gap" else None,
        include_entry=case == "entry_found")
    if case == "late_archive":
        from unittest.mock import patch
        from liangjian_funnel.data.cache import _observed_at
        with patch("liangjian_funnel.data.cache._observed_at", side_effect=lambda value:
                at("15:05:00").isoformat() if value is None else _observed_at(value)):
            minutes.write_live([bar], as_of=at("15:00:00"))
    if case == "hash_corrupt":
        with sqlite3.connect(minutes.path) as c:
            c.execute("UPDATE minute_decision_snapshots SET payload_sha256='bad' WHERE decision_as_of=?", (at("15:00:00").isoformat(),))
    result = finalize(store, minutes, now=at("14:59:00") if case == "not_due" else None)
    assert result["closed"] == 0
    assert store.get_a4_signal_lifecycle("h1-entry")["status"] == "SIGNALLED"
    assert store.list_fills("paper:lane_1") == ()
    if case != "not_due":
        assert result["unresolved"] == 1
        assert "A4_ENTRY_WINDOW_UNRESOLVED" in capsys.readouterr().err


def test_r2_terminal_and_matching_proof_commit_in_one_transaction(tmp_path):
    store, _, bar = seeded(tmp_path)
    minutes = archive_day(tmp_path, bar)
    with sqlite3.connect(store.path) as c:
        c.execute("CREATE TRIGGER fault BEFORE UPDATE ON a4_signal_lifecycles BEGIN SELECT RAISE(ABORT,'LOCAL_FAULT'); END")
    with pytest.raises(Exception, match="LOCAL_FAULT|persist|PERSIST|database|STATE"):
        finalize(store, minutes)
    assert store.get_a4_signal_lifecycle("h1-entry")["status"] == "SIGNALLED"
    with sqlite3.connect(store.path) as c:
        assert c.execute("SELECT count(*) FROM a4_entry_matching_attempts").fetchone()[0] == 0


def test_r2_actual_post_close_entrypoint_finalizes_before_review_without_model_or_network(tmp_path, monkeypatch):
    import liangjian_funnel.workflow as workflow
    from datetime import datetime
    store, _, bar = seeded(tmp_path)
    minutes = archive_day(tmp_path, bar)
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return at("20:15:00")
    monkeypatch.setattr(workflow, "datetime", Clock)
    monkeypatch.setattr(workflow, "_primary_model_for_settings", lambda _: ("fixture", 0, "lane_1"))
    monkeypatch.setattr(workflow, "A5IndependentVerifier", lambda **_: None)
    class Review:
        def __init__(self, **kwargs):
            assert kwargs["store"].get_a4_signal_lifecycle("h1-entry")["status"] == "DATA_ERROR"
        def run(self, **kwargs):
            return kwargs["close_archive"]["entry_window_reconciliation"]
    monkeypatch.setattr(workflow, "A5DailyReviewService", Review)
    app = SimpleNamespace(store=store, minute_store=minutes,
        trading_calendar=SimpleNamespace(is_trading_day=lambda _: True),
        collect_close_finalization=lambda **_: {"status": "FIXTURE_NO_NETWORK"},
        settings=SimpleNamespace(workflow_output_dir=tmp_path, fact_store_dir=tmp_path, review_model="fixture"),
        prompts=None, review_model_client=None, fact_cache=None, market_data=SimpleNamespace(), lark_publisher=None)
    result = WorkflowApplication.run_a5_review(app, A5ReviewKind.POST_CLOSE, now=at("20:15:00"))
    assert result["closed"] == 1 and result["unresolved"] == 0
