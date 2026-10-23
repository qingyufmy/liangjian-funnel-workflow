"""Frozen Hot100 source identity and real capture cutoff are separate clocks."""
from copy import deepcopy
from datetime import datetime
from pathlib import Path
import runpy

import pytest

from liangjian_funnel.data.hot100_observation import observe_hot100, observe_snapshot_hot100
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
from liangjian_funnel.runtime.closed_session_capture import build_capture_proof
from liangjian_funnel.pipeline.deterministic import screen_a2
from liangjian_funnel.pipeline.research.common import (
    _project_prompt_value, _with_daily_emotion_overlay, _sha256_json,
)
from test_wp4_hot100_consumers import full_source, gate_input, records_hash


def frozen(*, capture="2026-10-23T03:07:58+08:00", source_day="2026-10-22"):
    stamp = datetime.fromisoformat(capture)
    source = full_source(at=datetime.fromisoformat(source_day + "T15:10:04+08:00"), members=["600000.SH"])
    snapshot, output = gate_input(source)
    snapshot["snapshot_manifest"]["as_of"] = stamp.isoformat()
    snapshot["CLOSED_SESSION_CAPTURE"] = build_capture_proof(source_trade_date=source_day,
        started_at=stamp, captured_at=stamp, completed_at=stamp, calendar=ExchangeTradingCalendar())
    snapshot["MARKET_DATA_AS_OF"] = snapshot["CLOSED_SESSION_CAPTURE"]["source_market_close_at"]
    return snapshot, output


@pytest.mark.parametrize("capture,source_day", [
    ("2026-10-23T03:07:58+08:00", "2026-10-22"),
    ("2026-10-25T14:00:00+08:00", "2026-10-23"),
    ("2026-10-08T03:00:00+08:00", "2026-09-30"),
])
def test_valid_closed_session_uses_market_day_and_actual_capture(capture, source_day):
    snapshot, _ = frozen(capture=capture, source_day=source_day)
    original = deepcopy(snapshot)
    assert observe_snapshot_hot100(snapshot).complete
    assert not observe_hot100(snapshot["EASTMONEY_HOT100_SNAPSHOT"], decision_as_of=capture).complete
    assert snapshot == original


@pytest.mark.parametrize("case", ["missing_market", "market", "manifest", "authority", "source",
    "target", "capture", "deadline", "null", "naive", "holiday_source"])
def test_present_invalid_proof_never_falls_back_to_same_day(case):
    snapshot, _ = frozen()
    proof = snapshot["CLOSED_SESSION_CAPTURE"]
    if case == "missing_market": snapshot.pop("MARKET_DATA_AS_OF")
    elif case == "market": snapshot["MARKET_DATA_AS_OF"] = "2026-10-21T15:00:00+08:00"
    elif case == "manifest": snapshot["snapshot_manifest"]["as_of"] = "2026-10-23T04:00:00+08:00"
    elif case == "authority": proof["authority"] = "fake"
    elif case == "source": proof["source_trade_date"] = "2026-10-21"
    elif case == "target": proof["target_trade_date"] = "2026-10-26"
    elif case == "capture": proof["capture_as_of"] = "2026-10-23T04:00:00+08:00"
    elif case == "deadline": proof["publication_deadline"] = "2026-10-24T06:30:00+08:00"
    elif case == "null": snapshot["CLOSED_SESSION_CAPTURE"] = None
    elif case == "naive": snapshot["snapshot_manifest"]["as_of"] = "2026-10-23T03:07:58"
    elif case == "holiday_source": proof["source_trade_date"] = "2026-10-01"
    result = observe_snapshot_hot100(snapshot)
    assert result.reason_code == "HOT100_CLOSED_SESSION_CLOCK_UNPROVEN"
    assert result.records == () and result.membership("600000.SH") == "UNKNOWN"


@pytest.mark.parametrize("case", ["99", "duplicate", "hash", "projection", "source_day", "observed_day", "contract"])
def test_closed_session_keeps_full_original_evidence_gates(case):
    snapshot, _ = frozen()
    source = snapshot["EASTMONEY_HOT100_SNAPSHOT"]
    if case == "99": source["records"].pop()
    elif case == "duplicate":
        source["records"][-1]["symbol"] = source["records"][0]["symbol"]
        source["content_hash"] = records_hash(source["records"])
    elif case == "hash": source["content_hash"] = "0" * 64
    elif case == "projection": source["projection_scope"] = "TOP10_PLUS_BATCH_MATCHES"
    elif case == "source_day": source["trade_date"] = "2026-10-21"
    elif case == "observed_day": source["as_of"] = "2026-10-23T02:00:00+08:00"
    elif case == "contract": source["point_in_time"] = False
    result = observe_snapshot_hot100(snapshot)
    assert not result.complete and result.records == ()


def test_actual_capture_future_guard_is_not_market_close_cutoff():
    snapshot, _ = frozen(capture="2026-10-22T15:09:00+08:00")
    snapshot["as_of"] = "2026-10-22T16:00:00+08:00"
    assert observe_snapshot_hot100(snapshot).reason_code == "HOT100_OBSERVATION_AFTER_DECISION"
    snapshot, _ = frozen()
    assert observe_snapshot_hot100(snapshot).complete  # 15:10 is after close, before actual capture.


def test_calendar_unavailable_fails_closed(monkeypatch):
    snapshot, _ = frozen()
    from liangjian_funnel.runtime.calendar import TradingCalendarError
    def unavailable(self, day):
        raise TradingCalendarError()
    monkeypatch.setattr(ExchangeTradingCalendar, "is_trading_day", unavailable)
    assert observe_snapshot_hot100(snapshot).reason_code == "HOT100_CLOSED_SESSION_CLOCK_UNPROVEN"


def test_no_proof_retains_same_day_and_explicit_none_projection_is_not_full_source():
    snapshot, _ = frozen()
    snapshot.pop("CLOSED_SESSION_CAPTURE")
    assert observe_snapshot_hot100(snapshot).reason_code == "HOT100_TRADE_DATE_MISMATCH"
    snapshot["snapshot_manifest"]["as_of"] = "2026-10-22T15:20:00+08:00"
    assert observe_snapshot_hot100(snapshot).complete
    assert not observe_snapshot_hot100(snapshot, value=None).complete


@pytest.mark.parametrize("valid", [True, False])
def test_four_consumers_agree_and_news_child_clock_cannot_replace_frozen_manifest(valid):
    snapshot, output = frozen()
    if not valid:
        snapshot["CLOSED_SESSION_CAPTURE"]["authority"] = "forged"
    snapshot["as_of"] = "2026-10-23T10:00:00+08:00"  # request-local enriched outer clock
    before = deepcopy((snapshot, output))
    observation = observe_snapshot_hot100(snapshot)
    _, overlay = _with_daily_emotion_overlay(output, snapshot, set(snapshot["g0_symbols"]))
    projected = _project_prompt_value("EASTMONEY_HOT100_SNAPSHOT",
        snapshot["EASTMONEY_HOT100_SNAPSHOT"], {"600000.SH"}, snapshot_data=snapshot)
    gate = screen_a2(snapshot, output, minimum_identifiability_score=0, review_all_eligible=True)
    envelope = dict(snapshot_id="dual-clock", snapshot_hash=_sha256_json(snapshot),
        as_of=snapshot["as_of"], data=snapshot)
    lane = {"stages": [dict(stage="A1", snapshot_id="dual-clock", status="VALIDATED",
        output=output, output_hash=_sha256_json(output))]}
    audit = runpy.run_path(str(Path(__file__).resolve().parents[1] / "scripts/audit_disclosure_batch_offline.py"))
    audited = audit["audit_projected_batch"](envelope, lane)
    assert observation.complete is valid and overlay["available"] is valid
    assert projected["full_snapshot_validated"] is valid
    assert all(row["channel_source_health"]["emotion"]["available"] is valid for row in gate.decisions)
    assert audited["hot100_source_health"]["available"] is valid
    assert set(audited["prefilter"]["source_sets"]["hot100"]) == {r["symbol"] for r in observation.records}
    assert (snapshot, output) == before
