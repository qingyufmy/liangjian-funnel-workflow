from copy import deepcopy
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.pipeline.early_discovery import discover_early_setups, attach_discovery_queue, recheck_a1_discovery

TZ = ZoneInfo("Asia/Shanghai")


def bars(prices):
    start = datetime(2026, 5, 1, 15, tzinfo=TZ)
    return [{"date": (start + timedelta(days=i)).isoformat(), "open": p, "high": p + .1,
             "low": p - .1, "close": p, "volume": 10000, "adjust_mode": "qfq"}
            for i, p in enumerate(prices)]


def repair_bars():
    return bars([20.] * 45 + [20. - i * .5 for i in range(20)] + [11., 12., 14., 16.])


def scan(rows, **kwargs):
    return discover_early_setups({"603186.SH": rows}, as_of=datetime.fromisoformat(rows[-1]["date"]),
                                 symbols=["603186.SH"], **kwargs)


def test_repair_found_before_complete_bull_stack_no_order_authority():
    rows = repair_bars()
    original = deepcopy(rows)
    result = scan(rows)
    assert rows == original
    assert result["records"][0]["signals"] == ["REPAIR_WATCH"]
    assert result["records"][0]["execution_permission"] == "BLOCKED"
    assert result["records"][0]["evidence"]["ma"][5] < result["records"][0]["evidence"]["ma"][20]


def test_watch_persists_after_cross_and_fails_on_ma20_break():
    prices = [10 + i * .1 for i in range(65)] + [16.7, 16.8, 14., 18.5, 18.6]
    result = scan(bars(prices))
    assert result["records"][0]["event_state"] == "CONTINUING_WATCH"
    assert result["records"][0]["evidence"]["observation_age_sessions"] == 1
    assert scan(bars([*prices, 12.]))["records"] == []
    assert scan(bars([*prices, 12., 17.]))["records"] == []


@pytest.mark.parametrize("prices", [[10.] * 80, [30 - i * .2 for i in range(80)]])
def test_flat_or_falling_negative_controls(prices):
    assert scan(bars(prices))["records"] == []


def test_restart_after_pullback_and_failed_breakout_control():
    prices = [10 + i * .1 for i in range(65)] + [16.7, 16.8, 14., 18.5]
    positive = scan(bars(prices))
    assert "TREND_RESTART_WATCH" in positive["records"][0]["signals"]
    assert scan(bars([*prices[:-1], 13.]))["records"] == []


def test_future_bars_do_not_change_closed_discovery_and_duplicates_fail_closed():
    rows = repair_bars()
    cutoff = datetime.fromisoformat(rows[-1]["date"])
    expected = scan(rows)
    future = {**rows[-1], "date": (cutoff + timedelta(days=1)).isoformat(), "close": 100., "high": 101.}
    assert discover_early_setups({"603186.SH": [*rows, future]}, as_of=cutoff, symbols=["603186.SH"]) == expected
    result = scan([*rows, rows[-1]])
    assert not result["records"]
    assert "DUPLICATE_DAILY_BAR" in result["data_gaps"][0]["reasons"]


def test_intraday_never_consumes_todays_daily_close():
    rows = repair_bars()
    cutoff = datetime.fromisoformat(rows[-1]["date"]).replace(hour=10)
    result = discover_early_setups({"603186.SH": rows}, as_of=cutoff, symbols=["603186.SH"])
    assert all(row["evidence"]["bar_end"] < cutoff.isoformat() for row in result["records"])


def test_stale_adjustment_and_missing_history_are_explicit_gaps():
    rows = repair_bars()
    cutoff = datetime.fromisoformat(rows[-1]["date"]) + timedelta(days=1)
    stale = discover_early_setups({"603186.SH": rows}, as_of=cutoff, symbols=["603186.SH"])
    assert stale["data_gaps"][0]["reasons"] == ["DAILY_REFERENCE_STALE"]
    rows[-1]["adjust_mode"] = "hfq"
    assert scan(rows)["data_gaps"][0]["reasons"] == ["ADJUSTMENT_BASIS_UNRESOLVED"]
    assert scan(rows[-20:])["data_gaps"][0]["reasons"] == ["DAILY_HISTORY_INSUFFICIENT"]


def test_budget_retains_overflow_without_admission_and_is_order_stable():
    rows = repair_bars()
    result = discover_early_setups({s: rows for s in ["B", "A"]},
        as_of=datetime.fromisoformat(rows[-1]["date"]), symbols=["B", "A"], limit=1)
    assert len(result["records"]) == 2
    assert [r["review_budget_selected"] for r in result["records"]] == [True, False]
    original = {"active_research_pool": [], "monitor_pool": [], "rejected_candidates": []}
    bound = attach_discovery_queue(original, result)
    assert bound["active_research_pool"] == []
    assert "early_discovery" not in original
    assert bound["early_discovery"]["evidence_recheck_symbols"] == ["A"]


def test_recheck_never_admits_undated_snapshot():
    original = {"active_research_pool": [], "monitor_pool": [], "rejected_candidates": []}
    result = recheck_a1_discovery(original, {"EARLY_DISCOVERY_SNAPSHOT": scan(repair_bars())})
    assert not result["active_research_pool"]
    assert result["early_discovery"]["recheck_blocker"] == "SNAPSHOT_CUTOFF_UNAVAILABLE"


def test_recheck_prunes_future_and_undated_evidence_before_existing_a1_gate(monkeypatch):
    from liangjian_funnel.pipeline import deterministic
    from types import SimpleNamespace
    captured = {}
    def fake_gate(snapshot, output):
        captured.update(snapshot)
        return SimpleNamespace(decisions=())
    monkeypatch.setattr(deterministic, "screen_a1", fake_gate)
    cutoff = datetime(2026, 8, 10, 16, tzinfo=TZ)
    original = {"active_research_pool": [], "monitor_pool": [], "rejected_candidates": []}
    source = {"snapshot_manifest": {"as_of": cutoff.isoformat()},
        "EARLY_DISCOVERY_SNAPSHOT": scan(repair_bars()),
        "COMPANY_FUNDAMENTALS": {"603186.SH": {"statements": {"INCOME": [
            {"report_date_ms": (cutoff + timedelta(days=20)).timestamp() * 1000},
            {"profit": 999}, {"report_date_ms": (cutoff - timedelta(days=20)).timestamp() * 1000} ]}}},
        "MAIN_BUSINESS_EVIDENCE": {"603186.SH": {"available": True, "evidence": [
            {"publish_time": "2026-08-30", "source_ref": "future"},
            {"source_ref": "undated"}, {"publish_time": "2026-08-01", "source_ref": "known"}]}}}
    frozen = deepcopy(source)
    result = recheck_a1_discovery(original, source)
    assert source == frozen
    assert len(captured["COMPANY_FUNDAMENTALS"]["603186.SH"]["statements"]["INCOME"]) == 1
    assert captured["MAIN_BUSINESS_EVIDENCE"]["603186.SH"]["evidence"] == [{"publish_time": "2026-08-01", "source_ref": "known"}]
    assert result["active_research_pool"] == []


def test_verified_delta_has_lineage_dedup_and_does_not_rewrite_sealed_pool(monkeypatch):
    from liangjian_funnel.pipeline import deterministic
    from types import SimpleNamespace
    admitted = {"symbol": "603186.SH", "research_route": "HALF_YEAR_FUNDAMENTAL",
                "downstream_trade_eligible": True, "half_year_support": {"supported": True, "report_date_ms": 1}}
    monkeypatch.setattr(deterministic, "screen_a1", lambda *a, **k: SimpleNamespace(decisions=(
        {"symbol": "603186.SH", "reason_codes": ["A1_HALF_YEAR_FUNDAMENTAL_CONFIRMED"]},)))
    monkeypatch.setattr(deterministic, "local_active_items", lambda gate: [dict(admitted)])
    original = {"active_research_pool": [{"symbol": "603186.SH", "research_route": "DAILY_EMOTION_OVERLAY"}],
                "monitor_pool": [{"symbol": "603186.SH"}], "rejected_candidates": []}
    frozen = deepcopy(original)
    result = recheck_a1_discovery(original, {"snapshot_manifest": {"as_of": "2026-09-16T15:30:00+08:00"},
        "EARLY_DISCOVERY_SNAPSHOT": scan(repair_bars())})
    assert original == frozen
    assert len(result["active_research_pool"]) == 1
    assert not result["monitor_pool"]
    delta = result["active_research_pool"][0]["daily_verified_increment"]
    assert not delta["monthly_generation_mutated"] and delta["evidence_hash"]
    assert result["early_discovery"]["admitted_symbols"] == ["603186.SH"]


def test_strong_observation_is_handed_to_a3_but_cannot_publish():
    from liangjian_funnel.pipeline.research import _build_a3_candidate_domain, _apply_a3_candidate_origin_policy
    row = {"symbol": "603186.SH", "stock_behavior_type": "TREND", "route_permission": ["TREND_MA5", "MA520_SWING"],
           "route": "MARKET_CORE", "top_rotation_theme": False, "strong_trend_observation": True,
           "research_observation_scope": "RESEARCH_ONLY_NO_AUTOMATIC_ENTRY", "data_sufficiency_state": "SUFFICIENT",
           "execution_permission": "BLOCKED"}
    domain, origins = _build_a3_candidate_domain({"focus_pool": [], "watch_only_pool": [row]})
    assert len(domain["focus_pool"]) == 1
    output, _ = _apply_a3_candidate_origin_policy({"core_watch_pool": [{"symbol": "603186.SH", "eligibility": "QUALIFIED", "review_status": "PASS"}]},
        {"A3_CANDIDATE_ORIGIN": origins, "A2_BOTTLENECK_CONTEXT": {"603186.SH": row}})
    assert not output["core_watch_pool"]
    watch = output["secondary_watch_pool"][0]
    assert watch["risk_unit"] == "NO_ENTRY" and watch["execution_permission"] == "BLOCKED"
    assert watch["strong_trend_observation"] is True
    assert not watch.get("rotation_reserve_eligible")


def test_new_a2_qualifications_survive_frozen_context_projection():
    from liangjian_funnel.pipeline.research import _with_a2_bottleneck_context, FrozenInputSnapshot
    from liangjian_funnel.pipeline.deterministic import DeterministicGateResult
    row = {"symbol": "603186.SH", "bottleneck_context": {}, "stock_behavior_type": "EMOTION",
           "route_permission": ["LEADER_INTRADAY", "TREND_MA5"], "independent_strategy_review": True,
           "research_route_qualifications": {"TREND_MA5": {"eligible": True}}, "execution_permission": "BLOCKED"}
    gate = DeterministicGateResult(stage="A2_LOCAL_ROLE", decisions=(row,), review_symbols=("603186.SH",), monitor_symbols=(), rejected_symbols=())
    snapshot = FrozenInputSnapshot("test", {}, "hash", datetime(2026, 9, 16, 15, tzinfo=TZ))
    output = _with_a2_bottleneck_context(snapshot, gate).data["A2_BOTTLENECK_CONTEXT"]["603186.SH"]
    assert output["independent_strategy_review"] is True
    assert output["research_route_qualifications"] == row["research_route_qualifications"]
    assert output["execution_permission"] == "BLOCKED"
