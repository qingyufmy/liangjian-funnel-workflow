"""Use the real A2 gate to audit an independently built disclosure domain.

Synthetic fixtures exercise route contracts, not original-day/model replay.
"""
from datetime import timedelta

from liangjian_funnel.pipeline.a2_features import stock_trend_structure
from liangjian_funnel.pipeline.deterministic import screen_a2
from liangjian_funnel.pipeline.disclosure_scope import (
    audit_disclosure_scope, build_disclosure_prefilter,
)
from test_deterministic_pipeline_v2 import (
    NOW, _complete_a2_factor_scores, _snapshot, _trend_a2_factor_scores,
)


def _bars(up=True):
    values = range(10, 31) if up else range(30, 9, -1)
    return [{"date_ms": int((NOW-timedelta(days=20-i)).timestamp()*1000),
             "close_price": value} for i, value in enumerate(values)]


def _inputs(count):
    snapshot = _snapshot(count)
    symbols = snapshot["g0_symbols"]
    snapshot["A2_SCORE_WEIGHTS"] = {key: 1.0 for key in _complete_a2_factor_scores(90)}
    snapshot["CAPITAL_FLOW_SNAPSHOT"] = {
        "available": True, "by_symbol": {
            symbol: {"available": True, "capital_flow_score": 90} for symbol in symbols}}
    snapshot["MARKET_EMOTION_SNAPSHOT"] = {
        "available": True, "emotion_cycle_stage": "STARTUP", "new_long_permission": "PROBE_ONLY"}
    rows = [{"symbol": symbol, "candidate_id": f"a1:{symbol}",
             "primary_theme": "theme-monthly", "industry_chain_node": "node-monthly",
             "business_exposure": {"revenue_exposure_pct": 65, "source_ref": f"cninfo:{symbol}"},
             "a2_factor_scores": _trend_a2_factor_scores(90), "data_quality_score": 90}
            for symbol in symbols]
    return snapshot, rows


def _materialize(snapshot, daily):
    # Match today's producer contract instead of relying on the older fixture's
    # weekly score as a surrogate for stock-local daily structure.
    snapshot["A2_FACTOR_SNAPSHOT"] = {"available": True, "by_symbol": {
        symbol: {"technical_summary": {"relative_strength_score": 90},
                 "factors": {"stock_trend_structure": stock_trend_structure(bars, NOW.date())}}
        for symbol, bars in daily.items()}}


def test_real_gate_nonempty_emotion_primary_reserve_and_strong_are_retained():
    snapshot, rows = _inputs(5)
    emotion, primary, reserve, strong, negative = snapshot["g0_symbols"]
    daily = {symbol: _bars(symbol != negative) for symbol in snapshot["g0_symbols"]}
    # An emotion anchor can have negative daily structure and still needs disclosure.
    daily[emotion] = _bars(False)
    _materialize(snapshot, daily)
    rows[0]["a2_factor_scores"]["tier_structure"] = {
        "score": 90, "available": True, "availability_state": "OBSERVED_VALUE",
        "first_board_observed": True, "ladder_height": 1, "event_source": "HITHINK_LIMIT_UP_POOL"}
    snapshot["EASTMONEY_HOT100_SNAPSHOT"] = {
        "available": True, "trade_date": NOW.date().isoformat(), "record_count": 100,
        "records": [{"symbol": emotion, "rank": 5}]}
    board = {"available": True, "trade_date": NOW.date().isoformat(), "by_symbol": {
        primary: [{"board_code": "PRIMARY", "strategy_theme_id": "theme-monthly",
                   "board_name": "主方向", "strength": 100, "main_net_inflow_cny": 100,
                   "selected_for_rotation": True, "primary_rank": 1}],
        reserve: [{"board_code": "RESERVE", "strategy_theme_id": "theme-monthly",
                   "board_name": "备用方向", "strength": 80, "main_net_inflow_cny": 100,
                   "selected_for_rotation": False, "rotation_reserve_rank": 1,
                   "rotation_reserve_scope": "RESEARCH_ONLY_NO_AUTOMATIC_ENTRY"}]}}
    snapshot["SELECTED_BOARD_SNAPSHOT"] = board
    # Construct before A2; no decisions/announcements can determine this set.
    scope = build_disclosure_prefilter(symbols=snapshot["g0_symbols"], trade_date=NOW.date(),
        daily=daily, selected_board=board, event_symbols=[emotion],
        event_sources_complete=True, hot_symbols=[emotion])
    gate = screen_a2(snapshot, {"active_research_pool": rows},
                     minimum_identifiability_score=0, review_all_eligible=True)
    decisions = {row["symbol"]: row for row in gate.decisions}
    assert decisions[emotion]["a2_pool_channel"] == "EMOTION"
    assert decisions[primary]["trend_core_eligible"] is True
    assert decisions[reserve]["rotation_reserve_eligible"] is True
    assert decisions[strong]["strong_trend_observation"] is True
    assert decisions[negative]["local_eligible_for_review"] is False
    assert set(gate.review_symbols) == {emotion, primary, reserve, strong}
    assert scope["deferred_symbols"] == [negative]
    assert all(row["uncertainty_retained"] is False for row in scope["records"])
    audit = audit_disclosure_scope(scope, gate.review_symbols, decisions=gate.decisions)
    assert audit["status"] == "COVERED" and audit["missing_symbols"] == []
    for name, symbol in (("EMOTION", emotion), ("PRIMARY_TREND", primary),
                         ("RESERVE", reserve), ("STRONG_TREND_OBSERVATION", strong)):
        assert symbol in audit["route_coverage"][name]["symbols"]
    assert audit["execution_authority"] is False and audit["changes_query_scope"] is False


def test_real_full_market_fallback_is_not_vacuous_legacy_coverage():
    snapshot, rows = _inputs(1)
    symbol = snapshot["g0_symbols"][0]
    daily = {symbol: _bars()}
    _materialize(snapshot, daily)
    snapshot["THS_INDUSTRY_MEMBERSHIP"]["records"][0]["memberships"] = [
        {"industry_thscode": "884001.TI", "industry_name": "算力设备"}]
    snapshot["A2_THEME_METRICS"] = {"theme_metrics": {"INDUSTRY:884001.TI": {
        "available": True, "taxonomy": "INDUSTRY", "taxonomy_code": "884001.TI",
        "taxonomy_name": "算力设备", "score": 88, "breadth": 0.70,
        "turnover_share": 0.08, "weekly_confirmation_score": 82}}}
    snapshot["A2_SECTOR_HEALTH_SNAPSHOT"] = {"by_taxonomy": {"industry": {"sectors": [{
        "taxonomy_code": "884001.TI", "taxonomy_name": "算力设备", "capital_flow": {
            "available": True, "source": "EASTMONEY_BOARD_CAPITAL_FLOW",
            "windows": {"today": {"main_net_cny": 2_467_000_000, "change_pct": 1.2}}}}]}}}
    rows[0].update(primary_theme="theme-compute", industry_chain_node="node-compute-device")
    scope = build_disclosure_prefilter(symbols=[symbol], trade_date=NOW.date(), daily=daily,
        selected_board={}, event_symbols=[], event_sources_complete=True)
    gate = screen_a2(snapshot, {"active_research_pool": rows, "taxonomy_links": [{
        "node_id": "node-compute-device", "taxonomy": "INDUSTRY", "taxonomy_code": "884001.TI"}]},
        minimum_identifiability_score=0, review_all_eligible=True)
    assert gate.review_symbols == (symbol,)
    assert gate.decisions[0]["rotation_input_source"] == "FULL_MARKET_ROTATION_FALLBACK"
    audit = audit_disclosure_scope(scope, gate.review_symbols, decisions=gate.decisions)
    assert audit["status"] == "COVERED"
    # This fallback is a TREND row, not an a2_pool_channel=LEGACY row.
    assert audit["route_coverage"]["LEGACY"]["count"] == 0
    assert audit["route_coverage"]["FULL_MARKET_FALLBACK"]["symbols"] == [symbol]


def test_legacy_channel_is_separately_named_without_granting_scope_authority():
    snapshot, rows = _inputs(1)
    symbol = snapshot["g0_symbols"][0]
    daily = {symbol: _bars()}
    _materialize(snapshot, daily)
    scope = build_disclosure_prefilter(symbols=[symbol], trade_date=NOW.date(),
        daily=daily, selected_board={}, event_symbols=[],
        event_sources_complete=True)
    gate = screen_a2(snapshot, {"active_research_pool": rows},
                     minimum_identifiability_score=0, review_all_eligible=True)
    assert gate.review_symbols == (symbol,)
    assert gate.decisions[0]["a2_pool_channel"] == "LEGACY"
    audit = audit_disclosure_scope(scope, gate.review_symbols, decisions=gate.decisions)
    assert audit["route_coverage"]["LEGACY"]["symbols"] == [symbol]
    assert audit["execution_authority"] is False
