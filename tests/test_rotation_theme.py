"""Contract tests for the independent free-data rotation-theme module.

The tests deliberately inject every provider boundary.  They exercise the
point-in-time and fail-closed contracts without making a network request.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
import yaml

from liangjian_funnel.data.rotation_theme import (
    CHILD,
    LIANGJIAN_ROTATION_THEME_V1,
    MINIMUM_FACTOR_COVERAGE,
    ROTATION_THEME_CONFIG_SCHEMA,
    ROTATION_THEME_SCHEMA,
    ROTATION_THEME_SOURCE_ID,
    RotationThemeConfigError,
    RotationThemeDataError,
    _content_hash,
    aggregate_tencent_theme_flows,
    build_membership_snapshot,
    build_rotation_theme_snapshot,
    calculate_rotation_strength,
    collect_eastmoney_board_catalog,
    collect_eastmoney_board_flow,
    collect_eastmoney_board_members,
    collect_rotation_theme_snapshot,
    collect_tencent_capital_flow,
    load_membership_snapshot,
    load_rotation_theme_config,
    load_rotation_theme_snapshot,
    validate_rotation_theme_config,
    write_membership_snapshot,
    write_rotation_theme_snapshot,
)


TZ = ZoneInfo("Asia/Shanghai")
DAY = date(2026, 9, 4)
AS_OF = datetime(2026, 9, 4, 15, 0, tzinfo=TZ)
CAPTURE = datetime(2026, 9, 4, 14, 55, tzinfo=TZ)
LATER_DAY = date(2026, 9, 8)
LATER_AS_OF = datetime(2026, 9, 8, 15, 0, tzinfo=TZ)
LATER_CAPTURE = datetime(2026, 9, 8, 14, 55, tzinfo=TZ)


def _registry_payload(*, themes: list[dict] | None = None) -> dict:
    if themes is None:
        themes = [
            {
                "theme_id": "TEST_THEME",
                "name": "测试方向",
                "kind": "PRIMARY",
                "parent": None,
                "eastmoney_board_codes": ["BK0001"],
                "aliases": ["测试方向", "测试主题"],
                "effective_from": "2026-09-01",
                "effective_to": None,
                "evidence": ["test taxonomy evidence"],
            }
        ]
    return {
        "schema_version": ROTATION_THEME_CONFIG_SCHEMA,
        "version": LIANGJIAN_ROTATION_THEME_V1,
        "description": "test taxonomy",
        "themes": themes,
    }


def _write_registry(path: Path, *, themes: list[dict] | None = None) -> Path:
    path.write_text(
        yaml.safe_dump(_registry_payload(themes=themes), allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    return path


def _east_page_for(day: date, captured: datetime, kind: str, value: str, page: int, page_size: int) -> dict:
    """Return a complete, dated Eastmoney-like response for one page."""

    if kind == "members":
        rows = [
            {"f12": "600001", "f14": "甲公司", "f2": 10.0, "f3": 5.0, "f5": 100, "f6": 1000},
            {"f12": "000002", "f14": "乙公司", "f2": 20.0, "f3": 2.0, "f5": 200, "f6": 2000},
        ]
    elif kind == "catalog":
        rows = [{"f12": "BK0001", "f14": "测试方向"}]
    else:
        rows = [{"f12": "BK0001", "f14": "测试方向", "f3": 4.0, "f62": 3000}]
    start = (page - 1) * page_size
    return {
        "total": len(rows),
        "rows": rows[start : start + page_size],
        "captured_at": captured.isoformat(),
    }


def _east_page(kind: str, value: str, page: int, page_size: int) -> dict:
    return _east_page_for(DAY, CAPTURE, kind, value, page, page_size)


def _incomplete_east_page(kind: str, value: str, page: int, page_size: int) -> dict:
    if page == 1:
        return {
            "total": 3,
            "rows": [{"f12": "600001", "f14": "甲公司"}],
            "captured_at": CAPTURE.isoformat(),
        }
    return {"total": 3, "rows": [], "captured_at": CAPTURE.isoformat()}


def _membership(
    *,
    theme_id: str = "TEST_THEME",
    captured: datetime = CAPTURE,
    effective: date = date(2026, 9, 1),
    old_quotes: bool = False,
) -> dict:
    return build_membership_snapshot(
        theme_id=theme_id,
        members=[
            {
                "symbol": "600001.SH",
                "name": "甲公司",
                "latest_price": 1.0 if old_quotes else 10.0,
                "change_pct": -9.0 if old_quotes else 5.0,
                "turnover_cny": 1.0 if old_quotes else 100.0,
            },
            {
                "symbol": "000002.SZ",
                "name": "乙公司",
                "latest_price": 2.0 if old_quotes else 20.0,
                "change_pct": -8.0 if old_quotes else 2.0,
                "turnover_cny": 1.0 if old_quotes else 100.0,
            },
        ],
        captured_at=captured,
        effective_from=effective,
        source="EASTMONEY_TEST",
        pagination_evidence={
            "total": 2,
            "page_size": 100,
            "pages": [{"page": 1, "requested": 100, "returned": 2}],
            "complete": True,
        },
    )


def _metric_row(
    theme_id: str,
    *,
    kind: str = "PRIMARY",
    parent: str | None = None,
    main: float = 100.0,
    fund_coverage: float | None = 1.0,
    flow_coverage: float | None = None,
    price_coverage: float = 1.0,
    eastmoney_main: float | None = 100.0,
    relative: float = 5.0,
    breadth: float = 0.8,
    missing: set[str] | None = None,
) -> dict:
    values = {
        "relative_return_pct": relative,
        "breadth": breadth,
        "momentum_3d_pct": 3.0,
        "momentum_5d_pct": 5.0,
        "leader_structure_score": 0.6,
        "rank_persistence_score": 0.7,
    }
    for factor in missing or set():
        values[factor] = None
    return {
        "theme_id": theme_id,
        "board_name": f"{theme_id}方向",
        "kind": kind,
        "parent": parent,
        "constituents": ["600001.SH", "000002.SZ"],
        "member_count": 2,
        "member_snapshot_complete": True,
        "member_snapshot_trade_date": DAY,
        "price_coverage": price_coverage,
        "turnover_coverage": fund_coverage,
        "flow_coverage": flow_coverage,
        "tencent_main_net_inflow_cny": main,
        "eastmoney_main_net_inflow_cny": eastmoney_main,
        **values,
    }


def _top_level_fetchers(*, day: date = DAY, captured: datetime = CAPTURE, member_fetcher=None, flow_fetcher=None, catalog_fetcher=None):
    page_fetcher = lambda kind, value, page, page_size: _east_page_for(
        day, captured, kind, value, page, page_size
    )
    return {
        "eastmoney_catalog": catalog_fetcher or page_fetcher,
        "eastmoney_flow": flow_fetcher or page_fetcher,
        "eastmoney_members": member_fetcher or page_fetcher,
        "tencent_flow": lambda symbol: {
            "main_net_inflow_cny": 100.0,
            "turnover_cny": 100.0,
            "latest_price": 10.0,
            "change_pct": 5.0,
            "trade_date": day.isoformat(),
        },
        "tencent_capture_time": captured,
        "tencent_quote": lambda symbol: {
            "symbol": symbol,
            "latest_price": 10.0,
            "change_pct": 5.0,
            "turnover_cny": 100.0,
            "trade_date": day.isoformat(),
        },
    }


def _board(result: dict, theme_id: str) -> dict:
    return next(row for row in result["boards"] if row["theme_id"] == theme_id)


def test_default_taxonomy_is_strict_and_parent_child_legal():
    config = load_rotation_theme_config()

    assert config.version == LIANGJIAN_ROTATION_THEME_V1
    assert len(config.themes) >= 15
    primaries = {theme.theme_id for theme in config.themes if theme.kind != CHILD}
    assert primaries
    assert all(
        theme.parent in primaries
        for theme in config.themes
        if theme.kind == CHILD
    )
    codes = [code for theme in config.themes for code in theme.eastmoney_board_codes]
    assert len(codes) == len(set(codes))
    assert all(theme.effective_from <= DAY for theme in config.themes)


def test_taxonomy_rejects_duplicate_code_unknown_parent_and_missing_evidence():
    base = load_rotation_theme_config().as_dict()

    duplicate = json.loads(json.dumps(base, ensure_ascii=False))
    duplicate["themes"][1]["eastmoney_board_codes"] = duplicate["themes"][0]["eastmoney_board_codes"]
    with pytest.raises(RotationThemeConfigError, match="ROTATION_THEME_BOARD_CODE_AMBIGUOUS"):
        validate_rotation_theme_config(duplicate)

    unknown_parent = json.loads(json.dumps(base, ensure_ascii=False))
    child = next(row for row in unknown_parent["themes"] if row["kind"] == CHILD)
    child["parent"] = "DOES_NOT_EXIST"
    with pytest.raises(RotationThemeConfigError, match="ROTATION_THEME_PARENT_MISSING"):
        validate_rotation_theme_config(unknown_parent)

    no_evidence = _registry_payload()
    no_evidence["themes"][0]["eastmoney_board_codes"] = []
    no_evidence["themes"][0]["evidence"] = []
    with pytest.raises(RotationThemeConfigError, match="ROTATION_THEME_UNRESOLVED_CODE_EVIDENCE_MISSING"):
        validate_rotation_theme_config(no_evidence)


def test_eastmoney_collectors_require_complete_pagination_and_nonempty_members():
    catalog = collect_eastmoney_board_catalog(
        as_of=AS_OF,
        expected_trade_date=DAY,
        fetch_page=_east_page,
        page_size=1,
    )
    flow = collect_eastmoney_board_flow(
        as_of=AS_OF,
        expected_trade_date=DAY,
        fetch_page=_east_page,
        page_size=1,
    )
    members = collect_eastmoney_board_members(
        as_of=AS_OF,
        expected_trade_date=DAY,
        board_code="BK0001",
        fetch_page=_east_page,
        page_size=1,
    )

    assert catalog["available"] and catalog["provider_total"] == len(catalog["records"]) == 1
    assert flow["available"] and flow["provider_total"] == len(flow["records"]) == 1
    assert members["available"] and members["provider_total"] == len(members["records"]) == 2
    assert members["pagination_evidence"]["complete"] is True
    assert len(members["pagination_evidence"]["pages"]) == 2
    assert {row["symbol"] for row in members["records"]} == {"600001.SH", "000002.SZ"}


def test_eastmoney_incomplete_page_is_fail_closed():
    result = collect_eastmoney_board_members(
        as_of=AS_OF,
        expected_trade_date=DAY,
        board_code="BK0001",
        fetch_page=_incomplete_east_page,
    )

    assert result["available"] is False
    assert result["reason_code"] == "EASTMONEY_PAGINATION_INCOMPLETE"
    assert result["records"] == []


def test_tencent_undated_flow_needs_fixed_capture_and_same_day_quote_proof():
    def flow(symbol: str) -> dict:
        return {"main_net_inflow_cny": 100.0, "turnover_cny": 50.0}

    def quote(symbol: str) -> dict:
        return {
            "symbol": symbol,
            "latest_price": 10.0,
            "change_pct": 2.0,
            "turnover_cny": 50.0,
            "quote_time": "2026-09-04T14:59:00+08:00",
            "trade_date": DAY.isoformat(),
        }

    proved = collect_tencent_capital_flow(
        as_of=AS_OF,
        expected_trade_date=DAY,
        expected_symbols=["600001"],
        fetch_symbol=flow,
        capture_timestamp=CAPTURE,
        quote_fetch=quote,
    )
    unproved = collect_tencent_capital_flow(
        as_of=AS_OF,
        expected_trade_date=DAY,
        expected_symbols=["600001"],
        fetch_symbol=flow,
        capture_timestamp=CAPTURE,
    )

    assert proved["available"] is True
    assert proved["same_day_quote_validated"] is True
    assert proved["quote_records"]["600001.SH"]["trade_date"] == DAY.isoformat()
    assert unproved["available"] is False
    assert unproved["reason_code"] == "TENCENT_SAME_DAY_QUOTE_PROOF_MISSING"


def test_tencent_concurrent_partial_failure_exposes_coverage():
    def flow(symbol: str) -> dict:
        if symbol == "000002.SZ":
            raise RuntimeError("provider timeout")
        return {"main_net_inflow_cny": 100.0, "turnover_cny": 100.0}

    result = collect_tencent_capital_flow(
        as_of=AS_OF,
        expected_trade_date=DAY,
        expected_symbols=["600001.SH", "000002.SZ"],
        fetch_symbol=flow,
        capture_timestamp=CAPTURE,
        quote_fetch=lambda symbol: {
            "symbol": symbol,
            "latest_price": 10.0,
            "change_pct": 1.0,
            "turnover_cny": 100.0,
            "trade_date": DAY.isoformat(),
        },
        workers=2,
    )

    assert result["available"] is True
    assert result["returned_symbol_count"] == 1
    assert result["failed_symbol_count"] == 1
    assert result["failed_symbols"] == ["000002.SZ"]
    assert result["coverage"] == pytest.approx(0.5)


def test_membership_versions_warn_expire_and_never_backfill_future(tmp_path: Path):
    old_path = write_membership_snapshot(tmp_path, _membership(captured=datetime(2026, 9, 1, 15, tzinfo=TZ)))
    future = _membership(
        captured=datetime(2026, 9, 20, 15, tzinfo=TZ),
        effective=date(2026, 9, 20),
    )
    write_membership_snapshot(tmp_path, future)

    warning = load_membership_snapshot(tmp_path, "TEST_THEME", date(2026, 9, 8))
    fallback = load_membership_snapshot(
        tmp_path,
        "TEST_THEME",
        date(2026, 9, 8),
        update_failed=True,
    )
    expired = load_membership_snapshot(tmp_path, "TEST_THEME", date(2026, 9, 16))
    before_future = load_membership_snapshot(tmp_path, "TEST_THEME", date(2026, 9, 10))

    assert warning["available"] is True
    assert warning["path"] == str(old_path)
    assert warning["age_days"] == 7
    assert warning["warning"] == "MEMBERSHIP_SNAPSHOT_STALE_WARNING"
    assert fallback["warning"] == "MEMBERSHIP_UPDATE_FAILED_FALLBACK"
    assert expired["available"] is False
    assert expired["reason_code"] == "MEMBERSHIP_SNAPSHOT_EXPIRED"
    assert before_future["available"] is True
    assert before_future["path"] == str(old_path)


def test_membership_version_same_hash_is_idempotent(tmp_path: Path):
    snapshot = _membership(captured=datetime(2026, 9, 4, 14, tzinfo=TZ))
    first = write_membership_snapshot(tmp_path, snapshot)
    second = write_membership_snapshot(tmp_path, snapshot)

    assert second == first
    assert len(list(tmp_path.glob("membership-*.json"))) == 1


def test_top_level_refreshes_due_membership_and_keeps_daily_quotes_current(tmp_path: Path):
    registry = _write_registry(tmp_path / "registry.yaml")
    write_membership_snapshot(
        tmp_path / "daily" / "memberships",
        _membership(captured=datetime(2026, 9, 1, 15, tzinfo=TZ), old_quotes=True),
    )

    member_calls: list[str] = []

    def members(kind: str, value: str, page: int, page_size: int) -> dict:
        member_calls.append(value)
        return _east_page_for(LATER_DAY, LATER_CAPTURE, kind, value, page, page_size)

    result = collect_rotation_theme_snapshot(
        as_of=LATER_AS_OF,
        expected_trade_date=LATER_DAY,
        registry_path=registry,
        snapshot_dir=tmp_path / "daily",
        membership_refresh_days=7,
        workers=2,
        fetchers=_top_level_fetchers(day=LATER_DAY, captured=LATER_CAPTURE, member_fetcher=members),
        tencent_capture_timestamp=LATER_CAPTURE,
    )

    board = _board(result, "TEST_THEME")
    assert result["available"] is True
    assert member_calls == ["BK0001"]
    assert result["source_health"]["membership_update_warnings"] == {}
    assert result["source_health"]["degraded_membership_count"] == 0
    assert board["member_snapshot_complete"] is True
    assert board["price_coverage"] == pytest.approx(1.0)
    assert board["breadth"] == pytest.approx(1.0)
    assert board["main_net_inflow_cny"] == pytest.approx(200.0)
    assert len(list((tmp_path / "daily" / "memberships").glob("membership-*.json"))) == 2


def test_old_membership_never_supplies_daily_quote_facts(tmp_path: Path):
    registry = _write_registry(tmp_path / "registry.yaml")
    write_membership_snapshot(
        tmp_path / "daily" / "memberships",
        _membership(captured=datetime(2026, 9, 1, 15, tzinfo=TZ), old_quotes=True),
    )

    def should_not_refresh(*args, **kwargs):
        raise AssertionError("old membership was unexpectedly refreshed")

    result = collect_rotation_theme_snapshot(
        as_of=LATER_AS_OF,
        expected_trade_date=LATER_DAY,
        registry_path=registry,
        snapshot_dir=tmp_path / "daily",
        membership_refresh_days=30,
        workers=2,
        eastmoney_members_fetcher=should_not_refresh,
        eastmoney_catalog_fetcher=lambda kind, value, page, page_size: _east_page_for(LATER_DAY, LATER_CAPTURE, kind, value, page, page_size),
        eastmoney_flow_fetcher=lambda kind, value, page, page_size: _east_page_for(LATER_DAY, LATER_CAPTURE, kind, value, page, page_size),
        tencent_fetch_symbol=lambda symbol: {
            "main_net_inflow_cny": 100.0,
            "turnover_cny": 100.0,
            "latest_price": 10.0,
            "change_pct": 5.0,
            "trade_date": LATER_DAY.isoformat(),
        },
        tencent_capture_timestamp=LATER_CAPTURE,
        tencent_quote_fetcher=lambda symbol: {},
    )

    board = _board(result, "TEST_THEME")
    assert result["available"] is True
    assert result["source_health"]["degraded_membership_count"] == 1
    assert board["breadth"] == pytest.approx(1.0)
    assert board["price_coverage"] == pytest.approx(1.0)
    assert board["relative_return_pct"] == pytest.approx(4.0)
    assert board["constituents"] == ["000002.SZ", "600001.SH"]


def test_six_factor_missing_values_are_unavailable_and_scores_renormalize():
    partial = _metric_row(
        "PARTIAL",
        missing={"momentum_3d_pct", "momentum_5d_pct", "leader_structure_score", "rank_persistence_score"},
    )
    result = calculate_rotation_strength([partial], expected_trade_date=DAY)
    row = _board(result, "PARTIAL")

    assert row["factor_coverage"] == pytest.approx(3 / 6)
    assert row["strength_factors"]["momentum_3_5d"] is None
    assert row["strength_factors"]["leader_structure"] is None
    assert "momentum_3_5d" in row["missing_factors"]
    assert row["strength"] == pytest.approx(100.0)
    assert row["selection_status"] == "ELIGIBLE_PRIMARY"

    low = _metric_row(
        "LOW_COVERAGE",
        missing={"relative_return_pct", "momentum_3d_pct", "momentum_5d_pct", "leader_structure_score", "rank_persistence_score"},
    )
    low_result = calculate_rotation_strength([low], expected_trade_date=DAY)
    low_row = _board(low_result, "LOW_COVERAGE")
    assert low_row["factor_coverage"] < MINIMUM_FACTOR_COVERAGE
    assert low_row["selection_status"] == "OBSERVATION_ONLY_FACTOR_COVERAGE_LOW"


def test_selection_gates_require_positive_tencent_flow_coverage_price_and_consistent_eastmoney():
    rows = [
        _metric_row("PASS", main=100, fund_coverage=0.8, price_coverage=0.9, eastmoney_main=1),
        _metric_row("NEGATIVE", main=-1, fund_coverage=1, price_coverage=1, eastmoney_main=1),
        _metric_row("LOW_FUNDS", main=100, fund_coverage=0.79, price_coverage=1, eastmoney_main=1),
        _metric_row("LOW_PRICE", main=100, fund_coverage=1, price_coverage=0.89, eastmoney_main=1),
        _metric_row("EAST_CONFLICT", main=100, fund_coverage=1, price_coverage=1, eastmoney_main=-1),
    ]
    result = calculate_rotation_strength(rows, expected_trade_date=DAY)
    statuses = {row["theme_id"]: row["selection_status"] for row in result["boards"]}

    assert statuses == {
        "PASS": "ELIGIBLE_PRIMARY",
        "NEGATIVE": "OBSERVATION_ONLY_TENCENT_FLOW_NON_POSITIVE",
        "LOW_FUNDS": "OBSERVATION_ONLY_TENCENT_COVERAGE_LOW",
        "LOW_PRICE": "OBSERVATION_ONLY_PRICE_COVERAGE_LOW",
        "EAST_CONFLICT": "OBSERVATION_ONLY_EASTMONEY_FLOW_CONFLICT",
    }


def test_effective_fund_coverage_prefers_validated_turnover_over_member_count():
    row = _metric_row("TURNOVER_FIRST", fund_coverage=0.8, flow_coverage=0.1)
    result = calculate_rotation_strength([row], expected_trade_date=DAY)
    board = _board(result, "TURNOVER_FIRST")

    assert board["turnover_coverage"] == pytest.approx(0.8)
    assert board["flow_coverage"] == pytest.approx(0.1)
    assert board["effective_fund_coverage"] == pytest.approx(0.8)
    assert board["coverage_basis"] == "turnover"
    assert board["coverage_degraded"] is False
    assert board["coverage_degraded_reason"] is None
    assert board["selection_status"] == "ELIGIBLE_PRIMARY"


def test_effective_fund_coverage_degrades_to_member_count_when_turnover_missing():
    row = _metric_row("MEMBER_COUNT_FALLBACK", fund_coverage=None, flow_coverage=0.8)
    result = calculate_rotation_strength([row], expected_trade_date=DAY)
    board = _board(result, "MEMBER_COUNT_FALLBACK")

    assert board["turnover_coverage"] is None
    assert board["flow_coverage"] == pytest.approx(0.8)
    assert board["effective_fund_coverage"] == pytest.approx(0.8)
    assert board["coverage_basis"] == "member_count"
    assert board["coverage_degraded"] is True
    assert board["coverage_degraded_reason"] == "TENCENT_TURNOVER_DENOMINATOR_UNAVAILABLE"
    assert board["selection_status"] == "ELIGIBLE_PRIMARY"


def test_member_count_fallback_below_threshold_still_blocks_selection():
    row = _metric_row("LOW_MEMBER_COUNT", fund_coverage=None, flow_coverage=0.79)
    result = calculate_rotation_strength([row], expected_trade_date=DAY)
    board = _board(result, "LOW_MEMBER_COUNT")

    assert board["coverage_basis"] == "member_count"
    assert board["effective_fund_coverage"] == pytest.approx(0.79)
    assert board["selection_status"] == "OBSERVATION_ONLY_TENCENT_COVERAGE_LOW"


def test_member_count_fallback_does_not_bypass_independent_price_coverage_gate():
    row = _metric_row("LOW_MEMBER_PRICE", fund_coverage=None, flow_coverage=0.8, price_coverage=0.89)
    result = calculate_rotation_strength([row], expected_trade_date=DAY)
    board = _board(result, "LOW_MEMBER_PRICE")

    assert board["effective_fund_coverage"] == pytest.approx(0.8)
    assert board["selection_status"] == "OBSERVATION_ONLY_PRICE_COVERAGE_LOW"


def test_tencent_coverage_fallback_and_b_share_exclusion_keep_a_share_denominator():
    flows = [
        {"symbol": "600001.SH", "main_net_inflow_cny": 100.0},
        {"symbol": "000002.SZ", "main_net_inflow_cny": 100.0},
        {"symbol": "830001.BJ", "main_net_inflow_cny": 100.0},
    ]
    members = {
        "AGRI": [
            {"symbol": "600001.SH", "turnover_cny": None},
            {"symbol": "000002.SZ", "turnover_cny": None},
            {"symbol": "200019.SZ", "turnover_cny": None},
            {"symbol": "900901.SH", "turnover_cny": None},
            {"symbol": "830001.BJ", "turnover_cny": None},
        ]
    }

    metrics = aggregate_tencent_theme_flows(flows, members, expected_trade_date=DAY)
    board = metrics["AGRI"]

    assert board["member_count"] == 3
    assert board["covered_member_count"] == 3
    assert board["flow_coverage"] == pytest.approx(1.0)
    assert board["turnover_coverage"] is None
    assert board["effective_fund_coverage"] == pytest.approx(1.0)
    assert board["coverage_basis"] == "member_count"
    assert board["coverage_degraded"] is True
    assert board["excluded_non_a_share_symbols"] == ["200019.SZ", "900901.SH"]


def test_collection_excludes_b_shares_before_daily_price_coverage_denominator(tmp_path: Path):
    registry = _write_registry(tmp_path / "registry.yaml")

    def member_page(kind: str, value: str, page: int, page_size: int) -> dict:
        if kind != "members":
            return _east_page_for(DAY, CAPTURE, kind, value, page, page_size)
        rows = [
            {"f12": "600001", "f14": "甲公司", "f2": 10.0, "f3": 5.0, "f5": 100, "f6": 1000},
            {"f12": "000002", "f14": "乙公司", "f2": 20.0, "f3": 2.0, "f5": 200, "f6": 2000},
            {"f12": "830001", "f14": "北交所公司", "f2": 30.0, "f3": 1.0, "f5": 300, "f6": 3000},
            {"f12": "200019", "f14": "深B股", "f2": 40.0, "f3": 1.0, "f5": 400, "f6": 4000},
            {"f12": "900901", "f14": "沪B股", "f2": 50.0, "f3": 1.0, "f5": 500, "f6": 5000},
        ]
        return {"total": len(rows), "rows": rows, "captured_at": CAPTURE.isoformat()}

    fetchers = _top_level_fetchers()
    fetchers["eastmoney_members"] = member_page
    result = collect_rotation_theme_snapshot(
        as_of=AS_OF,
        expected_trade_date=DAY,
        registry_path=registry,
        snapshot_dir=tmp_path / "daily",
        fetchers=fetchers,
        tencent_capture_timestamp=CAPTURE,
        workers=2,
    )
    board = _board(result, "TEST_THEME")

    assert board["member_count"] == 3
    assert board["price_coverage"] == pytest.approx(1.0)
    assert board["excluded_non_a_share_count"] == 2
    assert board["excluded_non_a_share_symbols"] == ["200019.SZ", "900901.SH"]
    assert all(symbol not in board["constituents"] for symbol in ("200019.SZ", "900901.SH"))
    assert "830001.BJ" in board["constituents"]


def test_child_inherits_parent_rank_without_consuming_primary_top5():
    rows = [
        _metric_row(f"PRIMARY_{index}", relative=100 - index)
        for index in range(6)
    ]
    rows.append(_metric_row("CHILD_0", kind=CHILD, parent="PRIMARY_0", relative=999))
    result = calculate_rotation_strength(rows, rotation_theme_count=5, expected_trade_date=DAY)

    assert len(result["selected_primary_boards"]) == 5
    assert all(row["board_code"] != "CHILD_0" for row in result["selected_primary_boards"])
    parent = _board(result, "PRIMARY_0")
    child = _board(result, "CHILD_0")
    assert parent["selected_for_rotation"] is True
    assert child["selected_for_rotation"] is True
    assert child["primary_rank"] == parent["primary_rank"]
    assert child["selection_status"] == "INHERITED_FROM_PRIMARY"


def test_strong_child_fills_top5_when_parent_is_not_eligible():
    rows = [
        _metric_row(f"PRIMARY_{index}", relative=100 - index)
        for index in range(4)
    ]
    rows.extend([
        _metric_row("WEAK_PARENT", main=-100.0, eastmoney_main=-100.0),
        _metric_row(
            "STRONG_CHILD",
            kind=CHILD,
            parent="WEAK_PARENT",
            main=500.0,
            eastmoney_main=500.0,
            relative=120.0,
        ),
    ])

    result = calculate_rotation_strength(
        rows,
        rotation_theme_count=5,
        expected_trade_date=DAY,
    )

    assert len(result["selected_primary_boards"]) == 5
    assert result["selected_primary_boards"][0]["board_code"] == "STRONG_CHILD"
    child = _board(result, "STRONG_CHILD")
    parent = _board(result, "WEAK_PARENT")
    assert child["selected_for_rotation"] is True
    assert child["selection_status"] == "ELIGIBLE_CHILD_STANDALONE"
    assert child["primary_rank"] == 1
    assert parent["selected_for_rotation"] is False


def test_public_snapshot_has_stable_schema_and_main_flow_compatibility():
    snapshot = build_rotation_theme_snapshot(
        as_of=AS_OF,
        expected_trade_date=DAY,
        board_rows=[_metric_row("TEST_THEME")],
        eastmoney_flows={"TEST_THEME": {"main_net_inflow_cny": 1.0}},
    )

    assert snapshot["schema_version"] == ROTATION_THEME_SCHEMA
    assert snapshot["source_id"] == ROTATION_THEME_SOURCE_ID
    assert snapshot["taxonomy_substitution_forbidden"] is False
    assert snapshot["content_hash"] == _content_hash(snapshot)
    board = _board(snapshot, "TEST_THEME")
    assert board["main_net_inflow_cny"] == pytest.approx(100.0)
    assert snapshot["by_symbol"]["600001.SH"][0]["main_net_inflow_cny"] == pytest.approx(100.0)
    assert snapshot["by_symbol"]["600001.SH"][0]["strategy_theme_id"] == "TEST_THEME"


def test_collection_keeps_each_theme_provider_rank(tmp_path: Path):
    themes = [
        {
            "theme_id": "THEME_ONE",
            "name": "方向一",
            "kind": "PRIMARY",
            "parent": None,
            "eastmoney_board_codes": ["BK0001"],
            "aliases": ["方向一"],
            "effective_from": "2026-09-01",
            "effective_to": None,
            "evidence": ["test taxonomy evidence"],
        },
        {
            "theme_id": "THEME_TWO",
            "name": "方向二",
            "kind": "PRIMARY",
            "parent": None,
            "eastmoney_board_codes": ["BK0002"],
            "aliases": ["方向二"],
            "effective_from": "2026-09-01",
            "effective_to": None,
            "evidence": ["test taxonomy evidence"],
        },
    ]
    registry = _write_registry(tmp_path / "registry.yaml", themes=themes)

    def eastmoney(kind: str, value: str, page: int, page_size: int) -> dict:
        if kind == "catalog":
            rows = [
                {"f12": "BK0001", "f14": "方向一"},
                {"f12": "BK0002", "f14": "方向二"},
            ]
        elif kind == "flow":
            rows = [
                {"f12": "BK0001", "f14": "方向一", "f3": 5.0, "f62": 3000},
                {"f12": "BK0002", "f14": "方向二", "f3": 3.0, "f62": 2000},
            ]
        else:
            code = "600001" if value == "BK0001" else "000002"
            rows = [{"f12": code, "f14": f"{value}公司"}]
        start = (page - 1) * page_size
        return {
            "total": len(rows),
            "rows": rows[start : start + page_size],
            "captured_at": CAPTURE.isoformat(),
        }

    result = collect_rotation_theme_snapshot(
        as_of=AS_OF,
        expected_trade_date=DAY,
        registry_path=registry,
        snapshot_dir=tmp_path / "daily",
        workers=2,
        eastmoney_catalog_fetcher=eastmoney,
        eastmoney_flow_fetcher=eastmoney,
        eastmoney_members_fetcher=eastmoney,
        tencent_fetch_symbol=lambda symbol: {
            "main_net_inflow_cny": 100.0,
            "turnover_cny": 100.0,
            "latest_price": 10.0,
            "change_pct": 5.0,
            "trade_date": DAY.isoformat(),
        },
        tencent_quote_fetcher=lambda symbol: {
            "symbol": symbol,
            "latest_price": 10.0,
            "change_pct": 5.0,
            "turnover_cny": 100.0,
            "trade_date": DAY.isoformat(),
        },
        tencent_capture_timestamp=CAPTURE,
    )

    assert _board(result, "THEME_ONE")["provider_rank"] == 1
    assert _board(result, "THEME_TWO")["provider_rank"] == 2


def test_daily_snapshot_versions_are_immutable_and_historical_reads_are_network_free(tmp_path: Path):
    registry = _write_registry(tmp_path / "registry.yaml")
    result = collect_rotation_theme_snapshot(
        as_of=AS_OF,
        expected_trade_date=DAY,
        registry_path=registry,
        snapshot_dir=tmp_path / "daily",
        workers=2,
        fetchers=_top_level_fetchers(),
        tencent_capture_timestamp=CAPTURE,
    )
    daily_dir = tmp_path / "daily"
    day_path = daily_dir / f"rotation-theme-{DAY.isoformat()}.json"
    version_files = list((daily_dir / "daily_versions").glob("*.json"))
    original = day_path.read_bytes()

    assert result["snapshot_path"] == str(day_path)
    assert len(version_files) == 1
    persisted = json.loads(day_path.read_text(encoding="utf-8"))
    assert write_rotation_theme_snapshot(daily_dir, persisted) == day_path
    assert day_path.read_bytes() == original

    changed = dict(result)
    changed["reason_code"] = "RECOVERED_LATER_CAPTURE"
    changed["captured_at"] = datetime(2026, 9, 4, 15, 5, tzinfo=TZ).isoformat()
    changed["content_hash"] = ""
    changed["content_hash"] = _content_hash(changed)
    assert write_rotation_theme_snapshot(daily_dir, changed) == day_path
    assert day_path.read_bytes() != original
    assert len(list((daily_dir / "daily_versions").glob("*.json"))) == 2

    older = dict(result)
    older["reason_code"] = "OLDER_REPLAY"
    older["captured_at"] = datetime(2026, 9, 4, 14, 59, tzinfo=TZ).isoformat()
    older["content_hash"] = ""
    older["content_hash"] = _content_hash(older)
    with pytest.raises(RotationThemeDataError, match="ROTATION_THEME_IMMUTABLE_OVERWRITE_FORBIDDEN"):
        write_rotation_theme_snapshot(daily_dir, older)

    def fail_network(*args, **kwargs):
        raise AssertionError("historical request touched a provider")

    archived = collect_rotation_theme_snapshot(
        as_of=datetime(2026, 9, 5, 15, tzinfo=TZ),
        expected_trade_date=DAY,
        registry_path=registry,
        snapshot_dir=daily_dir,
        fetchers={
            "eastmoney_catalog": fail_network,
            "eastmoney_flow": fail_network,
            "eastmoney_members": fail_network,
            "tencent_flow": fail_network,
            "tencent_quote": fail_network,
        },
    )
    assert archived["archive_read_only"] is True
    assert archived["trade_date"] == DAY.isoformat()
