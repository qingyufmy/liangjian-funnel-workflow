from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest
import requests
from zoneinfo import ZoneInfo

import liangjian_funnel.data.a2_market as a2_market
from liangjian_funnel.data.a2_market import (
    CapitalFlowError,
    build_capital_flow_snapshot,
    collect_eastmoney_capital_flow,
    load_capital_flow_snapshot,
    unavailable_capital_flow_snapshot,
    write_capital_flow_snapshot,
)


TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 8, 29, 15, 10, tzinfo=TZ)


def _row(symbol: str, *, ratio: object = 10, amount: object = "1.2亿") -> dict[str, object]:
    return {
        "symbol": symbol,
        "name": "测试股",
        "net_inflow_ratio": ratio,
        "net_inflow_amount": amount,
        "large_inflow_ratio": "2.5%",
        "super_inflow_ratio": 1,
    }


def test_capital_flow_preserves_window_gaps_and_marks_partial_snapshot() -> None:
    snapshot = build_capital_flow_snapshot(
        {
            "today": [_row("600001", ratio="12.5%")],
            "3d": [_row("600001", ratio=-2, amount=-5000)],
            # 5d is intentionally absent; 10d uses the explicit suffix aliases.
            "10d": [{"代码": "600001", "10日主力净流入占比": 5, "10日主力净流入-净额": 10}],
        },
        as_of=NOW,
        expected_symbols=["600001.SH"],
        failures={"5d": "UPSTREAM_TIMEOUT"},
    )

    assert snapshot["available"] is True
    assert snapshot["reason_code"] == "PARTIAL_WINDOWS"
    row = snapshot["by_symbol"]["600001.SH"]
    assert row["available"] is True
    assert row["metrics"]["today"]["net_inflow_ratio_pct"] == 12.5
    assert row["metrics"]["5d"]["availability_state"] == "SOURCE_FAILED"
    assert row["metrics"]["5d"]["reason_code"] == "UPSTREAM_TIMEOUT"
    assert snapshot["coverage_by_window"]["5d"]["reason_code"] == "UPSTREAM_TIMEOUT"
    assert snapshot["turnover_is_capital_flow"] is False


def test_current_collection_captures_all_provider_failures_and_rejects_tampered_cache(tmp_path: Path) -> None:
    def failing_fetch(_indicator: str):
        raise TimeoutError("provider unavailable")

    snapshot = collect_eastmoney_capital_flow(
        as_of=NOW,
        now=NOW,
        expected_symbols=["600001.SH"],
        cache_dir=tmp_path,
        fetch_rank=failing_fetch,
    )
    assert snapshot["available"] is False
    assert snapshot["reason_code"] == "CAPITAL_FLOW_COVERAGE_INSUFFICIENT"
    assert snapshot["failures"] == {key: "TIMEOUTERROR" for key in ("today", "3d", "5d", "10d")}
    cached_path = tmp_path / "capital-flow-2026-08-29.json"
    assert cached_path.is_file()
    assert load_capital_flow_snapshot(tmp_path, "2026-08-29") == snapshot

    tampered = dict(snapshot)
    tampered["reason_code"] = "TAMPERED"
    cached_path.write_text(json.dumps(tampered, ensure_ascii=False), encoding="utf-8")
    assert load_capital_flow_snapshot(tmp_path, "2026-08-29") is None

    # A valid cached point-in-time observation wins over a provider callback;
    # historical/current callers therefore cannot accidentally overwrite it.
    cached_path.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")
    called = False

    def should_not_fetch(_indicator: str):
        nonlocal called
        called = True
        return []

    assert collect_eastmoney_capital_flow(
        as_of=NOW,
        now=NOW,
        expected_symbols=["600001.SH"],
        cache_dir=tmp_path,
        fetch_rank=should_not_fetch,
    ) == snapshot
    assert called is False


def test_eastmoney_fetcher_paginates_and_rejects_invalid_provider_envelopes(monkeypatch: pytest.MonkeyPatch) -> None:
    class Response:
        def __init__(self, payload: dict[str, object], *, error: Exception | None = None):
            self.payload = payload
            self.error = error

        def raise_for_status(self) -> None:
            if self.error:
                raise self.error

        def json(self):
            return self.payload

    class Session:
        def __init__(self):
            self.calls: list[dict[str, object]] = []

        def mount(self, *_args, **_kwargs):
            return None

        def get(self, _url, *, params, headers, timeout):
            self.calls.append({"params": params, "headers": headers, "timeout": timeout})
            if params["pn"] == "1":
                return Response({"data": {"total": 2, "diff": [{"f12": "600001", "f14": "甲", "f62": 10, "f184": 2, "f72": 1, "f75": 1, "f69": 1}]}})
            return Response({"data": {"total": 2, "diff": [{"f12": "000002", "f14": "乙", "f62": -5, "f184": -1, "f72": -1, "f75": -1, "f69": -1}]}})

    session = Session()
    monkeypatch.setattr(requests, "Session", lambda: session)
    rows = a2_market._eastmoney_rank_fetcher("今日")
    assert [row["symbol"] for row in rows] == ["600001", "000002"]
    assert len(session.calls) == 2
    assert session.calls[0]["params"]["pz"] == "5000"
    assert session.calls[1]["params"]["pn"] == "2"

    with pytest.raises(CapitalFlowError, match="unsupported"):
        a2_market._eastmoney_rank_fetcher("unknown")

    class InvalidSession(Session):
        def get(self, *_args, **_kwargs):
            return Response({"data": {"total": 1, "diff": []}})

    monkeypatch.setattr(requests, "Session", InvalidSession)
    with pytest.raises(CapitalFlowError, match="page incomplete"):
        a2_market._eastmoney_rank_fetcher("今日")


def test_capital_flow_file_and_timestamp_contracts_are_fail_closed(tmp_path: Path) -> None:
    with pytest.raises(CapitalFlowError, match="trade_date"):
        write_capital_flow_snapshot(tmp_path, {"trade_date": "not-a-date"})
    with pytest.raises(CapitalFlowError, match="timezone-aware"):
        build_capital_flow_snapshot({}, as_of=datetime(2026, 8, 29, 15, 10), expected_symbols=[])
    empty = unavailable_capital_flow_snapshot(
        as_of=NOW,
        reason_code="SOURCE_NOT_CONFIGURED",
        expected_symbols=["600001", "800001"],
    )
    assert empty["by_symbol"]["600001.SH"]["availability_state"] == "NOT_CONFIGURED"
    assert empty["by_symbol"]["800001.BJ"]["availability_state"] == "NOT_CONFIGURED"
    assert load_capital_flow_snapshot(tmp_path, "2026-08-29") is None
