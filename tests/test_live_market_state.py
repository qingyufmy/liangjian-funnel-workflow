import json
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

import liangjian_funnel.runtime.live_market as live_market
from liangjian_funnel.runtime.live_market import (
    classify_full_market,
    classify_index_fallback,
    load_or_refresh_live_market_state,
)


NOW = datetime(2026, 9, 3, 10, 5, tzinfo=ZoneInfo("Asia/Shanghai"))


def _rows(up: int, down: int, *, up_change: float = 1.0, down_change: float = -1.0):
    return ([{"price_change_ratio_pct": up_change}] * up) + (
        [{"price_change_ratio_pct": down_change}] * down
    )


def test_full_market_allows_normal_breadth() -> None:
    state = classify_full_market(_rows(700, 300), as_of=NOW, expected_count=1000)
    assert state["status"] == "READY"
    assert state["entry_permission"] == "ALLOW"
    assert state["trade_date"] == "2026-09-03"
    assert state["breadth"] == 0.7


def test_full_market_weak_rotation_is_caution_not_block() -> None:
    state = classify_full_market(_rows(400, 600), as_of=NOW, expected_count=1000)
    assert state["status"] == "READY"
    assert state["entry_permission"] == "CAUTION"
    assert state["suggested_position_cap_pct"] == 0.5


def test_full_market_only_blocks_confirmed_systemic_selloff() -> None:
    state = classify_full_market(
        _rows(100, 900, up_change=0.2, down_change=-2.5),
        as_of=NOW,
        expected_count=1000,
    )
    assert state["status"] == "READY"
    assert state["entry_permission"] == "BLOCK_NEW_ENTRY"
    assert state["reason_code"] == "A4_LIVE_SYSTEMIC_SELL_OFF"


def test_full_market_rejects_partial_coverage() -> None:
    state = classify_full_market(_rows(400, 100), as_of=NOW, expected_count=1000)
    assert state["status"] == "DATA_BLOCKED"
    assert state["entry_permission"] == "UNKNOWN"


def test_index_fallback_retains_degraded_quality_and_permission() -> None:
    quotes = [
        {"price": 101, "previous_close": 100},
        {"price": 100.5, "previous_close": 100},
        {"price": 99.8, "previous_close": 100},
    ]
    state = classify_index_fallback(quotes, as_of=NOW)
    assert state["status"] == "READY_DEGRADED"
    assert state["entry_permission"] == "ALLOW"
    assert state["breadth"] is None


def test_index_fallback_cannot_claim_readiness_without_three_indices() -> None:
    state = classify_index_fallback(
        [{"price": 101, "previous_close": 100}],
        as_of=NOW,
    )
    assert state["status"] == "DATA_BLOCKED"
    assert state["reason_code"] == "A4_LIVE_MARKET_SOURCE_UNAVAILABLE"


class _Row:
    def __init__(self, change: float = 1.0):
        self.change = change

    def model_dump(self, mode: str = "json") -> dict[str, float]:
        del mode
        return {"price_change_ratio_pct": self.change}


class _Quote:
    def model_dump(self, mode: str = "json") -> dict[str, float]:
        del mode
        return {"price": 101.0, "previous_close": 100.0}


class _FullMarketClient:
    def __init__(self, result: object, calls: list[int]):
        self.result = result
        self.calls = calls

    def __enter__(self):
        self.calls.append(1)
        return self

    def __exit__(self, *_args):
        return None

    def market_snapshot(self, **_kwargs):
        return self.result


def _full_result(*, ready: bool) -> SimpleNamespace:
    return SimpleNamespace(
        ok=ready,
        complete=ready,
        reason_code="OK" if ready else "REQUEST_FAILED",
        items=tuple(_Row(1.0) for _ in range(1_000 if ready else 0)),
        total=1_000,
    )


def _index_market(*, ready: bool, calls: list[str]):
    class Market:
        def fetch_quote(self, symbol, *, as_of, max_age_seconds):
            del as_of, max_age_seconds
            calls.append(symbol)
            return SimpleNamespace(
                complete=ready,
                quote=_Quote() if ready else None,
                reason_code="OK" if ready else "REQUEST_FAILED",
            )

    return Market()


def test_live_market_retries_full_market_then_recovers_without_real_sleep(tmp_path, monkeypatch):
    full_calls: list[int] = []
    results = iter((_full_result(ready=False), _full_result(ready=True)))
    monkeypatch.setattr(
        live_market,
        "HithinkClient",
        lambda _settings: _FullMarketClient(next(results), full_calls),
    )
    index_calls: list[str] = []
    sleeps: list[float] = []
    settings = SimpleNamespace(fact_store_dir=Path(tmp_path))

    state = load_or_refresh_live_market_state(
        settings,
        _index_market(ready=False, calls=index_calls),
        as_of=NOW,
        sleep=sleeps.append,
    )

    assert state["status"] == "READY"
    assert state["source"] == "HITHINK_FULL_MARKET"
    assert full_calls == [1, 1]
    assert len(index_calls) == 4
    assert sleeps == [pytest.approx(0.5)]
    assert state["diagnostics"]["total_attempts"] == 2
    assert state["diagnostics"]["recovered_after_retry"] is True
    assert len(state["diagnostics"]["attempts"]) == 2
    assert state["diagnostics"]["attempts"][0]["full_market"]["status"] == "DATA_BLOCKED"
    assert state["diagnostics"]["attempts"][1]["full_market"]["status"] == "READY"


def test_live_market_exhaustion_stays_blocked_and_blocked_cache_is_not_reused(tmp_path, monkeypatch):
    full_calls: list[int] = []
    monkeypatch.setattr(
        live_market,
        "HithinkClient",
        lambda _settings: _FullMarketClient(_full_result(ready=False), full_calls),
    )
    index_calls: list[str] = []
    settings = SimpleNamespace(fact_store_dir=Path(tmp_path))
    sleeps: list[float] = []

    first = load_or_refresh_live_market_state(
        settings,
        _index_market(ready=False, calls=index_calls),
        as_of=NOW,
        sleep=sleeps.append,
    )
    second = load_or_refresh_live_market_state(
        settings,
        _index_market(ready=False, calls=index_calls),
        as_of=NOW,
        sleep=sleeps.append,
    )

    assert first["status"] == second["status"] == "DATA_BLOCKED"
    assert first["diagnostics"]["total_attempts"] == 2
    assert second["diagnostics"]["total_attempts"] == 2
    # The second invocation collected again instead of reusing the blocked
    # five-minute file.
    assert len(full_calls) == 4
    assert len(index_calls) == 16
    assert len(sleeps) == 2
    path = Path(tmp_path) / "a4_live_market" / "2026-09-03" / "1005.json"
    assert json.loads(path.read_text(encoding="utf-8"))["status"] == "DATA_BLOCKED"


def test_live_market_ready_cache_is_reused_and_future_cache_is_rejected(tmp_path, monkeypatch):
    full_calls: list[int] = []
    monkeypatch.setattr(
        live_market,
        "HithinkClient",
        lambda _settings: _FullMarketClient(_full_result(ready=True), full_calls),
    )
    settings = SimpleNamespace(fact_store_dir=Path(tmp_path))
    market = _index_market(ready=False, calls=[])
    first = load_or_refresh_live_market_state(settings, market, as_of=NOW, sleep=lambda _delay: None)

    future_calls: list[int] = []

    class FailClient:
        def __enter__(self):
            future_calls.append(1)
            return self

        def __exit__(self, *_args):
            return None

        def market_snapshot(self, **_kwargs):
            return _full_result(ready=False)

    monkeypatch.setattr(live_market, "HithinkClient", lambda _settings: FailClient())
    cached = load_or_refresh_live_market_state(settings, market, as_of=NOW, sleep=lambda _delay: None)
    assert first["status"] == cached["status"] == "READY"
    assert cached["cache_hit"] is True
    assert cached["cache_reused"] is True
    assert cached["diagnostics"]["cache_hit"] is True
    assert len(full_calls) == 1

    # A future observation in the same bucket cannot be used for an earlier
    # decision tick; the call therefore attempts fresh collection.
    future_path = Path(tmp_path) / "a4_live_market" / "2026-09-03" / "1005.json"
    future = dict(first)
    future["as_of"] = "2026-09-03T10:06:00+08:00"
    future_path.write_text(json.dumps(future, ensure_ascii=False), encoding="utf-8")
    refreshed = load_or_refresh_live_market_state(
        settings,
        market,
        as_of=NOW,
        sleep=lambda _delay: None,
    )
    assert refreshed["status"] == "DATA_BLOCKED"
    assert len(future_calls) == 2
