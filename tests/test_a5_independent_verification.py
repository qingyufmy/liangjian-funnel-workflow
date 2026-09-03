from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from liangjian_funnel.review.verification import A5IndependentVerifier


TZ = ZoneInfo("Asia/Shanghai")


def _bar(symbol: str, stamp: datetime, close: float, *, interval: str = "1m", source: str = "TEST") -> dict:
    return {
        "symbol": symbol,
        "interval": interval,
        "bar_end": stamp,
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": 1000,
        "amount": close * 1000,
        "source_id": source,
        "adjust_mode": "none",
    }


class _Provider:
    def __init__(self, rows: dict[tuple[str, str], list[dict]]):
        self.rows = rows

    def fetch_bars(self, symbol, interval, required_bars, *, as_of=None):
        bars = tuple(self.rows.get((symbol, interval), ()))
        return SimpleNamespace(bars=bars, reason_code="OK")


class _Daily:
    def __init__(self, rows: dict[str, list[dict]]):
        self.rows = rows

    def latest_daily_bar_windows_before(self, symbols, **kwargs):
        return {symbol: self.rows.get(symbol, []) for symbol in symbols}


class _MinuteStore:
    def __init__(self, rows: dict[str, list[dict]]):
        self.rows = rows

    def load_latest(self, symbol, interval, *, limit):
        assert interval == "1m"
        return tuple(self.rows.get(symbol, ())[-limit:])


def test_a5_independent_verifier_recomputes_and_traces_counterexample() -> None:
    cutoff = datetime(2026, 9, 3, 11, 30, tzinfo=TZ)
    symbols = ("000001.SZ", "000002.SZ", "000003.SZ")
    tencent_rows = {}
    for symbol, start, end in zip(symbols, (10.0, 10.0, 10.0), (10.6, 10.3, 9.9), strict=True):
        tencent_rows[(symbol, "1m")] = [
            _bar(symbol, cutoff.replace(hour=9, minute=31), start, source="TENCENT"),
            _bar(symbol, cutoff, end, source="TENCENT"),
        ]
    tdx_rows = {
        ("000001.SZ", "1m"): [
            _bar("000001.SZ", cutoff.replace(hour=9, minute=31), 10.0, source="TDX"),
            _bar("000001.SZ", cutoff, 10.6, source="TDX"),
        ],
        ("000001.SZ", "5m"): [
            _bar("000001.SZ", datetime(2026, 9, 2, 15, 0, tzinfo=TZ), 10.0, interval="5m", source="TDX"),
        ],
    }
    daily_rows = []
    for index in range(60):
        close = 9.41 + index * 0.01
        daily_rows.append({
            "timestamp": (datetime(2026, 7, 1, 15, 0, tzinfo=TZ) + timedelta(days=index)).isoformat(),
            "payload": {"close": close},
        })
    closes = [row["payload"]["close"] for row in daily_rows]
    ma = lambda length: sum(closes[-length:]) / length
    plan = {
        "plan_id": "plan-1", "symbol": "000001.SZ",
        "valid_from": datetime(2026, 9, 3, 9, 31, tzinfo=TZ).isoformat(),
        "payload_json": {
            "symbol": "000001.SZ", "stock_behavior_type": "TREND",
            "strategy_profile": "TREND_MA5", "trigger_low": 10.2,
            "trigger_high": 10.4, "stop_level": 9.8, "no_chase_price": 10.8,
            "ma_analysis": {"daily": {"ma5": ma(5), "ma20": ma(20), "ma60": ma(60)}},
        },
    }
    event = {
        "minute_end": cutoff.isoformat(), "action": "NO_ACTION", "effective": False,
        "reason_code": "UNEXPECTED_DROP", "payload_json": {
            "plan_id": "plan-1", "symbol": "000001.SZ",
            "strategy": {"action": "BUY_SIGNAL"},
        },
    }
    candidates = [
        {"symbol": symbols[0], "name": "甲", "theme_id": "AI", "theme_name": "人工智能", "pool": "FOCUS"},
        {"symbol": symbols[1], "name": "乙", "theme_id": "AI", "theme_name": "人工智能", "pool": "WATCH"},
        {"symbol": symbols[2], "name": "丙", "theme_id": "BANK", "theme_name": "银行", "pool": "WATCH"},
    ]
    verifier = A5IndependentVerifier(
        daily_cache=_Daily({"000001.SZ": daily_rows}),
        minute_store=_MinuteStore({"000001.SZ": tdx_rows[("000001.SZ", "1m")]}),
        tencent=_Provider(tencent_rows), mootdx=_Provider(tdx_rows), workers=2,
    )

    result = verifier.verify(
        a2={"themes": [{"theme_id": "AI"}], "candidates": candidates},
        plan_rows=[plan], event_rows=[event], cutoff_at=cutoff,
    )

    assert result["a2"]["covered_count"] == 3
    assert result["a2"]["independent_top3_theme_ids"] == ["AI", "BANK"]
    assert result["counterexamples"][0]["symbol"] == "000001.SZ"
    assert result["counterexamples"][0]["drop_stage"] == "A4_NO_EFFECTIVE_SIGNAL"
    assert result["a3"]["plans"][0]["formula_status"] == "MATCH"
    assert result["a3"]["plans"][0]["route_contract_match"] is True
    assert result["a3"]["plans"][0]["price_levels_valid"] is True
    assert result["a4"]["plans"][0]["orchestration_omission_count"] == 1
    assert result["a4"]["plans"][0]["cross_source_status"] == "MATCH"
    assert result["a4"]["plans"][0]["archived_tdx_status"] == "MATCH"


def test_a5_independent_verifier_never_calls_missing_scope_a_success() -> None:
    cutoff = datetime(2026, 9, 3, 15, 0, tzinfo=TZ)
    verifier = A5IndependentVerifier(
        daily_cache=_Daily({}), minute_store=_MinuteStore({}),
        tencent=None, mootdx=None,
    )
    result = verifier.verify(a2={"themes": [], "candidates": []}, plan_rows=[], event_rows=[], cutoff_at=cutoff)
    assert result["a2"]["status"] == "UNAVAILABLE"
    assert result["a3"]["status"] == "READY"
    assert result["a4"]["status"] == "READY"
    assert result["status"] == "DEGRADED"
    assert result["counterexamples"] == []


def test_a5_post_close_scans_full_a1_universe_before_confirming_top_percentile() -> None:
    cutoff = datetime(2026, 9, 3, 15, 0, tzinfo=TZ)
    prior = datetime(2026, 9, 2, 15, 0, tzinfo=TZ)
    current = datetime(2026, 9, 3, 15, 0, tzinfo=TZ)
    daily = {
        "000010.SZ": [
            {"timestamp": prior.isoformat(), "payload": {"close": 10.0}},
            {"timestamp": current.isoformat(), "payload": {"close": 11.0}, "content_hash": "a", "fetched_at": current.isoformat()},
        ],
        "000011.SZ": [
            {"timestamp": prior.isoformat(), "payload": {"close": 10.0}},
            {"timestamp": current.isoformat(), "payload": {"close": 10.2}, "content_hash": "b", "fetched_at": current.isoformat()},
        ],
    }
    tencent = {
        ("000010.SZ", "1m"): [
            _bar("000010.SZ", prior, 10.0, source="TENCENT"),
            _bar("000010.SZ", current, 11.0, source="TENCENT"),
        ],
        ("000011.SZ", "1m"): [
            _bar("000011.SZ", prior, 10.0, source="TENCENT"),
            _bar("000011.SZ", current, 10.2, source="TENCENT"),
        ],
    }
    verifier = A5IndependentVerifier(
        daily_cache=_Daily(daily), minute_store=_MinuteStore({}),
        tencent=_Provider(tencent), mootdx=None, workers=2,
    )
    result = verifier.verify(
        a2={"themes": [], "candidates": []},
        market_universe=[
            {"symbol": "000010.SZ", "name": "强势样本", "theme_id": "AI", "pool": "A1_MONITOR"},
            {"symbol": "000011.SZ", "name": "次强样本", "theme_id": "BANK", "pool": "A1_ACTIVE"},
        ],
        plan_rows=[], event_rows=[], cutoff_at=cutoff,
    )
    assert result["a2"]["market_universe_count"] == 2
    assert result["a2"]["alternate_confirmation_requested_count"] == 2
    assert result["counterexamples"][0]["symbol"] == "000010.SZ"
    assert result["counterexamples"][0]["drop_stage"] == "A1_NOT_ACTIVE"
    assert result["counterexamples"][0]["alternate_source_confirmation_available"] is True
