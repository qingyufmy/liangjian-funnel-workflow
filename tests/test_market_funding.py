from __future__ import annotations

from datetime import date, datetime, timedelta
import json
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.pipeline.market_funding import build_market_funding_regime


TZ = ZoneInfo("Asia/Shanghai")
AS_OF = datetime(2026, 8, 28, 15, 10, tzinfo=TZ)


def _universe(*changes: float) -> list[dict[str, object]]:
    return [
        {"symbol": f"60000{index + 1}.SH", "change_ratio_pct": change}
        for index, change in enumerate(changes)
    ]


def _bars(
    symbols: list[str],
    *,
    baseline: float = 100.0,
    latest: float = 100.0,
    latest_date: date = AS_OF.date(),
) -> dict[str, list[dict[str, object]]]:
    result: dict[str, list[dict[str, object]]] = {}
    for symbol in symbols:
        result[symbol] = [
            {
                "trade_date": (latest_date - timedelta(days=offset)).isoformat(),
                "amount": baseline,
            }
            for offset in range(5, 0, -1)
        ] + [{"date": latest_date.isoformat(), "turnover": latest}]
    return result


def test_future_daily_bar_is_ignored_and_result_is_json_serializable() -> None:
    universe = _universe(1.0, 1.0, -1.0, -1.0)
    symbols = [row["symbol"] for row in universe]
    bars = _bars(symbols, baseline=100.0, latest=100.0)
    bars[symbols[0]].append({"date_ms": int((AS_OF.date() + timedelta(days=1)).strftime("%Y%m%d")), "amount": 99_999.0})

    result = build_market_funding_regime(universe, bars, as_of=AS_OF)

    assert result["available"] is True
    assert result["latest_trade_date"] == AS_OF.date().isoformat()
    assert result["latest_total_amount"] == pytest.approx(400.0)
    assert result["evidence"]["future_bars_dropped"] == 1
    assert "FUTURE_BARS_EXCLUDED" in result["data_gaps"]
    json.dumps(result, allow_nan=False)


def test_latest_session_below_coverage_is_unresolved() -> None:
    universe = _universe(1.0, 1.0, -1.0, -1.0)
    symbols = [row["symbol"] for row in universe]
    bars = _bars(symbols, baseline=100.0, latest=100.0)
    bars.pop(symbols[-1])

    result = build_market_funding_regime(universe, bars, as_of=AS_OF)

    assert result["available"] is False
    assert result["state"] == "UNRESOLVED"
    assert result["coverage"] == pytest.approx(0.75)
    assert "LATEST_SESSION_COVERAGE_INSUFFICIENT" in result["reason_codes"]


def test_latest_and_baseline_use_one_comparable_symbol_pool() -> None:
    universe = _universe(1.0, 1.0, -1.0)
    symbols = [row["symbol"] for row in universe]
    bars = _bars(symbols, baseline=100.0, latest=200.0)
    # The baseline is only available for the first two symbols.  The extra
    # symbol is deliberately outside the universe and must never enter totals.
    for symbol in symbols[2:]:
        bars[symbol] = [{"date": AS_OF.date().isoformat(), "amount": 1_000_000.0}]
    bars["OUTSIDE.SH"] = [{"date": AS_OF.date().isoformat(), "amount": 1_000_000_000.0}]

    result = build_market_funding_regime(
        universe,
        bars,
        as_of=AS_OF,
        min_coverage=0.5,
    )

    assert result["available"] is True
    assert result["comparable_symbol_count"] == 2
    assert result["coverage"] == pytest.approx(2 / 3)
    assert result["latest_total_amount"] == pytest.approx(400.0)
    assert result["baseline_total_amount"] == pytest.approx(200.0)
    assert result["amount_ratio"] == pytest.approx(2.0)
    assert result["state"] == "INCREMENTAL_EXPANSION"
    assert "OUTSIDE.SH" not in result["evidence"]["comparable_symbols"]


@pytest.mark.parametrize(
    ("latest", "changes", "expected"),
    [
        (108.0, (1.0, 1.0, 1.0, -1.0), "INCREMENTAL_EXPANSION"),
        (100.0, (1.0, 1.0, -1.0, -1.0), "EXISTING_FUNDS_ROTATION"),
        (92.0, (1.0, 1.0, 1.0, -1.0), "LIQUIDITY_CONTRACTION"),
    ],
)
def test_three_funding_states_are_deterministic(
    latest: float,
    changes: tuple[float, ...],
    expected: str,
) -> None:
    universe = _universe(*changes)
    symbols = [row["symbol"] for row in universe]
    result = build_market_funding_regime(
        universe,
        _bars(symbols, baseline=100.0, latest=latest),
        as_of=AS_OF,
    )

    assert result["available"] is True
    assert result["state"] == expected
    assert result["turnover_is_capital_flow"] is False
    assert result["state_is_execution_context_only"] is True
    assert "score" not in result
