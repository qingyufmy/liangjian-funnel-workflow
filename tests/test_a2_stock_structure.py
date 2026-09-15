from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from liangjian_funnel.pipeline.a2_features import stock_trend_structure
from liangjian_funnel.pipeline.deterministic import _a2_behavior_evidence, _relative_strength_score

NOW = datetime(2026, 9, 15, 15, tzinfo=ZoneInfo("Asia/Shanghai"))


def bars(values):
    return [{"date_ms": int((NOW-timedelta(days=len(values)-i-1)).timestamp()*1000), "close_price": v}
            for i,v in enumerate(values)]


def test_structure_is_stock_local_not_return_rank():
    up = stock_trend_structure(bars(list(range(10, 31))), NOW.date())
    down = stock_trend_structure(bars(list(range(30, 9, -1))), NOW.date())
    flat = stock_trend_structure(bars([10]*21), NOW.date())
    assert up["structure_confirmed"] is True
    assert down["structure_confirmed"] is False
    assert flat["structure_confirmed"] is True
    assert up["ma20"] == 20.5 and up["previous_ma20"] == 19.5


def test_missing_stale_conflicting_and_future_bars():
    data = bars(list(range(10,31)))
    assert stock_trend_structure(data[:-1], NOW.date())["available"] is False
    assert stock_trend_structure(data[-20:], NOW.date())["available"] is False
    assert stock_trend_structure(data+[dict(data[-1], close_price=999)], NOW.date())["available"] is False
    assert stock_trend_structure(data+[data[-1]], NOW.date()) == stock_trend_structure(data, NOW.date())
    future = {"date_ms": int((NOW+timedelta(days=1)).timestamp()*1000), "close_price": 999}
    assert stock_trend_structure(data+[future], NOW.date()) == stock_trend_structure(data, NOW.date())


def test_missing_structure_never_falls_back_to_strong_peer_rank():
    evidence = _a2_behavior_evidence(item={}, factor_scores={
        "stock_trend_structure": stock_trend_structure([], NOW.date()),
        "trend_strength_proxy": {"available": True, "score": 99}},
        identifiability=90, minimum_identifiability_score=60, relative=99,
        liquidity=90, legacy_role="TREND_CORE", as_of=NOW.isoformat())
    assert evidence["medium_term_trend"]["available"] is False
    assert evidence["medium_term_trend"]["met"] is None
    assert evidence["relative_strength"]["met"] is True


def test_utc_daily_identity_is_converted_to_exchange_date():
    from datetime import timezone
    data = [{"timestamp": (NOW.replace(hour=0)-timedelta(days=20-i)).astimezone(timezone.utc).isoformat(),
             "close": 10+i} for i in range(21)]
    assert stock_trend_structure(data, NOW.date())["structure_confirmed"] is True


def test_recovery_is_not_rejected_only_because_ma20_lags():
    data = bars([30]+[10]*15+[11,12,13,14,15])
    value = stock_trend_structure(data, NOW.date())
    assert value["ma20"] < value["previous_ma20"]
    assert value["structure_phase"] == "RECOVERY_CANDIDATE"
    assert value["structure_confirmed"] is True
    # Being above a falling MA20 is insufficient if the short structure
    # is falling too; a one-day bounce alone is not an A2 recovery anchor.
    weak = stock_trend_structure(bars([30]+[10]*15+[17,16,15,14,13]), NOW.date())
    assert weak["structure_confirmed"] is False
    assert _relative_strength_score({"factors": {"stock_trend_structure": value}}, default=None) is None
