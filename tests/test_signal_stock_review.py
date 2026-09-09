from datetime import datetime
from zoneinfo import ZoneInfo

from liangjian_funnel.review.signal_audit import build_signal_stock_reviews

TZ = ZoneInfo("Asia/Shanghai")


def at(value):
    return f"2026-09-09T{value}:00+08:00"


def fixture():
    strategy = {"reference_price": 10, "live_reward_risk": 3, "minimum_reward_risk": 2.5,
                "closed_5m_end": at("09:45"), "closed_15m_end": at("09:45"),
                "confirmation_results": {"test": {"available": True, "met": True}}}
    event = {"event_id": "e", "event_key": "signal-1", "action": "BUY_SIGNAL", "effective": True,
             "minute_end": at("09:51"), "payload_json": {"symbol": "000001.SZ", "plan_id": "p",
             "minute_snapshot_id": "frozen-1", "llm_reason_code": "LLM_PASS", "llm_veto": False, "strategy": strategy}}
    fill = {"fill_id": "f", "signal_id": "signal-1", "action": "BUY", "symbol": "000001.SZ",
            "bar_end": at("09:52"), "qty": 100, "price": 10.2, "fee": 5, "account_id": "paper"}
    def bar(t, close, high, low):
        return {"bar_end": at(t), "close": close, "high": high, "low": low}
    market = {"000001.SZ": {"previous_close": 8, "expected_minutes": 120, "source": "FROZEN",
              "bars": [bar("09:52", 10.3, 15, 5), bar("11:30", 11, 11.2, 10.1), bar("13:01", 99, 99, 99)]}}
    return event, fill, market


def run(events, fills, market):
    return build_signal_stock_reviews(events, [{"plan_id": "p", "payload_json": {"name": "测试"}}],
                                      fills, market, datetime(2026, 9, 9, 11, 30, tzinfo=TZ))


def test_returns_are_separate_and_fill_minute_extremes_excluded():
    event, fill, market = fixture()
    row = run([event], [fill], market)[0]
    p, a = row["performance"], row["entry_audit"]
    assert p["day_return_pct"] == 37.5
    assert p["signal_return_pct"] == 10
    assert p["entry_mark_return_pct"] == 7.8431
    assert p["post_entry_high_return_pct"] == 9.8039
    assert p["post_entry_low_return_pct"] == -0.9804
    assert p["coverage_complete"] is False
    assert a["state"] == "证据齐全" and a["entry_fee"] == 5
    assert "T+1" in a["t1_note"] and "不是已实现收益" in p["basis"]


def test_future_fill_other_signal_and_non_effective_do_not_leak():
    event, fill, market = fixture()
    future = {**fill, "bar_end": at("13:01")}
    other = {**fill, "signal_id": "signal-2"}
    row = run([event, {**event, "effective": False}], [future, other], market)[0]
    assert row["entry_audit"]["fill_qty"] == 0
    assert row["performance"]["entry_mark_return_pct"] is None
    assert row["performance"]["last_price"] == 11


def test_missing_market_is_unknown_not_zero_and_false_confirmation_is_flagged():
    event, fill, _ = fixture()
    event["payload_json"]["strategy"]["confirmation_results"]["test"]["met"] = False
    row = run([event], [fill], {})[0]
    assert row["performance"]["day_return_pct"] is None
    assert row["entry_audit"]["state"] == "需核对入场证据"
    assert "资料不足" in row["performance_summary"]


def test_multiple_signals_same_stock_join_exact_fill_and_exit_not_counted_as_entry():
    event, fill, market = fixture()
    other = {**event, "event_id": "e2", "event_key": "signal-2", "minute_end": at("10:15")}
    sell = {**event, "event_id": "e3", "event_key": "signal-3", "action": "SELL_SIGNAL"}
    rows = run([event, other, sell], [fill], market)
    assert [r["entry_audit"]["fill_qty"] for r in rows] == [100, 0, 0]
    assert rows[2]["entry_audit"]["state"] == "退出事件（非新入场）"


def test_duplicate_minute_does_not_inflate_coverage():
    event, fill, market = fixture()
    market["000001.SZ"]["bars"] *= 2
    row = run([event], [fill], market)[0]
    assert row["performance"]["observed_minute_count"] == 2


def test_unfilled_reason_is_exact_signal_and_not_future_state():
    event, _, market = fixture()
    def audit(updated):
        return build_signal_stock_reviews([event], [], [], market, datetime(2026, 9, 9, 11, 30, tzinfo=TZ),
            lifecycles=[{"entry_event_key": "signal-1", "status": "UNFILLED", "exit_reason": "PRICE_OUTSIDE_BAR", "updated_at": updated}])[0]
    assert audit(at("09:52"))["entry_audit"]["unfilled_reason_code"] == "PRICE_OUTSIDE_BAR"
    assert audit(at("13:01"))["entry_audit"]["unfilled_reason_code"] is None
