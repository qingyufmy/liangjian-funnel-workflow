from __future__ import annotations

import json
import copy
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.runtime.lark import LarkDeliveryResult
from liangjian_funnel.runtime.lark_notifications import WorkflowLarkPublisher
from liangjian_funnel.runtime.state import RuntimeStore


SHANGHAI = ZoneInfo("Asia/Shanghai")


class FakeNotifier:
    enabled = True

    def __init__(self):
        self.calls: list[tuple[str, list[str], str]] = []

    def send(self, title: str, body: list[str], color: str) -> LarkDeliveryResult:
        self.calls.append((title, list(body), color))
        return LarkDeliveryResult(True, "LARK_SENT", 200, 1)


def test_a5_failure_is_once_per_period_and_success_emits_one_recovery(tmp_path):
    store = RuntimeStore(tmp_path / "a5-health.sqlite3")
    publisher = WorkflowLarkPublisher(
        store,
        "https://open.larksuite.com/open-apis/bot/v2/hook/test-token",
    )
    fake = FakeNotifier()
    publisher.notifier = fake
    now = datetime(2026, 9, 21, 16, 2, tzinfo=SHANGHAI)
    diagnostics = {
        "prompt_chars": 286031,
        "limit_chars": 250000,
        "input_hash": "a" * 64,
    }

    first = publisher.publish_a5_task_failure(
        "POST_CLOSE",
        reason_code="A5_MODEL_CONTEXT_TOO_LARGE",
        diagnostics=diagnostics,
        now=now,
    )
    duplicate = publisher.publish_a5_task_failure(
        "POST_CLOSE",
        reason_code="A5_MODEL_CONTEXT_TOO_LARGE",
        diagnostics=diagnostics,
        now=now,
    )
    recovered = publisher.publish_a5_task_recovery(
        "POST_CLOSE",
        input_hash="a" * 64,
        now=now,
    )
    recovery_duplicate = publisher.publish_a5_task_recovery(
        "POST_CLOSE",
        input_hash="a" * 64,
        now=now,
    )

    assert first["status"] == "SENT" and duplicate["duplicate"] is True
    assert recovered["status"] == "SENT" and recovery_duplicate["duplicate"] is True
    assert len(fake.calls) == 2
    failure_body = "\n".join(fake.calls[0][1])
    assert "A5_MODEL_CONTEXT_TOO_LARGE" in failure_body
    assert "286031" in failure_body and "250000" in failure_body
    assert "不可以，需先修复失败原因" in failure_body
    assert len(store.list_notification_deliveries(kind="A5_TASK_FAILURE")) == 1
    assert len(store.list_notification_deliveries(kind="A5_TASK_RECOVERY")) == 1


def test_a5_evidence_failure_card_does_not_mislabel_input_as_oversize(tmp_path):
    store = RuntimeStore(tmp_path / "a5-output-health.sqlite3")
    publisher = WorkflowLarkPublisher(
        store, "https://open.larksuite.com/open-apis/bot/v2/hook/test-token",
    )
    fake = FakeNotifier()
    publisher.notifier = fake
    publisher.publish_a5_task_failure(
        "POST_CLOSE", reason_code="A5_OUTPUT_EVIDENCE_INVALID",
        diagnostics={"prompt_chars": 146914, "limit_chars": 250000,
                     "input_hash": "a" * 64, "invalid_evidence_count": 1},
        now=datetime(2026, 9, 22, 16, 4, tzinfo=SHANGHAI),
    )
    body = "\n".join(fake.calls[0][1])
    assert "模型已返回结果" in body
    assert "输入检查：未超限" in body
    assert "不是输入超限" in body


def test_premarket_empty_scope_status_is_once_per_day_and_not_a_plan(tmp_path):
    store = RuntimeStore(tmp_path / "premarket.sqlite3")
    publisher = WorkflowLarkPublisher(
        store, "https://open.larksuite.com/open-apis/bot/v2/hook/test-token",
    )
    fake = FakeNotifier()
    publisher.notifier = fake
    now = datetime(2026, 9, 22, 8, 30, tzinfo=SHANGHAI)
    first = publisher.publish_a3_premarket_status(
        analyzed_at=now, reason_code="NO_PENDING_A3_PLANS",
    )
    again = publisher.publish_a3_premarket_status(
        analyzed_at=now, reason_code="NO_PENDING_A3_PLANS",
    )
    assert first["status"] == "SENT" and again["duplicate"] is True
    assert len(fake.calls) == 1
    title, lines, _color = fake.calls[0]
    assert "盘前计划未就绪" in title
    assert "今日有效 A3 计划：0 只" in "\n".join(lines)
    assert "不构成买入信号" in "\n".join(lines)
    assert store.list_notification_deliveries(kind="PREMARKET_A3_STATUS")


def _plan(index: int) -> dict[str, object]:
    symbol = f"0000{index:02d}.SZ"
    return {
        "plan_id": f"run:lane_1:plan-{index}",
        "lane_id": "lane_1",
        "symbol": symbol,
        "payload_json": json.dumps(
            {
                "name": f"测试股票{index}",
                "source_run_id": "run-close-1",
                "theme": "AI算力",
                "strategy_profile": "TREND_MA5",
                "plan_priority": "P1" if index == 2 else "P2",
                "priority_reasons": ["QUALIFIED_STANDARD", "STRONG_SETUP:MAIN_RISE"],
                "selection_reasons": ["周日趋势保持多头", "板块与个股共振"],
                "reference_price": 10.2 + index,
                "reference_price_as_of": "2026-09-01T15:00:00+08:00",
                "trigger_low": 10 + index,
                "trigger_high": 10.5 + index,
                "stop_level": 9.5 + index,
                "no_chase_price": 10.8 + index,
                "pressure_reduce_price": 12.0 + index,
                "pressure_basis": "FIRST_RESISTANCE",
                "required_conditions": ["5分钟趋势确认", "成交量不背离"],
            },
            ensure_ascii=False,
        ),
    }


def test_position_health_incident_change_recovery_survives_restart(tmp_path):
    from datetime import timedelta
    store = RuntimeStore(tmp_path / "health.sqlite3")
    publisher = WorkflowLarkPublisher(store, "https://open.larksuite.com/open-apis/bot/v2/hook/test-token")
    fake = FakeNotifier()
    publisher.notifier = fake
    now = datetime(2026, 9, 15, 13, 2, tzinfo=SHANGHAI)
    partial = {"lane_1:600001.SH": {"symbol": "600001.SH", "status": "HARD_STOP_ONLY"}}
    assert publisher.publish_position_data_health({}, now=now) == []
    assert publisher.publish_position_data_health(partial, now=now)[0]["status"] == "SENT"
    publisher = WorkflowLarkPublisher(store, "https://open.larksuite.com/open-apis/bot/v2/hook/test-token")
    publisher.notifier = fake
    assert publisher.publish_position_data_health(partial, now=now+timedelta(minutes=1)) == []
    partial["lane_1:600001.SH"]["status"] = "UNOBSERVABLE"
    assert publisher.publish_position_data_health(partial, now=now+timedelta(minutes=2))[0]["status"] == "SENT"
    assert publisher.publish_position_data_health({}, now=now+timedelta(minutes=3))[0]["status"] == "SENT"
    assert len(fake.calls) == 3
    assert "多周期退出暂不可核验" in str(fake.calls[0])
    assert "暂不可观测" in str(fake.calls[1])


def test_premarket_cards_are_chunked_colored_and_idempotent(tmp_path):
    store = RuntimeStore(tmp_path / "state.sqlite3")
    publisher = WorkflowLarkPublisher(
        store,
        "https://open.larksuite.com/open-apis/bot/v2/hook/test-token",
    )
    fake = FakeNotifier()
    publisher.notifier = fake
    now = datetime(2026, 9, 2, 9, 26, tzinfo=SHANGHAI)
    plans = [_plan(index) for index in range(1, 6)]
    evidence = {str(plan["symbol"]): {"price": 10.2} for plan in plans}

    first = publisher.publish_premarket(plans, reviewed_at=now, evidence=evidence)
    second = publisher.publish_premarket(plans, reviewed_at=now, evidence=evidence)

    assert [item["status"] for item in first] == ["SENT", "SENT"]
    assert all(item.get("duplicate") for item in second)
    assert len(fake.calls) == 2
    assert fake.calls[0][2] != fake.calls[1][2]
    body = "\n".join(fake.calls[0][1])
    assert "早盘复核结果" in body
    assert "适用策略" in body
    assert "禁止追价" in body
    assert "AI" not in body
    assert ".SZ" not in body
    assert len(store.list_notification_deliveries()) == 2


def test_a4_only_sends_effective_event_with_condition_logic(tmp_path):
    store = RuntimeStore(tmp_path / "state.sqlite3")
    publisher = WorkflowLarkPublisher(
        store,
        "https://open.larksuite.com/open-apis/bot/v2/hook/test-token",
    )
    fake = FakeNotifier()
    publisher.notifier = fake
    now = datetime(2026, 9, 2, 10, 5, tzinfo=SHANGHAI)
    plan = _plan(1)
    plan_payload = json.loads(str(plan["payload_json"]))
    plan_payload.update({
        "primary_theme": "CONSUMER_ELECTRONICS",
        "market_role": "TREND_CORE",
        "theme_stage": "ACCELERATING",
        "relative_strength": {"percentile": 88},
        "market_environment": "WEAK_ROTATION",
        "emotion_cycle_stage": "REPAIR",
        "market_funding_state": "EXISTING_FUNDS_ROTATION",
        "cycle_alignment": {"market_funding": {
            "state": "EXISTING_FUNDS_ROTATION", "amount_ratio": 1.03, "coverage": 0.96,
        }},
        "setup_pattern": "MAIN_RISE",
    })
    plan["payload_json"] = json.dumps(plan_payload, ensure_ascii=False)
    event_payload = {
        "plan_id": plan["plan_id"],
        "symbol": plan["symbol"],
        "entry_contract": {"status": "READY", "limit_price": 10.1, "signal_reference": 10.1},
        "strategy": {
            "met_conditions": ["首次回踩5日线企稳"],
            "unmet_conditions": [],
            "veto_conditions": ["放量跌破5日线"],
            "market_gate": {"status": "READY", "decision": "ALLOW", "as_of": now.isoformat()},
            "closed_5m_end": now.isoformat(),
            "closed_15m_end": now.replace(minute=0).isoformat(),
            "live_entry_price": 10.1,
            "live_reward_risk": 2.8,
        },
    }
    events = [
        {
            "event_key": "internal:noop",
            "lane_id": "lane_1",
            "minute_end": now.isoformat(),
            "action": "NO_ACTION",
            "reason_code": "STRATEGY_WAITING",
            "effective": 0,
            "payload_json": "{}",
        },
        {
            "event_key": "effective:lane_1:plan:BUY_SIGNAL",
            "lane_id": "lane_1",
            "minute_end": now.isoformat(),
            "action": "BUY_SIGNAL",
            "reason_code": "DETERMINISTIC_TRIGGER_PASS",
            "effective": 1,
            "payload_json": json.dumps(event_payload, ensure_ascii=False),
        },
    ]

    result = publisher.publish_a4_events(
        events,
        plans={str(plan["plan_id"]): plan},
        now=now,
    )

    assert len(result) == 1
    assert len(fake.calls) == 1
    body = "\n".join(fake.calls[0][1])
    assert "首次回踩5日线企稳" in body
    assert "放量跌破5日线" in body
    assert "NO_ACTION" not in body
    assert "待成交" in fake.calls[0][0]
    assert "入场理由" in body
    assert "当日实时许可为允许关注" in body
    assert "消费电子" in body
    assert "存量资金轮动" in body
    assert "市场成交额比103.00%" in body
    assert "板块净流入未形成可核验数值" in body
    assert "实时盈亏比2.8" in body


def test_a4_fill_receipt_explains_price_basis_and_frozen_reasons(tmp_path):
    store = RuntimeStore(tmp_path / "state.sqlite3")
    publisher = WorkflowLarkPublisher(
        store,
        "https://open.larksuite.com/open-apis/bot/v2/hook/test-token",
    )
    fake = FakeNotifier()
    publisher.notifier = fake
    now = datetime(2026, 9, 18, 10, 39, tzinfo=SHANGHAI)
    result = publisher.publish_a4_execution_results([{
        "account_id": "paper:lane_1",
        "signal_id": "effective:signal-1",
        "symbol": "300136.SZ",
        "name": "信维通信",
        "action": "BUY",
        "status": "FILLED",
        "reason_code": "FILLED",
        "qty": 2500,
        "price": 58.43,
        "fee": 45.29,
        "signal_time": "2026-09-18T10:38:00+08:00",
        "signal_reference": 58.32,
        "limit_price": 58.45,
        "fill_delay_seconds": 60,
        "execution_basis": "NEXT_COMPLETE_1M_BAR_SIMULATION",
        "execution_bar": {
            "source_id": "MOOTDX:10.0.0.1:7709",
            "bar_end": now.isoformat(),
            "open": 58.35, "high": 58.50, "low": 58.30, "close": 58.43,
            "volume": 640000,
        },
        "decision_context": {
            "environment": {"live_decision": "ALLOW", "live_as_of": now.isoformat(),
                            "market_environment": "WEAK_ROTATION", "emotion_cycle_stage": "REPAIR"},
            "sector": {"theme_name": "消费电子", "market_role": "TREND_CORE",
                       "theme_stage": "ACCELERATING", "relative_strength_percentile": 88},
            "capital": {"state": "EXISTING_FUNDS_ROTATION", "amount_ratio": 1.03, "coverage": 0.96},
            "strategy": {"profile": "TREND_MA5", "setup_pattern": "MAIN_RISE",
                         "met_conditions": ["五分钟转强"], "closed_5m_end": now.isoformat()},
            "price": {"live_reward_risk": 2.8},
        },
        "account_snapshot": {"equity": 9950704.51, "cash": 6369756.71,
                             "position_total_qty": 2500, "position_sellable_qty": 0},
    }], now=now)

    assert result[0]["status"] == "SENT"
    title, lines, _color = fake.calls[0]
    body = "\n".join(lines)
    assert "信维通信（300136）" in title
    assert "信号参考价：58.32" in body
    assert "模拟成交价：58.43" in body
    assert "下一根可交易的完整一分钟K线模拟撮合" in body
    assert "不是交易所真实成交回报" in body
    assert "通达信一分钟行情" in body
    assert "市场环境" in body and "板块位置" in body and "资金条件" in body and "技术策略" in body
    assert "当日可卖：0股" in body
    assert "NEXT_TICK" not in body and "一档可见量" not in body


@pytest.mark.parametrize("action", ["PLAN_INVALIDATED", "LLM_VETO", "START_CONFIRMATION", "NO_ACTION", "EMPTY_SCOPE"])
def test_normal_state_events_are_silent_and_not_mutated(tmp_path, action):
    store = RuntimeStore(tmp_path / "state.sqlite3")
    publisher = WorkflowLarkPublisher(store, "https://open.larksuite.com/open-apis/bot/v2/hook/test-token")
    fake = FakeNotifier()
    publisher.notifier = fake
    event = {"effective": 1, "action": action, "plan_id": "p", "payload_json": "{}"}
    original = copy.deepcopy(event)
    assert publisher.publish_a4_events([event], plans={}, now=datetime(2026, 9, 11, 9, 45, tzinfo=SHANGHAI)) == []
    assert not fake.calls and not store.list_notification_deliveries()
    assert event == original


@pytest.mark.parametrize("action", ["BUY_SIGNAL", "ADD_SIGNAL", "DATA_BLOCK"])
def test_unready_data_is_alert_not_trade_signal(tmp_path, action):
    store = RuntimeStore(tmp_path / "state.sqlite3")
    publisher = WorkflowLarkPublisher(store, "https://open.larksuite.com/open-apis/bot/v2/hook/test-token")
    fake = FakeNotifier()
    publisher.notifier = fake
    event = {"effective": 1, "action": action, "plan_id": "p", "lane_id": "lane_1",
        "reason_code": "MINUTE_DATA_MISSING", "payload_json": "{}"}
    now = datetime(2026, 9, 11, 9, 45, tzinfo=SHANGHAI)
    result = publisher.publish_a4_events([event], plans={}, now=now)
    publisher.publish_a4_events([event], plans={}, now=now)
    assert result[0]["status"] == "SENT" and len(fake.calls) == 1
    assert fake.calls[0][0].startswith("A4 数据告警")
    assert "不是交易信号" in "\n".join(fake.calls[0][1])
    assert store.list_notification_deliveries()[0]["kind"] == "A4_DATA_ALERT"


def test_systemic_timestamp_block_is_not_sent_once_per_stock(tmp_path):
    store = RuntimeStore(tmp_path / "state.sqlite3")
    publisher = WorkflowLarkPublisher(store, "https://open.larksuite.com/open-apis/bot/v2/hook/test-token")
    fake = FakeNotifier()
    publisher.notifier = fake
    event = {"effective": 1, "action": "DATA_BLOCK", "plan_id": "p", "lane_id": "lane_1",
        "reason_code": "STALE_1M", "payload_json": "{}"}
    assert publisher.publish_a4_events([event], plans={}, now=datetime(2026, 9, 21, 10, 0, tzinfo=SHANGHAI)) == []
    assert not fake.calls and not store.list_notification_deliveries()


def test_expected_auxiliary_deferral_does_not_toggle_source_alert(tmp_path):
    store = RuntimeStore(tmp_path / "state.sqlite3")
    publisher = WorkflowLarkPublisher(store, "https://open.larksuite.com/open-apis/bot/v2/hook/test-token")
    fake = FakeNotifier()
    publisher.notifier = fake
    now = datetime(2026, 9, 21, 10, 0, tzinfo=SHANGHAI)
    result = publisher.publish_minute_source_health(
        {}, auxiliary_failures={"600519.SH": "AUXILIARY_5M_DEFERRED"}, now=now,
    )
    assert result == []
    assert not fake.calls and not store.list_notification_deliveries()


@pytest.mark.parametrize("action", ["SELL_SIGNAL", "REDUCE_SIGNAL", "FORCED_RISK_EXIT"])
def test_position_risk_events_still_notify_t1_constraint(tmp_path, action):
    store = RuntimeStore(tmp_path / "state.sqlite3")
    publisher = WorkflowLarkPublisher(store, "https://open.larksuite.com/open-apis/bot/v2/hook/test-token")
    fake = FakeNotifier()
    publisher.notifier = fake
    event = {"effective": 1, "action": action, "plan_id": "p", "lane_id": "lane_1",
        "payload_json": json.dumps({"strategy": {"execution_eligibility": {
            "reason": "BLOCKED_T1", "total_qty": 100, "sellable_qty": 0}}})}
    publisher.publish_a4_events([event], plans={}, now=datetime(2026, 9, 11, 9, 45, tzinfo=SHANGHAI))
    assert len(fake.calls) == 1
    assert "T+1限制，尚未执行" in "\n".join(fake.calls[0][1])


def _a4_health_state(
    now: datetime,
    *,
    status: str,
    recovered_after_retry: bool = False,
) -> dict[str, object]:
    ready = status in {"READY", "READY_DEGRADED"}
    return {
        "status": status,
        "source": "HITHINK_FULL_MARKET" if ready else "TENCENT_INDEX_FALLBACK",
        "reason_code": "A4_LIVE_MARKET_CAUTION" if ready else "A4_LIVE_MARKET_SOURCE_UNAVAILABLE",
        "as_of": now.isoformat(),
        "trade_date": now.date().isoformat(),
        "cache_bucket": now.replace(minute=now.minute - now.minute % 5).isoformat(),
        "diagnostics": {
            "total_attempts": 2,
            "recovered_after_retry": recovered_after_retry,
            "attempts": [
                {
                    "attempt": 2,
                    "full_market": {
                        "status": "READY" if ready else "DATA_BLOCKED",
                        "reason_code": "OK" if ready else "A4_LIVE_MARKET_FULL_REQUEST_FAILED",
                        "observed_count": 5_549 if ready else 0,
                        "expected_count": 5_566 if ready else 0,
                    },
                    "index_fallback": {
                        "status": "NOT_ATTEMPTED" if ready else "DATA_BLOCKED",
                        "reason_code": "FULL_MARKET_READY" if ready else "A4_LIVE_MARKET_SOURCE_UNAVAILABLE",
                        "observed_count": 0,
                        "expected_count": 4,
                    },
                }
            ],
        },
    }


def test_a4_system_health_alert_is_aggregated_idempotent_and_recovers_once(tmp_path):
    store = RuntimeStore(tmp_path / "system-health.sqlite3")
    publisher = WorkflowLarkPublisher(
        store,
        "https://open.larksuite.com/open-apis/bot/v2/hook/test-token",
    )
    fake = FakeNotifier()
    publisher.notifier = fake
    blocked_at = datetime(2026, 9, 3, 14, 45, tzinfo=SHANGHAI)
    blocked = _a4_health_state(blocked_at, status="DATA_BLOCKED")

    first = publisher.publish_a4_system_health(blocked, affected_plan_count=5, now=blocked_at)
    duplicate = publisher.publish_a4_system_health(blocked, affected_plan_count=5, now=blocked_at)

    assert first[0]["status"] == "SENT"
    assert duplicate[0]["duplicate"] is True
    assert len(fake.calls) == 1
    title, lines, _color = fake.calls[0]
    body = "\n".join(lines)
    assert "系统告警" in title
    assert "受影响计划：5 只" in body
    assert "同花顺全市场行情" in body
    assert "腾讯指数兜底行情" in body
    assert "A4_LIVE" not in body

    # Five plans blocked by the same market state must not produce five more
    # stock cards; the aggregate system card above is the only alert.
    events = [
        {
            "event_key": f"effective:lane_1:plan-{index}:DATA_BLOCK",
            "lane_id": "lane_1",
            "minute_end": blocked_at.isoformat(),
            "action": "DATA_BLOCK",
            "reason_code": "LIVE_MARKET_STATE_NOT_READY",
            "effective": 1,
            "payload_json": json.dumps({"plan_id": f"plan-{index}", "symbol": f"00000{index}.SZ"}),
        }
        for index in range(1, 6)
    ]
    assert publisher.publish_a4_events(events, plans={}, now=blocked_at) == []
    assert len(fake.calls) == 1

    recovered_at = datetime(2026, 9, 3, 14, 50, tzinfo=SHANGHAI)
    recovered = _a4_health_state(recovered_at, status="READY")
    recovery = publisher.publish_a4_system_health(recovered, affected_plan_count=5, now=recovered_at)
    assert recovery[0]["status"] == "SENT"
    assert "系统恢复" in fake.calls[-1][0]

    later_at = datetime(2026, 9, 3, 14, 55, tzinfo=SHANGHAI)
    later = _a4_health_state(later_at, status="READY")
    assert publisher.publish_a4_system_health(later, affected_plan_count=5, now=later_at) == []
    assert len(fake.calls) == 2


def test_a4_system_health_reports_same_run_retry_recovery_once(tmp_path):
    store = RuntimeStore(tmp_path / "retry-recovery.sqlite3")
    publisher = WorkflowLarkPublisher(
        store,
        "https://open.larksuite.com/open-apis/bot/v2/hook/test-token",
    )
    fake = FakeNotifier()
    publisher.notifier = fake
    now = datetime(2026, 9, 3, 10, 5, tzinfo=SHANGHAI)
    ready = _a4_health_state(now, status="READY", recovered_after_retry=True)

    first = publisher.publish_a4_system_health(ready, affected_plan_count=3, now=now)
    duplicate = publisher.publish_a4_system_health(ready, affected_plan_count=3, now=now)

    assert first[0]["status"] == "SENT"
    assert duplicate[0]["duplicate"] is True
    assert len(fake.calls) == 1
    assert "重试后恢复" in "\n".join(fake.calls[0][1])


def test_a4_system_health_understands_fallback_only_diagnostics(tmp_path):
    store = RuntimeStore(tmp_path / "fallback-health.sqlite3")
    publisher = WorkflowLarkPublisher(store, "https://open.larksuite.com/open-apis/bot/v2/hook/test-token")
    fake = FakeNotifier()
    publisher.notifier = fake
    now = datetime(2026, 9, 21, 11, 15, tzinfo=SHANGHAI)
    state = {
        "status": "DATA_BLOCKED", "source": "TENCENT_INDEX_FALLBACK",
        "reason_code": "A4_LIVE_MARKET_SOURCE_UNAVAILABLE",
        "as_of": now.isoformat(), "trade_date": now.date().isoformat(),
        "observed_count": 1, "expected_count": 4,
        "diagnostics": {
            "fallback_only": True,
            "full_market_reason_code": "A4_LIVE_MARKET_DEADLINE_EXCEEDED",
            "index_quotes": [],
        },
    }
    publisher.publish_a4_system_health(state, affected_plan_count=48, now=now)
    summary = json.loads(store.list_notification_deliveries(kind="A4_SYSTEM_HEALTH")[0]["payload_json"])
    assert summary["full_market_status"] == "DATA_BLOCKED"
    assert summary["full_market_reason_code"] == "A4_LIVE_MARKET_DEADLINE_EXCEEDED"
    assert summary["index_fallback_status"] == "DATA_BLOCKED"
    assert summary["index_fallback_reason_code"] == "A4_LIVE_MARKET_SOURCE_UNAVAILABLE"
    assert summary["index_fallback_observed_count"] == 1


def test_a2_rotation_health_alerts_in_chinese_and_recovers_once(tmp_path):
    store = RuntimeStore(tmp_path / "rotation-health.sqlite3")
    publisher = WorkflowLarkPublisher(
        store,
        "https://open.larksuite.com/open-apis/bot/v2/hook/test-token",
    )
    fake = FakeNotifier()
    publisher.notifier = fake
    now = datetime(2026, 9, 4, 15, 26, tzinfo=SHANGHAI)
    blocked = {
        "available": False,
        "trade_date": "2026-09-04",
        "source_id": "LIANGJIAN_FREE_ROTATION_V1",
        "reason_code": "ROTATION_MEMBERSHIP_EXPIRED",
        "selected_primary_boards": [],
        "quality": {"membership_age_days": 15},
    }

    first = publisher.publish_rotation_theme_health(blocked, now=now)
    duplicate = publisher.publish_rotation_theme_health(blocked, now=now)

    assert first[0]["status"] == "SENT"
    assert duplicate[0]["duplicate"] is True
    assert len(fake.calls) == 1
    title, lines, _color = fake.calls[0]
    body = "\n".join(lines)
    assert "板块数据阻断" in title
    assert "超过允许使用期限" in body
    assert "ROTATION_" not in body
    assert "LIANGJIAN_" not in body

    recovered = {
        "available": True,
        "trade_date": "2026-09-04",
        "source_id": "LIANGJIAN_FREE_ROTATION_V1",
        "selected_board_count": 5,
        "selected_primary_boards": [{"board_code": f"T{i}"} for i in range(5)],
        "quality": {"warning_codes": []},
    }
    recovery = publisher.publish_rotation_theme_health(recovered, now=now.replace(minute=31))
    assert recovery[0]["status"] == "SENT"
    assert "系统恢复" in fake.calls[-1][0]
    assert publisher.publish_rotation_theme_health(recovered, now=now.replace(minute=36)) == []
    assert len(fake.calls) == 2


def test_delivery_timestamp_is_acknowledgement_not_business_cutoff(tmp_path):
    store = RuntimeStore(tmp_path / "state.db")
    started = datetime(2026, 9, 8, 17, 5, tzinfo=SHANGHAI)
    ack = started.replace(second=2)
    clock = iter([started, ack])
    publisher = WorkflowLarkPublisher(store, None, clock=lambda: next(clock))
    publisher.notifier = FakeNotifier()
    publisher._send(delivery_key="timestamp", kind="A5", source_id="review", title="复盘", lines=["完成"],
                    summary={}, now=started.replace(hour=16, minute=0))
    row = store.get_delivery_by_key("timestamp")
    assert row["created_at"] == started.isoformat()
    assert row["sent_at"] == ack.isoformat()
    assert json.loads(row["payload_json"])["business_at"] == started.replace(hour=16, minute=0).isoformat()


def test_a5_review_card_is_structured_chinese_and_idempotent(tmp_path):
    store = RuntimeStore(tmp_path / "state.sqlite3")
    publisher = WorkflowLarkPublisher(
        store,
        "https://open.larksuite.com/open-apis/bot/v2/hook/test-token",
    )
    fake = FakeNotifier()
    publisher.notifier = fake
    now = datetime(2026, 9, 3, 16, 5, tzinfo=SHANGHAI)
    review = {
        "review_id": "a5-review-1",
        "trade_date": "2026-09-03",
        "review_kind": "POST_CLOSE",
        "cutoff_at": "2026-09-03T15:00:00+08:00",
        "fact_snapshot_json": json.dumps(
            {
                "metrics": {
                    "a2_focus_count": 61,
                    "a2_watch_count": 42,
                    "a2_theme_count": 3,
                    "a3_plan_count": 9,
                    "a4_effective_event_count": 2,
                    "a4_lifecycle_count": 2,
                },
                "data_quality": {"status": "READY"},
                "independent_verification": {"status": "DEGRADED"},
            },
            ensure_ascii=False,
        ),
        "report_json": json.dumps(
            {
                "trade_date": "2026-09-03",
                "review_kind": "POST_CLOSE",
                "overall_verdict": "NEEDS_ATTENTION",
                "executive_summary": "A2 至 A4 已完成，Tencent 异源数据存在 DATA_LIMITED。",
                "a2_review": {"verdict": "HEALTHY", "summary": "板块方向与异源行情基本一致。"},
                "a3_review": {"verdict": "NEEDS_ATTENTION", "summary": "MA5 计划需要继续核对。"},
                "a4_review": {"verdict": "DATA_LIMITED", "summary": "部分分钟行情尚未完整。"},
                "signal_reviews": [{
                    "symbol": "002837.SZ", "name": "英维克",
                    "strategy_profile": "TREND_MA5", "attribution": "GOOD_EXECUTION",
                    "assessment": "按 TREND_MA5 完成确认。",
                }],
                "missed_opportunity_reviews": [{
                    "symbol": "000001.SZ", "name": "平安银行",
                    "funnel_drop_stage": "A2", "observed_performance": "收盘表现较强",
                    "assessment": "A2_NOT_FOCUSED，需要复核板块宽度。", "is_confirmed_defect": False,
                }],
                "signal_stock_reviews": [{"symbol": "002837.SZ", "name": "英维克",
                    "performance_summary": "较昨收+1.00%；价格表现不是已实现收益",
                    "entry_audit_summary": "模拟成交100股；当日新买入股份受T+1限制，当日不可卖"}],
                "core_defects": [{
                    "layer": "A3", "severity": "MEDIUM",
                    "problem": "MA20 数据覆盖不足。", "blocked_by_data": True,
                }],
                "improvement_proposals": [{
                    "type": "DATA_FIX", "target": "A3",
                    "proposed_change": "补齐 TDX 日线并做 SHADOW_TEST。", "min_shadow_days": 10,
                }],
            },
            ensure_ascii=False,
        ),
    }

    facts_payload = json.loads(review["fact_snapshot_json"])
    facts_payload["independent_verification"]["top_performance_ledger"] = [
        {"symbol": f"00000{i}.SZ", "coverage_status": "CAPTURED_EFFECTIVE_A4" if i == 0 else "A2_QUANT_FILTERED"}
        for i in range(7)
    ]
    report_payload = json.loads(review["report_json"])
    seed = report_payload["missed_opportunity_reviews"][0]
    report_payload["missed_opportunity_reviews"] = [
        {**seed, "symbol": f"00000{i}.SZ", "name": f"反例{i}"}
        for i in range(1, 7)
    ]
    review["fact_snapshot_json"] = json.dumps(facts_payload, ensure_ascii=False)
    review["report_json"] = json.dumps(report_payload, ensure_ascii=False)

    first = publisher.publish_a5_review(review, now=now)
    second = publisher.publish_a5_review(review, now=now)

    assert first[0]["status"] == "SENT"
    assert second[0]["duplicate"] is True
    assert len(fake.calls) == 1
    title, lines, _color = fake.calls[0]
    body = "\n".join(lines)
    assert title == "A股 A5 盘后复盘｜2026-09-03"
    for heading in ("复盘结论", "漏斗概况", "分层验收", "信号复盘", "反向拷问", "核心缺陷", "改进与验证", "执行边界"):
        assert heading in body
    for internal in ("NEEDS_ATTENTION", "DATA_LIMITED", "GOOD_EXECUTION", "TREND_MA5", "A2_NOT_FOCUSED", "DATA_FIX", "SHADOW_TEST", "Tencent", "TDX", ".SZ"):
        assert internal not in body
    assert "腾讯行情" in body
    assert "趋势 5 日线" in body
    assert "未进入 A2 聚焦池" in body
    assert "数据修复" in body
    assert "当日表现与入场审计" in body
    assert "较昨收+1.00%" in body
    assert "当日不可卖" in body
    assert "全市场强势覆盖 7 只，其中产生有效盘中信号 1 只" in body
    assert "其余 1 只已写入完整复盘" in body
    assert store.list_notification_deliveries(kind="A5_POST_CLOSE_REVIEW")


def test_a3_premarket_analysis_is_distinct_from_auction_activation(tmp_path):
    store = RuntimeStore(tmp_path / "state.sqlite3")
    publisher = WorkflowLarkPublisher(
        store,
        "https://open.larksuite.com/open-apis/bot/v2/hook/test-token",
    )
    fake = FakeNotifier()
    publisher.notifier = fake
    now = datetime(2026, 9, 2, 8, 30, tzinfo=SHANGHAI)
    plans = [_plan(1), _plan(2)]

    first = publisher.publish_a3_premarket_analysis(
        plans,
        analyzed_at=now,
        source_run_id="run-close-1",
        research_context={
            "status": "READY",
            "model": "deepseek-v4-pro-0813",
            "market_trade_date": "2026-09-01",
            "target_trade_date": "2026-09-02",
            "a1": {
                "status": "VALIDATED",
                "active_count": 106,
                "macro": {
                    "liquidity_condition": "NEUTRAL",
                    "profit_cycle_position": "RECOVERY",
                    "policy_direction": ["科技自立", "能源资源安全"],
                    "key_uncertainties": ["海外流动性扰动"],
                },
                "monthly_industries": [
                    {"name": "农业种植", "return_5d": 0.08, "relative_strength_percentile_20d": 95}
                ],
            },
            "a2": {
                "status": "VALIDATED",
                "active_themes": [
                    {
                        "name": "农业种植",
                        "score": 88,
                        "weekly_state": "ACCELERATING",
                        "new_entry_policy": "ALLOW",
                        "breadth": 82,
                        "capital_flow": 76,
                        "leader_structure": 90,
                        "tier_structure": 70,
                        "index_chain_resonance": 85,
                        "chase_risk_level": "MEDIUM",
                    }
                ],
            },
            "a3": {
                "status": "VALIDATED",
                "market_open_constraints": {
                    "regime": "ROTATION",
                    "new_entry_allowed": True,
                    "total_position_cap_pct": 0.5,
                },
            },
        },
    )
    second = publisher.publish_a3_premarket_analysis(
        plans,
        analyzed_at=now,
        source_run_id="run-close-1",
        research_context={"status": "READY"},
    )

    assert len(first) == 2
    assert first[0]["status"] == "SENT"
    assert first[1]["status"] == "SENT"
    assert second[0]["duplicate"] is True
    assert second[1]["duplicate"] is True
    assert fake.calls[0][0].startswith("A股专业盘前研究")
    body = "\n".join(fake.calls[0][1])
    plan_body = "\n".join(fake.calls[1][1])
    assert "09:26 独立竞价复核" in body
    assert "科技自立" in body
    assert "农业种植" in body
    assert "梯队 70" in body
    assert "测试股票1" in plan_body
    assert "方向：人工智能算力" in plan_body
    assert "000001" in plan_body
    assert ".SZ" not in plan_body
    assert "run-close-1" not in plan_body
    assert "三种情景" in plan_body
    assert "常规优先" in plan_body
    assert "P2" not in plan_body
    assert "READY" not in body
    assert "ACCELERATING" not in body
    assert "ALLOW" not in body
    assert plan_body.index("测试股票2") < plan_body.index("测试股票1")
    assert "参考收盘" in plan_body
    assert "压力参考" in plan_body
    assert store.list_notification_deliveries(kind="PREMARKET_A3_ANALYSIS")
    assert not store.list_notification_deliveries(kind="PREMARKET_A3")


def test_a3_premarket_repairs_cross_date_recovery_timestamp(tmp_path):
    store = RuntimeStore(tmp_path / "state.sqlite3")
    publisher = WorkflowLarkPublisher(
        store,
        "https://open.larksuite.com/open-apis/bot/v2/hook/test-token",
    )
    fake = FakeNotifier()
    publisher.notifier = fake
    plan = _plan(1)
    payload = json.loads(str(plan["payload_json"]))
    payload["reference_price_as_of"] = "2026-09-02T10:04:58+08:00"
    plan["payload_json"] = json.dumps(payload, ensure_ascii=False)

    result = publisher.publish_a3_premarket_analysis(
        [plan],
        analyzed_at=datetime(2026, 9, 2, 10, 20, tzinfo=SHANGHAI),
        source_run_id="run-close-1",
        research_context={
            "status": "READY",
            "market_trade_date": "2026-09-01",
            "target_trade_date": "2026-09-02",
            "a1": {"macro": {}},
            "a2": {"active_themes": []},
            "a3": {"market_open_constraints": {}},
        },
        activation_state="ACTIVE_CURRENT_SESSION",
    )

    assert [item["status"] for item in result] == ["SENT", "SENT"]
    plan_body = "\n".join(fake.calls[1][1])
    assert "2026年09月01日 15:00" in plan_body
    assert "2026-09-02T10:04:58+08:00" not in plan_body


def test_missing_webhook_is_disabled_without_delivery_row(tmp_path):
    store = RuntimeStore(tmp_path / "state.sqlite3")
    publisher = WorkflowLarkPublisher(store, None)

    result = publisher.publish_premarket(
        [_plan(1)],
        reviewed_at=datetime(2026, 9, 2, 9, 26, tzinfo=SHANGHAI),
        evidence={},
    )

    assert result == [{"status": "DISABLED", "reason_code": "LARK_WEBHOOK_NOT_CONFIGURED"}]
    assert store.list_notification_deliveries() == ()


def test_file_webhook_is_reloaded_without_restarting_publisher(tmp_path):
    store = RuntimeStore(tmp_path / "state.sqlite3")
    config_path = tmp_path / "state" / "lark_webhook.json"
    publisher = WorkflowLarkPublisher(store, None, webhook_path=config_path)

    assert publisher.enabled is False
    config_path.parent.mkdir(parents=True)
    config_path.write_text(
        json.dumps(
            {
                "schemaVersion": 1,
                "webhookUrl": "https://open.larksuite.com/open-apis/bot/v2/hook/test-runtime-token",
                "updatedAt": "2026-09-01T09:00:00.000Z",
            }
        ),
        encoding="utf-8",
    )
    assert publisher.enabled is True

    config_path.write_text("{}", encoding="utf-8")
    assert publisher.enabled is False
