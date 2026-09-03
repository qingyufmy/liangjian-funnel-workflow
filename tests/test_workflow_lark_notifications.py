from __future__ import annotations

import json
from datetime import datetime
from zoneinfo import ZoneInfo

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
    event_payload = {
        "plan_id": plan["plan_id"],
        "symbol": plan["symbol"],
        "strategy": {
            "met_conditions": ["首次回踩5日线企稳"],
            "unmet_conditions": [],
            "veto_conditions": ["放量跌破5日线"],
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
