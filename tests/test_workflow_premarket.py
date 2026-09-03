from __future__ import annotations

import json
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from liangjian_funnel.runtime.state import PlanStatus, RuntimeStore
from liangjian_funnel.workflow import WorkflowApplication, _build_premarket_research_context


TZ = ZoneInfo("Asia/Shanghai")


class FakePublisher:
    def __init__(self) -> None:
        self.calls: list[tuple[tuple[dict[str, object], ...], datetime, str, dict[str, object]]] = []

    def publish_a3_premarket_analysis(self, plans, *, analyzed_at, source_run_id, research_context):
        self.calls.append((tuple(plans), analyzed_at, source_run_id, dict(research_context)))
        return [{"status": "SENT"}]


def _app(tmp_path):
    store = RuntimeStore(tmp_path / "state.sqlite3")
    publisher = FakePublisher()
    app = SimpleNamespace(
        store=store,
        settings=SimpleNamespace(
            research_primary_lane_id="lane_1",
            workflow_output_dir=tmp_path / "outputs",
        ),
        lark_publisher=publisher,
        # The real method uses the exchange calendar here. The fixture keeps
        # that check deterministic and proves no account/quote dependency is
        # needed for the read-only report.
        _ensure_trading_day=lambda _current, *, synchronize_accounts=True: None,
    )
    return app, store, publisher


def _plan(store, plan_id, symbol, source, *, priority="P2"):
    return store.create_execution_plan(
        plan_id,
        "lane_1",
        symbol,
        status=PlanStatus.PENDING_MORNING_REVIEW,
        expires_at=datetime(2026, 9, 2, 15, 0, tzinfo=TZ),
        payload={
            "name": symbol,
            "source_run_id": source,
            "strategy_profile": "TREND_MA5",
            "plan_priority": priority,
            "priority_reasons": ["QUALIFIED_STANDARD"],
            "selection_reasons": ["日线趋势右侧确认"],
            "reference_price": 10.2,
            "reference_price_as_of": "2026-09-01T15:00:00+08:00",
            "trigger_low": 10,
            "trigger_high": 10.5,
            "stop_level": 9.5,
            "no_chase_price": 10.8,
            "pressure_reduce_price": 12.0,
            "pressure_basis": "FIRST_RESISTANCE",
            "required_conditions": ["回踩不破 5 日线"],
            "overnight_invalidators": ["跌破日线失效价"],
        },
    )


def test_a3_premarket_selects_latest_primary_batch_without_quote_or_activation(tmp_path):
    app, store, publisher = _app(tmp_path)
    _plan(store, "old-plan", "000001.SZ", "run-old")
    _plan(store, "new-plan", "000002.SZ", "run-new")
    # A comparison lane must never enter the primary premarket report.
    store.create_execution_plan(
        "comparison-plan",
        "lane_2",
        "000003.SZ",
        status=PlanStatus.PENDING_MORNING_REVIEW,
        expires_at=datetime(2026, 9, 2, 15, 0, tzinfo=TZ),
        payload={"source_run_id": "run-comparison", "name": "comparison"},
    )

    current = datetime(2026, 9, 2, 8, 30, tzinfo=TZ)
    result = WorkflowApplication.publish_a3_premarket_analysis(app, now=current)

    assert result["status"] == "READY"
    assert result["source_run_id"] == "run-new"
    assert result["plan_count"] == 1
    assert [item["symbol"] for item in result["plans"]] == ["000002.SZ"]
    assert len(publisher.calls) == 1
    assert [row["symbol"] for row in publisher.calls[0][0]] == ["000002.SZ"]
    assert publisher.calls[0][1] == current
    assert publisher.calls[0][2] == "run-new"
    assert publisher.calls[0][3]["status"] == "DEGRADED"
    assert publisher.calls[0][3]["reason_codes"] == ["PREMARKET_RESEARCH_CONTEXT_NOT_PERSISTED"]
    assert store.get_execution_plan("old-plan")["status"] == PlanStatus.PENDING_MORNING_REVIEW.value
    assert store.get_execution_plan("new-plan")["status"] == PlanStatus.PENDING_MORNING_REVIEW.value
    report = tmp_path / "outputs" / "runs" / "2026-09-02-a3-premarket.json"
    saved = json.loads(report.read_text(encoding="utf-8"))
    assert saved["activation_deferred_to"] == "09:26"
    assert saved["plans"][0]["plan_priority"] == "P2"
    assert saved["plans"][0]["reference_price"] == 10.2
    assert saved["plans"][0]["pressure_reduce_price"] == 12.0
    markdown = report.with_suffix(".md").read_text(encoding="utf-8")
    assert "000002.SZ" in markdown
    assert "09:26 竞价复核前不会激活 A4" in markdown
    assert "A2 主题上下文未持久化" in markdown


def test_a3_premarket_empty_scope_is_explicit_and_does_not_notify(tmp_path):
    app, store, publisher = _app(tmp_path)
    current = datetime(2026, 9, 2, 8, 30, tzinfo=TZ)

    result = WorkflowApplication.publish_a3_premarket_analysis(app, now=current)

    assert result["status"] == "EMPTY_SCOPE"
    assert result["reason_code"] == "NO_PENDING_A3_PLANS"
    assert publisher.calls == []
    markdown = tmp_path / "outputs" / "runs" / "2026-09-02-a3-premarket.md"
    assert "NO_PENDING_A3_PLANS" in markdown.read_text(encoding="utf-8")


def test_a3_premarket_loads_compact_source_context_without_lane_audit(tmp_path):
    app, store, publisher = _app(tmp_path)
    _plan(store, "plan-1", "000001.SZ", "run-compact")
    run_dir = tmp_path / "outputs" / "runs"
    run_dir.mkdir(parents=True)
    (run_dir / "run-compact.json").write_text(
        json.dumps(
            {
                "premarket_research_context": {
                    "schema_version": "liangjian-premarket-context/2.0.0",
                    "status": "READY",
                    "market_trade_date": "2026-09-01",
                    "a1": {"status": "VALIDATED", "macro": {}},
                    "a2": {"status": "VALIDATED", "active_themes": []},
                    "a3": {"status": "VALIDATED", "market_open_constraints": {}},
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    result = WorkflowApplication.publish_a3_premarket_analysis(
        app,
        now=datetime(2026, 9, 2, 8, 30, tzinfo=TZ),
    )

    assert result["research_context"]["status"] == "READY"
    assert result["research_context"]["market_trade_date"] == "2026-09-01"
    assert result["research_context"]["target_trade_date"] == "2026-09-02"
    assert publisher.calls[0][3]["status"] == "READY"


def test_compact_premarket_context_projects_a1_a2_a3_without_lane_audit():
    a1_output = {
        "analysis_summary": {"active_research_count": 106, "monitor_count": 3000},
        "macro_regime": {
            "liquidity_condition": "NEUTRAL",
            "profit_cycle_position": "RECOVERY",
            "policy_direction": ["科技自立"],
            "key_uncertainties": ["海外流动性"],
            "source_refs": ["official:macro"],
        },
        "structural_themes": [
            {
                "theme_id": "TH_AI",
                "display_name": "AI 算力",
                "capacity_score": 90,
                "weekly_state": "PERSISTENT",
                "weekly_confirmation": {"pricing_state": "PARTIALLY_PRICED"},
            }
        ],
        "monthly_industry_decisions": [
            {
                "final_decision": "INCLUDE",
                "rank": 1,
                "industry_name": "通信设备",
                "metrics": {"return_5d": 0.03, "return_20d": 0.2, "relative_strength_percentile_20d": 96},
            }
        ],
    }
    a2_output = {
        "analysis_summary": {"focus_pool": 8, "watch_only_pool": 20, "rejected_candidates": 30},
        "active_themes": [
            {
                "theme_id": "TH_AI",
                "theme_score": 88,
                "score_breakdown": {"breadth": 80, "capital_flow": 75, "leader_structure": 90, "tier_structure": 70},
            }
        ],
        "focus_pool": [{"symbol": "000001.SZ", "name": "测试", "theme_id": "TH_AI"}],
    }
    a3_output = {
        "analysis_summary": {"core_watch_pool": 7, "secondary_watch_pool": 3, "rejected_candidates": 20},
        "market_open_constraints": {"regime": "TREND_MAINLINE", "new_entry_allowed": True},
    }
    stages = tuple(
        SimpleNamespace(stage=name, status="VALIDATED", output=output, reason_codes=())
        for name, output in (("A1", a1_output), ("A2", a2_output), ("A3", a3_output))
    )
    result = SimpleNamespace(
        run_id="run-1",
        lanes=(SimpleNamespace(lane="lane_1", model="deepseek-v4-pro-0813", stages=stages),),
    )

    context = _build_premarket_research_context(
        result,
        primary_lane_id="lane_1",
        source_as_of=datetime(2026, 9, 1, 15, 10, tzinfo=TZ),
        market_trade_date="2026-09-01",
        target_trade_date="2026-09-02",
    )

    assert context["status"] == "READY"
    assert context["a1"]["monthly_industries"][0]["name"] == "通信设备"
    assert context["a2"]["active_themes"][0]["name"] == "AI 算力"
    assert context["a2"]["active_themes"][0]["capital_flow"] == 75
    assert context["a3"]["market_open_constraints"] == {
        "prior_market_environment": "TREND_MAINLINE",
        "recommended_position_min_pct": 0.5,
        "recommended_position_max_pct": 0.6,
        "a3_authority": "POSITION_GUIDANCE_ONLY",
        "a4_entry_authority": "CURRENT_SESSION_LIVE_STATE",
    }
    assert len(json.dumps(context, ensure_ascii=False)) < 20_000
