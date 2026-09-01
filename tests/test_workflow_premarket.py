from __future__ import annotations

import json
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from liangjian_funnel.runtime.state import PlanStatus, RuntimeStore
from liangjian_funnel.workflow import WorkflowApplication


TZ = ZoneInfo("Asia/Shanghai")


class FakePublisher:
    def __init__(self) -> None:
        self.calls: list[tuple[tuple[dict[str, object], ...], datetime, str]] = []

    def publish_a3_premarket_analysis(self, plans, *, analyzed_at, source_run_id):
        self.calls.append((tuple(plans), analyzed_at, source_run_id))
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


def _plan(store, plan_id, symbol, source):
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
            "selection_reasons": ["日线趋势右侧确认"],
            "trigger_low": 10,
            "trigger_high": 10.5,
            "stop_level": 9.5,
            "no_chase_price": 10.8,
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
    assert store.get_execution_plan("old-plan")["status"] == PlanStatus.PENDING_MORNING_REVIEW.value
    assert store.get_execution_plan("new-plan")["status"] == PlanStatus.PENDING_MORNING_REVIEW.value
    report = tmp_path / "outputs" / "runs" / "2026-09-02-a3-premarket.json"
    saved = json.loads(report.read_text(encoding="utf-8"))
    assert saved["activation_deferred_to"] == "09:26"
    markdown = report.with_suffix(".md").read_text(encoding="utf-8")
    assert "000002.SZ" in markdown
    assert "09:26 竞价复核前不会激活 A4" in markdown


def test_a3_premarket_empty_scope_is_explicit_and_does_not_notify(tmp_path):
    app, store, publisher = _app(tmp_path)
    current = datetime(2026, 9, 2, 8, 30, tzinfo=TZ)

    result = WorkflowApplication.publish_a3_premarket_analysis(app, now=current)

    assert result["status"] == "EMPTY_SCOPE"
    assert result["reason_code"] == "NO_PENDING_A3_PLANS"
    assert publisher.calls == []
    markdown = tmp_path / "outputs" / "runs" / "2026-09-02-a3-premarket.md"
    assert "NO_PENDING_A3_PLANS" in markdown.read_text(encoding="utf-8")
