from __future__ import annotations

import json
import uuid
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from liangjian_funnel.pipeline.model_client import ModelCallResult
from liangjian_funnel.pipeline.prompts import PromptRepository
from liangjian_funnel.review.daily import A5DailyReviewService, A5ReviewKind, build_a5_fact_snapshot
from liangjian_funnel.runtime.state import MonitorAction, PlanStatus, RuntimeStore


TZ = ZoneInfo("Asia/Shanghai")
ROOT = Path(__file__).resolve().parents[1]


def _seed(store: RuntimeStore, output_dir: Path) -> None:
    output_dir.joinpath("research").mkdir(parents=True)
    output_dir.joinpath("research", "research_close-run_lane_1.json").write_text(
        json.dumps(
            {
                "stages": [
                    {
                        "stage": "A2",
                        "status": "VALIDATED",
                        "reason_codes": [],
                        "output": {
                            "active_themes": [{"theme_id": "AI", "theme_name": "人工智能", "theme_score": 88}],
                            "focus_pool": [{"symbol": "000001.SZ", "name": "测试股份", "theme_id": "AI", "market_role": "LEADER", "selection_reasons": ["板块共振"]}],
                            "watch_only_pool": [{"symbol": "000002.SZ", "name": "观察股份", "theme_id": "AI"}],
                            "rejected_candidates": [],
                        },
                    }
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    store.create_execution_plan(
        "plan-1", "lane_1", "000001.SZ", status=PlanStatus.ACTIVE_TODAY,
        valid_from=datetime(2026, 9, 3, 9, 26, tzinfo=TZ),
        expires_at=datetime(2026, 9, 3, 15, 0, tzinfo=TZ),
        payload={
            "source_run_id": "close-run", "name": "测试股份", "stock_behavior_type": "TREND",
            "strategy_profile": "TREND_MA5", "plan_priority": "P1", "trigger_low": 10,
            "trigger_high": 10.2, "stop_level": 9.7, "selection_reasons": ["日线趋势确认"],
        },
    )
    store.record_monitor_event(
        event_key="a4-event-1", lane_id="lane_1",
        minute_end=datetime(2026, 9, 3, 10, 15, tzinfo=TZ),
        action=MonitorAction.BUY_SIGNAL, reason_code="DETERMINISTIC_TRIGGER_PASS",
        effective=True,
        payload={"plan_id": "plan-1", "symbol": "000001.SZ", "strategy": {"strategy_profile": "TREND_MA5"}},
    )


def _report(*, include_counterexample: bool = False) -> dict:
    layer = {"verdict": "HEALTHY", "summary": "事实链完整。", "strengths": [], "defects": [], "evidence_ids": ["METRICS:DAILY"], "data_limitations": []}
    return {
        "schema_version": "a5-daily-review/1.0.0", "review_kind": "MIDDAY", "trade_date": "2026-09-03",
        "overall_verdict": "HEALTHY", "executive_summary": "A2 至 A4 链路已产生可复盘事实。",
        "sample_sufficient_for_strategy_change": False,
        "a2_review": layer, "a3_review": layer, "a4_review": layer,
        "signal_reviews": [{"symbol": "000001.SZ", "name": "测试股份", "strategy_profile": "TREND_MA5", "lifecycle_status": "SIGNALLED", "assessment": "信号已记录，仍需后续表现。", "evidence_ids": ["A4:EVENT:" + str(uuid.uuid5(uuid.NAMESPACE_URL, "liangjian-monitor:a4-event-1"))], "attribution": "NOT_AN_ERROR"}],
        "missed_opportunity_reviews": ([{
            "symbol": "000002.SZ", "name": "观察股份", "theme": "人工智能",
            "observed_performance": "候选域内涨幅位居前列", "funnel_drop_stage": "A2",
            "assessment": "生产结果未聚焦，仍需核对当时硬条件。",
            "evidence_ids": ["A5V:MISS:000002.SZ"], "is_confirmed_defect": False,
        }] if include_counterexample else []),
        "core_defects": [], "improvement_proposals": [], "data_collection_tasks": ["继续跟踪离场结果"],
        "unresolved_questions": [{"question": "单日样本能否稳定复现", "reason": "INSUFFICIENT_SAMPLE", "resolution": "累计至少十个交易日。"}],
    }


class _Model:
    def __init__(self, *, include_counterexample: bool = False):
        self.calls = 0
        self.include_counterexample = include_counterexample

    def complete(self, model, messages, **kwargs):
        self.calls += 1
        return ModelCallResult(model=model, output=_report(include_counterexample=self.include_counterexample), prompt_hash=kwargs.get("prompt_hash"), input_hash=kwargs.get("input_hash"), latency_ms=12, attempts=1, thinking_variant="reasoning_effort_low")


class _Verifier:
    def verify(self, **kwargs):
        return {
            "schema_version": "a5-independent-verification/1.0.0", "status": "READY",
            "evidence_id": "A5V:SUMMARY", "counterexamples": [{
                "evidence_id": "A5V:MISS:000002.SZ", "symbol": "000002.SZ",
                "drop_stage": "A2_NOT_FOCUSED",
            }],
        }


class _FailingPublisher:
    def __init__(self):
        self.calls = 0

    def publish_a5_review(self, review, *, now):
        self.calls += 1
        raise RuntimeError("simulated delivery failure")


def test_a5_snapshot_joins_a2_a3_a4_without_live_data(tmp_path: Path):
    store = RuntimeStore(tmp_path / "state.sqlite3")
    output_dir = tmp_path / "outputs"
    _seed(store, output_dir)
    snapshot = build_a5_fact_snapshot(
        store, output_dir, trade_date=date(2026, 9, 3),
        cutoff_at=datetime(2026, 9, 3, 11, 30, tzinfo=TZ),
        review_kind=A5ReviewKind.MIDDAY, lane_id="lane_1",
    )
    assert snapshot["metrics"]["a2_focus_count"] == 1
    assert snapshot["metrics"]["a2_watch_count"] == 1
    assert snapshot["metrics"]["a3_plan_count"] == 1
    assert snapshot["metrics"]["a4_effective_event_count"] == 1
    assert snapshot["data_quality"]["status"] == "READY"


def test_a5_service_persists_markdown_and_is_idempotent(tmp_path: Path):
    store = RuntimeStore(tmp_path / "state.sqlite3")
    output_dir = tmp_path / "outputs"
    _seed(store, output_dir)
    model = _Model()
    service = A5DailyReviewService(
        store=store, prompts=PromptRepository(ROOT / "prompts"), model_client=model,
        output_dir=output_dir, lane_id="lane_1", model="deepseek-v4-pro-0813",
    )
    first = service.run(review_kind=A5ReviewKind.MIDDAY, now=datetime(2026, 9, 3, 11, 36, tzinfo=TZ))
    second = service.run(review_kind=A5ReviewKind.MIDDAY, now=datetime(2026, 9, 3, 11, 40, tzinfo=TZ))
    assert first["created"] is True
    assert second["created"] is False
    assert model.calls == 1
    assert Path(str(first["markdown_path"])).is_file()
    assert str(first["markdown_path"]).endswith(f"midday-{first['input_hash'][:12]}.md")
    assert "不修改生产策略" in Path(str(first["markdown_path"])).read_text(encoding="utf-8")
    assert len(store.list_a5_reviews()) == 1


def test_a5_service_accepts_independent_counterexample_evidence(tmp_path: Path):
    store = RuntimeStore(tmp_path / "state.sqlite3")
    output_dir = tmp_path / "outputs"
    _seed(store, output_dir)
    service = A5DailyReviewService(
        store=store, prompts=PromptRepository(ROOT / "prompts"),
        model_client=_Model(include_counterexample=True), output_dir=output_dir,
        lane_id="lane_1", model="deepseek-v4-pro-0813",
        independent_verifier=_Verifier(),
    )
    result = service.run(review_kind=A5ReviewKind.MIDDAY, now=datetime(2026, 9, 3, 11, 36, tzinfo=TZ))
    markdown = Path(str(result["markdown_path"])).read_text(encoding="utf-8")
    assert "反向拷问" in markdown
    assert "观察股份" in markdown


def test_a5_delivery_failure_does_not_rollback_review_or_repeat_model(tmp_path: Path):
    store = RuntimeStore(tmp_path / "state.sqlite3")
    output_dir = tmp_path / "outputs"
    _seed(store, output_dir)
    model = _Model()
    publisher = _FailingPublisher()
    service = A5DailyReviewService(
        store=store, prompts=PromptRepository(ROOT / "prompts"), model_client=model,
        output_dir=output_dir, lane_id="lane_1", model="deepseek-v4-pro-0813",
        notification_publisher=publisher,
    )

    first = service.run(
        review_kind=A5ReviewKind.MIDDAY,
        now=datetime(2026, 9, 3, 11, 36, tzinfo=TZ),
    )
    second = service.run(
        review_kind=A5ReviewKind.MIDDAY,
        now=datetime(2026, 9, 3, 11, 40, tzinfo=TZ),
    )

    assert first["status"] == "COMPLETED"
    assert first["notifications"] == [{"status": "FAILED", "reason_code": "LARK_NOTIFICATION_FAILED"}]
    assert second["created"] is False
    assert model.calls == 1
    assert publisher.calls == 2
    assert len(store.list_a5_reviews()) == 1


def test_a5_zero_plan_day_still_reviews_latest_primary_a2(tmp_path: Path):
    store = RuntimeStore(tmp_path / "state.sqlite3")
    output_dir = tmp_path / "outputs"
    output_dir.joinpath("research").mkdir(parents=True)
    output_dir.joinpath("research", "research_previous-close_lane_1.json").write_text(
        json.dumps({
            "stages": [{
                "stage": "A2", "status": "VALIDATED", "reason_codes": ["A2_NO_FOCUS_OPPORTUNITY"],
                "output": {"active_themes": [], "focus_pool": [], "watch_only_pool": [], "rejected_candidates": []},
            }],
        }),
        encoding="utf-8",
    )
    store.record_workflow_run(
        run_id="previous-close", lane_id="lane_1", trade_date="2026-09-02", slot="close",
        model="deepseek-v4-pro-0813", status="PUBLISHED", snapshot_hash="a" * 64,
    )

    snapshot = build_a5_fact_snapshot(
        store, output_dir, trade_date=date(2026, 9, 3),
        cutoff_at=datetime(2026, 9, 3, 11, 30, tzinfo=TZ),
        review_kind=A5ReviewKind.MIDDAY, lane_id="lane_1",
    )

    assert snapshot["source_run_ids"] == ["previous-close"]
    assert snapshot["a2"]["status"] == "VALIDATED"
    assert snapshot["metrics"]["a3_plan_count"] == 0
    assert snapshot["data_quality"]["status"] == "READY"


def test_a5_review_ledger_rejects_content_conflict(tmp_path: Path):
    store = RuntimeStore(tmp_path / "state.sqlite3")
    kwargs = dict(
        trade_date="2026-09-03", review_kind="MIDDAY", cutoff_at=datetime(2026, 9, 3, 11, 30, tzinfo=TZ),
        status="COMPLETED", model="deepseek-v4-pro-0813", source_run_ids=["r1"], input_hash="a" * 64,
        prompt_hash="b" * 64, output_hash="c" * 64, latency_ms=1, attempts=1,
        thinking_variant="reasoning_effort_low", fact_snapshot={"x": 1}, report={"y": 1}, markdown_path="x.md",
    )
    _row, inserted = store.record_a5_review(**kwargs)
    same, inserted_again = store.record_a5_review(**kwargs)
    assert inserted is True and inserted_again is False
    assert same["review_kind"] == "MIDDAY"
