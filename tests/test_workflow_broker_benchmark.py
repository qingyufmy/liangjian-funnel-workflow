from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from liangjian_funnel.pipeline.research import LaneResult, ResearchRunResult, StageAudit
from liangjian_funnel.workflow import _write_broker_gold_benchmark


SHANGHAI = ZoneInfo("Asia/Shanghai")
AS_OF = datetime(2026, 8, 28, 15, 10, tzinfo=SHANGHAI)


def _result() -> ResearchRunResult:
    a1 = StageAudit(
        lane="lane_1",
        model="deepseek-v4-pro-0813",
        stage="A1",
        status="VALIDATED",
        snapshot_id="snapshot-1",
        prompt_hash="p",
        input_hash="i",
        output_hash="o",
        latency_ms=1,
        attempts=1,
        thinking_variant="enabled",
        symbols=("600001.SH",),
        reason_codes=(),
        output={
            "active_research_pool": [
                {
                    "symbol": "600001.SH",
                    "company_name": "甲公司",
                    "primary_theme": "T1",
                    "industry_chain_node": "N1",
                    "structural_score": 88,
                }
            ],
            "monitor_pool": [],
        },
    )
    lane = LaneResult(
        lane="lane_1",
        model="deepseek-v4-pro-0813",
        status="READY",
        stages=(a1,),
        final_output={},
    )
    return ResearchRunResult(
        run_id="2026-08-28-close-test",
        generated_at=AS_OF,
        snapshot_id="snapshot-1",
        snapshot_hash="hash",
        status="READY",
        lanes=(lane,),
        audit_paths=(),
        markdown_path=None,
    )


def test_missing_broker_benchmark_writes_explicit_non_runtime_report(tmp_path: Path) -> None:
    summary = _write_broker_gold_benchmark(
        _result(),
        as_of=AS_OF,
        benchmark_dir=tmp_path / "benchmarks",
        output_dir=tmp_path / "outputs",
    )

    assert summary["status"] == "NOT_CONFIGURED"
    assert summary["benchmark_not_runtime_input"] is True
    payload = json.loads(Path(summary["json_path"]).read_text(encoding="utf-8"))
    assert payload["reason_code"] == "BROKER_GOLD_BENCHMARK_NOT_CONFIGURED"
    assert payload["lanes"] == {}


def test_broker_benchmark_evaluates_each_a1_lane_without_mutating_result(tmp_path: Path) -> None:
    benchmark_dir = tmp_path / "benchmarks"
    benchmark_dir.mkdir()
    (benchmark_dir / "2026-08.json").write_text(
        json.dumps(
            [
                {
                    "month": "2026-08",
                    "broker": "甲券商",
                    "symbol": "600001.SH",
                    "name": "甲公司",
                    "publish_time": "2026-08-01T09:00:00+08:00",
                    "source_ref": "broker-public-1",
                }
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    result = _result()

    summary = _write_broker_gold_benchmark(
        result,
        as_of=AS_OF,
        benchmark_dir=benchmark_dir,
        output_dir=tmp_path / "outputs",
    )

    assert summary["status"] == "EVALUATED"
    payload = json.loads(Path(summary["json_path"]).read_text(encoding="utf-8"))
    assert payload["benchmark_not_runtime_input"] is True
    assert payload["lanes"]["lane_1"]["active_coverage"]["coverage"] == 1.0
    assert result.lanes[0].stages[0].symbols == ("600001.SH",)
    markdown = Path(summary["markdown_path"]).read_text(encoding="utf-8")
    assert "100.0%" in markdown
    assert "不参与A1运行时选股" in markdown
