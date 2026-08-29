from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from liangjian_funnel.evaluation.replay_window import evaluate_replay_window


TZ = ZoneInfo("Asia/Shanghai")
CUTOFF = datetime(2026, 8, 20, 15, 30, tzinfo=TZ)


def _hash(value: object) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _trading_dates(count: int) -> list[date]:
    result: list[date] = []
    current = date(2026, 8, 3)
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current)
        current += timedelta(days=1)
    return result


def _write_fixture(root: Path, *, count: int = 10, include_future: bool = False) -> tuple[Path, Path]:
    runs = root / "runs"
    audits = root / "research"
    runs.mkdir(parents=True)
    audits.mkdir(parents=True)
    for index, trade_date in enumerate(_trading_dates(count), start=1):
        _write_run(runs, audits, trade_date, index=index)
    if include_future:
        _write_run(runs, audits, date(2026, 8, 21), index=99)
    return runs, audits


def _write_run(runs: Path, audits: Path, trade_date: date, *, index: int) -> None:
    run_id = f"{trade_date.isoformat()}-close-test-{index}"
    snapshot_data = {
        "trade_date": trade_date.isoformat(),
        "index": index,
        "universe_count": 1000,
    }
    snapshot_id = f"snapshot-{trade_date.isoformat()}-{index}"
    snapshot_hash = _hash(snapshot_data)
    as_of = datetime(trade_date.year, trade_date.month, trade_date.day, 15, 10, tzinfo=TZ).isoformat()

    def stage(stage_name: str, symbols: list[str], input_count: int, output: dict[str, list[dict[str, object]]]) -> dict[str, object]:
        selected = len(symbols)
        return {
            "stage": stage_name,
            "status": "VALIDATED",
            "snapshot_id": snapshot_id,
            "symbols": symbols,
            "input_count": input_count,
            "evaluated_count": input_count,
            "output_count": selected,
            "output": output,
            "outcome_v2": {
                "schema_version": "research-outcome/2.0.0",
                "stage": stage_name,
                "lifecycle_state": "TERMINAL",
                "quality_state": "VALIDATED",
                "opportunity_state": "PRESENT",
                "publication_state": "READY",
                "reason_codes": [],
                "counts": {"input": input_count, "evaluated": input_count, "selected": selected},
                "data_coverage": {"required": input_count, "actual": input_count},
                "legacy_status": "VALIDATED",
            },
        }

    a1_symbols = ["600001.SH", "600002.SH"]
    a2_symbols = ["600001.SH"]
    a3_symbols = ["600001.SH"]
    audit = {
        "lane": "lane_1",
        "model": "test-model",
        "status": "READY",
        "snapshot_id": snapshot_id,
        "snapshot_hash": snapshot_hash,
        "as_of": as_of,
        "trade_date": trade_date.isoformat(),
        "stages": [
            stage("A1", a1_symbols, 1000, {"active_research_pool": [{"symbol": symbol} for symbol in a1_symbols]}),
            stage("A2", a2_symbols, 2, {"focus_pool": [{"symbol": a2_symbols[0]}]}),
            stage("A3", a3_symbols, 1, {"core_watch_pool": [{"symbol": a3_symbols[0]}]}),
        ],
    }
    summary = {
        "run_id": run_id,
        "slot": "close",
        "status": "READY",
        "test_only": True,
        "trade_date": trade_date.isoformat(),
        "snapshot": {
            "snapshot_id": snapshot_id,
            "snapshot_hash": snapshot_hash,
            "as_of": as_of,
            "trade_date": trade_date.isoformat(),
            "research_universe_count": 1000,
        },
    }
    (runs / f"{run_id}.json").write_text(json.dumps(summary, ensure_ascii=False), encoding="utf-8")
    (audits / f"research_{run_id}_lane_1.json").write_text(json.dumps(audit, ensure_ascii=False), encoding="utf-8")


def test_ten_test_only_point_in_time_days_produce_replay_report(tmp_path: Path) -> None:
    runs, audits = _write_fixture(tmp_path)
    broker_dir = tmp_path / "broker_gold"
    broker_dir.mkdir()
    broker_dir.joinpath("2026-08.json").write_text(
        json.dumps(
            [
                {"month": "2026-08", "broker": "甲券商", "symbol": "600001.SH", "source_ref": "fixture:1", "publish_time": "2026-08-01T09:00:00+08:00"},
                {"month": "2026-08", "broker": "乙券商", "symbol": "600002.SH", "source_ref": "fixture:2", "publish_time": "2026-08-01T09:00:00+08:00"},
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    output_json = tmp_path / "reports" / "replay.json"
    output_md = tmp_path / "reports" / "replay.md"

    report = evaluate_replay_window(
        runs,
        audit_dir=audits,
        broker_gold_dir=broker_dir,
        cutoff=CUTOFF,
        output_json=output_json,
        output_markdown=output_md,
    )

    assert report["status"] == "READY"
    assert report["summary"]["independent_trading_days"] == 10
    assert report["summary"]["terminal_days"] == 10
    assert report["summary"]["classification_counts"] == {
        "SUCCESS": 10,
        "EMPTY_OPPORTUNITY": 0,
        "DATA_INSUFFICIENT": 0,
        "EXECUTION_FAILURE": 0,
        "UNKNOWN": 0,
    }
    assert report["summary"]["success_rate"] == 1.0
    assert report["summary"]["stage_conversion_rates"]["A1"]["observations"] == 10
    assert report["days"][0]["stages"]["A1"]["selected_count"] == 2
    assert report["days"][0]["stages"]["A2"]["selected_count"] == 1
    assert report["days"][0]["stages"]["A3"]["selected_count"] == 1
    assert report["days"][0]["four_axes"]["quality_state"] == "VALIDATED"
    assert report["summary"]["broker_gold_available_days"] == 10
    assert output_json.is_file() and output_md.is_file()
    persisted = json.loads(output_json.read_text(encoding="utf-8"))
    assert persisted["report_hash"] == report["report_hash"]
    assert "逐日结果" in output_md.read_text(encoding="utf-8")


def test_nine_independent_days_are_explicitly_blocked(tmp_path: Path) -> None:
    runs, audits = _write_fixture(tmp_path, count=9)

    report = evaluate_replay_window(runs, audit_dir=audits, cutoff=CUTOFF)

    assert report["status"] == "BLOCKED_REPLAY_WINDOW_INSUFFICIENT"
    assert report["summary"]["independent_trading_days"] == 9
    assert any(reason["code"] == "BLOCKED_REPLAY_WINDOW_INSUFFICIENT" for reason in report["blocking_reasons"])
    assert report["summary"]["minimum_days"] == 10


def test_future_point_in_time_data_is_rejected_and_not_counted(tmp_path: Path) -> None:
    runs, audits = _write_fixture(tmp_path, include_future=True)

    report = evaluate_replay_window(runs, audit_dir=audits, cutoff=CUTOFF)

    assert report["status"] == "BLOCKED_REPLAY_FUTURE_DATA"
    assert report["future_data_rejected"] == 1
    assert report["summary"]["independent_trading_days"] == 10
    assert any(reason["code"] == "REPLAY_FUTURE_DATA_REJECTED" for reason in report["blocking_reasons"])


def test_duplicate_primary_run_same_day_is_not_counted_as_another_day(tmp_path: Path) -> None:
    runs, audits = _write_fixture(tmp_path, count=10)
    # Select the source run deterministically and derive its audit from that
    # run id.  ``Path.glob`` iteration order is filesystem-dependent; picking
    # the two files independently can accidentally pair different trading
    # days and exercise the identity-mismatch guard instead of duplicate-day
    # detection.
    source_run_path = sorted(runs.glob("*.json"))[0]
    source_run = json.loads(source_run_path.read_text(encoding="utf-8"))
    source_run_id = source_run["run_id"]
    duplicate = source_run.copy()
    duplicate["run_id"] = "duplicate-same-day"
    (runs / "duplicate-same-day.json").write_text(json.dumps(duplicate), encoding="utf-8")
    # Use the audit belonging to the same source run, so both summaries
    # resolve to the same snapshot/date before duplicate-day validation.
    source_audit_path = audits / f"research_{source_run_id}_lane_1.json"
    duplicate_audit = json.loads(source_audit_path.read_text(encoding="utf-8"))
    (audits / "research_duplicate-same-day_lane_1.json").write_text(json.dumps(duplicate_audit), encoding="utf-8")

    report = evaluate_replay_window(runs, audit_dir=audits, cutoff=CUTOFF)

    assert report["status"] == "BLOCKED_REPLAY_DUPLICATE_PRIMARY_DAY"
    assert report["summary"]["independent_trading_days"] == 9
    assert any(reason["code"] == "REPLAY_DUPLICATE_PRIMARY_TRADE_DATE" for reason in report["blocking_reasons"])
