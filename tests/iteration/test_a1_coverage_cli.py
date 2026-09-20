from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from liangjian_funnel.cli import _a1_coverage_command
from liangjian_funnel.pipeline.a1_coverage import (
    A1CoverageLedger,
    CoverageObservation,
    CoverageRequirement,
)


TZ = ZoneInfo("Asia/Shanghai")
AS_OF = datetime(2026, 9, 3, 15, 0, tzinfo=TZ)


def _settings(tmp_path: Path) -> SimpleNamespace:
    return SimpleNamespace(state_db_path=tmp_path / "state" / "runtime.sqlite3", root=tmp_path)


def _args(command: str, tmp_path: Path, *, dry_run: bool) -> argparse.Namespace:
    return argparse.Namespace(
        command=command,
        scope="A1_BASE",
        as_of=AS_OF.isoformat(),
        source_version="fixture:v1",
        output_dir=str(tmp_path / "reports"),
        dry_run=dry_run,
        limit=10,
        retry_budget=3,
    )


def _seed(settings: SimpleNamespace) -> A1CoverageLedger:
    ledger = A1CoverageLedger(settings.state_db_path.parent / "a1_registry.sqlite3")
    requirement = CoverageRequirement(
        symbol="600519.SH",
        dataset="INCOME",
        field="operating_income",
        report_period="2026Q2",
        as_of=AS_OF,
        source_version="fixture:v1",
        consumer_path="A1_BASE",
    )
    ledger.record(
        requirement,
        CoverageObservation(
            requested=False,
            raw_found=False,
            parsed=False,
            attempted_at=AS_OF,
        ),
        recorded_at=AS_OF,
    )
    return ledger


def test_a1_backfill_dry_run_is_read_only_and_writes_auditable_plan(
    tmp_path: Path,
    capsys,
) -> None:
    settings = _settings(tmp_path)
    ledger = _seed(settings)

    exit_code = _a1_coverage_command(_args("a1-backfill-plan", tmp_path, dry_run=True), settings)
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["status"] == "PLANNED"
    assert payload["plan"][0]["status"] == "PROPOSED"
    assert ledger.task_report()["by_status"] == {}
    assert Path(payload["output"]).is_file()


def test_a1_backfill_run_without_registered_adapter_fails_closed(
    tmp_path: Path,
    capsys,
) -> None:
    settings = _settings(tmp_path)
    ledger = _seed(settings)

    exit_code = _a1_coverage_command(_args("a1-backfill-run", tmp_path, dry_run=False), settings)
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["status"] == "BLOCKED"
    assert payload["reason_code"] == "A1_BACKFILL_SOURCE_ADAPTER_NOT_CONFIGURED"
    assert payload["source_fetch_triggered"] is False
    assert ledger.task_report()["by_status"] == {"QUEUED": 1}

