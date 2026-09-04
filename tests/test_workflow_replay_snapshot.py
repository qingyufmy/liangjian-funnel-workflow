import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel import workflow as workflow_module
from liangjian_funnel.settings import Settings
from liangjian_funnel.workflow import WorkflowApplication, WorkflowError
from scripts.replay_frozen_research import _resume_stage_rows


TZ = ZoneInfo("Asia/Shanghai")


def _application(tmp_path: Path) -> WorkflowApplication:
    application = WorkflowApplication.__new__(WorkflowApplication)
    application.settings = Settings.from_env({}, root=tmp_path)
    return application


def test_verified_snapshot_replay_loads_full_g0_without_live_fetch(tmp_path: Path):
    application = _application(tmp_path)
    snapshot_id = "snapshot-20260828T210944+0800-fixture"
    data = {
        "g0_symbols": ["600519.SH", "000001.SZ"],
        "universe_candidates": [{"symbol": "600519.SH"}, {"symbol": "000001.SZ"}],
        "trade_candidates": [{"symbol": "600519.SH"}],
    }
    payload = {
        "snapshot_id": snapshot_id,
        "snapshot_hash": workflow_module._hash_json(data),
        "as_of": datetime(2026, 8, 28, 21, 9, 44, tzinfo=TZ).isoformat(),
        "data": data,
    }
    application.settings.snapshot_dir.mkdir(parents=True, exist_ok=True)
    (application.settings.snapshot_dir / f"{snapshot_id}.json").write_text(
        json.dumps(payload, ensure_ascii=False), encoding="utf-8"
    )

    loaded = application._load_research_snapshot_by_id(snapshot_id, expected_date="2026-08-28")
    assert loaded.snapshot.snapshot_hash == payload["snapshot_hash"]
    assert loaded.selected_count == 2
    assert loaded.trade_universe_count == 1


def test_snapshot_replay_rejects_hash_and_date_mismatch(tmp_path: Path):
    application = _application(tmp_path)
    snapshot_id = "snapshot-20260828T210944+0800-bad"
    application.settings.snapshot_dir.mkdir(parents=True, exist_ok=True)
    (application.settings.snapshot_dir / f"{snapshot_id}.json").write_text(json.dumps({
        "snapshot_id": snapshot_id,
        "snapshot_hash": "bad",
        "as_of": datetime(2026, 8, 28, 21, 9, 44, tzinfo=TZ).isoformat(),
        "data": {"g0_symbols": ["600519.SH"]},
    }), encoding="utf-8")
    with pytest.raises(WorkflowError, match="SNAPSHOT_HASH_MISMATCH"):
        application._load_research_snapshot_by_id(snapshot_id, expected_date="2026-08-28")


def test_isolated_a2_replay_can_restore_a1_lineage_for_a3(tmp_path: Path):
    audit_root = tmp_path.resolve()
    source_path = audit_root / "source_lane.json"
    a1_stage = {
        "stage": "A1",
        "status": "VALIDATED",
        "snapshot_id": "snapshot-1",
        "output": {"active_research_pool": [{"symbol": "600001.SH"}]},
    }
    source_path.write_text(
        json.dumps({"stages": [a1_stage]}, ensure_ascii=False),
        encoding="utf-8",
    )
    a2_stage = {
        "stage": "A2",
        "status": "VALIDATED",
        "snapshot_id": "snapshot-1:a2",
        "output": {"focus_pool": [{"symbol": "600001.SH"}]},
    }

    previous, lineage = _resume_stage_rows(
        {
            "run_role": "A2_ISOLATED_REPLAY",
            "a2_stage": a2_stage,
            "resume_source_audit": str(source_path),
        },
        stage="A3",
        audit_root=audit_root,
    )

    assert previous == a2_stage
    assert [item["stage"] for item in lineage] == ["A1", "A2"]
