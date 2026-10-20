from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest

from liangjian_funnel.runtime.publication_archive import (
    capture_plan_originals, write_publication_manifest,
)
from liangjian_funnel.runtime.publication_receipt import object_hash


def source(tmp_path):
    raw = {"symbol": "603565.SH", "plan_id": "p", "plan_expiry": "2026-10-15T15:00:00+08:00"}
    a3 = {"stage": "A3", "lane": "lane_1", "status": "VALIDATED", "snapshot_id": "snap",
          "output": {"core_watch_pool": [raw]}}
    a3["output_hash"] = object_hash(a3["output"])
    audit = tmp_path / "audit.json"
    audit.write_text(json.dumps({"stages": [a3]}), encoding="utf-8")
    base = tmp_path / "snap.json"
    base.write_text(json.dumps({"snapshot_id": "snap", "snapshot_hash": object_hash({}), "data": {}}), encoding="utf-8")
    capture = capture_plan_originals(raw=raw, pool="core_watch_pool", index=0,
        a3_audit=a3, authority=None)
    row = {"plan_id": "run:lane_1:p", "lane_id": "lane_1", "symbol": "603565.SH",
           "payload": {**raw, "source_run_id": "run"}, "expires_at": "2026-10-16T15:00:00+08:00"}
    capture.update(normalized_payload={"value": deepcopy(raw), "sha256": object_hash(raw)},
        submitted_payload={"value": deepcopy(row["payload"]), "sha256": object_hash(row["payload"])},
        shadow_projection=None)
    return raw, a3, audit, base, capture, row


def write(tmp_path, capture, row, audit, base):
    return write_publication_manifest(output_dir=tmp_path / "proofs", run_id="run", lane_id="lane_1",
        base_snapshot_path=base, base_snapshot_id="snap", base_snapshot_hash=object_hash({}),
        audit_path=audit, originals={row["plan_id"]: capture}, published=[row],
        published_at="2026-10-15T16:30:00+08:00", publication_mode="CLOSE",
        minimum_trade_date="2026-10-16")


def test_capture_precedes_and_survives_caller_mutation(tmp_path):
    raw, _, audit, base, capture, row = source(tmp_path)
    raw["symbol"] = "000001.SZ"
    assert capture["raw_plan"]["value"]["symbol"] == "603565.SH"
    result = write(tmp_path, capture, row, audit, base)
    assert result["status"] == "CAPTURED_NOT_VALIDATED"
    path = Path(result["path"])
    assert hashlib.sha256(path.read_bytes()).hexdigest() == result["sha256"]
    data = json.loads(path.read_text(encoding="utf-8"))
    assert "data" not in data["base_snapshot"]
    assert "raw_plan" not in data["plans"][0]
    assert data["plans"][0]["raw_plan_sha256"] == capture["raw_plan"]["sha256"]
    assert data["plans"][0]["published_payload"]["value"] == row["payload"]
    facts = data["plans"][0]["entry_confirmation_daily_facts"]
    assert facts["schema_version"] == "entry-confirmation-daily-facts/1"
    assert facts["sha256"] == object_hash(facts["value"])
    assert facts["value"]["status"] == "UNPROVEN"


def test_resealed_different_audit_cannot_substitute_original(tmp_path):
    _, a3, audit, base, capture, row = source(tmp_path)
    a3["output"]["core_watch_pool"][0]["symbol"] = "000001.SZ"
    a3["output_hash"] = object_hash(a3["output"])
    audit.write_text(json.dumps({"stages": [a3]}), encoding="utf-8")
    with pytest.raises(ValueError, match="ORIGINAL_AUDIT_CONFLICT"):
        write(tmp_path, capture, row, audit, base)


def test_missing_original_is_not_rebuilt_from_final_row(tmp_path):
    _, _, audit, base, capture, row = source(tmp_path)
    capture.pop("raw_plan")
    with pytest.raises(ValueError, match="ORIGINAL_PLAN_MISSING"):
        write(tmp_path, capture, row, audit, base)


def test_no_overwrite_and_db_payload_conflict_preserved(tmp_path):
    _, _, audit, base, capture, row = source(tmp_path)
    result = write(tmp_path, capture, row, audit, base)
    original_bytes = Path(result["path"]).read_bytes()
    with pytest.raises(FileExistsError):
        write(tmp_path, capture, row, audit, base)
    assert Path(result["path"]).read_bytes() == original_bytes
    other = tmp_path / "other"; other.mkdir()
    row["payload"] = {**row["payload"], "stop_level": 999}
    result = write(other, capture, row, audit, base)
    body = json.loads(Path(result["path"]).read_text(encoding="utf-8"))
    entry = body["plans"][0]
    assert entry["published_payload"]["sha256"] != entry["submitted_payload"]["sha256"]
    assert result["status"] == "CAPTURED_NOT_VALIDATED"


def test_real_publisher_archives_current_call_not_reverse_engineered_final(tmp_path):
    from dataclasses import replace
    from types import SimpleNamespace
    from test_w2_shadow_inputs_publication import publisher_fixture, AT, TARGET
    from liangjian_funnel.pipeline.research import StageAudit
    from liangjian_funnel.workflow import WorkflowApplication, _plan_payload
    from liangjian_funnel.runtime.plan_expiry import normalize_publication_expiry
    app, result, data, raw = publisher_fixture()
    stage = StageAudit(lane="lane_1", model="fixture", stage="A3", status="VALIDATED",
        snapshot_id=result.snapshot_id, prompt_hash=None, input_hash=None,
        output_hash=object_hash(result.lanes[0].final_output), latency_ms=0, attempts=1,
        thinking_variant=None, symbols=(raw["symbol"],), reason_codes=(), output=result.lanes[0].final_output)
    audit_path = tmp_path / "lane.json"
    lane = replace(result.lanes[0], stages=(stage,), audit_path=audit_path)
    audit_path.write_text(json.dumps(lane.as_dict()), encoding="utf-8")
    result = replace(result, lanes=(lane,))
    base = tmp_path / "base.json"
    base.write_text(json.dumps({"snapshot_id": result.snapshot_id,
        "snapshot_hash": result.snapshot_hash, "as_of": AT.isoformat(), "data": data}), encoding="utf-8")
    app.settings = SimpleNamespace(workflow_output_dir=tmp_path / "outputs")
    before = deepcopy(raw)
    publication = WorkflowApplication._publish_plans(app, result, "close", AT,
        snapshot_data=data, minimum_trade_date=TARGET, publication_snapshot_path=base)
    assert len(publication["created"]) == 1 and raw == before
    proof = publication["projection_manifests"][0]
    assert proof["status"] == "CAPTURED_NOT_VALIDATED", proof
    body = json.loads(Path(proof["path"]).read_text(encoding="utf-8"))
    entry = body["plans"][0]
    assert entry["raw_plan_sha256"] == object_hash(raw)
    assert entry["normalized_payload"]["value"] == normalize_publication_expiry(
        _plan_payload(raw), app.store.batch[0]["expires_at"])
    assert "source_run_id" not in entry["normalized_payload"]["value"]
    assert entry["submitted_payload"]["value"] == app.store.batch[0]["payload"]
    assert entry["published_payload"] == entry["submitted_payload"]
    assert body["research_as_of"] == AT.isoformat()
    assert body["observed_at"] != body["research_as_of"]
