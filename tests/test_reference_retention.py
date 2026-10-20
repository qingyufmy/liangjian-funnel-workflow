import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from liangjian_funnel.runtime.reference_retention import plan_reference_retention, scan_reference_retention

NOW = datetime(2026, 10, 20, tzinfo=timezone.utc)


def row(path, *, date="2026-01-01T00:00:00+00:00", understood=True):
    return dict(path=path, bytes=10, mtime=date, understood=understood)


def test_permanent_closure_survives_age_cycles_and_all_plans_not_only_active():
    result = plan_reference_retention([row(x) for x in "abcd"],
        references={"old_expired_plan": ["a"], "a": ["b"], "b": ["a", "c"]},
        permanent_roots=["old_expired_plan"], now=NOW, census_complete=True)
    assert result["permanently_protected_count"] == 3
    assert [x["path"] for x in result["files"] if x["reason_code"].startswith("ELIGIBLE")] == ["d"]
    assert result["execution_available"] is False and result["deleted_files"] == 0


@pytest.mark.parametrize("complete,unknown", [(False, []), (True, ["UNKNOWN_AUDIT_FORMAT"])])
def test_incomplete_census_holds_everything(complete, unknown):
    result = plan_reference_retention([row("a")], references={}, permanent_roots=[],
        now=NOW, census_complete=complete, unknowns=unknown)
    assert result["candidate_count"] == 0


def test_recent_future_unknown_and_missing_reference():
    result = plan_reference_retention([row("a", date=NOW.isoformat()), row("b", understood=False)],
        references={}, permanent_roots=[], now=NOW, census_complete=True)
    assert result["candidate_count"] == 0
    assert result["files"][0]["reason_code"] == "KEEP_WITHIN_RETENTION"
    result = plan_reference_retention([row("a")], references={"a": ["missing"]},
        permanent_roots=[], now=NOW, census_complete=True)
    assert not result["reference_census_complete"]


def policy(**updates):
    return dict(schema="reference-retention/1", dry_run_only=True, keep_days=30,
                managed_roots=["outputs/research", "state/research_checkpoints"], **updates)


def setup_root(tmp_path):
    for p in ("outputs/research", "state/research_checkpoints"):
        (tmp_path / p).mkdir(parents=True)
    return tmp_path / "outputs/research"


def test_scanner_reads_nested_db_json_and_transitive_file_reference_without_writes(tmp_path):
    folder = setup_root(tmp_path)
    (folder / "a.json").write_text(json.dumps({"snapshot_path": "outputs/research/b.json"}))
    (folder / "b.json").write_text("{}")
    before = {str(p): p.read_bytes() for p in tmp_path.rglob("*.json")}
    result = scan_reference_retention(tmp_path, policy(),
        reference_records=[{"payload_json": json.dumps({"publication_manifest_path": "outputs/research/a.json"})}],
        reference_census_complete=True, now=NOW)
    assert result["permanently_protected_count"] == 2
    assert before == {str(p): p.read_bytes() for p in tmp_path.rglob("*.json")}


@pytest.mark.parametrize("payload", ["not-json", "x" * 100])
def test_scanner_unknown_or_large_files_disable_candidates(tmp_path, payload):
    folder = setup_root(tmp_path)
    (folder / "bad.json").write_text(payload)
    result = scan_reference_retention(tmp_path, policy(max_json_bytes=50), reference_records=[],
        reference_census_complete=True, now=NOW)
    assert not result["reference_census_complete"] and result["candidate_count"] == 0


def test_unresolved_external_path_is_not_silently_ignored(tmp_path):
    setup_root(tmp_path)
    result = scan_reference_retention(tmp_path, policy(),
        reference_records=[{"audit_path": "/old/missing.json"}], reference_census_complete=True, now=NOW)
    assert not result["reference_census_complete"]


def test_leading_whitespace_db_json_is_traversed(tmp_path):
    folder = setup_root(tmp_path)
    (folder / "a.json").write_text("{}")
    result = scan_reference_retention(tmp_path, policy(),
        reference_records=[{"payload_json": ' \n {"path":"outputs/research/a.json"}'}],
        reference_census_complete=True, now=NOW)
    assert result["permanently_protected_count"] == 1


def test_expired_plan_run_reference_protects_all_run_outputs_and_checkpoints(tmp_path):
    folder = setup_root(tmp_path)
    (folder / "run-old.json").write_text('{"run_id":"run-old"}')
    (folder / "a2.json").write_text('{"run_id":"run-old","result":{}}')
    checkpoint = tmp_path / "state/research_checkpoints/checkpoint_abc.json"
    checkpoint.write_text('{"key":{"run_id":"run-old"}}')
    result = scan_reference_retention(tmp_path, policy(),
        reference_records=[{"status":"EXPIRED", "payload_json":'{"source_run_id":"run-old"}'}],
        reference_census_complete=True, now=NOW)
    assert result["permanently_protected_count"] == 3


def test_large_and_aggregate_nested_records_hold_all(tmp_path):
    setup_root(tmp_path)
    for limits, records in [
        ({"max_json_bytes": 20}, [{"payload_json": '{"path":"outputs/research/a.json"}'}]),
        ({"max_total_read_bytes": 10}, [{"payload_json": '{}'}, {"payload_json": '[1,2,3,4,5]'}]),
    ]:
        result = scan_reference_retention(tmp_path, policy(**limits), reference_records=records,
            reference_census_complete=True, now=NOW)
        assert not result["reference_census_complete"]
        assert result["candidate_count"] == 0


def test_cyclic_python_record_cannot_recurse_forever(tmp_path):
    setup_root(tmp_path)
    record = {}; record["audit"] = record
    result = scan_reference_retention(tmp_path, policy(), reference_records=[record],
        reference_census_complete=True, now=NOW)
    assert not result["reference_census_complete"]


def test_no_archive_or_apply_configuration(tmp_path):
    setup_root(tmp_path)
    with pytest.raises(ValueError, match="DRY_RUN"):
        scan_reference_retention(tmp_path, {**policy(), "dry_run_only": False},
            reference_records=[], reference_census_complete=True, now=NOW)
    with pytest.raises(ValueError, match="ROOT_NOT_ALLOWED"):
        scan_reference_retention(tmp_path, {**policy(), "managed_roots": ["."]},
            reference_records=[], reference_census_complete=True, now=NOW)


def test_declaration_symlink_is_never_followed(tmp_path):
    setup_root(tmp_path)
    target = tmp_path / "elsewhere"
    target.mkdir()
    link = tmp_path / "outputs/research/link"
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError:
        pytest.skip("symlink privilege unavailable")
    result = scan_reference_retention(tmp_path, policy(), reference_records=[], reference_census_complete=True, now=NOW)
    assert not result["reference_census_complete"] and result["candidate_count"] == 0
