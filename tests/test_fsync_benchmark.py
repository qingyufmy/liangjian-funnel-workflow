"""Local tmp_path tiny writes and explicit fault fixtures; no SSH or VM."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "benchmark_fsync_latency.py"
spec = importlib.util.spec_from_file_location("fsync_benchmark_test_module", SCRIPT)
tool = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = tool
spec.loader.exec_module(tool)


def config(tmp_path, **kwargs):
    values = {"parent": tmp_path, "allowed_parent": tmp_path, "mount_reference": tmp_path,
              "iterations": 3, "file_size_bytes": 128}
    values.update(kwargs)
    return tool.BenchmarkConfig(**values)


def fake_fs(path, **kwargs):
    return {"status": "SUPPORTED", "mount_id": "fixture-mount", "filesystem": "fixture-fs",
            "mount_point": str(path), "device": "fixture-device", "disk_model": None,
            "disk_model_status": "UNKNOWN", "virtio_identifier": None, "source": "FIXTURE"}


def portable_complete(monkeypatch):
    monkeypatch.setattr(tool, "_filesystem_info", fake_fs)
    monkeypatch.setattr(tool, "_fsync_directory", lambda path: ("COMPLETED", None))
    monkeypatch.setattr(tool.platform, "system", lambda: "Linux")


def test_real_local_tiny_run_has_timings_evidence_and_exact_cleanup(tmp_path):
    sentinel = tmp_path / "sentinel.txt"
    sentinel.write_text("unchanged", encoding="utf-8")
    receipt = tool.run_benchmark(config(tmp_path))
    assert receipt["status"] in {"COMPLETED", "UNSUPPORTED"}
    assert receipt["exit_code"] in {0, 2}
    assert receipt["cleanup"]["status"] == "COMPLETED"
    assert sentinel.read_text(encoding="utf-8") == "unchanged"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["sentinel.txt"]
    assert receipt["completed_iterations"] == 3
    assert receipt["total_bytes_written"] == 384
    assert len(receipt["iterations"]) == 3
    assert len(receipt["script_sha256_before"]) == 64
    assert receipt["script_sha256_before"] == receipt["script_sha256_after"]
    assert receipt["environment"]["os"]["system"]
    assert receipt["preflight"]["same_mount"] is True
    assert receipt["power_loss_proven"] is False
    assert receipt["architecture_status"] == "WAITING_BUSY_EVIDENCE"
    for sample in receipt["iterations"]:
        assert sample["file_fsync_status"] == "COMPLETED"
        assert sample["total_ns"] >= sample["write_ns"] >= 0
    assert json.loads(json.dumps(receipt, allow_nan=False)) == receipt


def test_closes_and_fsyncs_every_file_then_directory(tmp_path, monkeypatch):
    portable_complete(monkeypatch)
    calls = []
    original = tool.os.fsync
    monkeypatch.setattr(tool.os, "fsync", lambda fd: (calls.append("file"), original(fd))[-1])
    monkeypatch.setattr(tool, "_fsync_directory", lambda path: (calls.append("directory"), ("COMPLETED", None))[-1])
    receipt = tool.run_benchmark(config(tmp_path, iterations=2))
    assert receipt["exit_code"] == 0
    assert calls.count("file") == 2
    assert calls.count("directory") >= 4  # create parent, each iteration, cleanup.
    assert receipt["durability_class"] == "FSYNC_FILE_AND_DIR"


@pytest.mark.parametrize("field,value", [("iterations", 0), ("iterations", 1001),
    ("iterations", True), ("file_size_bytes", 0), ("file_size_bytes", 153601),
    ("file_size_bytes", False), ("minimum_free_bytes", 1)])
def test_invalid_workload_rejected(field, value, tmp_path):
    with pytest.raises(ValueError):
        config(tmp_path, **{field: value})


def test_parent_outside_allowlist_blocked_before_write(tmp_path):
    allow = tmp_path / "allowed"
    allow.mkdir()
    receipt = tool.run_benchmark(config(tmp_path, allowed_parent=allow))
    assert receipt["exit_code"] == 3
    assert receipt["reason_code"] == "PARENT_OUTSIDE_ALLOWED_ROOT"
    assert not receipt["iterations"]
    assert list(tmp_path.iterdir()) == [allow]


def test_missing_parent_not_created(tmp_path):
    missing = tmp_path / "absent"
    receipt = tool.run_benchmark(config(tmp_path, parent=missing))
    assert receipt["reason_code"] == "PARENT_NOT_EXISTING_DIRECTORY"
    assert not missing.exists()


def test_relative_parent_rejected(tmp_path):
    with pytest.raises(ValueError):
        config(tmp_path, parent=Path("relative"))


def test_symlink_parent_rejected_without_target_mutation(tmp_path):
    destination = tmp_path / "real"
    destination.mkdir()
    alias = tmp_path / "alias"
    try:
        alias.symlink_to(destination, target_is_directory=True)
    except OSError:
        pytest.skip("local platform cannot create test symlink")
    receipt = tool.run_benchmark(config(tmp_path, parent=alias))
    assert receipt["exit_code"] == 3
    assert receipt["reason_code"] == "SYMLINK_PATH_REJECTED"
    assert not list(destination.iterdir())


def test_space_shortage_blocks_before_mkdir(tmp_path, monkeypatch):
    monkeypatch.setattr(tool.shutil, "disk_usage", lambda path: type("Usage", (), {"free": 299999999})())
    receipt = tool.run_benchmark(config(tmp_path))
    assert receipt["reason_code"] == "INSUFFICIENT_FREE_SPACE"
    assert receipt["preflight"]["required_free_bytes"] >= 300000000
    assert not list(tmp_path.iterdir())


def test_mount_mismatch_blocks_before_mkdir(tmp_path, monkeypatch):
    ref = tmp_path / "reference"
    ref.mkdir()
    monkeypatch.setattr(tool, "_filesystem_info", lambda path: {**fake_fs(path),
        "mount_id": "other" if path == ref else "fixture-mount"})
    receipt = tool.run_benchmark(config(tmp_path, mount_reference=ref))
    assert receipt["reason_code"] == "REFERENCE_MOUNT_MISMATCH"
    assert list(tmp_path.iterdir()) == [ref]


def test_mount_unknown_cannot_be_called_same_device(tmp_path, monkeypatch):
    monkeypatch.setattr(tool, "_filesystem_info", lambda path: {"status": "UNSUPPORTED", "mount_id": None})
    receipt = tool.run_benchmark(config(tmp_path))
    assert receipt["reason_code"] == "FILESYSTEM_IDENTITY_UNAVAILABLE"
    assert not list(tmp_path.iterdir())


def test_unique_directory_collision_is_never_reused_or_cleaned(tmp_path, monkeypatch):
    portable_complete(monkeypatch)
    name = "liangjian-fsync-benchmark-" + "a"*32
    existing = tmp_path / name
    existing.mkdir()
    sentinel = existing / "foreign.txt"
    sentinel.write_bytes(b"do-not-delete")
    monkeypatch.setattr(tool, "_new_directory_name", lambda: name)
    receipt = tool.run_benchmark(config(tmp_path))
    assert receipt["reason_code"] == "UNIQUE_DIRECTORY_ALREADY_EXISTS"
    assert sentinel.read_bytes() == b"do-not-delete"


def test_unsafe_generated_name_rejected(tmp_path, monkeypatch):
    monkeypatch.setattr(tool, "_new_directory_name", lambda: "../foreign")
    receipt = tool.run_benchmark(config(tmp_path))
    assert receipt["exit_code"] == 3
    assert not list(tmp_path.iterdir())


def test_file_fsync_failure_cleanup_retains_error_evidence(tmp_path, monkeypatch):
    portable_complete(monkeypatch)
    def fail(fd):
        raise OSError("secret message must not be printed")
    monkeypatch.setattr(tool.os, "fsync", fail)
    receipt = tool.run_benchmark(config(tmp_path))
    assert receipt["reason_code"] == "FILE_FSYNC_FAILED"
    assert receipt["exit_code"] == 3
    assert receipt["iterations"][0]["file_fsync_status"] == "FAILED"
    assert receipt["cleanup"]["status"] == "COMPLETED"
    assert not list(tmp_path.iterdir())
    assert "secret" not in json.dumps(receipt)


def test_unsupported_directory_sync_never_claims_file_and_dir_durable(tmp_path, monkeypatch):
    monkeypatch.setattr(tool, "_fsync_directory", lambda path: ("UNSUPPORTED", "DIR_FSYNC_UNSUPPORTED"))
    receipt = tool.run_benchmark(config(tmp_path))
    assert receipt["exit_code"] == 2
    assert receipt["status"] == "UNSUPPORTED"
    assert receipt["durability_class"] != "FSYNC_FILE_AND_DIR"
    assert receipt["file_and_dir_fsync_completed_count"] == 0
    assert receipt["percentiles"]["directory_fsync_ms"]["sample_count"] == 0
    assert receipt["cleanup"]["status"] == "COMPLETED"


def test_foreign_entry_never_deleted_and_cleanup_error_propagates(tmp_path, monkeypatch):
    portable_complete(monkeypatch)
    original = tool._cleanup_owned
    marker = None
    def inject(directory, identity, owned):
        nonlocal marker
        marker = directory / "foreign.txt"
        marker.write_bytes(b"foreign")
        return original(directory, identity, owned)
    monkeypatch.setattr(tool, "_cleanup_owned", inject)
    receipt = tool.run_benchmark(config(tmp_path))
    assert receipt["exit_code"] == 3
    assert receipt["cleanup"]["status"] == "FAILED"
    assert marker.read_bytes() == b"foreign"
    assert list(marker.parent.iterdir()) == [marker]
    marker.unlink()  # test owns its fixture, not benchmark cleanup.
    marker.parent.rmdir()


def test_file_replacement_not_deleted_by_cleanup(tmp_path, monkeypatch):
    portable_complete(monkeypatch)
    original = tool._cleanup_iteration
    replacement = None
    def inject(directory, identity, owned, target, sample):
        nonlocal replacement
        target.unlink()
        replacement = target
        replacement.write_bytes(b"foreign replacement")
        return original(directory, identity, owned, target, sample)
    monkeypatch.setattr(tool, "_cleanup_iteration", inject)
    receipt = tool.run_benchmark(config(tmp_path, iterations=1))
    assert receipt["cleanup"]["status"] == "FAILED"
    assert replacement.read_bytes() == b"foreign replacement"
    replacement.unlink()
    replacement.parent.rmdir()


def test_exact_percentile_nearest_rank_and_no_empty_zero():
    p = tool.percentiles_ms([1_000_000, 2_000_000, 3_000_000, 4_000_000])
    assert p == {"sample_count": 4, "method": "NEAREST_RANK", "p50": 2.0, "p95": 4.0,
                 "p99": 4.0, "max": 4.0}
    assert tool.percentiles_ms([])["p99"] is None


def test_cli_requires_explicit_paths_and_workload():
    result = subprocess.run([sys.executable, str(SCRIPT)], capture_output=True, text=True)
    assert result.returncode != 0


def test_real_cli_local_small_data_outputs_machine_readable_result(tmp_path):
    result = subprocess.run([sys.executable, str(SCRIPT), "--parent", str(tmp_path),
        "--allowed-parent", str(tmp_path), "--mount-reference", str(tmp_path),
        "--iterations", "2", "--file-size-bytes", "64"], capture_output=True, text=True)
    assert result.returncode in {0, 2}
    receipt = json.loads(result.stdout)
    assert receipt["exit_code"] == result.returncode
    assert receipt["completed_iterations"] == 2
    assert not list(tmp_path.iterdir())


def test_cli_precheck_error_returns_json_and_nonzero(tmp_path):
    result = subprocess.run([sys.executable, str(SCRIPT), "--parent", str(tmp_path / "absent"),
        "--allowed-parent", str(tmp_path), "--mount-reference", str(tmp_path),
        "--iterations", "1", "--file-size-bytes", "64"], capture_output=True, text=True)
    assert result.returncode == 3
    receipt = json.loads(result.stdout)
    assert receipt["reason_code"] == "PARENT_NOT_EXISTING_DIRECTORY"
    assert not list(tmp_path.iterdir())


def test_replaced_file_after_flush_error_is_not_adopted_as_owned(tmp_path, monkeypatch):
    portable_complete(monkeypatch)
    real_open, real_fstat = tool.Path.open, tool.os.fstat
    replacement = None
    original_info = None
    class FailedStream:
        def __init__(self, path):
            nonlocal original_info
            self.path, self.calls, self.closed = path, 0, False
            self.path.write_bytes(b"")
            original_info = self.path.lstat()
        def fileno(self):
            return 987654321
        def write(self, payload):
            nonlocal original_info
            self.path.write_bytes(payload)
            original_info = self.path.lstat()
            return len(payload)
        def flush(self):
            nonlocal replacement
            self.calls += 1
            if self.calls == 2:
                self.path.unlink()
                self.path.write_bytes(b"foreign replacement")
                replacement = self.path
            raise OSError("flush failed")
        def close(self):
            self.closed = True
    monkeypatch.setattr(tool.Path, "open", lambda path, mode="r", *a, **kw:
        FailedStream(path) if mode == "xb" and path.name.startswith("payload-")
        else real_open(path, mode, *a, **kw))
    monkeypatch.setattr(tool.os, "fstat", lambda fd: original_info if fd == 987654321 else real_fstat(fd))
    receipt = tool.run_benchmark(config(tmp_path, iterations=1))
    assert receipt["exit_code"] == 3
    assert receipt["reason_code"] == "FILE_FLUSH_FAILED"
    assert replacement.exists()
    assert replacement.read_bytes() == b"foreign replacement"
    replacement.unlink()
    replacement.parent.rmdir()


def test_script_hash_failure_keeps_completed_measurements_and_cleanup(tmp_path, monkeypatch):
    portable_complete(monkeypatch)
    original = tool._sha256
    calls = 0
    def fail_after(path):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("script unavailable")
        return original(path)
    monkeypatch.setattr(tool, "_sha256", fail_after)
    receipt = tool.run_benchmark(config(tmp_path, iterations=1))
    assert receipt["exit_code"] == 3
    assert receipt["reason_code"] == "SCRIPT_HASH_AFTER_FAILED"
    assert len(receipt["iterations"]) == 1
    assert receipt["cleanup"]["status"] == "COMPLETED"
    assert not list(tmp_path.iterdir())


def test_created_directory_on_different_mount_is_rejected_before_file_writes(tmp_path, monkeypatch):
    portable_complete(monkeypatch)
    monkeypatch.setattr(tool, "_filesystem_info", lambda path: {**fake_fs(path),
        "mount_id": "replacement-mount" if path.name.startswith("liangjian-fsync-benchmark-") else "fixture-mount"})
    receipt = tool.run_benchmark(config(tmp_path))
    assert receipt["exit_code"] == 3
    assert receipt["reason_code"] == "CREATED_DIRECTORY_MOUNT_MISMATCH"
    assert not receipt["iterations"]
    assert receipt["cleanup"]["status"] == "FAILED"  # Do not delete an unknown mounted directory.
    Path(receipt["scratch_directory"]).rmdir()  # This is only our simulated mount fixture.


def test_parent_not_writable_never_creates_directory(tmp_path, monkeypatch):
    monkeypatch.setattr(tool.os, "access", lambda *args: False)
    receipt = tool.run_benchmark(config(tmp_path))
    assert receipt["reason_code"] == "PARENT_NOT_WRITABLE"
    assert not list(tmp_path.iterdir())


def test_script_changes_mark_failed_without_losing_iteration_evidence(tmp_path, monkeypatch):
    portable_complete(monkeypatch)
    values = iter(["a"*64, "b"*64])
    monkeypatch.setattr(tool, "_sha256", lambda path: next(values))
    receipt = tool.run_benchmark(config(tmp_path, iterations=1))
    assert receipt["reason_code"] == "SCRIPT_CHANGED_DURING_BENCHMARK"
    assert receipt["exit_code"] == 3
    assert len(receipt["iterations"]) == 1
    assert not list(tmp_path.iterdir())


def test_keyboard_interrupt_keeps_partial_evidence_and_cleans(tmp_path, monkeypatch):
    portable_complete(monkeypatch)
    def interrupted(fd):
        raise KeyboardInterrupt()
    monkeypatch.setattr(tool.os, "fsync", interrupted)
    try:
        receipt = tool.run_benchmark(config(tmp_path, iterations=1))
    except KeyboardInterrupt:
        pytest.fail("interrupt discarded JSON evidence")
    assert receipt["status"] == "INTERRUPTED"
    assert receipt["exit_code"] == 3
    assert receipt["reason_code"] == "BENCHMARK_INTERRUPTED"
    assert len(receipt["iterations"]) == 1
    assert not list(tmp_path.iterdir())


def test_linux_mountinfo_parser_picks_longest_mount_and_unescapes(tmp_path, monkeypatch):
    monkeypatch.setattr(tool.platform, "system", lambda: "Linux")
    real_read = tool.Path.read_text
    nested = tmp_path / "with space"
    parent_mount = str(tmp_path).replace(" ", "\\040")
    child_mount = str(nested).replace(" ", "\\040")
    data = f"1 0 8:0 / {parent_mount} rw - ext4 /dev/vda rw\n2 1 8:0 / {child_mount} rw - xfs /dev/vdb rw\n"
    monkeypatch.setattr(tool.Path, "read_text", lambda path, *a, **kw:
        data if path.as_posix() == "/proc/self/mountinfo" else real_read(path, *a, **kw))
    info = tool._filesystem_info(nested)
    assert info["mount_id"] == "linux-mount:2"
    assert info["filesystem"] == "xfs"
    assert info["mount_point"] == str(nested)


def test_config_error_cli_json_still_binds_script(tmp_path):
    result = subprocess.run([sys.executable, str(SCRIPT), "--parent", str(tmp_path),
        "--allowed-parent", str(tmp_path), "--mount-reference", str(tmp_path),
        "--iterations", "0", "--file-size-bytes", "64"], capture_output=True, text=True)
    receipt = json.loads(result.stdout)
    assert result.returncode == 3
    assert receipt["reason_code"] == "WORKLOAD_OR_SPACE_LIMIT_INVALID"
    assert len(receipt["script_sha256_before"]) == 64
    assert not list(tmp_path.iterdir())


def test_iteration_deletes_before_next_write_and_reports_separate_latency(tmp_path, monkeypatch):
    original = tool._write_iteration
    file_counts = []
    def inspect(directory, index, payload, owned):
        file_counts.append(len(list(directory.glob("payload-*.bin"))))
        return original(directory, index, payload, owned)
    monkeypatch.setattr(tool, "_write_iteration", inspect)
    receipt = tool.run_benchmark(config(tmp_path))
    assert file_counts == [0, 0, 0]
    assert receipt["cleanup_policy"] == "PER_ITERATION_EXACT_UNLINK_AND_DIR_FSYNC"
    assert receipt["total_removed_files"] == 3
    for row in receipt["iterations"]:
        assert row["file_removed"] is True
        assert row["unlink_ns"] >= 0
        assert row["iteration_total_ns"] >= row["total_ns"]
    assert not list(tmp_path.iterdir())


def test_iteration_cleanup_dir_fsync_failure_records_error_but_no_foreign_delete(tmp_path, monkeypatch):
    portable_complete(monkeypatch)
    calls = 0
    def sync(path):
        nonlocal calls
        calls += 1
        if calls == 3:  # parent create, payload dir, per-iteration unlink dir.
            raise tool.BenchmarkFailure("DIRECTORY_FSYNC_FAILED")
        return "COMPLETED", None
    monkeypatch.setattr(tool, "_fsync_directory", sync)
    receipt = tool.run_benchmark(config(tmp_path))
    assert receipt["exit_code"] == 3
    assert receipt["iterations"][0]["file_removed"] is True
    assert receipt["iterations"][0]["cleanup_status"] == "FAILED"
    assert receipt["total_removed_files"] == 1
    assert receipt["cleanup"]["status"] == "COMPLETED"
    assert not list(tmp_path.iterdir())


def test_unknown_directory_entry_is_not_deleted_during_next_iteration(tmp_path, monkeypatch):
    original = tool._cleanup_iteration
    marker = None
    def add_marker(directory, identity, owned, path, sample):
        nonlocal marker
        marker = directory / "not-owned.txt"
        marker.write_bytes(b"foreign")
        return original(directory, identity, owned, path, sample)
    monkeypatch.setattr(tool, "_cleanup_iteration", add_marker)
    receipt = tool.run_benchmark(config(tmp_path, iterations=2))
    assert receipt["exit_code"] == 3  # Final rmdir must fail, not recursive-delete it.
    assert receipt["total_removed_files"] == 2
    assert marker.read_bytes() == b"foreign"
    marker.unlink()
    marker.parent.rmdir()
