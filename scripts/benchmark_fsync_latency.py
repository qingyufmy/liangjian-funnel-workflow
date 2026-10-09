"""Explicit-path isolated fsync benchmark. VM use still requires authorization.

No production path, SSH, configuration/permission changes, or load generator.
Stdout is the JSON evidence; exclusive scratch files are cleaned by identity.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import errno
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import shutil
import stat
import sys
import time
import uuid


_DIRECTORY_NAME = re.compile(r"^liangjian-fsync-benchmark-[0-9a-f]{32}$")
_PHASES = ("open", "write", "flush", "file_fsync", "close", "directory_fsync", "total",
           "unlink", "cleanup_directory_fsync", "iteration_total")


class BenchmarkFailure(Exception):
    """Fixed, non-secret reason codes only."""


@dataclass(frozen=True)
class BenchmarkConfig:
    parent: Path
    allowed_parent: Path
    mount_reference: Path
    iterations: int
    file_size_bytes: int
    minimum_free_bytes: int = 300_000_000

    def __post_init__(self):
        for field in ("parent", "allowed_parent", "mount_reference"):
            path = Path(getattr(self, field))
            if not path.is_absolute() or ".." in path.parts:
                raise ValueError("ABSOLUTE_EXPLICIT_PATH_REQUIRED")
            object.__setattr__(self, field, path)
        for value, low, high in ((self.iterations, 1, 1000),
                                 (self.file_size_bytes, 1, 153600),
                                 (self.minimum_free_bytes, 300_000_000, 2**53-1)):
            if type(value) is not int or not low <= value <= high:
                raise ValueError("WORKLOAD_OR_SPACE_LIMIT_INVALID")


def _sha256(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _new_directory_name():
    return "liangjian-fsync-benchmark-" + uuid.uuid4().hex


def _checked_path(path, *, directory=False):
    for part in (path, *path.parents):
        if part.is_symlink():
            raise BenchmarkFailure("SYMLINK_PATH_REJECTED")
    if not path.exists() or (directory and not path.is_dir()):
        raise BenchmarkFailure("PARENT_NOT_EXISTING_DIRECTORY" if directory else "REFERENCE_NOT_FOUND")
    return path.resolve(strict=True)


def _unescape_mount(text):
    return re.sub(r"\\([0-7]{3})", lambda m: chr(int(m.group(1), 8)), text)


def _filesystem_info(path):
    """Read metadata only. Missing source never becomes a guessed identity."""
    system = platform.system()
    if system == "Linux":
        selected = None
        try:
            for line in Path("/proc/self/mountinfo").read_text(encoding="utf-8").splitlines():
                left, right = line.split(" - ", 1)
                fields, extra = left.split(), right.split()
                mount_point = Path(_unescape_mount(fields[4]))
                if path == mount_point or path.is_relative_to(mount_point):
                    candidate = (len(mount_point.parts), fields, extra, mount_point)
                    if selected is None or candidate[0] > selected[0]:
                        selected = candidate
            if selected is None:
                raise ValueError()
            _, fields, extra, mount_point = selected
            block = Path("/sys/dev/block") / fields[2]
            model = None
            virtio = None
            if block.exists():
                resolved = block.resolve()
                for candidate in (resolved, resolved.parent):
                    model_path = candidate / "device" / "model"
                    if model_path.is_file():
                        model = model_path.read_text(encoding="utf-8").strip()[:160] or None
                        break
                virtio = next((part for part in resolved.parts if re.fullmatch(r"virtio[0-9]+", part)), None)
            return {"status": "SUPPORTED", "mount_id": "linux-mount:"+fields[0],
                    "mount_point": str(mount_point), "filesystem": extra[0],
                    "device": _unescape_mount(extra[1]), "device_major_minor": fields[2],
                    "disk_model": model, "disk_model_status": "OBSERVED" if model else "UNKNOWN",
                    "virtio_identifier": virtio, "source": "PROC_SELF_MOUNTINFO_SYSFS"}
        except (OSError, ValueError, IndexError):
            return {"status": "UNSUPPORTED", "mount_id": None, "reason_code": "LINUX_MOUNT_METADATA_UNAVAILABLE"}
    if system == "Windows":
        try:
            import ctypes
            from ctypes import wintypes
            kernel = ctypes.WinDLL("kernel32", use_last_error=True)
            root = ctypes.create_unicode_buffer(32768)
            get_root = kernel.GetVolumePathNameW
            get_root.argtypes = [wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD]
            get_root.restype = wintypes.BOOL
            if not get_root(str(path), root, len(root)):
                raise OSError()
            fs = ctypes.create_unicode_buffer(256)
            serial, maximum, flags = wintypes.DWORD(), wintypes.DWORD(), wintypes.DWORD()
            info = kernel.GetVolumeInformationW
            info.argtypes = [wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD,
                ctypes.POINTER(wintypes.DWORD), ctypes.POINTER(wintypes.DWORD),
                ctypes.POINTER(wintypes.DWORD), wintypes.LPWSTR, wintypes.DWORD]
            info.restype = wintypes.BOOL
            if not info(root.value, None, 0, ctypes.byref(serial), ctypes.byref(maximum), ctypes.byref(flags), fs, len(fs)):
                raise OSError()
            return {"status": "SUPPORTED", "mount_id": f"windows-volume:{root.value.casefold()}:{serial.value:08x}",
                    "mount_point": root.value, "filesystem": fs.value,
                    "device": f"volume-serial:{serial.value:08x}", "disk_model": None,
                    "disk_model_status": "UNKNOWN", "virtio_identifier": None,
                    "source": "GET_VOLUME_PATH_AND_INFORMATION_W"}
        except (OSError, AttributeError):
            return {"status": "UNSUPPORTED", "mount_id": None, "reason_code": "WINDOWS_VOLUME_METADATA_UNAVAILABLE"}
    return {"status": "UNSUPPORTED", "mount_id": None, "reason_code": "PLATFORM_MOUNT_METADATA_UNSUPPORTED"}


def _fsync_directory(path):
    if platform.system() == "Windows":
        return "UNSUPPORTED", "WINDOWS_DIRECTORY_FSYNC_UNAVAILABLE"
    descriptor = None
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0))
        os.fsync(descriptor)
        return "COMPLETED", None
    except OSError as exc:
        if exc.errno in {errno.EINVAL, errno.ENOSYS, getattr(errno, "ENOTSUP", errno.EINVAL)}:
            return "UNSUPPORTED", "DIRECTORY_FSYNC_UNSUPPORTED"
        raise BenchmarkFailure("DIRECTORY_FSYNC_FAILED") from None
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _file_identity(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _directory_identity(path):
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_ino == 0:
        raise BenchmarkFailure("DIRECTORY_IDENTITY_UNVERIFIED")
    return info.st_dev, info.st_ino


def _check_owned_directory(directory, identity):
    _checked_path(directory, directory=True)
    if _directory_identity(directory) != identity["directory"] or _directory_identity(directory.parent) != identity["parent"]:
        raise BenchmarkFailure("CLEANUP_DIRECTORY_IDENTITY_CHANGED")
    filesystem = _filesystem_info(directory)
    if filesystem.get("status") != "SUPPORTED" or filesystem.get("mount_id") != identity["mount_id"]:
        raise BenchmarkFailure("CLEANUP_DIRECTORY_MOUNT_CHANGED")


def _unlink_owned_file(directory, identity, path, expected):
    _check_owned_directory(directory, identity)
    if path.parent != directory:
        raise BenchmarkFailure("CLEANUP_FILE_OUTSIDE_DIRECTORY")
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or _file_identity(info) != expected:
        raise BenchmarkFailure("CLEANUP_FILE_IDENTITY_CHANGED")
    path.unlink()


def _cleanup_iteration(directory, identity, owned, path, sample):
    sample["cleanup_status"] = "FAILED"
    _phase(sample, "unlink", lambda: _unlink_owned_file(directory, identity, path, owned[path]))
    del owned[path]  # Once unlinked, never sweep a replacement later.
    sample["file_removed"] = True
    sync_status, reason = _phase(sample, "cleanup_directory_fsync", lambda: _fsync_directory(directory))
    sample["cleanup_directory_fsync_status"] = sync_status
    if sync_status == "UNSUPPORTED":
        sample["cleanup_directory_fsync_attempt_ns"] = sample["cleanup_directory_fsync_ns"]
        sample["cleanup_directory_fsync_ns"] = None
        sample["cleanup_unsupported_reason"] = reason
    sample["cleanup_status"] = sync_status


def _cleanup_owned(directory, identity, owned):
    """Never recurse, glob-delete, or delete an unrecognized replacement."""
    result = {"status": "COMPLETED", "removed_files": 0, "directory_removed": False,
              "directory_fsync_status": "NOT_ATTEMPTED", "reason_codes": []}
    try:
        _check_owned_directory(directory, identity)
    except (OSError, BenchmarkFailure):
        result.update(status="FAILED", reason_codes=["CLEANUP_DIRECTORY_IDENTITY_CHANGED"])
        return result
    for path, expected in owned.items():
        try:
            _unlink_owned_file(directory, identity, path, expected)
            result["removed_files"] += 1
        except (OSError, BenchmarkFailure):
            result["reason_codes"].append("CLEANUP_FILE_IDENTITY_CHANGED_OR_UNLINK_FAILED")
    try:
        sync_status, reason = _fsync_directory(directory)
        result["directory_fsync_status"] = sync_status
        if reason:
            result["unsupported_reason"] = reason
    except (OSError, BenchmarkFailure):
        result["reason_codes"].append("CLEANUP_DIRECTORY_FSYNC_FAILED")
    try:
        directory.rmdir()
        result["directory_removed"] = True
    except OSError:
        result["reason_codes"].append("CLEANUP_RMDIR_FAILED_OR_FOREIGN_ENTRIES")
    if result["reason_codes"]:
        result["status"] = "FAILED"
    return result


def percentiles_ms(durations_ns):
    if any(type(n) is not int or n < 0 for n in durations_ns):
        raise ValueError("FINITE_NONNEGATIVE_INTEGER_TIMING_REQUIRED")
    values = sorted(durations_ns)
    result = {"sample_count": len(values), "method": "NEAREST_RANK"}
    for name, fraction in (("p50", .50), ("p95", .95), ("p99", .99)):
        result[name] = values[max(0, math.ceil(fraction*len(values))-1)] / 1_000_000 if values else None
    result["max"] = values[-1] / 1_000_000 if values else None
    return result


def _phase(sample, phase, function):
    start = time.perf_counter_ns()
    try:
        return function()
    finally:
        elapsed = time.perf_counter_ns() - start
        if elapsed < 0:
            raise BenchmarkFailure("MONOTONIC_CLOCK_REGRESSED")
        sample[phase+"_ns"] = elapsed


def _write_iteration(directory, index, payload, owned):
    sample = {"index": index, "status": "FAILED", "bytes_written": 0,
              "file_fsync_status": "NOT_ATTEMPTED", "directory_fsync_status": "NOT_ATTEMPTED",
              **{phase+"_ns": None for phase in _PHASES}}
    path = directory / f"payload-{index:06d}.bin"
    stream = None
    phase = "FILE_OPEN"
    started = time.perf_counter_ns()
    reason = None
    try:
        stream = _phase(sample, "open", lambda: path.open("xb"))
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_ino == 0:
            raise BenchmarkFailure("FILE_IDENTITY_UNVERIFIED")
        owned[path] = _file_identity(info)
        phase = "FILE_WRITE"
        count = _phase(sample, "write", lambda: stream.write(payload))
        if count != len(payload):
            raise BenchmarkFailure("SHORT_WRITE")
        sample["bytes_written"] = count
        phase = "FILE_FLUSH"
        _phase(sample, "flush", stream.flush)
        phase = "FILE_FSYNC"
        _phase(sample, "file_fsync", lambda: os.fsync(stream.fileno()))
        sample["file_fsync_status"] = "COMPLETED"
        owned[path] = _file_identity(os.fstat(stream.fileno()))
        phase = "FILE_CLOSE"
        _phase(sample, "close", stream.close)
        stream = None
        phase = "DIRECTORY_FSYNC"
        sync_status, sync_reason = _phase(sample, "directory_fsync", lambda: _fsync_directory(directory))
        sample["directory_fsync_status"] = sync_status
        if sync_status == "UNSUPPORTED":
            sample["directory_fsync_attempt_ns"] = sample["directory_fsync_ns"]
            sample["directory_fsync_ns"] = None
            sample["unsupported_reason"] = sync_reason
        sample["status"] = "COMPLETED" if sync_status == "COMPLETED" else "UNSUPPORTED"
    except (OSError, BenchmarkFailure, KeyboardInterrupt) as exc:
        reason = ("BENCHMARK_INTERRUPTED" if isinstance(exc, KeyboardInterrupt) else
                  str(exc) if isinstance(exc, BenchmarkFailure) else phase+"_FAILED")
        sample["reason_code"] = reason
        if phase == "FILE_FSYNC":
            sample["file_fsync_status"] = "FAILED"
        if phase == "DIRECTORY_FSYNC":
            sample["directory_fsync_status"] = "FAILED"
    finally:
        if stream is not None and not stream.closed:
            try:
                # Bind the partial file too, so cleanup can remove our own failed write.
                stream.flush()
            except OSError:
                pass
            try:
                info = os.fstat(stream.fileno())
                if stat.S_ISREG(info.st_mode) and info.st_ino != 0:
                    owned[path] = _file_identity(info)
            except OSError:
                pass  # Never adopt a path replacement as the original opened file.
            try:
                _phase(sample, "close", stream.close)
            except OSError:
                reason = reason or "FILE_CLOSE_FAILED"
        sample["total_ns"] = time.perf_counter_ns() - started
    return sample, reason


def run_benchmark(config):
    if not isinstance(config, BenchmarkConfig):
        raise ValueError("NORMAL_CONFIG_REQUIRED")
    report = {"schema_version": "isolated-fsync-benchmark/1", "status": "BLOCKED", "exit_code": 3,
        "reason_code": None, "started_at": datetime.now(timezone.utc).isoformat(),
        "authorization_authenticated": False, "measurement_context": "CALLER_SCOPED_NOT_PRODUCTION_CERTIFIED",
        "architecture_status": "WAITING_BUSY_EVIDENCE", "power_loss_proven": False,
        "durability_class": "UNSUPPORTED_DURABILITY", "iterations": [],
        "requested_iterations": config.iterations, "file_size_bytes": config.file_size_bytes,
        "cleanup_policy": "PER_ITERATION_EXACT_UNLINK_AND_DIR_FSYNC",
        "total_requested_bytes": config.iterations*config.file_size_bytes,
        "completed_iterations": 0, "file_and_dir_fsync_completed_count": 0, "total_bytes_written": 0,
        "preflight": {}, "environment": {"os": {"system": platform.system(), "release": platform.release(),
            "version": platform.version(), "machine": platform.machine()}, "python": sys.version.split()[0]},
        "script_sha256_before": _sha256(__file__), "script_sha256_after": None,
        "cleanup": {"status": "NOT_REQUIRED", "removed_files": 0, "directory_removed": False},
        "parent_directory_creation_fsync_status": "NOT_ATTEMPTED",
        "parent_directory_cleanup_fsync_status": "NOT_ATTEMPTED"}
    directory = None
    identity = None
    owned = {}
    try:
        parent = _checked_path(config.parent, directory=True)
        allowed = _checked_path(config.allowed_parent, directory=True)
        reference = _checked_path(config.mount_reference)
        if parent != allowed and not parent.is_relative_to(allowed):
            raise BenchmarkFailure("PARENT_OUTSIDE_ALLOWED_ROOT")
        if not os.access(parent, os.W_OK | os.X_OK):
            raise BenchmarkFailure("PARENT_NOT_WRITABLE")
        parent_identity = _directory_identity(parent)
        free = shutil.disk_usage(parent).free
        required = max(config.minimum_free_bytes, 2*report["total_requested_bytes"])
        report["preflight"].update(parent=str(parent), allowed_parent=str(allowed), mount_reference=str(reference),
            parent_writable=True, free_bytes=free, required_free_bytes=required)
        if free < required:
            raise BenchmarkFailure("INSUFFICIENT_FREE_SPACE")
        filesystem, ref_filesystem = _filesystem_info(parent), _filesystem_info(reference)
        report["environment"].update(filesystem=filesystem, reference_filesystem=ref_filesystem)
        if any(item.get("status") != "SUPPORTED" or not item.get("mount_id") for item in (filesystem, ref_filesystem)):
            raise BenchmarkFailure("FILESYSTEM_IDENTITY_UNAVAILABLE")
        same_mount = filesystem["mount_id"] == ref_filesystem["mount_id"] and parent.stat().st_dev == reference.stat().st_dev
        report["preflight"]["same_mount"] = same_mount
        if not same_mount:
            raise BenchmarkFailure("REFERENCE_MOUNT_MISMATCH")
        name = _new_directory_name()
        if not isinstance(name, str) or _DIRECTORY_NAME.fullmatch(name) is None:
            raise BenchmarkFailure("UNSAFE_DIRECTORY_NAME")
        candidate = parent / name
        try:
            candidate.mkdir(exist_ok=False)
        except FileExistsError:
            raise BenchmarkFailure("UNIQUE_DIRECTORY_ALREADY_EXISTS") from None
        directory = candidate
        identity = {"directory": _directory_identity(directory), "parent": parent_identity,
                    "mount_id": filesystem["mount_id"]}
        if identity["directory"][0] != parent_identity[0]:
            raise BenchmarkFailure("CREATED_DIRECTORY_DEVICE_MISMATCH")
        report["scratch_directory"] = str(directory)
        created_filesystem = _filesystem_info(directory)
        if created_filesystem.get("status") != "SUPPORTED" or created_filesystem.get("mount_id") != filesystem["mount_id"]:
            raise BenchmarkFailure("CREATED_DIRECTORY_MOUNT_MISMATCH")
        sync_status, sync_reason = _fsync_directory(parent)
        report["parent_directory_creation_fsync_status"] = sync_status
        if sync_reason:
            report["parent_creation_unsupported_reason"] = sync_reason
        payload = b"x"*config.file_size_bytes
        for index in range(config.iterations):
            iteration_started = time.perf_counter_ns()
            sample, failure = _write_iteration(directory, index, payload, owned)
            report["iterations"].append(sample)
            report["total_bytes_written"] += sample["bytes_written"]
            if failure:
                raise BenchmarkFailure(failure)
            path = directory / f"payload-{index:06d}.bin"
            try:
                _cleanup_iteration(directory, identity, owned, path, sample)
            except (OSError, BenchmarkFailure, KeyboardInterrupt) as exc:
                sample.update(status="FAILED", cleanup_status="FAILED")
                reason = ("BENCHMARK_INTERRUPTED" if isinstance(exc, KeyboardInterrupt) else
                          str(exc) if isinstance(exc, BenchmarkFailure) else "ITERATION_CLEANUP_FAILED")
                sample["reason_code"] = reason
                raise BenchmarkFailure(reason) from None
            finally:
                sample["iteration_total_ns"] = time.perf_counter_ns() - iteration_started
        report["status"] = "COMPLETED"
        report["exit_code"] = 0
    except (OSError, BenchmarkFailure, KeyboardInterrupt) as exc:
        reason = ("BENCHMARK_INTERRUPTED" if isinstance(exc, KeyboardInterrupt) else
                  str(exc) if isinstance(exc, BenchmarkFailure) else "FILESYSTEM_OPERATION_FAILED")
        report["status"] = "INTERRUPTED" if reason == "BENCHMARK_INTERRUPTED" else "FAILED" if directory is not None else "BLOCKED"
        report["reason_code"] = reason
        report["exit_code"] = 3
    finally:
        if directory is not None:
            if identity is None:
                report["cleanup"] = {"status": "FAILED", "removed_files": 0, "directory_removed": False,
                    "reason_codes": ["CREATED_DIRECTORY_IDENTITY_UNVERIFIED_NO_DELETE"]}
            else:
                report["cleanup"] = _cleanup_owned(directory, identity, owned)
                if report["cleanup"]["directory_removed"]:
                    try:
                        sync_status, sync_reason = _fsync_directory(directory.parent)
                        report["parent_directory_cleanup_fsync_status"] = sync_status
                        if sync_reason:
                            report["parent_cleanup_unsupported_reason"] = sync_reason
                    except (OSError, BenchmarkFailure):
                        report["cleanup"]["status"] = "FAILED"
                        report["cleanup"].setdefault("reason_codes", []).append("PARENT_CLEANUP_FSYNC_FAILED")
                if report["cleanup"]["status"] != "COMPLETED":
                    report.update(status="FAILED", exit_code=3, reason_code=report["reason_code"] or "CLEANUP_FAILED")
        try:
            report["script_sha256_after"] = _sha256(__file__)
            if report["script_sha256_before"] != report["script_sha256_after"]:
                report.update(status="FAILED", exit_code=3, reason_code="SCRIPT_CHANGED_DURING_BENCHMARK")
        except OSError:
            report.update(status="FAILED", exit_code=3, reason_code=report["reason_code"] or "SCRIPT_HASH_AFTER_FAILED")
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
    eligible_samples = [sample for sample in report["iterations"] if sample["status"] in {"COMPLETED", "UNSUPPORTED"}]
    report["completed_iterations"] = len(eligible_samples)
    report["total_removed_files"] = sum(sample.get("file_removed") is True for sample in report["iterations"]) + report["cleanup"]["removed_files"]
    report["file_and_dir_fsync_completed_count"] = sum(sample["status"] == "COMPLETED" for sample in eligible_samples)
    report["percentiles"] = {phase+"_ms": percentiles_ms([sample[phase+"_ns"] for sample in eligible_samples if sample[phase+"_ns"] is not None]) for phase in _PHASES}
    if report["status"] == "COMPLETED":
        all_dir = (report["file_and_dir_fsync_completed_count"] == config.iterations and
            all(sample.get("cleanup_status") == "COMPLETED" for sample in report["iterations"]) and
            report["parent_directory_creation_fsync_status"] == "COMPLETED" and
            report["parent_directory_cleanup_fsync_status"] == "COMPLETED" and
            report["cleanup"].get("directory_fsync_status") == "COMPLETED")
        if all_dir and platform.system() == "Linux":
            report["durability_class"] = "FSYNC_FILE_AND_DIR"
        else:
            report.update(status="UNSUPPORTED", exit_code=2, reason_code="FILE_AND_DIRECTORY_DURABILITY_UNSUPPORTED")
            if platform.system() == "Windows":
                report["durability_class"] = "PROCESS_CRASH_ONLY"
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ("parent", "allowed-parent", "mount-reference"):
        parser.add_argument("--"+flag, type=Path, required=True)
    parser.add_argument("--iterations", type=int, required=True)
    parser.add_argument("--file-size-bytes", type=int, required=True)
    parser.add_argument("--minimum-free-bytes", type=int, default=300_000_000)
    args = parser.parse_args(argv)
    try:
        receipt = run_benchmark(BenchmarkConfig(args.parent, args.allowed_parent, args.mount_reference,
            args.iterations, args.file_size_bytes, args.minimum_free_bytes))
    except (ValueError, OSError) as exc:
        reason = str(exc) if isinstance(exc, ValueError) and str(exc) in {
            "ABSOLUTE_EXPLICIT_PATH_REQUIRED", "WORKLOAD_OR_SPACE_LIMIT_INVALID", "NORMAL_CONFIG_REQUIRED"
        } else "CONFIG_OR_EVIDENCE_FILE_INVALID"
        receipt = {"schema_version": "isolated-fsync-benchmark/1", "status": "BLOCKED", "exit_code": 3,
                   "reason_code": reason, "durability_class": "UNSUPPORTED_DURABILITY",
                   "architecture_status": "WAITING_BUSY_EVIDENCE", "script_sha256_before": None}
        try:
            receipt["script_sha256_before"] = _sha256(__file__)
        except OSError:
            pass
    print(json.dumps(receipt, ensure_ascii=False, sort_keys=True, allow_nan=False))
    return receipt["exit_code"]


if __name__ == "__main__":
    raise SystemExit(main())
