"""Read-only host resource measurements and fail-closed research gates."""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

try:  # ``resource`` is POSIX-only; the deployed VM uses this branch.
    import resource as _resource
except ImportError:  # pragma: no cover - exercised by Windows CI.
    _resource = None


@dataclass(frozen=True, slots=True)
class ResourceSnapshot:
    rss_current_mb: float
    rss_peak_mb: float
    system_mem_available_mb: float | None
    swap_used_mb: float | None
    disk_free_mb: float
    disk_free_ratio: float
    open_file_descriptors: int | None

    def as_dict(self) -> dict[str, float | int | None]:
        return {
            "rss_current_mb": round(self.rss_current_mb, 3),
            "rss_peak_mb": round(self.rss_peak_mb, 3),
            "system_mem_available_mb": _rounded(self.system_mem_available_mb),
            "swap_used_mb": _rounded(self.swap_used_mb),
            "disk_free_mb": round(self.disk_free_mb, 3),
            "disk_free_ratio": round(self.disk_free_ratio, 6),
            "open_file_descriptors": self.open_file_descriptors,
        }


@dataclass(frozen=True, slots=True)
class ResourceDecision:
    allowed: bool
    reason_codes: tuple[str, ...]
    snapshot: ResourceSnapshot

    def as_dict(self) -> dict[str, object]:
        return {
            "allowed": self.allowed,
            "reason_codes": list(self.reason_codes),
            "snapshot": self.snapshot.as_dict(),
        }


def measure_resources(path: str | Path) -> ResourceSnapshot:
    target = Path(path).resolve()
    disk = shutil.disk_usage(target)
    current_rss = _current_rss_mb()
    peak = _peak_rss_raw()
    # Linux reports KiB; macOS reports bytes. Windows does not expose this
    # module path in production, but retain a conservative conversion.
    peak_mb = peak / (1024 * 1024) if peak > 10**9 else peak / 1024
    mem_available, swap_used = _linux_memory()
    return ResourceSnapshot(
        rss_current_mb=current_rss,
        rss_peak_mb=peak_mb,
        system_mem_available_mb=mem_available,
        swap_used_mb=swap_used,
        disk_free_mb=disk.free / (1024 * 1024),
        disk_free_ratio=(disk.free / disk.total) if disk.total else 0.0,
        open_file_descriptors=_fd_count(),
    )


def evaluate_resources(
    path: str | Path,
    *,
    minimum_mem_available_mb: float = 512,
    minimum_disk_free_mb: float = 4 * 1024,
    minimum_disk_free_ratio: float = 0.10,
    maximum_process_rss_mb: float = 1024,
    probe: Callable[[str | Path], ResourceSnapshot] = measure_resources,
) -> ResourceDecision:
    snapshot = probe(path)
    reasons: list[str] = []
    if snapshot.system_mem_available_mb is not None and snapshot.system_mem_available_mb < minimum_mem_available_mb:
        reasons.append("BLOCKED_RESOURCE_MEMORY_LOW")
    if snapshot.rss_current_mb > maximum_process_rss_mb:
        reasons.append("BLOCKED_RESOURCE_PROCESS_RSS_HIGH")
    if snapshot.disk_free_mb < minimum_disk_free_mb or snapshot.disk_free_ratio < minimum_disk_free_ratio:
        reasons.append("BLOCKED_RESOURCE_DISK_LOW")
    return ResourceDecision(allowed=not reasons, reason_codes=tuple(reasons), snapshot=snapshot)


def _current_rss_mb() -> float:
    statm = Path("/proc/self/statm")
    try:
        resident_pages = int(statm.read_text(encoding="ascii").split()[1])
        return resident_pages * os.sysconf("SC_PAGE_SIZE") / (1024 * 1024)
    except (OSError, ValueError, IndexError, AttributeError):
        peak = _peak_rss_raw()
        return peak / (1024 * 1024) if peak > 10**9 else peak / 1024


def _peak_rss_raw() -> float:
    if _resource is None:
        return 0.0
    return float(_resource.getrusage(_resource.RUSAGE_SELF).ru_maxrss)


def _linux_memory() -> tuple[float | None, float | None]:
    values: dict[str, int] = {}
    try:
        for line in Path("/proc/meminfo").read_text(encoding="ascii").splitlines():
            name, raw = line.split(":", 1)
            token = raw.strip().split()[0]
            values[name] = int(token)
    except (OSError, ValueError, IndexError):
        return None, None
    available = values.get("MemAvailable")
    swap_total = values.get("SwapTotal")
    swap_free = values.get("SwapFree")
    return (
        None if available is None else available / 1024,
        None if swap_total is None or swap_free is None else max(0, swap_total - swap_free) / 1024,
    )


def _fd_count() -> int | None:
    try:
        return len(tuple(Path("/proc/self/fd").iterdir()))
    except OSError:
        return None


def _rounded(value: float | None) -> float | None:
    return None if value is None else round(value, 3)


__all__ = ["ResourceDecision", "ResourceSnapshot", "evaluate_resources", "measure_resources"]
