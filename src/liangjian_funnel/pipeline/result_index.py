"""Bounded NDJSON stage-decision index for control-plane pagination."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any


POOL_KEYS = {
    "A1": {
        "approved": "active_research_pool",
        "watch": "monitor_pool",
        "rejected": "rejected_candidates",
    },
    "A2": {
        "approved": "focus_pool",
        "watch": "watch_only_pool",
        "rejected": "rejected_candidates",
    },
    "A3": {
        "approved": "core_watch_pool",
        "watch": "secondary_watch_pool",
        "rejected": "rejected_candidates",
    },
}
SAFE = re.compile(r"[^A-Za-z0-9._-]+")


def write_lane_result_index(
    directory: str | Path,
    *,
    run_id: str,
    lane_id: str,
    stages: Sequence[Any],
    model: str | None = None,
    a1_input_count: int | None = None,
    name_catalog: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Write one record per stage/pool/stock without retaining a second copy.

    The lane JSON remains the immutable audit artifact.  This index is a
    derived projection for bounded server-side scans and may be rebuilt at any
    time from that audit artifact.
    """

    root = Path(directory).resolve()
    root.mkdir(parents=True, exist_ok=True)
    stem = f"research_{_safe(run_id)}_{_safe(lane_id)}"
    ndjson_path = root / f"{stem}.decisions.ndjson"
    manifest_path = root / f"{stem}.decisions.json"
    names = {str(key).upper(): str(value) for key, value in (name_catalog or {}).items() if value}
    counts = {stage: {pool: 0 for pool in pools} for stage, pools in POOL_KEYS.items()}
    reason_options = {stage: {pool: set() for pool in pools} for stage, pools in POOL_KEYS.items()}
    stage_meta: dict[str, dict[str, Any]] = {}
    previous_output_count = max(0, int(a1_input_count or 0))
    digest = hashlib.sha256()
    descriptor, temporary = tempfile.mkstemp(prefix=f".{ndjson_path.name}.", dir=root)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            for raw_stage in stages:
                stage = _stage_name(raw_stage)
                if stage not in POOL_KEYS:
                    continue
                output = _stage_output(raw_stage)
                stage_output_count = len(_stage_symbols(raw_stage))
                stage_meta[stage] = {
                    "status": _stage_value(raw_stage, "status"),
                    "latency_ms": _integer(_stage_value(raw_stage, "latency_ms")),
                    "input_count": previous_output_count,
                    "output_count": stage_output_count,
                }
                previous_output_count = stage_output_count
                for pool, key in POOL_KEYS[stage].items():
                    values = output.get(key)
                    if not isinstance(values, Sequence) or isinstance(values, (str, bytes, bytearray)):
                        continue
                    for raw_item in values:
                        if not isinstance(raw_item, Mapping):
                            continue
                        item = dict(raw_item)
                        symbol = _symbol(item)
                        if not symbol:
                            continue
                        if not _name(item) and names.get(symbol):
                            item["name"] = names[symbol]
                            item["name_source"] = "snapshot_index"
                        reasons = _reason_codes(item)
                        record = {"stage": stage, "pool": pool, "symbol": symbol, "item": item}
                        encoded = json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                        handle.write(encoded + "\n")
                        digest.update(encoded.encode("utf-8"))
                        digest.update(b"\n")
                        counts[stage][pool] += 1
                        reason_options[stage][pool].update(reasons)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, ndjson_path)
    except BaseException:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise
    manifest = {
        "schema_version": "research-stage-decision-index/1.0.0",
        "run_id": run_id,
        "lane_id": lane_id,
        "model": model,
        "data_file": ndjson_path.name,
        "sha256": digest.hexdigest(),
        "counts": counts,
        "stages": stage_meta,
        "reason_options": {
            stage: {pool: sorted(values) for pool, values in pools.items()}
            for stage, pools in reason_options.items()
        },
    }
    _atomic_json(manifest_path, manifest)
    return {**manifest, "manifest_path": str(manifest_path), "data_path": str(ndjson_path)}


def snapshot_name_catalog(snapshot_data: Mapping[str, Any]) -> dict[str, str]:
    names: dict[str, str] = {}
    for key in ("universe_candidates", "trade_candidates", "g0_candidates"):
        values = snapshot_data.get(key)
        if not isinstance(values, Sequence) or isinstance(values, (str, bytes, bytearray)):
            continue
        for value in values:
            if not isinstance(value, Mapping):
                continue
            symbol = _symbol(value)
            name = _name(value)
            if symbol and name:
                names.setdefault(symbol, name)
    return names


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(value, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def _stage_name(value: Any) -> str:
    raw = value.get("stage") if isinstance(value, Mapping) else getattr(value, "stage", "")
    return str(raw).upper()


def _stage_output(value: Any) -> Mapping[str, Any]:
    raw = value.get("output") if isinstance(value, Mapping) else getattr(value, "output", None)
    return raw if isinstance(raw, Mapping) else {}


def _stage_symbols(value: Any) -> Sequence[Any]:
    raw = value.get("symbols") if isinstance(value, Mapping) else getattr(value, "symbols", ())
    return raw if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)) else ()


def _stage_value(value: Any, key: str) -> Any:
    return value.get(key) if isinstance(value, Mapping) else getattr(value, key, None)


def _integer(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _symbol(value: Mapping[str, Any]) -> str:
    for key in ("symbol", "stock_code", "code", "thscode"):
        raw = value.get(key)
        if isinstance(raw, str) and raw.strip():
            return raw.strip().upper()
    return ""


def _name(value: Mapping[str, Any]) -> str:
    for key in ("name", "company_name", "stock_name", "security_name"):
        raw = value.get(key)
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
    return ""


def _reason_codes(value: Mapping[str, Any]) -> tuple[str, ...]:
    result: list[str] = []
    for key in ("reason_codes", "reason_code", "risk_flags"):
        raw = value.get(key)
        values = raw if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)) else (raw,)
        for item in values:
            if isinstance(item, str) and item.strip() and item.strip() not in result:
                result.append(item.strip()[:120])
    return tuple(result)


def _safe(value: str) -> str:
    return SAFE.sub("_", str(value)).strip("._-")[:200] or "unknown"


__all__ = ["POOL_KEYS", "snapshot_name_catalog", "write_lane_result_index"]
