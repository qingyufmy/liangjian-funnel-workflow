"""Bounded streaming projection of one stage from a large immutable audit."""
from pathlib import Path

import ijson
from ijson.common import ObjectBuilder

A1_POOLS = ("active_research_pool", "monitor_pool", "rejected_candidates")
A1_FIELDS = ("symbol", "name", "company_name", "theme_id", "primary_theme", "theme_name",
             "primary_theme_name", "core_thesis", "bear_case")


def _retain_a1(prefix, event, value):
    path = prefix + "." + value if event == "map_key" else prefix
    roots = ("stages.item.stage", "stages.item.status", "stages.item.reason_codes")
    leaves = roots + tuple(f"stages.item.output.{pool}.item.{field}"
                           for pool in A1_POOLS for field in A1_FIELDS)
    return any(path == leaf or path.startswith(leaf + ".") or leaf.startswith(path + ".")
               for leaf in leaves)


def read_audit_stage(path: Path, stage: str, *, max_file_bytes=512 * 1024 * 1024,
                     max_stage_bytes=64 * 1024 * 1024) -> dict:
    try:
        return _read_audit_stage(path, stage, max_file_bytes, max_stage_bytes)
    except (ijson.JSONError, OverflowError) as exc:
        raise ValueError("AUDIT_JSON_INVALID") from exc


def _read_audit_stage(path, stage, max_file_bytes, max_stage_bytes):
    before = path.stat()
    if before.st_size > max_file_bytes:
        raise ValueError("AUDIT_FILE_LIMIT")
    target = None
    index = -1
    # First pass never materializes A1's large evidence graph. A sorted JSON
    # audit can put the stage name after its output, so discover its index first.
    with path.open("rb") as handle:
        for prefix, event, value in ijson.parse(handle, use_float=True):
            if prefix == "stages.item" and event == "start_map":
                index += 1
            if prefix == "stages.item.stage" and event == "string" and value == stage:
                if target is not None:
                    raise ValueError("AUDIT_STAGE_AMBIGUOUS")
                target = index
    if target is None:
        return {}
    index = -1
    builder = ObjectBuilder()
    size = 0
    with path.open("rb") as handle:
        for prefix, event, value in ijson.parse(handle, use_float=True):
            if prefix == "stages.item" and event == "start_map":
                index += 1
            if index != target:
                continue
            # A5 consumes identities/theses, not A1's duplicated evidence graph.
            # Keep every stock and every field used by _a1_market_universe.
            if stage == "A1" and not _retain_a1(prefix, event, value):
                continue
            size += len(value.encode("utf-8")) if isinstance(value, str) else 16
            if size > max_stage_bytes:
                raise ValueError("AUDIT_STAGE_LIMIT")
            builder.event(event, value)
            if prefix == "stages.item" and event == "end_map":
                break
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise ValueError("AUDIT_CHANGED_DURING_READ")
    result = builder.value
    if not isinstance(result, dict) or result.get("stage") != stage:
        raise ValueError("AUDIT_STAGE_INVALID")
    return result
