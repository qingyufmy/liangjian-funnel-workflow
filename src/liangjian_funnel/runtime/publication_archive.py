"""Small current-call publication originals, with refs to existing large facts.

Capture is not validation or acquisition authentication. In particular this
writer never invents an overlay file or repairs a previous publication.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from datetime import date, datetime
import hashlib
import json
from pathlib import Path
import re
from typing import Any

from .publication_receipt import object_hash


def _bound(value):
    value = deepcopy(value)
    return {"value": value, "sha256": object_hash(value)}


def capture_plan_originals(*, raw: Mapping, pool: str, index: int,
                           a3_audit: Mapping | None, authority: Mapping | None) -> dict:
    """Capture before transformations; no final-payload reverse projection."""
    return {"raw_plan": _bound(raw), "raw_plan_location": [pool, index],
        "a3_audit_sha256": object_hash(a3_audit) if a3_audit is not None else None,
        "authority": deepcopy(authority)}


def _file(path: Path) -> dict:
    path = Path(path).resolve(strict=True)
    before = path.stat()
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    after = path.stat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
            after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
        raise ValueError("PUBLICATION_REFERENCE_CHANGED")
    return {"path": str(path), "sha256": digest.hexdigest()}


def _iso(value):
    return value.isoformat() if isinstance(value, (date, datetime)) else value


def write_publication_manifest(*, output_dir: Path, run_id: str, lane_id: str,
        base_snapshot_path: Path, base_snapshot_id: str, base_snapshot_hash: str,
        audit_path: Path, originals: Mapping[str, Mapping], published: Sequence[Mapping],
        published_at: datetime | str, publication_mode: str,
        minimum_trade_date: date | str | None, research_as_of: datetime | str | None = None) -> dict[str, Any]:
    """Exclusively write new small metadata; errors never overwrite originals.

    Byte-pin the existing base without loading it into memory. Reader validates
    its canonical data and every original recipe against the original audit.
    """
    if not all(re.fullmatch(r"[A-Za-z0-9_.:-]+", value) for value in (run_id, lane_id)):
        raise ValueError("PUBLICATION_ID_INVALID")
    base_pin, audit_pin = _file(base_snapshot_path), _file(audit_path)
    audit = json.loads(Path(audit_path).read_text(encoding="utf-8"))
    if _file(audit_path) != audit_pin:
        raise ValueError("PUBLICATION_REFERENCE_CHANGED")
    audits = {object_hash(item): item for item in audit.get("stages", ()) if isinstance(item, Mapping)}
    entries = []
    for row in published:
        if row.get("lane_id") != lane_id:
            continue
        original = originals.get(str(row["plan_id"])) or {}
        raw = original.get("raw_plan")
        if not isinstance(raw, Mapping) or object_hash(raw.get("value")) != raw.get("sha256"):
            raise ValueError("ORIGINAL_PLAN_MISSING")
        a3 = audits.get(original.get("a3_audit_sha256"))
        if a3 is None or a3.get("stage") != "A3" or a3.get("lane") != lane_id:
            raise ValueError("ORIGINAL_AUDIT_CONFLICT")
        pool, index = original["raw_plan_location"]
        try:
            if object_hash(a3["output"][pool][index]) != raw["sha256"]:
                raise ValueError("ORIGINAL_AUDIT_CONFLICT")
        except (KeyError, IndexError, TypeError) as exc:
            raise ValueError("ORIGINAL_AUDIT_CONFLICT") from exc
        payload = row.get("payload")
        if payload is None:
            payload = json.loads(row["payload_json"])
        expiry = _iso(row["expires_at"])
        entry = {"plan_id": row["plan_id"], "raw_plan_location": [pool, index],
            "raw_plan_sha256": raw["sha256"], "a3_audit_sha256": original["a3_audit_sha256"],
            "normalized_payload": original.get("normalized_payload"),
            "submitted_payload": original.get("submitted_payload"),
            "published_payload": _bound(payload), "shadow_projection": original.get("shadow_projection"),
            "authority": original.get("authority"), "target_trade_date": str(expiry)[:10],
            "server_expires_at": expiry, "published_at": _iso(published_at),
            "publication_mode": publication_mode, "minimum_trade_date": _iso(minimum_trade_date)}
        entries.append(entry)
    body = {"schema_version": "publication-projection-manifest/1", "run_id": run_id,
        "lane_id": lane_id, "status": "CAPTURED_NOT_VALIDATED",
        "base_snapshot": {**base_pin, "snapshot_id": base_snapshot_id, "snapshot_hash": base_snapshot_hash},
        "audit": audit_pin, "plans": entries,
        "research_as_of": _iso(research_as_of), "observed_at": _iso(published_at),
        "acquisition_provenance_authenticated": False}
    body["manifest_sha256"] = object_hash(body)
    encoded = json.dumps(body, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False).encode("utf-8")
    output_dir = Path(output_dir); output_dir.mkdir(parents=True, exist_ok=True)
    # Hash-qualified name permits safe idempotent retry without replacing any
    # old proof; an identical second write is explicitly refused.
    path = output_dir / f"{run_id}-{lane_id}-{body['manifest_sha256'][:16]}.json"
    with path.open("xb") as handle:
        handle.write(encoded)
    return {"path": str(path.resolve()), "sha256": hashlib.sha256(encoded).hexdigest(),
        "status": "CAPTURED_NOT_VALIDATED", "plan_ids": [item["plan_id"] for item in entries]}
