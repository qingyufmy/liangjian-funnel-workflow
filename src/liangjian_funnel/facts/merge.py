"""Deterministic composition of independently collected fact manifests."""

from __future__ import annotations

import hashlib
from collections.abc import Sequence

from .contracts import FactSnapshotManifest, canonical_json_bytes


def merge_fact_manifests(
    manifests: Sequence[FactSnapshotManifest],
    *,
    snapshot_id: str | None = None,
) -> FactSnapshotManifest:
    if not manifests:
        raise ValueError("at least one fact manifest is required")
    facts = tuple(fact for manifest in manifests for fact in manifest.facts)
    health = tuple(item for manifest in manifests for item in manifest.source_health)
    checksums: dict[str, str] = {}
    coverage: dict[str, float] = {}
    for manifest in manifests:
        for key, value in manifest.source_checksums.items():
            existing = checksums.get(key)
            if existing is not None and existing != value:
                raise ValueError(f"conflicting source checksum: {key}")
            checksums[key] = value
        for key, value in manifest.coverage_by_fact_type.items():
            coverage[key] = min(coverage.get(key, 1.0), value)
    as_of = max(manifest.as_of for manifest in manifests)
    if snapshot_id is None:
        digest = hashlib.sha256(
            canonical_json_bytes(
                {
                    "as_of": as_of,
                    "manifests": [
                        {"snapshot_id": manifest.snapshot_id, "manifest_hash": manifest.manifest_hash}
                        for manifest in manifests
                    ],
                }
            )
        ).hexdigest()
        snapshot_id = f"merged-{digest[:24]}"
    return FactSnapshotManifest(
        snapshot_id=snapshot_id,
        as_of=as_of,
        facts=facts,
        source_health=health,
        source_checksums=checksums,
        coverage_by_fact_type=coverage,
    )


__all__ = ["merge_fact_manifests"]
