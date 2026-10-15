"""Explicit original-object adapter; never reconstruct proofs from final DB rows.

This module has no filesystem, database, provider, model or notification I/O.
An envelope is a local declaration and cannot authenticate acquisition provenance.
"""
from __future__ import annotations

import base64
from collections.abc import Mapping, Sequence
from dataclasses import asdict, replace
from datetime import date, datetime
import json
import re
from typing import Any
from zoneinfo import ZoneInfo

from .calendar import ExchangeTradingCalendar
from .publication_receipt import (
    AuthorityProof, BoundObject, FieldChange, PublicationInputs,
    PublicationReceiptBuilder, SnapshotProof, object_hash,
)

SCHEMA = "publication-projection-inputs/1"


def _changes(before, after, stage, *, authority=None, projection=None):
    result = []
    for field in sorted(set(before) | set(after)):
        if field in before and field in after and object_hash(before[field]) == object_hash(after[field]):
            continue
        basis = "SERVER_NORMALIZATION" if stage == "NORMALIZE" else (
            "SOURCE_RUN" if field == "source_run_id" else
            "TREND_RULE" if field == "trend_entry_rule_version" else
            "SHADOW_PROJECTION" if field == "strategy_facts" and projection else "A2_AUTHORITY")
        result.append(FieldChange(stage, field, field in before, field in after,
            before.get(field), after.get(field), basis,
            authority.value.sha256 if authority and basis == "A2_AUTHORITY" else None,
            projection.sha256 if projection and basis == "SHADOW_PROJECTION" else None))
    return tuple(result)


def capture_publication_projection(
    originals: PublicationInputs, *, shadow_projection: BoundObject | None,
    submitted_payload: BoundObject, published_payload: BoundObject,
) -> PublicationInputs:
    """Add a captured independent shadow result to current-call original proofs.

    `originals.raw_plan`, `.normalized_payload`, `.a3_audit`, `.authority` and
    `.snapshot_chain` MUST be captured before publication. The final object only
    supplies observed differences; the builder separately derives the expected
    whitelist transformation from the originals and rejects arbitrary changes.
    """
    return replace(originals, submitted_payload=submitted_payload,
        published_payload=published_payload, shadow_projection=shadow_projection,
        changes=_changes(originals.raw_plan.value, originals.normalized_payload.value, "NORMALIZE")
        + _changes(originals.normalized_payload.value, submitted_payload.value, "PUBLISH",
                   authority=originals.authority, projection=shadow_projection))


def validate_projected_payload(
    inputs: PublicationInputs, *, payload: Mapping[str, Any], raw_plan: Mapping[str, Any],
    run_id: str, lane_id: str, plan_id: str,
    calendar: ExchangeTradingCalendar | None = None,
    builder: PublicationReceiptBuilder | None = None,
) -> dict[str, Any]:
    """Reader hook: exact final/raw/identity binding plus the WHOLE receipt.

    A reader must additionally bind the original audit/snapshots to its pinned
    publication references. No historical/final-row-only fallback is provided.
    """
    receipt = (builder or PublicationReceiptBuilder()).build(inputs, calendar=calendar or ExchangeTradingCalendar())
    reasons = set(receipt["reason_codes"])
    try:
        matches = ((inputs.run_id, inputs.lane_id, inputs.plan_id) == (run_id, lane_id, plan_id)
            and object_hash(raw_plan) == inputs.raw_plan.sha256
            and object_hash(payload) == inputs.published_payload.sha256)
    except (AttributeError, TypeError, ValueError, OverflowError):
        matches = False
    if not matches:
        reasons.add("PROJECTED_READER_BINDING_CONFLICT")
    return {"evidence_complete": not reasons, "reason_codes": sorted(reasons),
            "receipt": receipt, "eligibility_released": False,
            "acquisition_provenance_authenticated": False}


def encode_projection_inputs(inputs: PublicationInputs) -> dict[str, Any]:
    """Portable test/review envelope; do not duplicate large snapshots in summary.

    Production archives should retain the already frozen files and resolve their
    pinned refs into PublicationInputs, rather than store a second 140MB copy.
    """
    def encode(value):
        if isinstance(value, bytes): return {"_base64_bytes": base64.b64encode(value).decode("ascii")}
        if isinstance(value, (date, datetime)): return value.isoformat()
        if isinstance(value, Mapping): return {key: encode(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)): return [encode(item) for item in value]
        return value
    body = {"schema_version": SCHEMA, "inputs": encode(asdict(inputs))}
    return {**body, "envelope_sha256": object_hash(body)}


def decode_projection_inputs(envelope: Mapping[str, Any]) -> PublicationInputs:
    """Decode original pins without resealing or replacing any object hash."""
    body = {key: value for key, value in envelope.items() if key != "envelope_sha256"}
    if envelope.get("schema_version") != SCHEMA or object_hash(body) != envelope.get("envelope_sha256"):
        raise ValueError("PROJECTION_ENVELOPE_HASH_MISMATCH")
    def bound(value): return BoundObject(value["value"], value["sha256"])
    def optional(value): return bound(value) if value is not None else None
    value = dict(envelope["inputs"])
    for key in ("a3_audit", "raw_plan", "normalized_payload", "published_payload"):
        value[key] = bound(value[key])
    for key in ("submitted_payload", "shadow_projection"): value[key] = optional(value[key])
    if value["authority"] is not None:
        entry = value["authority"]
        value["authority"] = AuthorityProof(bound(entry["value"]), bound(entry["source"]),
            tuple(entry["selector"]), entry["origin"])
    for name in ("snapshot_chain", "authority_snapshot_chain"):
        snapshots = []
        for entry in value.get(name, ()):
            raw = entry["file_bytes"]
            snapshots.append(SnapshotProof(bound(entry["snapshot"]),
                base64.b64decode(raw["_base64_bytes"], validate=True) if raw is not None else None,
                entry["file_sha256"], entry["parent_snapshot_hash"],
                optional(entry["hash_recipe"]), entry["recipe_kind"], entry.get("representation", "ORIGINAL_FILE")))
        value[name] = tuple(snapshots)
    value["changes"] = tuple(FieldChange(**entry) for entry in value["changes"])
    value["raw_plan_location"] = tuple(value["raw_plan_location"])
    value["target_trade_date"] = date.fromisoformat(value["target_trade_date"])
    if value["minimum_trade_date"] is not None:
        value["minimum_trade_date"] = date.fromisoformat(value["minimum_trade_date"])
    for key in ("server_expires_at", "published_at"): value[key] = datetime.fromisoformat(value[key])
    return PublicationInputs(**value)


def card_plan_binding(
    all_plans: Sequence[Mapping[str, Any]], selected: Sequence[Mapping[str, Any]], *,
    source_run_id: str | None, trade_date: str, displayed_symbols: Sequence[str],
) -> dict[str, Any]:
    """Bind actual DB row IDs to exact rendered page symbols, not theme labels.

    Metadata failure must not interrupt the existing customer delivery path.
    Keep a DATA_LIMITED declaration and never fabricate substitute IDs.
    """
    reasons = set()
    try:
        target_date = date.fromisoformat(trade_date)
    except (TypeError, ValueError):
        target_date = None
        reasons.add("CARD_TRADE_DATE_INVALID")
    catalog = {}
    for row in all_plans:
        plan_id, lane, symbol = row.get("plan_id"), row.get("lane_id"), row.get("symbol")
        if (not isinstance(plan_id, str) or not isinstance(lane, str)
                or not re.fullmatch(r"[A-Za-z0-9_.:-]+", plan_id)
                or not isinstance(source_run_id, str)
                or not plan_id.startswith(source_run_id + ":" + lane + ":")
                or not plan_id[len(source_run_id + ":" + lane + ":"):]
                or not isinstance(symbol, str) or not re.fullmatch(r"[0-9]{6}\.(SH|SZ|BJ)", symbol)):
            reasons.add("CARD_PLAN_IDENTITY_INVALID"); continue
        try:
            payload = row.get("payload") or row.get("payload_json") or {}
            payload = json.loads(payload) if isinstance(payload, str) else payload
            if not source_run_id or payload.get("source_run_id") != source_run_id:
                reasons.add("CARD_SOURCE_RUN_CONFLICT")
        except (TypeError, ValueError, AttributeError):
            reasons.add("CARD_SOURCE_RUN_CONFLICT")
        # Server columns own the current publication/session contract. An old
        # payload.plan_expiry must neither validate nor invalidate this pin.
        try:
            expiry = row.get("expires_at")
            expiry = expiry if isinstance(expiry, datetime) else datetime.fromisoformat(expiry)
            if expiry.utcoffset() is None or expiry.astimezone(ZoneInfo("Asia/Shanghai")).date() != target_date:
                reasons.add("CARD_PLAN_TRADE_DATE_CONFLICT")
        except (TypeError, ValueError, OverflowError):
            reasons.add("CARD_PLAN_TRADE_DATE_CONFLICT")
        if plan_id in catalog: reasons.add("CARD_PLAN_DUPLICATE")
        catalog[plan_id] = row
    plan_ids, symbols = [], []
    for row in selected:
        plan_id = row.get("plan_id")
        if plan_id not in catalog or object_hash(catalog[plan_id]) != object_hash(row):
            reasons.add("CARD_PAGE_OUTSIDE_PLAN_SET"); continue
        plan_ids.append(plan_id); symbols.append(str(row["symbol"]).split(".")[0])
    if len(set(plan_ids)) != len(plan_ids): reasons.add("CARD_PAGE_DUPLICATE")
    rendered = [str(symbol).split(".")[0] for symbol in displayed_symbols]
    if symbols != rendered: reasons.add("CARD_CONTENT_PLAN_SET_CONFLICT")
    # Internal card evidence only: customer title/body/colour are untouched.
    # Use the same frozen-label interpretation as A5; UNKNOWN has no shared
    # exposure count and is not manufactured from a security-code prefix.
    from ..review.observability import _cluster
    from collections import Counter
    def cluster(row):
        return _cluster({"payload_json":row.get("payload") or row.get("payload_json")})
    known_counts = Counter(label for label, _ in map(cluster, all_plans) if label != "UNKNOWN")
    clusters = []
    for row in selected:
        label, basis = cluster(row)
        clusters.append({"plan_id":row.get("plan_id"), "cluster_label":label,
            "cluster_basis":basis, "cluster_count":known_counts.get(label) if label != "UNKNOWN" else None})
    body = {"schema_version": "premarket-card-plan-binding/1", "status": "DATA_LIMITED" if reasons else "BOUND",
        "source_run_id": source_run_id, "trade_date": trade_date,
        "plan_ids": plan_ids if not reasons else [], "symbols": symbols,
        "plan_rowset_sha256": object_hash(list(selected)), "reason_codes": sorted(reasons),
        "clusters":clusters, "distinct_known_clusters":len(known_counts), "cluster_action":"LABEL_ONLY_NO_PLAN_OR_SIGNAL_CAP"}
    return {**body, "binding_sha256": object_hash(body)}
