"""Pure publication-object evidence. No persistence, publisher or eligibility.

Callers must supply original objects/actual serialized snapshot bytes from the
current invocation. Nothing here obtains or reconstructs historical evidence.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, time
import hashlib
import json
import math
import re
from typing import Any
from zoneinfo import ZoneInfo

from ..pipeline.a2_role_logic import route_execution_permission
from .calendar import ExchangeTradingCalendar

SCHEMA = "wp5-normalized-publication-receipt/1"
NORMALIZATION_VERSION = "workflow-plan-payload/1"
AUTHORITY_VERSION = "route-execution-permission/1"
SHANGHAI = ZoneInfo("Asia/Shanghai")
NORMAL_FIELDS = frozenset({"symbol", "trigger_low", "trigger_high", "stop_level", "no_chase",
                          "confirmation_bars", "action"})
PERMISSION_FIELDS = frozenset({"execution_permission", "research_only_reason", "a2_execution_permission",
                              "a2_research_only_reason"})
PUBLISH_FIELDS = PERMISSION_FIELDS | {"source_run_id", "trend_entry_rule_version"}
SAFE_PERMISSION_VALUES = frozenset({"BLOCKED", "ALLOW", "ALLOW_A4", "REQUIRES_A3_A4_CONFIRMATION"})


def _json_default(value: Any) -> Any:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError("non-JSON original evidence")


def object_hash(value: Any) -> str:
    """Canonical JSON object hash, separate from original file-byte SHA."""
    digest = hashlib.sha256()
    encoder = json.JSONEncoder(ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                              allow_nan=False, default=_json_default)
    for chunk in encoder.iterencode(value):
        digest.update(chunk.encode("utf-8"))
    return digest.hexdigest()


def _sha(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _id(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_.:-]{1,1024}", value) is not None


def _aware(value: Any) -> datetime:
    result = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("aware timestamp required")
    return result.astimezone(SHANGHAI)


@dataclass(frozen=True)
class BoundObject:
    value: Mapping[str, Any]
    sha256: str


@dataclass(frozen=True)
class AuthorityProof:
    value: BoundObject
    source: BoundObject
    selector: tuple[str | int, ...]
    origin: str


@dataclass(frozen=True)
class SnapshotProof:
    snapshot: BoundObject
    file_bytes: bytes | None
    file_sha256: str | None
    parent_snapshot_hash: str | None
    hash_recipe: BoundObject | None
    recipe_kind: str


@dataclass(frozen=True)
class FieldChange:
    stage: str
    field: str
    old_present: bool
    new_present: bool
    old: Any
    new: Any
    basis: str
    authority_sha256: str | None = None


@dataclass(frozen=True)
class PublicationInputs:
    run_id: str
    lane_id: str
    plan_id: str
    a3_audit: BoundObject
    raw_plan: BoundObject
    raw_plan_location: tuple[str, int]
    normalized_payload: BoundObject
    submitted_payload: BoundObject | None
    published_payload: BoundObject
    changes: Sequence[FieldChange]
    authority: AuthorityProof | None
    snapshot_chain: Sequence[SnapshotProof]
    target_trade_date: date
    server_expires_at: datetime
    published_at: datetime
    publication_mode: str
    minimum_trade_date: date | None = None


class PublicationReceiptBuilder:
    """Normally constructed, stateless pure builder; no caller objects mutated."""

    @staticmethod
    def _bound(value: BoundObject | None, reasons: set[str]) -> bool:
        if value is None or not isinstance(value.value, Mapping):
            reasons.add("ORIGINAL_OBJECT_PROOF_MISSING")
            return False
        try:
            matches = _sha(value.sha256) and object_hash(value.value) == value.sha256
        except (TypeError, ValueError, OverflowError):
            reasons.add("HASH_INPUT_INVALID")
            return False
        if not matches:
            reasons.add("OBJECT_HASH_MISMATCH")
        return matches

    def build(self, inputs: PublicationInputs, *, calendar: ExchangeTradingCalendar) -> dict[str, Any]:
        try:
            inputs = replace(inputs, changes=tuple(inputs.changes), snapshot_chain=tuple(inputs.snapshot_chain))
            return self._build(inputs, calendar=calendar)
        except (AttributeError, KeyError, IndexError, TypeError, ValueError, OverflowError):
            body = {"schema_version": SCHEMA, "implementation_status": "IMPLEMENTATION_PARTIAL",
                    "evidence_status": "DATA_LIMITED", "evidence_complete": False,
                    "eligibility_released": False, "historical_reconstructed": False,
                    "publication_execution_authenticated": False, "reason_codes": ["INPUT_STRUCTURE_INVALID"]}
            body["receipt_sha256"] = object_hash(body)
            return body

    def _build(self, inputs: PublicationInputs, *, calendar: ExchangeTradingCalendar) -> dict[str, Any]:
        reasons: set[str] = set()
        for value in (inputs.a3_audit, inputs.raw_plan, inputs.normalized_payload, inputs.published_payload):
            self._bound(value, reasons)
        if inputs.submitted_payload is None:
            reasons.add("SUBMITTED_PAYLOAD_PROOF_MISSING")
        else:
            self._bound(inputs.submitted_payload, reasons)
            if object_hash(inputs.submitted_payload.value) != object_hash(inputs.published_payload.value):
                reasons.add("POST_PUBLICATION_PAYLOAD_CONFLICT")
        if not all(_id(value) for value in (inputs.run_id, inputs.lane_id, inputs.plan_id)):
            reasons.add("PUBLICATION_ID_INVALID")
        raw, normalized, published = inputs.raw_plan.value, inputs.normalized_payload.value, inputs.published_payload.value
        submitted = inputs.submitted_payload.value if inputs.submitted_payload else normalized
        self._audit(inputs, reasons)
        authority = self._authority(inputs, reasons)
        self._payloads(inputs, authority, reasons)
        modifications = self._changes(inputs, raw, normalized, submitted, reasons)
        chain = self._snapshots(tuple(inputs.snapshot_chain), reasons)
        try:
            if any(_aware(proof.snapshot.value.get("as_of")) > _aware(inputs.published_at) for proof in inputs.snapshot_chain):
                reasons.add("SNAPSHOT_AFTER_PUBLICATION")
        except (AttributeError, TypeError, ValueError):
            reasons.add("SNAPSHOT_CLOCK_UNPROVEN")
        final_id = chain[-1]["snapshot_id"] if chain else None
        if inputs.a3_audit.value.get("snapshot_id") != final_id:
            reasons.add("A3_SNAPSHOT_CHAIN_MISMATCH")
        target, expiry, published_at = self._dates(inputs, calendar, reasons)
        # Frozen dataclasses do not freeze nested caller mappings. Re-pin every
        # original object at seal time; never certify a concurrent late mutation.
        originals = [inputs.a3_audit, inputs.raw_plan, inputs.normalized_payload, inputs.published_payload]
        if inputs.submitted_payload is not None:
            originals.append(inputs.submitted_payload)
        if inputs.authority is not None:
            originals.extend((inputs.authority.value, inputs.authority.source))
        for proof in inputs.snapshot_chain:
            originals.append(proof.snapshot)
            if proof.hash_recipe is not None:
                originals.append(proof.hash_recipe)
        for original in originals:
            self._bound(original, reasons)
        body = {"schema_version": SCHEMA, "implementation_status": "IMPLEMENTATION_PARTIAL",
            "evidence_status": "DATA_LIMITED" if reasons else "LOCAL_RECEIPT_BOUND",
            "evidence_complete": not reasons, "eligibility_released": False,
            "historical_reconstructed": False, "publication_execution_authenticated": False,
            "acquisition_provenance_authenticated": False,
            "snapshot_chain_scope": "RESEARCH_BASE_TO_A3_STAGE",
            "run_id": inputs.run_id if _id(inputs.run_id) else None,
            "lane_id": inputs.lane_id if _id(inputs.lane_id) else None,
            "plan_id": inputs.plan_id if _id(inputs.plan_id) else None,
            "symbol": normalized.get("symbol") if isinstance(normalized.get("symbol"), str) and re.fullmatch(r"[0-9]{6}\.(SH|SZ|BJ)", normalized["symbol"]) else None,
            "a3_audit_sha256": inputs.a3_audit.sha256 if _sha(inputs.a3_audit.sha256) else None,
            "raw_plan_sha256": inputs.raw_plan.sha256 if _sha(inputs.raw_plan.sha256) else None,
            "normalized_payload_sha256": inputs.normalized_payload.sha256 if _sha(inputs.normalized_payload.sha256) else None,
            "submitted_payload_sha256": inputs.submitted_payload.sha256 if inputs.submitted_payload and _sha(inputs.submitted_payload.sha256) else None,
            "published_payload_sha256": inputs.published_payload.sha256 if _sha(inputs.published_payload.sha256) else None,
            "raw_plan_location": [inputs.raw_plan_location[0] if inputs.raw_plan_location[0] in {"core_watch_pool", "secondary_watch_pool"} else "UNSUPPORTED",
                                  inputs.raw_plan_location[1] if type(inputs.raw_plan_location[1]) is int else None],
            "normalization_version": NORMALIZATION_VERSION, "authority_rule_version": AUTHORITY_VERSION,
            "authority": self._authority_reference(inputs.authority), "modifications": modifications,
            "snapshot_chain": chain, "target_trade_date": target, "server_expires_at": expiry,
            "minimum_trade_date": inputs.minimum_trade_date.isoformat() if isinstance(inputs.minimum_trade_date, date) and not isinstance(inputs.minimum_trade_date, datetime) else None,
            "minimum_trade_date_input_sha256": object_hash(inputs.minimum_trade_date),
            "original_inputs_unchanged": "OBJECT_HASH_MISMATCH" not in reasons and "HASH_INPUT_INVALID" not in reasons,
            "published_at": published_at, "publication_mode": inputs.publication_mode if inputs.publication_mode == "CLOSE" else "UNSUPPORTED",
            "reason_codes": sorted(reasons),
            "limitations": ["Local explicit-object declaration, not publisher/DB execution proof.",
                "Original files and objects must be captured at the actual call; no historical repair.",
                "No existing reader gap or qualification is changed by this isolated module."]}
        body["receipt_sha256"] = object_hash(body)
        return body

    def _audit(self, inputs: PublicationInputs, reasons: set[str]) -> None:
        audit = inputs.a3_audit.value
        if audit.get("stage") != "A3" or audit.get("lane") != inputs.lane_id or audit.get("status") not in {"VALIDATED", "VALIDATED_NO_SETUP"}:
            reasons.add("A3_AUDIT_INVALID")
        output = audit.get("output")
        if not isinstance(output, Mapping) or object_hash(output) != audit.get("output_hash"):
            reasons.add("A3_OUTPUT_HASH_MISMATCH")
            return
        try:
            pool, index = inputs.raw_plan_location
            if pool not in {"core_watch_pool", "secondary_watch_pool"} or isinstance(index, bool) or not isinstance(index, int) or index < 0:
                raise ValueError()
            if object_hash(output[pool][index]) != inputs.raw_plan.sha256:
                raise ValueError()
        except (KeyError, IndexError, TypeError, ValueError):
            reasons.add("RAW_PLAN_AUDIT_MEMBERSHIP_MISMATCH")
        logical = str(inputs.raw_plan.value.get("plan_id") or object_hash(inputs.raw_plan.value)[:16])
        if inputs.plan_id != f"{inputs.run_id}:{inputs.lane_id}:{logical}":
            reasons.add("PLAN_ID_ORIGINAL_OBJECT_CONFLICT")

    def _authority(self, inputs: PublicationInputs, reasons: set[str]) -> Mapping[str, Any] | None:
        proof = inputs.authority
        if proof is None:
            return None
        if not self._bound(proof.value, reasons) or not self._bound(proof.source, reasons):
            return None
        try:
            selected: Any = proof.source.value
            for part in proof.selector:
                if isinstance(part, bool):
                    raise ValueError()
                selected = selected[part]
            if object_hash(selected) != proof.value.sha256:
                raise ValueError()
            if proof.origin == "A2_AUDIT_ROW":
                source = proof.source.value
                if (source.get("stage") != "A2" or source.get("lane") != inputs.lane_id
                        or proof.value.value.get("symbol") != inputs.normalized_payload.value.get("symbol")
                        or not isinstance(source.get("output"), Mapping)
                        or object_hash(source["output"]) != source.get("output_hash")
                        or len(proof.selector) != 3 or proof.selector[0] != "output"
                        or proof.selector[1] not in {"focus_pool", "watch_only_pool", "outside_rotation_pool", "rejected_candidates"}
                        or source.get("snapshot_id") not in {item.snapshot.value.get("snapshot_id") for item in inputs.snapshot_chain}):
                    raise ValueError()
            elif proof.origin == "SNAPSHOT_A2_CONTEXT":
                if (proof.selector != ("data", "A2_BOTTLENECK_CONTEXT", inputs.normalized_payload.value.get("symbol"))
                        or proof.source.sha256 not in {item.snapshot.sha256 for item in inputs.snapshot_chain}):
                    raise ValueError()
            else:
                raise ValueError()
        except (TypeError, ValueError, KeyError, IndexError):
            reasons.add("A2_AUTHORITY_SOURCE_MISMATCH")
            return None
        return proof.value.value

    @staticmethod
    def _authority_reference(proof: AuthorityProof | None) -> dict[str, Any] | None:
        if proof is None:
            return None
        return {"origin": proof.origin if proof.origin in {"A2_AUDIT_ROW", "SNAPSHOT_A2_CONTEXT"} else "UNSUPPORTED",
            "authority_sha256": proof.value.sha256 if _sha(proof.value.sha256) else None,
            "source_object_sha256": proof.source.sha256 if _sha(proof.source.sha256) else None,
            "selector_sha256": object_hash(list(proof.selector)), "rule_version": AUTHORITY_VERSION}

    @staticmethod
    def _payloads(inputs: PublicationInputs, authority: Mapping[str, Any] | None, reasons: set[str]) -> None:
        raw, normalized = inputs.raw_plan.value, inputs.normalized_payload.value
        submitted = inputs.submitted_payload.value if inputs.submitted_payload else normalized
        # Lazy import avoids a module-level workflow/runtime cycle. This is
        # the existing pure production function, not WorkflowApplication
        # construction or a duplicate normalization implementation.
        from ..workflow import _plan_payload
        expected = _plan_payload(raw)
        if expected["symbol"] is None:
            reasons.add("NORMALIZED_SYMBOL_UNPROVEN")
        if object_hash(expected) != object_hash(normalized):
            reasons.add("NORMALIZED_PAYLOAD_CONFLICT")
        expected = {**normalized, "source_run_id": inputs.run_id}
        strategy = str(raw.get("strategy_profile") or "").upper()
        if strategy == "TREND_MA5":
            expected["trend_entry_rule_version"] = "trend-ma5/2"
        rewritten = any(key in submitted and (key not in normalized or object_hash(submitted[key]) != object_hash(normalized[key]))
                        for key in PERMISSION_FIELDS)
        if rewritten and authority is None:
            reasons.add("A2_AUTHORITY_PROOF_MISSING")
        if authority is not None:
            scoped = route_execution_permission(authority, strategy)
            if scoped is not None:
                expected.update(a2_execution_permission=authority.get("execution_permission"),
                    a2_research_only_reason=authority.get("research_only_reason"), execution_permission=scoped,
                    research_only_reason=None if scoped != "BLOCKED" else authority.get("research_only_reason"))
        if object_hash(expected) != object_hash(submitted):
            reasons.add("PUBLISH_PAYLOAD_CONFLICT")

    def _changes(self, inputs, raw, normalized, submitted, reasons):
        diffs = {}
        for stage, before, after in (("NORMALIZE", raw, normalized), ("PUBLISH", normalized, submitted)):
            for field in set(before) | set(after):
                if field in before and field in after and object_hash(before[field]) == object_hash(after[field]):
                    continue
                diffs[(stage, field)] = (field in before, field in after, before.get(field), after.get(field))
        changes = tuple(inputs.changes)
        keys = [(change.stage, change.field) for change in changes]
        if len(set(keys)) != len(keys) or set(keys) != set(diffs):
            reasons.add("MODIFICATION_SET_MISMATCH")
        output = []
        for change in changes:
            allowed = change.field in (NORMAL_FIELDS if change.stage == "NORMALIZE" else PUBLISH_FIELDS if change.stage == "PUBLISH" else ())
            if not allowed:
                reasons.add("MODIFICATION_FIELD_UNSUPPORTED")
            actual = diffs.get((change.stage, change.field))
            if (actual is None or type(change.old_present) is not bool or type(change.new_present) is not bool
                    or actual[:2] != (change.old_present, change.new_present)
                    or object_hash(actual[2]) != object_hash(change.old) or object_hash(actual[3]) != object_hash(change.new)):
                reasons.add("MODIFICATION_VALUE_MISMATCH")
            expected_basis = "SERVER_NORMALIZATION" if change.stage == "NORMALIZE" else (
                "SOURCE_RUN" if change.field == "source_run_id" else "TREND_RULE" if change.field == "trend_entry_rule_version" else "A2_AUTHORITY")
            if change.basis != expected_basis:
                reasons.add("MODIFICATION_BASIS_MISMATCH")
            if change.field in PERMISSION_FIELDS and change.stage == "PUBLISH":
                if not inputs.authority:
                    reasons.add("A2_AUTHORITY_PROOF_MISSING")
                elif change.authority_sha256 != inputs.authority.value.sha256:
                    reasons.add("MODIFICATION_AUTHORITY_HASH_MISMATCH")
            entry = {"stage": change.stage if change.stage in {"NORMALIZE", "PUBLISH"} else "UNSUPPORTED",
                "field": change.field if allowed else "UNSUPPORTED", "field_name_sha256": object_hash(change.field),
                "old_present": change.old_present is True, "new_present": change.new_present is True,
                "old_sha256": object_hash(change.old), "new_sha256": object_hash(change.new),
                "basis": expected_basis, "authority_sha256": change.authority_sha256 if _sha(change.authority_sha256) else None}
            for name, value in (("old", change.old), ("new", change.new)):
                safe, projection = self._safe_value(change.field, value, inputs.run_id)
                if allowed and safe:
                    entry[name + "_value"] = projection
            output.append(entry)
        return sorted(output, key=lambda row: (row["stage"], row["field"], row["field_name_sha256"]))

    @staticmethod
    def _safe_value(field, value, run_id):
        if value is None:
            return True, None
        if field in {"trigger_low", "trigger_high", "stop_level", "no_chase", "confirmation_bars"}:
            if isinstance(value, str) and len(value) <= 24 and re.fullmatch(r"-?[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?", value):
                return math.isfinite(float(value)), value
            return (isinstance(value, (float, int)) and not isinstance(value, bool) and math.isfinite(value)), value
        if field == "symbol":
            return isinstance(value, str) and re.fullmatch(r"(?:[0-9]{6}\.(SH|SZ|BJ)|SHSE\.[0-9]{6}|SZSE\.[0-9]{6})", value) is not None, value
        if field in {"execution_permission", "a2_execution_permission"}:
            return isinstance(value, str) and value in SAFE_PERMISSION_VALUES, value
        if field == "action":
            return value == "BUY_SIGNAL", value
        if field == "source_run_id":
            return value == run_id, value
        if field == "trend_entry_rule_version":
            return value == "trend-ma5/2", value
        # Reason strings and unsupported fields retain hash evidence only.
        return False, None

    def _snapshots(self, proofs: tuple[SnapshotProof, ...], reasons: set[str]) -> list[dict[str, Any]]:
        if not proofs:
            reasons.add("SNAPSHOT_CHAIN_PROOF_MISSING")
            return []
        output, prior = [], None
        seen = set()
        for proof in proofs:
            self._bound(proof.snapshot, reasons)
            value = proof.snapshot.value
            sid, internal_hash = value.get("snapshot_id"), value.get("snapshot_hash")
            if not _id(sid) or not _sha(internal_hash) or sid in seen or not isinstance(value.get("data"), Mapping):
                reasons.add("SNAPSHOT_OBJECT_INVALID")
            seen.add(sid if isinstance(sid, str) else None)
            if proof.file_bytes is None or proof.file_sha256 is None:
                reasons.add("SNAPSHOT_FILE_PROOF_MISSING")
            else:
                try:
                    if (not isinstance(proof.file_bytes, bytes) or not _sha(proof.file_sha256)
                            or hashlib.sha256(proof.file_bytes).hexdigest() != proof.file_sha256):
                        reasons.add("SNAPSHOT_FILE_HASH_MISMATCH")
                    def pairs(items):
                        result = {}
                        for key, entry in items:
                            if key in result:
                                raise ValueError("duplicate key in snapshot file")
                            result[key] = entry
                        return result
                    decoded = json.loads(proof.file_bytes, object_pairs_hook=pairs)
                    if object_hash(decoded) != proof.snapshot.sha256:
                        reasons.add("SNAPSHOT_FILE_OBJECT_MISMATCH")
                except (ValueError, TypeError, UnicodeError):
                    reasons.add("SNAPSHOT_FILE_INVALID")
            if prior is None:
                if (proof.recipe_kind != "BASE_RESEARCH_DATA" or proof.parent_snapshot_hash is not None
                        or proof.hash_recipe is not None or object_hash(value.get("data")) != internal_hash):
                    reasons.add("BASE_SNAPSHOT_HASH_CONFLICT")
            else:
                if proof.parent_snapshot_hash != prior.get("snapshot_hash"):
                    reasons.add("SNAPSHOT_PARENT_HASH_CONFLICT")
                if proof.hash_recipe is None:
                    reasons.add("SNAPSHOT_HASH_RECIPE_MISSING")
                else:
                    self._bound(proof.hash_recipe, reasons)
                    recipe = proof.hash_recipe.value
                    if recipe.get("base_snapshot_hash") != prior.get("snapshot_hash") or object_hash(recipe) != internal_hash:
                        reasons.add("SNAPSHOT_HASH_RECIPE_CONFLICT")
                    expected, tag = self._snapshot_transform(proof.recipe_kind, prior, recipe, reasons)
                    if expected is not None and object_hash(expected) != object_hash(value.get("data")):
                        reasons.add("SNAPSHOT_TRANSFORM_CONFLICT")
                    if tag is not None and sid != f"{prior.get('snapshot_id')}:{tag}:{str(internal_hash)[:12]}":
                        reasons.add("SNAPSHOT_ID_HASH_CONFLICT")
                    if value.get("as_of") != prior.get("as_of"):
                        reasons.add("SNAPSHOT_AS_OF_CONFLICT")
            output.append({"snapshot_id": sid if _id(sid) else None,
                "snapshot_hash": internal_hash if _sha(internal_hash) else None,
                "object_sha256": proof.snapshot.sha256 if _sha(proof.snapshot.sha256) else None,
                "file_sha256": proof.file_sha256 if _sha(proof.file_sha256) else None,
                "parent_snapshot_id": prior.get("snapshot_id") if prior and _id(prior.get("snapshot_id")) else None,
                "parent_snapshot_hash": proof.parent_snapshot_hash if _sha(proof.parent_snapshot_hash) else None,
                "hash_recipe_sha256": proof.hash_recipe.sha256 if proof.hash_recipe and _sha(proof.hash_recipe.sha256) else None,
                "recipe_kind": proof.recipe_kind if proof.recipe_kind in {"BASE_RESEARCH_DATA", "STAGE_OVERLAY", "A2_BOTTLENECK_CONTEXT", "A3_CANDIDATE_CONTEXT", "A3_DETERMINISTIC_CONTEXT"} else "UNSUPPORTED"})
            prior = value
        return output

    @staticmethod
    def _snapshot_transform(kind, parent, recipe, reasons):
        expected = dict(parent.get("data") or {})
        keys = set(recipe)
        if kind == "STAGE_OVERLAY" and keys == {"base_snapshot_hash", "stage", "overlay"} and recipe.get("stage") in {"A2", "A3"} and isinstance(recipe.get("overlay"), Mapping):
            expected.update(recipe["overlay"])
            return expected, recipe["stage"].lower()
        if kind == "A2_BOTTLENECK_CONTEXT" and keys == {"base_snapshot_hash", "stage", "context"} and recipe.get("stage") == kind and isinstance(recipe.get("context"), Mapping):
            expected["A2_BOTTLENECK_CONTEXT"] = recipe["context"]
            return expected, "a2-bottleneck"
        if kind == "A3_CANDIDATE_CONTEXT" and keys == {"base_snapshot_hash", "stage", "origins"} and recipe.get("stage") == kind and isinstance(recipe.get("origins"), Mapping):
            origins = recipe["origins"]
            if any(value not in {"FOCUS", "WATCH_ONLY"} for value in origins.values()):
                reasons.add("SNAPSHOT_TRANSFORM_CONFLICT")
            expected["A3_CANDIDATE_ORIGIN"] = dict(sorted(origins.items()))
            expected["A3_CANDIDATE_SCOPE"] = sorted(origins)
            return expected, "a3-candidates"
        if kind == "A3_DETERMINISTIC_CONTEXT" and keys == {"base_snapshot_hash", "stage", "context"} and recipe.get("stage") == kind and isinstance(recipe.get("context"), Mapping):
            factors = expected.get("FACTOR_SNAPSHOT")
            if isinstance(factors, Mapping):
                bounded = {}
                for symbol, factor in factors.items():
                    if not isinstance(factor, Mapping):
                        continue
                    projected = dict(factor)
                    if isinstance(factor.get("timeframes"), Mapping):
                        projected["timeframes"] = {key: value for key, value in factor["timeframes"].items() if str(key) in {"monthly", "weekly", "daily"}}
                    summary = factor.get("technical_summary")
                    if isinstance(summary, Mapping):
                        projected["technical_summary"] = dict(summary)
                        if isinstance(summary.get("timeframes"), Mapping):
                            projected["technical_summary"]["timeframes"] = {key: value for key, value in summary["timeframes"].items() if str(key) in {"monthly", "weekly", "daily"}}
                    bounded[str(symbol)] = projected
                expected["FACTOR_SNAPSHOT"] = bounded
            expected["A3_DETERMINISTIC_CONTEXT"] = recipe["context"]
            return expected, "a3-gate"
        reasons.add("SNAPSHOT_TRANSFORM_UNSUPPORTED")
        return None, None

    @staticmethod
    def _dates(inputs, calendar, reasons):
        target = expiry = observed = None
        try:
            day = inputs.target_trade_date
            if isinstance(day, datetime) or not isinstance(day, date) or not calendar.is_trading_day(day):
                raise ValueError()
            now, expires = _aware(inputs.published_at), _aware(inputs.server_expires_at)
            expected = inputs.minimum_trade_date or calendar.next_trading_day(now.date())
            if isinstance(expected, datetime) or not isinstance(expected, date):
                raise ValueError()
            if not calendar.is_trading_day(expected):
                expected = calendar.next_trading_day(expected)
            if inputs.publication_mode != "CLOSE":
                reasons.add("PUBLICATION_MODE_UNSUPPORTED")
            if expected != day or expires.date() != day or expires <= now or expires.time().replace(tzinfo=None) < time(15):
                reasons.add("PUBLICATION_TARGET_EXPIRY_CONFLICT")
            for payload in (inputs.normalized_payload.value, inputs.published_payload.value):
                if payload.get("target_trade_date") is not None and payload["target_trade_date"] != day.isoformat():
                    reasons.add("PAYLOAD_TARGET_CONFLICT")
                if payload.get("plan_expiry") is not None and _aware(payload["plan_expiry"]) != expires:
                    reasons.add("PLAN_EXPIRY_PAYLOAD_CONFLICT")
            target, expiry, observed = day.isoformat(), expires.isoformat(), now.isoformat()
        except (AttributeError, TypeError, ValueError):
            reasons.add("PUBLICATION_DATE_PROOF_INVALID")
        return target, expiry, observed


def validate_publication_receipt(receipt: Mapping[str, Any], inputs: PublicationInputs,
                                 *, calendar: ExchangeTradingCalendar) -> dict[str, Any]:
    """Validate archive against the original current-call evidence, not itself."""
    try:
        expected = PublicationReceiptBuilder().build(inputs, calendar=calendar)
        body = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
        valid = (receipt.get("schema_version") == SCHEMA and receipt.get("eligibility_released") is False
                 and object_hash(body) == receipt.get("receipt_sha256") and object_hash(receipt) == object_hash(expected))
        reasons = [] if valid else ["RECEIPT_ORIGINAL_INPUT_MISMATCH"]
        return {"valid": valid, "evidence_complete": valid and expected["evidence_complete"],
                "eligibility_released": False, "reason_codes": reasons}
    except (AttributeError, TypeError, ValueError, OverflowError):
        return {"valid": False, "evidence_complete": False, "eligibility_released": False,
                "reason_codes": ["RECEIPT_INVALID"]}
