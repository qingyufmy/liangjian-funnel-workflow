"""Original-object fixtures only, not actual publication or historical replay."""
from copy import deepcopy
from dataclasses import replace
from datetime import date, datetime
import hashlib
import json
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.runtime.publication_receipt import (
    AuthorityProof, BoundObject, FieldChange, PublicationInputs,
    PublicationReceiptBuilder, SnapshotProof, object_hash, validate_publication_receipt,
)
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar

NOW = datetime(2026, 10, 8, 16, tzinfo=ZoneInfo("Asia/Shanghai"))
TARGET = date(2026, 10, 9)
EXPIRY = datetime(2026, 10, 9, 15, tzinfo=ZoneInfo("Asia/Shanghai"))
RUN, LANE, SYMBOL = "fixture-close", "lane_1", "600000.SH"


def bound(value):
    return BoundObject(value, object_hash(value))


def snapshot(value, *, recipe_kind="BASE_RESEARCH_DATA", parent=None, recipe=None):
    raw_bytes = json.dumps(value, ensure_ascii=False, sort_keys=True).encode()
    return SnapshotProof(bound(value), raw_bytes, hashlib.sha256(raw_bytes).hexdigest(),
                         parent, bound(recipe) if recipe is not None else None, recipe_kind)


def changes(before, after, stage, authority=None):
    output = []
    for field in sorted(set(before) | set(after)):
        if field in before and field in after and object_hash(before[field]) == object_hash(after[field]):
            continue
        basis = "SERVER_NORMALIZATION" if stage == "NORMALIZE" else (
            "SOURCE_RUN" if field == "source_run_id" else "TREND_RULE" if field == "trend_entry_rule_version"
            else "A2_AUTHORITY")
        output.append(FieldChange(stage, field, field in before, field in after,
                                 before.get(field), after.get(field), basis, authority))
    return tuple(output)


def fixture(*, permission=False, strategy="MA520_SWING"):
    raw = {"plan_id": "logical", "symbol": SYMBOL, "strategy_profile": strategy,
           "trigger_zone": {"low": "10", "high": 11}, "invalidation_level": 9,
           "no_chase_price": 12, "confirmation_bars": 2,
           "plan_expiry": EXPIRY.isoformat(), "analysis_summary": "private-model-text"}
    normalized = {**raw, "trigger_low": 10.0, "trigger_high": 11.0, "stop_level": 9.0,
                  "no_chase": 12.0, "confirmation_bars": 1, "action": "BUY_SIGNAL"}
    published = {**normalized, "source_run_id": RUN}
    authority = None
    if strategy == "TREND_MA5":
        published["trend_entry_rule_version"] = "trend-ma5/2"
    data = {"fixture": "base", "untouched": {"private": "snapshot-text"}}
    base = {"snapshot_id": "base", "snapshot_hash": object_hash(data), "as_of": NOW.isoformat(), "data": data}
    overlay = {"fixture_a3": 1}
    recipe = {"base_snapshot_hash": base["snapshot_hash"], "stage": "A3", "overlay": overlay}
    child_hash = object_hash(recipe)
    child = {"snapshot_id": f"base:a3:{child_hash[:12]}", "snapshot_hash": child_hash,
             "as_of": NOW.isoformat(), "data": {**data, **overlay}}
    chain = (snapshot(base), snapshot(child, recipe_kind="STAGE_OVERLAY",
                                     parent=base["snapshot_hash"], recipe=recipe))
    if permission:
        row = {"symbol": SYMBOL, "execution_permission": "REQUIRES_A3_A4_CONFIRMATION",
               "research_only_reason": None}
        output = {"focus_pool": [row]}
        source = {"stage": "A2", "lane": LANE, "snapshot_id": "base", "output": output,
                  "output_hash": object_hash(output)}
        authority = AuthorityProof(bound(row), bound(source), ("output", "focus_pool", 0), "A2_AUDIT_ROW")
        published.update(a2_execution_permission=row["execution_permission"], a2_research_only_reason=None,
                         execution_permission=row["execution_permission"], research_only_reason=None)
    a3output = {"core_watch_pool": [raw], "secondary_watch_pool": [], "analysis_summary": "private-audit-text"}
    audit = {"lane": LANE, "stage": "A3", "status": "VALIDATED", "snapshot_id": child["snapshot_id"],
             "output": a3output, "output_hash": object_hash(a3output)}
    observed_changes = changes(raw, normalized, "NORMALIZE") + changes(normalized, published, "PUBLISH",
        authority.value.sha256 if authority else None)
    return PublicationInputs(run_id=RUN, lane_id=LANE, plan_id=f"{RUN}:{LANE}:logical",
        a3_audit=bound(audit), raw_plan=bound(raw), raw_plan_location=("core_watch_pool", 0),
        normalized_payload=bound(normalized), submitted_payload=bound(published),
        published_payload=bound(published), changes=observed_changes, authority=authority,
        snapshot_chain=chain, target_trade_date=TARGET, server_expires_at=EXPIRY,
        published_at=NOW, publication_mode="CLOSE")


def build(inputs):
    return PublicationReceiptBuilder().build(inputs, calendar=ExchangeTradingCalendar())


def test_original_object_hashes_and_source_inputs_unchanged():
    inputs = fixture(permission=True)
    before = deepcopy(inputs)
    receipt = build(inputs)
    assert receipt["evidence_status"] == "LOCAL_RECEIPT_BOUND"
    assert receipt["evidence_complete"] is True
    assert receipt["eligibility_released"] is False
    assert receipt["implementation_status"] == "IMPLEMENTATION_PARTIAL"
    assert receipt["a3_audit_sha256"] == inputs.a3_audit.sha256
    assert receipt["normalized_payload_sha256"] == inputs.normalized_payload.sha256
    assert receipt["published_payload_sha256"] == inputs.published_payload.sha256
    assert receipt["snapshot_chain"][1]["parent_snapshot_hash"] == inputs.snapshot_chain[0].snapshot.value["snapshot_hash"]
    assert inputs == before
    assert validate_publication_receipt(receipt, inputs, calendar=ExchangeTradingCalendar())["valid"] is True
    encoded = json.dumps(receipt)
    assert "private-model-text" not in encoded
    assert "private-audit-text" not in encoded
    assert "snapshot-text" not in encoded


@pytest.mark.parametrize("field", ["a3_audit", "raw_plan", "normalized_payload", "submitted_payload", "published_payload"])
def test_wrong_original_object_hash_is_data_limited(field):
    inputs = fixture()
    value = getattr(inputs, field)
    receipt = build(replace(inputs, **{field: BoundObject(value.value, "f" * 64)}))
    assert receipt["evidence_status"] == "DATA_LIMITED"
    assert "OBJECT_HASH_MISMATCH" in receipt["reason_codes"]


def test_missing_change_not_reconstructed_as_complete_certificate():
    inputs = fixture(permission=True)
    receipt = build(replace(inputs, changes=inputs.changes[:-1]))
    assert "MODIFICATION_SET_MISMATCH" in receipt["reason_codes"]
    assert receipt["evidence_complete"] is False


def test_wrong_change_old_new_or_basis_is_not_proof():
    inputs = fixture()
    first = inputs.changes[0]
    for change in (replace(first, old=99), replace(first, new=99), replace(first, basis="MODEL")):
        assert build(replace(inputs, changes=(change, *inputs.changes[1:])))["evidence_complete"] is False


def test_permission_without_original_authority_is_unproven():
    inputs = fixture(permission=True)
    receipt = build(replace(inputs, authority=None))
    assert "A2_AUTHORITY_PROOF_MISSING" in receipt["reason_codes"]


def test_authority_must_really_belong_to_original_frozen_source():
    inputs = fixture(permission=True)
    row = {**inputs.authority.value.value, "execution_permission": "ALLOW_A4"}
    proof = replace(inputs.authority, value=bound(row))
    receipt = build(replace(inputs, authority=proof))
    assert "A2_AUTHORITY_SOURCE_MISMATCH" in receipt["reason_codes"]


def test_authority_hash_on_every_permission_change_is_required():
    inputs = fixture(permission=True)
    bad = tuple(replace(c, authority_sha256="f" * 64) if c.basis == "A2_AUTHORITY" else c for c in inputs.changes)
    assert "MODIFICATION_AUTHORITY_HASH_MISMATCH" in build(replace(inputs, changes=bad))["reason_codes"]


def test_missing_real_overlay_file_is_not_id_chain_proof():
    inputs = fixture()
    child = replace(inputs.snapshot_chain[1], file_bytes=None, file_sha256=None)
    result = build(replace(inputs, snapshot_chain=(inputs.snapshot_chain[0], child)))
    assert "SNAPSHOT_FILE_PROOF_MISSING" in result["reason_codes"]
    assert result["evidence_complete"] is False


@pytest.mark.parametrize("field,value", [("file_sha256", "f" * 64), ("parent_snapshot_hash", "f" * 64)])
def test_wrong_file_or_parent_hash_blocks(field, value):
    inputs = fixture()
    child = replace(inputs.snapshot_chain[1], **{field: value})
    assert build(replace(inputs, snapshot_chain=(inputs.snapshot_chain[0], child)))["evidence_complete"] is False


def test_same_id_set_wrong_recipe_or_unhashed_data_cannot_prove_overlay():
    inputs = fixture()
    child = inputs.snapshot_chain[1]
    body = deepcopy(child.snapshot.value)
    body["data"]["untouched"]["private"] = "forged"
    altered = snapshot(body, recipe_kind=child.recipe_kind, parent=child.parent_snapshot_hash,
                       recipe=child.hash_recipe.value)
    receipt = build(replace(inputs, snapshot_chain=(inputs.snapshot_chain[0], altered)))
    assert "SNAPSHOT_TRANSFORM_CONFLICT" in receipt["reason_codes"]


@pytest.mark.parametrize("field,value", [("target_trade_date", date(2026, 10, 12)),
    ("server_expires_at", datetime(2026, 10, 8, 15, tzinfo=NOW.tzinfo)),
    ("server_expires_at", datetime(2026, 10, 9, 14, tzinfo=NOW.tzinfo)),
    ("publication_mode", "MORNING")])
def test_target_expiry_mode_conflicts_do_not_repair_inputs(field, value):
    inputs = fixture()
    assert build(replace(inputs, **{field: value}))["evidence_complete"] is False


def test_post_publication_payload_must_equal_submitted_not_pre_normalization_guess():
    inputs = fixture()
    published = {**inputs.published_payload.value, "stop_level": 8}
    receipt = build(replace(inputs, published_payload=bound(published)))
    assert "POST_PUBLICATION_PAYLOAD_CONFLICT" in receipt["reason_codes"]


def test_missing_submitted_payload_explicit_gap():
    assert "SUBMITTED_PAYLOAD_PROOF_MISSING" in build(replace(fixture(), submitted_payload=None))["reason_codes"]


def test_receipt_tamper_and_wrong_original_inputs_fail_validation():
    inputs = fixture()
    receipt = build(inputs)
    changed = deepcopy(receipt)
    changed["published_payload_sha256"] = "f" * 64
    assert validate_publication_receipt(changed, inputs, calendar=ExchangeTradingCalendar())["valid"] is False
    altered = deepcopy(receipt)
    altered["eligibility_released"] = True
    assert validate_publication_receipt(altered, inputs, calendar=ExchangeTradingCalendar())["valid"] is False


def test_trend_rule_is_existing_captured_change_not_new_strategy():
    inputs = fixture(strategy="TREND_MA5")
    assert build(inputs)["evidence_complete"] is True
    result = build(replace(inputs, changes=tuple(c for c in inputs.changes if c.field != "trend_entry_rule_version")))
    assert result["evidence_complete"] is False


def test_real_snapshot_context_authority_has_symbol_key_not_required_row_symbol():
    inputs = fixture(permission=True)
    row = dict(inputs.authority.value.value)
    row.pop("symbol")
    base = deepcopy(inputs.snapshot_chain[0].snapshot.value)
    base["data"]["A2_BOTTLENECK_CONTEXT"] = {SYMBOL: row}
    base["snapshot_hash"] = object_hash(base["data"])
    child = deepcopy(inputs.snapshot_chain[1].snapshot.value)
    recipe = {**inputs.snapshot_chain[1].hash_recipe.value, "base_snapshot_hash": base["snapshot_hash"]}
    child["snapshot_hash"] = object_hash(recipe)
    child["snapshot_id"] = f"base:a3:{child['snapshot_hash'][:12]}"
    child["data"] = {**base["data"], **recipe["overlay"]}
    chain = (snapshot(base), snapshot(child, recipe_kind="STAGE_OVERLAY", parent=base["snapshot_hash"], recipe=recipe))
    authority = AuthorityProof(bound(row), chain[0].snapshot, ("data", "A2_BOTTLENECK_CONTEXT", SYMBOL), "SNAPSHOT_A2_CONTEXT")
    audit = deepcopy(inputs.a3_audit.value)
    audit["snapshot_id"] = child["snapshot_id"]
    observed_changes = tuple(replace(change, authority_sha256=authority.value.sha256)
        if change.basis == "A2_AUTHORITY" else change for change in inputs.changes)
    receipt = build(replace(inputs, authority=authority, snapshot_chain=chain, a3_audit=bound(audit), changes=observed_changes))
    assert receipt["evidence_complete"] is True


def test_a2_authority_from_other_lane_is_not_same_actual_call():
    inputs = fixture(permission=True)
    source = {**inputs.authority.source.value, "lane": "lane_2"}
    proof = replace(inputs.authority, source=bound(source))
    assert build(replace(inputs, authority=proof))["evidence_complete"] is False


def test_snapshot_bytes_with_duplicate_keys_are_not_valid_original_file():
    inputs = fixture()
    base = inputs.snapshot_chain[0]
    encoded = base.file_bytes.decode().replace('"snapshot_id": "base"', '"snapshot_id": "forged", "snapshot_id": "base"')
    altered = replace(base, file_bytes=encoded.encode(), file_sha256=hashlib.sha256(encoded.encode()).hexdigest())
    assert build(replace(inputs, snapshot_chain=(altered, inputs.snapshot_chain[1])))["evidence_complete"] is False


def test_future_snapshot_clock_cannot_be_from_published_call():
    inputs = fixture()
    chain = []
    for proof in inputs.snapshot_chain:
        body = {**proof.snapshot.value, "as_of": "2026-10-10T16:00:00+08:00"}
        chain.append(snapshot(body, recipe_kind=proof.recipe_kind, parent=proof.parent_snapshot_hash,
                              recipe=proof.hash_recipe.value if proof.hash_recipe else None))
    assert build(replace(inputs, snapshot_chain=tuple(chain)))["evidence_complete"] is False


def known_chain(inputs):
    base = deepcopy(inputs.snapshot_chain[0].snapshot.value)
    base["data"]["FACTOR_SNAPSHOT"] = {SYMBOL: {
        "timeframes": {"monthly": {"value": 1}, "daily": {"value": 2}, "5m": {"value": 99}},
        "technical_summary": {"timeframes": {"weekly": {}, "15m": {"value": 99}}}}}
    base["snapshot_hash"] = object_hash(base["data"])
    chain = [snapshot(base)]
    specifications = [
        ("A2_BOTTLENECK_CONTEXT", "context", {SYMBOL: {"execution_permission": None}}, "a2-bottleneck"),
        ("A3_CANDIDATE_CONTEXT", "origins", {SYMBOL: "FOCUS"}, "a3-candidates"),
        ("A3_DETERMINISTIC_CONTEXT", "context", {SYMBOL: {"decision": "fixture"}}, "a3-gate")]
    for kind, field, content, tag in specifications:
        parent = chain[-1].snapshot.value
        recipe = {"base_snapshot_hash": parent["snapshot_hash"], "stage": kind, field: content}
        data = deepcopy(parent["data"])
        if kind == "A2_BOTTLENECK_CONTEXT":
            data[kind] = content
        elif kind == "A3_CANDIDATE_CONTEXT":
            data["A3_CANDIDATE_ORIGIN"] = content
            data["A3_CANDIDATE_SCOPE"] = [SYMBOL]
        else:
            data[kind] = content
            data["FACTOR_SNAPSHOT"][SYMBOL]["timeframes"].pop("5m")
            data["FACTOR_SNAPSHOT"][SYMBOL]["technical_summary"]["timeframes"].pop("15m")
        digest = object_hash(recipe)
        child = {"snapshot_id": parent["snapshot_id"] + f":{tag}:{digest[:12]}", "snapshot_hash": digest,
                 "as_of": NOW.isoformat(), "data": data}
        chain.append(snapshot(child, recipe_kind=kind, parent=parent["snapshot_hash"], recipe=recipe))
    audit = deepcopy(inputs.a3_audit.value)
    audit["snapshot_id"] = chain[-1].snapshot.value["snapshot_id"]
    return replace(inputs, snapshot_chain=tuple(chain), a3_audit=bound(audit))


def test_all_existing_context_recipes_and_factor_projection_are_verified():
    inputs = known_chain(fixture())
    result = build(inputs)
    assert result["evidence_complete"] is True
    assert len(result["snapshot_chain"]) == 4
    assert result["snapshot_chain"][3]["parent_snapshot_id"] == inputs.snapshot_chain[2].snapshot.value["snapshot_id"]
    assert inputs.snapshot_chain[0].snapshot.value["data"]["FACTOR_SNAPSHOT"][SYMBOL]["timeframes"]["5m"]["value"] == 99


@pytest.mark.parametrize("layer", [1, 2, 3])
def test_known_recipe_wrong_context_or_recipe_object_hash_is_gap(layer):
    inputs = known_chain(fixture())
    chain = list(inputs.snapshot_chain)
    proof = chain[layer]
    recipe = {**proof.hash_recipe.value, "unexpected_original_field": "private-proof-text"}
    chain[layer] = replace(proof, hash_recipe=bound(recipe))
    result = build(replace(inputs, snapshot_chain=tuple(chain)))
    assert result["evidence_complete"] is False
    assert "private-proof-text" not in json.dumps(result)


@pytest.mark.parametrize("layer", [0, 1, 2, 3])
def test_any_missing_file_layer_still_cannot_make_id_set_complete(layer):
    inputs = known_chain(fixture())
    chain = list(inputs.snapshot_chain)
    chain[layer] = replace(chain[layer], file_bytes=None, file_sha256=None)
    assert "SNAPSHOT_FILE_PROOF_MISSING" in build(replace(inputs, snapshot_chain=tuple(chain)))["reason_codes"]


def test_duplicate_modifications_and_unknown_field_are_not_new_rules():
    inputs = fixture()
    duplicate = replace(inputs, changes=(*inputs.changes, inputs.changes[0]))
    assert "MODIFICATION_SET_MISMATCH" in build(duplicate)["reason_codes"]
    extra = FieldChange("PUBLISH", "private-model-field", False, True, None, "private-secret-value", "MODEL")
    result = build(replace(inputs, changes=(*inputs.changes, extra)))
    assert result["evidence_complete"] is False
    assert "private-secret-value" not in json.dumps(result)
    assert "private-model-field" not in json.dumps(result)


def test_raw_plan_must_be_actual_audit_member_not_similar_symbol():
    inputs = fixture()
    raw = {**inputs.raw_plan.value, "invalidation_level": 8}
    result = build(replace(inputs, raw_plan=bound(raw)))
    assert "RAW_PLAN_AUDIT_MEMBERSHIP_MISMATCH" in result["reason_codes"]


def test_file_bytes_and_envelope_object_cannot_be_different_originals():
    inputs = fixture()
    proof = inputs.snapshot_chain[1]
    body = {**proof.snapshot.value, "as_of": "2026-10-08T15:00:00+08:00"}
    raw_bytes = json.dumps(body).encode()
    proof = replace(proof, file_bytes=raw_bytes, file_sha256=hashlib.sha256(raw_bytes).hexdigest())
    result = build(replace(inputs, snapshot_chain=(inputs.snapshot_chain[0], proof)))
    assert "SNAPSHOT_FILE_OBJECT_MISMATCH" in result["reason_codes"]


def test_actual_later_same_target_expiry_can_be_recorded_without_reader_relaxation():
    inputs = fixture()
    later = EXPIRY.replace(minute=30)
    def updated(obj):
        return bound({**obj.value, "plan_expiry": later.isoformat()})
    raw = updated(inputs.raw_plan)
    audit = deepcopy(inputs.a3_audit.value)
    audit["output"]["core_watch_pool"][0] = raw.value
    audit["output_hash"] = object_hash(audit["output"])
    result = build(replace(inputs, raw_plan=raw, a3_audit=bound(audit),
        normalized_payload=updated(inputs.normalized_payload), submitted_payload=updated(inputs.submitted_payload),
        published_payload=updated(inputs.published_payload), server_expires_at=later))
    assert result["evidence_complete"] is True
    assert result["server_expires_at"] == later.isoformat()
    assert result["eligibility_released"] is False


def test_a2_permission_changes_match_existing_deterministic_authority_function():
    inputs = fixture(permission=True, strategy="TREND_MA5")
    row = {"symbol": SYMBOL, "execution_permission": "BLOCKED", "research_only_reason": "A2_EMOTION_CYCLE_NO_NEW_ENTRY",
           "independent_strategy_review": True, "trend_core_eligible": True, "a1_formal_member": True,
           "research_route_qualifications": {"TREND_MA5": {"eligible": True}}}
    source = deepcopy(inputs.authority.source.value)
    source["output"]["focus_pool"] = [row]
    source["output_hash"] = object_hash(source["output"])
    authority = AuthorityProof(bound(row), bound(source), ("output", "focus_pool", 0), "A2_AUDIT_ROW")
    payload = {**inputs.published_payload.value, "a2_execution_permission": "BLOCKED",
               "a2_research_only_reason": row["research_only_reason"]}
    observed_changes = changes(inputs.raw_plan.value, inputs.normalized_payload.value, "NORMALIZE") + changes(
        inputs.normalized_payload.value, payload, "PUBLISH", authority.value.sha256)
    result = build(replace(inputs, authority=authority, submitted_payload=bound(payload),
                           published_payload=bound(payload), changes=observed_changes))
    assert result["evidence_complete"] is True
    reason_entries = [row for row in result["modifications"] if row["field"] == "a2_research_only_reason"]
    assert "new_value" not in reason_entries[0]


@pytest.mark.parametrize("field,value", [("target_trade_date", None), ("server_expires_at", "not a timestamp"),
                                       ("raw_plan_location", ("private-model-location", "private-index"))])
def test_missing_or_malformed_proof_is_data_limited_without_text_leak(field, value):
    result = build(replace(fixture(), **{field: value}))
    assert result["evidence_status"] == "DATA_LIMITED"
    assert "private-model-location" not in json.dumps(result)
    assert "private-index" not in json.dumps(result)


def test_new_receipt_hash_alone_cannot_validate_wrong_original_objects():
    inputs = fixture()
    receipt = build(inputs)
    changed = deepcopy(receipt)
    changed["normalized_payload_sha256"] = "f" * 64
    changed["receipt_sha256"] = object_hash({key: value for key, value in changed.items() if key != "receipt_sha256"})
    assert validate_publication_receipt(changed, inputs, calendar=ExchangeTradingCalendar())["valid"] is False


def test_original_objects_changed_during_build_cannot_be_sealed_complete(monkeypatch):
    inputs = fixture()
    builder = PublicationReceiptBuilder()
    original = builder._snapshots
    def mutate(proofs, reasons):
        result = original(proofs, reasons)
        inputs.a3_audit.value["private_after_hash"] = "private-concurrent-model-text"
        return result
    monkeypatch.setattr(builder, "_snapshots", mutate)
    receipt = builder.build(inputs, calendar=ExchangeTradingCalendar())
    assert receipt["evidence_complete"] is False
    assert "OBJECT_HASH_MISMATCH" in receipt["reason_codes"]
    assert "private-concurrent-model-text" not in json.dumps(receipt)


def test_numeric_string_old_geometry_is_safe_but_not_arbitrary_model_text():
    inputs = fixture()
    raw = {**inputs.raw_plan.value, "trigger_low": "10"}
    audit = deepcopy(inputs.a3_audit.value)
    audit["output"]["core_watch_pool"][0] = raw
    audit["output_hash"] = object_hash(audit["output"])
    observed = changes(raw, inputs.normalized_payload.value, "NORMALIZE") + changes(
        inputs.normalized_payload.value, inputs.submitted_payload.value, "PUBLISH")
    result = build(replace(inputs, raw_plan=bound(raw), a3_audit=bound(audit), changes=observed))
    assert result["evidence_complete"] is True
    change = next(row for row in result["modifications"] if row["field"] == "trigger_low")
    assert change["old_value"] == "10"
    assert change["new_value"] == 10.0


def test_module_has_no_persistence_settings_network_or_publisher_entrypoint():
    import ast
    from pathlib import Path
    path = Path(__file__).resolve().parents[1] / "src/liangjian_funnel/runtime/publication_receipt.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported = [node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
    imported.extend(alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names)
    assert not any(any(word in name for word in ("settings", "runtime.state", "sqlite", "workflow", "httpx", "requests")) for name in imported)
    assert not any(isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in {
        "open", "RuntimeStore", "Settings", "WorkflowApplication"} for node in ast.walk(tree))
