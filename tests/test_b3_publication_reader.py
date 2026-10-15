"""Cold local DB + independently pinned original audit/base/recipe fixtures."""
from copy import deepcopy
from dataclasses import replace
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from test_wp5_auction_publication_export import fixture as db_fixture, export, save, sha, NOW, RUN, SYMBOL
from liangjian_funnel.runtime.auction_publication_export import (
    ExportError, FilePin, FrozenDatabase, PublicationReference, ReadOnlyPublicationReader,
)
from liangjian_funnel.runtime.publication_receipt import object_hash
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
from liangjian_funnel.runtime.plan_expiry import normalize_publication_expiry
from liangjian_funnel.runtime.shadow_inputs import limited_shadow_inputs
from liangjian_funnel.workflow import _plan_payload


def bound(value): return {"value": value, "sha256": object_hash(value)}


def node(base, stage, overlay):
    recipe = {"base_snapshot_hash": base["snapshot_hash"], "stage": stage, "overlay": overlay}
    digest = object_hash(recipe)
    return {"recipe_kind": "STAGE_OVERLAY", "status": "CAPTURED",
        "parent_snapshot_id": base["snapshot_id"], "parent_snapshot_hash": base["snapshot_hash"],
        "parent_as_of": base["as_of"], "snapshot_id": f'{base["snapshot_id"]}:{stage.lower()}:{digest[:12]}',
        "snapshot_hash": digest, "as_of": base["as_of"], "hash_recipe": recipe,
        "data_recipe": {"operation": "SHALLOW_OVERLAY", "overlay": overlay}}


def with_proof(tmp_path, *, count=1):
    db, ref = db_fixture(tmp_path)
    audit, summary = ref.audit.read(), ref.summary.read()
    raw = audit["final_output"]["core_watch_pool"][0]
    raws = [dict(raw, plan_id=f"logical-{index}", symbol=f"{index+1:06d}.SZ") for index in range(count)]
    raw_ids = [f'{RUN}:lane_1:{row["plan_id"]}' for row in raws]
    data = {"original": {"fixture": "independently frozen"}}
    base = {"snapshot_id": "fixture-snapshot", "snapshot_hash": object_hash(data),
            "as_of": "2026-10-08T16:00:00+08:00", "data": data}
    base_pin = save(tmp_path / "base.json", base)
    a2_node = node(base, "A2", {"a2_fixture": "source"})
    a3_node = node(base, "A3", {"a3_fixture": "source"})
    def proof(item):
        return {"schema_version": "research-snapshot-proof/1", "status": "CAPTURED",
            "snapshot_id": item["snapshot_id"], "snapshot_hash": item["snapshot_hash"],
            "as_of": item["as_of"], "chain": [item]}
    a2_rows = [{"symbol": row["symbol"], "execution_permission": "REQUIRES_A3_A4_CONFIRMATION",
                "research_only_reason": None} for row in raws]
    a2_output = {"focus_pool": a2_rows}
    a2 = {"stage": "A2", "lane": "lane_1", "status": "VALIDATED",
        "snapshot_id": a2_node["snapshot_id"], "output": a2_output,
        "output_hash": object_hash(a2_output), "snapshot_proof": proof(a2_node)}
    final = {"core_watch_pool": raws, "secondary_watch_pool": []}
    a3 = {**audit["stages"][0], "snapshot_id": a3_node["snapshot_id"], "output": final,
        "output_hash": object_hash(final), "snapshot_proof": proof(a3_node)}
    audit.update(final_output=final, stages=[{"stage": "A1", "snapshot_id": base["snapshot_id"]}, a2, a3])
    audit_pin = save(tmp_path / "audit.json", audit)
    entries = []
    with sqlite3.connect(db.path) as conn:
        conn.execute("DELETE FROM execution_plans")
        conn.execute("UPDATE workflow_runs SET snapshot_hash=?", (base["snapshot_hash"],))
        for index, (raw, plan_id, authority) in enumerate(zip(raws, raw_ids, a2_rows)):
            expiry = __import__("datetime").datetime.fromisoformat("2026-10-09T15:00:00+08:00")
            normalized = normalize_publication_expiry(_plan_payload(raw), expiry)
            shadow = limited_shadow_inputs("FROZEN_FACTOR_MISSING")
            published = {**normalized, "source_run_id": RUN,
                "a2_execution_permission": authority["execution_permission"], "a2_research_only_reason": None,
                "execution_permission": authority["execution_permission"], "research_only_reason": None,
                "strategy_facts": {"shadow_inputs": shadow}}
            conn.execute("INSERT INTO execution_plans VALUES(?,?,?,?,?,?,?,?,?,?)", (plan_id, "lane_1", raw["symbol"],
                "PENDING_MORNING_REVIEW", 1, None, expiry.isoformat(), json.dumps(published),
                "2026-10-08T16:00:00+08:00", "2026-10-08T16:00:00+08:00"))
            entries.append({"plan_id": plan_id, "raw_plan_location": ["core_watch_pool", index],
                "raw_plan_sha256": object_hash(raw), "normalized_payload": bound(normalized),
                "submitted_payload": bound(published), "published_payload": bound(published),
                "shadow_projection": bound(shadow), "authority": {"origin": "A2_AUDIT_ROW",
                    "selector": ["output", "focus_pool", index], "value_sha256": object_hash(authority)},
                "target_trade_date": "2026-10-09", "server_expires_at": expiry.isoformat(),
                "published_at": "2026-10-08T16:00:00+08:00", "publication_mode": "CLOSE", "minimum_trade_date": None})
    summary["snapshot"] = {"snapshot_id": base["snapshot_id"], "snapshot_hash": base["snapshot_hash"]}
    summary["plan_publication"]["created"] = raw_ids
    manifest = {"schema_version": "publication-projection-manifest/1", "run_id": RUN, "lane_id": "lane_1",
        "audit": {"path": str(audit_pin.path), "sha256": audit_pin.sha256},
        "base_snapshot": {"path": str(base_pin.path), "sha256": base_pin.sha256,
            "snapshot_id": base["snapshot_id"], "snapshot_hash": base["snapshot_hash"]}, "plans": entries}
    manifest["manifest_sha256"] = object_hash(manifest)
    manifest_pin = save(tmp_path / "projection.json", manifest)
    summary["plan_publication"]["projection_manifest"] = {"path": str(manifest_pin.path), "sha256": manifest_pin.sha256}
    summary_pin = save(tmp_path / "summary.json", summary)
    return FrozenDatabase(db.path, sha(db.path), NOW), PublicationReference(RUN, "lane_1", summary_pin, audit_pin, manifest_pin)


def run_export(db, ref):
    reader_type = ReadOnlyPublicationReader
    if os.environ.get("B3_READER_LEGACY_COUNTEREXAMPLE") == "1":
        name = "liangjian_funnel.runtime._b3_original_reader"
        spec = importlib.util.spec_from_loader(name, loader=None)
        module = importlib.util.module_from_spec(spec); sys.modules[name] = module
        source = subprocess.check_output(["git", "show", "4296925:src/liangjian_funnel/runtime/auction_publication_export.py"])
        exec(compile(source, "4296925-original-reader", "exec"), module.__dict__)
        reader_type = module.ReadOnlyPublicationReader
    return reader_type(db).export(publications=(ref,), target_trade_date=NOW.date(),
        account_ids=("paper:lane_1",), calendar=ExchangeTradingCalendar())


def rewrite_manifest(ref, change):
    value = ref.projection_manifest.read(); change(value)
    value["manifest_sha256"] = object_hash({key: item for key, item in value.items() if key != "manifest_sha256"})
    updated = save(ref.projection_manifest.path, value)
    summary = ref.summary.read(); summary["plan_publication"]["projection_manifest"] = {"path": str(updated.path), "sha256": updated.sha256}
    return replace(ref, projection_manifest=updated, summary=save(ref.summary.path, summary))


def test_pinned_originals_reconcile_a2_and_shadow_without_weakening_default(tmp_path):
    db, ref = with_proof(tmp_path)
    before = (sha(db.path), sha(ref.audit.path), sha(ref.projection_manifest.path))
    result = run_export(db, ref)
    assert result["scope_complete"] is True, result["reason_codes"]
    assert result["plans"][0]["payload_binding"] == "PINNED_ORIGINAL_PUBLICATION_PROJECTION"
    assert result["eligibility_released"] is False
    assert before == (sha(db.path), sha(ref.audit.path), sha(ref.projection_manifest.path))
    old = export(db, replace(ref, projection_manifest=None))
    assert old["scope_complete"] is False
    assert "AUDIT_OVERLAY_BINDING_UNPROVEN" in old["reason_codes"]


@pytest.mark.parametrize("field", ["raw_hash", "a2_selector", "a2_value", "base", "plan_set", "normalized", "shadow", "final"])
def test_self_resealed_manifest_cannot_replace_original_authorities(tmp_path, field):
    db, ref = with_proof(tmp_path)
    def mutate(value):
        row = value["plans"][0]
        if field == "raw_hash": row["raw_plan_sha256"] = "f"*64
        elif field == "a2_selector": row["authority"]["selector"][-1] = 88
        elif field == "a2_value": row["authority"]["value_sha256"] = "f"*64
        elif field == "base": value["base_snapshot"]["snapshot_hash"] = "f"*64
        elif field == "plan_set": value["plans"] = []
        elif field == "normalized": row["normalized_payload"] = bound({**row["normalized_payload"]["value"], "stop_level": 8})
        elif field == "shadow": row["shadow_projection"] = bound({**row["shadow_projection"]["value"], "execution_authority": True})
        elif field == "final": row["published_payload"] = bound({**row["published_payload"]["value"], "stop_level": 8})
    result = run_export(db, rewrite_manifest(ref, mutate))
    assert result["scope_complete"] is False
    assert result["plans"] == []


def test_missing_recipe_stays_unproven_instead_of_fabricating_overlay_files(tmp_path):
    db, ref = with_proof(tmp_path)
    audit = ref.audit.read(); audit["stages"][2].pop("snapshot_proof")
    changed = save(ref.audit.path, audit)
    ref = replace(ref, audit=changed)
    ref = rewrite_manifest(ref, lambda item: item.update(audit={"path": str(changed.path), "sha256": changed.sha256}))
    result = run_export(db, ref)
    assert "AUDIT_OVERLAY_BINDING_UNPROVEN" in result["reason_codes"]


def test_batch_resolves_large_base_only_once_and_caches_original_json_decode(tmp_path, monkeypatch):
    db, ref = with_proof(tmp_path, count=3)
    import liangjian_funnel.runtime.auction_publication_export as reader_module
    import liangjian_funnel.runtime.publication_receipt as receipt_module
    base_path = Path(ref.projection_manifest.read()["base_snapshot"]["path"])
    original_read, original_loads = Path.read_bytes, receipt_module.json.loads
    reads, decodes = [], []
    def read(path):
        if path == base_path: reads.append(str(path))
        return original_read(path)
    def loads(value, *args, **kwargs):
        if isinstance(value, bytes) and b'"independently frozen"' in value: decodes.append(1)
        return original_loads(value, *args, **kwargs)
    monkeypatch.setattr(Path, "read_bytes", read)
    monkeypatch.setattr(receipt_module.json, "loads", loads)
    result = run_export(db, ref)
    assert result["scope_complete"] is True, result["reason_codes"]
    assert len(result["plans"]) == 3
    assert len(reads) == 1
    # One initial parse, one strict ORIGINAL_FILE decode per independent A2/A3
    # chain; not 3 plans times 2 chains. Every object remains seal-hash checked.
    assert len(decodes) <= 3


def rebind_audit(ref, audit):
    changed = save(ref.audit.path, audit)
    def update(manifest):
        manifest["audit"] = {"path": str(changed.path), "sha256": changed.sha256}
        for row in manifest["plans"]:
            row["a3_audit_sha256"] = object_hash(audit["stages"][2])
            if row.get("authority") and row["authority"]["origin"] == "A2_AUDIT_ROW":
                row["authority"]["source_sha256"] = object_hash(audit["stages"][1])
    return rewrite_manifest(replace(ref, audit=changed), update)


@pytest.mark.parametrize("at,complete", [("2026-10-08T16:01:00+08:00", True), ("2026-10-08T16:03:00+08:00", False)])
def test_actual_a2_news_clock_can_advance_but_cannot_exceed_publication(tmp_path, at, complete):
    db, ref = with_proof(tmp_path)
    audit = ref.audit.read()
    a2 = audit["stages"][1]; proof = a2["snapshot_proof"]; old = proof["chain"][0]
    recipe = {"base_snapshot_hash": old["parent_snapshot_hash"], "stage": "A2",
        "NEWS_HEAT_SNAPSHOT": {"available": True, "collected_at": at}, "A2_STOCK_NEWS_SCOPE": [SYMBOL]}
    digest = object_hash(recipe)
    item = {**old, "recipe_kind": "A2_STOCK_NEWS", "snapshot_hash": digest,
        "snapshot_id": f'{old["parent_snapshot_id"]}:a2:{digest[:12]}', "as_of": at,
        "hash_recipe": recipe, "data_recipe": {key: recipe[key] for key in ("NEWS_HEAT_SNAPSHOT", "A2_STOCK_NEWS_SCOPE")}}
    a2["snapshot_id"] = item["snapshot_id"]
    a2["snapshot_proof"] = {**proof, "snapshot_id": item["snapshot_id"], "snapshot_hash": digest, "as_of": at, "chain": [item]}
    ref = rebind_audit(ref, audit)
    ref = rewrite_manifest(ref, lambda value: value["plans"][0].update(published_at="2026-10-08T16:02:00+08:00"))
    result = run_export(db, ref)
    assert result["scope_complete"] is complete, result["reason_codes"]
    if complete:
        receipt = result["plans"][0]["publication_projection_receipt"]
        assert receipt["authority_snapshot_chain"][-1]["representation"] == "PINNED_RECIPE"
        assert receipt["authority_snapshot_chain"][-1]["file_sha256"] is None
        assert receipt["acquisition_provenance_authenticated"] is False


def test_multi_lane_manifest_summary_requires_exactly_one_matching_lane(tmp_path):
    db, ref = with_proof(tmp_path)
    summary = ref.summary.read()
    claim = summary["plan_publication"].pop("projection_manifest")
    summary["plan_publication"]["projection_manifests"] = [{**claim, "lane_id": "lane_1"}]
    ref = replace(ref, summary=save(ref.summary.path, summary))
    assert run_export(db, ref)["scope_complete"] is True
    summary["plan_publication"]["projection_manifests"].append({**claim, "lane_id": "lane_1"})
    result = run_export(db, replace(ref, summary=save(ref.summary.path, summary)))
    assert "PROJECTION_SUMMARY_REFERENCE_CONFLICT" in result["reason_codes"]


@pytest.mark.parametrize("mutation", ["data_recipe", "unknown_wrapper", "child_id", "parent_hash"])
def test_recipe_labels_are_not_transform_evidence(tmp_path, mutation):
    db, ref = with_proof(tmp_path)
    audit = ref.audit.read()
    node = audit["stages"][2]["snapshot_proof"]["chain"][0]
    if mutation == "data_recipe": node["data_recipe"]["overlay"]["injected"] = 1
    elif mutation == "unknown_wrapper": node["recipe_kind"] = "UNKNOWN_WRAPPER"
    elif mutation == "child_id": node["snapshot_id"] = "wrong-child"
    else: node["parent_snapshot_hash"] = "f"*64
    result = run_export(db, rebind_audit(ref, audit))
    assert result["scope_complete"] is False
    assert result["plans"] == []


def test_base_original_changed_after_plan_validation_is_not_sealed(tmp_path, monkeypatch):
    db, ref = with_proof(tmp_path)
    base = Path(ref.projection_manifest.read()["base_snapshot"]["path"])
    original = ReadOnlyPublicationReader._plan
    def changed(self, *args, **kwargs):
        result = original(self, *args, **kwargs)
        base.write_text("{}", encoding="utf-8")
        return result
    monkeypatch.setattr(ReadOnlyPublicationReader, "_plan", changed)
    with pytest.raises(ExportError, match="PROJECTION_REFERENCE_CHANGED"):
        run_export(db, ref)


def test_snapshot_decode_cache_never_hides_mutated_original_object():
    from test_b3_publication_projection import projected
    from liangjian_funnel.runtime.publication_receipt import PublicationReceiptBuilder
    inputs = projected(); builder = PublicationReceiptBuilder()
    assert builder.build(inputs, calendar=ExchangeTradingCalendar())["evidence_complete"] is True
    inputs.snapshot_chain[0].snapshot.value["data"]["late_mutation"] = True
    result = builder.build(inputs, calendar=ExchangeTradingCalendar())
    assert result["evidence_complete"] is False
    assert "OBJECT_HASH_MISMATCH" in result["reason_codes"]


def test_a2_rotation_recipe_is_an_independent_proven_branch_not_an_a3_parent(tmp_path):
    db, ref = with_proof(tmp_path)
    audit = ref.audit.read(); a2 = audit["stages"][1]
    parent = a2["snapshot_proof"]["chain"][-1]
    from liangjian_funnel.data.rotation_policy import POLICY_ID
    recipe = {"source_hash": parent["snapshot_hash"], "policy": POLICY_ID, "board_overlay": None}
    digest = object_hash(recipe)
    rotation = {"recipe_kind": "A2_ROTATION_POLICY", "status": "CAPTURED",
        "parent_snapshot_id": parent["snapshot_id"], "parent_snapshot_hash": parent["snapshot_hash"],
        "parent_as_of": parent["as_of"], "snapshot_id": parent["snapshot_id"] + ":rotation-policy",
        "snapshot_hash": digest, "as_of": parent["as_of"], "hash_recipe": recipe,
        "data_recipe": {"operation": "SHALLOW_OVERLAY", "overlay": {
            "A2_ROTATION_THEME_COUNT": 8, "A2_AUTHORIZED_ROTATION_POLICY": POLICY_ID}}}
    a2["snapshot_id"] = rotation["snapshot_id"]
    a2["snapshot_proof"].update(snapshot_id=rotation["snapshot_id"], snapshot_hash=digest,
        chain=[parent, rotation])
    result = run_export(db, rebind_audit(ref, audit))
    assert result["scope_complete"] is True, result["reason_codes"]
    receipt = result["plans"][0]["publication_projection_receipt"]
    assert receipt["authority_snapshot_chain"][-1]["recipe_kind"] == "A2_ROTATION_POLICY"
    assert receipt["snapshot_chain"][-1]["recipe_kind"] == "STAGE_OVERLAY"


def test_manifest_unknown_wrapper_cannot_hide_behind_base_snapshot_id(tmp_path):
    db, ref = with_proof(tmp_path)
    audit = ref.audit.read(); base = ref.projection_manifest.read()["base_snapshot"]
    audit["stages"][2]["snapshot_id"] = base["snapshot_id"]
    audit["stages"][2]["snapshot_proof"] = {"schema_version": "research-snapshot-proof/1",
        "status": "UNPROVEN", "snapshot_id": base["snapshot_id"], "snapshot_hash": base["snapshot_hash"],
        "as_of": "2026-10-08T16:00:00+08:00", "chain": []}
    result = run_export(db, rebind_audit(ref, audit))
    assert "PROJECTION_BASE_STAGE_BINDING_UNPROVEN" in result["reason_codes"]
