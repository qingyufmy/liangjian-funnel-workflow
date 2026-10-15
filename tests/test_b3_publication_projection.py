"""Declarative local originals, not production/historical publication claims."""
from copy import deepcopy
from dataclasses import replace
import json

import pytest

from test_wp5_publication_receipt import fixture, bound, build, NOW, EXPIRY, RUN
from test_workflow_lark_notifications import FakeNotifier, _plan as legacy_plan
from liangjian_funnel.runtime.publication_projection import (
    capture_publication_projection, validate_projected_payload, card_plan_binding,
    encode_projection_inputs, decode_projection_inputs,
)
from liangjian_funnel.runtime.lark_notifications import WorkflowLarkPublisher
from liangjian_funnel.runtime.state import RuntimeStore


def _plan(index):
    row = legacy_plan(index)
    row["plan_id"] = f"run-close-1:lane_1:plan-{index}"
    row["expires_at"] = "2026-09-02T15:00:00+08:00"
    return row


def projected():
    original = fixture(permission=True, strategy="TREND_MA5")
    shadow = {"schema_version": "a4-shadow-inputs/1", "status": "DATA_LIMITED",
              "gap_codes": ["FROZEN_FACTOR_MISSING"], "execution_authority": False}
    before = deepcopy(original.submitted_payload.value)
    after = {**before, "strategy_facts": {"shadow_inputs": shadow}}
    return capture_publication_projection(original, shadow_projection=bound(shadow),
        submitted_payload=bound(after), published_payload=bound(deepcopy(after)))


def test_full_original_projection_is_exact_and_unchanged():
    value = projected()
    before = deepcopy(value)
    receipt = build(value)
    assert receipt["evidence_complete"] is True, receipt
    assert any(change["basis"] == "SHADOW_PROJECTION" for change in receipt["modifications"])
    assert value == before
    assert validate_projected_payload(value, payload=value.published_payload.value,
        raw_plan=value.raw_plan.value, run_id=value.run_id, lane_id=value.lane_id,
        plan_id=value.plan_id)["evidence_complete"] is True


def test_projection_does_not_use_final_payload_as_authority():
    value = projected()
    bad = deepcopy(value.published_payload.value)
    bad["strategy_facts"]["injected"] = "unproved"
    forged = replace(value, submitted_payload=bound(bad), published_payload=bound(bad))
    assert build(forged)["evidence_complete"] is False
    assert validate_projected_payload(value, payload=bad, raw_plan=value.raw_plan.value,
        run_id=value.run_id, lane_id=value.lane_id, plan_id=value.plan_id)["evidence_complete"] is False


@pytest.mark.parametrize("mutation", ["authority", "shadow_hash", "shadow_authority", "normal_alias", "identity"])
def test_unproved_changes_fail_closed(mutation):
    value = projected()
    if mutation == "authority": value = replace(value, authority=None)
    elif mutation == "shadow_hash": value = replace(value, shadow_projection=replace(value.shadow_projection, sha256="f"*64))
    elif mutation == "shadow_authority":
        value = replace(value, shadow_projection=bound({**value.shadow_projection.value, "execution_authority": True}))
    elif mutation == "normal_alias":
        value = replace(value, normalized_payload=bound({**value.normalized_payload.value, "trigger_low": 123}))
    elif mutation == "identity": value = replace(value, run_id="another-run")
    assert build(value)["evidence_complete"] is False


def test_missing_shadow_proof_keeps_old_strict_failure():
    value = projected()
    assert build(replace(value, shadow_projection=None))["evidence_complete"] is False


def test_expiry_child_pass_cannot_override_full_authority_conflict():
    value = projected()
    assert value.normalized_payload.value["plan_expiry"] == value.server_expires_at.isoformat()
    no_authority = replace(value, authority=None)
    result = validate_projected_payload(no_authority, payload=value.published_payload.value,
        raw_plan=value.raw_plan.value, run_id=value.run_id, lane_id=value.lane_id,
        plan_id=value.plan_id)
    assert result["evidence_complete"] is False
    assert "A2_AUTHORITY_PROOF_MISSING" in result["reason_codes"]


def test_all_non_shadow_strategy_facts_must_stay_exact():
    value = fixture(permission=True)
    facts = {"daily": {"ma5": 10}, "opaque": {"original": "preserve"}}
    raw = {**value.raw_plan.value, "strategy_facts": facts}
    output = {**value.a3_audit.value["output"], "core_watch_pool": [raw]}
    from liangjian_funnel.runtime.publication_receipt import object_hash
    audit = {**value.a3_audit.value, "output": output, "output_hash": object_hash(output)}
    normalized = {**value.normalized_payload.value, "strategy_facts": facts}
    value = replace(value, raw_plan=bound(raw), a3_audit=bound(audit), normalized_payload=bound(normalized))
    shadow = bound({"schema_version": "a4-shadow-inputs/1", "status": "DATA_LIMITED", "execution_authority": False})
    published = {**value.submitted_payload.value, "strategy_facts": {**facts, "shadow_inputs": shadow.value}}
    valid = capture_publication_projection(value, shadow_projection=shadow,
        submitted_payload=bound(published), published_payload=bound(published))
    assert build(valid)["evidence_complete"] is True
    changed = deepcopy(published); changed["strategy_facts"]["daily"]["ma5"] = 11
    invalid = capture_publication_projection(value, shadow_projection=shadow,
        submitted_payload=bound(changed), published_payload=bound(changed))
    assert build(invalid)["evidence_complete"] is False


def test_serialized_original_envelope_is_self_pinned_but_not_self_certifying():
    value = projected()
    envelope = encode_projection_inputs(value)
    decoded = decode_projection_inputs(json.loads(json.dumps(envelope)))
    assert build(decoded)["evidence_complete"] is True
    envelope["inputs"]["published_payload"]["value"]["stop_level"] = 999
    with pytest.raises(ValueError): decode_projection_inputs(envelope)
    from liangjian_funnel.runtime.publication_receipt import object_hash
    envelope["envelope_sha256"] = object_hash({key: item for key, item in envelope.items() if key != "envelope_sha256"})
    assert build(decode_projection_inputs(envelope))["evidence_complete"] is False


def test_card_pages_bind_real_plan_ids_and_not_themes_or_symbols():
    rows = [_plan(i) for i in range(1, 6)]
    result = card_plan_binding(rows, rows[:3], source_run_id="run-close-1",
                              trade_date="2026-09-02", displayed_symbols=["000001", "000002", "000003"])
    assert result["status"] == "BOUND"
    assert result["plan_ids"] == [row["plan_id"] for row in rows[:3]]
    for fake in ("000001.SZ", "AI算力", "run:wrong-lane:logical", "wrong-run:lane_1:logical"):
        bad = deepcopy(rows[:3]); bad[0]["plan_id"] = fake
        assert card_plan_binding(bad, bad, source_run_id="run-close-1", trade_date="2026-09-02",
            displayed_symbols=[row["symbol"] for row in bad])["status"] == "DATA_LIMITED"
    assert card_plan_binding(rows, rows[:3], source_run_id="run-close-1", trade_date="2026-09-02",
        displayed_symbols=["000004", "000002", "000003"])["status"] == "DATA_LIMITED"
    outside = _plan(7)
    assert card_plan_binding(rows, [outside], source_run_id="run-close-1", trade_date="2026-09-02",
        displayed_symbols=[outside["symbol"]])["status"] == "DATA_LIMITED"


def test_actual_premarket_path_archives_overview_all_and_page_only(tmp_path):
    store = RuntimeStore(tmp_path / "state.sqlite3")
    publisher = WorkflowLarkPublisher(store, "https://open.larksuite.com/open-apis/bot/v2/hook/test-token")
    fake = FakeNotifier(); publisher.notifier = fake
    rows = [_plan(i) for i in range(1, 6)]
    publisher.publish_a3_premarket_analysis(rows, analyzed_at=NOW.replace(year=2026, month=9, day=2),
        source_run_id="run-close-1", research_context={"status": "READY"})
    deliveries = store.list_notification_deliveries(kind="PREMARKET_A3_ANALYSIS")
    assert len(deliveries) == 3
    ids = {row["plan_id"] for row in rows}
    page_ids = []
    for row in deliveries:
        payload = json.loads(row["payload_json"])
        from pathlib import Path
        archived = json.loads(Path(payload["card_evidence_path"]).read_text(encoding="utf-8"))
        assert archived["plan_ids"] == payload["plan_ids"]
        assert payload["plan_binding"]["status"] == "BOUND"
        if payload["card"] == "overview": assert set(payload["plan_ids"]) == ids
        else:
            assert len(payload["plan_ids"]) <= 3
            page_ids.extend(payload["plan_ids"])
    assert set(page_ids) == ids and len(page_ids) == len(ids)
    # No internal IDs leak into customer text; channel/title/body are unchanged.
    assert all(not any(plan_id in str(call) for plan_id in ids) for call in fake.calls)


@pytest.mark.parametrize("expiry", [None, "not-a-date", "2026-09-01T15:00:00+08:00",
                                    "2026-09-02T15:00:00"])
def test_card_binding_does_not_certify_missing_malformed_stale_or_naive_db_expiry(expiry):
    row = _plan(1)
    row["expires_at"] = expiry
    result = card_plan_binding([row], [row], source_run_id="run-close-1",
        trade_date="2026-09-02", displayed_symbols=[row["symbol"]])
    assert result["status"] == "DATA_LIMITED"
    assert result["plan_ids"] == []
    assert "CARD_PLAN_TRADE_DATE_CONFLICT" in result["reason_codes"]


def test_card_binding_uses_server_expiry_in_shanghai_not_old_payload_or_offset_date():
    from datetime import datetime
    row = _plan(1)
    # Same Shanghai session as the card, despite the UTC date boundary.
    row["expires_at"] = datetime.fromisoformat("2026-09-01T23:00:00+00:00")
    payload = json.loads(row["payload_json"])
    payload["plan_expiry"] = "2026-09-01T15:00:00+08:00"
    row["payload_json"] = json.dumps(payload)
    original = deepcopy(row)
    result = card_plan_binding([row], [row], source_run_id="run-close-1",
        trade_date="2026-09-02", displayed_symbols=[row["symbol"]])
    assert result["status"] == "BOUND"
    assert row == original


@pytest.mark.parametrize("stale", [False, True])
def test_card_date_metadata_never_changes_customer_delivery(tmp_path, monkeypatch, stale):
    import liangjian_funnel.runtime.publication_projection as projection
    rows = [_plan(i) for i in range(1, 6)]
    if stale:
        rows[0]["expires_at"] = "2026-09-01T15:00:00+08:00"
    before = deepcopy(rows)
    calls = []
    for index in range(2):
        store = RuntimeStore(tmp_path / f"run-{index}" / "state.sqlite3")
        publisher = WorkflowLarkPublisher(store, "https://open.larksuite.com/open-apis/bot/v2/hook/test-token")
        fake = FakeNotifier()
        publisher.notifier = fake
        if index:
            monkeypatch.setattr(projection, "card_plan_binding", lambda all_plans, selected, **kwargs: {
                "status": "BOUND", "plan_ids": [row["plan_id"] for row in selected]})
        publisher.publish_a3_premarket_analysis(rows,
            analyzed_at=NOW.replace(year=2026, month=9, day=2),
            source_run_id="run-close-1", research_context={"status": "READY"})
        calls.append(fake.calls)
    assert calls[0] == calls[1]
    assert len(calls[0]) == 3
    assert rows == before


def test_cluster_labels_are_frozen_metadata_only_unknown_not_a_shared_cluster():
    rows = [_plan(i) for i in range(1, 4)]
    for row in rows[:2]:
        known = json.loads(row["payload_json"])
        known["theme_id"] = "AI_COMPUTE"
        row["payload_json"] = json.dumps(known)
    payload = json.loads(rows[2]["payload_json"])
    payload.pop("theme")
    rows[2]["payload_json"] = json.dumps(payload)
    before = deepcopy(rows)
    result = card_plan_binding(rows, rows, source_run_id="run-close-1",
        trade_date="2026-09-02", displayed_symbols=[row["symbol"] for row in rows])
    assert result["status"] == "BOUND"
    assert result["distinct_known_clusters"] == 1
    assert [row["cluster_count"] for row in result["clusters"]] == [2,2,None]
    assert result["clusters"][-1]["cluster_label"] == "UNKNOWN"
    assert result["plan_ids"] == [row["plan_id"] for row in rows]
    assert rows == before
