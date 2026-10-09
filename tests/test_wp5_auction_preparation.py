"""Shadow auction split contract, never scheduler/production acceptance."""
from copy import deepcopy
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.pipeline.snapshot import FrozenInputSnapshot, UniverseSnapshot
from liangjian_funnel.data.cninfo import CninfoFetchResult
from liangjian_funnel.runtime.auction_preparation import (
    PreparationError, build_slow_bundle, write_slow_bundle, fast_preflight,
)

TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 10, 9, 7, tzinfo=TZ)
SCOPE = ["000001.SZ", "600000.SH"]
CALENDAR = ["2026-09-29", "2026-09-30", "2026-10-08", "2026-10-09"]
ROLES = ["FINANCIALS", "MEMBERSHIPS", "GOV_POLICY_90D", "BUSINESS_REPORTS", "BUSINESS_PDFS", "DEFERRED_QUEUE"]


def digest(body):
    return hashlib.sha256(body).hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    return digest(path.read_bytes())


def setup(tmp_path, *, generation="2026-10-08", plans=1):
    created = datetime.fromisoformat(generation + "T22:00:00+08:00")
    universe = UniverseSnapshot.from_records([
        {"thscode": symbol, "name": "fixture", "close": 10, "volume": 100, "amount": 1000}
        for symbol in SCOPE], as_of=created)
    snapshot = FrozenInputSnapshot.freeze(universe, as_of=created, max_candidates=2,
                                          retain_incomplete=True)
    snapshot_path = tmp_path / "snapshot.json"
    snapshot_path.write_text(snapshot.model_dump_json(), encoding="utf-8")
    binding = {"path": "snapshot.json", "sha256": digest(snapshot_path.read_bytes()),
               "snapshot_id": snapshot.snapshot_id, "snapshot_hash": snapshot.snapshot_hash}
    components = {}
    for role in ROLES:
        path = tmp_path / (role + ".json")
        components[role] = {"path": path.name, "sha256": save(path, {"fixture": role}),
            "source_id": "local-fixture", "source_fetched_at": (created - timedelta(hours=1)).isoformat(),
            "complete": True, "scope": SCOPE,
            "window_from": "2026-07-01", "window_through": generation}
    bundle = build_slow_bundle(input_root=tmp_path, generation_id="fixture-night-" + generation,
        trade_date=generation, scope=SCOPE, created_at=created,
        components=components, snapshot_reference=binding)
    bundle_path = tmp_path / "slow.json"
    slow_hash = write_slow_bundle(bundle_path, bundle, input_root=tmp_path)
    fast = {}
    for role in ("PREVIOUS_MARKET_FINAL", "NEWS", "MACRO"):
        path = tmp_path / (role + ".json")
        fast[role] = {"path": path.name, "sha256": save(path, {"fixture": role}),
            "source_id": "local-fixture", "source_fetched_at": NOW.isoformat(),
            "complete": True, "finalized": True,
            "trade_date": "2026-10-08" if role == "PREVIOUS_MARKET_FINAL" else "2026-10-09"}
    publication = {"complete": True, "source_sha256": "a" * 64,
        "previous_close_trade_date": "2026-10-08", "plans": [
            {"symbol": SCOPE[0], "status": "PENDING_MORNING_REVIEW", "a3_published": True,
             "published_trade_date": "2026-10-08", "target_trade_date": "2026-10-09"}
        ] if plans else []}
    positions = {"complete": True, "source_sha256": "b" * 64,
                 "as_of": NOW.isoformat(), "records": [{"symbol": SCOPE[1], "total_qty": 100}]}
    results = {symbol: CninfoFetchResult(symbol=symbol, start_date="2026-09-29",
        end_date="2026-10-09", ok=True, complete=True, reason_code="NO_RECORDS",
        total=0, fetched_at=NOW, metadata={"search_keyword": ""}).model_dump(mode="json") for symbol in SCOPE}
    return dict(input_root=tmp_path, slow_bundle_path=bundle_path, expected_slow_sha256=slow_hash,
        expected_scope=SCOPE, now=NOW, trade_calendar=CALENDAR,
        plan_publication=publication, positions=positions,
        fast_inputs=fast, delta_results=results, budget_seconds=60)


def test_shadow_ready_preserves_component_refs_no_payload_or_execution_authority(tmp_path):
    args = setup(tmp_path)
    result = fast_preflight(**args)
    assert result["contract_status"] == "READY"
    assert result["implementation_status"] == "IMPLEMENTATION_PARTIAL"
    assert result["publish_plans"] is False and result["eligibility_released"] is False
    assert result["network_requests"] == 0 and result["slow_fallback_attempted"] is False
    assert result["delta_scope"] == SCOPE and result["delta_count"] == 2
    assert result["query_end"] == "2026-10-09" and result["disclosure_ttl_seconds"] == 21600
    assert result["slow_trade_day_lag"] == 1
    assert result["research_index_allowed"] is True
    bundle = json.loads(args["slow_bundle_path"].read_text())
    assert '"fixture":' not in json.dumps(bundle)
    assert len(bundle["components"]) == 6


def test_immutable_write_refuses_overwrite(tmp_path):
    args = setup(tmp_path)
    original = args["slow_bundle_path"].read_bytes()
    with pytest.raises(PreparationError, match="REFUSE_OVERWRITE"):
        write_slow_bundle(args["slow_bundle_path"], json.loads(original), input_root=tmp_path)
    assert args["slow_bundle_path"].read_bytes() == original


@pytest.mark.parametrize("fault", ["raw_file", "metadata", "rehashed_metadata", "scope", "snapshot", "escape"])
def test_slow_hash_scope_snapshot_and_source_binding_fail_closed(tmp_path, fault):
    args = setup(tmp_path)
    bundle = json.loads(args["slow_bundle_path"].read_text())
    if fault == "raw_file": (tmp_path / "FINANCIALS.json").write_text("changed")
    elif fault == "metadata": bundle["components"]["FINANCIALS"]["source_fetched_at"] = NOW.isoformat()
    elif fault == "rehashed_metadata":
        bundle["generation_id"] = "reblessed"
        body = {k:v for k,v in bundle.items() if k != "manifest_sha256"}
        bundle["manifest_sha256"] = digest(json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode())
    elif fault == "scope": args["expected_scope"] = [SCOPE[0]]
    elif fault == "snapshot": (tmp_path / "snapshot.json").write_text("{}")
    elif fault == "escape": bundle["components"]["FINANCIALS"]["path"] = "../outside.json"
    if fault in {"metadata", "rehashed_metadata", "escape"}:
        args["slow_bundle_path"].write_text(json.dumps(bundle))
    result = fast_preflight(**args)
    assert result["contract_status"] == "BLOCKED"
    assert result["research_index_allowed"] is False and result["slow_fallback_attempted"] is False


def test_missing_slow_never_network_falls_back(tmp_path):
    args = setup(tmp_path)
    args["slow_bundle_path"] = tmp_path / "missing.json"
    result = fast_preflight(**args)
    assert result["reason_code"] == "SLOW_BUNDLE_MISSING"
    assert result["delta_count"] == 0 and result["slow_fallback_attempted"] is False


def test_stale_trade_calendar_not_wall_days_retains_research_but_no_release(tmp_path):
    args = setup(tmp_path, generation="2026-09-30")
    result = fast_preflight(**args)
    assert result["reason_code"] == "SLOW_EVIDENCE_STALE"
    assert result["slow_trade_day_lag"] == 2
    assert result["research_index_allowed"] is True
    assert result["eligibility_released"] is False
    assert all(v == "SLOW_EVIDENCE_STALE" for v in result["stock_blocks"].values())


def test_scope_only_previous_published_a3_target_day_and_positive_positions(tmp_path):
    args = setup(tmp_path)
    args["plan_publication"]["plans"] += [
        {"symbol": "600519.SH", "status": "ACTIVE_TODAY", "target_trade_date": "2026-10-09"},
        {"symbol": "600519.SH", "status": "PENDING_MORNING_REVIEW", "target_trade_date": "2026-10-12"},
    ]
    args["positions"]["records"] += [{"symbol": "600519.SH", "total_qty": 0}]
    assert fast_preflight(**args)["delta_scope"] == SCOPE
    args["delta_results"]["600519.SH"] = args["delta_results"][SCOPE[0]]
    assert fast_preflight(**args)["reason_code"] == "DELTA_SCOPE_OUT_OF_BOUNDS"


@pytest.mark.parametrize("fault", ["expired", "yesterday_end", "missing", "partial", "wrong_symbol", "future", "filtered", "stale_fallback"])
def test_each_failed_delta_stock_is_gap_not_reblessed_by_slow(tmp_path, fault):
    args = setup(tmp_path)
    row = args["delta_results"][SCOPE[0]]
    if fault == "expired": row["fetched_at"] = (NOW - timedelta(hours=6, seconds=1)).isoformat()
    elif fault == "yesterday_end": row["end_date"] = "2026-10-08"
    elif fault == "missing": args["delta_results"].pop(SCOPE[0])
    elif fault == "partial": row.update(ok=False, complete=False, reason_code="UNAVAILABLE")
    elif fault == "wrong_symbol": row["symbol"] = SCOPE[1]
    elif fault == "future": row["fetched_at"] = (NOW + timedelta(seconds=1)).isoformat()
    elif fault == "filtered": row["metadata"]["search_keyword"] = "年度报告"
    elif fault == "stale_fallback": row["metadata"]["availability_state"] = "STALE_VERIFIED_FALLBACK"
    result = fast_preflight(**args)
    assert result["stock_blocks"][SCOPE[0]] == "RECENT_DISCLOSURE_GAP"
    assert SCOPE[1] not in result["stock_blocks"]
    assert result["reason_code"] == "RECENT_DISCLOSURE_GAP"


def test_exact_ttl_no_pending_and_zero_positive_positions(tmp_path):
    args = setup(tmp_path, plans=0)
    args["positions"]["records"] = []
    args["delta_results"] = {}
    assert fast_preflight(**args)["delta_count"] == 0
    args["positions"]["records"] = [{"symbol": SCOPE[1], "total_qty": 1}]
    args["delta_results"] = {SCOPE[1]: CninfoFetchResult(symbol=SCOPE[1], start_date="2026-09-29",
        end_date="2026-10-09", ok=True, complete=True, reason_code="NO_RECORDS", total=0,
        fetched_at=NOW-timedelta(hours=6)).model_dump(mode="json")}
    assert fast_preflight(**args)["contract_status"] == "READY"


@pytest.mark.parametrize("budget", [0, -1, 1201, float("nan"), True])
def test_explicit_budget_cannot_expand_twenty_minute_shadow_window(tmp_path, budget):
    args = setup(tmp_path)
    args["budget_seconds"] = budget
    assert fast_preflight(**args)["reason_code"] == "FAST_BUDGET_INVALID"


def test_elapsed_budget_and_forbidden_fast_slow_role(tmp_path):
    args = setup(tmp_path)
    times = iter([0, 61])
    assert fast_preflight(**args, clock=lambda: next(times))["reason_code"] == "FAST_BUDGET_EXCEEDED"
    args["fast_inputs"]["FINANCIALS"] = args["fast_inputs"]["NEWS"]
    assert fast_preflight(**args)["reason_code"] == "FAST_INPUT_ROLE_FORBIDDEN"


@pytest.mark.parametrize("fault", ["naive_source", "future_source", "short_gov", "partial_component", "component_scope"])
def test_night_manifest_cannot_stamp_unproven_source_time_or_completeness(tmp_path, fault):
    args = setup(tmp_path)
    bundle = json.loads(args["slow_bundle_path"].read_text())
    components = bundle["components"]
    if fault == "naive_source": components["FINANCIALS"]["source_fetched_at"] = "2026-10-08T21:00:00"
    elif fault == "future_source": components["FINANCIALS"]["source_fetched_at"] = NOW.isoformat()
    elif fault == "short_gov": components["GOV_POLICY_90D"]["window_from"] = "2026-10-01"
    elif fault == "partial_component": components["BUSINESS_PDFS"]["complete"] = False
    elif fault == "component_scope": components["MEMBERSHIPS"]["scope"] = [SCOPE[0]]
    with pytest.raises(PreparationError):
        build_slow_bundle(input_root=tmp_path, generation_id="new-generation", trade_date="2026-10-08",
            scope=SCOPE, created_at=datetime(2026, 10, 8, 22, tzinfo=TZ),
            components=components, snapshot_reference=bundle["snapshot_reference"])


def test_internal_manifest_hash_cannot_be_ignored_even_if_external_bytes_pin_updated(tmp_path):
    args = setup(tmp_path)
    bundle = json.loads(args["slow_bundle_path"].read_text())
    bundle["created_at"] = "2026-10-08T23:00:00+08:00"
    args["slow_bundle_path"].write_text(json.dumps(bundle))
    args["expected_slow_sha256"] = digest(args["slow_bundle_path"].read_bytes())
    assert fast_preflight(**args)["reason_code"] == "SLOW_MANIFEST_HASH_MISMATCH"


def test_external_pin_plus_internal_rehash_cannot_change_component_bytes(tmp_path):
    args = setup(tmp_path)
    bundle = json.loads(args["slow_bundle_path"].read_text())
    (tmp_path / "BUSINESS_PDFS.json").write_text("revised")
    body = {k:v for k,v in bundle.items() if k != "manifest_sha256"}
    bundle["manifest_sha256"] = digest(json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode())
    args["slow_bundle_path"].write_text(json.dumps(bundle))
    args["expected_slow_sha256"] = digest(args["slow_bundle_path"].read_bytes())
    assert fast_preflight(**args)["reason_code"] == "COMPONENT_HASH_MISMATCH"


@pytest.mark.parametrize("fault", ["calendar_missing", "backwards_clock", "positions_bool", "publication_partial", "unpublished_pending", "market_not_final", "new_market_day", "fast_file_revised", "news_old_source", "market_future_source", "night_today_generation"])
def test_invalid_calendar_scope_fast_inputs_or_clock_never_produce_ready(tmp_path, fault):
    args = setup(tmp_path)
    kwargs = {}
    if fault == "calendar_missing": args["trade_calendar"] = ["2026-10-09"]
    elif fault == "backwards_clock":
        times = iter([10, 9])
        kwargs["clock"] = lambda: next(times)
    elif fault == "positions_bool": args["positions"]["records"][0]["total_qty"] = True
    elif fault == "publication_partial": args["plan_publication"]["complete"] = False
    elif fault == "unpublished_pending": args["plan_publication"]["plans"][0]["a3_published"] = False
    elif fault == "market_not_final": args["fast_inputs"]["PREVIOUS_MARKET_FINAL"]["finalized"] = False
    elif fault == "new_market_day": args["fast_inputs"]["PREVIOUS_MARKET_FINAL"]["trade_date"] = "2026-10-09"
    elif fault == "fast_file_revised": (tmp_path / "NEWS.json").write_text("changed")
    elif fault == "news_old_source": args["fast_inputs"]["NEWS"]["source_fetched_at"] = "2026-10-08T21:00:00+08:00"
    elif fault == "market_future_source": args["fast_inputs"]["PREVIOUS_MARKET_FINAL"]["source_fetched_at"] = "2026-10-09T08:00:00+08:00"
    elif fault == "night_today_generation":
        bundle = json.loads(args["slow_bundle_path"].read_text())
        bundle["trade_date"] = "2026-10-09"
        bundle["created_at"] = "2026-10-09T06:00:00+08:00"
        for ref in bundle["components"].values(): ref["window_through"] = "2026-10-09"
        body = {k:v for k,v in bundle.items() if k != "manifest_sha256"}
        bundle["manifest_sha256"] = digest(json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode())
        args["slow_bundle_path"].write_text(json.dumps(bundle))
        args["expected_slow_sha256"] = digest(args["slow_bundle_path"].read_bytes())
    assert fast_preflight(**args, **kwargs)["contract_status"] == "BLOCKED"


def test_stale_slow_preserves_stock_delta_failure_code(tmp_path):
    args = setup(tmp_path, generation="2026-09-30")
    args["delta_results"].pop(SCOPE[0])
    result = fast_preflight(**args)
    assert result["reason_code"] == "SLOW_EVIDENCE_STALE"
    assert result["stock_blocks"][SCOPE[0]] == "RECENT_DISCLOSURE_GAP"
    assert result["stock_blocks"][SCOPE[1]] == "SLOW_EVIDENCE_STALE"


def test_requested_today_delta_is_not_a_short_today_only_query(tmp_path):
    args = setup(tmp_path)
    args["delta_results"][SCOPE[0]]["start_date"] = "2026-10-09"
    result = fast_preflight(**args)
    assert result["stock_blocks"][SCOPE[0]] == "RECENT_DISCLOSURE_GAP"


def test_older_snapshot_cannot_be_relabelled_as_new_night_generation(tmp_path):
    args = setup(tmp_path)
    old_time = datetime(2026, 9, 30, 22, tzinfo=TZ)
    old = FrozenInputSnapshot.freeze(UniverseSnapshot.from_records([
        {"thscode": s, "name": "fixture", "close": 10} for s in SCOPE], as_of=old_time),
        as_of=old_time, max_candidates=2, retain_incomplete=True)
    path = tmp_path / "old-snapshot.json"
    path.write_text(old.model_dump_json(), encoding="utf-8")
    bundle = json.loads(args["slow_bundle_path"].read_text())
    with pytest.raises(PreparationError, match="FROZEN_SNAPSHOT_ID_SCOPE_MISMATCH"):
        build_slow_bundle(input_root=tmp_path, generation_id="new-night", trade_date="2026-10-08",
            scope=SCOPE, created_at=datetime(2026, 10, 8, 22, tzinfo=TZ), components=bundle["components"],
            snapshot_reference={"path": path.name, "sha256": digest(path.read_bytes()),
                                "snapshot_id": old.snapshot_id, "snapshot_hash": old.snapshot_hash})


def test_positive_holding_outside_slow_scope_keeps_delta_risk_review_but_not_research_release(tmp_path):
    args = setup(tmp_path)
    args["positions"]["records"].append({"symbol": "600519.SH", "total_qty": 100})
    delta = deepcopy(args["delta_results"][SCOPE[0]])
    delta["symbol"] = "600519.SH"
    args["delta_results"]["600519.SH"] = delta
    result = fast_preflight(**args)
    assert "600519.SH" in result["delta_scope"]
    assert result["stock_slow_gaps"] == ["600519.SH"]
    assert result["stock_blocks"]["600519.SH"] == "SLOW_EVIDENCE_SCOPE_GAP"
    assert result["reason_code"] == "SLOW_EVIDENCE_SCOPE_GAP"
    assert result["eligibility_released"] is False
    assert result["scope_sha256"] == digest(json.dumps(SCOPE, separators=(",", ":")).encode())


@pytest.mark.parametrize("scope", [[SCOPE[0], SCOPE[0]], ["600000.sh"], [], [True]])
def test_invalid_or_partial_expected_scope_cannot_pass(tmp_path, scope):
    args = setup(tmp_path)
    args["expected_scope"] = scope
    assert fast_preflight(**args)["contract_status"] == "BLOCKED"


@pytest.mark.parametrize("status", ["EXPIRED", "INVALIDATED"])
def test_final_status_not_rewritten_into_historical_pending_scope(tmp_path, status):
    args = setup(tmp_path, plans=0)
    args["positions"]["records"] = []
    args["delta_results"] = {}
    args["plan_publication"]["plans"] = [
        {"symbol": SCOPE[0], "status": status, "explicit_target_trade_date": None,
         "source_run_id": "2026-09-30-close-fixture", "expires_at": "2026-10-08T15:00:00+08:00"}]
    result = fast_preflight(**args)
    assert result["delta_scope"] == [] and result["delta_count"] == 0


def test_shadow_cli_only_explicit_local_inputs_and_no_overwrite(tmp_path):
    args = setup(tmp_path)
    input_json = {k:v for k,v in args.items() if k not in {"input_root", "slow_bundle_path", "now"}}
    input_json["slow_bundle_path"] = "slow.json"
    input_json["now"] = NOW.isoformat()
    save(tmp_path / "request.json", input_json)
    script = Path(__file__).resolve().parents[1] / "scripts/audit_auction_fast_preflight.py"
    command = [sys.executable, "-B", str(script), "--input-root", str(tmp_path),
               "--request", "request.json", "--output", str(tmp_path / "receipt.json")]
    result = subprocess.run(command, capture_output=True, text=True)
    assert result.returncode == 2  # Partial implementation, even local READY.
    receipt = json.loads((tmp_path / "receipt.json").read_text())
    assert receipt["contract_status"] == "READY" and receipt["network_requests"] == 0
    original = (tmp_path / "receipt.json").read_bytes()
    again = subprocess.run(command, capture_output=True, text=True)
    assert again.returncode == 3 and "REFUSE_OVERWRITE" in again.stdout
    assert (tmp_path / "receipt.json").read_bytes() == original
