"""Local DB fixtures are not historical/natural-session acceptance evidence."""
from datetime import date, datetime
import hashlib
import json
from pathlib import Path
import re
import sqlite3
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.runtime.auction_publication_export import (
    ExportError, FilePin, FrozenDatabase, PublicationReference,
    ReadOnlyPublicationReader, freeze_local_sqlite, write_export,
)
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar

TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 10, 9, 7, tzinfo=TZ)
RUN = "2026-10-08-close-fixture"
SNAPSHOT = "a" * 64
SYMBOL = "000001.SZ"
DDL = """
CREATE TABLE execution_plans(plan_id TEXT PRIMARY KEY,lane_id TEXT,symbol TEXT,
 status TEXT,plan_version INTEGER,valid_from TEXT,expires_at TEXT,payload_json TEXT,
 created_at TEXT,updated_at TEXT);
CREATE TABLE workflow_runs(run_id TEXT,lane_id TEXT,trade_date TEXT,slot TEXT,
 model TEXT,status TEXT,snapshot_hash TEXT,prompt_hash TEXT,config_hash TEXT,
 reason_codes_json TEXT,created_at TEXT,updated_at TEXT,outcome_json TEXT,
 PRIMARY KEY(run_id,lane_id));
CREATE TABLE workflow_stages(run_id TEXT,lane_id TEXT,stage TEXT,status TEXT,
 reason_codes_json TEXT,updated_at TEXT,outcome_json TEXT,PRIMARY KEY(run_id,lane_id,stage));
CREATE TABLE virtual_accounts(account_id TEXT PRIMARY KEY,model TEXT,initial_cash REAL,
 cash REAL,equity REAL,status TEXT,created_at TEXT,updated_at TEXT);
CREATE TABLE virtual_positions(account_id TEXT,symbol TEXT,total_qty INTEGER,sellable_qty INTEGER,
 avg_cost REAL,stop_level REAL,plan_id TEXT,updated_at TEXT,PRIMARY KEY(account_id,symbol));
"""


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save(path, body):
    path.write_text(json.dumps(body, ensure_ascii=False), encoding="utf-8")
    return FilePin(path, sha(path))


def fixture(tmp_path, *, empty=False, summary_target=None, status="PENDING_MORNING_REVIEW",
            payload_expiry="2026-10-09T15:00:00+08:00", payload_target=None,
            run_status="PUBLISHED", stage_status="VALIDATED", position_qty=0):
    path = tmp_path / "source.sqlite3"
    conn = sqlite3.connect(path)
    conn.executescript(DDL)
    conn.execute("INSERT INTO workflow_runs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                 (RUN, "lane_1", "2026-10-08", "CLOSE", "fixture", run_status, SNAPSHOT,
                  "b" * 64, "c" * 64, "[]", "2026-10-08T16:00:00+08:00",
                  "2026-10-08T16:01:00+08:00", "{}"))
    conn.execute("INSERT INTO workflow_stages VALUES(?,?,?,?,?,?,?)",
                 (RUN, "lane_1", "A3", stage_status, "[]", "2026-10-08T16:00:00+08:00", "{}"))
    conn.execute("INSERT INTO virtual_accounts VALUES(?,?,?,?,?,?,?,?)",
                 ("paper:lane_1", "fixture", 1, 1, 1, "ACTIVE", "2026-10-08T00:00:00+08:00",
                  "2026-10-08T00:00:00+08:00"))
    conn.execute("INSERT INTO virtual_positions VALUES(?,?,?,?,?,?,?,?)",
                 ("paper:lane_1", "600000.SH", position_qty, position_qty, 10, None, None,
                  "2026-10-09T06:00:00+08:00"))
    raw = {"plan_id": "logical", "symbol": SYMBOL, "plan_expiry": payload_expiry,
           "strategy_profile": "MA520_SWING", "eligibility": "QUALIFIED",
           "trigger_zone": {"low": "10.0", "high": 11}, "invalidation_level": 9,
           "no_chase_price": 12, "risk_unit": "NORMAL"}
    if payload_target is not None:
        raw["target_trade_date"] = payload_target
    pid = f"{RUN}:lane_1:logical"
    if not empty:
        conn.execute("INSERT INTO execution_plans VALUES(?,?,?,?,?,?,?,?,?,?)",
                     (pid, "lane_1", SYMBOL, status, 1, None, "2026-10-09T15:00:00+08:00",
                      json.dumps({**raw, "source_run_id": RUN, "trigger_low": 10.0, "trigger_high": 11.0,
                                  "stop_level": 9.0, "no_chase": 12.0, "confirmation_bars": 1,
                                  "action": "BUY_SIGNAL"}), "2026-10-08T16:00:00+08:00",
                      "2026-10-08T16:00:00+08:00"))
    conn.commit()
    conn.close()
    summary = {"run_id": RUN, "slot": "close", "status": "READY", "run_role": "primary",
               "source_as_of": "2026-10-08T16:00:00+08:00", "market_trade_date": "2026-10-08",
               "target_trade_date": summary_target, "primary_lane_ids": ["lane_1"],
               "snapshot": {"snapshot_id": "fixture-snapshot", "snapshot_hash": SNAPSHOT},
               "plan_publication": {"atomic": True, "created": [] if empty else [pid],
                                    "activated": [], "blocked": [], "primary_lane": "lane_1",
                                    "publication_mode": "CLOSE"}}
    final = {"core_watch_pool": [] if empty else [raw], "secondary_watch_pool": []}
    audit = {"lane": "lane_1", "status": "READY", "final_output": final,
             "stages": [{"lane": "lane_1", "stage": "A3", "status": "VALIDATED",
                         "snapshot_id": "fixture-snapshot", "output": final,
                         "output_hash": hashlib.sha256(json.dumps(final, ensure_ascii=False,
                             sort_keys=True, separators=(",", ":")).encode()).hexdigest()}]}
    ref = PublicationReference(RUN, "lane_1", save(tmp_path / "summary.json", summary),
                               save(tmp_path / "audit.json", audit))
    db = FrozenDatabase(path, sha(path), NOW)
    return db, ref


def export(db, ref, **kwargs):
    return ReadOnlyPublicationReader(db).export(publications=(ref,), target_trade_date=date(2026, 10, 9),
        account_ids=("paper:lane_1",), calendar=ExchangeTradingCalendar(), **kwargs)


def modify(db, sql, args=()):
    conn = sqlite3.connect(db.path)
    conn.execute(sql, args)
    conn.commit()
    conn.close()
    return FrozenDatabase(db.path, sha(db.path), db.observed_at)


def alter_reference(ref, field, value):
    body = json.loads(ref.summary.path.read_text(encoding="utf-8"))
    body[field] = value
    return PublicationReference(ref.run_id, ref.lane_id, save(ref.summary.path, body), ref.audit)


def test_verified_local_pending_and_non_a1_positive_position(tmp_path):
    db, ref = fixture(tmp_path, position_qty=100)
    before = sha(db.path)
    result = export(db, ref)
    assert result["scope_complete"] is True
    assert result["morning_symbols"] == [SYMBOL, "600000.SH"]
    assert result["plans"][0]["target_binding"] == "TARGET_BOUND_BY_SERVER_EXPIRY"
    assert result["eligibility_released"] is False
    assert result["implementation_status"] == "IMPLEMENTATION_PARTIAL"
    assert result["historical_pending_state_proven"] is False
    assert result["source_unchanged"] is True
    assert result["source_sha256_before"] == result["source_sha256_after"] == before
    assert len(result["rowset_sha256"]) == 64
    assert not db.path.with_name(db.path.name + "-wal").exists()


@pytest.mark.parametrize("status", ["EXPIRED", "INVALIDATED", "ACTIVE_TODAY"])
def test_final_status_is_not_rewound_to_pending(tmp_path, status):
    db, ref = fixture(tmp_path, status=status, position_qty=10)
    result = export(db, ref)
    assert result["scope_complete"] is False
    assert result["plans"] == []
    assert result["morning_symbols"] == ["600000.SH"]
    assert "PLAN_NOT_PENDING" in result["reason_codes"]


@pytest.mark.parametrize("payload_expiry", ["2026-09-30T15:00:00+08:00", "invalid",
                                          "2026-10-09T15:00:00"])
def test_payload_expiry_conflicts_block(tmp_path, payload_expiry):
    db, ref = fixture(tmp_path, payload_expiry=payload_expiry)
    assert export(db, ref)["scope_complete"] is False


def test_absent_payload_expiry_is_not_filled_or_guessed(tmp_path):
    db, ref = fixture(tmp_path, payload_expiry=None)
    result = export(db, ref)
    assert result["scope_complete"] is True
    assert result["plans"][0]["target_binding"] == "TARGET_BOUND_BY_SERVER_EXPIRY"
    conn = sqlite3.connect(db.path)
    try:
        assert json.loads(conn.execute("SELECT payload_json FROM execution_plans").fetchone()[0])["plan_expiry"] is None
    finally:
        conn.close()


@pytest.mark.parametrize("field,value", [("target_trade_date", "2026-10-12"), ("run_id", "other"),
    ("status", "FAILED"), ("slot", "morning"), ("market_trade_date", "2026-09-30"),
    ("run_role", "comparison")])
def test_summary_identity_and_publication_boundary(tmp_path, field, value):
    db, ref = fixture(tmp_path)
    result = export(db, alter_reference(ref, field, value))
    assert result["scope_complete"] is False
    assert result["plans"] == []


@pytest.mark.parametrize("run_status,stage_status", [("READY_TO_PUBLISH", "VALIDATED"),
                                                  ("PUBLISHED", "NOT_RUN_UPSTREAM_BLOCKED")])
def test_database_publication_and_a3_are_not_certificates(tmp_path, run_status, stage_status):
    db, ref = fixture(tmp_path, run_status=run_status, stage_status=stage_status)
    assert export(db, ref)["scope_complete"] is False


def test_empty_requires_explicit_successful_target_publication(tmp_path):
    db, ref = fixture(tmp_path, empty=True)
    result = export(db, ref)
    assert "EMPTY_TARGET_UNPROVEN" in result["reason_codes"]
    ref = alter_reference(ref, "target_trade_date", "2026-10-09")
    result = export(db, ref)
    assert result["scope_complete"] is True
    assert result["morning_symbols"] == []
    assert result["empty_domain_proven"] is True
    assert result["eligibility_released"] is False


def test_empty_without_publication_is_unproven(tmp_path):
    db, _ = fixture(tmp_path, empty=True, summary_target="2026-10-09")
    result = ReadOnlyPublicationReader(db).export(publications=(), target_trade_date=date(2026, 10, 9),
        account_ids=("paper:lane_1",), calendar=ExchangeTradingCalendar())
    assert result["scope_complete"] is False
    assert "PUBLICATION_REFERENCE_MISSING" in result["reason_codes"]


@pytest.mark.parametrize("sql", ["DELETE FROM virtual_accounts", "DROP TABLE virtual_positions"])
def test_missing_account_or_position_table_never_means_zero(tmp_path, sql):
    db, ref = fixture(tmp_path)
    db = modify(db, sql)
    if sql.startswith("DROP"):
        with pytest.raises(ExportError, match="SCHEMA_MISSING"):
            export(db, ref)
    else:
        result = export(db, ref)
        assert "ACCOUNT_SET_UNPROVEN" in result["reason_codes"]
        assert result["scope_complete"] is False


@pytest.mark.parametrize("sql,args", [
    ("UPDATE execution_plans SET expires_at=?", ("2026-10-08T15:00:00+08:00",)),
    ("UPDATE workflow_runs SET snapshot_hash=?", ("d" * 64,)),
    ("UPDATE execution_plans SET symbol=?", ("600000.SH",)),
    ("UPDATE execution_plans SET payload_json=?", ("not-json",)),
    ("UPDATE virtual_positions SET total_qty=?", (-1,)),
    ("UPDATE virtual_positions SET updated_at=?", ("2026-10-10T00:00:00+08:00",)),
])
def test_real_row_identity_time_and_quantity_counterexamples(tmp_path, sql, args):
    db, ref = fixture(tmp_path)
    assert export(modify(db, sql, args), ref)["scope_complete"] is False


def test_external_pin_and_reference_pin_refuse_tamper(tmp_path):
    db, ref = fixture(tmp_path)
    with pytest.raises(ExportError, match="DB_SHA_MISMATCH"):
        export(FrozenDatabase(db.path, "0" * 64, NOW), ref)
    ref.summary.path.write_text("{}", encoding="utf-8")
    with pytest.raises(ExportError, match="REFERENCE_SHA_MISMATCH"):
        export(db, ref)


@pytest.mark.parametrize("sidecar", ["-wal", "-shm", "-journal"])
def test_immutable_input_refuses_active_sidecars(tmp_path, sidecar):
    db, ref = fixture(tmp_path)
    db.path.with_name(db.path.name + sidecar).write_bytes(b"fixture")
    with pytest.raises(ExportError, match="SQLITE_SIDECAR_PRESENT"):
        export(db, ref)


def test_actual_db_read_is_readonly(tmp_path):
    db, _ = fixture(tmp_path)
    reader = ReadOnlyPublicationReader(db)
    with reader.connection() as conn:
        with pytest.raises(sqlite3.OperationalError, match="readonly|authorized"):
            conn.execute("DELETE FROM execution_plans")
    assert sha(db.path) == db.sha256


def test_hash_after_read_detects_source_mutation(tmp_path, monkeypatch):
    db, ref = fixture(tmp_path)
    import liangjian_funnel.runtime.auction_publication_export as module
    original = module.file_sha256
    calls = 0
    def changed(path):
        nonlocal calls
        value = original(path)
        if Path(path) == db.path:
            calls += 1
            if calls >= 2:
                return "f" * 64
        return value
    monkeypatch.setattr(module, "file_sha256", changed)
    with pytest.raises(ExportError, match="DB_CHANGED"):
        export(db, ref)


def test_consistent_local_backup_is_exclusive_and_pinned(tmp_path):
    db, ref = fixture(tmp_path)
    target = tmp_path / "frozen.sqlite3"
    frozen = freeze_local_sqlite(source=db.path, expected_sha256=db.sha256,
                                observed_at=NOW, output=target)
    assert frozen.path == target.resolve()
    assert frozen.sha256 == sha(target)
    assert export(frozen, ref)["scope_complete"] is True
    assert sha(db.path) == db.sha256
    with pytest.raises(ExportError, match="REFUSE_OVERWRITE"):
        freeze_local_sqlite(source=db.path, expected_sha256=db.sha256, observed_at=NOW, output=target)


def test_output_refuses_overwrite_and_has_receipt_hash(tmp_path):
    db, ref = fixture(tmp_path)
    result = export(db, ref)
    output = tmp_path / "export.json"
    assert write_export(output, result) == sha(output)
    with pytest.raises(ExportError, match="REFUSE_OVERWRITE"):
        write_export(output, result)


def test_historical_observation_never_becomes_current_scope(tmp_path):
    db, ref = fixture(tmp_path)
    result = export(FrozenDatabase(db.path, db.sha256, datetime(2026, 10, 10, 7, tzinfo=TZ)), ref)
    assert result["scope_complete"] is False
    assert "OBSERVATION_TARGET_MISMATCH" in result["reason_codes"]
    assert result["historical_pending_state_proven"] is False


def test_explicit_payload_target_must_match_even_with_good_server_expiry(tmp_path):
    db, ref = fixture(tmp_path, payload_target="2026-10-12")
    assert "PLAN_PAYLOAD_TARGET_CONFLICT" in export(db, ref)["reason_codes"]


def test_validated_no_setup_empty_is_success_not_missing_a3(tmp_path):
    db, ref = fixture(tmp_path, empty=True, summary_target="2026-10-09", stage_status="VALIDATED_NO_SETUP")
    audit = json.loads(ref.audit.path.read_bytes())
    audit["stages"][0]["status"] = "VALIDATED_NO_SETUP"
    ref = PublicationReference(RUN, "lane_1", ref.summary, save(ref.audit.path, audit))
    assert export(db, ref)["empty_domain_proven"] is True


def test_audit_no_setup_and_database_validated_mismatch_is_gap(tmp_path):
    db, ref = fixture(tmp_path, empty=True, summary_target="2026-10-09")
    audit = json.loads(ref.audit.path.read_bytes())
    audit["stages"][0]["status"] = "VALIDATED_NO_SETUP"
    ref = PublicationReference(RUN, "lane_1", ref.summary, save(ref.audit.path, audit))
    assert "AUDIT_A3_INVALID" in export(db, ref)["reason_codes"]


def test_partial_invalid_publication_does_not_release_partial_plan_scope(tmp_path):
    db, ref = fixture(tmp_path, position_qty=10)
    conn = sqlite3.connect(db.path)
    row = list(conn.execute("SELECT * FROM execution_plans").fetchone())
    row[0] = f"{RUN}:lane_1:second"
    row[2], row[3] = "600001.SH", "EXPIRED"
    payload = json.loads(row[7])
    payload.update(plan_id="second", symbol="600001.SH")
    row[7] = json.dumps(payload)
    conn.execute("INSERT INTO execution_plans VALUES(?,?,?,?,?,?,?,?,?,?)", row)
    conn.commit()
    conn.close()
    db = FrozenDatabase(db.path, sha(db.path), NOW)
    summary = json.loads(ref.summary.path.read_bytes())
    summary["plan_publication"]["created"].append(row[0])
    audit = json.loads(ref.audit.path.read_bytes())
    raw = {key: value for key, value in payload.items() if key not in {
        "source_run_id", "trigger_low", "trigger_high", "stop_level", "no_chase", "confirmation_bars", "action"}}
    audit["final_output"]["core_watch_pool"].append(raw)
    audit["stages"][0]["output"] = audit["final_output"]
    audit["stages"][0]["output_hash"] = hashlib.sha256(json.dumps(audit["final_output"],
        ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    ref = PublicationReference(RUN, "lane_1", save(ref.summary.path, summary), save(ref.audit.path, audit))
    result = export(db, ref)
    assert result["plans"] == []
    assert result["morning_symbols"] == ["600000.SH"]
    assert result["scope_complete"] is False


@pytest.mark.parametrize("sql,args", [
    ("UPDATE execution_plans SET payload_json=?", (json.dumps({"source_run_id": "other", "symbol": SYMBOL}),)),
    ("DELETE FROM execution_plans", ()),
])
def test_declared_publication_ids_are_reconciled_not_ignored(tmp_path, sql, args):
    db, ref = fixture(tmp_path)
    assert "PUBLICATION_PLAN_SET_MISMATCH" in export(modify(db, sql, args), ref)["reason_codes"]


def test_audit_output_hash_is_recomputed(tmp_path):
    db, ref = fixture(tmp_path)
    audit = json.loads(ref.audit.path.read_bytes())
    audit["final_output"]["core_watch_pool"][0]["symbol"] = "600001.SH"
    ref = PublicationReference(RUN, "lane_1", ref.summary, save(ref.audit.path, audit))
    assert "AUDIT_OUTPUT_HASH_MISMATCH" in export(db, ref)["reason_codes"]


def test_empty_success_missing_account_set_is_not_empty_domain(tmp_path):
    db, ref = fixture(tmp_path, empty=True, summary_target="2026-10-09")
    with pytest.raises(ExportError, match="ACCOUNT_SET_INVALID"):
        ReadOnlyPublicationReader(db).export(publications=(ref,), target_trade_date=date(2026, 10, 9),
            account_ids=(), calendar=ExchangeTradingCalendar())


def test_positive_position_is_risk_scope_not_account_status_permission(tmp_path):
    db, ref = fixture(tmp_path, position_qty=100)
    result = export(modify(db, "UPDATE virtual_accounts SET status='SUSPENDED'"), ref)
    assert result["positive_positions"][0]["symbol"] == "600000.SH"
    assert result["accounts"][0]["status"] == "SUSPENDED"
    assert result["eligibility_released"] is False


@pytest.mark.parametrize("field,value", [("plan_publication", []), ("primary_lane_ids", None),
                                         ("status", [])])
def test_malformed_summary_never_crashes_or_becomes_empty_success(tmp_path, field, value):
    db, ref = fixture(tmp_path)
    assert export(db, alter_reference(ref, field, value))["scope_complete"] is False


@pytest.mark.parametrize("suffix", ["24639360c697", "abcdef123456", "000000000000"])
def test_known_or_arbitrary_overlay_id_is_not_hash_chain_proof(tmp_path, suffix):
    db, ref = fixture(tmp_path)
    audit = json.loads(ref.audit.path.read_bytes())
    audit["stages"].insert(0, {"stage": "A1", "snapshot_id": "fixture-snapshot"})
    audit["stages"][-1]["snapshot_id"] = "fixture-snapshot:a2:" + suffix + ":a3:bf8ab6274680"
    ref = PublicationReference(RUN, "lane_1", ref.summary, save(ref.audit.path, audit))
    result = export(db, ref)
    assert "AUDIT_OVERLAY_BINDING_UNPROVEN" in result["reason_codes"]
    assert result["scope_complete"] is False
    assert result["publication_evidence"][0]["snapshot_binding"] == "BASE_ID_PLUS_PINNED_AUDIT_OVERLAY_DECLARATION"
    assert result["plans"] == []


def test_server_expiry_before_1500_is_not_published_close_horizon(tmp_path):
    db, ref = fixture(tmp_path, payload_expiry="2026-10-09T08:00:00+08:00")
    db = modify(db, "UPDATE execution_plans SET expires_at=?", ("2026-10-09T08:00:00+08:00",))
    assert "PLAN_EXPIRY_TARGET_CONFLICT" in export(db, ref)["reason_codes"]


def test_calendar_holiday_target_is_rejected(tmp_path):
    db, ref = fixture(tmp_path)
    with pytest.raises(ExportError, match="TARGET_NOT_TRADING_DAY"):
        ReadOnlyPublicationReader(db).export(publications=(ref,), target_trade_date=date(2026, 10, 1),
            account_ids=("paper:lane_1",), calendar=ExchangeTradingCalendar())


def test_missing_original_summary_is_explicit_not_manual_certificate(tmp_path):
    db, ref = fixture(tmp_path)
    ref = PublicationReference(RUN, "lane_1", FilePin(tmp_path / "missing.json", "a" * 64), ref.audit)
    with pytest.raises(ExportError, match="REFERENCE_MISSING"):
        export(db, ref)


def test_reference_json_duplicate_key_is_rejected(tmp_path):
    db, ref = fixture(tmp_path)
    ref.summary.path.write_bytes(b'{"run_id":"old","run_id":"new"}')
    ref = PublicationReference(RUN, "lane_1", FilePin(ref.summary.path, sha(ref.summary.path)), ref.audit)
    with pytest.raises(ExportError, match="JSON_DUPLICATE_KEY"):
        export(db, ref)


def test_source_table_view_or_missing_actual_column_is_rejected(tmp_path):
    db, ref = fixture(tmp_path)
    conn = sqlite3.connect(db.path)
    conn.executescript("ALTER TABLE virtual_positions RENAME TO old_positions; CREATE VIEW virtual_positions AS SELECT * FROM old_positions;")
    conn.close()
    with pytest.raises(ExportError, match="SCHEMA_MISSING"):
        export(FrozenDatabase(db.path, sha(db.path), NOW), ref)


def test_tampered_export_receipt_is_not_archived(tmp_path):
    db, ref = fixture(tmp_path)
    result = export(db, ref)
    result["morning_symbols"] = ["600001.SH"]
    with pytest.raises(ExportError, match="EXPORT_RECEIPT_HASH_MISMATCH"):
        write_export(tmp_path / "tampered.json", result)
    assert not (tmp_path / "tampered.json").exists()


def test_no_input_credentials_or_model_payload_in_export(tmp_path):
    db, ref = fixture(tmp_path)
    db = modify(db, "UPDATE workflow_runs SET model=?,outcome_json=?",
                ("arbitrary_model_text_not_a_consumer_dto", '{"secret":"fixture-secret"}'))
    result = export(db, ref)
    encoded = json.dumps(result)
    assert "fixture-secret" not in encoded
    assert "arbitrary_model_text_not_a_consumer_dto" not in encoded


def test_no_network_store_settings_or_workflow_imports():
    import ast
    module = Path(__file__).resolve().parents[1] / "src/liangjian_funnel/runtime/auction_publication_export.py"
    tree = ast.parse(module.read_text(encoding="utf-8"))
    imported = [n.module or "" for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)]
    imported += [alias.name for n in ast.walk(tree) if isinstance(n, ast.Import) for alias in n.names]
    assert not any(re.search(r"(settings|workflow|runtime\.state|httpx|requests|provider)", name) for name in imported)


@pytest.mark.parametrize("key,value", [("stop_level", 8), ("trigger_low", 10.5),
                                     ("trigger_zone", {"low": 11, "high": 12}),
                                     ("strategy_profile", "TREND_MA5")])
def test_current_db_byte_pin_cannot_hide_changed_strategy_plan(tmp_path, key, value):
    db, ref = fixture(tmp_path)
    conn = sqlite3.connect(db.path)
    payload = json.loads(conn.execute("SELECT payload_json FROM execution_plans").fetchone()[0])
    conn.close()
    payload[key] = value
    db = modify(db, "UPDATE execution_plans SET payload_json=?", (json.dumps(payload),))
    result = export(db, ref)
    assert result["scope_complete"] is False
    assert "PLAN_AUDIT_PAYLOAD_CONFLICT" in result["reason_codes"]


def test_permission_normalization_without_original_authority_is_unproven(tmp_path):
    db, ref = fixture(tmp_path)
    conn = sqlite3.connect(db.path)
    payload = json.loads(conn.execute("SELECT payload_json FROM execution_plans").fetchone()[0])
    conn.close()
    payload.update(execution_permission="ALLOW_A4", research_only_reason=None,
                   a2_execution_permission="REQUIRES_A3_A4_CONFIRMATION", a2_research_only_reason=None)
    db = modify(db, "UPDATE execution_plans SET payload_json=?", (json.dumps(payload),))
    result = export(db, ref)
    assert result["scope_complete"] is False
    assert "PLAN_AUDIT_PAYLOAD_BINDING_UNPROVEN" in result["reason_codes"]


@pytest.mark.parametrize("expiry", ["2026-10-09T15:30:00+08:00", "2026-10-09T16:00:00+08:00"])
def test_later_expiry_is_outside_supported_default_horizon(tmp_path, expiry):
    db, ref = fixture(tmp_path, payload_expiry=expiry)
    db = modify(db, "UPDATE execution_plans SET expires_at=?", (expiry,))
    assert export(db, ref)["scope_complete"] is False


def test_older_valid_empty_cannot_hide_newer_published_close(tmp_path):
    db, ref = fixture(tmp_path, empty=True, summary_target="2026-10-09")
    conn = sqlite3.connect(db.path)
    row = list(conn.execute("SELECT * FROM workflow_runs").fetchone())
    row[0], row[10], row[11] = "newer-close", "2026-10-08T17:00:00+08:00", "2026-10-08T17:01:00+08:00"
    conn.execute("INSERT INTO workflow_runs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", row)
    conn.commit()
    conn.close()
    result = export(FrozenDatabase(db.path, sha(db.path), NOW), ref)
    assert result["scope_complete"] is False
    assert "PUBLICATION_SUPERSEDED" in result["reason_codes"]


def test_same_lane_target_pending_with_other_source_is_not_silent_empty(tmp_path):
    db, ref = fixture(tmp_path, empty=True, summary_target="2026-10-09")
    conn = sqlite3.connect(db.path)
    conn.execute("INSERT INTO execution_plans VALUES(?,?,?,?,?,?,?,?,?,?)",
        ("other-plan", "lane_1", "600000.SH", "PENDING_MORNING_REVIEW", 1, None,
         "2026-10-09T15:00:00+08:00", json.dumps({"source_run_id": "other-run"}),
         "2026-10-08T16:00:00+08:00", "2026-10-08T16:00:00+08:00"))
    conn.commit()
    conn.close()
    result = export(FrozenDatabase(db.path, sha(db.path), NOW), ref)
    assert result["scope_complete"] is False
    assert "OTHER_PENDING_PUBLICATION_PRESENT" in result["reason_codes"]


def test_reference_changed_after_materialization_is_not_sealed(tmp_path, monkeypatch):
    db, ref = fixture(tmp_path)
    import liangjian_funnel.runtime.auction_publication_export as module
    original = module.file_sha256
    calls = 0
    def changed(path):
        nonlocal calls
        value = original(path)
        if Path(path) == ref.summary.path:
            calls += 1
            if calls > 1:
                return "f" * 64
        return value
    monkeypatch.setattr(module, "file_sha256", changed)
    with pytest.raises(ExportError, match="REFERENCE_CHANGED"):
        export(db, ref)


@pytest.mark.parametrize("index,value", [(1, "lane_2"), (2, "2026-09-30"),
                                       (3, "MORNING"), (10, "2026-10-10T00:00:00+08:00")])
def test_superseded_check_uses_only_original_lane_day_close_and_observation(tmp_path, index, value):
    db, ref = fixture(tmp_path, empty=True, summary_target="2026-10-09")
    conn = sqlite3.connect(db.path)
    row = list(conn.execute("SELECT * FROM workflow_runs").fetchone())
    row[0], row[10], row[11] = "other-run", "2026-10-08T17:00:00+08:00", "2026-10-08T17:01:00+08:00"
    row[index] = value
    conn.execute("INSERT INTO workflow_runs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", row)
    conn.commit()
    conn.close()
    assert export(FrozenDatabase(db.path, sha(db.path), NOW), ref)["scope_complete"] is True
