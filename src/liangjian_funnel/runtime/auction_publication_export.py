"""Pinned, offline publication/position declarations; never eligibility or history.

No application/store construction, provider calls, scheduler or production wiring.
Only an explicit cold local SQLite input is accepted. Byte pins bind evidence;
they do not authenticate the acquisition timestamp or restore past row states.
"""
from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time
import hashlib
from importlib.metadata import PackageNotFoundError, version
import json
import math
from pathlib import Path
import re
import sqlite3
from typing import Any
from zoneinfo import ZoneInfo

from .calendar import ExchangeTradingCalendar

SHANGHAI = ZoneInfo("Asia/Shanghai")
SCHEMA_VERSION = "wp5-auction-publication-export/1"
# Matches pipeline/research/common.py _PUBLISHABLE_STAGE_STATUSES; no completed
# but unpublished/degraded stage is accepted by this offline reader.
PUBLISHABLE_A3_STATUSES = frozenset({"VALIDATED", "VALIDATED_NO_SETUP"})
REQUIRED_COLUMNS = {
    "execution_plans": {"plan_id", "lane_id", "symbol", "status", "plan_version", "valid_from",
                        "expires_at", "payload_json", "created_at", "updated_at"},
    "workflow_runs": {"run_id", "lane_id", "trade_date", "slot", "status", "snapshot_hash",
                      "created_at", "updated_at"},
    "workflow_stages": {"run_id", "lane_id", "stage", "status", "updated_at"},
    "virtual_accounts": {"account_id", "status", "created_at", "updated_at"},
    "virtual_positions": {"account_id", "symbol", "total_qty", "sellable_qty", "updated_at"},
}


class ExportError(ValueError):
    def __init__(self, reason_code: str):
        self.reason_code = reason_code
        super().__init__(reason_code)


def _hash(body: Any) -> str:
    return hashlib.sha256(json.dumps(body, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()


def file_sha256(path: Path) -> str:
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def _pin(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ExportError("SHA256_INVALID")
    return value


def _time(value: Any) -> datetime:
    try:
        result = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00"))
        if result.tzinfo is None or result.utcoffset() is None:
            raise ValueError()
        return result.astimezone(SHANGHAI)
    except (AttributeError, TypeError, ValueError) as exc:
        raise ExportError("TIMESTAMP_INVALID") from exc


def _day(value: Any) -> date:
    try:
        if isinstance(value, datetime):
            raise ValueError()
        result = value if isinstance(value, date) else date.fromisoformat(value)
        return result
    except (TypeError, ValueError) as exc:
        raise ExportError("DATE_INVALID") from exc


def _symbol(value: Any) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{6}\.(SH|SZ|BJ)", value):
        raise ExportError("SYMBOL_INVALID")
    return value


def _identifier(value: Any) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,1024}", value):
        raise ExportError("SOURCE_IDENTIFIER_INVALID")
    return value


def _calendar_version() -> str:
    try:
        return version("exchange-calendars")
    except PackageNotFoundError:
        return "UNAVAILABLE"


def _json(value: str | bytes) -> dict[str, Any]:
    def pairs(items):
        output = {}
        for key, entry in items:
            if key in output:
                raise ExportError("JSON_DUPLICATE_KEY")
            output[key] = entry
        return output
    try:
        result = json.loads(value, object_pairs_hook=pairs,
                            parse_constant=lambda _: (_ for _ in ()).throw(ExportError("JSON_NONFINITE")))
        if not isinstance(result, dict):
            raise ExportError("JSON_OBJECT_REQUIRED")
        return result
    except (UnicodeError, json.JSONDecodeError, TypeError) as exc:
        raise ExportError("JSON_INVALID") from exc


def _cold(path: Path) -> None:
    if not path.is_file():
        raise ExportError("SQLITE_INPUT_MISSING")
    if any(path.with_name(path.name + suffix).exists() for suffix in ("-wal", "-shm", "-journal")):
        raise ExportError("SQLITE_SIDECAR_PRESENT")


@dataclass(frozen=True)
class FilePin:
    path: Path
    sha256: str

    def read(self) -> dict[str, Any]:
        _pin(self.sha256)
        try:
            path = Path(self.path).resolve(strict=True)
        except FileNotFoundError as exc:
            raise ExportError("REFERENCE_MISSING") from exc
        body = path.read_bytes()
        if hashlib.sha256(body).hexdigest() != self.sha256 or file_sha256(path) != self.sha256:
            raise ExportError("REFERENCE_SHA_MISMATCH")
        return _json(body)


@dataclass(frozen=True)
class FrozenDatabase:
    path: Path
    sha256: str
    observed_at: datetime


@dataclass(frozen=True)
class PublicationReference:
    run_id: str
    lane_id: str
    summary: FilePin
    audit: FilePin


def freeze_local_sqlite(*, source: Path, expected_sha256: str, observed_at: datetime,
                        output: Path) -> FrozenDatabase:
    """SQLite backup from a pinned cold local file, exclusive destination.

    This is not a live-DB backup API: active sidecars are rejected. Neither the
    original state nor the supplied observation time is promoted to history.
    """
    source, output = Path(source).resolve(strict=True), Path(output).resolve()
    _pin(expected_sha256)
    observed_at = _time(observed_at)
    _cold(source)
    if file_sha256(source) != expected_sha256:
        raise ExportError("DB_SHA_MISMATCH")
    try:
        with output.open("xb"):
            pass
    except FileExistsError as exc:
        raise ExportError("REFUSE_OVERWRITE") from exc
    try:
        with closing(sqlite3.connect(source.as_uri() + "?mode=ro&immutable=1", uri=True)) as original:
            original.execute("PRAGMA query_only=ON")
            original.execute("BEGIN")
            if original.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                raise ExportError("SQLITE_INTEGRITY_FAILED")
            with closing(sqlite3.connect(output)) as copied:
                original.backup(copied)
                if copied.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                    raise ExportError("SQLITE_INTEGRITY_FAILED")
        _cold(source)
        if file_sha256(source) != expected_sha256:
            raise ExportError("DB_CHANGED")
        _cold(output)
        return FrozenDatabase(output, file_sha256(output), observed_at)
    except Exception:
        # Only our exclusively-created file, never the source/previous output.
        output.unlink(missing_ok=True)
        raise


class ReadOnlyPublicationReader:
    def __init__(self, database: FrozenDatabase):
        self.database = FrozenDatabase(Path(database.path).resolve(strict=True),
                                       _pin(database.sha256), _time(database.observed_at))

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        path = self.database.path
        _cold(path)
        if file_sha256(path) != self.database.sha256:
            raise ExportError("DB_SHA_MISMATCH")
        connection = sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA query_only=ON")
            connection.execute("PRAGMA trusted_schema=OFF")
            connection.execute("BEGIN")
            if [tuple(row) for row in connection.execute("PRAGMA quick_check")] != [("ok",)]:
                raise ExportError("SQLITE_INTEGRITY_FAILED")
            yield connection
        finally:
            connection.close()
            _cold(path)
            if file_sha256(path) != self.database.sha256:
                raise ExportError("DB_CHANGED")

    def _schema(self, connection: sqlite3.Connection) -> dict[str, Any]:
        schema = {}
        for table, required in REQUIRED_COLUMNS.items():
            kind = connection.execute("SELECT type FROM sqlite_master WHERE name=?", (table,)).fetchone()
            rows = [dict(row) for row in connection.execute(f"PRAGMA table_info({table})")]
            if not kind or kind[0] != "table" or not required.issubset({row["name"] for row in rows}):
                raise ExportError("SCHEMA_MISSING")
            schema[table] = rows
        return schema

    def export(self, *, publications: Sequence[PublicationReference], target_trade_date: date,
               account_ids: Sequence[str], calendar: ExchangeTradingCalendar) -> dict[str, Any]:
        """Export observations, not A3/A4 permission; gaps keep risk positions."""
        target = _day(target_trade_date)
        observed = self.database.observed_at
        reasons: set[str] = set()
        publications = tuple(publications)
        accounts = tuple(account_ids)
        if (not accounts or any(not isinstance(item, str) or not item.strip() for item in accounts)
                or len(set(accounts)) != len(accounts)):
            raise ExportError("ACCOUNT_SET_INVALID")
        if not calendar.is_trading_day(target):
            raise ExportError("TARGET_NOT_TRADING_DAY")
        previous = calendar.previous_trading_day(target)
        if calendar.next_trading_day(previous) != target:
            raise ExportError("CALENDAR_SESSION_CONFLICT")
        if observed.date() != target:
            reasons.add("OBSERVATION_TARGET_MISMATCH")
        if not publications:
            reasons.add("PUBLICATION_REFERENCE_MISSING")
        keys = [(ref.run_id, ref.lane_id) for ref in publications]
        for run_id, lane_id in keys:
            _identifier(run_id)
            _identifier(lane_id)
        if len(set(keys)) != len(keys) or len({ref.lane_id for ref in publications}) != len(publications):
            raise ExportError("PUBLICATION_REFERENCE_DUPLICATE")
        plans, positions, account_evidence, publication_evidence, observed_plans = [], [], [], [], []
        rows_for_hash: dict[str, Any] = {"runs": [], "stages": [], "plans": [], "accounts": [],
                                      "positions": [], "lane_plan_scans": [], "publication_candidates": []}
        with self.connection() as connection:
            schema = self._schema(connection)
            for ref in publications:
                summary, audit = ref.summary.read(), ref.audit.read()
                claimed_publication = summary.get("plan_publication")
                claimed_ids = (claimed_publication.get("created", [])
                               if isinstance(claimed_publication, dict) else [])
                if not isinstance(claimed_ids, list):
                    claimed_ids = []
                run = connection.execute("SELECT * FROM workflow_runs WHERE run_id=? AND lane_id=?",
                                         (ref.run_id, ref.lane_id)).fetchone()
                stage = connection.execute("SELECT * FROM workflow_stages WHERE run_id=? AND lane_id=? AND stage='A3'",
                                           (ref.run_id, ref.lane_id)).fetchone()
                source_rows = [dict(row) for row in connection.execute(
                    "SELECT * FROM execution_plans WHERE lane_id=? ORDER BY plan_id", (ref.lane_id,))]
                candidates = [dict(row) for row in connection.execute(
                    "SELECT run_id,lane_id,trade_date,slot,status,created_at FROM workflow_runs "
                    "WHERE lane_id=? AND slot='CLOSE' AND trade_date=? AND status='PUBLISHED' ORDER BY run_id",
                    (ref.lane_id, previous.isoformat()))]
                rows_for_hash["publication_candidates"].append(candidates)
                rows_for_hash["lane_plan_scans"].append(source_rows)
                linked = []
                lane_gaps: set[str] = set()
                for row in source_rows:
                    try:
                        payload = _json(row["payload_json"])
                    except ExportError:
                        # A malformed claimed publication ID must not disappear.
                        if row["plan_id"] in claimed_ids:
                            linked.append(row)
                        elif row["status"] == "PENDING_MORNING_REVIEW":
                            lane_gaps.add("UNATTRIBUTED_PENDING_PLAN")
                        continue
                    if payload.get("source_run_id") == ref.run_id:
                        linked.append(row)
                    elif row["status"] == "PENDING_MORNING_REVIEW":
                        try:
                            if _time(row["expires_at"]).date() == target:
                                lane_gaps.add("OTHER_PENDING_PUBLICATION_PRESENT")
                        except ExportError:
                            lane_gaps.add("UNATTRIBUTED_PENDING_PLAN")
                rows_for_hash["runs"].append(dict(run) if run else None)
                rows_for_hash["stages"].append(dict(stage) if stage else None)
                rows_for_hash["plans"].extend(linked)
                observed_plans.extend({"plan_id": row["plan_id"], "lane_id": row["lane_id"],
                    "symbol": row["symbol"], "status": row["status"], "expires_at": row["expires_at"],
                    "payload_sha256": hashlib.sha256(row["payload_json"].encode()).hexdigest()} for row in linked)
                evidence = {"run_id": ref.run_id, "lane_id": ref.lane_id,
                    "summary": {"path": str(Path(ref.summary.path).resolve()), "sha256": ref.summary.sha256},
                    "audit": {"path": str(Path(ref.audit.path).resolve()), "sha256": ref.audit.sha256},
                    "bound": False, "reason_codes": []}
                publication_evidence.append(evidence)
                local_reasons: set[str] = set(lane_gaps)
                if run:
                    try:
                        original_created = _time(run["created_at"])
                        if any(candidate["run_id"] != ref.run_id
                               and original_created < _time(candidate["created_at"]) <= observed
                               for candidate in candidates):
                            local_reasons.add("PUBLICATION_SUPERSEDED")
                    except ExportError:
                        local_reasons.add("PUBLISHED_CANDIDATE_TIME_UNPROVEN")
                try:
                    logical = self._publication(ref, summary, audit, run, stage, target, previous, observed, evidence)
                    created = summary["plan_publication"]["created"]
                    if set(created) != {row["plan_id"] for row in linked}:
                        raise ExportError("PUBLICATION_PLAN_SET_MISMATCH")
                    if not created and summary.get("target_trade_date") is None:
                        raise ExportError("EMPTY_TARGET_UNPROVEN")
                    selected = []
                    for row in linked:
                        try:
                            selected.append(self._plan(row, ref, summary, logical, target, observed))
                        except ExportError as exc:
                            local_reasons.add(exc.reason_code)
                    # A partial invalid publication cannot silently become ready.
                    if not local_reasons:
                        plans.extend(selected)
                        evidence["bound"] = True
                        evidence["empty_publication_proven"] = not created
                except ExportError as exc:
                    local_reasons.add(exc.reason_code)
                evidence["reason_codes"] = sorted(local_reasons)
                reasons.update(local_reasons)
            for account_id in sorted(accounts):
                account = connection.execute("SELECT * FROM virtual_accounts WHERE account_id=?", (account_id,)).fetchone()
                rows = [dict(row) for row in connection.execute(
                    "SELECT * FROM virtual_positions WHERE account_id=? ORDER BY symbol", (account_id,))]
                rows_for_hash["accounts"].append(dict(account) if account else None)
                rows_for_hash["positions"].extend(rows)
                if not account:
                    reasons.add("ACCOUNT_SET_UNPROVEN")
                    continue
                entry = {"account_id": account_id, "status": account["status"],
                         "updated_at": account["updated_at"], "position_rows": len(rows)}
                account_evidence.append(entry)
                try:
                    self._row_time(account, observed)
                    for row in rows:
                        self._row_time(row, observed)
                        _symbol(row["symbol"])
                        quantity, sellable = row["total_qty"], row["sellable_qty"]
                        if (isinstance(quantity, bool) or not isinstance(quantity, int) or quantity < 0
                                or not isinstance(sellable, int) or not 0 <= sellable <= quantity):
                            raise ExportError("POSITION_QUANTITY_INVALID")
                        if quantity > 0:
                            positions.append({"account_id": account_id, "symbol": row["symbol"],
                                              "total_qty": quantity, "updated_at": row["updated_at"]})
                except ExportError as exc:
                    reasons.add(exc.reason_code)
        plans.sort(key=lambda row: (row["lane_id"], row["symbol"], row["plan_id"]))
        positions.sort(key=lambda row: (row["account_id"], row["symbol"]))
        if len({(row["lane_id"], row["symbol"]) for row in plans}) != len(plans):
            reasons.add("PLAN_SYMBOL_DUPLICATE")
            plans = []
        if "OBSERVATION_TARGET_MISMATCH" in reasons:
            plans = []
        for ref in publications:
            for pin in (ref.summary, ref.audit):
                if file_sha256(Path(pin.path).resolve(strict=True)) != pin.sha256:
                    raise ExportError("REFERENCE_CHANGED")
        body = {"schema_version": SCHEMA_VERSION, "implementation_status": "IMPLEMENTATION_PARTIAL",
            "evidence_status": "DATA_LIMITED" if reasons else "LOCAL_DECLARATION_BOUND",
            "scope_complete": not reasons, "eligibility_released": False,
            "historical_pending_state_proven": False, "historical_positions_proven": False,
            "observed_at": observed.isoformat(), "target_trade_date": target.isoformat(),
            "previous_close_trade_date": previous.isoformat(),
            "source_path": str(self.database.path), "source_sha256_before": self.database.sha256,
            "source_sha256_after": self.database.sha256, "source_unchanged": True,
            "schema_sha256": _hash(schema), "rowset_sha256": _hash(rows_for_hash),
            "calendar": {"calendar_type": f"{type(calendar).__module__}.{type(calendar).__qualname__}",
                         "exchange_calendars_version": _calendar_version(),
                         "previous": previous.isoformat(), "next": calendar.next_trading_day(previous).isoformat()},
            "publication_evidence": publication_evidence, "account_ids": sorted(accounts),
            "accounts": account_evidence, "observed_plans": observed_plans,
            "plans": plans, "positive_positions": positions,
            "morning_symbols": sorted({row["symbol"] for row in [*plans, *positions]}),
            "empty_domain_proven": not reasons and not plans and not positions,
            "reason_codes": sorted(reasons),
            "limitations": ["Observed final state is not historical pending/positions evidence.",
                "Byte pins bind local files, not their acquisition provenance or observation clock.",
                "Original FIS/A1/close-scope validation and slow/fast adapters remain separate.",
                "This declaration never grants A1/A3/A4 eligibility or execution permission."]}
        body["receipt_sha256"] = _hash(body)
        return body

    @staticmethod
    def _row_time(row: Mapping[str, Any], observed: datetime) -> None:
        updated = _time(row["updated_at"])
        if updated > observed:
            raise ExportError("ROW_AFTER_OBSERVATION")
        if "created_at" in row.keys() and _time(row["created_at"]) > updated:
            raise ExportError("ROW_TIME_CONFLICT")

    def _publication(self, ref, summary, audit, run, stage, target, previous, observed, evidence):
        publication = summary.get("plan_publication")
        snapshot = summary.get("snapshot")
        if (any(not isinstance(summary.get(key), str) for key in ("run_id", "slot", "status", "run_role"))
                or not isinstance(summary.get("primary_lane_ids"), list)):
            raise ExportError("PUBLICATION_SUMMARY_INVALID")
        if (summary.get("run_id") != ref.run_id or summary.get("slot") not in {"close", "CLOSE"}
                or summary.get("status") not in {"READY", "READY_DEGRADED"}
                or summary.get("run_role") not in {"primary", "full"}
                or summary.get("auction_refresh") is True
                or ref.lane_id not in summary.get("primary_lane_ids", [])
                or not isinstance(publication, dict) or publication.get("atomic") is not True
                or publication.get("publication_mode") != "CLOSE"
                or publication.get("primary_lane") != ref.lane_id
                or not isinstance(publication.get("created"), list)
                or any(not isinstance(item, str) for item in publication["created"])
                or len(set(publication["created"])) != len(publication["created"])
                or publication.get("activated") != [] or not isinstance(snapshot, dict)):
            raise ExportError("PUBLICATION_SUMMARY_INVALID")
        if not run or run["status"] != "PUBLISHED" or run["slot"] != "CLOSE":
            raise ExportError("RUN_NOT_PUBLISHED_CLOSE")
        if not stage or stage["status"] not in PUBLISHABLE_A3_STATUSES:
            raise ExportError("A3_NOT_VALIDATED")
        self._row_time(run, observed)
        self._row_time(stage, observed)
        source_time = _time(summary.get("source_as_of"))
        if (source_time.date() != previous or source_time > observed
                or _day(run["trade_date"]) != previous
                or _day(summary.get("market_trade_date")) != previous):
            raise ExportError("PUBLICATION_DATE_MISMATCH")
        if summary.get("target_trade_date") is not None and _day(summary["target_trade_date"]) != target:
            raise ExportError("PUBLICATION_TARGET_MISMATCH")
        if snapshot.get("snapshot_hash") != _pin(run["snapshot_hash"]) or not snapshot.get("snapshot_id"):
            raise ExportError("PUBLICATION_SNAPSHOT_MISMATCH")
        _identifier(snapshot["snapshot_id"])
        if (audit.get("lane") != ref.lane_id or not isinstance(audit.get("status"), str)
                or audit["status"] not in {"READY", "READY_DEGRADED"}
                or not isinstance(audit.get("stages"), list)):
            raise ExportError("AUDIT_LANE_INVALID")
        stages = [item for item in audit.get("stages", []) if isinstance(item, dict) and item.get("stage") == "A3"]
        if (len(stages) != 1 or stages[0].get("status") != stage["status"]
                or stages[0].get("lane") != ref.lane_id):
            raise ExportError("AUDIT_A3_INVALID")
        audit_stage = stages[0]
        _identifier(audit_stage.get("snapshot_id"))
        overlay_unproven = audit_stage.get("snapshot_id") != snapshot["snapshot_id"]
        if overlay_unproven:
            bases = [item for item in audit["stages"] if isinstance(item, dict) and item.get("stage") == "A1"]
            if len(bases) != 1 or bases[0].get("snapshot_id") != snapshot["snapshot_id"]:
                raise ExportError("AUDIT_SNAPSHOT_MISMATCH")
            evidence["snapshot_binding"] = "BASE_ID_PLUS_PINNED_AUDIT_OVERLAY_DECLARATION"
            evidence["base_snapshot_id"] = snapshot["snapshot_id"]
            evidence["a3_snapshot_id"] = audit_stage.get("snapshot_id")
        else:
            evidence["snapshot_binding"] = "BASE_ID_AND_PINNED_AUDIT_DECLARATION"
        final = audit.get("final_output")
        if (not isinstance(final, dict) or not isinstance(audit_stage.get("output"), dict)
                or _hash(audit_stage["output"]) != audit_stage.get("output_hash")
                or _hash(final) != _hash(audit_stage["output"])):
            raise ExportError("AUDIT_OUTPUT_HASH_MISMATCH")
        logical = {}
        for pool in ("core_watch_pool", "secondary_watch_pool"):
            if not isinstance(final.get(pool), list):
                raise ExportError("AUDIT_PLAN_POOLS_MISSING")
            for raw in final[pool]:
                if not isinstance(raw, dict):
                    raise ExportError("AUDIT_PLAN_INVALID")
                symbol = _symbol(raw.get("symbol"))
                key = str(raw.get("plan_id") or _hash(raw)[:16])
                _identifier(key)
                plan_id = f"{ref.run_id}:{ref.lane_id}:{key}"
                if plan_id in logical:
                    raise ExportError("AUDIT_PLAN_DUPLICATE")
                logical[plan_id] = {"symbol": symbol, "raw_sha256": _hash(raw), "raw": raw}
        if not set(publication["created"]).issubset(logical):
            raise ExportError("PUBLICATION_AUDIT_PLAN_MISMATCH")
        if overlay_unproven:
            # An ID prefix or arbitrary 12hex suffix is not a derived snapshot
            # hash-chain proof. This slice never reconstructs absent overlays.
            raise ExportError("AUDIT_OVERLAY_BINDING_UNPROVEN")
        return logical

    def _plan(self, row, ref, summary, logical, target, observed):
        self._row_time(row, observed)
        _identifier(row["plan_id"])
        if row["status"] != "PENDING_MORNING_REVIEW":
            raise ExportError("PLAN_NOT_PENDING")
        payload = _json(row["payload_json"])
        symbol = _symbol(row["symbol"])
        if (payload.get("source_run_id") != ref.run_id or payload.get("symbol") != symbol
                or logical.get(row["plan_id"], {}).get("symbol") != symbol):
            raise ExportError("PLAN_IDENTITY_CONFLICT")
        expiry = _time(row["expires_at"])
        # Narrow supported default horizon. The existing producer has a later
        # same-day branch, but this slice does not authenticate that branch.
        if expiry <= observed or expiry.date() != target or expiry.time().replace(tzinfo=None) != time(15):
            raise ExportError("PLAN_EXPIRY_TARGET_CONFLICT")
        if row["valid_from"] is not None and _time(row["valid_from"]).date() != target:
            raise ExportError("PLAN_VALID_FROM_CONFLICT")
        if payload.get("plan_expiry") is not None:
            payload_expiry = _time(payload["plan_expiry"])
            if payload_expiry != expiry:
                raise ExportError("PLAN_PAYLOAD_EXPIRY_CONFLICT")
        if payload.get("target_trade_date") is not None and _day(payload["target_trade_date"]) != target:
            raise ExportError("PLAN_PAYLOAD_TARGET_CONFLICT")
        self._payload_binding(payload, logical[row["plan_id"]]["raw"], ref.run_id)
        explicit = summary.get("target_trade_date") is not None or payload.get("target_trade_date") is not None
        return {"plan_id": row["plan_id"], "lane_id": row["lane_id"], "symbol": symbol,
            "status": row["status"], "source_run_id": ref.run_id,
            "published_trade_date": summary["market_trade_date"], "target_trade_date": target.isoformat(),
            "expires_at": row["expires_at"], "target_binding": "EXPLICIT_TARGET" if explicit else "TARGET_BOUND_BY_SERVER_EXPIRY",
            "payload_sha256": hashlib.sha256(row["payload_json"].encode()).hexdigest(),
            "audit_plan_sha256": logical[row["plan_id"]]["raw_sha256"],
            "payload_binding": "RAW_FIELDS_AND_KNOWN_SERVER_ALIASES"}

    @staticmethod
    def _payload_binding(payload: dict[str, Any], raw: dict[str, Any], run_id: str) -> None:
        """Reconcile actual close payload construction, not arbitrary raw equality.

        workflow._plan_payload preserves raw fields and derives numeric aliases;
        _publish_plans may subsequently rewrite permissions from original A2
        authority/snapshot context. That context is absent from this slice.
        """
        aliases = {"symbol", "trigger_low", "trigger_high", "stop_level", "no_chase",
                   "confirmation_bars", "action", "trend_entry_rule_version"}
        permissions = {"execution_permission", "research_only_reason", "a2_execution_permission",
                       "a2_research_only_reason"}
        for key, value in raw.items():
            if key in aliases or key in permissions:
                continue
            if key not in payload or _hash(payload[key]) != _hash(value):
                raise ExportError("PLAN_AUDIT_PAYLOAD_CONFLICT")
        # These additions indicate an A2-authority branch we cannot reconstruct
        # using only the publication/audit/DB files. Do not infer permission.
        if any(key in payload for key in ("a2_execution_permission", "a2_research_only_reason")):
            raise ExportError("PLAN_AUDIT_PAYLOAD_BINDING_UNPROVEN")
        for key in permissions:
            if (key in raw) != (key in payload) or raw.get(key) != payload.get(key):
                raise ExportError("PLAN_AUDIT_PAYLOAD_BINDING_UNPROVEN")
        zone = raw.get("trigger_zone") if isinstance(raw.get("trigger_zone"), dict) else {}

        def numeric(value):
            try:
                number = float(value)
            except (TypeError, ValueError):
                return None
            if not math.isfinite(number):
                raise ExportError("PLAN_AUDIT_PAYLOAD_BINDING_UNPROVEN")
            return number if number > 0 else None

        expected = {"symbol": _symbol(raw.get("symbol")), "trigger_low": numeric(zone.get("low")),
            "trigger_high": numeric(zone.get("high")), "stop_level": numeric(raw.get("invalidation_level")),
            "no_chase": numeric(raw.get("no_chase_price")),
            "confirmation_bars": 1 if raw.get("strategy_profile") else 2,
            "action": "BUY_SIGNAL", "source_run_id": run_id}
        if str(raw.get("strategy_profile") or "").upper() == "TREND_MA5":
            expected["trend_entry_rule_version"] = "trend-ma5/2"
        elif "trend_entry_rule_version" in raw:
            expected["trend_entry_rule_version"] = raw["trend_entry_rule_version"]
        for key, value in expected.items():
            if key not in payload or payload[key] != value or isinstance(payload[key], bool) != isinstance(value, bool):
                raise ExportError("PLAN_AUDIT_PAYLOAD_CONFLICT")
        if set(payload) - set(raw) - set(expected):
            raise ExportError("PLAN_AUDIT_PAYLOAD_BINDING_UNPROVEN")


def write_export(path: Path, result: Mapping[str, Any]) -> str:
    """Exclusive archive write; no output reuse or mutation of plan state."""
    if result.get("schema_version") != SCHEMA_VERSION or result.get("eligibility_released") is not False:
        raise ExportError("EXPORT_CONTRACT_INVALID")
    body = {key: value for key, value in result.items() if key != "receipt_sha256"}
    if _hash(body) != result.get("receipt_sha256"):
        raise ExportError("EXPORT_RECEIPT_HASH_MISMATCH")
    encoded = json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False).encode("utf-8")
    try:
        with Path(path).open("xb") as stream:
            stream.write(encoded)
    except FileExistsError as exc:
        raise ExportError("REFUSE_OVERWRITE") from exc
    return hashlib.sha256(encoded).hexdigest()
