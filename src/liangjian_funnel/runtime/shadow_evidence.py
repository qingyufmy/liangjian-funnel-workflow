"""Isolated arrival-time shadow evidence, not an account or execution ledger."""
from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import tempfile
from contextlib import closing, contextmanager
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from ..data.mootdx import MinuteBar
from ..evaluation.ablation.outcomes import evaluate_outcome
from ..evaluation.ablation.price_limits import resolve_price_limits
from .simulation import _first_complete_bar_end

SH = ZoneInfo("Asia/Shanghai")
VERSION = "shadow-evidence/1"
TABLES = {"shadow_events", "shadow_signals", "shadow_states", "shadow_minutes",
          "shadow_identity", "shadow_bars", "shadow_labels", "shadow_clock"}
LIMIT_KEYS = ("upper_limit", "lower_limit", "limit_up", "limit_down", "price_limits", "price_limit")


class _Invalid(ValueError):
    pass


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _hash(value):
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _copy(value):
    return json.loads(_json(value))


def _stamp(value):
    result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    if result.tzinfo is None or result.utcoffset() is None:
        raise _Invalid("AWARE_TIMESTAMP_REQUIRED")
    return result.astimezone(SH)


def _binding(value):
    return (isinstance(value.get("source_ref"), str) and bool(value["source_ref"].strip())
            and re.fullmatch(r"[0-9a-fA-F]{64}", str(value.get("source_input_sha256", ""))))


def _pending(trigger):
    expected = _first_complete_bar_end(trigger)
    return {"fill_status": "PENDING_NEXT_COMPLETE_MINUTE", "fill_price": None,
            "fill_bar_end": None, "expected_bar_end": expected.isoformat() if expected else None,
            "reason_codes": [], "t1_close_return": None, "t3_close_return": None,
            "t5_close_return": None, "stop_r": None, "mfe": None, "mae": None,
            "return_kind": "RESEARCH_COUNTERFACTUAL", "account_pnl": None}


class ShadowEvidenceLedger:
    """Independent caller-selected DB/JSONL pair; no production discovery.

    Public writes do not propagate exceptions. SQLite is authoritative, JSONL
    is a recoverable hash-chained mirror. A failed mirror is explicitly PENDING.
    """

    def __init__(self, db_path, jsonl_path):
        self.db_path, self.jsonl_path = Path(db_path).resolve(), Path(jsonl_path).resolve()
        self._init_error = False
        try:
            runtime = Path(__file__).resolve().parent
            sources = [runtime / name for name in ('shadow_evidence.py', 'simulation.py', 'stock_trading_rules.py')]
            sources += [runtime.parent / 'evaluation' / 'ablation' / name for name in ('outcomes.py', 'price_limits.py')]
            self.implementation_source_sha256 = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sources}
            if self.db_path == self.jsonl_path:
                raise _Invalid("DISTINCT_EVIDENCE_PATHS_REQUIRED")
            if self.db_path.exists():
                with closing(sqlite3.connect(self.db_path.as_uri() + "?mode=ro", uri=True)) as db:
                    names = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
                    if not names or not names <= TABLES:
                        raise _Invalid("FOREIGN_DATABASE_REFUSED")
            elif self.jsonl_path.exists() and self.jsonl_path.stat().st_size:
                raise _Invalid("FOREIGN_MIRROR_REFUSED")
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            with self._connect() as db:
                db.executescript("""
                  CREATE TABLE IF NOT EXISTS shadow_events(seq INTEGER PRIMARY KEY, event_key TEXT UNIQUE NOT NULL, record TEXT NOT NULL);
                  CREATE TABLE IF NOT EXISTS shadow_signals(id TEXT PRIMARY KEY, record TEXT NOT NULL, outcome TEXT);
                  CREATE TABLE IF NOT EXISTS shadow_states(id TEXT PRIMARY KEY, minute TEXT NOT NULL, input_hash TEXT NOT NULL, state_hash TEXT NOT NULL, plan_hash TEXT NOT NULL, triggered INTEGER NOT NULL);
                  CREATE TABLE IF NOT EXISTS shadow_minutes(id TEXT PRIMARY KEY, record TEXT NOT NULL);
                  CREATE TABLE IF NOT EXISTS shadow_identity(id TEXT PRIMARY KEY, record TEXT NOT NULL);
                  CREATE TABLE IF NOT EXISTS shadow_bars(id TEXT PRIMARY KEY, record TEXT NOT NULL, conflicted INTEGER NOT NULL DEFAULT 0);
                  CREATE TABLE IF NOT EXISTS shadow_labels(id TEXT PRIMARY KEY, record TEXT NOT NULL);
                  CREATE TABLE IF NOT EXISTS shadow_clock(id TEXT PRIMARY KEY, observed_at TEXT NOT NULL);
                """)
        except Exception:
            self._init_error = True

    @contextmanager
    def _connect(self):
        db = sqlite3.connect(self.db_path, timeout=1)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def _event(self, db, kind, key, payload):
        if db.execute("SELECT 1 FROM shadow_events WHERE event_key=?", (key,)).fetchone():
            return False
        previous = db.execute("SELECT seq,record FROM shadow_events ORDER BY seq DESC LIMIT 1").fetchone()
        seq = previous["seq"] + 1 if previous else 1
        record = {"schema": VERSION, "seq": seq, "kind": kind, "event_key": key,
                  "implementation_source_sha256": self.implementation_source_sha256,
                  "payload": payload, "payload_sha256": _hash(payload),
                  "previous_event_sha256": json.loads(previous["record"])["event_sha256"] if previous else None}
        record["event_sha256"] = _hash(record)
        db.execute("INSERT INTO shadow_events VALUES(?,?,?)", (seq, key, _json(record)))
        return True

    def _write_mirror(self):
        self.jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = None
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            expected = db.execute("SELECT record FROM shadow_events ORDER BY seq").fetchall()
            if self.jsonl_path.exists():
                with self.jsonl_path.open("rb") as current:
                    for i, line in enumerate(current):
                        if i >= len(expected):
                            raise _Invalid("MIRROR_DIVERGED")
                        encoded = expected[i]["record"].encode("utf-8") + b"\n"
                        # A torn last line is repairable only as an exact prefix.
                        if line != encoded and not (not line.endswith(b"\n") and encoded.startswith(line)):
                            raise _Invalid("MIRROR_DIVERGED")
            try:
                with tempfile.NamedTemporaryFile(mode="wb", dir=self.jsonl_path.parent, prefix=".shadow-", suffix=".tmp", delete=False) as target:
                    temporary = Path(target.name)
                    for row in expected:
                        target.write(row["record"].encode("utf-8") + b"\n")
                    target.flush()
                    os.fsync(target.fileno())
                os.replace(temporary, self.jsonl_path)
                temporary = None
                if os.name != "nt":
                    fd = os.open(self.jsonl_path.parent, os.O_RDONLY)
                    try:
                        os.fsync(fd)
                    finally:
                        os.close(fd)
                db.commit()
            finally:
                if temporary is not None:
                    temporary.unlink(missing_ok=True)

    def _write(self, operation):
        if self._init_error:
            return {"ok": False, "stored": False, "error_code": "SHADOW_EVIDENCE_WRITE_FAILED"}
        try:
            with self._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                count = db.execute("SELECT COUNT(*) FROM shadow_events").fetchone()[0]
                result = operation(db)
                result.update(ok=True, stored=True,
                              event_count=db.execute("SELECT COUNT(*) FROM shadow_events").fetchone()[0] - count)
                db.commit()
        except _Invalid as exc:
            return {"ok": False, "stored": False, "error_code": str(exc)}
        except Exception:
            return {"ok": False, "stored": False, "error_code": "SHADOW_EVIDENCE_WRITE_FAILED"}
        try:
            self._write_mirror()
            result["mirror_status"] = "SYNCED"
        except Exception:
            result.update(mirror_status="PENDING", mirror_error_code="SHADOW_JSONL_WRITE_FAILED")
        return result

    def recover_mirror(self):
        if self._init_error:
            return {"ok": False, "error_code": "SHADOW_EVIDENCE_WRITE_FAILED"}
        try:
            self._write_mirror()
            return {"ok": True, "mirror_status": "SYNCED"}
        except Exception:
            return {"ok": False, "mirror_status": "PENDING", "error_code": "SHADOW_JSONL_WRITE_FAILED"}

    def seal_price_limit_evidence(self, plan_id, evidence, *, observed_at, raw_response=None):
        def operation(db):
            now, frozen = _stamp(observed_at), _copy(evidence)
            source_ok = bool(_binding(frozen))
            if raw_response is not None:
                if not isinstance(raw_response, bytes):
                    raise _Invalid("RAW_RESPONSE_BYTES_REQUIRED")
                if hashlib.sha256(raw_response).hexdigest() != str(frozen.get("source_input_sha256", "")).lower():
                    raise _Invalid("SOURCE_RESPONSE_HASH_MISMATCH")
            try:
                source_at = _stamp(frozen.get("observed_at"))
                clock_ok = source_at.date() == now.date() and source_at <= now and frozen.get("trade_date") == now.date().isoformat()
            except (ValueError, TypeError):
                clock_ok = False
            normalized = _copy(frozen)
            # Positive ST-name evidence only; absence of ST proves nothing.
            name = normalized.get("security_name")
            if isinstance(name, str) and "ST" in name.upper():
                normalized["is_st"] = True
            resolved = resolve_price_limits({**normalized, "price_limit_evidence": normalized},
                                             symbol=str(frozen.get("symbol", "")), at=now)
            name_unproven = (resolved.get("basis") == "DERIVED_FROM_PRECLOSE_AND_BOARD_RULE"
                             and (not isinstance(name, str) or not name.strip()))
            if not source_ok or not clock_ok or name_unproven:
                resolved = {"status": "UNKNOWN", "upper": None, "lower": None, "derived_limit_prices": None,
                            "reason_code": "PRICE_LIMITS_UNKNOWN"}
            reason = ("SOURCE_DATE_UNPROVEN" if not clock_ok else "SOURCE_BINDING_UNPROVEN" if not source_ok else
                      "EXCHANGE_NAME_UNPROVEN" if name_unproven else
                      resolved.get("evidence_reason") or resolved.get("reason_code"))
            annotations = []
            if (normalized.get("preclose_basis") == "EXCHANGE_DISPLAYED_PRECLOSE"
                    and normalized.get("preclose") is not None and normalized.get("prior_raw_close") is not None
                    and normalized["preclose"] != normalized["prior_raw_close"]):
                annotations.append("EX_RIGHTS_REFERENCE_OBSERVED")
            item = {"plan_id": str(plan_id), "trade_date": now.date().isoformat(),
                    "captured_at": now.isoformat(), "original_evidence": frozen,
                    "price_limit_evidence": normalized, "evidence_sha256": _hash(frozen),
                    "limits": resolved, "reason_code": reason, "annotations": annotations,
                    "source_authenticated": False, "raw_response_hash_verified": raw_response is not None}
            key = str(plan_id) + ":" + now.date().isoformat()
            old = db.execute("SELECT record FROM shadow_identity WHERE id=?", (key,)).fetchone()
            if old:
                if json.loads(old["record"])["evidence_sha256"] != item["evidence_sha256"]:
                    raise _Invalid("FROZEN_IDENTITY_CONFLICT")
                return {"duplicate": True}
            db.execute("INSERT INTO shadow_identity VALUES(?,?)", (key, _json(item)))
            self._event(db, "PRICE_LIMIT_EVIDENCE", "identity:" + key, item)
            return {"duplicate": False}
        return self._write(operation)

    def record_signal(self, signal, plan, *, observed_at):
        def operation(db):
            now, frozen, p = _stamp(observed_at), _copy(signal), _copy(plan)
            minute = _stamp(frozen.get("minute"))
            if minute > now or minute.date() != now.date():
                raise _Invalid("SIGNAL_CLOCK_INVALID")
            schemas = [frozen[k] for k in ("schema", "schema_version") if k in frozen]
            if frozen.get("cohort") != "REALTIME_SHADOW" or not schemas or any(s != "a4-shadow-signal/1" for s in schemas):
                raise _Invalid("SHADOW_COHORT_REQUIRED")
            for field in ("plan_id", "symbol"):
                if not p.get(field) or p[field] != frozen.get(field):
                    raise _Invalid("SIGNAL_PLAN_IDENTITY_MISMATCH")
            profiles = [p[k] for k in ("profile", "strategy_profile") if k in p]
            if not profiles or any(v != frozen.get("profile") for v in profiles):
                raise _Invalid("SIGNAL_PLAN_IDENTITY_MISMATCH")
            if frozen["profile"] not in {"TREND_MA5", "MA520_SWING"}:
                raise _Invalid("SHADOW_PROFILE_UNSUPPORTED")
            if frozen.get("observation_time") is not None and not _stamp(frozen["observation_time"]) <= minute <= now:
                raise _Invalid("SIGNAL_CLOCK_INVALID")
            start = _stamp(p.get("valid_from"))
            ends = [_stamp(p.get("valid_until", p.get("expires_at")))]
            if p.get("invalidated_at") is not None:
                ends.append(_stamp(p["invalidated_at"]))
            end = min(ends)
            if not start <= minute <= now <= end or start.date() != now.date():
                raise _Invalid("SIGNAL_OUTSIDE_PLAN_VALIDITY")
            if not frozen.get("variant_id") or not frozen.get("variant_set_version"):
                raise _Invalid("SHADOW_VARIANT_ID_REQUIRED")
            action, status = frozen.get("variant_action"), frozen.get("status")
            if status not in {"OK", "DATA_LIMITED", "ERROR", "BUDGET"} or (status == "OK" and not isinstance(action, str)):
                raise _Invalid("SIGNAL_STATUS_REQUIRED")
            if "data_block" in frozen and not isinstance(frozen["data_block"], bool):
                raise _Invalid("SIGNAL_DATA_BLOCK_INVALID")
            triggered = status == "OK" and not frozen.get("data_block", False) and action in {"BUY_SIGNAL", "ADD_SIGNAL", "BUY", "ADD"}
            expected = _first_complete_bar_end(now)
            if triggered and (expected is None or expected > end):
                raise _Invalid("SIGNAL_OUTSIDE_PLAN_VALIDITY")
            key = ":".join((now.date().isoformat(), p["plan_id"], frozen["variant_set_version"], frozen["variant_id"]))
            old = db.execute("SELECT * FROM shadow_states WHERE id=?", (key,)).fetchone()
            state_hash = _hash({k: frozen.get(k) for k in ("status", "variant_action", "variant_state", "state", "data_block", "variant_conditions")})
            input_hash, plan_hash = _hash(frozen), _hash(p)
            if old:
                if old["plan_hash"] != plan_hash:
                    raise _Invalid("FROZEN_PLAN_CONFLICT")
                old_at = _stamp(old["minute"])
                if minute < old_at:
                    raise _Invalid("OUT_OF_ORDER_SIGNAL")
                if minute == old_at:
                    if input_hash != old["input_hash"]:
                        raise _Invalid("CONFLICTING_SIGNAL_REENTRY")
                    return {"duplicate": True}
            first_trigger = triggered and not (old and old["triggered"])
            emit = old is None or first_trigger or old["state_hash"] != state_hash
            db.execute("INSERT OR REPLACE INTO shadow_states VALUES(?,?,?,?,?,?)",
                       (key, minute.isoformat(), input_hash, state_hash, plan_hash,
                        int(first_trigger or bool(old and old["triggered"]))))
            if emit:
                item_id = key + ":" + minute.isoformat()
                item = {"id": item_id, "signal": frozen, "plan": p, "plan_sha256": plan_hash,
                        "source_kind": "REALTIME_SHADOW", "account_pnl": None,
                        "event_kind": "FIRST_TRIGGER" if first_trigger else "STATE_CHANGE" if old else "INITIAL_STATE",
                        "trigger_at": now.isoformat() if first_trigger else None, "recorded_at": now.isoformat()}
                outcome = _pending(now) if first_trigger else None
                db.execute("INSERT INTO shadow_signals VALUES(?,?,?)", (item_id, _json(item), _json(outcome) if outcome else None))
                self._event(db, "SHADOW_SIGNAL", "signal:" + item_id, {**item, "outcome": outcome})
            return {"duplicate": False, "emitted": emit}
        return self._write(operation)

    def record_minute(self, summary, *, observed_at):
        def operation(db):
            now, item = _stamp(observed_at), _copy(summary)
            minute = _stamp(item.get("minute"))
            if minute > now or minute.date() != now.date():
                raise _Invalid("MINUTE_CLOCK_INVALID")
            for key in ("evaluated_plan_count", "evaluated_variant_count"):
                if type(item.get(key)) is not int or item[key] < 0:
                    raise _Invalid("MINUTE_COUNTS_REQUIRED")
            counts = [item[k] for k in ("budget_exceeded_count", "shadow_budget_exceeded_count") if k in item]
            if not counts or any(type(v) is not int or v < 0 or v != counts[0] for v in counts):
                raise _Invalid("MINUTE_COUNTS_REQUIRED")
            if not isinstance(item.get("elapsed_ms"), (int, float)) or isinstance(item["elapsed_ms"], bool) or item["elapsed_ms"] < 0:
                raise _Invalid("MINUTE_ELAPSED_REQUIRED")
            key = minute.isoformat()
            old = db.execute("SELECT record FROM shadow_minutes WHERE id=?", (key,)).fetchone()
            if old:
                if old["record"] != _json(item):
                    raise _Invalid("CONFLICTING_MINUTE_REENTRY")
                return {"duplicate": True}
            db.execute("INSERT INTO shadow_minutes VALUES(?,?)", (key, _json(item)))
            self._event(db, "SHADOW_MINUTE", "minute:" + key, item)
            return {"duplicate": False}
        return self._write(operation)

    def advance_outcomes(self, bars, *, observed_at, outcome_labels=()):
        def operation(db):
            now = _stamp(observed_at)
            old_clock = db.execute("SELECT observed_at FROM shadow_clock WHERE id='ARRIVAL'").fetchone()
            if old_clock and now < _stamp(old_clock['observed_at']):
                raise _Invalid("ARRIVAL_CLOCK_REGRESSED")
            db.execute("INSERT OR REPLACE INTO shadow_clock VALUES('ARRIVAL',?)", (now.isoformat(),))
            accepted, rejected, conflicts = 0, 0, 0
            for source in bars:
                try:
                    raw = _copy(source)
                    end, capture = _stamp(raw["bar_end"]), _stamp(raw["captured_at"])
                    if (end > capture or capture > now or raw.get("complete", raw.get("is_complete")) is not True
                            or raw.get("interval") != "1m" or raw.get("volume_unit") != "shares"
                            or raw.get("adjust_mode") not in {"none", "raw", "unadjusted"}
                            or raw.get("evidence_kind") != "MARKET_BAR" or not _binding(raw)):
                        raise _Invalid("ARRIVAL_BAR_EVIDENCE_INVALID")
                    parsed = MinuteBar.model_validate({**raw, "adjust_mode": "none"})
                    key = parsed.symbol + ":" + end.isoformat()
                    raw["symbol"], raw["bar_end"] = parsed.symbol, end.isoformat()
                    old = db.execute("SELECT record FROM shadow_bars WHERE id=?", (key,)).fetchone()
                    if old:
                        old_raw = json.loads(old['record'])
                        equivalent = {k: v for k, v in old_raw.items() if k != 'captured_at'} == {k: v for k, v in raw.items() if k != 'captured_at'}
                        if not equivalent:
                            db.execute("UPDATE shadow_bars SET conflicted=1 WHERE id=?", (key,))
                            conflicts += 1
                            self._event(db, "BAR_CONFLICT", "bar-conflict:" + key + ":" + _hash(raw), {"bar_id": key, "conflicting_bar": raw})
                        continue
                    db.execute("INSERT INTO shadow_bars VALUES(?,?,0)", (key, _json(raw)))
                    self._event(db, "BAR_ARRIVAL", "bar:" + key, raw)
                    accepted += 1
                except (ValueError, TypeError, KeyError):
                    rejected += 1
            for source in outcome_labels:
                try:
                    raw = _copy(source)
                    capture = _stamp(raw["captured_at"])
                    target = str(raw.get("trade_date", raw.get("target_trade_date")))
                    close_end = _stamp(target + "T15:00:00+08:00")
                    if not close_end <= capture <= now or not _binding(raw):
                        continue
                    key = _hash(raw)
                    if not db.execute("SELECT 1 FROM shadow_labels WHERE id=?", (key,)).fetchone():
                        db.execute("INSERT INTO shadow_labels VALUES(?,?)", (key, _json(raw)))
                        self._event(db, "HORIZON_LABEL_ARRIVAL", "label:" + key, raw)
                except (ValueError, TypeError, KeyError):
                    continue
            updates = 0
            all_bars = [json.loads(row["record"]) for row in db.execute("SELECT record FROM shadow_bars WHERE conflicted=0")]
            all_labels = [json.loads(row["record"]) for row in db.execute("SELECT record FROM shadow_labels")]
            for row in db.execute("SELECT * FROM shadow_signals WHERE outcome IS NOT NULL").fetchall():
                item, previous = json.loads(row["record"]), json.loads(row["outcome"])
                trigger = _stamp(item["trigger_at"])
                if now < trigger or (previous.get("expected_bar_end") and now < _stamp(previous["expected_bar_end"])):
                    continue
                p = _copy(item["plan"])
                # Unproved A3 limit fields cannot substitute for sealed PIT.
                for key in (*LIMIT_KEYS, "price_limit_evidence", "derived_limit_prices"):
                    p.pop(key, None)
                end = _stamp(p.get("valid_until", p.get("expires_at")))
                if p.get("invalidated_at") is not None:
                    end = min(end, _stamp(p["invalidated_at"]))
                p["valid_until"] = end.isoformat()
                identity = db.execute("SELECT record FROM shadow_identity WHERE id=?",
                                      (p["plan_id"] + ":" + trigger.date().isoformat(),)).fetchone()
                if identity:
                    sealed = json.loads(identity["record"])
                    if (sealed["limits"]["status"] == "KNOWN" and _stamp(sealed["captured_at"]) <= now
                            and sealed["original_evidence"].get("symbol") == p["symbol"]):
                        p["price_limit_evidence"] = sealed["price_limit_evidence"]
                        p["price_limit_trade_date"] = trigger.date().isoformat()
                        p["upper_limit"], p["lower_limit"] = sealed["limits"]["upper"], sealed["limits"]["lower"]
                relevant = [b for b in all_bars if b["symbol"] == p["symbol"]
                            and _stamp(b["captured_at"]) <= now and _stamp(b["bar_end"]) <= now]
                outcome = evaluate_outcome(p, trigger, relevant, outcome_labels=[
                    b for b in all_labels if _stamp(b["captured_at"]) <= now])
                outcome["available_at"] = now.isoformat()
                comparable = {k: v for k, v in outcome.items() if k != "available_at"}
                old_comparable = {k: v for k, v in previous.items() if k != "available_at"}
                if comparable != old_comparable:
                    db.execute("UPDATE shadow_signals SET outcome=? WHERE id=?", (_json(outcome), row["id"]))
                    self._event(db, "OUTCOME_UPDATE", "outcome:" + row["id"] + ":" + _hash(comparable),
                                {"signal_id": row["id"], "observed_at": now.isoformat(), "outcome": outcome})
                    updates += 1
            summary = {"observed_at": now.isoformat(), "accepted_bar_count": accepted,
                       "rejected_bar_count": rejected, "conflicting_bar_count": conflicts,
                       "outcome_update_count": updates}
            self._event(db, "ARRIVAL_SUMMARY", "arrival:" + _hash(summary), summary)
            return {**summary, "duplicate": False}
        return self._write(operation)

    def snapshot(self):
        if self._init_error:
            return {"ok": False, "error_code": "SHADOW_EVIDENCE_READ_FAILED"}
        return read_shadow_evidence(self.db_path)


def read_shadow_evidence(db_path, jsonl_path=None):
    """W3 consumer: mode=ro, one read transaction, no schema/init/recovery.

    Canonical snapshot hash binds this read view; a concurrently live SQLite
    file cannot be claimed as a stable byte snapshot. Quiesced copied-file SHA
    must be supplied separately by the export owner.
    """
    try:
        path = Path(db_path).resolve()
        with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as db:
            db.row_factory = sqlite3.Row
            db.execute('BEGIN')
            names = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
            if names != TABLES:
                raise _Invalid('SHADOW_SCHEMA_UNPROVEN')
            signals = []
            for row in db.execute('SELECT * FROM shadow_signals ORDER BY rowid'):
                item = json.loads(row['record'])
                item['outcome'] = json.loads(row['outcome']) if row['outcome'] else None
                signals.append(item)
            result = {'schema': VERSION, 'signals': signals,
                      'minutes': [json.loads(r['record']) for r in db.execute('SELECT record FROM shadow_minutes ORDER BY id')],
                      'price_limit_evidence': [json.loads(r['record']) for r in db.execute('SELECT record FROM shadow_identity ORDER BY id')],
                      'events': [json.loads(r['record']) for r in db.execute('SELECT record FROM shadow_events ORDER BY seq')],
                      'account_pnl': None, 'source_authenticated': False}
            db.rollback()
        previous = None
        projected = {}
        projected_identity, projected_minutes = [], []
        for seq, event in enumerate(result['events'], 1):
            unhashed = dict(event)
            digest = unhashed.pop('event_sha256')
            if (type(event['seq']) is not int or event['seq'] != seq or event.get('schema') != VERSION
                    or event['previous_event_sha256'] != previous or _hash(unhashed) != digest
                    or _hash(event['payload']) != event['payload_sha256']):
                raise _Invalid('SHADOW_HASH_CHAIN_INVALID')
            previous = digest
            if event['kind'] == 'SHADOW_SIGNAL':
                payload = _copy(event['payload'])
                projected[payload['id']] = payload
            elif event['kind'] == 'OUTCOME_UPDATE':
                payload = event['payload']
                projected[payload['signal_id']]['outcome'] = payload['outcome']
            elif event['kind'] == 'PRICE_LIMIT_EVIDENCE':
                projected_identity.append(event['payload'])
            elif event['kind'] == 'SHADOW_MINUTE':
                projected_minutes.append(event['payload'])
        if _json(sorted(signals, key=lambda s: s['id'])) != _json(sorted(projected.values(), key=lambda s: s['id'])):
            raise _Invalid('SHADOW_SIGNAL_PROJECTION_INVALID')
        for actual, expected in ((result['price_limit_evidence'], projected_identity), (result['minutes'], projected_minutes)):
            if _json(sorted(actual, key=_hash)) != _json(sorted(expected, key=_hash)):
                raise _Invalid('SHADOW_MATERIAL_PROJECTION_INVALID')
        result['snapshot_canonical_sha256'] = _hash(result)
        result.update(ok=True, hash_chain_status='MATCHED', mirror_status='NOT_REQUESTED',
                      source_db_sha256=None, source_db_sha256_status='QUIESCED_COPY_REQUIRED')
        if jsonl_path is not None:
            digest, count, matched = hashlib.sha256(), 0, True
            with Path(jsonl_path).open('rb') as mirror:
                for line in mirror:
                    digest.update(line)
                    if count >= len(result['events']) or line != _json(result['events'][count]).encode('utf-8') + b'\n':
                        matched = False
                    count += 1
            result.update(mirror_file_sha256=digest.hexdigest(),
                          mirror_status='SYNCED' if matched and count == len(result['events']) else 'PENDING_OR_DIVERGED')
        return result
    except _Invalid as exc:
        return {'ok': False, 'error_code': str(exc)}
    except Exception:
        return {'ok': False, 'error_code': 'SHADOW_EVIDENCE_READ_FAILED'}


__all__ = ["ShadowEvidenceLedger", "read_shadow_evidence"]
