"""Once-only 09:25 source observations; never activates or rebases a plan."""
from collections.abc import Mapping
from datetime import datetime, time
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
from zoneinfo import ZoneInfo

SHANGHAI = ZoneInfo("Asia/Shanghai")


def _aware(value):
    value = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("OPENING_TIME_INVALID")
    return value.astimezone(SHANGHAI)


def _in_window(value):
    return time(9, 25) <= value.time().replace(tzinfo=None) <= time(9, 25, 30)


def _safe(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return {"non_finite_observation": repr(value)}
    return value


def _scope(settings, current):
    database = Path(settings.state_db_path).resolve(strict=True)
    with sqlite3.connect(database.as_uri() + "?mode=ro", uri=True, timeout=2) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        rows = [dict(row) for row in conn.execute(
            "SELECT plan_id,lane_id,symbol,status,expires_at,payload_json FROM execution_plans "
            "WHERE lane_id=? AND status IN ('PENDING_MORNING_REVIEW','ACTIVE_TODAY') "
            "AND substr(expires_at,1,10)=? ORDER BY symbol,plan_id",
            (settings.research_primary_lane_id, current.date().isoformat()))]
    for row in rows:
        if _aware(row["expires_at"]).date() != current.date():
            raise ValueError("OPENING_PLAN_DATE_CONFLICT")
    return rows


POLL_SECONDS=(3,8,15,24)


def capture_scheduled_opening_evidence(settings, *, calendar=None, clock=None, fetch=None):
    """Four fixed slots, at most one request per slot across process restarts.

    A two-second dispatch allowance covers normal Python start-up, not a retry
    or catch-up window. A busy/missed slot remains missing evidence. Each
    individual child retains the existing 30s Node limit and strict receive
    deadline; a source call after the receive deadline can never claim final.
    """
    clock=clock or (lambda:datetime.now(SHANGHAI))
    current=_aware(clock())
    slot=next((second for second in POLL_SECONDS if second<=current.second<second+2),None)
    if not _in_window(current) or slot is None:
        return {'status':'NOOP','reason_code':'OPENING_POLL_SLOT_MISSED'}
    return capture_opening_evidence(settings,calendar=calendar,now=current,clock=clock,fetch=fetch,poll_slot=slot)


def capture_opening_evidence(settings, *, calendar=None, now=None, clock=None, fetch=None,poll_slot=None):
    """Capture one existing Tencent batch within the actual opening window.

    A true final-price *claim* means an original provider-reported 09:25:00
    print arrived by 09:25:30, with matching identity, observed positive price
    and reported trading. It does not authenticate exchange provenance or
    release execution eligibility. Later ticks are retained, never relabelled.
    The daily exclusive claim prevents duplicate network calls after restart.
    """
    clock = clock or (lambda: datetime.now(SHANGHAI))
    current = _aware(now or clock())
    if poll_slot is not None and (type(poll_slot) is not int or poll_slot not in POLL_SECONDS
            or not poll_slot<=current.second<poll_slot+2):
        raise ValueError('OPENING_POLL_SLOT_INVALID')
    if not _in_window(current):
        return {"status":"NOOP", "reason_code":"OPENING_EVIDENCE_WINDOW_MISSED"}
    if calendar is None:
        from .calendar import ExchangeTradingCalendar
        calendar = ExchangeTradingCalendar()
    if not calendar.is_trading_day(current.date()):
        return {"status":"NOOP", "reason_code":"NON_TRADING_DAY"}
    rows = _scope(settings, current)
    if not rows:
        return {"status":"NOOP", "reason_code":"OPENING_EVIDENCE_SCOPE_EMPTY"}
    directory = Path(settings.workflow_output_dir) / "opening_evidence" / current.date().isoformat()
    directory.mkdir(parents=True, exist_ok=True)
    claim = directory / ("once.json" if poll_slot is None else f"poll-{poll_slot:02d}.json")
    try:
        with claim.open("x", encoding="utf-8") as stream:
            json.dump({"claimed_at":current.isoformat(),"poll_slot_second":poll_slot,
                "plan_ids":[row["plan_id"] for row in rows]}, stream)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError:
        return {"status":"NOOP", "reason_code":"OPENING_EVIDENCE_ALREADY_CLAIMED"}
    # Calendar and read-only scope preparation may consume the narrow window.
    before_fetch = _aware(clock())
    source_error = None
    quotes = {}
    if not _in_window(before_fetch) or before_fetch.date() != current.date():
        source_error = "OPENING_EVIDENCE_WINDOW_MISSED"
    else:
        if fetch is None:
            from ..data.rotation_theme import _default_tencent_quote_batch_fetch
            fetch = _default_tencent_quote_batch_fetch
        try:
            quotes = fetch(sorted({row["symbol"] for row in rows}))
            if not isinstance(quotes, Mapping):
                raise ValueError("QUOTE_MAPPING_REQUIRED")
        except Exception:
            source_error = "OPENING_EVIDENCE_SOURCE_FAILED"
            quotes = {}
    received = _aware(clock())
    observed = {}
    for symbol in sorted({row["symbol"] for row in rows}):
        quote = quotes.get(symbol) or {}
        reasons = []
        if not isinstance(quote, Mapping):
            quote = {}
        if quote.get("symbol") != symbol or quote.get("source_id") != "TENCENT:qt.gtimg.cn":
            reasons.append("OPENING_QUOTE_IDENTITY_OR_SOURCE_UNPROVEN")
        try:
            stamp = _aware(quote.get("quote_time"))
            if stamp.date() != current.date() or stamp.time().replace(tzinfo=None) != time(9, 25):
                reasons.append("OPENING_ORIGINAL_092500_PRINT_MISSING")
            if stamp > received:
                reasons.append("OPENING_SOURCE_TIME_IN_FUTURE")
        except (TypeError, ValueError):
            reasons.append("OPENING_SOURCE_TIMESTAMP_INVALID")
        price = quote.get("latest_price")
        if (isinstance(price, bool) or not isinstance(price, (int, float))
                or not math.isfinite(price) or price <= 0 or quote.get("no_reported_trades") is not False):
            reasons.append("OPENING_REPORTED_TRADE_PRICE_UNPROVEN")
        if received.date() != current.date() or not _in_window(received):
            reasons.append("OPENING_RECEIVE_WINDOW_MISSED")
        if source_error:
            reasons.append(source_error)
        observed[symbol] = {
            **{key:_safe(quote.get(key)) for key in (
                "symbol", "source_id", "quote_time", "latest_price", "previous_close", "no_reported_trades")},
            "auction_final_price_claimed":not reasons,
            "claim_basis":"ORIGINAL_PROVIDER_092500_PRINT_RECEIVED_WITHIN_092530",
            "exchange_provenance_authenticated":False, "reason_codes":sorted(set(reasons)),
        }
    claims = sum(item["auction_final_price_claimed"] for item in observed.values())
    encoded_scope = json.dumps(rows, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    body = {"schema_version":"opening-auction-source-evidence/1", "observed_at":current.isoformat(),
        "poll_slot_second":poll_slot,
        "received_at":received.isoformat(), "trade_date":current.date().isoformat(),
        "plan_ids":[row["plan_id"] for row in rows], "plan_rows_sha256":hashlib.sha256(encoded_scope).hexdigest(),
        "by_symbol":observed, "claimed_symbol_count":claims, "requested_symbol_count":len(observed),
        "status":"CAPTURED" if claims == len(observed) else "DATA_LIMITED",
        "reason_code":source_error or ("OPENING_EVIDENCE_CAPTURED" if claims == len(observed) else "OPENING_EVIDENCE_INCOMPLETE"),
        "execution_authority":False, "activation_or_rebase_performed":False, "source_attempts":int(source_error != "OPENING_EVIDENCE_WINDOW_MISSED")}
    raw = json.dumps(body, ensure_ascii=False, sort_keys=True, allow_nan=False).encode()
    digest = hashlib.sha256(raw).hexdigest()
    path = directory / ("capture-" + digest[:16] + ".json")
    with path.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    return {"status":body["status"], "reason_code":body["reason_code"], "evidence_path":str(path.resolve()),
            "evidence_sha256":digest, "claimed_symbol_count":claims, "execution_authority":False}
