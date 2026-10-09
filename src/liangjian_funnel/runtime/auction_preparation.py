"""Local shadow auction preflight; no scheduler, Settings, Store or network.

File certificates are caller assertions, not a night acquisition adapter.
READY here is a shadow contract result, never production/eligibility READY.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
import hashlib
import json
import math
from pathlib import Path
import re
import time
from zoneinfo import ZoneInfo

from ..data.cninfo import CninfoFetchResult
from ..data.disclosure_incremental import covers_query
from ..pipeline.snapshot import FrozenInputSnapshot

TZ = ZoneInfo("Asia/Shanghai")
SLOW_ROLES = frozenset({"FINANCIALS", "MEMBERSHIPS", "GOV_POLICY_90D",
                        "BUSINESS_REPORTS", "BUSINESS_PDFS", "DEFERRED_QUEUE"})
FAST_ROLES = frozenset({"PREVIOUS_MARKET_FINAL", "NEWS", "MACRO"})
DISCLOSURE_TTL = timedelta(hours=6)
VERSION = "auction-split-shadow/1"
_SYMBOL = re.compile(r"^[0-9]{6}\.(SH|SZ|BJ)$")
_HASH = re.compile(r"^[0-9a-f]{64}$")
_TOKEN = re.compile(r"^[A-Za-z0-9_.:-]{1,100}$")


class PreparationError(ValueError):
    """Fixed safe reason, never paths/provider response/exception text."""


def canonical(value) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _date(value) -> date:
    try:
        result = date.fromisoformat(value)
        if result.isoformat() != value:
            raise ValueError
        return result
    except (ValueError, TypeError):
        raise PreparationError("TRADE_DATE_INVALID") from None


def _aware(value) -> datetime:
    try:
        result = datetime.fromisoformat(value) if isinstance(value, str) else value
        if not isinstance(result, datetime) or result.tzinfo is None or result.utcoffset() is None:
            raise ValueError
        return result.astimezone(TZ)
    except (ValueError, TypeError):
        raise PreparationError("SOURCE_TIME_INVALID") from None


def _scope(values) -> list[str]:
    if (not isinstance(values, list) or len(values) > 10000
            or not all(isinstance(s, str) and _SYMBOL.fullmatch(s) for s in values)
            or len(values) != len(set(values))):
        raise PreparationError("SCOPE_INVALID")
    return sorted(values)


def local_path(input_root: Path, value) -> Path:
    if not isinstance(value, str) or not value or Path(value).is_absolute():
        raise PreparationError("INPUT_PATH_OUTSIDE_ROOT")
    root = Path(input_root).resolve()
    candidate = (root / value).resolve()
    if not candidate.is_relative_to(root) or candidate == root:
        raise PreparationError("INPUT_PATH_OUTSIDE_ROOT")
    return candidate


def read_local_json(input_root: Path, value):
    path = local_path(input_root, value)
    try:
        return json.loads(path.read_bytes())
    except (OSError, ValueError, UnicodeError):
        raise PreparationError("LOCAL_INPUT_INVALID") from None


def _read_ref(input_root: Path, ref: dict) -> Path:
    if not isinstance(ref, dict) or not _HASH.fullmatch(str(ref.get("sha256", ""))):
        raise PreparationError("COMPONENT_HASH_INVALID")
    path = local_path(input_root, ref.get("path"))
    try:
        # Streaming avoids cloning a multi-MiB PDF/snapshot into the manifest.
        with path.open("rb") as stream:
            calculated = hashlib.file_digest(stream, "sha256").hexdigest()
        if calculated != ref["sha256"]:
            raise PreparationError("COMPONENT_HASH_MISMATCH")
    except OSError:
        raise PreparationError("COMPONENT_FILE_MISSING") from None
    return path


def _check_snapshot(input_root: Path, ref: dict, scope: list[str], created: datetime):
    path = _read_ref(input_root, ref)
    try:
        frozen = FrozenInputSnapshot.model_validate_json(path.read_bytes())
    except (ValueError, TypeError, OSError):
        raise PreparationError("FROZEN_SNAPSHOT_INVALID") from None
    if (frozen.snapshot_id != ref.get("snapshot_id") or frozen.snapshot_hash != ref.get("snapshot_hash")
            or frozen.as_of > created or frozen.as_of.date() != created.date()
            or sorted(record.symbol for record in frozen.universe_candidates) != scope):
        raise PreparationError("FROZEN_SNAPSHOT_ID_SCOPE_MISMATCH")


def _check_slow_body(bundle: dict, input_root: Path):
    if not isinstance(bundle, dict) or bundle.get("schema_version") != VERSION:
        raise PreparationError("SLOW_BUNDLE_INVALID")
    scope = _scope(bundle.get("scope"))
    if not scope or bundle.get("scope_sha256") != digest(canonical(scope)):
        raise PreparationError("SLOW_SCOPE_HASH_MISMATCH")
    generation = _date(bundle.get("trade_date"))
    created = _aware(bundle.get("created_at"))
    if created.date() != generation or not _TOKEN.fullmatch(str(bundle.get("generation_id", ""))):
        raise PreparationError("SLOW_GENERATION_INVALID")
    components = bundle.get("components")
    if not isinstance(components, dict) or set(components) != SLOW_ROLES:
        raise PreparationError("SLOW_COMPONENTS_INCOMPLETE")
    for role, ref in components.items():
        if (not isinstance(ref, dict) or ref.get("complete") is not True
                or _scope(ref.get("scope")) != scope
                or not _TOKEN.fullmatch(str(ref.get("source_id", "")))):
            raise PreparationError("SLOW_COMPONENT_SCOPE_OR_COMPLETENESS_UNPROVEN")
        fetched = _aware(ref.get("source_fetched_at"))
        first, last = _date(ref.get("window_from")), _date(ref.get("window_through"))
        if fetched > created or first > last or last != generation:
            raise PreparationError("SLOW_COMPONENT_SOURCE_TIME_OR_WINDOW_INVALID")
        if role == "GOV_POLICY_90D" and first > generation - timedelta(days=90):
            raise PreparationError("GOV_POLICY_90D_WINDOW_INCOMPLETE")
        _read_ref(input_root, ref)
    _check_snapshot(input_root, bundle.get("snapshot_reference"), scope, created)


def build_slow_bundle(*, input_root: Path, generation_id: str, trade_date: str,
                      scope: list[str], created_at: datetime, components: dict,
                      snapshot_reference: dict) -> dict:
    """Manifest only, referencing original immutable bytes; no acquisition.

    Source times/completeness must come from source receipts, never file mtime.
    All six scopes and the real FrozenInputSnapshot binding are revalidated.
    """
    allowed = ("path", "sha256", "source_id", "source_fetched_at", "complete",
               "scope", "window_from", "window_through")
    if not isinstance(components, dict):
        raise PreparationError("SLOW_COMPONENTS_INCOMPLETE")
    if not isinstance(snapshot_reference, dict):
        raise PreparationError("FROZEN_SNAPSHOT_INVALID")
    normalized_scope = _scope(scope)
    body = {"schema_version": VERSION, "generation_id": generation_id,
            "trade_date": trade_date, "created_at": _aware(created_at).isoformat(),
            "scope": normalized_scope, "scope_sha256": digest(canonical(normalized_scope)),
            "snapshot_reference": {key: snapshot_reference.get(key) for key in
                ("path", "sha256", "snapshot_id", "snapshot_hash")},
            "components": {role: {key: ref.get(key) for key in allowed}
                           for role, ref in components.items() if isinstance(ref, dict)},
            "implementation_status": "IMPLEMENTATION_PARTIAL",
            "night_acquisition_adapter": "NOT_IMPLEMENTED"}
    _check_slow_body(body, input_root)
    body["manifest_sha256"] = digest(canonical(body))
    return body


def write_slow_bundle(path: Path, bundle: dict, *, input_root: Path) -> str:
    """Exclusive create only; return original file-byte SHA for later pinning."""
    path = Path(path)
    if path.exists():
        raise PreparationError("REFUSE_OVERWRITE")
    _check_slow_body(bundle, input_root)
    body = {key: value for key, value in bundle.items() if key != "manifest_sha256"}
    if digest(canonical(body)) != bundle.get("manifest_sha256"):
        raise PreparationError("SLOW_MANIFEST_HASH_MISMATCH")
    encoded = canonical(bundle) + b"\n"
    try:
        with path.open("xb") as stream:
            stream.write(encoded)
    except FileExistsError:
        raise PreparationError("REFUSE_OVERWRITE") from None
    except OSError:
        raise PreparationError("LOCAL_OUTPUT_FAILED") from None
    return digest(encoded)


def _calendar(trade_calendar, now: datetime):
    if not isinstance(trade_calendar, list) or not trade_calendar:
        raise PreparationError("TRADE_CALENDAR_UNPROVEN")
    parsed = [_date(day) for day in trade_calendar]
    if parsed != sorted(set(parsed)) or now.date() not in parsed or parsed.index(now.date()) == 0:
        raise PreparationError("TRADE_CALENDAR_UNPROVEN")
    return parsed, parsed[parsed.index(now.date()) - 1]


def _morning_scope(publication, positions, *, previous: date, now: datetime):
    if (not isinstance(publication, dict) or publication.get("complete") is not True
            or publication.get("previous_close_trade_date") != previous.isoformat()
            or not _HASH.fullmatch(str(publication.get("source_sha256", "")))
            or not isinstance(publication.get("plans"), list)):
        raise PreparationError("PREVIOUS_A3_PUBLICATION_UNPROVEN")
    pending = set()
    for row in publication["plans"]:
        if not isinstance(row, dict):
            raise PreparationError("PREVIOUS_A3_PUBLICATION_UNPROVEN")
        if row.get("status") != "PENDING_MORNING_REVIEW":
            continue
        _date(row.get("target_trade_date"))
        if row["target_trade_date"] != now.date().isoformat():
            continue
        if row.get("a3_published") is not True or row.get("published_trade_date") != previous.isoformat():
            raise PreparationError("PREVIOUS_A3_PUBLICATION_UNPROVEN")
        pending.update(_scope([row.get("symbol")]))
    if (not isinstance(positions, dict) or positions.get("complete") is not True
            or not _HASH.fullmatch(str(positions.get("source_sha256", "")))
            or not isinstance(positions.get("records"), list) or _aware(positions.get("as_of")) > now):
        raise PreparationError("POSITIONS_UNPROVEN")
    holding = set()
    for row in positions["records"]:
        quantity = row.get("total_qty") if isinstance(row, dict) else None
        if type(quantity) not in (int, float) or not math.isfinite(quantity) or quantity < 0:
            raise PreparationError("POSITIONS_UNPROVEN")
        if quantity > 0:
            holding.update(_scope([row.get("symbol")]))
    return sorted(pending | holding), sorted(pending), sorted(holding)


def _valid_delta(value, *, symbol: str, start: str, end: str, now: datetime) -> tuple[bool, str | None]:
    try:
        result = CninfoFetchResult.model_validate(value)
        valid = (covers_query(result, symbol=symbol, start=start, end=end, keyword="", now=now)
                 and timedelta(0) <= now - result.fetched_at <= DISCLOSURE_TTL
                 and result.metadata.get("availability_state") != "STALE_VERIFIED_FALLBACK"
                 and result.total is not None and result.total == len(result.announcements)
                 and len({row.announcement_id for row in result.announcements}) == len(result.announcements)
                 and all(row.sec_code == symbol[:6] and row.publish_time <= now
                         and result.start_date <= row.publish_time.date().isoformat() <= end
                         for row in result.announcements))
        return bool(valid), digest(canonical(result.model_dump(mode="json")))
    except (ValueError, TypeError, OverflowError):
        return False, None


def fast_preflight(*, input_root: Path, slow_bundle_path: Path, expected_slow_sha256: str,
                   expected_scope: list[str], now: datetime, trade_calendar: list[str],
                   plan_publication: dict, positions: dict, fast_inputs: dict,
                   delta_results: dict, budget_seconds: float, clock=time.monotonic) -> dict:
    """Validate only explicitly supplied local files/results, without fallback.

    Fast source roles are exactly prior-market-final/news/macro. Disclosure
    results are unfiltered recent-10D/today, only previous A3 pending union
    positive positions. No result can relax a production announcement gate.
    """
    started = clock()
    report = {"schema_version": VERSION, "mode": "AUCTION_RESEARCH_ONLY_SHADOW",
              "implementation_status": "IMPLEMENTATION_PARTIAL", "night_acquisition_adapter": "NOT_IMPLEMENTED",
              "contract_status": "BLOCKED", "reason_code": None,
              "publish_plans": False, "eligibility_released": False,
              "research_index_allowed": False, "production_scheduler_connected": False,
              "network_requests": 0, "slow_fallback_attempted": False,
              "disclosure_ttl_seconds": int(DISCLOSURE_TTL.total_seconds()),
              "budget_seconds": budget_seconds if type(budget_seconds) in (int, float) and math.isfinite(budget_seconds) else None,
              "budget_origin": "EXPLICIT_SHADOW_REQUEST_NOT_PRODUCTION_TIMER",
              "elapsed_seconds": 0, "delta_scope": [], "delta_count": 0,
              "stock_blocks": {}, "stock_slow_gaps": [], "delta_results": [], "fast_input_hashes": {},
              "slow_trade_day_lag": None}

    def finish(reason=None):
        elapsed = clock() - started
        report["elapsed_seconds"] = elapsed if math.isfinite(elapsed) and elapsed >= 0 else None
        if reason:
            report["reason_code"] = reason
        return report

    if type(budget_seconds) not in (int, float) or not math.isfinite(budget_seconds) or not 0 < budget_seconds <= 1200:
        return finish("FAST_BUDGET_INVALID")
    try:
        now = _aware(now)
        report["target_trade_date"] = now.date().isoformat()
        calendar, previous = _calendar(trade_calendar, now)
        report["previous_trade_date"] = previous.isoformat()
        bundle_path = Path(slow_bundle_path).resolve()
        if not bundle_path.is_relative_to(Path(input_root).resolve()):
            raise PreparationError("INPUT_PATH_OUTSIDE_ROOT")
        if not bundle_path.is_file():
            return finish("SLOW_BUNDLE_MISSING")
        raw = bundle_path.read_bytes()
        if not _HASH.fullmatch(str(expected_slow_sha256)) or digest(raw) != expected_slow_sha256:
            raise PreparationError("SLOW_MANIFEST_BYTE_HASH_MISMATCH")
        bundle = json.loads(raw)
        if not isinstance(bundle, dict):
            raise PreparationError("SLOW_BUNDLE_INVALID")
        body = {key: value for key, value in bundle.items() if key != "manifest_sha256"}
        if digest(canonical(body)) != bundle.get("manifest_sha256"):
            raise PreparationError("SLOW_MANIFEST_HASH_MISMATCH")
        _check_slow_body(bundle, input_root)
        if _scope(expected_scope) != bundle["scope"]:
            raise PreparationError("SLOW_SCOPE_MISMATCH")
        generation = _date(bundle["trade_date"])
        if generation not in calendar or generation > previous or _aware(bundle["created_at"]) > now:
            raise PreparationError("SLOW_GENERATION_CALENDAR_UNPROVEN")
        lag = calendar.index(now.date()) - calendar.index(generation)
        report.update(slow_trade_day_lag=lag, slow_generation_id=bundle["generation_id"],
                      slow_manifest_byte_sha256=expected_slow_sha256,
                      slow_manifest_sha256=bundle["manifest_sha256"],
                      scope_sha256=bundle["scope_sha256"],
                      frozen_snapshot_hash=bundle["snapshot_reference"]["snapshot_hash"])
        if not isinstance(fast_inputs, dict) or set(fast_inputs) != FAST_ROLES:
            raise PreparationError("FAST_INPUT_ROLE_FORBIDDEN")
        for role, ref in fast_inputs.items():
            if not isinstance(ref, dict) or ref.get("complete") is not True or _aware(ref.get("source_fetched_at")) > now:
                raise PreparationError("FAST_INPUT_INCOMPLETE")
            required_day = previous if role == "PREVIOUS_MARKET_FINAL" else now.date()
            if _date(ref.get("trade_date")) != required_day or (role == "PREVIOUS_MARKET_FINAL" and ref.get("finalized") is not True):
                raise PreparationError("FAST_INPUT_DATE_OR_FINALITY_UNPROVEN")
            if _aware(ref["source_fetched_at"]).date() < required_day:
                raise PreparationError("FAST_SOURCE_WINDOW_UNPROVEN")
            _read_ref(input_root, ref)
            report["fast_input_hashes"][role] = ref["sha256"]
        domain, pending, holdings = _morning_scope(plan_publication, positions, previous=previous, now=now)
        if not isinstance(delta_results, dict) or any(symbol not in domain for symbol in delta_results):
            raise PreparationError("DELTA_SCOPE_OUT_OF_BOUNDS")
        report.update(delta_scope=domain, delta_count=len(domain),
                      pending_plan_count=len(pending), positive_position_count=len(holdings),
                      plan_publication_sha256=digest(canonical(plan_publication)),
                      positions_sha256=digest(canonical(positions)),
                      query_start=(now.date() - timedelta(days=10)).isoformat(),
                      query_end=now.date().isoformat())
        # Out-of-research holdings still need risk/disclosure review. Never
        # expand research candidates or imply that missing slow facts pass.
        report["stock_slow_gaps"] = [symbol for symbol in domain if symbol not in bundle["scope"]]
        for symbol in report["stock_slow_gaps"]:
            report["stock_blocks"][symbol] = "SLOW_EVIDENCE_SCOPE_GAP"
        for symbol in domain:
            valid, result_hash = _valid_delta(delta_results.get(symbol), symbol=symbol,
                start=report["query_start"], end=report["query_end"], now=now)
            if not valid:
                report["stock_blocks"][symbol] = "RECENT_DISCLOSURE_GAP"
            report["delta_results"].append({"symbol": symbol, "status": "COVERED" if valid else "RECENT_DISCLOSURE_GAP",
                                             "result_sha256": result_hash})
        report["research_index_allowed"] = True
        stale = lag > 1
        if stale:
            # Preserve each disclosure failure rather than hiding it under stale.
            for symbol in domain:
                report["stock_blocks"].setdefault(symbol, "SLOW_EVIDENCE_STALE")
        disclosure_gap = any(reason == "RECENT_DISCLOSURE_GAP" for reason in report["stock_blocks"].values())
        reason = ("SLOW_EVIDENCE_STALE" if stale else "RECENT_DISCLOSURE_GAP" if disclosure_gap
                  else "SLOW_EVIDENCE_SCOPE_GAP" if report["stock_slow_gaps"] else None)
        report["contract_status"] = "STALE" if stale else "DEGRADED" if reason else "READY"
        result = finish(reason)
        if result["elapsed_seconds"] is None or result["elapsed_seconds"] > budget_seconds:
            result.update(contract_status="BLOCKED", reason_code="FAST_BUDGET_EXCEEDED", research_index_allowed=False)
        return result
    except PreparationError as exc:
        return finish(str(exc))
    except (OSError, ValueError, TypeError, OverflowError):
        return finish("LOCAL_INPUT_INVALID")
