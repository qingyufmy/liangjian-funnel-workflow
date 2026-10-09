"""Read-only September corporate-action evidence, never a factor implementation.

Uses the existing HithinkClient transport/retry/throttle, NOT its paginator.
Only allowlisted typed values leave the provider response. Raw bytes are hashed
but never persisted. No RuntimeStore, DB, model, notifier or production writes.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, time
from decimal import Decimal
import hashlib
import inspect
import json
import math
from pathlib import Path
import re
from typing import Any
from zoneinfo import ZoneInfo

from liangjian_funnel.pipeline.data_source import HithinkClient
from liangjian_funnel.settings import Settings

ENDPOINT = "/api/a-share/corporate-actions/adjustment-factors"
DOC_URL = "https://fuyao.aicubes.cn/docs/api-reference/corporate-actions/"
START, END = "2026-09-01", "2026-09-30"
SHANGHAI = ZoneInfo("Asia/Shanghai")
SYMBOL = re.compile(r"^[0-9]{6}\.(SH|SZ|BJ)$")
HASH = re.compile(r"^[0-9a-f]{64}$")
KNOWN_FIELDS = {"code", "data", "item", "thscode", "ticker", "ex_date_ms",
                "dividend_per_share", "per_share_bonus", "event_type", "record_date",
                "adjust_factor", "rights_issue_ratio", "rights_issue_price", "factor_basis"}


def digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def number(value: Any) -> bool:
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value) and value >= 0)


def validate_reference(payload: Any, *, expected_count: int = 20) -> list[dict]:
    if (not isinstance(payload, dict) or payload.get("evidence_type") !=
            "OFFICIAL_ANNOUNCEMENT_REVERSE_LOOKUP_NOT_ENDPOINT_RESPONSE"):
        raise ValueError("REFERENCE_EVIDENCE_TYPE_INVALID")
    rows = payload.get("records")
    if not isinstance(rows, list) or len(rows) != expected_count:
        raise ValueError("REFERENCE_COUNT_INVALID")
    seen = set()
    for row in rows:
        if not isinstance(row, dict) or not SYMBOL.fullmatch(str(row.get("symbol", ""))):
            raise ValueError("REFERENCE_SYMBOL_INVALID")
        if row["symbol"] in seen:
            raise ValueError("REFERENCE_SYMBOL_DUPLICATE")
        seen.add(row["symbol"])
        if not HASH.fullmatch(str(row.get("pdf_sha256", ""))):
            raise ValueError("REFERENCE_HASH_INVALID")
        try:
            ex_date = date.fromisoformat(row["ex_date"])
        except (KeyError, ValueError, TypeError):
            raise ValueError("REFERENCE_DATE_INVALID") from None
        if not START <= ex_date.isoformat() <= END:
            raise ValueError("REFERENCE_DATE_OUTSIDE_WINDOW")
        url = row.get("announcement_url", "")
        if not re.fullmatch(r"https://static\.cninfo\.com\.cn/finalpage/\d{4}-\d{2}-\d{2}/\d+\.PDF", url):
            raise ValueError("REFERENCE_URL_INVALID")
        for field in ("cash_per_share", "capital_reserve_transfer_per_share",
                      "price_reference_cash_per_share"):
            if row.get(field) is not None and not number(row[field]):
                raise ValueError("REFERENCE_AMOUNT_INVALID")
        if row.get("differential_distribution") is not None and type(row["differential_distribution"]) is not bool:
            raise ValueError("REFERENCE_DIFFERENTIAL_INVALID")
    return rows


def _comparison(actual: Any, expected: Any) -> str:
    if expected is None:
        return "UNKNOWN_REFERENCE_FIELD"
    return ("MATCH" if abs(Decimal(str(actual)) - Decimal(str(expected))) <= Decimal("0.000000001")
            else "CONFLICT")


def inspect_response(response: Any, reference: dict) -> dict:
    """All-row validation; invalid envelopes cannot be upgraded by a good first row."""
    out = {"http_status": int(response.status_code), "response_bytes": len(response.content),
           "response_sha256": digest(response.content), "schema_valid": False,
           "date_match_status": "UNKNOWN", "reason_code": "INVALID_ENVELOPE",
           "safe_field_names": [], "rows": [], "factor_contract_status": "UNVERIFIED"}
    if response.status_code != 200:
        out["reason_code"] = "HTTP_ERROR"
        return out
    try:
        envelope = response.json()
    except (ValueError, UnicodeError):
        out["reason_code"] = "INVALID_JSON"
        return out
    if not isinstance(envelope, dict):
        return out
    fields = set(envelope) & KNOWN_FIELDS
    # Provider strings, unknown fields/values, errors, request IDs and headers
    # are intentionally NOT copied, even if they masquerade as credentials.
    if type(envelope.get("code")) not in (int, str):
        return out
    if envelope["code"] not in (0, "0"):
        out["reason_code"] = "BUSINESS_ERROR"
        return out
    data = envelope.get("data")
    if not isinstance(data, dict):
        return out
    fields.update(set(data) & KNOWN_FIELDS)
    if data.get("thscode") != reference["symbol"] or data.get("ticker") != reference["symbol"][:6]:
        out["reason_code"] = "ENVELOPE_SYMBOL_CONFLICT"
        return out
    items = data.get("item")
    if not isinstance(items, list):
        return out
    dates = set()
    rows = []
    for item in items:
        if not isinstance(item, dict):
            out["reason_code"] = "ROW_INVALID"
            return out
        fields.update(set(item) & KNOWN_FIELDS)
        if item.get("ticker") != reference["symbol"][:6]:
            out["reason_code"] = "ROW_SYMBOL_CONFLICT"
            return out
        timestamp = item.get("ex_date_ms")
        if type(timestamp) is not int:
            out["reason_code"] = "ROW_DATE_INVALID"
            return out
        try:
            moment = datetime.fromtimestamp(timestamp / 1000, SHANGHAI)
        except (ValueError, OSError, OverflowError):
            out["reason_code"] = "ROW_DATE_INVALID"
            return out
        if moment.time() != time(0) or not START <= moment.date().isoformat() <= END:
            out["reason_code"] = "ROW_DATE_OUTSIDE_WINDOW_OR_NOT_MIDNIGHT"
            return out
        if timestamp in dates:
            out["reason_code"] = "DUPLICATE_EVENT_DATE"
            return out
        dates.add(timestamp)
        if not all(number(item.get(field)) for field in ("dividend_per_share", "per_share_bonus")):
            out["reason_code"] = "ROW_AMOUNT_INVALID"
            return out
        rows.append({"ticker": item["ticker"], "ex_date_ms": timestamp,
                     "ex_date": moment.date().isoformat(),
                     "dividend_per_share": item["dividend_per_share"],
                     "per_share_bonus": item["per_share_bonus"]})
    out.update(schema_valid=True, reason_code="OK", rows=rows,
               safe_field_names=sorted(fields), row_count=len(rows),
               safe_rows_sha256=digest(canonical(rows)))
    matches = [row for row in rows if row["ex_date"] == reference["ex_date"]]
    out["date_match_status"] = "MATCH" if len(matches) == 1 else "CONFLICT_MISSING_EXPECTED_DATE"
    out["extra_event_dates"] = [row["ex_date"] for row in rows if row not in matches]
    if matches:
        item = matches[0]
        out["cash_comparison"] = _comparison(item["dividend_per_share"], reference.get("cash_per_share"))
        out["price_reference_cash_comparison"] = _comparison(
            item["dividend_per_share"], reference.get("price_reference_cash_per_share"))
        out["bonus_comparison"] = "UNKNOWN_NO_VERIFIED_STOCK_BONUS_REFERENCE"
        if reference.get("capital_reserve_transfer_per_share") is not None:
            out["bonus_comparison"] = "UNKNOWN_CAPITAL_RESERVE_TRANSFER_IS_NOT_VERIFIED_STOCK_BONUS"
        if (reference.get("price_reference_cash_per_share") is not None and
                reference.get("cash_per_share") != reference["price_reference_cash_per_share"]):
            out["factor_contract_status"] = "CONFLICT_DISTRIBUTED_CASH_VS_PRICE_REFERENCE_CASH"
        elif reference.get("differential_distribution") is True:
            out["factor_contract_status"] = "UNKNOWN_DIFFERENTIAL_PRICE_REFERENCE"
    return out


def run_probe(settings: Settings, references: list[dict], *, client: Any = None) -> dict:
    if settings.hithink_min_request_interval_seconds < 0.5:
        raise ValueError("REFUSE_REDUCED_RATE_LIMIT")
    out = {"schema_version": "wp5-corporate-action-probe/1.0", "endpoint": ENDPOINT,
           "official_docs_url": DOC_URL, "window": {"from": START, "to": END},
           "observed_at": datetime.now(SHANGHAI).isoformat(),
           "mode": "ISOLATED_READONLY_NO_DB_NO_MODEL_NO_NOTIFICATION",
           "reference_evidence_is_not_endpoint_response": True,
           "min_request_interval_seconds": settings.hithink_min_request_interval_seconds,
           "timeout_seconds": settings.timeout_seconds, "trust_env": False,
           "paginator_used": False,
           "transport_implementation_sha256": digest(inspect.getsource(HithinkClient._get_with_retries).encode()),
           "script_sha256": digest(Path(__file__).read_bytes()), "records": []}
    if settings.hithink_api_key is None:
        out.update(status="BLOCKED", reason_code="HITHINK_API_KEY_MISSING", exit_code=3)
        return out
    owns = client is None
    transport_client = client or HithinkClient(settings)
    try:
        for reference in references:
            record = {"symbol": reference["symbol"], "expected_ex_date": reference["ex_date"],
                      "reference_pdf_sha256": reference["pdf_sha256"],
                      "reference_announcement_url": reference["announcement_url"],
                      "observed_at": datetime.now(SHANGHAI).isoformat()}
            try:
                outcome = transport_client._get_with_retries(ENDPOINT, {
                    "thscode": reference["symbol"], "from": START, "to": END})
                record["transport_attempts"] = int(outcome.metadata.get("attempts", 1))
                record["http_response_received"] = outcome.http_status is not None
                if outcome.response is None:
                    reason = outcome.reason_code
                    record.update(http_status=outcome.http_status, schema_valid=False,
                                  response_sha256=None, response_body_available=False,
                                  date_match_status="UNKNOWN", reason_code=reason if reason in
                                  {"REQUEST_FAILED", "HTTP_ERROR", "RATE_LIMITED"} else "TRANSPORT_FAILED")
                else:
                    record.update(inspect_response(outcome.response, reference))
            except Exception:
                # No exception text: it can contain URLs, headers or provider secrets.
                record.update(schema_valid=False, date_match_status="UNKNOWN",
                              reason_code="UNEXPECTED_PROBE_ERROR")
            out["records"].append(record)
    finally:
        if owns:
            transport_client.close()
    counts = Counter(record["reason_code"] for record in out["records"])
    success = all(record.get("schema_valid") and record.get("date_match_status") == "MATCH"
                  and record.get("cash_comparison") != "CONFLICT" for record in out["records"])
    out.update(status="OBSERVED" if success else "OBSERVED_WITH_GAPS", exit_code=0 if success else 2,
               response_count=sum(row.get("http_response_received", False) for row in out["records"]),
               hashed_response_count=sum(bool(row.get("response_sha256")) for row in out["records"]),
               schema_valid_count=sum(row.get("schema_valid", False) for row in out["records"]),
               date_match_count=sum(row.get("date_match_status") == "MATCH" for row in out["records"]),
               reason_counts=dict(counts), factor_contract_status="UNVERIFIED_NO_FACTOR_IMPLEMENTATION")
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--env-root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit("REFUSE_OVERWRITE")
    raw = args.reference.read_bytes()
    references = validate_reference(json.loads(raw))
    settings = Settings.from_env(root=args.env_root)
    report = run_probe(settings, references)
    report["reference_file_sha256"] = digest(raw)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False).encode("utf-8") + b"\n"
    with args.output.open("xb") as stream:
        stream.write(body)
    print(json.dumps({"status": report["status"], "exit_code": report["exit_code"],
                      "response_count": report.get("response_count", 0),
                      "schema_valid_count": report.get("schema_valid_count", 0),
                      "date_match_count": report.get("date_match_count", 0),
                      "artifact_sha256": digest(body)}))
    return report["exit_code"]


if __name__ == "__main__":
    raise SystemExit(main())
