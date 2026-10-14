"""Deterministic A5 business counts, independent of model narrative."""
from collections import Counter, defaultdict
from datetime import datetime
import json
import re
import hashlib
from collections.abc import Mapping


def business_audit(*, events, plans, delivery_counts, operational, replay):
    by_plan = {str(row.get("plan_id")): row for row in plans}
    reasons = defaultdict(Counter)
    buy_ids = set()
    malformed = 0
    for row in events:
        try:
            payload = json.loads(row.get("payload_json") or "{}") if isinstance(row.get("payload_json"), str) else row.get("payload_json") or row.get("payload") or {}
            if not isinstance(payload, Mapping):raise ValueError("PAYLOAD_NOT_MAPPING")
        except (ValueError, TypeError):
            payload = {};malformed += 1
        plan = by_plan.get(str(payload.get("plan_id")), {})
        profile = payload.get("strategy_profile") or plan.get("strategy_profile") or "UNKNOWN"
        strategy = payload.get("strategy") or {}
        strategy = strategy if isinstance(strategy, Mapping) else {}
        profile = strategy.get("strategy_profile") or profile
        first = row.get("reason_code") or "UNKNOWN"
        reasons[str(profile)][str(first)] += 1
        if row.get("action") == "BUY_SIGNAL" and row.get("effective"):
            buy_ids.add(str(row.get("event_id")))
    gap_causes = []
    for item in operational:
        kind = item.get("kind")
        cause = ("TASK_SIGTERM" if kind == "JOB_TERMINATED" else
                 "PROCESS_RESTART" if kind == "NODE_PROCESS_RESTART" else
                 "DATA_SOURCE" if kind in {"SOURCE_HEALTH_EVENT", "MINUTE_PUBLICATION_AUDIT"} else "UNKNOWN")
        gap_causes.append({"cause": cause, "evidence_id": item.get("evidence_id"),
                           "run_id": item.get("run_id"), "time": item.get("time"),
                           "gap_proven": False})
    missing = []
    for window in replay.get("trigger_windows", []):
        if window.get("classification") == "MISSING_EXECUTION_RECORD":
            attribution={"cause":"UNKNOWN", "cause_evidence_id":None, "run_id":None}
            for incident in operational:
                match=re.search(r'monitor-(\d{4}-\d{2}-\d{2})T(\d{2})-(\d{2})-(\d{2})-',str(incident.get('run_id') or ''))
                if match and incident.get('kind')=='JOB_TERMINATED':
                    try:
                        started=datetime.fromisoformat(f"{match[1]}T{match[2]}:{match[3]}:{match[4]}+00:00")
                        minute=datetime.fromisoformat(window['minute_end'])
                    except (ValueError, TypeError):continue
                    if started.astimezone(minute.tzinfo).replace(second=0,microsecond=0)==minute:
                        attribution={'cause':'TASK_SIGTERM','cause_evidence_id':incident.get('evidence_id'),'run_id':incident.get('run_id')}
                        break
            missing.append({"evidence_id": window.get("evidence_id"), "plan_id": window.get("plan_id"),
                            "minute_end": window.get("minute_end"), "cause": "UNKNOWN",
                            **attribution})
    return {"schema_version": "a5-business-audit/1", "evidence_id": "A5:BUSINESS_AUDIT",
        "target_buy_signals": {"minimum": 2, "maximum": 5, "actual": len(buy_ids), "target_is_not_quota": True},
        "first_reason_by_strategy": {k:dict(v) for k,v in sorted(reasons.items())},
        "event_rows": len(events), "unique_buy_signal_events": len(buy_ids),
        "malformed_event_payload_count": malformed,
        "notification_delivery_rows": delivery_counts["total"], "notification_status_counts": delivery_counts["statuses"],
        "notification_count_truncated": delivery_counts["truncated"],
        "gap_cause_evidence": gap_causes, "missing_execution_records": missing,
        "missing_records_not_automatically_market_failure": True,
        "ROTATION_GATE_EMPTY_plan_count": sum(row.get("ROTATION_GATE_EMPTY") is True for row in plans),
        "A2_REVIEW_DEGRADED_plan_count": sum(row.get("A2_REVIEW_DEGRADED") is True for row in plans),
        "report_status": "REPORT_GENERATING", "no_report_status": "NO_REPORT_TODAY"}


def compact_business_audit(value):
    """Group missing decisions without duplicating thousands of minute rows.

    The immutable facts retain every source ID; aggregate count/hash/locator
    lets the reviewer traverse back without growing the model prompt again.
    """
    result = {k:v for k,v in value.items() if k not in {"missing_execution_records", "gap_cause_evidence"}}
    grouped = defaultdict(list)
    for row in value.get("missing_execution_records", []):
        grouped[(row.get("plan_id"), row.get("cause"), row.get("run_id"), row.get("cause_evidence_id"))].append(row)
    groups=[]
    for key, rows in sorted(grouped.items(), key=lambda item: str(item[0])):
        digest=hashlib.sha256(json.dumps(rows,ensure_ascii=False,sort_keys=True,separators=(",", ":")).encode()).hexdigest()
        groups.append({"evidence_id":"A5B:GAP:"+digest[:16], "plan_id":key[0], "cause":key[1],
            "run_id":key[2], "cause_evidence_id":key[3], "count":len(rows),
            "first_minute":min(str(row.get("minute_end")) for row in rows),
            "last_minute":max(str(row.get("minute_end")) for row in rows), "raw_evidence_sha256":digest,
            "archive_locator":"business_audit.missing_execution_records"})
    result["missing_execution_record_groups"]=groups
    result["missing_execution_record_count"]=sum(row["count"] for row in groups)
    causes=value.get("gap_cause_evidence", [])
    result["gap_cause_evidence_reference"]={"count":len(causes),
        "raw_evidence_sha256":hashlib.sha256(json.dumps(causes,ensure_ascii=False,sort_keys=True,separators=(",", ":")).encode()).hexdigest(),
        "archive_locator":"business_audit.gap_cause_evidence", "model_source":"operational_evidence"}
    return result
