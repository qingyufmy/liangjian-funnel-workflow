"""Deterministic A5 business counts, independent of model narrative."""
from collections import Counter, defaultdict
from datetime import datetime
import json
import re
import hashlib
from collections.abc import Mapping


_HEALTHY_STATES = frozenset({"OBSERVED_STABLE", "READY", "REQUIRED_INPUTS_READY", "RECOVERED", "WAITING_BAR_CLOSE"})
_ERROR_STATES = frozenset({"INPUT_UNAVAILABLE", "DATA_BLOCKED", "BLOCKED", "ERROR", "FAILED", "DEGRADED"})
_NORMAL_WAIT_REASONS = frozenset({"WAITING_BAR_CLOSE", "MINUTE_PUBLICATION_PENDING"})


def classify_operational_evidence(rows):
    """Classify evidence, not missing minutes: incidents never prove a gap.

    Publication aggregates can contain both healthy observations and errors.
    The original rows stay untouched and retain their independent evidence IDs.
    Recovery records describe recovery even when old reasons are retained.
    """
    incidents, unknown = [], []
    healthy_states = Counter()
    healthy_rows = 0
    for item in rows:
        kind = item.get("kind")
        base = {"evidence_id": item.get("evidence_id"), "run_id": item.get("run_id"),
                "time": item.get("time"), "plan_ids": list(item.get("plan_ids") or []),
                "minute_range": item.get("minute_range"), "gap_proven": False}
        known_job = {"JOB_TERMINATED": "TASK_SIGTERM", "NODE_PROCESS_RESTART": "PROCESS_RESTART", "JOB_FAILED": "TASK_FAILED"}
        if kind in known_job:
            incidents.append({**base, "cause": known_job[kind]})
            continue
        if kind == "MINUTE_PUBLICATION_AUDIT":
            raw_counts = item.get("state_counts", {})
            raw_reasons = item.get("reason_counts", {})
            malformed = not isinstance(raw_counts, Mapping) or not isinstance(raw_reasons, Mapping)
            raw_counts = raw_counts if isinstance(raw_counts, Mapping) else {}
            raw_reasons = raw_reasons if isinstance(raw_reasons, Mapping) else {}
            malformed = malformed or any(not isinstance(v, int) or isinstance(v, bool) or v < 0
                                         for v in (*raw_counts.values(), *raw_reasons.values()))
            counts = {str(k): v for k, v in raw_counts.items() if isinstance(v, int) and not isinstance(v, bool) and v > 0}
            reasons = {str(k): v for k, v in raw_reasons.items() if isinstance(v, int) and not isinstance(v, bool) and v > 0}
            errors = any(k not in _NORMAL_WAIT_REASONS for k in reasons)
            healthy = {k: v for k, v in counts.items() if k in _HEALTHY_STATES}
            pending = counts.get("PENDING_PUBLICATION", 0)
            if pending:
                # Exact writer codes: a first bounded wait is normal; only
                # consecutive pending escalates to UNCONFIRMED. Mixed daily
                # aggregates retain the explicitly counted normal subset.
                normal_pending = min(pending, reasons.get("MINUTE_PUBLICATION_PENDING", 0)) if errors else pending
                if normal_pending:
                    healthy["PENDING_PUBLICATION"] = normal_pending
            healthy_states.update(healthy)
            healthy_rows += bool(healthy)
            if errors or any(k in _ERROR_STATES for k in counts):
                incidents.append({**base, "cause": "DATA_SOURCE"})
            unclassified = set(counts) - set(healthy) - _ERROR_STATES
            # A pending row accompanied by decision errors is a blocked input,
            # not an unknown publication state or a normal closure wait.
            if errors:
                unclassified.discard("PENDING_PUBLICATION")
            if unclassified or malformed or item.get("unreadable_files", 0) or not counts and not errors:
                unknown.append({**base, "cause": "UNKNOWN_PENDING"})
            continue
        state = item.get("state") if isinstance(item.get("state"), str) else None
        if state in _HEALTHY_STATES:
            healthy_states[state] += 1
            healthy_rows += 1
        elif state in _ERROR_STATES:
            incidents.append({**base, "cause": "DATA_SOURCE" if kind in {"SOURCE_HEALTH_EVENT", "POSITION_DATA_HEALTH_EVENT"} else "UNKNOWN_PENDING"})
        else:
            unknown.append({**base, "cause": "UNKNOWN_PENDING"})
    return {"incident_candidates": incidents,
            "healthy_observations": {"record_count": healthy_rows, "observation_count": sum(healthy_states.values()),
                                     "state_counts": dict(sorted(healthy_states.items()))},
            "unknown_observations": unknown}


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
    operational_classification = classify_operational_evidence(operational)
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
        **operational_classification,
        "gap_cause_evidence": operational_classification["incident_candidates"],
        "gap_cause_evidence_semantics": "INCIDENT_CANDIDATES_ONLY_NOT_PROOF_OF_EXECUTION_GAP",
        "missing_execution_records": missing,
        "missing_records_not_automatically_market_failure": True,
        "ROTATION_GATE_EMPTY_plan_count": sum(row.get("ROTATION_GATE_EMPTY") is True for row in plans),
        "A2_REVIEW_DEGRADED_plan_count": sum(row.get("A2_REVIEW_DEGRADED") is True for row in plans),
        "report_status": "REPORT_GENERATING", "no_report_status": "NO_REPORT_TODAY"}


def compact_business_audit(value):
    """Group missing decisions without duplicating thousands of minute rows.

    The immutable facts retain every source ID; aggregate count/hash/locator
    lets the reviewer traverse back without growing the model prompt again.
    """
    result = {k:v for k,v in value.items() if k not in {"missing_execution_records", "gap_cause_evidence", "incident_candidates", "unknown_observations"}}
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
    for key in ("incident_candidates", "unknown_observations"):
        if key in value:
            rows = value[key]
            result[key + "_reference"] = {"count": len(rows),
                "raw_evidence_sha256": hashlib.sha256(json.dumps(rows,ensure_ascii=False,sort_keys=True,separators=(",", ":")).encode()).hexdigest(),
                "archive_locator": "business_audit." + key, "model_source": "operational_evidence"}
    return result
