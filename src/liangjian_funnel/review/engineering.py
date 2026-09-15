"""Bounded, deterministic operational evidence that market-only A5 missed."""
from datetime import datetime
import json


def operational_evidence(store, output_dir, *, cutoff):
    rows = []
    for item in store.list_notification_deliveries(kind="A4_MINUTE_SOURCE_HEALTH", limit=500):
        try:
            payload = json.loads(item.get("payload_json") or "{}")
            stamp = datetime.fromisoformat(str(item.get("created_at") or item.get("sent_at") or ""))
            if stamp.tzinfo is None or stamp > cutoff or stamp.date() != cutoff.date():
                continue
        except (ValueError, TypeError):
            continue
        rows.append({"evidence_id": f"ENGINEERING:ALERT:{len(rows)}", "kind": "SOURCE_HEALTH_EVENT",
                     "time": stamp.isoformat(), "state": payload.get("state"),
                     "reasons": payload.get("reasons", []), "symbols": payload.get("symbols", []),
                     "note": "告警事实不等于真实停机；需与冻结输入和执行动作交叉核对。"})
    log = output_dir / "node" / f"node-{cutoff.date()}.jsonl"
    if log.exists():
        with log.open(encoding="utf-8") as stream:
            for line in stream:
                try:
                    item = json.loads(line)
                    stamp = datetime.fromisoformat(item["timestamp"].replace("Z", "+00:00"))
                    if stamp > cutoff or item.get("stream") != "node":
                        continue
                    message = item.get("message", "")
                    # Do not copy raw messages: retain safe counters/identities,
                    # and do not classify the 15:00 finality block as downtime.
                    if "任务结束" not in message or not any(x in message for x in ("status=failed", "status=terminated")):
                        continue
                    rows.append({"evidence_id": f"ENGINEERING:JOB:{item.get('id')}",
                        "kind": "JOB_TERMINATED" if "status=terminated" in message else "JOB_FAILED",
                        "job": item.get("job"), "run_id": item.get("runId"),
                        "time": stamp.isoformat(), "reason": "TIMEOUT" if "status=terminated" in message else "NON_ZERO_EXIT"})
                except (ValueError, KeyError, TypeError):
                    continue
    return rows
