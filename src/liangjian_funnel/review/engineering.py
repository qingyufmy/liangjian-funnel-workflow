"""Bounded, deterministic operational evidence that market-only A5 missed."""
from datetime import datetime
from collections import Counter
import json
import re


_SAFE_JOB_REASON = re.compile(r'"reason_code"\s*:\s*"([A-Z0-9_]{1,80})"')


def operational_evidence(store, output_dir, *, cutoff):
    rows = []
    # Quiet publication waits remain reviewable even when no card was sent.
    quality_dir = output_dir / "monitor" / "data_quality" / cutoff.date().isoformat()
    states, reasons, symbols, files = Counter(), Counter(), set(), []
    malformed = 0
    for path in sorted(quality_dir.glob("*.json")):
        try:
            item = json.loads(path.read_text(encoding="utf-8"))
            stamp = datetime.fromisoformat(item["market_cutoff"])
            if stamp.tzinfo is None or stamp > cutoff or stamp.date() != cutoff.date():
                continue
            for symbol, observation in item["symbols"].items():
                states[observation["state"]] += 1
                if observation.get("decision_error"):
                    reasons[observation["decision_error"]] += 1
                    symbols.add(symbol)
            files.append(path.name)
        except (OSError, ValueError, KeyError, TypeError):
            malformed += 1
    if files or malformed:
        rows.append({"evidence_id": "ENGINEERING:PUBLICATION", "kind": "MINUTE_PUBLICATION_AUDIT",
                     "time": cutoff.isoformat(), "state_counts": dict(states), "reason_counts": dict(reasons),
                     "symbols": sorted(symbols), "minute_count": len(files), "unreadable_files": malformed,
                     "source_dir": str(quality_dir),
                     "note": "计数单位为股票×分钟，含仅归档股票；等待确认不等于停机或漏买，稳定不等于交易所最终定稿。"})
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
                     "event_category": "MARKET_DATA_QUALITY", "order_outcome": "NOT_AN_ORDER_RESULT",
                     "note": "这是行情质量提醒，不是买入信号或委托失败回报；不等于真实停机，需与冻结输入和执行动作交叉核对。"})
    for item in store.list_notification_deliveries(kind="A4_POSITION_DATA_HEALTH", limit=500):
        try:
            payload = json.loads(item.get("payload_json") or "{}")
            stamp = datetime.fromisoformat(str(item.get("created_at") or item.get("sent_at") or ""))
            if not isinstance(payload.get("positions"), dict) or stamp.tzinfo is None or stamp > cutoff or stamp.date() != cutoff.date():
                continue
        except (ValueError, TypeError):
            continue
        rows.append({"evidence_id": f"ENGINEERING:POSITION:{len(rows)}", "kind": "POSITION_DATA_HEALTH_EVENT",
                     "time": stamp.isoformat(), "state": payload.get("state"), "positions": payload["positions"],
                     "note": "持仓风险输入受限或恢复；不是成交，也不能推断发生漏卖。"})
    log = output_dir / "node" / f"node-{cutoff.date()}.jsonl"
    if log.exists():
        reason_by_run = {}
        with log.open(encoding="utf-8") as stream:
            for line in stream:
                try:
                    item = json.loads(line)
                    stamp = datetime.fromisoformat(item["timestamp"].replace("Z", "+00:00"))
                    if stamp > cutoff:
                        continue
                    message = item.get("message", "")
                    run_id = str(item.get("runId") or "")
                    if item.get("stream") == "stdout" and run_id:
                        match = _SAFE_JOB_REASON.search(str(message))
                        if match:
                            reason_by_run[run_id] = match.group(1)
                    if item.get("stream") != "node":
                        continue
                    # Do not copy raw messages: retain safe counters/identities,
                    # and do not classify the 15:00 finality block as downtime.
                    if "任务结束" not in message or not any(x in message for x in ("status=failed", "status=terminated")):
                        continue
                    rows.append({"evidence_id": f"ENGINEERING:JOB:{item.get('id')}",
                        "kind": "JOB_TERMINATED" if "status=terminated" in message else "JOB_FAILED",
                        "job": item.get("job"), "run_id": item.get("runId"),
                        "time": stamp.isoformat(), "reason": (
                            "TIMEOUT" if "status=terminated" in message
                            else reason_by_run.get(run_id, "NON_ZERO_EXIT"))})
                except (ValueError, KeyError, TypeError):
                    continue
    return rows
