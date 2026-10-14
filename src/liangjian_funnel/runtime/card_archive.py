"""Pre-send sanitized evidence; never changes the card delivered to Lark."""
from datetime import datetime
import hashlib
import json
import re
from pathlib import Path
from collections.abc import Mapping
from ..redaction import sanitize
from ..reporting import atomic_write_json


def archive_card(*, directory: Path, card: Mapping, delivery_key: str,
                 source_run_id: str, plan_ids, trade_date: str, observed_at: datetime):
    def clean(value):
        if isinstance(value, dict):
            return {k: ("[REDACTED]" if any(x in k.lower() for x in ("webhook","token","secret","authorization","api_key")) else clean(v)) for k,v in value.items()}
        if isinstance(value, list):return [clean(v) for v in value]
        if isinstance(value, str):
            return re.sub(r"https://open\.(?:feishu\.cn|larksuite\.com)/open-apis/bot/v2/hook/[^\s]+", "[REDACTED_WEBHOOK]", value)
        return value
    safe = clean(sanitize(dict(card)))
    raw = json.dumps(safe, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    digest = hashlib.sha256(raw).hexdigest()
    key = hashlib.sha256(delivery_key.encode()).hexdigest()
    path = directory / trade_date / (key + "-" + digest[:16] + ".json")
    payload = {"schema_version": "lark-card-evidence/1", "card": safe,
        "sanitized_card_sha256": digest, "delivery_key_hash": key,
        "source_run_id": source_run_id, "plan_ids": list(plan_ids),
        "trade_date": trade_date, "observed_at": observed_at.isoformat(),
        "sent_card_mutated": False, "sanitized_not_wire_bytes": True}
    if path.exists():
        previous=json.loads(path.read_text(encoding="utf-8"))
        for field in ("card", "sanitized_card_sha256", "source_run_id", "plan_ids", "trade_date"):
            if previous.get(field)!=payload.get(field):raise ValueError("CARD_EVIDENCE_BINDING_CONFLICT")
    else:
        atomic_write_json(path, payload)
    return {"card_evidence_path": str(path), "sanitized_card_sha256": digest}
