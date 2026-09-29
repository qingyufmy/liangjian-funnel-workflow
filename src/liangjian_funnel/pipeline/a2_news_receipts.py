"""Isolated local receipts for explicitly authorized shadow reviews.

Atomic directory claim excludes concurrent logical requests. An ambiguous
in-flight outcome never retries automatically. No scheduler integration.
"""
import json
from pathlib import Path

from ..reporting import atomic_write_json
from .a2_news_context import _hash
from .a2_news_review import SYSTEM_PROMPT, validate_review


def review_with_receipt(packet, client, *, receipt_root: Path, allow_model_call=False):
    if packet.get("input_hash") != _hash({k: v for k, v in packet.items() if k != "input_hash"}):
        raise ValueError("NEWS_REVIEW_INPUT_HASH_MISMATCH")
    if not packet.get("events"):
        raise ValueError("NEWS_REVIEW_EMPTY_INPUT")
    messages = [{"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": json.dumps(packet, ensure_ascii=False)}]
    if sum(len(m["content"]) for m in messages) > 18000:
        raise ValueError("NEWS_REVIEW_INPUT_TOO_LARGE")
    identity = {"input_hash": packet["input_hash"], "prompt_hash": _hash(SYSTEM_PROMPT),
                "provider_hash": _hash(str(getattr(getattr(client, "settings", None), "model_base_url", type(client).__name__))),
                "model": "deepseek-v4-pro", "timeout_seconds": 90, "max_output_tokens": 3000}
    target = Path(receipt_root) / _hash(identity)
    response_path = target / "response.json"
    if response_path.exists():
        saved = json.loads(response_path.read_text(encoding="utf-8"))
        if saved.get("identity") != identity or saved.get("output_hash") != _hash(saved.get("output")):
            raise ValueError("NEWS_REVIEW_RECEIPT_CORRUPT")
        return {**validate_review(saved["output"], packet), "receipt_reused": True}
    if not allow_model_call:
        raise ValueError("NEWS_REVIEW_MODEL_CALL_NOT_AUTHORIZED")
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        target.mkdir()  # claim before network; never remove after timeout/crash
    except FileExistsError as exc:
        raise ValueError("NEWS_REVIEW_OUTCOME_UNKNOWN_NO_RETRY") from exc
    atomic_write_json(target / "request.json", {"identity": identity, "input": packet, "state": "CLAIMED"})
    # Exceptions leave the claim in place; provider retries inside the existing
    # client remain subject to its own budget. This is logical-call deduplication.
    response = client.complete(identity["model"], messages, stage="A2", **{
        k: identity[k] for k in ("prompt_hash", "input_hash", "timeout_seconds", "max_output_tokens")})
    atomic_write_json(response_path, {"identity": identity, "output": response.output,
                                     "output_hash": _hash(response.output)})
    # Preserve rejected model output too. Never overwrite it with a repair.
    validated = validate_review(response.output, packet)
    atomic_write_json(target / "validated.json", validated)
    return {**validated, "receipt_reused": False}
