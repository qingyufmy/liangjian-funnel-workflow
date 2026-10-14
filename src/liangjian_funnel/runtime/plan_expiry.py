"""Publication expiry is authoritative; preserve the research source date."""
from datetime import datetime
from typing import Any, Mapping


def normalize_publication_expiry(payload: Mapping[str, Any], expires_at: datetime) -> dict[str, Any]:
    if expires_at.tzinfo is None or expires_at.utcoffset() is None:
        raise ValueError("PUBLICATION_EXPIRY_MUST_BE_AWARE")
    result = dict(payload)
    original = result.get("plan_expiry")
    if original is not None:
        result.setdefault("research_source_expiry", original)
    result["plan_expiry"] = expires_at.isoformat()
    return result
