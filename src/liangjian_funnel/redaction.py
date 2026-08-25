from __future__ import annotations

import hashlib
import re
from typing import Any


SECRET_PATTERN = re.compile(r"(?i)(sk-[a-z0-9_-]{8,}|bearer\s+[a-z0-9._-]{8,}|x-api-key\s*[:=]\s*\S+)")
SENSITIVE_KEYS = {"authorization", "x-api-key", "api_key", "apikey", "token", "secret"}


def redact_text(value: str) -> str:
    return SECRET_PATTERN.sub("[REDACTED]", value)


def safe_error(exc: BaseException) -> str:
    # Exception messages may include request URLs or provider-generated text.
    # The exception class is sufficient for a capability report.
    return type(exc).__name__


def sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): ("[REDACTED]" if str(key).lower() in SENSITIVE_KEYS else sanitize(item))
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [sanitize(item) for item in value]
    if isinstance(value, tuple):
        return tuple(sanitize(item) for item in value)
    if isinstance(value, str):
        return redact_text(value)
    return value


def digest_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
