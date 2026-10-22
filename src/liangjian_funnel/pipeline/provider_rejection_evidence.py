"""Durable, fully redacted evidence for received HTTP429/5xx rejections.

No retry, request-body capture, database, claim or marker writes. The caller
keeps its original model failure classification and must abort on evidence
failure rather than silently claiming a recorded rejection.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qsl, quote, quote_plus, urlencode, urlsplit, urlunsplit
from zoneinfo import ZoneInfo
import hashlib
import html
import json
import os
import re
import uuid
import time

import httpx

REDACTED = "[REDACTED]"
_URL = re.compile(r"https?://[^\s<>\"']+", re.IGNORECASE)
_AUTH = re.compile(r"\b(?:Bearer|Basic)\s+[A-Za-z0-9._~+/=:-]+", re.IGNORECASE)
_SK = re.compile(r"\bsk-[A-Za-z0-9_-]{8,}\b")
_SECRET_KEY = r"(?:authorization|proxy[-_ ]?authorization|api[-_ ]?key|access[-_ ]?key|access[-_ ]?token|refresh[-_ ]?token|id[-_ ]?token|client[-_ ]?secret|password|passwd|secret|credential|token|cookie|set[-_ ]?cookie)"
_QUOTED_PAIR = re.compile(r"(?P<prefix>[\"']?" + _SECRET_KEY + r"[\"']?\s*[:=]\s*)(?P<quote>[\"'])(?:\\.|[^\r\n])*?(?P=quote)", re.IGNORECASE)
_BARE_PAIR = re.compile(r"(?P<prefix>\b" + _SECRET_KEY + r"\s*[:=]\s*)(?![\"'])(?:Bearer\s+|Basic\s+)?[^\s,;&}\]<>]+", re.IGNORECASE)
_ECHO_KEYS = {"messages", "prompt", "input", "inputs", "contents", "instructions", "systemprompt", "systeminstruction", "requestbody", "requestpayload", "requestmessages", "requestprompt"}
_ECHO_PAIR = re.compile(r"(?P<prefix>[\"']?(?:messages|prompt|inputs?|contents|instructions|system[-_ ]?(?:prompt|instruction)|request[-_ ]?(?:body|payload|messages|prompt))[\"']?\s*[:=]\s*)", re.IGNORECASE)
_QUERY_SECRET = {"key", "auth", "code", "sig", "signature", "sessionid", "credential", "credentials"}


class ProviderRejectionEvidenceError(RuntimeError):
    """Safe local evidence failure; does not replace the provider reason code."""

    def __init__(self, reason_code: str, *, status_code: int | None = None,
                 body_sha256: str | None = None, receipt_path: str | None = None):
        self.reason_code = reason_code
        self.status_code = status_code
        self.body_sha256 = body_sha256
        self.receipt_path = receipt_path
        super().__init__(reason_code)


def _normalized(key: str) -> str:
    return re.sub(r"[^a-z0-9]", "", key.lower())


def _sensitive(key: str) -> bool:
    key = _normalized(key)
    return (key in {"token", "key", "auth", "password", "passwd", "pwd", "secret", "cookie", "setcookie", "authorization", "proxyauthorization", "credentials"}
            or any(part in key for part in ("apikey", "accesskey", "accesstoken", "refreshtoken", "idtoken", "securitytoken", "bearertoken", "authtoken", "clientsecret", "secretkey", "privatekey", "signingkey", "sessionkey", "credential", "password")))


def _secret_values(settings, supplied: Iterable[str]) -> tuple[str, ...]:
    result = set()
    values = [supplied] if isinstance(supplied, str) else list(supplied)
    model_key = getattr(settings, "model_api_key", None)
    if model_key is not None:
        values.append(model_key)
    for value in values:
        if hasattr(value, "get_secret_value"):
            value = value.get_secret_value()
        if value is None:
            continue
        if not isinstance(value, str):
            raise ValueError("KNOWN_SECRET_TYPE_INVALID")
        if not value:
            continue
        result.update((value, quote(value, safe=""), quote_plus(value), html.escape(value),
                       json.dumps(value, ensure_ascii=True)[1:-1], json.dumps(value, ensure_ascii=False)[1:-1]))
    return tuple(sorted(result, key=len, reverse=True))


def _replace_known(text: str, secrets: tuple[str, ...]) -> str:
    for secret in secrets:
        text = text.replace(secret, REDACTED)
    return text


def _redact_url(match: re.Match[str], secrets: tuple[str, ...]) -> str:
    try:
        p = urlsplit(match.group(0))
        host = p.netloc.rsplit("@", 1)[-1]
        if "@" in p.netloc:
            host = REDACTED + "@" + host
        query = []
        for key, value in parse_qsl(p.query, keep_blank_values=True):
            sensitive = _sensitive(key) or _normalized(key) in _QUERY_SECRET or _normalized(key).endswith(("token", "signature"))
            query.append((key, REDACTED if sensitive else _redact_text(value, secrets)))
        return urlunsplit((p.scheme, host, _replace_known(p.path, secrets),
                           urlencode(query), REDACTED if p.fragment else ""))
    except (ValueError, UnicodeError, RecursionError):
        return "[REDACTED_URL]"


def _redact_text(text: str, secrets: tuple[str, ...]) -> str:
    # URL query parsing precedes generic key=value scrubbing, so percent-encoded
    # authentication and userinfo cannot survive inside an echoed URL.
    text = _URL.sub(lambda m: _redact_url(m, secrets), text)
    # Some gateways put a JSON request echo inside otherwise plain error text.
    # Remove the whole echoed value, not merely its first token/line fragment.
    cursor = 0
    parts = []
    for match in _ECHO_PAIR.finditer(text):
        if match.start() < cursor:
            continue
        start = match.end()
        try:
            _, consumed = json.JSONDecoder().raw_decode(text[start:])
            end = start + consumed
        except (ValueError, RecursionError):
            end = text.find("\n", start)
            if end < 0:
                end = len(text)
        parts.extend((text[cursor:start], json.dumps(REDACTED)))
        cursor = end
    if parts:
        text = "".join(parts) + text[cursor:]
    text = _QUOTED_PAIR.sub(lambda m: m.group("prefix") + m.group("quote") + REDACTED + m.group("quote"), text)
    text = _BARE_PAIR.sub(lambda m: m.group("prefix") + REDACTED, text)
    text = _AUTH.sub(REDACTED, text)
    text = _SK.sub(REDACTED, text)
    return _replace_known(text, secrets)


def _redact(value, secrets: tuple[str, ...]):
    if isinstance(value, Mapping):
        return {_redact_text(str(key), secrets): REDACTED if _sensitive(str(key)) or _normalized(str(key)) in _ECHO_KEYS
                else _redact(item, secrets) for key, item in value.items()}
    if isinstance(value, list):
        return [_redact(item, secrets) for item in value]
    if isinstance(value, str):
        return _redact_text(value, secrets)
    return value


def _body(raw: bytes, response: httpx.Response):
    encoding = response.encoding or "utf-8"
    text = raw.decode(encoding, errors="strict")
    try:
        def reject_constant(_):
            raise ValueError("NON_JSON_NUMBER")
        return json.loads(text, parse_constant=reject_constant), "json", encoding
    except (json.JSONDecodeError, ValueError):
        return text, "text", encoding


def _headers(response: httpx.Response, secrets: tuple[str, ...], *, all_headers=False) -> dict[str, list[str]]:
    headers = {}
    for name, value in response.headers.multi_items():
        name = name.lower()
        if (all_headers or name == "retry-after" or name.startswith("x-ratelimit") or "quota" in name
                or "request-id" in name or "requestid" in name):
            headers.setdefault(_redact_text(name, secrets), []).append(REDACTED if _sensitive(name) else _redact_text(value, secrets))
    return headers


def _request_metadata(response: httpx.Response, secrets: tuple[str, ...]) -> dict:
    try:
        request = response.request
    except RuntimeError:
        return {"method": None, "url": None}
    # Deliberately never access request.content/messages or request.headers.
    return {"method": request.method, "url": _redact_text(str(request.url), secrets)}


def _fsync_directory(directory: Path) -> None:
    if hasattr(os, "O_DIRECTORY"):
        fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def persist_provider_rejection(response: httpx.Response, *, settings, model: str,
                               stage: str | None, session: str | None,
                               input_hash: str | None, prompt_hash: str | None,
                               attempt: int, known_secrets: Iterable[str] = (),
                               captured_at: datetime | None = None, all_headers: bool = False,
                               deadline: float | None = None, clock=time.monotonic) -> dict:
    """Persist one redacted receipt, or raise a safe evidence-specific error.

    Returns receipt path/SHA and original body SHA, never a raw response body.
    Partial redacted files remain on write/fsync failure as failed evidence;
    no success receipt is returned. Original HTTP failure attribution is the
    caller's responsibility and is not rewritten by this helper.
    """
    status = response.status_code
    if status not in (403, 429) and not 500 <= status <= 599:
        raise ProviderRejectionEvidenceError("PROVIDER_REJECTION_STATUS_NOT_ELIGIBLE", status_code=status)
    body_sha = None
    path = None
    try:
        if not isinstance(model, str) or not model or type(attempt) is not int or attempt < 1:
            raise ValueError("REQUEST_BINDING_INVALID")
        for value in (stage, session, input_hash, prompt_hash):
            if value is not None and not isinstance(value, str):
                raise ValueError("REQUEST_BINDING_INVALID")
        at = captured_at or datetime.now(ZoneInfo("Asia/Shanghai"))
        if not isinstance(at, datetime) or at.utcoffset() is None:
            raise ValueError("CAPTURE_TIME_INVALID")
        secrets = _secret_values(settings, known_secrets)
        if deadline is None:
            raw = response.read()
        else:
            chunks = []
            for chunk in response.iter_bytes():
                if clock() >= deadline:
                    raise TimeoutError('PROVIDER_REJECTION_CAPTURE_DEADLINE')
                chunks.append(chunk)
            if clock() >= deadline:
                raise TimeoutError('PROVIDER_REJECTION_CAPTURE_DEADLINE')
            raw = b''.join(chunks)
        body_sha = hashlib.sha256(raw).hexdigest()
        original, body_format, encoding = _body(raw, response)
        redacted = _redact(original, secrets)
        error = redacted.get("error", redacted) if isinstance(redacted, Mapping) else {}
        if not isinstance(error, Mapping):
            error = {"message": error}
        receipt = dict(schema_version="provider-rejection-evidence/1", captured_at=at.isoformat(),
                       status_code=status, body_sha256=body_sha, body_bytes=len(raw),
                       body_format=body_format, body_encoding=encoding,
                       error={key: error.get(key) for key in ("type", "code", "message")}, body=redacted,
                       response_headers=_headers(response, secrets, all_headers=all_headers),
                       request_binding=_redact(dict(model=model, stage=stage, session=session,
                           input_hash=input_hash, prompt_hash=prompt_hash, attempt=attempt,
                           **_request_metadata(response, secrets)), secrets),
                       request_messages_saved=False, request_authorization_saved=False,
                       automatic_retry_performed=False)
        payload = json.dumps(receipt, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    except Exception:
        raise ProviderRejectionEvidenceError("PROVIDER_REJECTION_CAPTURE_OR_REDACTION_FAILED", status_code=status,
                                             body_sha256=body_sha) from None
    try:
        root = Path(settings.root).resolve()
        output = Path(settings.workflow_output_dir)
        if not output.is_absolute():
            output = root / output
        if output.resolve() != output or not output.is_relative_to(root):
            raise ValueError("WORKFLOW_OUTPUT_SCOPE_INVALID")
        directory = output / "provider_rejections" / at.astimezone(ZoneInfo("Asia/Shanghai")).date().isoformat()
        if directory.resolve() != directory:
            raise ValueError("RECEIPT_DIRECTORY_SYMLINK_INVALID")
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        if directory.resolve() != directory:
            raise ValueError("RECEIPT_DIRECTORY_SYMLINK_INVALID")
        name = "rejection-" + at.strftime("%H%M%S%f") + "-" + uuid.uuid4().hex + ".json"
        path = directory / name
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        _fsync_directory(directory)
        digest = hashlib.sha256(payload).hexdigest()
        if path.stat().st_size != len(payload) or hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            raise OSError("RECEIPT_WRITE_VERIFICATION_FAILED")
    except Exception:
        raise ProviderRejectionEvidenceError("PROVIDER_REJECTION_PERSISTENCE_FAILED", status_code=status,
                                             body_sha256=body_sha, receipt_path=str(path) if path else None) from None
    return dict(receipt_path=str(path), receipt_sha256=digest, body_sha256=body_sha,
                status_code=status, captured_at=at.isoformat(), error=receipt["error"],
                response_headers=receipt["response_headers"], complete_redacted_body_preserved=True)
