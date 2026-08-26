"""Fail-closed OpenAI-compatible client used by the research lanes."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

import httpx

from ..probes.models import THINKING_VARIANTS
from ..redaction import digest_text
from ..settings import ALL_MODELS, Settings


# Long production prompts have completed reliably in bounded low-effort mode,
# while probe-verified full thinking can consume the entire stage deadline.
# Keep the probe variants as same-model fallbacks after strict JSON failures.
PRODUCTION_THINKING_VARIANTS: tuple[tuple[str, dict[str, Any]], ...] = (
    ("reasoning_effort_low", {"reasoning_effort": "low"}),
    *THINKING_VARIANTS,
)
NO_THINKING_VARIANTS: tuple[tuple[str, dict[str, Any]], ...] = (("thinking_disabled", {}),)


class ModelClientError(RuntimeError):
    """Stable, non-sensitive model-call error."""

    def __init__(
        self,
        reason_code: str,
        *,
        status_code: int | None = None,
        attempts: int | None = None,
        diagnostics: Mapping[str, Any] | None = None,
    ):
        self.reason_code = reason_code
        self.status_code = status_code
        self.attempts = attempts
        self.diagnostics = dict(diagnostics or {})
        suffix = f" status={status_code}" if status_code is not None else ""
        super().__init__(f"model client {reason_code}{suffix}")


class StrictJSONError(ModelClientError):
    def __init__(
        self,
        reason_code: str = "STRICT_JSON_INVALID",
        *,
        attempts: int | None = None,
        diagnostics: Mapping[str, Any] | None = None,
    ):
        super().__init__(reason_code, attempts=attempts, diagnostics=diagnostics)


class ModelNetworkError(ModelClientError):
    def __init__(self, reason_code: str = "NETWORK_RETRY_EXHAUSTED", *, attempts: int | None = None):
        super().__init__(reason_code, attempts=attempts)


class ModelHTTPError(ModelClientError):
    def __init__(self, reason_code: str, *, status_code: int, attempts: int | None = None):
        super().__init__(reason_code, status_code=status_code, attempts=attempts)


@dataclass(frozen=True, slots=True)
class ModelCallResult:
    """Safe model result; no reasoning text or raw response is retained."""

    model: str
    output: dict[str, Any]
    prompt_hash: str | None
    input_hash: str | None
    latency_ms: int
    attempts: int
    thinking_variant: str
    reasoning_tokens: int | None = None
    output_hash: str = ""

    def __post_init__(self) -> None:
        if not self.output_hash:
            object.__setattr__(self, "output_hash", digest_text(_canonical_json(self.output)))

    @property
    def json(self) -> dict[str, Any]:
        return self.output

    @property
    def data(self) -> dict[str, Any]:
        return self.output


class OpenAICompatibleModelClient:
    """Minimal JSON-only client for the configured gateway.

    Retries are deliberately local to one request.  A 429 is retried with the
    same model and no circuit breaker, and no request is silently downgraded to
    another model. Thinking is selected explicitly by the caller: enabled
    research clients start with bounded ``reasoning_effort=low`` and then try
    capability-probe variants, while disabled clients send no thinking fields
    and have no thinking-parameter fallback path.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        max_attempts: int = 3,
        retry_backoff_seconds: float = 0.25,
        thinking_enabled: bool = True,
    ):
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        self.settings = settings
        self.transport = transport
        self.sleep = sleep
        self.monotonic = monotonic
        self.max_attempts = max_attempts
        self.retry_backoff_seconds = max(0.0, float(retry_backoff_seconds))
        self.thinking_enabled = bool(thinking_enabled)
        self.thinking_variants = PRODUCTION_THINKING_VARIANTS if self.thinking_enabled else NO_THINKING_VARIANTS

    def complete(
        self,
        model: str,
        messages: Sequence[Mapping[str, Any]],
        *,
        prompt_hash: str | None = None,
        input_hash: str | None = None,
        snapshot_id: str | None = None,
        stage: str | None = None,
    ) -> ModelCallResult:
        del snapshot_id, stage  # metadata is retained by the pipeline audit, not sent as secrets
        if model not in ALL_MODELS:
            raise ModelClientError("MODEL_NOT_ALLOWED")
        if self.settings.model_api_key is None:
            raise ModelClientError("MODEL_API_KEY_MISSING")
        if not isinstance(messages, Sequence) or isinstance(messages, (str, bytes)) or not messages:
            raise ModelClientError("MESSAGES_INVALID")

        safe_messages = [dict(message) for message in messages]
        started_all = time.perf_counter()
        overall_deadline = self.monotonic() + self.settings.model_timeout_seconds
        total_attempts = 0
        last_variant = self.thinking_variants[0][0]
        strict_json_retry = False
        last_strict_error: StrictJSONError | None = None
        with httpx.Client(
            base_url=self.settings.model_base_url,
            timeout=self.settings.model_timeout_seconds,
            transport=self.transport,
            trust_env=False,
            headers={
                "Authorization": f"Bearer {self.settings.model_api_key.get_secret_value()}",
                "Accept": "text/event-stream",
                "Content-Type": "application/json",
            },
        ) as client:
            for variant_id, thinking_payload in self.thinking_variants:
                last_variant = variant_id
                variant_attempts = 0
                while variant_attempts < self.max_attempts:
                    variant_attempts += 1
                    total_attempts += 1
                    if self.monotonic() >= overall_deadline:
                        raise ModelNetworkError("MODEL_TOTAL_DEADLINE_EXCEEDED", attempts=total_attempts)
                    attempt_deadline = overall_deadline
                    body: dict[str, Any] = {
                        "model": model,
                        "temperature": 0,
                        "max_tokens": self.settings.model_max_output_tokens,
                        "messages": [
                            *safe_messages,
                            *(
                                [{
                                    "role": "user",
                                    "content": (
                                        "TRANSPORT_JSON_RETRY\n"
                                        "Regenerate the complete answer from the original request. "
                                        "Return exactly one valid JSON object, with no Markdown fence, "
                                        "prefix, suffix, commentary, or truncated fields."
                                    ),
                                }]
                                if strict_json_retry
                                else []
                            ),
                        ],
                        "response_format": {"type": "json_object"},
                        "stream": True,
                        **thinking_payload,
                    }
                    try:
                        with client.stream("POST", "/chat/completions", json=body) as response:
                            status = response.status_code
                            if status == 429 or status >= 500:
                                if variant_attempts < self.max_attempts:
                                    self._backoff(
                                        variant_attempts,
                                        retry_after=response.headers.get("Retry-After"),
                                        deadline=overall_deadline,
                                    )
                                    continue
                                reason = "RATE_LIMIT_RETRY_EXHAUSTED" if status == 429 else "UPSTREAM_5XX_RETRY_EXHAUSTED"
                                raise ModelHTTPError(reason, status_code=status, attempts=total_attempts)

                            if status >= 400:
                                # Unsupported thinking parameters are the sole reason
                                # to try the next already-verified thinking variant.
                                if status in {400, 404, 422} and variant_id != self.thinking_variants[-1][0]:
                                    break
                                raise ModelHTTPError("UPSTREAM_4XX", status_code=status, attempts=total_attempts)

                            content, reasoning_tokens = _decode_model_response(
                                response,
                                deadline=attempt_deadline,
                                clock=self.monotonic,
                            )
                            output = _strip_reasoning(strict_json_object(content))
                    except StrictJSONError as exc:
                        last_strict_error = exc
                        if variant_attempts < self.max_attempts and self.monotonic() < overall_deadline:
                            # Repeating the identical request often repeats the
                            # identical malformed wrapper/truncation. Ask the
                            # same model to regenerate; never parse or repair
                            # the rejected body locally.
                            strict_json_retry = True
                            self._backoff(variant_attempts, deadline=overall_deadline)
                            continue
                        if (
                            variant_id != self.thinking_variants[-1][0]
                            and self.monotonic() < overall_deadline
                        ):
                            break
                        raise StrictJSONError(
                            exc.reason_code,
                            attempts=total_attempts,
                            diagnostics=exc.diagnostics,
                        ) from exc
                    except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError, httpx.HTTPError) as exc:
                        if variant_attempts < self.max_attempts:
                            self._backoff(variant_attempts, deadline=overall_deadline)
                            continue
                        raise ModelNetworkError(attempts=total_attempts) from exc
                    return ModelCallResult(
                        model=model,
                        output=output,
                        prompt_hash=prompt_hash,
                        input_hash=input_hash,
                        latency_ms=int((time.perf_counter() - started_all) * 1000),
                        attempts=total_attempts,
                        thinking_variant=variant_id,
                        reasoning_tokens=reasoning_tokens,
                    )

        if last_strict_error is not None:
            raise StrictJSONError(
                last_strict_error.reason_code,
                attempts=total_attempts,
                diagnostics=last_strict_error.diagnostics,
            ) from last_strict_error
        raise ModelClientError("THINKING_VARIANTS_EXHAUSTED", attempts=total_attempts)

    # A short alias makes test doubles and callers that call the operation
    # rather than completion read naturally.
    call = complete
    complete_json = complete

    def _backoff(
        self,
        attempt: int,
        *,
        retry_after: str | None = None,
        deadline: float | None = None,
    ) -> None:
        delay = _retry_after_seconds(retry_after)
        if delay is None:
            delay = self.retry_backoff_seconds * (2 ** (attempt - 1))
        if deadline is not None:
            delay = min(delay, max(0.0, deadline - self.monotonic()))
        if delay > 0:
            self.sleep(delay)


# Short compatibility aliases for callers that do not use the longer class
# name; both names point to the exact same implementation.
OpenAICompatibleClient = OpenAICompatibleModelClient
ModelClient = OpenAICompatibleModelClient


def strict_json_object(content: Any) -> dict[str, Any]:
    """Parse one JSON object with only harmless transport wrappers allowed.

    Compatible gateways sometimes add outer whitespace or one exact Markdown
    JSON fence even when ``response_format=json_object`` was requested.  Both
    wrappers are deterministic and content-free, so normalise them while still
    rejecting prose, multiple objects, malformed fences and trailing text.
    """

    if not isinstance(content, str):
        raise StrictJSONError(diagnostics={"content_type": type(content).__name__})
    normalized = content.strip()
    diagnostics = _strict_json_shape(normalized)
    if normalized.startswith("```json\n") and normalized.endswith("\n```"):
        normalized = normalized[len("```json\n") : -len("\n```")].strip()
    elif normalized.startswith("```\n") and normalized.endswith("\n```"):
        normalized = normalized[len("```\n") : -len("\n```")].strip()
    elif normalized.startswith("```") or normalized.endswith("```"):
        raise StrictJSONError(diagnostics=diagnostics)
    try:
        parsed = json.loads(normalized, object_pairs_hook=_unique_object, parse_constant=_reject_constant)
    except (TypeError, ValueError) as exc:
        raise StrictJSONError(diagnostics=diagnostics) from exc
    if not isinstance(parsed, dict):
        raise StrictJSONError(
            "JSON_OBJECT_REQUIRED",
            diagnostics={**diagnostics, "parsed_type": type(parsed).__name__},
        )
    return parsed


def _strict_json_shape(content: str) -> dict[str, Any]:
    """Return non-content diagnostics safe for persisted model audits."""

    return {
        "content_chars": min(len(content), 10_000_000),
        "starts_with_object": content.startswith("{"),
        "ends_with_object": content.endswith("}"),
        "starts_with_fence": content.startswith("```"),
        "ends_with_fence": content.endswith("```"),
    }


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-standard JSON constant: {value}")


def _decode_model_response(
    response: httpx.Response,
    *,
    deadline: float | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> tuple[str, int | None]:
    """Decode a streamed response, with a compatibility path for JSON bodies.

    The transport is always opened with ``Client.stream``.  A few compatible
    gateways ignore the request's ``stream`` flag and return one ordinary JSON
    document; that response is accepted only after parsing the complete body.
    SSE data is decoded from lines and only ``delta.content`` is retained.
    Reasoning fields are intentionally inspected neither for persistence nor
    for diagnostics.
    """

    content_type = response.headers.get("content-type", "").lower()
    if "text/event-stream" in content_type:
        return _decode_sse_response(response, deadline=deadline, clock=clock)

    try:
        raw = response.read()
    except (TypeError, ValueError) as exc:
        raise StrictJSONError("RESPONSE_JSON_INVALID") from exc
    if not isinstance(raw, bytes):
        raise StrictJSONError("RESPONSE_JSON_INVALID")
    _enforce_stream_deadline(deadline, clock)

    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise StrictJSONError("RESPONSE_JSON_INVALID") from exc

    # Some gateways omit the SSE content type.  Detect the protocol from the
    # body without treating arbitrary non-JSON text as an SSE response.
    if _looks_like_sse(text):
        return _decode_sse_lines(text.splitlines(), deadline=deadline, clock=clock)

    try:
        payload = json.loads(text, object_pairs_hook=_unique_object, parse_constant=_reject_constant)
    except (TypeError, ValueError) as exc:
        raise StrictJSONError("RESPONSE_JSON_INVALID") from exc
    if not isinstance(payload, dict):
        raise StrictJSONError("RESPONSE_OBJECT_REQUIRED")
    message = _message_from_response(payload)
    return message.get("content"), _reasoning_tokens(payload)


def _decode_json_object(response: httpx.Response) -> dict[str, Any]:
    """Backward-compatible helper for callers that still decode JSON bodies."""

    try:
        payload = response.json()
    except (TypeError, ValueError) as exc:
        raise StrictJSONError("RESPONSE_JSON_INVALID") from exc
    if not isinstance(payload, dict):
        raise StrictJSONError("RESPONSE_OBJECT_REQUIRED")
    return payload


def _looks_like_sse(text: str) -> bool:
    stripped = text.lstrip(" \t\r\n\ufeff")
    return stripped.startswith("data:") or stripped.startswith(":")


def _decode_sse_response(
    response: httpx.Response,
    *,
    deadline: float | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> tuple[str, int | None]:
    try:
        return _decode_sse_lines(response.iter_lines(), deadline=deadline, clock=clock)
    except StrictJSONError:
        raise
    except (TypeError, ValueError, UnicodeError) as exc:
        raise StrictJSONError("STREAM_SSE_INVALID") from exc


def _decode_sse_lines(
    lines: Any,
    *,
    deadline: float | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> tuple[str, int | None]:
    """Parse SSE events without retaining raw response or reasoning text."""

    data_lines: list[str] = []
    content_parts: list[str] = []
    reasoning_tokens: int | None = None
    saw_data = False
    saw_done = False
    event_index = 0

    def flush_event() -> None:
        nonlocal data_lines, saw_data, saw_done, reasoning_tokens, event_index
        if not data_lines:
            return
        event_data = "\n".join(data_lines)
        data_lines = []
        if event_data == "[DONE]":
            saw_done = True
            return
        if saw_done:
            raise StrictJSONError("STREAM_SSE_INVALID")
        try:
            payload = json.loads(event_data, object_pairs_hook=_unique_object, parse_constant=_reject_constant)
        except (TypeError, ValueError) as exc:
            raise StrictJSONError("STREAM_SSE_INVALID") from exc
        if not isinstance(payload, dict):
            raise StrictJSONError("STREAM_SSE_INVALID")
        saw_data = True
        event_index += 1
        value = _reasoning_tokens(payload)
        if value is not None:
            reasoning_tokens = value

        choices = payload.get("choices")
        if choices is None:
            if isinstance(payload.get("error"), Mapping):
                raise StrictJSONError(
                    "STREAM_UPSTREAM_ERROR",
                    diagnostics=_stream_shape(payload, event_index),
                )
            allowed_metadata = {
                "id",
                "object",
                "created",
                "model",
                "system_fingerprint",
                "usage",
                "service_tier",
            }
            if set(payload).issubset(allowed_metadata):
                return
            raise StrictJSONError(
                "STREAM_CHOICES_INVALID",
                diagnostics=_stream_shape(payload, event_index),
            )
        if not isinstance(choices, list):
            raise StrictJSONError(
                "STREAM_CHOICES_INVALID",
                diagnostics=_stream_shape(payload, event_index),
            )
        if not choices:
            return
        choice = choices[0]
        if not isinstance(choice, Mapping):
            raise StrictJSONError(
                "STREAM_CHOICES_INVALID",
                diagnostics=_stream_shape(payload, event_index),
            )
        delta = choice.get("delta")
        if not isinstance(delta, Mapping):
            raise StrictJSONError("STREAM_DELTA_INVALID")
        if "content" not in delta:
            return
        content = delta.get("content")
        if content is None:
            return
        if not isinstance(content, str):
            raise StrictJSONError("STREAM_CONTENT_INVALID")
        content_parts.append(content)

    try:
        for raw_line in lines:
            _enforce_stream_deadline(deadline, clock)
            if not isinstance(raw_line, str):
                raise StrictJSONError("STREAM_SSE_INVALID")
            line = raw_line.rstrip("\r")
            if not line:
                flush_event()
                continue
            if line.startswith(":"):
                continue
            if line.startswith("data:"):
                value = line[5:]
                if value.startswith(" "):
                    value = value[1:]
                data_lines.append(value)
                continue
            # Standard SSE metadata is safe to ignore.  Unknown fields are
            # rejected so malformed streams cannot be mistaken for content.
            if line.startswith(("event:", "id:", "retry:")):
                continue
            raise StrictJSONError("STREAM_SSE_INVALID")
        flush_event()
        _enforce_stream_deadline(deadline, clock)
    except StrictJSONError:
        raise
    except (TypeError, ValueError, UnicodeError) as exc:
        raise StrictJSONError("STREAM_SSE_INVALID") from exc

    if not saw_data:
        raise StrictJSONError("STREAM_EMPTY")
    if not saw_done:
        raise StrictJSONError("STREAM_DONE_MISSING")
    content = "".join(content_parts)
    if not content:
        raise StrictJSONError("STREAM_CONTENT_MISSING")
    return content, reasoning_tokens


def _stream_shape(payload: Mapping[str, Any], event_index: int) -> dict[str, Any]:
    """Return bounded structural diagnostics without response values."""

    keys = sorted(str(key) for key in payload)[:20]
    return {
        "event_index": max(1, int(event_index)),
        "top_level_fields": keys,
        "top_level_types": {
            key: type(payload.get(key)).__name__
            for key in keys
        },
    }


def _retry_after_seconds(value: str | None) -> float | None:
    if not value:
        return None
    clean = value.strip()
    try:
        return min(30.0, max(0.0, float(clean)))
    except ValueError:
        pass
    try:
        parsed = datetime.strptime(clean, "%a, %d %b %Y %H:%M:%S %Z").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return min(30.0, max(0.0, (parsed - datetime.now(timezone.utc)).total_seconds()))


def _enforce_stream_deadline(deadline: float | None, clock: Callable[[], float]) -> None:
    if deadline is not None and clock() > deadline:
        raise httpx.ReadTimeout("model stream total deadline exceeded")


def _message_from_response(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices or not isinstance(choices[0], Mapping):
        raise StrictJSONError("CHOICES_INVALID")
    message = choices[0].get("message")
    if not isinstance(message, Mapping):
        raise StrictJSONError("MESSAGE_INVALID")
    return message


def _reasoning_tokens(payload: Mapping[str, Any]) -> int | None:
    usage = payload.get("usage")
    if not isinstance(usage, Mapping):
        return None
    paths = (
        ("reasoning_tokens",),
        ("completion_tokens_details", "reasoning_tokens"),
        ("output_tokens_details", "reasoning_tokens"),
    )
    for path in paths:
        value: Any = usage
        for part in path:
            value = value.get(part) if isinstance(value, Mapping) else None
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            return value
    return None


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _strip_reasoning(value: Any) -> Any:
    if isinstance(value, Mapping):
        hidden = {"reasoning", "reasoning_content", "thinking", "chain_of_thought", "cot"}
        return {
            str(key): _strip_reasoning(item)
            for key, item in value.items()
            if str(key).lower() not in hidden
        }
    if isinstance(value, list):
        return [_strip_reasoning(item) for item in value]
    return value


__all__ = [
    "ModelCallResult",
    "ModelClientError",
    "ModelHTTPError",
    "ModelNetworkError",
    "ModelClient",
    "OpenAICompatibleClient",
    "OpenAICompatibleModelClient",
    "StrictJSONError",
    "strict_json_object",
]
