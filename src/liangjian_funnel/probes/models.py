from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from ..contracts import CapabilityCheck, CapabilityReport, CapabilityStatus
from ..redaction import digest_text, safe_error
from ..settings import ALL_MODELS, Settings


THINKING_VARIANTS: tuple[tuple[str, dict[str, Any]], ...] = (
    ("thinking_object", {"thinking": {"type": "enabled"}}),
    ("reasoning_effort", {"reasoning_effort": "high"}),
    ("reasoning_object", {"reasoning": {"enabled": True}}),
)


class ModelProbe:
    def __init__(self, settings: Settings, *, transport: httpx.BaseTransport | None = None):
        self.settings = settings
        self.transport = transport

    def run(self, *, now: datetime | None = None) -> CapabilityReport:
        current = now or datetime.now(ZoneInfo(self.settings.timezone))
        if self.settings.model_api_key is None:
            checks = tuple(
                CapabilityCheck(name=model, status=CapabilityStatus.BLOCKED, reason_code="MODEL_API_KEY_MISSING")
                for model in ALL_MODELS
            )
            return CapabilityReport(provider="MODEL_GATEWAY", generated_at=current, overall_status=CapabilityStatus.BLOCKED, checks=checks)
        key = self.settings.model_api_key.get_secret_value()
        with httpx.Client(
            base_url=self.settings.model_base_url,
            timeout=self.settings.timeout_seconds,
            transport=self.transport,
            trust_env=False,
            headers={"Authorization": f"Bearer {key}", "Accept": "application/json", "Content-Type": "application/json"},
        ) as client:
            checks = tuple(self._probe_model(client, model) for model in ALL_MODELS)
        overall = CapabilityStatus.PASS if all(check.status is CapabilityStatus.PASS for check in checks) else CapabilityStatus.BLOCKED
        return CapabilityReport(provider="MODEL_GATEWAY", generated_at=current, overall_status=overall, checks=checks)

    def _probe_model(self, client: httpx.Client, model: str) -> CapabilityCheck:
        attempts: list[dict[str, Any]] = []
        for variant_id, thinking_payload in THINKING_VARIANTS:
            result = self._attempt(client, model, variant_id, thinking_payload)
            attempts.append(result[1])
            if result[0]:
                evidence = result[1]
                evidence["attempted_variants"] = [item["variant"] for item in attempts]
                return CapabilityCheck(
                    name=model,
                    status=CapabilityStatus.PASS,
                    latency_ms=int(evidence["latency_ms"]),
                    http_status=int(evidence["http_status"]),
                    evidence=evidence,
                )
        last = attempts[-1] if attempts else {}
        return CapabilityCheck(
            name=model,
            status=CapabilityStatus.BLOCKED,
            latency_ms=int(last.get("latency_ms", 0)),
            http_status=last.get("http_status"),
            reason_code="THINKING_OR_STRICT_JSON_UNVERIFIED",
            evidence={
                "attempted_variants": [item.get("variant") for item in attempts],
                "attempt_statuses": [item.get("status") for item in attempts],
            },
        )

    def _attempt(
        self, client: httpx.Client, model: str, variant_id: str, thinking_payload: dict[str, Any]
    ) -> tuple[bool, dict[str, Any]]:
        body: dict[str, Any] = {
            "model": model,
            "temperature": 0,
            "messages": [
                {"role": "system", "content": "Return exactly one JSON object and no markdown."},
                {"role": "user", "content": '{"probe":"reply with {\\"ok\\":true}"}'},
            ],
            "response_format": {"type": "json_object"},
            **thinking_payload,
        }
        started = time.perf_counter()
        try:
            response = client.post("/chat/completions", json=body)
            latency = int((time.perf_counter() - started) * 1000)
            if response.status_code >= 400:
                return False, {"variant": variant_id, "status": "HTTP_ERROR", "http_status": response.status_code, "latency_ms": latency}
            payload = response.json()
            message = (((payload.get("choices") or [{}])[0]).get("message") or {}) if isinstance(payload, dict) else {}
            content = message.get("content")
            strict_json = _strict_json(content)
            markers, reasoning_tokens = _reasoning_evidence(payload, message)
            evidence = {
                "variant": variant_id,
                "status": "PASS" if strict_json and markers else "UNVERIFIED",
                "http_status": response.status_code,
                "latency_ms": latency,
                "strict_json": strict_json,
                "reasoning_marker_fields": markers,
                "reasoning_tokens": reasoning_tokens,
                "content_sha256": digest_text(content) if isinstance(content, str) else None,
            }
            return bool(strict_json and markers), evidence
        except (httpx.HTTPError, ValueError, TypeError) as exc:
            return False, {
                "variant": variant_id,
                "status": "REQUEST_FAILED",
                "http_status": None,
                "latency_ms": int((time.perf_counter() - started) * 1000),
                "error": safe_error(exc),
            }


def _strict_json(content: Any) -> bool:
    if not isinstance(content, str) or content.strip() != content or content.startswith("```"):
        return False
    try:
        parsed = json.loads(content)
    except (TypeError, ValueError):
        return False
    return isinstance(parsed, dict) and parsed.get("ok") is True


def _reasoning_evidence(payload: dict[str, Any], message: dict[str, Any]) -> tuple[list[str], int | None]:
    markers: list[str] = []
    for key in ("reasoning_content", "reasoning", "thinking"):
        if key in message and message.get(key) not in (None, "", [], {}):
            markers.append(f"message.{key}")
    usage = payload.get("usage") if isinstance(payload, dict) else None
    reasoning_tokens: int | None = None
    if isinstance(usage, dict):
        for path in (
            ("reasoning_tokens",),
            ("completion_tokens_details", "reasoning_tokens"),
            ("output_tokens_details", "reasoning_tokens"),
        ):
            value: Any = usage
            for part in path:
                value = value.get(part) if isinstance(value, dict) else None
            if isinstance(value, int) and value > 0:
                reasoning_tokens = value
                markers.append("usage." + ".".join(path))
                break
    return sorted(set(markers)), reasoning_tokens

