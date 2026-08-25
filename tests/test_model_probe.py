from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx

from liangjian_funnel.contracts import CapabilityStatus
from liangjian_funnel.probes.models import ModelProbe
from liangjian_funnel.settings import ALL_MODELS, Settings


NOW = datetime(2026, 8, 24, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))


def _settings(tmp_path: Path, key: str | None = "model-secret") -> Settings:
    return Settings.from_env({"LIANGJIAN_MODEL_API_KEY": key or ""}, root=tmp_path)


def test_missing_model_key_blocks_all_models(tmp_path: Path):
    report = ModelProbe(_settings(tmp_path, None)).run(now=NOW)
    assert report.overall_status is CapabilityStatus.BLOCKED
    assert tuple(check.name for check in report.checks) == ALL_MODELS


def test_strict_json_without_reasoning_marker_is_not_enough(tmp_path: Path):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"choices": [{"message": {"content": '{"ok":true}'}}], "usage": {}}, request=request)

    report = ModelProbe(_settings(tmp_path), transport=httpx.MockTransport(handler)).run(now=NOW)
    assert report.overall_status is CapabilityStatus.BLOCKED
    assert all(check.reason_code == "THINKING_OR_STRICT_JSON_UNVERIFIED" for check in report.checks)


def test_reasoning_marker_and_strict_json_pass_without_retaining_reasoning(tmp_path: Path):
    secret_reasoning = "private-chain-of-thought-must-not-be-stored"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer model-secret"
        return httpx.Response(
            200,
            json={
                "choices": [{"message": {"content": '{"ok":true}', "reasoning_content": secret_reasoning}}],
                "usage": {"completion_tokens_details": {"reasoning_tokens": 12}},
            },
            request=request,
        )

    report = ModelProbe(_settings(tmp_path), transport=httpx.MockTransport(handler)).run(now=NOW)
    assert report.overall_status is CapabilityStatus.PASS
    dumped = report.model_dump_json()
    assert secret_reasoning not in dumped
    assert "message.reasoning_content" in dumped
    assert all(check.evidence["reasoning_tokens"] == 12 for check in report.checks)


def test_markdown_wrapped_json_fails_strict_json_gate(tmp_path: Path):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"choices": [{"message": {"content": '```json\n{"ok":true}\n```', "reasoning_content": "x"}}]},
            request=request,
        )

    report = ModelProbe(_settings(tmp_path), transport=httpx.MockTransport(handler)).run(now=NOW)
    assert report.overall_status is CapabilityStatus.BLOCKED

