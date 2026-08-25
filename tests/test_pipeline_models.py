from pathlib import Path

import httpx
import pytest

from liangjian_funnel.pipeline.model_client import (
    ModelClientError,
    ModelNetworkError,
    OpenAICompatibleModelClient,
    PRODUCTION_THINKING_VARIANTS,
    StrictJSONError,
    strict_json_object,
)
from liangjian_funnel.settings import Settings


class _ChunkedByteStream(httpx.SyncByteStream):
    def __init__(self, chunks: list[bytes]):
        self.chunks = chunks

    def __iter__(self):
        yield from self.chunks


def _sse_response(request: httpx.Request, events: list[dict | str], *, chunks: bool = False) -> httpx.Response:
    import json

    body = b"".join(
        (
            f"data: {event if isinstance(event, str) else json.dumps(event, separators=(',', ':'))}\n\n"
        ).encode("utf-8")
        for event in events
    )
    if chunks:
        stream = _ChunkedByteStream([body[:9], body[9:23], body[23:]])
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=stream, request=request)
    return httpx.Response(200, headers={"content-type": "text/event-stream"}, content=body, request=request)


def _settings(tmp_path: Path, key: str | None = "model-secret") -> Settings:
    return Settings.from_env({"LIANGJIAN_MODEL_API_KEY": key or ""}, root=tmp_path)


def test_client_uses_bounded_model_thinking_and_json_object(tmp_path: Path):
    seen: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = request.read()
        import json

        parsed = json.loads(body)
        seen.append(parsed)
        assert request.headers["accept"] == "text/event-stream"
        return httpx.Response(
            200,
            json={
                "choices": [{"message": {"content": '{"envelope": {"status": "OK"}}', "reasoning_content": "secret-cot"}}],
                "usage": {"completion_tokens_details": {"reasoning_tokens": 7}},
            },
            request=request,
        )

    client = OpenAICompatibleModelClient(_settings(tmp_path), transport=httpx.MockTransport(handler), sleep=lambda _: None)
    result = client.complete(
        "deepseek-v4-pro-0813",
        [{"role": "system", "content": "system"}, {"role": "user", "content": "RUNTIME_INPUT\n{}"}],
        prompt_hash="p" * 64,
        input_hash="i" * 64,
    )
    assert seen[0]["model"] == "deepseek-v4-pro-0813"
    assert seen[0]["reasoning_effort"] == "low"
    assert seen[0]["stream"] is True
    assert seen[0]["max_tokens"] == 12_000
    assert seen[0]["response_format"] == {"type": "json_object"}
    assert result.output == {"envelope": {"status": "OK"}}
    assert result.reasoning_tokens == 7
    assert "secret-cot" not in repr(result)


def test_client_decodes_multichunk_sse_and_never_retains_reasoning(tmp_path: Path):
    seen: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        import json

        seen.append(json.loads(request.read()))
        return _sse_response(
            request,
            [
                {"choices": [{"delta": {"reasoning_content": "private-chain-of-thought"}}]},
                {"choices": [{"delta": {"thinking": "also-private"}}]},
                {"choices": [{"delta": {"content": '{"envelope": '}}]},
                {"choices": [{"delta": {"content": '{"status":"OK"}'}}]},
                {
                    "choices": [{"delta": {"content": "}"}}],
                    "usage": {"completion_tokens_details": {"reasoning_tokens": 13}},
                },
                {"choices": [], "usage": {"reasoning_tokens": 13}},
                "[DONE]",
            ],
            chunks=True,
        )

    client = OpenAICompatibleModelClient(
        _settings(tmp_path), transport=httpx.MockTransport(handler), sleep=lambda _: None
    )
    result = client.complete("deepseek-v4-pro-0813", [{"role": "user", "content": "{}"}])

    assert seen[0]["stream"] is True
    assert result.output == {"envelope": {"status": "OK"}}
    assert result.reasoning_tokens == 13
    assert "private-chain-of-thought" not in repr(result)
    assert "also-private" not in repr(result)


def test_429_retries_same_model_without_circuit_or_downgrade(tmp_path: Path):
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        import json

        body = json.loads(request.read())
        calls.append(body["model"])
        if len(calls) < 3:
            return httpx.Response(429, json={"error": "rate limited"}, request=request)
        return _sse_response(
            request,
            [{"choices": [{"delta": {"content": '{"ok":true}'}}]}, "[DONE]"],
        )

    client = OpenAICompatibleModelClient(
        _settings(tmp_path), transport=httpx.MockTransport(handler), sleep=lambda _: None, max_attempts=3
    )
    result = client.complete("moonshotai/kimi-k3-free", [{"role": "user", "content": "{}"}])
    assert result.attempts == 3
    assert calls == ["moonshotai/kimi-k3-free"] * 3


def test_retry_after_is_honored_within_bounded_request_budget(tmp_path: Path):
    sleeps = []
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, headers={"Retry-After": "2"}, request=request)
        return _sse_response(request, [{"choices": [{"delta": {"content": '{"ok":true}'}}]}, "[DONE]"])

    client = OpenAICompatibleModelClient(
        _settings(tmp_path),
        transport=httpx.MockTransport(handler),
        sleep=sleeps.append,
        max_attempts=2,
    )
    assert client.complete("deepseek-v4-flash-0731", [{"role": "user", "content": "{}"}]).attempts == 2
    assert sleeps == [2.0]


def test_5xx_retries_same_model_and_succeeds_with_sse(tmp_path: Path):
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        import json

        body = json.loads(request.read())
        calls.append(body["model"])
        if len(calls) < 3:
            return httpx.Response(503, json={"error": "upstream"}, request=request)
        return _sse_response(
            request,
            [{"choices": [{"delta": {"content": '{"ok":true}'}}]}, "[DONE]"],
        )

    client = OpenAICompatibleModelClient(
        _settings(tmp_path), transport=httpx.MockTransport(handler), sleep=lambda _: None, max_attempts=3
    )
    result = client.complete("z-ai/glm-5.3-free", [{"role": "user", "content": "{}"}])

    assert result.output == {"ok": True}
    assert result.attempts == 3
    assert calls == ["z-ai/glm-5.3-free"] * 3


def test_continuous_sse_is_bounded_by_total_wall_clock(tmp_path: Path):
    def handler(request: httpx.Request) -> httpx.Response:
        return _sse_response(
            request,
            [
                {"choices": [{"delta": {"content": "{"}}]},
                {"choices": [{"delta": {"content": '"ok":true}'}}]},
                "[DONE]",
            ],
        )

    ticks = iter((0.0, 0.5, 2.0, 2.1))
    settings = _settings(tmp_path).model_copy(update={"model_timeout_seconds": 1.0})
    client = OpenAICompatibleModelClient(
        settings,
        transport=httpx.MockTransport(handler),
        sleep=lambda _: None,
        monotonic=lambda: next(ticks, 2.1),
        max_attempts=1,
    )

    with pytest.raises(ModelNetworkError) as exc_info:
        client.complete("deepseek-v4-pro-0813", [{"role": "user", "content": "{}"}])

    assert exc_info.value.attempts == 1


def test_unsupported_thinking_variant_falls_back_without_changing_model(tmp_path: Path):
    variants: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        import json

        body = json.loads(request.read())
        variants.append("thinking" if "thinking" in body else "reasoning_effort" if "reasoning_effort" in body else "other")
        if len(variants) == 1:
            return httpx.Response(400, json={"error": "unsupported"}, request=request)
        return httpx.Response(200, json={"choices": [{"message": {"content": "{\"ok\":true}"}}]}, request=request)

    client = OpenAICompatibleModelClient(_settings(tmp_path), transport=httpx.MockTransport(handler), sleep=lambda _: None)
    result = client.complete("z-ai/glm-5.3-free", [{"role": "user", "content": "{}"}])
    assert result.model == "z-ai/glm-5.3-free"
    assert variants[:2] == ["reasoning_effort", "thinking"]


@pytest.mark.parametrize(
    ("body", "reason_code"),
    [
        (b"", "STREAM_EMPTY"),
        (b"data: not-json\n\ndata: [DONE]\n\n", "STREAM_SSE_INVALID"),
        (b'data: {"choices":[{"delta":{"content":"{\\"ok\\":true}"}}]}\n\n', "STREAM_DONE_MISSING"),
        (b'data: {"choices":[{"delta":{"reasoning_content":"secret"}}]}\n\ndata: [DONE]\n\n', "STREAM_CONTENT_MISSING"),
        (b'data: {"choices":[{"delta":{"content":"not-json"}}]}\n\ndata: [DONE]\n\n', "STRICT_JSON_INVALID"),
    ],
)
def test_malformed_or_incomplete_sse_fails_closed_without_body(tmp_path: Path, body: bytes, reason_code: str):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, content=body, request=request)

    client = OpenAICompatibleModelClient(
        _settings(tmp_path), transport=httpx.MockTransport(handler), sleep=lambda _: None, max_attempts=1
    )
    with pytest.raises(StrictJSONError) as exc_info:
        client.complete("deepseek-v4-pro-0813", [{"role": "user", "content": "{}"}])

    assert exc_info.value.reason_code == reason_code
    assert exc_info.value.attempts == len(PRODUCTION_THINKING_VARIANTS)
    assert "not-json" not in str(exc_info.value)
    assert "secret" not in str(exc_info.value)


def test_sse_metadata_event_is_accepted_and_unknown_shape_has_safe_diagnostics(tmp_path: Path):
    def accepted(request: httpx.Request) -> httpx.Response:
        return _sse_response(
            request,
            [
                {"id": "opaque", "object": "chat.completion.chunk", "model": "opaque-model"},
                {"choices": [{"delta": {"content": '{"ok":true}'}}]},
                "[DONE]",
            ],
        )

    result = OpenAICompatibleModelClient(
        _settings(tmp_path), transport=httpx.MockTransport(accepted), sleep=lambda _: None
    ).complete("deepseek-v4-pro-0813", [{"role": "user", "content": "{}"}])
    assert result.output == {"ok": True}

    def rejected(request: httpx.Request) -> httpx.Response:
        return _sse_response(request, [{"unexpected": "sensitive-value"}, "[DONE]"])

    client = OpenAICompatibleModelClient(
        _settings(tmp_path),
        transport=httpx.MockTransport(rejected),
        sleep=lambda _: None,
        max_attempts=1,
    )
    with pytest.raises(StrictJSONError) as exc_info:
        client.complete("deepseek-v4-pro-0813", [{"role": "user", "content": "{}"}])
    assert exc_info.value.reason_code == "STREAM_CHOICES_INVALID"
    assert exc_info.value.diagnostics == {
        "event_index": 1,
        "top_level_fields": ["unexpected"],
        "top_level_types": {"unexpected": "str"},
    }
    assert "sensitive-value" not in repr(exc_info.value.diagnostics)


def test_strict_json_failure_retries_then_succeeds(tmp_path: Path):
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        content = "not-json" if calls == 1 else '{"ok":true}'
        return _sse_response(request, [{"choices": [{"delta": {"content": content}}]}, "[DONE]"])

    client = OpenAICompatibleModelClient(
        _settings(tmp_path), transport=httpx.MockTransport(handler), sleep=lambda _: None, max_attempts=2
    )
    result = client.complete("z-ai/glm-5.3-free", [{"role": "user", "content": "{}"}])
    assert result.output == {"ok": True}
    assert result.attempts == 2


def test_strict_json_retry_regenerates_with_json_only_instruction(tmp_path: Path):
    seen: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        import json

        body = json.loads(request.read())
        seen.append(body)
        content = "not-json" if len(seen) == 1 else '{"ok":true}'
        return _sse_response(request, [{"choices": [{"delta": {"content": content}}]}, "[DONE]"])

    client = OpenAICompatibleModelClient(
        _settings(tmp_path), transport=httpx.MockTransport(handler), sleep=lambda _: None, max_attempts=2
    )
    result = client.complete("deepseek-v4-pro-0813", [{"role": "user", "content": "original"}])

    assert result.output == {"ok": True}
    assert seen[0]["messages"] == [{"role": "user", "content": "original"}]
    assert seen[1]["messages"][-1]["content"].startswith("TRANSPORT_JSON_RETRY")
    assert "not-json" not in str(seen[1])


def test_strict_parser_normalizes_only_harmless_whitespace_and_exact_fence():
    assert strict_json_object('  {"ok":true}\n') == {"ok": True}
    assert strict_json_object('```json\n{"ok":true}\n```') == {"ok": True}
    assert strict_json_object('```\n{"ok":true}\n```') == {"ok": True}


def test_strict_parser_rejects_prose_malformed_fence_and_non_object():
    for value in (
        '```json {"ok":true}```',
        '```json\n{"ok":true}\n``` suffix',
        'prefix {"ok":true}',
        '[1, 2]',
        '{"ok":true} suffix',
    ):
        with pytest.raises(StrictJSONError):
            strict_json_object(value)


def test_client_rejects_unknown_model_before_network(tmp_path: Path):
    client = OpenAICompatibleModelClient(_settings(tmp_path), transport=httpx.MockTransport(lambda request: httpx.Response(200)))
    with pytest.raises(ModelClientError, match="MODEL_NOT_ALLOWED"):
        client.complete("some-other-model", [{"role": "user", "content": "{}"}])
