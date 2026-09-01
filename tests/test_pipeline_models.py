from pathlib import Path
import socket
import threading
import time

import httpx
import pytest

from liangjian_funnel.pipeline.model_client import (
    ModelClientError,
    ModelHTTPError,
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
    assert seen[0]["max_tokens"] == 393_216
    assert seen[0]["response_format"] == {"type": "json_object"}
    assert result.output == {"envelope": {"status": "OK"}}
    assert result.reasoning_tokens == 7
    assert "secret-cot" not in repr(result)


@pytest.mark.parametrize(
    ("status", "message"),
    [
        (400, "max_tokens is too large; maximum is 262144"),
        (413, "max_completion_tokens must be <= 262144"),
        (422, "requested max output tokens exceed the model limit"),
    ],
)
def test_explicit_output_budget_rejection_retries_same_variant_at_256k(
    tmp_path: Path, status: int, message: str
):
    seen: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        import json

        body = json.loads(request.read())
        seen.append(body)
        if len(seen) == 1:
            return httpx.Response(status, json={"error": {"message": message}}, request=request)
        return _sse_response(request, [{"choices": [{"delta": {"content": '{"ok":true}'}}]}, "[DONE]"])

    client = OpenAICompatibleModelClient(
        _settings(tmp_path),
        transport=httpx.MockTransport(handler),
        sleep=lambda _: None,
        max_attempts=1,
    )
    result = client.complete("deepseek-v4-pro-0813", [{"role": "user", "content": "same"}])

    assert result.output == {"ok": True}
    assert result.attempts == 2
    assert [body["max_tokens"] for body in seen] == [393_216, 262_144]
    assert [body["model"] for body in seen] == ["deepseek-v4-pro-0813"] * 2
    assert [body["reasoning_effort"] for body in seen] == ["low"] * 2
    assert seen[0]["messages"] == seen[1]["messages"]


def test_capacity_rejection_descends_to_128k_third_budget(tmp_path: Path):
    seen: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        import json

        seen.append(json.loads(request.read()))
        if len(seen) < 3:
            return httpx.Response(
                413,
                json={"error": {"message": "max_tokens too large; maximum is 131072"}},
                request=request,
            )
        return _sse_response(request, [{"choices": [{"delta": {"content": '{"ok":true}'}}]}, "[DONE]"])

    client = OpenAICompatibleModelClient(
        _settings(tmp_path),
        transport=httpx.MockTransport(handler),
        sleep=lambda _: None,
        max_attempts=1,
    )
    result = client.complete("z-ai/glm-5.3-free", [{"role": "user", "content": "same"}])

    assert result.output == {"ok": True}
    assert result.attempts == 3
    assert [body["max_tokens"] for body in seen] == [393_216, 262_144, 131_072]


def test_third_capacity_budget_rejection_closes_after_128k(tmp_path: Path):
    seen: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        import json

        seen.append(json.loads(request.read()))
        return httpx.Response(
            413,
            json={"error": {"message": "max_tokens too large; maximum is 65536"}},
            request=request,
        )

    client = OpenAICompatibleModelClient(
        _settings(tmp_path),
        transport=httpx.MockTransport(handler),
        sleep=lambda _: None,
        max_attempts=3,
    )
    with pytest.raises(ModelHTTPError) as exc_info:
        client.complete("deepseek-v4-pro-0813", [{"role": "user", "content": "same"}])

    assert exc_info.value.reason_code == "OUTPUT_BUDGET_FALLBACK_REJECTED"
    assert exc_info.value.status_code == 413
    assert exc_info.value.attempts == 3
    assert [body["max_tokens"] for body in seen] == [393_216, 262_144, 131_072]


def test_fallback_sequence_never_increases_a_custom_low_primary(tmp_path: Path):
    seen: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        import json

        seen.append(json.loads(request.read()))
        if len(seen) == 1:
            return httpx.Response(
                413,
                json={"error": {"message": "max_tokens too large; maximum is 8000"}},
                request=request,
            )
        return _sse_response(request, [{"choices": [{"delta": {"content": '{"ok":true}'}}]}, "[DONE]"])

    settings = Settings.from_env(
        {
            "LIANGJIAN_MODEL_API_KEY": "model-secret",
            "LIANGJIAN_MODEL_MAX_OUTPUT_TOKENS": "12000",
            "LIANGJIAN_MODEL_FALLBACK_OUTPUT_TOKENS": "8000",
            # This legacy/default-like tier is above the custom primary and
            # must be discarded instead of increasing the second request.
            "LIANGJIAN_MODEL_SECONDARY_FALLBACK_OUTPUT_TOKENS": "131072",
        },
        root=tmp_path,
    )
    client = OpenAICompatibleModelClient(
        settings,
        transport=httpx.MockTransport(handler),
        sleep=lambda _: None,
        max_attempts=1,
    )
    result = client.complete("deepseek-v4-pro-0813", [{"role": "user", "content": "same"}])

    assert result.output == {"ok": True}
    assert [body["max_tokens"] for body in seen] == [12_000, 8_000]


def test_generic_final_thinking_400_uses_the_configured_256k_fallback_once(tmp_path: Path):
    seen: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        import json

        seen.append(json.loads(request.read()))
        if len(seen) == 1:
            return httpx.Response(400, json={"error": "invalid request"}, request=request)
        return _sse_response(request, [{"choices": [{"delta": {"content": '{"ok":true}'}}]}, "[DONE]"])

    client = OpenAICompatibleModelClient(
        _settings(tmp_path),
        transport=httpx.MockTransport(handler),
        sleep=lambda _: None,
        max_attempts=1,
    )
    client.thinking_variants = (("reasoning_effort_low", {"reasoning_effort": "low"}),)
    result = client.complete("z-ai/glm-5.3-free", [{"role": "user", "content": "same"}])

    assert result.output == {"ok": True}
    assert [body["max_tokens"] for body in seen] == [393_216, 262_144]


def test_413_without_explicit_output_token_limit_does_not_downgrade(tmp_path: Path):
    seen: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        import json

        seen.append(json.loads(request.read()))
        return httpx.Response(413, json={"error": {"message": "request entity too large"}}, request=request)

    client = OpenAICompatibleModelClient(
        _settings(tmp_path),
        transport=httpx.MockTransport(handler),
        sleep=lambda _: None,
        max_attempts=1,
    )
    with pytest.raises(ModelHTTPError) as exc_info:
        client.complete("deepseek-v4-pro-0813", [{"role": "user", "content": "same"}])

    assert exc_info.value.reason_code == "UPSTREAM_4XX"
    assert exc_info.value.status_code == 413
    assert [body["max_tokens"] for body in seen] == [393_216]


def test_client_can_disable_thinking_without_emitting_or_falling_back_thinking_fields(tmp_path: Path):
    seen: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        import json

        parsed = json.loads(request.read())
        seen.append(parsed)
        return httpx.Response(
            400 if len(seen) == 1 else 200,
            json={"error": "unsupported request"}
            if len(seen) == 1
            else {"choices": [{"message": {"content": '{"ok":true}'}}]},
            request=request,
        )

    client = OpenAICompatibleModelClient(
        _settings(tmp_path),
        transport=httpx.MockTransport(handler),
        sleep=lambda _: None,
        thinking_enabled=False,
    )

    with pytest.raises(ModelHTTPError):
        client.complete("deepseek-v4-flash-0731", [{"role": "user", "content": "{}"}])

    assert len(seen) == 1
    assert not {"thinking", "reasoning", "reasoning_effort", "enable_thinking"}.intersection(seen[0])


def test_disabled_thinking_keeps_normal_429_retry_without_thinking_payload(tmp_path: Path):
    seen: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        import json

        seen.append(json.loads(request.read()))
        if len(seen) == 1:
            return httpx.Response(429, json={"error": "rate limited"}, request=request)
        return _sse_response(request, [{"choices": [{"delta": {"content": '{"ok":true}'}}]}, "[DONE]"])

    client = OpenAICompatibleModelClient(
        _settings(tmp_path),
        transport=httpx.MockTransport(handler),
        sleep=lambda _: None,
        max_attempts=2,
        thinking_enabled=False,
    )
    result = client.complete("deepseek-v4-flash-0731", [{"role": "user", "content": "{}"}])

    assert result.attempts == 2
    assert result.thinking_variant == "thinking_disabled"
    assert len(seen) == 2
    for body in seen:
        assert not {"thinking", "reasoning", "reasoning_effort", "enable_thinking"}.intersection(body)


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
    calls: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        import json

        body = json.loads(request.read())
        calls.append(body)
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
    assert [body["model"] for body in calls] == ["moonshotai/kimi-k3-free"] * 3
    assert [body["max_tokens"] for body in calls] == [393_216] * 3


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
    calls: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        import json

        body = json.loads(request.read())
        calls.append(body)
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
    assert [body["model"] for body in calls] == ["z-ai/glm-5.3-free"] * 3
    assert [body["max_tokens"] for body in calls] == [393_216] * 3


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


def test_silent_socket_is_interrupted_by_remaining_wall_clock_budget(tmp_path: Path):
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    host, port = listener.getsockname()
    release = threading.Event()

    def accept_without_responding() -> None:
        connection = None
        try:
            connection, _ = listener.accept()
            release.wait(timeout=2.0)
        finally:
            if connection is not None:
                connection.close()

    thread = threading.Thread(target=accept_without_responding, daemon=True)
    thread.start()
    settings = _settings(tmp_path).model_copy(
        update={
            "model_base_url": f"http://{host}:{port}/v1",
            "model_timeout_seconds": 0.2,
        }
    )
    client = OpenAICompatibleModelClient(settings, max_attempts=1)
    started = time.monotonic()
    try:
        with pytest.raises(ModelNetworkError) as exc_info:
            client.complete("deepseek-v4-pro-0813", [{"role": "user", "content": "{}"}])
    finally:
        release.set()
        listener.close()
        thread.join(timeout=1.0)

    assert exc_info.value.reason_code == "MODEL_WALL_CLOCK_TIMEOUT"
    assert time.monotonic() - started < 1.0


def test_call_timeout_override_cannot_exceed_remaining_stage_budget(tmp_path: Path):
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
    client = OpenAICompatibleModelClient(
        _settings(tmp_path),
        transport=httpx.MockTransport(handler),
        sleep=lambda _: None,
        monotonic=lambda: next(ticks, 2.1),
        max_attempts=1,
    )

    with pytest.raises(ModelNetworkError) as exc_info:
        client.complete(
            "deepseek-v4-pro-0813",
            [{"role": "user", "content": "{}"}],
            timeout_seconds=1.0,
        )

    assert exc_info.value.attempts == 1


def test_chunked_json_content_type_is_bounded_by_total_wall_clock(tmp_path: Path):
    def handler(request: httpx.Request) -> httpx.Response:
        body = b'{"choices":[{"message":{"content":"{\\"ok\\":true}"}}]}'
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            stream=_ChunkedByteStream([body[:16], body[16:]]),
            request=request,
        )

    ticks = iter((0.0, 0.5, 0.75, 2.0, 2.1))
    settings = _settings(tmp_path).model_copy(update={"model_timeout_seconds": 1.0})
    client = OpenAICompatibleModelClient(
        settings,
        transport=httpx.MockTransport(handler),
        sleep=lambda _: None,
        monotonic=lambda: next(ticks, 2.1),
        max_attempts=1,
    )

    with pytest.raises(ModelNetworkError) as exc_info:
        client.complete("z-ai/glm-5.3-free", [{"role": "user", "content": "{}"}])

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
    assert [body["max_tokens"] for body in seen] == [393_216, 393_216]
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
