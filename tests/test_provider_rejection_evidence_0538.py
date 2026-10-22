from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo
import hashlib
import json

import httpx
import pytest

from liangjian_funnel.pipeline.provider_rejection_evidence import (
    ProviderRejectionEvidenceError, persist_provider_rejection,
)


def settings(tmp_path):
    return SimpleNamespace(root=tmp_path, workflow_output_dir=tmp_path / "outputs", model_api_key=None)


def capture(tmp_path, response, **kwargs):
    return persist_provider_rejection(response, settings=kwargs.pop("settings", settings(tmp_path)),
        model="model-test", stage="A2", session="CLOSE_REVIEW", input_hash="input-hash", prompt_hash="prompt-hash",
        attempt=2, captured_at=datetime(2026, 10, 22, 7, 2, tzinfo=ZoneInfo("Asia/Shanghai")), **kwargs)


def load(result):
    raw = Path(result["receipt_path"]).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == result["receipt_sha256"]
    return json.loads(raw)


@pytest.mark.parametrize("status", [429, 500, 503, 599])
def test_complete_error_and_response_headers_without_request_body(tmp_path, status):
    response = httpx.Response(status, json={"error": {"type": "quota_error", "code": "insufficient_quota", "message": "quota exhausted"}, "details": [1, 2]},
        headers=[("retry-after", "30"), ("x-ratelimit-limit-requests", "300"), ("x-ratelimit-remaining-tokens", "1200"),
                 ("x-provider-quota", "none"), ("x-request-id", "request-123"), ("x-request-id", "request-456"), ("set-cookie", "DO_NOT_CAPTURE")],
        request=httpx.Request("POST", "https://provider.test/v1/chat", headers={"Authorization": "Bearer DO_NOT_CAPTURE"}, json={"messages": [{"content": "DO_NOT_CAPTURE"}]}))
    result = capture(tmp_path, response)
    body = load(result)
    assert body["error"] == {"type": "quota_error", "code": "insufficient_quota", "message": "quota exhausted"}
    assert body["body"]["details"] == [1, 2]
    assert body["response_headers"]["x-request-id"] == ["request-123", "request-456"]
    assert body["response_headers"]["x-ratelimit-remaining-tokens"] == ["1200"]
    assert "DO_NOT_CAPTURE" not in Path(result["receipt_path"]).read_text()
    assert body["body_sha256"] == hashlib.sha256(response.content).hexdigest()
    assert Path(result["receipt_path"]).parent == tmp_path / "outputs/provider_rejections/2026-10-22"


def test_body_headers_metadata_and_url_credentials_are_redacted(tmp_path):
    known = "abc-known-key-123456"
    actual = settings(tmp_path)
    actual.model_api_key = SimpleNamespace(get_secret_value=lambda: known)
    response = httpx.Response(429, json={"error": {"type": "quota", "code": "limited", "message": f"Bearer unknown-bearer-value {known}"},
        "api_key": "unknown-structured-key", "nested": {"password": "unknown-pass", "client_secret": "unknown-client-secret"},
        "messages": [{"content": "BUSINESS_REQUEST_ECHO"}], "request_body": "BUSINESS_RAW_ECHO",
        "link": "https://user:unknown-password@provider.test/path?api_key=unknown-query-key&token=unknown-token&trace=okay#access_token=unknown-fragment"},
        headers={"x-request-id": known, "x-ratelimit-debug": "Authorization: Bearer unknown-header-token", "x-quota-api-key": "unknown-header-key"},
        request=httpx.Request("POST", f"https://provider.test/v1?key={known}&trace=okay"))
    result = capture(tmp_path, response, settings=actual)
    raw = Path(result["receipt_path"]).read_text()
    for secret in (known, "unknown-bearer-value", "unknown-structured-key", "unknown-pass", "unknown-client-secret", "BUSINESS_REQUEST_ECHO", "BUSINESS_RAW_ECHO", "unknown-password", "unknown-query-key", "unknown-token", "unknown-fragment", "unknown-header-token", "unknown-header-key"):
        assert secret not in raw
    assert "okay" in raw


def test_long_error_is_complete_without_truncation(tmp_path):
    message = "错误说明-" + "long-error-" * 20000 + "-END_REAL_PROVIDER_ERROR"
    response = httpx.Response(503, json={"error": {"type": "overload", "code": "busy", "message": message}, "tail": "LAST_FIELD"})
    result = capture(tmp_path, response)
    receipt = load(result)
    assert receipt["body"]["error"]["message"] == message
    assert receipt["error"]["message"] == message
    assert receipt["body"]["tail"] == "LAST_FIELD"


def test_text_body_quoted_bare_and_url_credentials(tmp_path):
    text = 'backend failed api_key="not-known-key" password=not-known-pass; https://u:p@provider.test/a?access_token=not-known-token&retry=30 END'
    response = httpx.Response(503, text=text)
    result = capture(tmp_path, response)
    body = load(result)["body"]
    for secret in ("not-known-key", "not-known-pass", "not-known-token", "u:p"):
        assert secret not in body
    assert "backend failed" in body and "END" in body


def test_unique_files_no_overwrite_and_no_other_state_writes(tmp_path):
    response = httpx.Response(429, json={"error": {"message": "limited"}})
    one = capture(tmp_path, response)
    before = Path(one["receipt_path"]).read_bytes()
    two = capture(tmp_path, response)
    assert one["receipt_path"] != two["receipt_path"]
    assert Path(one["receipt_path"]).read_bytes() == before
    assert len(list(tmp_path.rglob("*.json"))) == 2
    assert not list(tmp_path.rglob("*.db")) and not list(tmp_path.rglob("*.started"))


@pytest.mark.parametrize("status", [200, 201, 400, 401, 404])
def test_non_rejection_status_refused_without_any_file(tmp_path, status):
    with pytest.raises(ProviderRejectionEvidenceError, match="STATUS_NOT_ELIGIBLE"):
        capture(tmp_path, httpx.Response(status, text="not eligible"))
    assert not list(tmp_path.iterdir())


def test_persistence_failure_is_explicit_not_silent_success(tmp_path):
    response = httpx.Response(429, text="provider error body")
    with patch("liangjian_funnel.pipeline.provider_rejection_evidence.os.open", side_effect=OSError("SENSITIVE_OS_FAILURE")):
        with pytest.raises(ProviderRejectionEvidenceError) as caught:
            capture(tmp_path, response)
    assert caught.value.reason_code == "PROVIDER_REJECTION_PERSISTENCE_FAILED"
    assert caught.value.status_code == 429
    assert caught.value.body_sha256 == hashlib.sha256(response.content).hexdigest()
    assert "SENSITIVE_OS_FAILURE" not in str(caught.value)


def test_exclusive_collision_fails_and_preserves_original(tmp_path):
    response = httpx.Response(429, text="limit")
    with patch("liangjian_funnel.pipeline.provider_rejection_evidence.uuid.uuid4", return_value=SimpleNamespace(hex="fixed-test-id")):
        one = capture(tmp_path, response)
        original = Path(one["receipt_path"]).read_bytes()
        with pytest.raises(ProviderRejectionEvidenceError, match="PERSISTENCE_FAILED"):
            capture(tmp_path, response)
    assert Path(one["receipt_path"]).read_bytes() == original


def test_response_read_failure_aborts_without_fake_capture(tmp_path):
    class Broken(httpx.SyncByteStream):
        def __iter__(self):
            raise httpx.ReadTimeout("DO_NOT_EXPOSE_RESPONSE_EXCEPTION")
            yield b""
    with pytest.raises(ProviderRejectionEvidenceError, match="CAPTURE_OR_REDACTION_FAILED") as caught:
        capture(tmp_path, httpx.Response(503, stream=Broken()))
    assert caught.value.body_sha256 is None
    assert not list(tmp_path.iterdir())


def test_plain_error_embedded_request_echo_is_not_persisted(tmp_path):
    text = 'Quota exhausted; echoed request {"messages":[{"role":"system","content":"BUSINESS_SYSTEM_TEXT"},{"role":"user","content":"BUSINESS_USER_TEXT"}],"prompt":"BUSINESS_PROMPT_TEXT"}; provider quota is zero END'
    body = load(capture(tmp_path, httpx.Response(429, text=text)))["body"]
    for secret in ("BUSINESS_SYSTEM_TEXT", "BUSINESS_USER_TEXT", "BUSINESS_PROMPT_TEXT"):
        assert secret not in body
    assert "Quota exhausted" in body and "provider quota is zero END" in body


def test_non_json_numeric_constants_are_preserved_as_redacted_text(tmp_path):
    text = '{"error":"bad gateway","quantity":NaN,"password":"hidden-test-password"}'
    receipt = load(capture(tmp_path, httpx.Response(503, text=text)))
    assert receipt["body_format"] == "text" and "NaN" in receipt["body"]
    assert "hidden-test-password" not in receipt["body"]


def test_single_known_secret_string_not_split_into_characters(tmp_path):
    secret = "single-known-secret"
    body = load(capture(tmp_path, httpx.Response(429, text="quota exhausted: " + secret), known_secrets=secret))["body"]
    assert body == "quota exhausted: [REDACTED]"


def test_symlink_directory_rejected_before_writing_outside(tmp_path):
    outputs = tmp_path / "outputs"
    outputs.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    try:
        (outputs / "provider_rejections").symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("Local account cannot create symlinks")
    with pytest.raises(ProviderRejectionEvidenceError, match="PERSISTENCE_FAILED"):
        capture(tmp_path, httpx.Response(429, text="limit"))
    assert not list(outside.iterdir())


def test_alternate_request_echo_and_signed_url_credentials(tmp_path):
    response = httpx.Response(503, json={"error": {"message": "backend overloaded"},
        "input": [{"text": "PRIVATE_RESPONSE_API_INPUT"}], "contents": [{"parts": ["PRIVATE_PROVIDER_CONTENTS"]}],
        "secret_key": "UNKNOWN_SIGNING_KEY", "security_token": "UNKNOWN_SECURITY_TOKEN",
        "retry_url": "https://provider.test/a?X-Amz-Signature=UNKNOWN_SIGNATURE&X-Amz-Security-Token=UNKNOWN_URL_TOKEN&trace=public"})
    result = capture(tmp_path, response)
    raw = Path(result["receipt_path"]).read_text()
    for secret in ("PRIVATE_RESPONSE_API_INPUT", "PRIVATE_PROVIDER_CONTENTS", "UNKNOWN_SIGNING_KEY", "UNKNOWN_SECURITY_TOKEN", "UNKNOWN_SIGNATURE", "UNKNOWN_URL_TOKEN"):
        assert secret not in raw
    assert "backend overloaded" in raw and "public" in raw


def test_diagnostics_return_contains_only_redacted_error_and_headers(tmp_path):
    secret = "diagnostics-key-123456"
    response = httpx.Response(429, json={"error": {"type": "limited", "code": "quota", "message": "quota key: " + secret}},
                             headers={"x-request-id": secret})
    result = capture(tmp_path, response, known_secrets=[secret])
    assert secret not in json.dumps(result)
    assert result["error"]["type"] == "limited"
    assert result["response_headers"]["x-request-id"] == ["[REDACTED]"]
