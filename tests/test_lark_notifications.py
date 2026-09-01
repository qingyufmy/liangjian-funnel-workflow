from __future__ import annotations

import json
from urllib.error import HTTPError

import pytest

from liangjian_funnel.runtime.lark import LarkConfigurationError, LarkNotifier


URL = "https://open.larksuite.com/open-apis/bot/v2/hook/test-token"


class _Response:
    status = 200

    def __init__(self, body: bytes):
        self.body = body
        self.closed = False

    def read(self, _limit: int = -1) -> bytes:
        return self.body

    def close(self) -> None:
        self.closed = True


def test_webhook_validation_is_strict_and_does_not_echo_secret():
    assert LarkNotifier(URL, retry_delay_seconds=0).enabled
    for value, reason in (
        ("http://open.larksuite.com/open-apis/bot/v2/hook/x", "HTTPS_REQUIRED"),
        ("https://evil.example/open-apis/bot/v2/hook/x", "INVALID_HOST"),
        (URL + "?x=1", "INVALID_PATH"),
        ("https://user:pass@open.larksuite.com/open-apis/bot/v2/hook/x", "INVALID_AUTHORITY"),
        ("https://open.larksuite.com/open-apis/bot/v2/hook/", "INVALID_PATH"),
    ):
        with pytest.raises(LarkConfigurationError, match=reason) as error:
            LarkNotifier(value, retry_delay_seconds=0)
        assert "test-token" not in str(error.value)
        assert value not in str(error.value)


def test_card_shape_is_bounded_and_secret_safe():
    card = LarkNotifier.build_card("盘前计划", ["600519.SH 贵州茅台", "触发区 100-102"], "turquoise")
    assert card["msg_type"] == "interactive"
    assert card["card"]["header"]["template"] == "turquoise"
    assert card["card"]["elements"][0]["text"]["content"].startswith("600519.SH")
    with pytest.raises(ValueError, match="UNSAFE"):
        LarkNotifier.build_card("x", "https://open.larksuite.com/open-apis/bot/v2/hook/token")


def test_http_429_retries_once_then_succeeds_without_response_text():
    calls: list[int] = []

    def opener(_request, *, timeout):
        assert timeout == 2
        calls.append(1)
        if len(calls) == 1:
            raise HTTPError(URL, 429, "secret response text", {}, None)
        return _Response(json.dumps({"code": 0, "msg": "success"}).encode())

    result = LarkNotifier(URL, timeout_seconds=2, opener=opener, retry_delay_seconds=0).send("A4", "BUY_SIGNAL", "green")
    assert result.ok is True
    assert result.reason_code == "LARK_SENT"
    assert result.http_status == 200
    assert result.attempts == 2
    assert len(calls) == 2


def test_api_rejection_is_safe_and_does_not_retry_non_transient_error():
    calls: list[int] = []

    def opener(_request, **_kwargs):
        calls.append(1)
        return _Response(json.dumps({"code": 19001, "msg": "not persisted"}).encode())

    result = LarkNotifier(URL, opener=opener, retry_delay_seconds=0).send("A4", "DATA_BLOCK", "red")
    assert result.ok is False
    assert result.reason_code == "LARK_API_REJECTED"
    assert result.attempts == 1
    assert len(calls) == 1


def test_missing_webhook_is_a_non_throwing_disabled_client():
    result = LarkNotifier(None).send("test", "disabled")
    assert result.ok is False
    assert result.reason_code == "LARK_WEBHOOK_NOT_CONFIGURED"
    assert result.attempts == 0
