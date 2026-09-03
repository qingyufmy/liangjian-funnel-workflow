"""Synchronous, secret-safe Lark bot delivery for workflow summaries."""

from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

from .state import NOTIFICATION_CARD_COLORS

_PREFIX = "/open-apis/bot/v2/hook/"
_TOKEN = re.compile(r"^[^/?#\\]+$")
_SECRET = re.compile(r"\b(?:sk|ghp|xoxb|xapp)-[A-Za-z0-9_-]{12,}\b", re.I)
_WEBHOOK = re.compile(r"open\.larksuite\.com/open-apis/bot/v2/hook", re.I)


class LarkConfigurationError(ValueError):
    def __init__(self, reason_code: str):
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True, slots=True)
class LarkDeliveryResult:
    ok: bool
    reason_code: str
    http_status: int | None = None
    attempts: int = 0

    @property
    def delivered(self) -> bool:
        return self.ok


def _secret(value: Any) -> str:
    getter = getattr(value, "get_secret_value", None)
    if callable(getter):
        value = getter()
    return value if isinstance(value, str) else ""


def _validate(value: Any) -> str:
    raw = _secret(value).strip()
    if not raw:
        raise LarkConfigurationError("LARK_WEBHOOK_MISSING")
    if len(raw) > 2048 or any(ord(char) < 32 or ord(char) == 127 for char in raw):
        raise LarkConfigurationError("LARK_WEBHOOK_INVALID_FORMAT")
    try:
        parsed = urllib.parse.urlsplit(raw)
        host, port = parsed.hostname, parsed.port
    except (TypeError, ValueError):
        raise LarkConfigurationError("LARK_WEBHOOK_INVALID_FORMAT") from None
    if parsed.scheme.lower() != "https":
        raise LarkConfigurationError("LARK_WEBHOOK_HTTPS_REQUIRED")
    if host is None or host.lower() != "open.larksuite.com":
        raise LarkConfigurationError("LARK_WEBHOOK_INVALID_HOST")
    if port is not None or parsed.username is not None or parsed.password is not None:
        raise LarkConfigurationError("LARK_WEBHOOK_INVALID_AUTHORITY")
    if parsed.query or parsed.fragment or not parsed.path.startswith(_PREFIX):
        raise LarkConfigurationError("LARK_WEBHOOK_INVALID_PATH")
    token = parsed.path[len(_PREFIX) :]
    if not token or not _TOKEN.fullmatch(urllib.parse.unquote(token)):
        raise LarkConfigurationError("LARK_WEBHOOK_INVALID_PATH")
    return raw


def validate_webhook_url(value: Any) -> bool:
    _validate(value)
    return True


def rotate_color(previous: str | None = None) -> str:
    try:
        index = NOTIFICATION_CARD_COLORS.index(str(previous).lower())
    except ValueError:
        index = -1
    return NOTIFICATION_CARD_COLORS[(index + 1) % len(NOTIFICATION_CARD_COLORS)]


def _text(value: Any, limit: int, code: str) -> str:
    if not isinstance(value, str):
        raise ValueError(code)
    value = value.strip()
    if _WEBHOOK.search(value) or _SECRET.search(value):
        raise ValueError(code.replace("_INVALID", "_UNSAFE"))
    if not value or len(value) > limit:
        raise ValueError(code)
    return value


def _card_safe(value: Any) -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            if any(part in str(key).lower() for part in ("webhook", "secret", "api_key", "token")):
                raise ValueError("LARK_CARD_UNSAFE")
            _card_safe(child)
    elif isinstance(value, (list, tuple)):
        for child in value:
            _card_safe(child)
    elif isinstance(value, str) and (_WEBHOOK.search(value) or _SECRET.search(value)):
        raise ValueError("LARK_CARD_UNSAFE")


class LarkNotifier:
    """POST one card; retry one time only for HTTP 429/5xx."""

    def __init__(
        self,
        webhook_url: Any,
        *,
        timeout_seconds: float = 8.0,
        opener: Callable[..., Any] | None = None,
        retry_delay_seconds: float = 0.2,
    ):
        raw = _secret(webhook_url).strip()
        self._url = _validate(raw) if raw else None
        try:
            self._timeout = float(timeout_seconds)
            self._delay = float(retry_delay_seconds)
        except (TypeError, ValueError):
            raise ValueError("LARK_TIMEOUT_INVALID") from None
        if not 0.1 <= self._timeout <= 30 or not 0 <= self._delay <= 5:
            raise ValueError("LARK_TIMEOUT_INVALID")
        self._opener = opener or urllib.request.urlopen

    @property
    def enabled(self) -> bool:
        return self._url is not None

    @staticmethod
    def build_card(title: str, body: str | Sequence[str], color: str = "blue") -> dict[str, Any]:
        title = _text(title, 128, "LARK_CARD_TITLE_INVALID")
        if isinstance(body, str):
            body_text = _text(body, 16_000, "LARK_CARD_BODY_INVALID")
        elif isinstance(body, Sequence):
            parts: list[str] = []
            for item in body:
                # Empty strings are intentional paragraph separators in the
                # structured cards. Every non-empty line still uses the same
                # type, size, and secret validation below.
                if isinstance(item, str) and not item.strip():
                    parts.append("")
                    continue
                parts.append(_text(item, 2_000, "LARK_CARD_BODY_INVALID"))
            body_text = "\n".join(parts)
            if not body_text.strip():
                raise ValueError("LARK_CARD_BODY_INVALID")
        else:
            raise ValueError("LARK_CARD_BODY_INVALID")
        color = str(color or "").strip().lower()
        if color not in NOTIFICATION_CARD_COLORS:
            raise ValueError("LARK_CARD_COLOR_INVALID")
        return {
            "msg_type": "interactive",
            "card": {
                "header": {"template": color, "title": {"tag": "plain_text", "content": title}},
                "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": body_text}}],
            },
        }

    def send(self, title: str, body: str | Sequence[str], color: str = "blue") -> LarkDeliveryResult:
        return self.send_card(self.build_card(title, body, color))

    def send_card(self, card: Mapping[str, Any]) -> LarkDeliveryResult:
        if not self.enabled:
            return LarkDeliveryResult(False, "LARK_WEBHOOK_NOT_CONFIGURED")
        if not isinstance(card, Mapping):
            raise ValueError("LARK_CARD_INVALID")
        _card_safe(card)
        try:
            data = json.dumps(dict(card), ensure_ascii=False, separators=(",", ":")).encode()
        except (TypeError, ValueError):
            raise ValueError("LARK_CARD_INVALID") from None
        if len(data) > 32 * 1024:
            raise ValueError("LARK_CARD_TOO_LARGE")
        request = urllib.request.Request(self._url or "", data=data, method="POST", headers={"Content-Type": "application/json; charset=utf-8"})
        last: LarkDeliveryResult | None = None
        for attempt in (1, 2):
            last = self._attempt(request)
            if last.ok or attempt == 2 or not self._retryable(last):
                return LarkDeliveryResult(last.ok, last.reason_code, last.http_status, attempt)
            if self._delay:
                time.sleep(self._delay)
        return LarkDeliveryResult(False, "LARK_SEND_ERROR", None, 2)

    def _attempt(self, request: urllib.request.Request) -> LarkDeliveryResult:
        response: Any = None
        try:
            response = self._opener(request, timeout=self._timeout)
            status = getattr(response, "status", None)
            status = int(status if status is not None else response.getcode())
            raw = response.read(65_537)
            if len(raw) > 65_536:
                return LarkDeliveryResult(False, "LARK_RESPONSE_TOO_LARGE", status, 1)
            try:
                result = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, TypeError, ValueError):
                return LarkDeliveryResult(False, "LARK_INVALID_RESPONSE", status, 1)
            if not isinstance(result, Mapping):
                return LarkDeliveryResult(False, "LARK_INVALID_RESPONSE", status, 1)
            code = result.get("code", result.get("StatusCode"))
            if 200 <= status < 300 and str(code) == "0":
                return LarkDeliveryResult(True, "LARK_SENT", status, 1)
            if 200 <= status < 300:
                return LarkDeliveryResult(False, "LARK_API_REJECTED" if code is not None else "LARK_INVALID_RESPONSE", status, 1)
            return LarkDeliveryResult(False, self._http_reason(status), status, 1)
        except urllib.error.HTTPError as error:
            status = int(error.code) if getattr(error, "code", None) is not None else None
            return LarkDeliveryResult(False, self._http_reason(status), status, 1)
        except TimeoutError:
            return LarkDeliveryResult(False, "LARK_TIMEOUT", None, 1)
        except (urllib.error.URLError, OSError):
            return LarkDeliveryResult(False, "LARK_NETWORK_ERROR", None, 1)
        except Exception:
            return LarkDeliveryResult(False, "LARK_SEND_ERROR", None, 1)
        finally:
            if response is not None:
                try:
                    response.close()
                except Exception:
                    pass

    @staticmethod
    def _http_reason(status: int | None) -> str:
        if status == 429 or status is not None and 500 <= status <= 599:
            return "LARK_HTTP_RETRYABLE"
        if status is not None and 400 <= status <= 499:
            return "LARK_HTTP_REJECTED"
        return "LARK_HTTP_ERROR"

    @staticmethod
    def _retryable(result: LarkDeliveryResult) -> bool:
        return result.reason_code == "LARK_HTTP_RETRYABLE"


__all__ = ["LarkConfigurationError", "LarkDeliveryResult", "LarkNotifier", "NOTIFICATION_CARD_COLORS", "rotate_color", "validate_webhook_url"]
