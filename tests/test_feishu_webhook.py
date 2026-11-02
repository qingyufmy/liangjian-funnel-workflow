"""Customer-channel regression coverage using fictional destinations only."""
from datetime import datetime
import json
from urllib.error import URLError

import pytest

from liangjian_funnel.redaction import redact_text, sanitize
from liangjian_funnel.reporting import atomic_write_json
from liangjian_funnel.pipeline.local_fact_cache import _sanitize
from liangjian_funnel.runtime.card_archive import archive_card
from liangjian_funnel.runtime.decision_observability import redact_observability
from liangjian_funnel.runtime.lark import LarkConfigurationError, LarkNotifier, validate_webhook_url
from liangjian_funnel.runtime.team_v2_internal import _body_text


@pytest.fixture(params=["open.feishu.cn", "open.larksuite.com", "OPEN.FEISHU.CN"])
def webhook(request):
    return f"https://{request.param}/open-apis/bot/v2/hook/FAKE_NOT_A_CREDENTIAL"


def test_customer_validation_accepts_both_domains(webhook):
    assert validate_webhook_url(webhook)
    assert LarkNotifier(webhook).enabled


@pytest.mark.parametrize("value,reason", [
    ("http://open.feishu.cn/open-apis/bot/v2/hook/FAKE_TOKEN", "HTTPS_REQUIRED"),
    ("https://open.feishu.cn.evil.test/open-apis/bot/v2/hook/FAKE_TOKEN", "INVALID_HOST"),
    ("https://evil.test/open-apis/bot/v2/hook/FAKE_TOKEN", "INVALID_HOST"),
    ("https://open.feishu.cn:444/open-apis/bot/v2/hook/FAKE_TOKEN", "INVALID_AUTHORITY"),
    ("https://user@open.feishu.cn/open-apis/bot/v2/hook/FAKE_TOKEN", "INVALID_AUTHORITY"),
    ("https://open.feishu.cn/open-apis/bot/v2/hook/FAKE_TOKEN?q=x", "INVALID_PATH"),
    ("https://open.feishu.cn/open-apis/bot/v2/hook/FAKE_TOKEN#x", "INVALID_PATH"),
    ("https://open.feishu.cn/wrong/FAKE_TOKEN", "INVALID_PATH"),
    ("https://open.feishu.cn/open-apis/bot/v2/hook/FAKE%2F_TOKEN", "INVALID_PATH"),
])
def test_customer_validation_stays_strict(value, reason):
    with pytest.raises(LarkConfigurationError, match=reason) as error:
        validate_webhook_url(value)
    assert value not in str(error.value)


def test_card_text_and_arbitrary_card_reject_destinations(webhook):
    for title, body in [(webhook, "body"), ("title", webhook), ("title", ["body", webhook])]:
        with pytest.raises(ValueError, match="UNSAFE"):
            LarkNotifier.build_card(title, body)
    with pytest.raises(ValueError, match="LARK_CARD_UNSAFE"):
        LarkNotifier(webhook).send_card({"card": {"elements": [{"content": webhook}]}})
    with pytest.raises(ValueError, match="INTERNAL_CARD_UNSAFE"):
        _body_text({"msg_type": "text", "content": {"text": webhook}})


def test_reports_and_card_archives_redact_without_mutating_card(tmp_path, webhook):
    card = {"body": f"destination {webhook}", "nested": [webhook]}
    before = json.dumps(card)
    assert redact_text(webhook) == "[REDACTED_WEBHOOK]"
    assert webhook not in json.dumps(sanitize(card))
    assert _sanitize(card)["nested"] == ["[REDACTED_WEBHOOK]"]
    assert redact_observability({"url": webhook, "detail": webhook}) == {
        "url": "[REDACTED_WEBHOOK]", "detail": "[REDACTED_WEBHOOK]",
    }
    report = tmp_path / "report.json"
    atomic_write_json(report, card)
    assert webhook not in report.read_text()
    assert "[REDACTED_WEBHOOK]" in report.read_text()
    result = archive_card(directory=tmp_path, card=card, delivery_key="fake-delivery",
                          source_run_id="fake-run", plan_ids=[], trade_date="2026-11-02",
                          observed_at=datetime.fromisoformat("2026-11-02T17:00:00+08:00"))
    archived = open(result["card_evidence_path"], encoding="utf-8").read()
    assert webhook not in archived
    assert "FAKE_NOT_A_CREDENTIAL" not in archived
    assert "[REDACTED_WEBHOOK]" in archived
    assert json.dumps(card) == before


def test_network_errors_never_expose_destination(webhook):
    def opener(*args, **kwargs):
        raise URLError(webhook)
    result = LarkNotifier(webhook, opener=opener, retry_delay_seconds=0).send("title", "body")
    assert result.reason_code == "LARK_NETWORK_ERROR"
    assert webhook not in repr(result)
