from liangjian_funnel.redaction import redact_text, sanitize


def test_secret_redaction_does_not_corrupt_ordinary_risk_urls():
    url = "https://example.test/people-at-risk-from-policy"

    assert redact_text(url) == url


def test_secret_redaction_still_covers_bounded_keys_and_sensitive_fields():
    assert redact_text("key=sk-example_secret_123") == "key=[REDACTED]"
    assert sanitize({"api_key": "anything", "url": "https://example.test/risk-safe"}) == {
        "api_key": "[REDACTED]",
        "url": "https://example.test/risk-safe",
    }
