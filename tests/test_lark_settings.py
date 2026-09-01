from liangjian_funnel.settings import Settings


def test_lark_webhook_uses_local_state_file_and_not_environment(tmp_path):
    settings = Settings.from_env(
        {"LIANGJIAN_LARK_TIMEOUT_SECONDS": "6"},
        root=tmp_path,
    )

    assert settings.lark_webhook_path == tmp_path / "state" / "lark_webhook.json"
    assert settings.lark_timeout_seconds == 6
    summary = settings.safe_summary()
    assert summary["lark_webhook_present"] is False

    settings.lark_webhook_path.parent.mkdir(parents=True)
    settings.lark_webhook_path.write_text("{}", encoding="utf-8")
    assert settings.safe_summary()["lark_webhook_present"] is True
