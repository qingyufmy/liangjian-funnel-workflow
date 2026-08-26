from pathlib import Path

import pytest
from pydantic import ValidationError

from liangjian_funnel.settings import ALL_MODELS, MONITOR_MODEL, RESEARCH_MODELS, Settings, load_dotenv, load_yaml


def test_exact_models_and_safe_summary_do_not_leak_keys(tmp_path: Path):
    secret = "unit-secret-value-not-for-output"
    settings = Settings.from_env(
        {
            "HITHINK_FINANCE_API_KEY": secret,
            "LIANGJIAN_MODEL_API_KEY": secret,
        },
        root=tmp_path,
    )
    assert settings.research_models == RESEARCH_MODELS
    assert settings.monitor_model == MONITOR_MODEL
    assert ALL_MODELS == (*RESEARCH_MODELS, MONITOR_MODEL)
    assert secret not in repr(settings)
    assert secret not in str(settings.safe_summary())
    assert settings.safe_summary()["model_key_present"] is True
    assert settings.model_timeout_seconds == 600
    assert settings.research_max_candidates == 1000
    assert settings.research_a1_batch_size == 20
    assert settings.research_a2_batch_size == 40


def test_only_https_endpoints_are_allowed(tmp_path: Path):
    with pytest.raises(ValidationError, match="HTTPS"):
        Settings(root=tmp_path, output_dir=tmp_path, model_base_url="http://example.test/v1")
    with pytest.raises(ValidationError, match="unapproved"):
        Settings.from_env({"LIANGJIAN_CNINFO_BASE_URL": "https://example.test"}, root=tmp_path)


def test_mootdx_server_override_is_strict_and_cache_stays_under_root(tmp_path: Path):
    settings = Settings.from_env({"MOOTDX_SERVERS": "127.0.0.1:7709,127.0.0.2:7719"}, root=tmp_path)
    assert settings.mootdx_servers == (("127.0.0.1", 7709), ("127.0.0.2", 7719))
    assert settings.minute_cache_dir == tmp_path / "storage" / "minute"
    assert settings.fact_store_dir == tmp_path / "storage" / "facts"
    assert settings.cninfo_pdf_cache_dir == tmp_path / "storage" / "cninfo_pdfs"
    assert settings.cninfo_pdf_max_documents_per_symbol == 3
    assert settings.prompt_dir == tmp_path / "prompts"
    assert settings.source_config_path == tmp_path / "config" / "funnel_config_v2.yaml"

    with pytest.raises(ValueError, match="ip:port"):
        Settings.from_env({"MOOTDX_SERVERS": "not-a-server"}, root=tmp_path)


def test_strict_dotenv_loader_supports_quotes_and_empty_optional_value(tmp_path: Path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "# local only\nHITHINK_FINANCE_API_KEY='secret'\nLIANGJIAN_OUTPUT_DIR=\n",
        encoding="utf-8",
    )
    assert load_dotenv(env_file) == {
        "HITHINK_FINANCE_API_KEY": "secret",
        "LIANGJIAN_OUTPUT_DIR": "",
    }


def test_project_funnel_config_uses_runtime_schema():
    config = load_yaml(Path(__file__).parents[1] / "config" / "funnel_config_v2.yaml")
    assert config["funnel_version"] == "LIANGJIAN_FUNNEL_V2/2.0.0"
    assert set(config["regime_overrides"]) == {
        "TREND_MAINLINE",
        "ROTATION_NO_MAINLINE",
        "RISK_OFF_RETREAT",
        "REPAIR",
    }


@pytest.mark.parametrize(
    "content, message",
    [
        ("NOT_AN_ASSIGNMENT\n", "assignment"),
        ("A=1\nA=2\n", "duplicate"),
        ("A='unterminated\n", "unclosed"),
    ],
)
def test_strict_dotenv_loader_rejects_ambiguous_input(tmp_path: Path, content: str, message: str):
    env_file = tmp_path / ".env"
    env_file.write_text(content, encoding="utf-8")
    with pytest.raises(ValueError, match=message):
        load_dotenv(env_file)
