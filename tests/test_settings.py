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
    assert settings.model_max_output_tokens == 393_216
    assert settings.model_fallback_output_tokens == 262_144
    assert settings.model_max_input_tokens == 1_000_000
    assert settings.research_a1_batch_size == 20
    assert settings.research_a2_batch_size == 40
    assert settings.research_batch_workers == 2
    assert settings.data_sync_batch_size == 50
    assert settings.cninfo_workers == 4
    assert settings.cninfo_pdf_workers == 2
    assert settings.fundamental_refresh_hours == 24
    assert settings.daily_refresh_hours == 4
    assert settings.research_thinking_enabled is True
    assert settings.monitor_thinking_enabled is False


def test_thinking_flags_are_explicit_and_strict(tmp_path: Path):
    settings = Settings.from_env(
        {
            "LIANGJIAN_RESEARCH_THINKING_ENABLED": "off",
            "LIANGJIAN_MONITOR_THINKING_ENABLED": "ON",
        },
        root=tmp_path,
    )
    assert settings.research_thinking_enabled is False
    assert settings.monitor_thinking_enabled is True

    with pytest.raises(ValueError, match="boolean"):
        Settings.from_env({"LIANGJIAN_MONITOR_THINKING_ENABLED": "maybe"}, root=tmp_path)


def test_model_token_budgets_are_configurable_and_legacy_primary_env_is_preserved(tmp_path: Path):
    settings = Settings.from_env(
        {
            "LIANGJIAN_MODEL_MAX_OUTPUT_TOKENS": "12000",
            "LIANGJIAN_MODEL_FALLBACK_OUTPUT_TOKENS": "8000",
            "LIANGJIAN_MODEL_MAX_INPUT_TOKENS": "900000",
        },
        root=tmp_path,
    )
    assert settings.model_max_output_tokens == 12_000
    assert settings.model_fallback_output_tokens == 8_000
    assert settings.model_max_input_tokens == 900_000
    summary = settings.safe_summary()
    assert summary["model_max_output_tokens"] == 12_000
    assert summary["model_primary_output_tokens"] == 12_000
    assert summary["model_fallback_output_tokens"] == 8_000
    assert summary["model_max_input_tokens"] == 900_000


def test_model_token_budget_validation_accepts_384k_and_rejects_above_context(tmp_path: Path):
    settings = Settings.from_env(
        {
            "LIANGJIAN_MODEL_MAX_OUTPUT_TOKENS": "393216",
            "LIANGJIAN_MODEL_FALLBACK_OUTPUT_TOKENS": "262144",
            "LIANGJIAN_MODEL_MAX_INPUT_TOKENS": "1000000",
        },
        root=tmp_path,
    )
    assert settings.model_max_output_tokens == 393_216
    assert settings.model_fallback_output_tokens == 262_144
    assert settings.model_max_input_tokens == 1_000_000

    with pytest.raises(ValidationError):
        Settings.from_env({"LIANGJIAN_MODEL_MAX_OUTPUT_TOKENS": "1000001"}, root=tmp_path)
    with pytest.raises(ValidationError):
        Settings.from_env({"LIANGJIAN_MODEL_MAX_INPUT_TOKENS": "1000001"}, root=tmp_path)


def test_cninfo_worker_bounds_are_configurable(tmp_path: Path):
    settings = Settings.from_env(
        {
            "LIANGJIAN_CNINFO_WORKERS": "32",
            "LIANGJIAN_CNINFO_PDF_WORKERS": "4",
        },
        root=tmp_path,
    )
    assert settings.cninfo_workers == 32
    assert settings.cninfo_pdf_workers == 4

    with pytest.raises(ValidationError):
        Settings.from_env({"LIANGJIAN_CNINFO_WORKERS": "0"}, root=tmp_path)
    with pytest.raises(ValidationError):
        Settings.from_env({"LIANGJIAN_CNINFO_PDF_WORKERS": "5"}, root=tmp_path)


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
    assert settings.fact_cache_db_path == tmp_path / "storage" / "facts" / "market_fact_cache.sqlite3"
    assert settings.cninfo_pdf_cache_dir == tmp_path / "storage" / "cninfo_pdfs"
    assert settings.cninfo_pdf_max_documents_per_symbol == 3
    assert settings.cninfo_pdf_retain_raw is False
    assert settings.workflow_progress_path == tmp_path / "state" / "workflow_progress.json"
    assert settings.research_checkpoint_dir == tmp_path / "state" / "research_checkpoints"
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
    assert config["universe_gate"]["minimum_daily_turnover_cny"] == 50_000_000
    assert config["universe_gate"]["research_universe_and_tradable_universe_are_separate"] is True


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
