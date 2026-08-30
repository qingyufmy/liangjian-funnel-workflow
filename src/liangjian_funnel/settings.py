from __future__ import annotations

import os
import re
import sys
from ipaddress import ip_address
from pathlib import Path
from typing import Mapping
from urllib.parse import urlparse

import yaml
from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationInfo, field_validator


RESEARCH_MODELS = (
    "deepseek-v4-pro-0813",
    "moonshotai/kimi-k3-free",
    "z-ai/glm-5.3-free",
)
MONITOR_MODEL = "deepseek-v4-flash-0731"
ALL_MODELS = (*RESEARCH_MODELS, MONITOR_MODEL)
ENV_KEY = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
DEFAULT_MOOTDX_SERVERS = (
    ("117.34.114.13", 7709),
    ("110.41.147.114", 7709),
    ("8.129.13.54", 7709),
    ("120.24.149.49", 7709),
)


def project_root() -> Path:
    source_root = Path(__file__).resolve().parents[2]
    if (source_root / "config" / "runtime.yaml").is_file():
        return source_root
    working_root = Path.cwd().resolve()
    if (working_root / "config" / "runtime.yaml").is_file():
        return working_root
    installed_root = Path(sys.prefix).resolve() / "share" / "liangjian-funnel"
    if (installed_root / "config" / "runtime.yaml").is_file():
        return installed_root
    return source_root


class Settings(BaseModel):
    model_config = ConfigDict(frozen=True)

    root: Path
    hithink_base_url: str = "https://fuyao.aicubes.cn"
    cninfo_base_url: str = "https://www.cninfo.com.cn"
    gov_policy_base_url: str = "https://sousuo.www.gov.cn"
    model_base_url: str = "https://ai-api.finpoints.tech/v1"
    hithink_api_key: SecretStr | None = Field(default=None, repr=False)
    model_api_key: SecretStr | None = Field(default=None, repr=False)
    timezone: str = "Asia/Shanghai"
    timeout_seconds: float = Field(default=30.0, gt=0, le=120)
    # Keep the production default at ten minutes, while permitting an explicit
    # bounded extension for slower free-model lanes.  The research-level
    # deadline remains the outer guard and prevents unbounded orchestration.
    model_timeout_seconds: float = Field(default=600.0, gt=0, le=1200)
    # ``model_max_output_tokens`` is the primary request budget.  Keep the
    # legacy field/env name so existing deployments can still override it.
    # The gateway models advertise a 1M-token context, so 384K output must be
    # accepted by local validation instead of being rejected at 32K.
    model_max_output_tokens: int = Field(default=393_216, ge=1_024, le=1_000_000)
    # Capacity-specific retries step through these lower budgets.  They are
    # separate from the primary value so every tier is visible in diagnostics.
    model_fallback_output_tokens: int = Field(default=262_144, ge=1_024, le=1_000_000)
    model_secondary_fallback_output_tokens: int = Field(default=131_072, ge=1_024, le=1_000_000)
    # Input prompt budget in tokens; research.py applies its conservative
    # character/token estimate before sending a request.
    model_max_input_tokens: int = Field(default=1_000_000, ge=1_024, le=1_000_000)
    hithink_min_request_interval_seconds: float = Field(default=0.5, ge=0, le=10)
    cninfo_min_request_interval_seconds: float = Field(default=0.5, ge=0, le=10)
    # CNINFO calls are network-bound, but the client still enforces one
    # process-wide request interval.  Keep the worker count bounded because
    # the VM has only two CPUs and less than 1 GiB of available memory.
    cninfo_workers: int = Field(default=4, ge=1, le=16)
    cninfo_pdf_workers: int = Field(default=2, ge=1, le=4)
    cninfo_pdf_max_documents_per_symbol: int = Field(default=3, ge=0, le=10)
    cninfo_pdf_retain_raw: bool = False
    gov_policy_min_request_interval_seconds: float = Field(default=0.5, ge=0, le=10)
    mootdx_servers: tuple[tuple[str, int], ...] = DEFAULT_MOOTDX_SERVERS
    mootdx_timeout_seconds: float = Field(default=10.0, gt=0, le=30)
    mootdx_page_size: int = Field(default=800, ge=1, le=800)
    mootdx_max_pages: int = Field(default=20, ge=1, le=100)
    mootdx_history_5m_required_bars: int = Field(default=12_240, ge=255, le=80_000)
    minute_cache_dir: Path
    output_dir: Path
    workflow_output_dir: Path
    snapshot_dir: Path
    fact_store_dir: Path
    open_macro_cache_dir: Path
    fact_cache_db_path: Path
    feature_store_db_path: Path
    broker_gold_dir: Path
    cninfo_pdf_cache_dir: Path
    state_db_path: Path
    workflow_progress_path: Path
    research_checkpoint_dir: Path
    prompt_dir: Path
    source_config_path: Path
    exchange_rules_path: Path
    news_source_config_path: Path
    open_news_timeout_seconds: float = Field(default=15.0, gt=0, le=60)
    open_news_rss_workers: int = Field(default=16, ge=1, le=40)
    open_news_stock_limit: int = Field(default=20, ge=1, le=50)
    open_news_flash_limit: int = Field(default=50, ge=1, le=100)
    open_macro_enabled: bool = True
    research_a1_batch_size: int = Field(default=20, ge=1, le=40)
    research_a2_batch_size: int = Field(default=40, ge=1, le=100)
    research_batch_workers: int = Field(default=1, ge=1, le=1)
    research_pipeline_mode: str = "deterministic_v2"
    a1_local_top_n_per_node: int = Field(default=15, ge=1, le=100)
    a1_llm_top_n_per_node: int = Field(default=3, ge=1, le=20)  # deprecated compatibility input
    a1_llm_representatives_per_theme: int = Field(default=8, ge=1, le=30)
    a1_policy_lookback_days: int = Field(default=120, ge=30, le=366)
    a1_policy_document_limit: int = Field(default=60, ge=12, le=120)
    a2_llm_top_n_per_theme: int = Field(default=8, ge=1, le=30)
    research_close_deadline_seconds: int = Field(default=5400, ge=300, le=24 * 3600)
    data_sync_batch_size: int = Field(default=50, ge=1, le=500)
    data_progress_every: int = Field(default=25, ge=1, le=500)
    fundamental_refresh_hours: int = Field(default=24, ge=1, le=24 * 31)
    daily_refresh_hours: int = Field(default=4, ge=1, le=24 * 7)
    a2_capital_flow_enabled: bool = True
    a2_capital_flow_minimum_coverage: float = Field(default=0.90, ge=0.50, le=1.0)
    a2_capital_flow_workers: int = Field(default=16, ge=1, le=32)
    simulation_initial_cash: float = Field(default=1_000_000.0, ge=0)
    # Thinking is an explicit capability of a client role. Research lanes
    # keep it enabled; the independent intraday monitor is intentionally
    # deterministic at the transport level by default.
    research_thinking_enabled: bool = True
    monitor_thinking_enabled: bool = False
    research_models: tuple[str, ...] = RESEARCH_MODELS
    research_primary_lane_id: str = "lane_1"
    # Optional comparison lanes are an audit plane, never a production
    # dependency.  Stable deployments disable this flag so the primary
    # DeepSeek lane owns A1 -> A2 -> A3 without Kimi/GLM consuming runtime.
    comparison_enabled: bool = True
    publish_comparison_lanes: bool = False
    monitor_model: str = MONITOR_MODEL

    @field_validator("hithink_base_url", "cninfo_base_url", "gov_policy_base_url", "model_base_url")
    @classmethod
    def https_only(cls, value: str, info: ValidationInfo) -> str:
        parsed = urlparse(value)
        if parsed.scheme != "https" or not parsed.netloc:
            raise ValueError("capability endpoints must use HTTPS")
        allowed = {
            "hithink_base_url": {"fuyao.aicubes.cn"},
            "cninfo_base_url": {"www.cninfo.com.cn"},
            "gov_policy_base_url": {"sousuo.www.gov.cn"},
            "model_base_url": {"ai-api.finpoints.tech"},
        }
        if parsed.hostname not in allowed.get(info.field_name, set()):
            raise ValueError(f"unapproved capability host for {info.field_name}")
        return value.rstrip("/")

    @field_validator("mootdx_servers")
    @classmethod
    def valid_mootdx_servers(cls, value: tuple[tuple[str, int], ...]) -> tuple[tuple[str, int], ...]:
        if not value:
            raise ValueError("at least one mootdx server is required")
        for host, port in value:
            ip_address(host)
            if not 1 <= port <= 65535:
                raise ValueError("invalid mootdx port")
        return value

    @field_validator("research_pipeline_mode")
    @classmethod
    def valid_research_pipeline_mode(cls, value: str) -> str:
        normalized = str(value or "").strip().lower()
        if normalized not in {"legacy", "deterministic_v2"}:
            raise ValueError("research_pipeline_mode must be legacy or deterministic_v2")
        return normalized

    @field_validator("research_primary_lane_id")
    @classmethod
    def valid_primary_lane(cls, value: str) -> str:
        normalized = str(value or "").strip().lower()
        if normalized not in {"lane_1", "lane_2", "lane_3"}:
            raise ValueError("research_primary_lane_id must be lane_1, lane_2 or lane_3")
        return normalized

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
        *,
        root: Path | None = None,
    ) -> "Settings":
        process_env = dict(os.environ if environ is None else environ)
        base = Path(root or process_env.get("LIANGJIAN_ROOT") or project_root()).resolve()
        if environ is None:
            env = {**load_dotenv(base / ".env"), **process_env}
        else:
            env = process_env
        output_raw = env.get("LIANGJIAN_OUTPUT_DIR")
        workflow_output_raw = env.get("LIANGJIAN_WORKFLOW_OUTPUT_DIR")
        snapshot_raw = env.get("LIANGJIAN_SNAPSHOT_DIR")
        fact_store_raw = env.get("LIANGJIAN_FACT_STORE_DIR")
        open_macro_cache_raw = env.get("LIANGJIAN_OPEN_MACRO_CACHE_DIR")
        fact_cache_db_raw = env.get("LIANGJIAN_FACT_CACHE_DB_PATH")
        feature_store_db_raw = env.get("LIANGJIAN_FEATURE_STORE_DB_PATH")
        broker_gold_dir_raw = env.get("LIANGJIAN_BROKER_GOLD_DIR")
        cninfo_pdf_cache_raw = env.get("LIANGJIAN_CNINFO_PDF_CACHE_DIR")
        state_db_raw = env.get("LIANGJIAN_STATE_DB_PATH")
        progress_path_raw = env.get("LIANGJIAN_WORKFLOW_PROGRESS_PATH")
        checkpoint_dir_raw = env.get("LIANGJIAN_RESEARCH_CHECKPOINT_DIR")
        prompt_raw = env.get("LIANGJIAN_PROMPT_DIR")
        source_config_raw = env.get("LIANGJIAN_SOURCE_CONFIG_PATH")
        exchange_rules_raw = env.get("LIANGJIAN_EXCHANGE_RULES_PATH")
        news_source_config_raw = env.get("LIANGJIAN_NEWS_SOURCE_CONFIG_PATH")
        default_prompt = base / "prompts"
        return cls(
            root=base,
            hithink_base_url=env.get("ASTOCK_HITHINK_BASE_URL", "https://fuyao.aicubes.cn"),
            cninfo_base_url=env.get("LIANGJIAN_CNINFO_BASE_URL", "https://www.cninfo.com.cn"),
            gov_policy_base_url=env.get("LIANGJIAN_GOV_POLICY_BASE_URL", "https://sousuo.www.gov.cn"),
            model_base_url=env.get("LIANGJIAN_MODEL_BASE_URL", "https://ai-api.finpoints.tech/v1"),
            hithink_api_key=_secret(env.get("HITHINK_FINANCE_API_KEY")),
            model_api_key=_secret(env.get("LIANGJIAN_MODEL_API_KEY")),
            timezone=env.get("LIANGJIAN_TIMEZONE", "Asia/Shanghai"),
            timeout_seconds=float(env.get("LIANGJIAN_HTTP_TIMEOUT_SECONDS", "30")),
            model_timeout_seconds=float(env.get("LIANGJIAN_MODEL_TIMEOUT_SECONDS", "600")),
            model_max_output_tokens=int(env.get("LIANGJIAN_MODEL_MAX_OUTPUT_TOKENS", "393216")),
            model_fallback_output_tokens=int(
                env.get("LIANGJIAN_MODEL_FALLBACK_OUTPUT_TOKENS", "262144")
            ),
            model_secondary_fallback_output_tokens=int(
                env.get("LIANGJIAN_MODEL_SECONDARY_FALLBACK_OUTPUT_TOKENS", "131072")
            ),
            model_max_input_tokens=int(env.get("LIANGJIAN_MODEL_MAX_INPUT_TOKENS", "1000000")),
            hithink_min_request_interval_seconds=float(
                env.get("ASTOCK_HITHINK_MIN_REQUEST_INTERVAL_SECONDS", "0.5")
            ),
            cninfo_min_request_interval_seconds=float(
                env.get("LIANGJIAN_CNINFO_MIN_REQUEST_INTERVAL_SECONDS", "0.5")
            ),
            cninfo_workers=int(env.get("LIANGJIAN_CNINFO_WORKERS", "4")),
            cninfo_pdf_workers=int(env.get("LIANGJIAN_CNINFO_PDF_WORKERS", "2")),
            cninfo_pdf_max_documents_per_symbol=int(
                env.get("LIANGJIAN_CNINFO_PDF_MAX_DOCUMENTS_PER_SYMBOL", "3")
            ),
            cninfo_pdf_retain_raw=_parse_bool(
                env.get("LIANGJIAN_CNINFO_PDF_RETAIN_RAW"),
                default=False,
            ),
            gov_policy_min_request_interval_seconds=float(
                env.get("LIANGJIAN_GOV_POLICY_MIN_REQUEST_INTERVAL_SECONDS", "0.5")
            ),
            mootdx_servers=_parse_mootdx_servers(env.get("MOOTDX_SERVERS")),
            mootdx_timeout_seconds=float(env.get("MOOTDX_TIMEOUT_SECONDS", "10")),
            mootdx_page_size=int(env.get("MOOTDX_PAGE_SIZE", "800")),
            mootdx_max_pages=int(env.get("MOOTDX_MAX_PAGES", "20")),
            mootdx_history_5m_required_bars=int(env.get("MOOTDX_HISTORY_5M_REQUIRED_BARS", "12240")),
            minute_cache_dir=Path(env["LIANGJIAN_MINUTE_CACHE_DIR"]).resolve()
            if env.get("LIANGJIAN_MINUTE_CACHE_DIR")
            else base / "storage" / "minute",
            output_dir=Path(output_raw).resolve() if output_raw else base / "outputs" / "capabilities",
            workflow_output_dir=Path(workflow_output_raw).resolve()
            if workflow_output_raw
            else base / "outputs",
            snapshot_dir=Path(snapshot_raw).resolve() if snapshot_raw else base / "storage" / "snapshots",
            fact_store_dir=Path(fact_store_raw).resolve()
            if fact_store_raw
            else base / "storage" / "facts",
            open_macro_cache_dir=Path(open_macro_cache_raw).resolve()
            if open_macro_cache_raw
            else base / "storage" / "facts" / "open_macro",
            fact_cache_db_path=Path(fact_cache_db_raw).resolve()
            if fact_cache_db_raw
            else base / "storage" / "facts" / "market_fact_cache.sqlite3",
            feature_store_db_path=Path(feature_store_db_raw).resolve()
            if feature_store_db_raw
            else base / "storage" / "features" / "research_feature_store.sqlite3",
            broker_gold_dir=Path(broker_gold_dir_raw).resolve()
            if broker_gold_dir_raw
            else base / "storage" / "benchmarks" / "broker_gold",
            cninfo_pdf_cache_dir=Path(cninfo_pdf_cache_raw).resolve()
            if cninfo_pdf_cache_raw
            else base / "storage" / "cninfo_pdfs",
            state_db_path=Path(state_db_raw).resolve() if state_db_raw else base / "state" / "workflow.sqlite3",
            workflow_progress_path=Path(progress_path_raw).resolve()
            if progress_path_raw
            else base / "state" / "workflow_progress.json",
            research_checkpoint_dir=Path(checkpoint_dir_raw).resolve()
            if checkpoint_dir_raw
            else base / "state" / "research_checkpoints",
            prompt_dir=Path(prompt_raw).resolve() if prompt_raw else default_prompt.resolve(),
            source_config_path=Path(source_config_raw).resolve()
            if source_config_raw
            else base / "config" / "funnel_config_v2.yaml",
            exchange_rules_path=Path(exchange_rules_raw).resolve()
            if exchange_rules_raw
            else base / "config" / "exchange_rules.yaml",
            news_source_config_path=Path(news_source_config_raw).resolve()
            if news_source_config_raw
            else base / "config" / "news_sources.json",
            open_news_timeout_seconds=float(env.get("LIANGJIAN_OPEN_NEWS_TIMEOUT_SECONDS", "15")),
            open_news_rss_workers=int(env.get("LIANGJIAN_OPEN_NEWS_RSS_WORKERS", "16")),
            open_news_stock_limit=int(env.get("LIANGJIAN_OPEN_NEWS_STOCK_LIMIT", "20")),
            open_news_flash_limit=int(env.get("LIANGJIAN_OPEN_NEWS_FLASH_LIMIT", "50")),
            open_macro_enabled=_parse_bool(env.get("LIANGJIAN_OPEN_MACRO_ENABLED"), default=True),
            research_a1_batch_size=int(env.get("LIANGJIAN_A1_BATCH_SIZE", "20")),
            research_a2_batch_size=int(env.get("LIANGJIAN_A2_BATCH_SIZE", "40")),
            # Production research is deliberately serial on the 4 GiB VM.
            # Ignore legacy worker overrides so deployment configuration
            # cannot silently re-enable concurrent prompt construction.
            research_batch_workers=1,
            research_pipeline_mode=env.get("LIANGJIAN_RESEARCH_PIPELINE_MODE", "deterministic_v2"),
            a1_local_top_n_per_node=int(env.get("LIANGJIAN_A1_LOCAL_TOP_N_PER_NODE", "15")),
            a1_llm_top_n_per_node=int(env.get("LIANGJIAN_A1_LLM_TOP_N_PER_NODE", "3")),
            a1_llm_representatives_per_theme=int(
                env.get("LIANGJIAN_A1_LLM_REPRESENTATIVES_PER_THEME", "8")
            ),
            a1_policy_lookback_days=int(env.get("LIANGJIAN_A1_POLICY_LOOKBACK_DAYS", "120")),
            a1_policy_document_limit=int(env.get("LIANGJIAN_A1_POLICY_DOCUMENT_LIMIT", "60")),
            a2_llm_top_n_per_theme=int(env.get("LIANGJIAN_A2_LLM_TOP_N_PER_THEME", "8")),
            research_close_deadline_seconds=int(
                env.get("LIANGJIAN_RESEARCH_CLOSE_DEADLINE_SECONDS", "5400")
            ),
            data_sync_batch_size=int(env.get("LIANGJIAN_DATA_SYNC_BATCH_SIZE", "50")),
            data_progress_every=int(env.get("LIANGJIAN_DATA_PROGRESS_EVERY", "25")),
            fundamental_refresh_hours=int(env.get("LIANGJIAN_FUNDAMENTAL_REFRESH_HOURS", "24")),
            daily_refresh_hours=int(env.get("LIANGJIAN_DAILY_REFRESH_HOURS", "4")),
            a2_capital_flow_enabled=_parse_bool(
                env.get("LIANGJIAN_A2_CAPITAL_FLOW_ENABLED"),
                default=True,
            ),
            a2_capital_flow_minimum_coverage=float(
                env.get("LIANGJIAN_A2_CAPITAL_FLOW_MINIMUM_COVERAGE", "0.90")
            ),
            a2_capital_flow_workers=int(env.get("LIANGJIAN_A2_CAPITAL_FLOW_WORKERS", "16")),
            simulation_initial_cash=float(env.get("LIANGJIAN_SIMULATION_INITIAL_CASH", "1000000")),
            research_thinking_enabled=_parse_bool(env.get("LIANGJIAN_RESEARCH_THINKING_ENABLED"), default=True),
            monitor_thinking_enabled=_parse_bool(env.get("LIANGJIAN_MONITOR_THINKING_ENABLED"), default=False),
            research_primary_lane_id=env.get("LIANGJIAN_RESEARCH_PRIMARY_LANE_ID", "lane_1"),
            comparison_enabled=_parse_bool(
                env.get("LIANGJIAN_COMPARISON_ENABLED"),
                default=True,
            ),
            publish_comparison_lanes=_parse_bool(
                env.get("LIANGJIAN_PUBLISH_COMPARISON_LANES"),
                default=False,
            ),
        )

    def safe_summary(self) -> dict[str, object]:
        return {
            "root": str(self.root),
            "hithink_base_url": self.hithink_base_url,
            "cninfo_base_url": self.cninfo_base_url,
            "gov_policy_base_url": self.gov_policy_base_url,
            "model_base_url": self.model_base_url,
            "hithink_key_present": self.hithink_api_key is not None,
            "model_key_present": self.model_api_key is not None,
            "timezone": self.timezone,
            "timeout_seconds": self.timeout_seconds,
            "model_timeout_seconds": self.model_timeout_seconds,
            "model_max_output_tokens": self.model_max_output_tokens,
            "model_primary_output_tokens": self.model_max_output_tokens,
            "model_fallback_output_tokens": self.model_fallback_output_tokens,
            "model_secondary_fallback_output_tokens": self.model_secondary_fallback_output_tokens,
            "model_max_input_tokens": self.model_max_input_tokens,
            "hithink_min_request_interval_seconds": self.hithink_min_request_interval_seconds,
            "cninfo_min_request_interval_seconds": self.cninfo_min_request_interval_seconds,
            "cninfo_workers": self.cninfo_workers,
            "cninfo_pdf_workers": self.cninfo_pdf_workers,
            "cninfo_pdf_max_documents_per_symbol": self.cninfo_pdf_max_documents_per_symbol,
            "cninfo_pdf_retain_raw": self.cninfo_pdf_retain_raw,
            "gov_policy_min_request_interval_seconds": self.gov_policy_min_request_interval_seconds,
            "mootdx_servers": [f"{host}:{port}" for host, port in self.mootdx_servers],
            "mootdx_timeout_seconds": self.mootdx_timeout_seconds,
            "mootdx_page_size": self.mootdx_page_size,
            "mootdx_max_pages": self.mootdx_max_pages,
            "mootdx_history_5m_required_bars": self.mootdx_history_5m_required_bars,
            "minute_cache_dir": str(self.minute_cache_dir),
            "output_dir": str(self.output_dir),
            "workflow_output_dir": str(self.workflow_output_dir),
            "snapshot_dir": str(self.snapshot_dir),
            "fact_store_dir": str(self.fact_store_dir),
            "open_macro_cache_dir": str(self.open_macro_cache_dir),
            "fact_cache_db_path": str(self.fact_cache_db_path),
            "feature_store_db_path": str(self.feature_store_db_path),
            "broker_gold_dir": str(self.broker_gold_dir),
            "cninfo_pdf_cache_dir": str(self.cninfo_pdf_cache_dir),
            "state_db_path": str(self.state_db_path),
            "workflow_progress_path": str(self.workflow_progress_path),
            "research_checkpoint_dir": str(self.research_checkpoint_dir),
            "prompt_dir": str(self.prompt_dir),
            "source_config_path": str(self.source_config_path),
            "exchange_rules_path": str(self.exchange_rules_path),
            "news_source_config_path": str(self.news_source_config_path),
            "open_news_timeout_seconds": self.open_news_timeout_seconds,
            "open_news_rss_workers": self.open_news_rss_workers,
            "open_news_stock_limit": self.open_news_stock_limit,
            "open_news_flash_limit": self.open_news_flash_limit,
            "open_macro_enabled": self.open_macro_enabled,
            "research_a1_batch_size": self.research_a1_batch_size,
            "research_a2_batch_size": self.research_a2_batch_size,
            "research_batch_workers": self.research_batch_workers,
            "research_pipeline_mode": self.research_pipeline_mode,
            "a1_local_top_n_per_node": self.a1_local_top_n_per_node,
            "a1_llm_top_n_per_node": self.a1_llm_top_n_per_node,
            "a1_llm_representatives_per_theme": self.a1_llm_representatives_per_theme,
            "a1_policy_lookback_days": self.a1_policy_lookback_days,
            "a1_policy_document_limit": self.a1_policy_document_limit,
            "a2_llm_top_n_per_theme": self.a2_llm_top_n_per_theme,
            "research_close_deadline_seconds": self.research_close_deadline_seconds,
            "data_sync_batch_size": self.data_sync_batch_size,
            "data_progress_every": self.data_progress_every,
            "fundamental_refresh_hours": self.fundamental_refresh_hours,
            "daily_refresh_hours": self.daily_refresh_hours,
            "a2_capital_flow_enabled": self.a2_capital_flow_enabled,
            "a2_capital_flow_minimum_coverage": self.a2_capital_flow_minimum_coverage,
            "a2_capital_flow_workers": self.a2_capital_flow_workers,
            "simulation_initial_cash": self.simulation_initial_cash,
            "research_thinking_enabled": self.research_thinking_enabled,
            "monitor_thinking_enabled": self.monitor_thinking_enabled,
            "research_models": list(self.research_models),
            "research_primary_lane_id": self.research_primary_lane_id,
            "comparison_enabled": self.comparison_enabled,
            "publish_comparison_lanes": self.publish_comparison_lanes,
            "monitor_model": self.monitor_model,
        }


def _secret(value: str | None) -> SecretStr | None:
    clean = (value or "").strip()
    return SecretStr(clean) if clean else None


def _parse_bool(value: str | None, *, default: bool) -> bool:
    """Parse an explicit boolean environment flag without silent coercion."""

    if value is None or not value.strip():
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError("boolean environment values must be true/false")


def _parse_mootdx_servers(value: str | None) -> tuple[tuple[str, int], ...]:
    if not (value or "").strip():
        return DEFAULT_MOOTDX_SERVERS
    servers: list[tuple[str, int]] = []
    for item in value.split(","):
        host, separator, raw_port = item.strip().rpartition(":")
        if not separator or not host or not raw_port.isdigit():
            raise ValueError("MOOTDX_SERVERS must be comma-separated ip:port values")
        servers.append((host, int(raw_port)))
    return tuple(servers)


def load_dotenv(path: Path) -> dict[str, str]:
    """Load a small, strict .env file without interpolation or secret logging."""
    if not path.is_file():
        return {}
    result: dict[str, str] = {}
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8-sig").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            raise ValueError(f"invalid .env assignment at line {line_number}")
        key, raw_value = line.split("=", 1)
        key = key.strip()
        if not ENV_KEY.fullmatch(key):
            raise ValueError(f"invalid .env key at line {line_number}")
        if key in result:
            raise ValueError(f"duplicate .env key at line {line_number}")
        value = raw_value.strip()
        if value[:1] in {'\"', "'"}:
            if len(value) < 2 or value[-1] != value[0]:
                raise ValueError(f"unclosed .env quote at line {line_number}")
            value = value[1:-1]
        result[key] = value
    return result


def load_yaml(path: Path) -> dict:
    content = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(content, dict):
        raise ValueError(f"YAML root must be a mapping: {path}")
    return content
