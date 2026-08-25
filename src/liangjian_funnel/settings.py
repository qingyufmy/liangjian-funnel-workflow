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
    model_timeout_seconds: float = Field(default=600.0, gt=0, le=600)
    model_max_output_tokens: int = Field(default=6_000, ge=1_024, le=32_768)
    hithink_min_request_interval_seconds: float = Field(default=0.5, ge=0, le=10)
    cninfo_min_request_interval_seconds: float = Field(default=0.5, ge=0, le=10)
    cninfo_pdf_max_documents_per_symbol: int = Field(default=3, ge=0, le=10)
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
    cninfo_pdf_cache_dir: Path
    state_db_path: Path
    prompt_dir: Path
    source_config_path: Path
    exchange_rules_path: Path
    news_source_config_path: Path
    open_news_timeout_seconds: float = Field(default=15.0, gt=0, le=60)
    open_news_rss_workers: int = Field(default=16, ge=1, le=40)
    open_news_stock_limit: int = Field(default=20, ge=1, le=50)
    open_news_flash_limit: int = Field(default=50, ge=1, le=100)
    research_max_candidates: int = Field(default=120, ge=1, le=300)
    research_a1_batch_size: int = Field(default=5, ge=1, le=20)
    simulation_initial_cash: float = Field(default=1_000_000.0, ge=0)
    research_models: tuple[str, ...] = RESEARCH_MODELS
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
        cninfo_pdf_cache_raw = env.get("LIANGJIAN_CNINFO_PDF_CACHE_DIR")
        state_db_raw = env.get("LIANGJIAN_STATE_DB_PATH")
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
            model_max_output_tokens=int(env.get("LIANGJIAN_MODEL_MAX_OUTPUT_TOKENS", "6000")),
            hithink_min_request_interval_seconds=float(
                env.get("ASTOCK_HITHINK_MIN_REQUEST_INTERVAL_SECONDS", "0.5")
            ),
            cninfo_min_request_interval_seconds=float(
                env.get("LIANGJIAN_CNINFO_MIN_REQUEST_INTERVAL_SECONDS", "0.5")
            ),
            cninfo_pdf_max_documents_per_symbol=int(
                env.get("LIANGJIAN_CNINFO_PDF_MAX_DOCUMENTS_PER_SYMBOL", "3")
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
            cninfo_pdf_cache_dir=Path(cninfo_pdf_cache_raw).resolve()
            if cninfo_pdf_cache_raw
            else base / "storage" / "cninfo_pdfs",
            state_db_path=Path(state_db_raw).resolve() if state_db_raw else base / "state" / "workflow.sqlite3",
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
            research_max_candidates=int(env.get("LIANGJIAN_RESEARCH_MAX_CANDIDATES", "120")),
            research_a1_batch_size=int(env.get("LIANGJIAN_A1_BATCH_SIZE", "5")),
            simulation_initial_cash=float(env.get("LIANGJIAN_SIMULATION_INITIAL_CASH", "1000000")),
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
            "hithink_min_request_interval_seconds": self.hithink_min_request_interval_seconds,
            "cninfo_min_request_interval_seconds": self.cninfo_min_request_interval_seconds,
            "cninfo_pdf_max_documents_per_symbol": self.cninfo_pdf_max_documents_per_symbol,
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
            "cninfo_pdf_cache_dir": str(self.cninfo_pdf_cache_dir),
            "state_db_path": str(self.state_db_path),
            "prompt_dir": str(self.prompt_dir),
            "source_config_path": str(self.source_config_path),
            "exchange_rules_path": str(self.exchange_rules_path),
            "news_source_config_path": str(self.news_source_config_path),
            "open_news_timeout_seconds": self.open_news_timeout_seconds,
            "open_news_rss_workers": self.open_news_rss_workers,
            "open_news_stock_limit": self.open_news_stock_limit,
            "open_news_flash_limit": self.open_news_flash_limit,
            "research_max_candidates": self.research_max_candidates,
            "research_a1_batch_size": self.research_a1_batch_size,
            "simulation_initial_cash": self.simulation_initial_cash,
            "research_models": list(self.research_models),
            "monitor_model": self.monitor_model,
        }


def _secret(value: str | None) -> SecretStr | None:
    clean = (value or "").strip()
    return SecretStr(clean) if clean else None


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
