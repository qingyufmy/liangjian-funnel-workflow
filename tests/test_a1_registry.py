import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.pipeline.a1_registry import (
    A1Registry,
    A1RegistryError,
    a1_global_input_hash,
    build_a1_manifest,
    compute_incremental_scope,
    merge_a1_partitions,
)
from liangjian_funnel.pipeline.model_client import ModelCallResult
from liangjian_funnel.pipeline.prompts import PROMPT_FILENAMES
from liangjian_funnel.pipeline.research import (
    FrozenInputSnapshot,
    ResearchPipeline,
    _restrict_snapshot_to_symbols,
    _with_daily_emotion_overlay,
)
from liangjian_funnel.settings import Settings
from liangjian_funnel.workflow import (
    WorkflowApplication,
    WorkflowError,
    _a1_maintenance_run_id,
    decide_a1_maintenance,
)


TZ = ZoneInfo("Asia/Shanghai")
MODELS = ("deepseek-v4-pro-0813", "moonshotai/kimi-k3-free", "z-ai/glm-5.3-free")


def test_daily_emotion_overlay_adds_hot100_to_a1_without_mutating_sealed_payload() -> None:
    sealed = {
        "active_research_pool": [{"symbol": "600001.SH", "candidate_id": "monthly-1"}],
        "monitor_pool": [{"symbol": "600002.SH"}],
        "rejected_candidates": [],
    }
    snapshot = {
        "g0_candidates": [
            {"symbol": "600001.SH", "name": "月度趋势"},
            {"symbol": "600002.SH", "name": "当日情绪"},
        ],
        "EASTMONEY_HOT100_SNAPSHOT": {
            "available": True,
            "trade_date": "2026-09-03",
            "record_count": 100,
            "records": [
                {"symbol": "600001.SH", "name": "月度趋势", "rank": 20},
                {"symbol": "600002.SH", "name": "当日情绪", "rank": 3},
            ],
        },
    }
    overlaid, summary = _with_daily_emotion_overlay(
        sealed,
        snapshot,
        {"600001.SH", "600002.SH"},
    )
    assert [row["symbol"] for row in sealed["active_research_pool"]] == ["600001.SH"]
    assert {row["symbol"] for row in overlaid["active_research_pool"]} == {"600001.SH", "600002.SH"}
    assert overlaid["monitor_pool"] == []
    assert summary["added_count"] == 1
    assert summary["annotated_count"] == 1
    assert overlaid["daily_emotion_overlay"]["monthly_generation_mutated"] is False


def test_daily_emotion_overlay_disposes_every_hot_row_and_rejects_hard_risk() -> None:
    sealed = {
        "active_research_pool": [{"symbol": "600002.SH", "candidate_id": "monthly-risk"}],
        "monitor_pool": [],
        "rejected_candidates": [],
    }
    snapshot = {
        "g0_candidates": [{"symbol": "600001.SH", "name": "可研究"}],
        "EASTMONEY_HOT100_SNAPSHOT": {
            "available": True,
            "trade_date": "2026-09-03",
            "record_count": 2,
            "records": [
                {"symbol": "600001.SH", "name": "可研究", "rank": 1},
                {"symbol": "600002.SH", "name": "重大风险", "rank": 2},
            ],
        },
        "RISK_EVENTS": {
            "available": True,
            "records": [{"symbol": "600002.SH", "severity": "HIGH", "event_type": "FRAUD"}],
        },
    }

    overlaid, summary = _with_daily_emotion_overlay(sealed, snapshot, {"600001.SH", "600002.SH"})

    assert [row["symbol"] for row in overlaid["active_research_pool"]] == ["600001.SH"]
    assert [row["symbol"] for row in sealed["active_research_pool"]] == ["600002.SH"]
    assert [row["symbol"] for row in overlaid["rejected_candidates"]] == ["600002.SH"]
    assert overlaid["rejected_candidates"][0]["reason_codes"] == ["A1_EMOTION_MAJOR_RISK"]
    assert summary["complete_source_disposition_count"] == 2


def test_a1_maintenance_attempt_ids_do_not_reuse_same_day_feature_binding() -> None:
    first = datetime(2026, 9, 1, 18, 0, 0, 100, tzinfo=TZ)
    retry = datetime(2026, 9, 1, 21, 45, 0, 200, tzinfo=TZ)

    first_id = _a1_maintenance_run_id(first, "FULL")
    retry_id = _a1_maintenance_run_id(retry, "FULL")

    assert first_id != retry_id
    assert first_id.startswith("2026-09-01-a1-full-")
    assert retry_id.startswith("2026-09-01-a1-full-")
    assert _a1_maintenance_run_id(retry, "FULL", explicit_run_id="operator-retry") == "operator-retry"


def _settings(tmp_path: Path) -> Settings:
    return Settings.from_env(
        {
            "LIANGJIAN_MODEL_API_KEY": "model-test-key",
            "LIANGJIAN_RESEARCH_PIPELINE_MODE": "legacy",
        },
        root=tmp_path,
    ).model_copy(update={"research_models": MODELS})


def _prompt_dir(tmp_path: Path) -> Path:
    path = tmp_path / "prompts"
    path.mkdir()
    for filename in PROMPT_FILENAMES:
        path.joinpath(filename).write_text("prompt " + filename, encoding="utf-8")
    return path


class _DownstreamOnlyClient:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def complete(self, model: str, messages, **metadata):
        runtime = json.loads(messages[1]["content"].split("\n", 1)[1])
        stage = runtime["stage"]
        self.calls.append(stage)
        output = {
            "envelope": {
                "schema_version": "test/1",
                "stage_id": {"A2": "AGENT_2", "A3": "AGENT_3"}[stage],
                "status": "OK",
                "input_snapshot_ids": [runtime["snapshot_id"]],
                "model_name": model,
                "config_version": "test-config",
                "prompt_version": "test-prompt",
                "market_regime": "REPAIR",
            }
        }
        symbol = runtime["upstream_symbols"][0]
        if stage == "A2":
            output["focus_pool"] = [{"symbol": symbol, "theme_score": 65}]
        else:
            output["core_watch_pool"] = [{"symbol": symbol, "technical_score": 80, "reward_risk": 3.0}]
        return ModelCallResult(
            model=model,
            output=output,
            prompt_hash=metadata.get("prompt_hash"),
            input_hash=metadata.get("input_hash"),
            latency_ms=1,
            attempts=1,
            thinking_variant="thinking_object",
        )


def _generation_payload(symbols: list[str]) -> dict:
    return {
        "schema_version": "liangjian-a1-registry/1.0.0",
        "lanes": {
            "lane_1": {
                "lane": "lane_1",
                "model": "deepseek-v4-pro-0813",
                "status": "VALIDATED",
                "output": {
                    "active_research_pool": [{"symbol": symbol} for symbol in symbols],
                    "monitor_pool": [],
                    "rejected_candidates": [],
                },
            }
        },
    }


def _generation_contract(symbols: list[str], *, snapshot_id: str = "snapshot-1") -> tuple[dict, dict]:
    payload = _generation_payload(symbols)
    payload.update(
        {
            "generation_id": "placeholder",
            "mode": "FULL",
            "snapshot_id": snapshot_id,
            "snapshot_hash": "a" * 64,
        }
    )
    manifest = build_a1_manifest(
        {
            "g0_symbols": symbols,
            "g0_candidates": [{"symbol": symbol, "research_eligible": True} for symbol in symbols],
        },
        {"lane_1": payload["lanes"]["lane_1"]["output"]},
        mode="FULL",
        snapshot_id=snapshot_id,
        snapshot_hash="a" * 64,
        as_of=datetime(2026, 6, 1, 18, 0, tzinfo=TZ),
    )
    return manifest, payload


def test_a1_schedule_monthly_full_wins_over_weekly_incremental():
    weekday = lambda value: value.weekday() < 5
    first_monday = decide_a1_maintenance(datetime(2026, 6, 1, 18, 0, tzinfo=TZ), weekday)
    assert first_monday is not None
    assert first_monday.mode == "FULL"
    last_friday = decide_a1_maintenance(datetime(2026, 6, 5, 18, 0, tzinfo=TZ), weekday)
    assert last_friday is not None
    assert last_friday.mode == "INCREMENTAL"
    assert decide_a1_maintenance(datetime(2026, 6, 1, 17, 59, tzinfo=TZ), weekday) is None


def test_a1_schedule_bootstraps_full_on_any_trading_day_after_1800():
    weekday = lambda value: value.weekday() < 5
    plan = decide_a1_maintenance(
        datetime(2026, 6, 3, 18, 0, tzinfo=TZ),
        weekday,
        has_active_generation=False,
    )
    assert plan is not None
    assert plan.mode == "FULL"
    assert plan.reason_code == "A1_BOOTSTRAP_FULL_DUE"


def test_a1_schedule_catches_up_a_missed_monthly_full():
    weekday = lambda value: value.weekday() < 5
    plan = decide_a1_maintenance(
        datetime(2026, 6, 3, 18, 0, tzinfo=TZ),
        weekday,
        has_active_generation=True,
        active_full_period="2026-05",
    )
    assert plan is not None
    assert plan.mode == "FULL"
    assert plan.reason_code == "A1_MONTHLY_FULL_CATCHUP_DUE"


def test_a1_registry_activation_is_atomic_and_strict(tmp_path: Path):
    registry = A1Registry(tmp_path / "a1.sqlite3")
    now = datetime(2026, 6, 1, 18, 0, tzinfo=TZ)
    manifest, payload = _generation_contract(["600519.SH"])
    generation = registry.create_generation(
        mode="FULL",
        snapshot_id="snapshot-1",
        snapshot_hash="a" * 64,
        as_of=now,
        manifest=manifest,
        payload=payload,
    )
    payload["generation_id"] = generation.generation_id
    sealed = registry.seal_generation(generation.generation_id, payload=payload, sealed_at=now)
    registry.activate_generation(sealed.generation_id, expected_current_id=None, activated_at=now)
    with pytest.raises(A1RegistryError, match="A1_ACTIVE_POINTER_CONFLICT"):
        registry.activate_generation(sealed.generation_id, expected_current_id=None, activated_at=now)
    assert registry.get_active_generation().generation_id == sealed.generation_id


def test_a1_registry_rejects_an_incomplete_partition(tmp_path: Path):
    registry = A1Registry(tmp_path / "a1.sqlite3")
    now = datetime(2026, 6, 1, 18, 0, tzinfo=TZ)
    manifest, payload = _generation_contract(["600519.SH", "000001.SZ"])
    payload["lanes"]["lane_1"]["output"]["active_research_pool"] = [{"symbol": "600519.SH"}]
    generation = registry.create_generation(
        mode="FULL",
        snapshot_id="snapshot-1",
        snapshot_hash="a" * 64,
        as_of=now,
        manifest=manifest,
        payload=payload,
    )
    payload["generation_id"] = generation.generation_id
    with pytest.raises(A1RegistryError, match="A1_PARTITION_COVERAGE_INCOMPLETE"):
        registry.seal_generation(generation.generation_id, payload=payload, sealed_at=now)


def test_a1_registry_allows_verified_broker_gold_research_outside_g0(tmp_path: Path):
    registry = A1Registry(tmp_path / "a1.sqlite3")
    now = datetime(2026, 9, 2, 18, 0, tzinfo=TZ)
    outside_symbol = "002293.SZ"
    institutional_row = {
        "symbol": outside_symbol,
        "autonomous_partition": "OUTSIDE_G0",
        "coverage_origin": "BROKER_GOLD_T2",
        "reason_codes": [
            "A1_INSTITUTIONAL_DIRECT_ENTRY",
            "A1_INSTITUTIONAL_OUTSIDE_G0",
        ],
        "institutional_coverage": {
            "evidence_tier": "T2",
            "direct_research_entry": True,
        },
    }
    output = {
        "active_research_pool": [
            {"symbol": "600519.SH"},
            {
                "symbol": outside_symbol,
                "selection_basis": "BROKER_GOLD_DIRECT",
                "research_route": "BROKER_GOLD_DIRECT",
                "downstream_trade_eligible": False,
            },
        ],
        "monitor_pool": [],
        "rejected_candidates": [],
        "institutional_coverage_pool": [institutional_row],
    }
    manifest = build_a1_manifest(
        {
            "g0_symbols": ["600519.SH"],
            "g0_candidates": [{"symbol": "600519.SH", "research_eligible": True}],
        },
        {"lane_1": output},
        mode="FULL",
        snapshot_id="snapshot-broker",
        snapshot_hash="b" * 64,
        as_of=now,
    )
    payload = {
        "schema_version": "liangjian-a1-registry/1.0.0",
        "mode": "FULL",
        "snapshot_id": "snapshot-broker",
        "snapshot_hash": "b" * 64,
        "lanes": {
            "lane_1": {
                "lane": "lane_1",
                "model": "deepseek-v4-pro-0813",
                "status": "VALIDATED",
                "output": output,
            }
        },
    }
    generation = registry.create_generation(
        mode="FULL",
        snapshot_id="snapshot-broker",
        snapshot_hash="b" * 64,
        as_of=now,
        manifest=manifest,
        payload=payload,
    )
    payload["generation_id"] = generation.generation_id
    sealed = registry.seal_generation(generation.generation_id, payload=payload, sealed_at=now)
    assert sealed.manifest["outside_g0_research_symbols_by_lane"] == {
        "lane_1": [outside_symbol]
    }


def test_a1_registry_allows_verified_broker_gold_monitor_outside_g0(tmp_path: Path):
    registry = A1Registry(tmp_path / "a1.sqlite3")
    now = datetime(2026, 9, 2, 18, 0, tzinfo=TZ)
    outside_symbol = "002956.SZ"
    institutional_row = {
        "symbol": outside_symbol,
        "autonomous_partition": "OUTSIDE_G0",
        "coverage_origin": "BROKER_GOLD_T2",
        "reason_codes": [
            "A1_INSTITUTIONAL_DIRECT_ENTRY",
            "A1_INSTITUTIONAL_OUTSIDE_G0",
        ],
        "institutional_coverage": {
            "evidence_tier": "T2",
            "direct_research_entry": True,
        },
    }
    output = {
        "active_research_pool": [{"symbol": "600519.SH"}],
        "monitor_pool": [{
            "symbol": outside_symbol,
            "selection_basis": "BROKER_GOLD_DIRECT",
            "research_route": "BROKER_GOLD_DIRECT",
            "downstream_trade_eligible": False,
        }],
        "rejected_candidates": [],
        "institutional_coverage_pool": [institutional_row],
    }
    manifest = build_a1_manifest(
        {
            "g0_symbols": ["600519.SH"],
            "g0_candidates": [{"symbol": "600519.SH", "research_eligible": True}],
        },
        {"lane_1": output},
        mode="FULL",
        snapshot_id="snapshot-broker-monitor",
        snapshot_hash="d" * 64,
        as_of=now,
    )
    payload = {
        "schema_version": "liangjian-a1-registry/1.0.0",
        "mode": "FULL",
        "snapshot_id": "snapshot-broker-monitor",
        "snapshot_hash": "d" * 64,
        "lanes": {
            "lane_1": {
                "lane": "lane_1",
                "model": "deepseek-v4-pro-0813",
                "status": "VALIDATED",
                "output": output,
            }
        },
    }
    generation = registry.create_generation(
        mode="FULL",
        snapshot_id="snapshot-broker-monitor",
        snapshot_hash="d" * 64,
        as_of=now,
        manifest=manifest,
        payload=payload,
    )
    payload["generation_id"] = generation.generation_id
    sealed = registry.seal_generation(generation.generation_id, payload=payload, sealed_at=now)

    assert sealed.manifest["outside_g0_research_symbols_by_lane"] == {
        "lane_1": [outside_symbol]
    }


def test_a1_registry_still_rejects_undeclared_symbol_outside_g0(tmp_path: Path):
    registry = A1Registry(tmp_path / "a1.sqlite3")
    now = datetime(2026, 9, 2, 18, 0, tzinfo=TZ)
    manifest, payload = _generation_contract(["600519.SH"], snapshot_id="snapshot-unknown")
    payload["snapshot_id"] = "snapshot-unknown"
    payload["lanes"]["lane_1"]["output"]["active_research_pool"].append(
        {"symbol": "002293.SZ"}
    )
    generation = registry.create_generation(
        mode="FULL",
        snapshot_id="snapshot-unknown",
        snapshot_hash="a" * 64,
        as_of=now,
        manifest=manifest,
        payload=payload,
    )
    payload["generation_id"] = generation.generation_id
    with pytest.raises(A1RegistryError, match="A1_PARTITION_SYMBOL_INVALID"):
        registry.seal_generation(generation.generation_id, payload=payload, sealed_at=now)


def test_a1_registry_rejects_broker_gold_outside_g0_when_marked_trade_eligible(tmp_path: Path):
    registry = A1Registry(tmp_path / "a1.sqlite3")
    now = datetime(2026, 9, 2, 18, 0, tzinfo=TZ)
    outside_symbol = "002293.SZ"
    output = {
        "active_research_pool": [
            {"symbol": "600519.SH"},
            {
                "symbol": outside_symbol,
                "selection_basis": "BROKER_GOLD_DIRECT",
                "downstream_trade_eligible": True,
            },
        ],
        "monitor_pool": [],
        "rejected_candidates": [],
        "institutional_coverage_pool": [{
            "symbol": outside_symbol,
            "autonomous_partition": "OUTSIDE_G0",
            "coverage_origin": "BROKER_GOLD_T2",
            "reason_codes": [
                "A1_INSTITUTIONAL_DIRECT_ENTRY",
                "A1_INSTITUTIONAL_OUTSIDE_G0",
            ],
            "institutional_coverage": {
                "evidence_tier": "T2",
                "direct_research_entry": True,
            },
        }],
    }
    manifest = build_a1_manifest(
        {"g0_symbols": ["600519.SH"]},
        {"lane_1": output},
        mode="FULL",
        snapshot_id="snapshot-unsafe-broker",
        snapshot_hash="c" * 64,
        as_of=now,
    )
    payload = {
        "schema_version": "liangjian-a1-registry/1.0.0",
        "mode": "FULL",
        "snapshot_id": "snapshot-unsafe-broker",
        "snapshot_hash": "c" * 64,
        "lanes": {"lane_1": {"status": "VALIDATED", "output": output}},
    }
    generation = registry.create_generation(
        mode="FULL",
        snapshot_id="snapshot-unsafe-broker",
        snapshot_hash="c" * 64,
        as_of=now,
        manifest=manifest,
        payload=payload,
    )
    payload["generation_id"] = generation.generation_id
    with pytest.raises(A1RegistryError, match="A1_OUTSIDE_G0_RESEARCH_CONTRACT_INVALID"):
        registry.seal_generation(generation.generation_id, payload=payload, sealed_at=now)


def test_incremental_scope_and_merge_preserve_unmodified_partitions(tmp_path: Path):
    old_data = {
        "g0_symbols": ["600519.SH", "000001.SZ", "300750.SZ"],
        "g0_candidates": [
            {"symbol": "600519.SH", "amount": 1, "research_eligible": True},
            {"symbol": "000001.SZ", "amount": 1, "research_eligible": True},
            {"symbol": "300750.SZ", "amount": 1, "research_eligible": True},
        ],
    }
    old_output = {
        "active_research_pool": [
            {"symbol": "600519.SH", "score": 1},
            {"symbol": "000001.SZ", "score": 1},
        ],
        "monitor_pool": [{"symbol": "300750.SZ", "score": 1}],
        "rejected_candidates": [],
    }
    old_manifest = build_a1_manifest(
        old_data,
        {"lane_1": old_output},
        mode="FULL",
        snapshot_id="old",
        snapshot_hash="a" * 64,
        as_of=datetime(2026, 6, 1, 18, 0, tzinfo=TZ),
    )
    new_data = {
        **old_data,
        "g0_symbols": ["600519.SH", "000001.SZ", "300750.SZ", "601318.SH"],
        "g0_candidates": [
            {"symbol": "600519.SH", "amount": 1, "research_eligible": True},
            {"symbol": "000001.SZ", "amount": 9, "research_eligible": False},
            {"symbol": "300750.SZ", "amount": 1, "research_eligible": True},
            {"symbol": "601318.SH", "amount": 1, "research_eligible": True},
        ],
    }
    scope = compute_incremental_scope(new_data, old_manifest)
    assert scope.added_symbols == ("601318.SH",)
    assert scope.changed_symbols == ("000001.SZ",)
    assert scope.symbols == ("000001.SZ", "601318.SH")
    merged = merge_a1_partitions(
        old_output,
        {
            "active_research_pool": [
                {"symbol": "000001.SZ", "score": 2},
                {"symbol": "601318.SH", "score": 2},
            ],
            "monitor_pool": [],
            "rejected_candidates": [],
        },
        updated_symbols=scope.symbols,
    )
    assert {item["symbol"] for item in merged["active_research_pool"]} == {"600519.SH", "000001.SZ", "601318.SH"}
    assert merged["monitor_pool"] == [{"symbol": "300750.SZ", "score": 1}]
    assert merged["analysis_summary"]["approved_count"] == 3


def test_a1_scope_ignores_short_cycle_fields_but_detects_global_context_change():
    base = {
        "g0_symbols": ["600519.SH"],
        "g0_candidates": [{"symbol": "600519.SH", "research_eligible": True, "amount": 1}],
        "THS_INDUSTRY_MEMBERSHIP": {"records": [{"thscode": "600519.SH", "industry_thscode": "I1"}]},
        "MACRO_POLICY_FEED": {"available": True, "as_of": "2026-06-01", "official_documents": []},
    }
    manifest = build_a1_manifest(
        base,
        {"lane_1": {"active_research_pool": [], "monitor_pool": [], "rejected_candidates": []}},
        mode="FULL",
        snapshot_id="base",
        snapshot_hash="a" * 64,
        as_of=datetime(2026, 6, 1, 18, 0, tzinfo=TZ),
    )
    changed_quote = {
        **base,
        "g0_candidates": [{"symbol": "600519.SH", "research_eligible": True, "amount": 999}],
        "MACRO_POLICY_FEED": {
            "available": True,
            "as_of": "2026-06-07",
            "official_documents": [{"fact_id": "policy-1", "title": "new policy"}],
        },
    }
    scope = compute_incremental_scope(changed_quote, manifest)
    assert scope.changed_symbols == ()
    assert scope.global_input_changed is True
    assert scope.symbols == ()
    assert scope.reason_codes == ("A1_GLOBAL_INPUT_CHANGED",)
    assert a1_global_input_hash(base) != a1_global_input_hash(changed_quote)


def test_a1_scope_indexes_low_frequency_rows_and_changes_only_the_affected_symbol():
    base = {
        "g0_symbols": ["600519.SH", "000001.SZ"],
        "g0_candidates": [
            {"symbol": "600519.SH", "research_eligible": True},
            {"symbol": "000001.SZ", "research_eligible": True},
        ],
        "COMPANY_FUNDAMENTALS": {
            "records": [
                {"symbol": "600519.SH", "roe": 20},
                {"symbol": "000001.SZ", "roe": 10},
            ]
        },
    }
    manifest = build_a1_manifest(
        base,
        {"lane_1": _generation_payload(["600519.SH", "000001.SZ"])["lanes"]["lane_1"]["output"]},
        mode="FULL",
        snapshot_id="base",
        snapshot_hash="a" * 64,
        as_of=datetime(2026, 6, 1, 18, 0, tzinfo=TZ),
    )
    changed = {
        **base,
        "COMPANY_FUNDAMENTALS": {
            "records": [
                {"symbol": "600519.SH", "roe": 21},
                {"symbol": "000001.SZ", "roe": 10},
            ]
        },
    }
    scope = compute_incremental_scope(changed, manifest)
    assert scope.changed_symbols == ("600519.SH",)


def test_incremental_snapshot_projection_preserves_filtered_record_envelopes():
    snapshot = FrozenInputSnapshot(
        snapshot_id="snapshot-scope",
        snapshot_hash="a" * 64,
        data={
            "g0_symbols": ["600519.SH", "000001.SZ"],
            "COMPANY_FUNDAMENTALS": {
                "source": "cache",
                "records": [
                    {"symbol": "600519.SH", "roe": 20},
                    {"symbol": "000001.SZ", "roe": 10},
                ],
            },
            "MAIN_BUSINESS_EVIDENCE": {
                "records": [
                    {"symbol": "600519.SH", "product": "A"},
                    {"symbol": "000001.SZ", "product": "B"},
                ]
            },
        },
    )
    projected = _restrict_snapshot_to_symbols(snapshot, {"600519.SH"})
    assert projected.data["g0_symbols"] == ["600519.SH"]
    assert projected.data["COMPANY_FUNDAMENTALS"]["source"] == "cache"
    assert projected.data["COMPANY_FUNDAMENTALS"]["records"] == [
        {"symbol": "600519.SH", "roe": 20}
    ]
    assert projected.data["MAIN_BUSINESS_EVIDENCE"]["records"] == [
        {"symbol": "600519.SH", "product": "A"}
    ]


def test_a1_registry_rejects_expired_active(tmp_path: Path):
    registry = A1Registry(tmp_path / "a1.sqlite3")
    old = datetime(2026, 6, 1, 18, 0, tzinfo=TZ)
    manifest, payload = _generation_contract(["600519.SH"])
    generation = registry.create_generation(
        mode="FULL",
        snapshot_id="snapshot-1",
        snapshot_hash="a" * 64,
        as_of=old,
        manifest=manifest,
        payload=payload,
    )
    payload["generation_id"] = generation.generation_id
    registry.activate_generation(
        registry.seal_generation(generation.generation_id, payload=payload).generation_id,
        activated_at=old,
    )
    assert registry.require_active(as_of=old + timedelta(days=10)).generation_id == generation.generation_id
    with pytest.raises(A1RegistryError, match="A1_ACTIVE_EXPIRED"):
        registry.require_active(as_of=old + timedelta(days=15))


def test_close_reuses_active_a1_without_an_a1_model_call(tmp_path: Path):
    now = datetime(2026, 6, 1, 15, 10, tzinfo=TZ)
    client = _DownstreamOnlyClient()
    pipeline = ResearchPipeline(
        _settings(tmp_path),
        prompt_repository=_prompt_dir(tmp_path),
        model_client=client,
        now=lambda: now,
    )
    snapshot = {
        "snapshot_id": "close-snapshot",
        "snapshot_hash": "b" * 64,
        "g0": ["600519.SH"],
    }
    active = {
        "generation_id": "a1-full-1",
        "lanes": {
            "lane_1": {
                "lane": "lane_1",
                "model": MODELS[0],
                "status": "VALIDATED",
                "output": {
                    "active_research_pool": [{"symbol": "600519.SH"}, {"symbol": "000001.SZ"}],
                    "monitor_pool": [],
                    "rejected_candidates": [],
                },
            }
        },
    }
    result = pipeline.run_from_active_a1(
        snapshot,
        active,
        models=(MODELS[0],),
        generated_at=now,
        run_id="close-reuse",
    )

    assert result.status == "READY"
    assert client.calls == ["A2", "A3"]
    assert [stage.stage for stage in result.lanes[0].stages] == ["A1", "A2", "A3"]
    assert result.lanes[0].stages[0].diagnostics["executed"] is False
    assert result.lanes[0].stages[0].diagnostics["scope_filtered_count"] == 1
    assert "A1_ACTIVE_SCOPE_FILTERED" in result.lanes[0].stages[0].reason_codes


def test_optional_a2_a3_model_reuses_the_single_canonical_primary_a1(tmp_path: Path):
    now = datetime(2026, 6, 1, 15, 10, tzinfo=TZ)
    client = _DownstreamOnlyClient()
    pipeline = ResearchPipeline(
        _settings(tmp_path),
        prompt_repository=_prompt_dir(tmp_path),
        model_client=client,
        now=lambda: now,
    )
    active = {
        "generation_id": "a1-full-1",
        "lanes": {
            "lane_1": {
                "lane": "lane_1",
                "model": MODELS[0],
                "status": "VALIDATED",
                "output": {
                    "active_research_pool": [{"symbol": "600519.SH"}],
                    "monitor_pool": [],
                    "rejected_candidates": [],
                },
            }
        },
    }
    result = pipeline.run_from_active_a1(
        {"snapshot_id": "close-snapshot", "snapshot_hash": "b" * 64, "g0": ["600519.SH"]},
        active,
        models=(MODELS[1],),
        lane_start_index=2,
        primary_lane_ids=("lane_2",),
        generated_at=now,
        run_id="comparison-reuse",
    )
    assert result.status == "READY"
    assert result.lanes[0].lane == "lane_2"
    assert client.calls == ["A2", "A3"]


@pytest.mark.parametrize("reason", ["A1_ACTIVE_MISSING", "A1_ACTIVE_EXPIRED"])
def test_close_fails_closed_when_active_a1_is_unavailable(tmp_path: Path, reason: str):
    app = object.__new__(WorkflowApplication)
    app.settings = _settings(tmp_path)
    app._ensure_trading_day = lambda *_args, **_kwargs: None

    class _UnavailableA1:
        def require_active(self, **_kwargs):
            raise A1RegistryError(reason)

    app.a1_registry = _UnavailableA1()
    with pytest.raises(WorkflowError, match=reason):
        app.run_research(
            "close",
            as_of=datetime(2026, 6, 1, 15, 10, tzinfo=TZ),
            from_active_a1=True,
        )
