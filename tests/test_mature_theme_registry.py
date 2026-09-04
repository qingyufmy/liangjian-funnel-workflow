from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import yaml

from liangjian_funnel.pipeline.mature_theme_registry import (
    DEFAULT_MATURE_THEME_REGISTRY,
    activate_mature_themes,
    augment_discovery_with_mature_registry,
    resolve_mature_theme_registry,
)
from liangjian_funnel.pipeline.research import _mature_theme_activation_minimum


def test_mature_theme_activation_minimum_handles_default_and_explicit_values() -> None:
    assert _mature_theme_activation_minimum(None) == 5
    assert _mature_theme_activation_minimum(0) == 1
    assert _mature_theme_activation_minimum("7") == 7


def _registry() -> dict:
    return {
        "schema_version": "mature-theme-registry/1.0.0",
        "version": "test-registry/v1",
        "activation": {"minimum_keyword_hits": 1},
        "themes": [
            {
                "canonical_id": "AI_COMPUTE_INFRASTRUCTURE",
                "display_name": "AI算力基础设施",
                "activation_keywords": ["算力", "液冷"],
                "industry_names": ["计算机设备", "计算机设备", "不存在行业"],
                "concept_names": ["数据中心(AIDC)", "不存在概念"],
            },
            {
                "canonical_id": "NATIONAL_DEFENSE",
                "display_name": "国防军工",
                "activation_keywords": ["军工"],
                "industry_names": ["军工装备"],
                "concept_names": ["军工"],
            },
            {
                "canonical_id": "CONSUMER_ELECTRONICS",
                "display_name": "消费电子",
                "activation_keywords": ["消费电子"],
                "industry_names": ["消费电子"],
                "concept_names": ["消费电子概念"],
            },
            {
                "canonical_id": "AGRICULTURE_FOOD_SECURITY",
                "display_name": "农业与粮食安全",
                "activation_keywords": ["农业"],
                "industry_names": ["种植业与林业"],
                "concept_names": ["农业种植"],
            },
            {
                "canonical_id": "FINANCIAL_HIGH_DIVIDEND",
                "display_name": "金融高股息",
                "activation_keywords": ["高股息"],
                "industry_names": ["银行"],
                "concept_names": ["高股息精选"],
            },
            {
                "canonical_id": "INNOVATIVE_MEDICINE_HEALTHCARE",
                "display_name": "创新药与医疗",
                "activation_keywords": ["创新药"],
                "industry_names": ["化学制药"],
                "concept_names": ["创新药"],
            },
        ],
    }


def _industry_catalog() -> dict:
    return {
        "records": [
            {"name": "银行", "thscode": "881155.TI"},
            {"name": "化学制药", "thscode": "881140.TI"},
            {"name": "消费电子", "thscode": "881124.TI"},
            {"name": "军工装备", "thscode": "881166.TI"},
            {"name": "种植业与林业", "thscode": "881101.TI"},
            {"name": "计算机设备", "thscode": "881130.TI"},
            # The duplicate is deliberately the same name and code.
            {"name": "计算机设备", "thscode": "881130.TI"},
        ]
    }


def _concept_catalog() -> dict:
    return {
        "records": [
            {"name": "创新药", "thscode": "886015.TI"},
            {"name": "军工", "thscode": "885700.TI"},
            {"name": "消费电子概念", "thscode": "885800.TI"},
            {"name": "农业种植", "thscode": "885812.TI"},
            {"name": "高股息精选", "thscode": "886072.TI"},
            {"name": "数据中心(AIDC)", "thscode": "885887.TI"},
        ]
    }


def test_config_contains_the_stable_registry_and_strategy_families() -> None:
    config_path = Path(__file__).parents[1] / "config" / "funnel_config_v2.yaml"
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    registry = config["agent_1"]["mature_theme_registry"]
    assert registry["version"] == "mature-theme-registry/2026.09.v1"
    assert len(registry["themes"]) >= 12
    for theme in registry["themes"]:
        assert {
            "canonical_id",
            "display_name",
            "activation_keywords",
            "industry_names",
            "concept_names",
        }.issubset(theme)


def test_resolve_uses_exact_names_deduplicates_and_exposes_unknowns() -> None:
    resolved = resolve_mature_theme_registry(_registry(), _industry_catalog(), _concept_catalog())
    ai = next(item for item in resolved["themes"] if item["canonical_id"] == "AI_COMPUTE_INFRASTRUCTURE")

    assert ai["industry_codes"] == ["881130.TI"]
    assert ai["concept_codes"] == ["885887.TI"]
    assert {item["name"] for item in ai["unresolved"]} == {"不存在行业", "不存在概念"}
    assert {item["name"] for item in resolved["unresolved"]} == {"不存在行业", "不存在概念"}
    assert all(
        item["taxonomy_name"] not in {"不存在行业", "不存在概念"}
        for item in ai["taxonomy_links"]
    )


def test_activation_is_deterministic_and_records_source_references() -> None:
    resolved = resolve_mature_theme_registry(_registry(), _industry_catalog(), _concept_catalog())
    evidence = [
        {"summary": "军工、算力、液冷、消费电子、农业、创新药和高股息均有月度研究证据。", "source_ref": "monthly:2026-09"},
    ]
    first = activate_mature_themes(resolved, evidence)
    second = activate_mature_themes(resolved, list(reversed(evidence)))

    assert len(first["activated_themes"]) >= 5
    assert first == second
    assert "monthly:2026-09" in first["activated_themes"][0]["source_refs"]
    assert set(first["activation_evidence"]) == {
        "AI_COMPUTE_INFRASTRUCTURE",
        "AGRICULTURE_FOOD_SECURITY",
        "CONSUMER_ELECTRONICS",
        "FINANCIAL_HIGH_DIVIDEND",
        "INNOVATIVE_MEDICINE_HEALTHCARE",
        "NATIONAL_DEFENSE",
    }


def test_augmentation_appends_only_validated_links_and_is_idempotent() -> None:
    resolved = resolve_mature_theme_registry(_registry(), _industry_catalog(), _concept_catalog())
    activated = activate_mature_themes(
        resolved,
        {"text": "军工和算力是本月方向", "source_url": "https://evidence.example/monthly"},
    )
    original = {
        "structural_themes": [{"theme_id": "MODEL_THEME", "display_name": "模型原始主题", "raw": "preserve"}],
        "industry_chain_graph": [{"node_id": "MODEL_NODE", "theme_ids": ["MODEL_THEME"], "raw": "preserve"}],
        "taxonomy_links": [],
        "industry_theme_mappings": [],
        "model_field": {"do_not_change": True},
    }
    before = deepcopy(original)
    augmented = augment_discovery_with_mature_registry(original, resolved, activated)

    assert original == before
    assert augmented["model_field"] == before["model_field"]
    assert any(item.get("canonical_id") == "AI_COMPUTE_INFRASTRUCTURE" for item in augmented["structural_themes"])
    assert any(item.get("canonical_id") == "NATIONAL_DEFENSE" for item in augmented["structural_themes"])
    assert all(item["source_refs"] for item in augmented["taxonomy_links"])
    assert all(item["taxonomy_code"] in {"881130.TI", "881166.TI", "885887.TI", "885700.TI"} for item in augmented["taxonomy_links"])
    assert all(item["node_id"].startswith("MTR:") for item in augmented["taxonomy_links"])
    assert not any(item.get("taxonomy_name") in {"不存在行业", "不存在概念"} for item in augmented["taxonomy_links"])
    # A second pass must not duplicate generated themes, nodes or links.
    assert augment_discovery_with_mature_registry(augmented, resolved, activated) == augmented


def test_default_registry_has_no_stock_lists() -> None:
    assert len(DEFAULT_MATURE_THEME_REGISTRY["themes"]) >= 12
    assert all("stocks" not in theme and "symbols" not in theme for theme in DEFAULT_MATURE_THEME_REGISTRY["themes"])
