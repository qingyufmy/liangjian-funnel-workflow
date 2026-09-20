from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

from liangjian_funnel.data.provider_governance import ProviderRegistry
from liangjian_funnel.data.supplemental_sidecar import (
    attach_shadow_report,
    compare_shadow_records,
    build_shadow_fact_envelope,
    fallback_eligibility,
    load_supplemental_source_policies,
    normalize_business_profile,
    normalize_opinion_records,
    normalize_quote_record,
    normalize_taxonomy_record,
    run_shadow_collection,
    to_a1_coverage_observation,
)
from liangjian_funnel.pipeline.a1_coverage import A1GapReason


ROOT = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 9, 18, 10, 0, tzinfo=timezone(timedelta(hours=8)))


def test_src_09_sidecar_enablement_never_mutates_formal_candidates_or_orders() -> None:
    policies = load_supplemental_source_policies(ROOT / "config" / "supplemental_sources.yaml")
    policy = policies["shortline_theme_candidate"]
    formal = {"focus_pool": [{"symbol": "300136.SZ"}], "orders": [{"order_id": "o1"}]}
    report = {"status": "ADAPTER_OFFLINE_TESTED", "records": [{"symbol": "300136.SZ"}]}

    disabled = attach_shadow_report(formal, report, policy=policy)
    enabled_shadow = attach_shadow_report(formal, report, policy=replace(policy, enabled=True))

    assert disabled["authoritative"] == enabled_shadow["authoritative"] == formal
    assert disabled["authoritative"] is not formal
    assert enabled_shadow["shadow"]["visible"] is True
    assert enabled_shadow["shadow"]["execution_authority"] is False


def test_src_10_taxonomy_identities_cannot_be_interchanged() -> None:
    kpl = normalize_taxonomy_record({"symbol": "300136.SZ", "name": "消费电子", "first_limit_time": "09:35:01"}, identity_type="KPL_THEME", source_id="fixture")
    concept = normalize_taxonomy_record({"symbol": "300136.SZ", "name": "消费电子"}, identity_type="CATALOG_CONCEPT", source_id="fixture")
    business = normalize_taxonomy_record({"symbol": "300136.SZ", "name": "射频连接器"}, identity_type="BUSINESS_DISCLOSURE", source_id="fixture")

    assert {kpl["identity_type"], concept["identity_type"], business["identity_type"]} == {
        "KPL_THEME", "CATALOG_CONCEPT", "BUSINESS_DISCLOSURE",
    }
    assert kpl["identity_key"] != concept["identity_key"] != business["identity_key"]
    assert kpl["business_ratio"] is None and concept["business_ratio"] is None


def test_src_11_http_file_time_without_trade_date_is_not_tradable() -> None:
    quote = normalize_quote_record(
        {"symbol": "300136.SZ", "latest_price": 58.32, "http_last_modified": "2026-09-18T02:00:00Z"},
        observed_at=NOW,
        source_id="fixture",
    )
    assert quote["status"] == "TIME_UNVERIFIED"
    assert quote["trade_date"] is None
    assert quote["tradable_snapshot"] is False
    assert quote["execution_authority"] is False


def test_src_12_reposts_form_one_source_chain_and_expired_opinion_has_no_authority() -> None:
    original = {
        "author": "研究员甲", "original_url": "https://example.test/original", "text": "需求改善，验证订单增速。",
        "original_published_at": "2026-08-01T09:00:00+08:00", "expires_at": "2026-09-01T00:00:00+08:00",
        "testable_conditions": ["下期订单同比增长"],
    }
    records = [original, *[{**original, "source_url": f"https://repost.test/{index}"} for index in range(9)]]
    normalized = normalize_opinion_records(records, as_of=NOW)
    assert normalized["source_chain_count"] == 1
    assert normalized["raw_copy_count"] == 10
    assert normalized["records"][0]["repost_count"] == 9
    assert normalized["records"][0]["state"] == "EXPIRED"
    assert normalized["records"][0]["testable_conditions"] == ["下期订单同比增长"]
    assert normalized["records"][0]["execution_authority"] is False


def test_src_13_business_parse_failure_keeps_industry_as_classification_only() -> None:
    result = normalize_business_profile(
        {"symbol": "600001.SH", "raw_text": "无法解析的受损文本", "industry": "软件开发", "publish_time": "2026-08-30T18:00:00+08:00"},
        source_id="fixture",
        extraction_succeeded=False,
    )
    assert result["raw_document"]["text"] == "无法解析的受损文本"
    assert result["business_segments"] == []
    assert result["classification_fallback"] == {"label": "软件开发", "identity_type": "INDUSTRY_CLASSIFICATION"}
    assert result["claimed_business_ratio"] is None
    assert result["coverage_observation"]["gap_reason"] == "PARSE_ERROR"
    coverage = to_a1_coverage_observation(result, attempted_at=NOW)
    assert coverage.raw_found is True and coverage.parsed is False
    assert coverage.gap_reason == A1GapReason.PARSE_ERROR.value


def test_supplemental_normalized_record_uses_existing_fact_envelope_contract() -> None:
    policy = load_supplemental_source_policies(ROOT / "config" / "supplemental_sources.yaml")["f10_business_candidate"]
    normalized = normalize_business_profile(
        {"symbol": "600001.SH", "raw_text": "主营软件服务", "industry": "软件开发", "publish_time": "2026-09-17T18:00:00+08:00", "business_segments": [{"name": "软件服务", "ratio": 0.8}]},
        source_id=policy.source_id,
        extraction_succeeded=True,
    )
    fact = build_shadow_fact_envelope(
        policy,
        normalized,
        fact_type="supplemental_business_profile",
        symbol="600001.SH",
        event_time=datetime(2026, 9, 17, 18, 0, tzinfo=NOW.tzinfo),
        publish_time=datetime(2026, 9, 17, 18, 0, tzinfo=NOW.tzinfo),
        fetch_time=NOW,
        ingest_time=NOW,
        source_url="https://example.test/reviewed-export",
    )
    assert fact.schema_version == "liangjian-fact/1.0.0"
    assert fact.source_tier == "T4"
    assert fact.payload["shadow_only"] is True
    assert fact.payload["execution_authority"] is False


def test_src_14_unlicensed_or_unverified_source_cannot_become_fallback() -> None:
    policies = load_supplemental_source_policies(ROOT / "config" / "supplemental_sources.yaml")
    policy = replace(policies["quote_backup_candidate"], enabled=True, shadow_only=False, execution_authority=True)
    registry = ProviderRegistry.load(ROOT / "config" / "capability_specs.yaml")

    verdict = fallback_eligibility(policy, live_acceptance={"semantic_match": True, "pit_verified": True, "coverage_verified": True})
    collection = run_shadow_collection(policy, registry=registry, governor=None, request=None, operation=None)

    assert verdict["eligible"] is False
    assert "LICENSE_NOT_VERIFIED" in verdict["reason_codes"]
    assert collection["status"] == "LIVE_UNVERIFIED"
    assert collection["execution_authority"] is False


def test_shadow_comparison_reports_incremental_fields_conflicts_age_and_cost() -> None:
    report = compare_shadow_records(
        primary=[{"object_id": "300136.SZ", "values": {"price": 58.3, "role": None}, "effective_at": "2026-09-18T09:59:00+08:00"}],
        shadow=[{"object_id": "300136.SZ", "values": {"price": 58.4, "role": "LEADER"}, "effective_at": "2026-09-18T09:58:00+08:00", "latency_ms": 120}],
        as_of=NOW,
    )
    assert report["matched_object_count"] == 1
    assert report["incremental_effective_field_count"] == 1
    assert report["conflict_count"] == 1
    assert report["shadow_age_seconds_max"] == 120
    assert report["resource_cost"]["latency_ms_total"] == 120
    assert report["execution_authority"] is False
