from __future__ import annotations

import copy
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.pipeline.research_consensus import (
    PRIVATE_ATTACHMENT_SOURCE_TYPE,
    SCHEMA_VERSION,
    SOURCE_TYPE,
    load_research_consensus,
    project_a2_research_hypotheses,
)
from liangjian_funnel.settings import Settings


TZ = ZoneInfo("Asia/Shanghai")
AS_OF = datetime(2026, 8, 30, 20, 0, tzinfo=TZ)


def _document(
    document_id: str,
    *,
    publish_time: str = "2026-08-29T10:00:00+08:00",
    effective_month: str = "2026-09",
) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "document_id": document_id,
        "title": f"机构策略 {document_id}",
        "source_url": f"https://finance.example.test/research/{document_id}/202608303859443558.html",
        "publisher": "研究中心",
        "publish_time": publish_time,
        "effective_month": effective_month,
        "source_type": SOURCE_TYPE,
        "institutions": [
            {
                "institution_id": "INST_A",
                "institution_name": "机构 A",
                "market_view": "修复但需要盈利验证",
                "style_tilts": ["QUALITY"],
                "industry_tilts": [
                    {"theme": "先进制造", "stance": "SELECTIVE", "conditions": ["订单改善"]}
                ],
                "conditions": ["流动性稳定"],
                "risks": ["外部扰动"],
            }
        ],
        "consensus_axes": [
            {
                "axis": "盈利验证",
                "stance": "需要确认",
                "confidence": "MEDIUM",
                "supporting_institutions": ["INST_A"],
                "conditions": ["业绩预期不下修"],
                "counter_signals": ["盈利继续下修"],
            }
        ],
        "disagreements": [],
        "verification_plan": [
            {
                "question": "盈利是否改善",
                "required_facts": ["盈利预测", "订单数据"],
                "decision_rule": "若事实改善则维持观察",
            }
        ],
    }


def _write(directory: Path, name: str, payload: dict) -> Path:
    path = directory / name
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def test_point_in_time_month_window_includes_september_at_august_end(tmp_path: Path) -> None:
    _write(tmp_path, "sept.json", _document("sept"))
    _write(
        tmp_path,
        "future.json",
        _document("future", publish_time="2026-08-31T09:00:00+08:00"),
    )
    _write(
        tmp_path,
        "old.json",
        _document("old", effective_month="2026-07"),
    )
    _write(
        tmp_path,
        "too-far.json",
        _document("too-far", effective_month="2026-12"),
    )

    result = load_research_consensus(tmp_path, as_of=AS_OF)

    assert result["available"] is True
    assert result["reason_code"] == "OK"
    assert [item["document_id"] for item in result["documents"]] == ["sept"]
    assert result["coverage"]["effective_months"] == ["2026-09"]
    assert result["invalid_document_count"] == 0


def test_bad_documents_are_isolated_and_no_raw_content_is_exposed(tmp_path: Path) -> None:
    _write(tmp_path, "good.json", _document("good"))
    bad_schema = _document("bad-schema")
    bad_schema["schema_version"] = "wrong/9.9.9"
    bad_schema["title"] = "DO NOT EXPOSE THIS TITLE"
    _write(tmp_path, "bad-schema.json", bad_schema)
    forbidden = _document("forbidden")
    forbidden["institutions"][0]["symbol"] = "600000.SH"
    _write(tmp_path, "forbidden.json", forbidden)

    result = load_research_consensus(tmp_path, as_of=AS_OF)

    assert result["available"] is True
    assert result["reason_code"] == "PARTIAL_DOCUMENTS"
    assert result["invalid_document_count"] == 2
    assert {item["reason_code"] for item in result["invalid_documents"]} == {
        "INVALID_SCHEMA_VERSION",
        "FORBIDDEN_RESEARCH_FIELD",
    }
    encoded = json.dumps(result, ensure_ascii=False)
    assert "DO NOT EXPOSE THIS TITLE" not in encoded
    assert "600000.SH" not in encoded
    assert result["documents"][0]["source_url"].endswith("202608303859443558.html")
    assert all(str(tmp_path) not in value for value in result["source_refs"])


def test_missing_directory_and_naive_as_of_fail_closed(tmp_path: Path) -> None:
    missing = load_research_consensus(tmp_path / "does-not-exist", as_of=AS_OF)
    assert missing["available"] is False
    assert missing["reason_code"] == "RESEARCH_CONSENSUS_DIRECTORY_MISSING"
    assert missing["documents"] == []

    with pytest.raises(ValueError, match="AS_OF_TIMEZONE_REQUIRED"):
        load_research_consensus(tmp_path, as_of=datetime(2026, 8, 30, 20))


def test_documents_are_stably_sorted_and_hash_is_repeatable(tmp_path: Path) -> None:
    first = _document("b", publish_time="2026-08-29T12:00:00+08:00")
    second = _document("a", publish_time="2026-08-29T12:00:00+08:00")
    _write(tmp_path, "z.json", first)
    _write(tmp_path, "a.json", second)

    one = load_research_consensus(tmp_path, as_of=AS_OF)
    two = load_research_consensus(tmp_path, as_of=AS_OF)

    assert [item["document_id"] for item in one["documents"]] == ["a", "b"]
    assert one["source_refs"] == [
        "https://finance.example.test/research/a/202608303859443558.html",
        "https://finance.example.test/research/b/202608303859443558.html",
    ]
    assert one["content_hash"] == two["content_hash"]
    assert one["source_refs"] == two["source_refs"]


def test_output_is_projected_to_allowlist_and_contract_is_fixed(tmp_path: Path) -> None:
    payload = _document("allowlist")
    payload["untrusted_extra"] = {"recommendation": "not allowed"}
    # The forbidden key must invalidate the document, even though it would not
    # otherwise be emitted by the projection.
    _write(tmp_path, "bad-extra.json", payload)
    valid = _document("valid")
    valid["harmless_extra"] = "ignored"
    _write(tmp_path, "valid.json", valid)

    result = load_research_consensus(tmp_path, as_of=AS_OF)

    assert result["available"] is True
    assert result["reason_code"] == "PARTIAL_DOCUMENTS"
    assert len(result["documents"]) == 1
    assert "harmless_extra" not in result["documents"][0]
    assert result["evidence_tier"] == "T2"
    assert result["primary_evidence"] is False
    assert result["viewpoint_only"] is True
    assert result["untrusted_text"] is True
    assert result["direct_stock_selection_allowed"] is False


def test_settings_default_and_environment_override(tmp_path: Path) -> None:
    settings = Settings.from_env({}, root=tmp_path)
    assert settings.research_consensus_dir == tmp_path / "config" / "research_consensus"
    assert settings.safe_summary()["research_consensus_dir"] == str(settings.research_consensus_dir)

    override = tmp_path / "reviewed-consensus"
    overridden = Settings.from_env(
        {"LIANGJIAN_RESEARCH_CONSENSUS_DIR": str(override)},
        root=tmp_path,
    )
    assert overridden.research_consensus_dir == override.resolve()


def test_private_attachment_uses_digest_provenance_without_local_path(tmp_path: Path) -> None:
    payload = _document("private-weekly")
    payload.pop("source_url")
    payload["source_type"] = PRIVATE_ATTACHMENT_SOURCE_TYPE
    payload["source_ref"] = "attachment-sha256:" + "a" * 64
    payload["source_label"] = "weekly-research-20260830.pdf"
    _write(tmp_path, "private.json", payload)

    result = load_research_consensus(tmp_path, as_of=AS_OF)

    assert result["available"] is True
    assert result["source_refs"] == ["attachment-sha256:" + "a" * 64]
    document = result["documents"][0]
    assert document["source_type"] == PRIVATE_ATTACHMENT_SOURCE_TYPE
    assert document["source_label"] == "weekly-research-20260830.pdf"
    assert "source_url" not in document


@pytest.mark.parametrize(
    ("source_ref", "source_label"),
    [
        ("attachment-sha256:bad", "weekly.pdf"),
        ("attachment-sha256:" + "a" * 64, "../weekly.pdf"),
    ],
)
def test_private_attachment_rejects_unverifiable_or_path_provenance(
    tmp_path: Path,
    source_ref: str,
    source_label: str,
) -> None:
    payload = _document("private-invalid")
    payload.pop("source_url")
    payload["source_type"] = PRIVATE_ATTACHMENT_SOURCE_TYPE
    payload["source_ref"] = source_ref
    payload["source_label"] = source_label
    _write(tmp_path, "private.json", payload)

    result = load_research_consensus(tmp_path, as_of=AS_OF)

    assert result["available"] is False
    assert result["reason_code"] == "NO_VALID_DOCUMENTS"


def test_a2_projection_is_compact_and_has_no_scoring_authority(tmp_path: Path) -> None:
    _write(tmp_path, "sept.json", _document("sept"))
    bundle = load_research_consensus(tmp_path, as_of=AS_OF)

    result = project_a2_research_hypotheses(bundle)

    assert result["available"] is True
    assert result["evidence_tier"] == "T2"
    assert result["deterministic_score_influence_allowed"] is False
    assert result["fact_substitution_allowed"] is False
    assert result["out_of_a1_selection_allowed"] is False
    assert result["documents"][0]["theme_hypotheses"] == [
        {
            "theme": "先进制造",
            "stance": "SELECTIVE",
            "conditions": ["订单改善"],
        }
    ]
    assert "market_view" not in json.dumps(result, ensure_ascii=False)
