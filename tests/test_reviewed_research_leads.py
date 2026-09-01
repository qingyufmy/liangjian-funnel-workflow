from __future__ import annotations

import copy
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from liangjian_funnel.pipeline.reviewed_research_leads import (
    SCHEMA_VERSION,
    load_reviewed_research_leads,
)


TZ = ZoneInfo("Asia/Shanghai")


def _document() -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "document_id": "public-lead",
        "title": "盘前研究人工审阅",
        "publisher": "公开研究作者",
        "source_urls": ["https://x.com/example/status/1234567890123456789"],
        "publish_time": "2026-09-01T09:02:00+08:00",
        "claimed_data_cutoff": "2026-09-01T08:30:00+08:00",
        "valid_until": "2026-09-08T09:30:00+08:00",
        "methodology_axes": [
            {
                "axis": "SOURCE_RESONANCE",
                "method": "独立来源交叉验证",
                "required_inputs": ["原始来源"],
                "failure_modes": ["重复转述"],
            }
        ],
        "theme_hypotheses": [
            {
                "theme": "先进制造",
                "stance": "VERIFY",
                "rationale": ["产业催化待确认"],
                "required_facts": ["订单"],
                "counter_signals": ["订单下修"],
            }
        ],
        "quality_flags": ["PREOPEN_CURRENT_SESSION_QUOTE_CONFLICT"],
    }


def _write(root: Path, payload: dict, name: str = "lead.json") -> None:
    (root / name).write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def test_loader_is_point_in_time_and_keeps_t3_non_scoring_boundary(tmp_path: Path) -> None:
    _write(tmp_path, _document())

    before = load_reviewed_research_leads(
        tmp_path,
        as_of=datetime(2026, 9, 1, 9, 1, tzinfo=TZ),
    )
    active = load_reviewed_research_leads(
        tmp_path,
        as_of=datetime(2026, 9, 1, 9, 5, tzinfo=TZ),
    )
    expired = load_reviewed_research_leads(
        tmp_path,
        as_of=datetime(2026, 9, 8, 9, 31, tzinfo=TZ),
    )

    assert before["available"] is False
    assert before["future_document_count"] == 1
    assert active["available"] is True
    assert active["evidence_tier"] == "T3"
    assert active["original_fact_authority"] is False
    assert active["direct_stock_selection_allowed"] is False
    assert active["deterministic_score_influence_allowed"] is False
    assert active["fact_substitution_allowed"] is False
    assert active["documents"][0]["quality_flags"] == [
        "PREOPEN_CURRENT_SESSION_QUOTE_CONFLICT"
    ]
    assert expired["available"] is False
    assert expired["expired_document_count"] == 1


def test_loader_rejects_stock_codes_and_direct_selection_fields(tmp_path: Path) -> None:
    stock_code = copy.deepcopy(_document())
    stock_code["theme_hypotheses"][0]["rationale"] = ["关注 600000"]
    _write(tmp_path, stock_code, "stock-code.json")

    direct_field = copy.deepcopy(_document())
    direct_field["stock_list"] = ["示例"]
    _write(tmp_path, direct_field, "direct-field.json")

    result = load_reviewed_research_leads(
        tmp_path,
        as_of=datetime(2026, 9, 1, 9, 5, tzinfo=TZ),
    )

    assert result["available"] is False
    assert result["reason_code"] == "NO_VALID_DOCUMENTS"
    assert result["invalid_document_count"] == 2
    assert {item["reason_code"] for item in result["invalid_documents"]} == {
        "FORBIDDEN_SELECTION_CONTENT",
        "INVALID_DOCUMENT_FIELDS",
    }


def test_premarket_document_requires_explicit_quote_review(tmp_path: Path) -> None:
    payload = _document()
    payload["quality_flags"] = ["PRIMARY_SOURCE_REFS_MISSING"]
    _write(tmp_path, payload)

    result = load_reviewed_research_leads(
        tmp_path,
        as_of=datetime(2026, 9, 1, 9, 5, tzinfo=TZ),
    )

    assert result["available"] is False
    assert result["invalid_documents"][0]["reason_code"] == "PREOPEN_QUOTE_REVIEW_REQUIRED"


def test_repository_document_loads_without_selection_content() -> None:
    root = Path(__file__).resolve().parents[1]
    result = load_reviewed_research_leads(
        root / "config" / "research_leads",
        as_of=datetime(2026, 9, 1, 9, 5, tzinfo=TZ),
    )

    assert result["available"] is True
    assert result["documents"][0]["document_id"] == "cnfinancewatch-premarket-20260901"
    encoded = json.dumps(result, ensure_ascii=False)
    assert "2094591596859973893" in encoded
    assert "浪潮信息" not in encoded
    assert "000977" not in encoded
