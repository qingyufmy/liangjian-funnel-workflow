"""Fail-closed extraction of explicit business revenue exposure evidence."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from .feature_store import content_hash


PARSER_VERSION = "business-exposure/1.1.0"
_PERCENT = r"(?P<percent>\d{1,3}(?:\.\d{1,4})?)\s*%"
_FORWARD = re.compile(
    rf"(?P<business>[\u3400-\u9fffA-Za-z（）()·+\-/]{{2,28}}?)"
    rf"(?:业务|产品|板块)(?:收入)?[^。；;\n]{{0,12}}?"
    rf"(?:占(?:公司)?(?:营业|主营业务)?收入|收入占比)[^0-9%]{{0,8}}{_PERCENT}",
    re.IGNORECASE,
)
_REVERSE = re.compile(
    rf"(?P<business>[\u3400-\u9fffA-Za-z（）()·+\-/]{{2,28}}?)"
    rf"(?:业务|产品|板块)(?:收入)?[^。；;\n]{{0,12}}?(?:实现|取得)?(?:营业|主营业务)?收入"
    rf"[^。；;\n]{{0,20}}?{_PERCENT}[^。；;\n]{{0,12}}?(?:占比|占(?:公司)?(?:营业|主营业务)?收入)",
    re.IGNORECASE,
)
_TABLE_SECTION = re.compile(
    r"(?P<section>分行业|分产品)\s+(?P<body>.*?)(?=\s+分行业|\s+分产品|\s+分地区|$)",
    re.IGNORECASE | re.DOTALL,
)
_TABLE_ROW = re.compile(
    r"(?P<business>[\u3400-\u9fffA-Za-z（）()·+\-/]{2,28})\s+"
    r"(?P<amount>\d[\d,]*(?:\.\d+)?)\s+"
    rf"{_PERCENT}",
    re.IGNORECASE,
)


def extract_business_exposure_facts(evidence_by_symbol: Any) -> list[dict[str, Any]]:
    """Return only percentages explicitly disclosed in source evidence.

    The parser deliberately does not infer a percentage from narrative text.
    Every row retains the announcement/page reference and a content hash.
    """

    if not isinstance(evidence_by_symbol, Mapping):
        return []
    facts: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, float]] = set()
    for raw_symbol, payload in evidence_by_symbol.items():
        symbol = str(raw_symbol or "").strip().upper()
        if not symbol or not isinstance(payload, Mapping):
            continue
        evidence = payload.get("evidence")
        if not isinstance(evidence, list):
            continue
        for row in evidence:
            if not isinstance(row, Mapping):
                continue
            text = str(row.get("text") or row.get("content") or "").strip()
            if not text:
                continue
            source_ref = str(row.get("source_ref") or row.get("announcement_id") or "").strip()
            if not source_ref:
                continue
            report_period = str(row.get("report_period") or row.get("publish_time") or "UNKNOWN")[:10]
            matches: list[tuple[re.Match[str], str]] = [
                (match, "NARRATIVE_EXPLICIT_PERCENT") for match in _FORWARD.finditer(text)
            ]
            for section in _TABLE_SECTION.finditer(text):
                body = section.group("body")
                matches.extend(
                    (match, f"REVENUE_COMPOSITION_TABLE_{section.group('section')}")
                    for match in _TABLE_ROW.finditer(body)
                )
            for match, extraction_method in matches:
                business = _clean_business(match.group("business"))
                try:
                    percent = float(match.group("percent"))
                except (TypeError, ValueError):
                    continue
                if not business or not 0 < percent <= 100:
                    continue
                excerpt = (
                    text[match.start():min(len(text), match.end() + 24)].strip()[:240]
                    if extraction_method == "NARRATIVE_EXPLICIT_PERCENT"
                    else match.group(0).strip()[:240]
                )
                if extraction_method == "NARRATIVE_EXPLICIT_PERCENT" and _ambiguous_exposure(excerpt):
                    continue
                key = (symbol, source_ref, business, percent)
                if key in seen:
                    continue
                seen.add(key)
                facts.append({
                    "symbol": symbol,
                    "report_period": report_period or "UNKNOWN",
                    "business_name": business,
                    "revenue_exposure_pct": percent,
                    "gross_profit_exposure_pct": None,
                    "node_id": None,
                    "evidence_ref": source_ref,
                    "page_number": _page_number(row.get("page_number")),
                    "confidence": 1.0,
                    "parser_version": PARSER_VERSION,
                    "extraction_method": extraction_method,
                    "content_hash": content_hash({"source_ref": source_ref, "text": text}),
                    "excerpt": excerpt,
                })
    return sorted(facts, key=lambda item: (item["symbol"], item["evidence_ref"], item["business_name"]))


def _clean_business(value: Any) -> str:
    text = re.sub(r"^[，。；;、：:\s]+|[，。；;、：:\s]+$", "", str(value or ""))
    for prefix in ("公司", "报告期内", "其中", "本期"):
        if text.startswith(prefix) and len(text) > len(prefix) + 1:
            text = text[len(prefix):]
    return text[-36:].strip()


def _ambiguous_exposure(value: str) -> bool:
    """Reject thresholds, applicability templates and other non-exact claims."""

    compact = re.sub(r"\s+", "", value)
    return any(token in compact for token in (
        "超过", "以上", "不低于", "不少于", "低于", "以下", "不超过",
        "约占", "大约", "不适用", "□适用", "☑适用", "适用☑", "适用□",
        "或营业利润",
    ))


def _page_number(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


__all__ = ["PARSER_VERSION", "extract_business_exposure_facts"]
