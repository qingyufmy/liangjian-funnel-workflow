"""Bounded, lossless transport for A5 evidence; never truncate business facts."""

from __future__ import annotations

import json
from collections import Counter
from typing import Any, Mapping


PROMPT_LIMIT = 250_000
PROMPT_TARGET = 200_000


class A5ReviewError(ValueError):
    def __init__(self, reason_code: str, *, diagnostics: Mapping[str, Any] | None = None):
        self.reason_code = reason_code
        self.diagnostics = {"reason_code": reason_code, **dict(diagnostics or {})}
        super().__init__(reason_code)


def json_size(value: Any) -> int:
    return len(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str))


def pack_evidence(value: Any) -> Any:
    """Remove repeated column names, preserving every value and missing cell.

    Full dictionaries remain in the immutable archive. Tables are used only
    when they save at least 256 characters; heterogeneous records are valid.
    """
    if isinstance(value, dict):
        return {key: pack_evidence(item) for key, item in value.items()}
    if not isinstance(value, list):
        return value
    items = [pack_evidence(item) for item in value]
    if len(items) < 3 or not all(isinstance(item, dict) for item in items):
        return items
    columns = sorted({key for item in items for key in item})
    rows = [[item.get(key) for key in columns] for item in items]
    missing = [[i, j] for i, item in enumerate(items) for j, key in enumerate(columns) if key not in item]
    table = {"encoding": "a5-column-table/1", "columns": columns, "rows": rows}
    if missing:
        table["missing_cells"] = missing
    return table if json_size(table) + 256 < json_size(items) else items


def render_a5_prompt(prompts: Any, filename: str, projection: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    original_projection = projection
    # Keep decisive facts readable even when the full evidence needs nested
    # lossless dictionaries. This is a duplicate, not a replacement or sample.
    critical = _critical_fact_header(projection)
    prompt = critical + prompts.render(filename, {"A5_FACT_SNAPSHOT": projection})
    original_chars = len(prompt)
    def render(value):
        # Pretty-print indentation grows with nested source audits. Compact
        # JSON changes whitespace only, and avoids opaque packing when enough.
        return critical + prompts.render(filename, {"A5_FACT_SNAPSHOT": json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)})
    prompt = render(projection)
    if len(prompt) > PROMPT_TARGET:
        # Counts, effective events, counterexamples and signal audits are not
        # sampled or discarded. Nested tables only deduplicate field names.
        projection = pack_evidence(projection)
        prompt = render(projection)
    section_sizes = {key: json_size(value) for key, value in projection.items()}
    if len(prompt) > PROMPT_TARGET:
        packed = pack_structure_dictionary(projection)
        candidate = render(packed)
        if len(candidate) < len(prompt):
            projection, prompt = packed, candidate
    if len(prompt) > PROMPT_TARGET:
        packed = pack_string_dictionary(projection)
        candidate = render(packed)
        if len(candidate) < len(prompt):
            projection, prompt = packed, candidate
    if len(prompt) > PROMPT_TARGET:
        packed = pack_key_dictionary(projection)
        candidate = render(packed)
        if len(candidate) < len(prompt):
            prompt = candidate
    diagnostics = {
        "prompt_chars": len(prompt), "unpacked_prompt_chars": original_chars,
        "limit_chars": PROMPT_LIMIT, "target_chars": PROMPT_TARGET,
        "input_hash": original_projection.get("input_hash"),
        "section_chars": section_sizes,
    }
    if len(prompt) > PROMPT_LIMIT:
        raise A5ReviewError("A5_MODEL_CONTEXT_TOO_LARGE", diagnostics=diagnostics)
    return prompt, diagnostics


def _critical_fact_header(projection: dict[str, Any]) -> str:
    if not projection.get("metrics"):
        return ""
    verification = projection.get("independent_verification") or {}
    a2 = verification.get("a2") or {}
    fields = {}
    for plan in (verification.get("a4") or {}).get("plans", []):
        for side in ("cross_source_field_checks", "archived_tdx_field_checks"):
            for name, row in (plan.get(side) or {}).items():
                if not isinstance(row, dict):
                    continue
                total = fields.setdefault(f"{side}:{name}", {
                    "statuses": {}, "compared_count": 0, "mismatch_count": 0, "not_comparable_count": 0})
                status = str(row.get("status", "UNKNOWN"))
                total["statuses"][status] = total["statuses"].get(status, 0) + 1
                for key in ("compared_count", "mismatch_count", "not_comparable_count"):
                    total[key] += row.get(key) or 0
    facts = {"metrics": projection["metrics"], "a2_comparison": {
        key: a2[key] for key in ("candidate_count", "covered_count", "quant_lineage_candidate_count",
        "ranking_comparable_to_production", "ranking_basis", "selected_theme_overlap_count",
        "selected_theme_overlap_ratio", "market_cross_section_status", "scope") if key in a2},
        "a4_field_totals": fields,
        "counterexample_stages": [{key: row.get(key) for key in
            ("symbol", "drop_stage", "has_a3_plan", "has_effective_a4_event", "evidence_id")}
            for row in verification.get("counterexamples", [])]}
    return ("A5权威计数与归因速查（从本次冻结事实原样复制或逐字段加总；详细证据仍完整保留）：\n"
        + json.dumps(facts, ensure_ascii=False, separators=(",", ":"), default=str)
        + "\n观察不是有效信号；适用计划为0不是指标预热失败；不可比较不是不匹配。"
          "主题排名口径不可比时，不据此证明选择正确或错误。历史摘要不得替代本次数字。\n\n")


def pack_key_dictionary(value: Any) -> dict:
    """Shorten repeated object keys reversibly; all values and rows survive."""
    keys = set()
    def collect(node):
        if isinstance(node, dict):
            keys.update(node)
            for child in node.values(): collect(child)
        elif isinstance(node, list):
            for child in node: collect(child)
    collect(value)
    dictionary = sorted(keys)
    aliases = {key: f"k{i}" for i, key in enumerate(dictionary)}
    def encode(node):
        if isinstance(node, dict): return {aliases[key]: encode(child) for key, child in node.items()}
        if isinstance(node, list): return [encode(child) for child in node]
        return node
    return {"encoding": "a5-key-dictionary/1", "keys": dictionary,
            "decoding": "In data only, recursively rename object key kN to keys[N]. Values are unchanged. Then decode the restored nested encodings. No business facts are omitted.",
            "data": encode(value)}


def pack_structure_dictionary(value: Any) -> dict:
    """Intern identical repeated JSON structures without dropping any fact."""
    counts = Counter()
    def key(node):
        if isinstance(node, (list, dict)):
            encoded = json.dumps(node, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
            if len(encoded) >= 80:
                return encoded
        return None
    def walk(node):
        encoded = key(node)
        if encoded is not None:
            counts[encoded] += 1
        if isinstance(node, list):
            for child in node:
                walk(child)
        elif isinstance(node, dict):
            for child in node.values():
                walk(child)
    walk(value)
    # A5 carries the same production/alternate-source checks twice when both
    # sources agree. Two copies already save space for the >=80-char entries.
    keys = sorted(encoded for encoded, count in counts.items() if count >= 2)
    eligible = set(keys)
    indices = {}
    dictionary = []
    def encode(node):
        encoded = key(node)
        if encoded in eligible:
            if encoded not in indices:
                indices[encoded] = len(dictionary)
                dictionary.append(json.loads(encoded))
            return {"$a5_value": indices[encoded]}
        if isinstance(node, list):
            return [encode(child) for child in node]
        if isinstance(node, dict):
            result = {name: encode(child) for name, child in node.items()}
            return {"$a5_value_object": result} if "$a5_value" in node or "$a5_value_object" in node else result
        return node
    data = encode(value)
    return {"encoding": "a5-value-dictionary/1", "dictionary": dictionary,
            "decoding": "Replace {$a5_value:i} with the literal dictionary[i] (do not reinterpret reserved keys inside literal entries); $a5_value_object wraps an original object. Decode before column tables. All stocks and evidence remain.",
            "data": data}


def pack_string_dictionary(value: Any) -> dict:
    """Intern repeated long values, preserving full candidate lineage."""
    counts = Counter()
    def count(node):
        if isinstance(node, str) and len(node) >= 12:
            counts[node] += 1
        elif isinstance(node, dict):
            for child in node.values():
                count(child)
        elif isinstance(node, list):
            for child in node:
                count(child)
    count(value)
    dictionary = sorted(text for text, n in counts.items() if n >= 3 and len(text) > 20)
    indices = {text: i for i, text in enumerate(dictionary)}
    def encode(node):
        if isinstance(node, str) and node in indices:
            return {"$a5_string": indices[node]}
        if isinstance(node, list):
            return [encode(child) for child in node]
        if isinstance(node, dict):
            result = {key: encode(child) for key, child in node.items()}
            return {"$a5_object": result} if "$a5_string" in node or "$a5_object" in node else result
        return node
    return {"encoding": "a5-string-dictionary/1",
            "decoding": "Replace each {$a5_string:i} with dictionary[i]. $a5_object wraps an original object. Decode before interpreting column tables. No facts were removed.",
            "dictionary": dictionary, "data": encode(value)}
