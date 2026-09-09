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
    prompt = prompts.render(filename, {"A5_FACT_SNAPSHOT": projection})
    original_chars = len(prompt)
    if original_chars > PROMPT_TARGET:
        # Counts, effective events, counterexamples and signal audits are not
        # sampled or discarded. Nested tables only deduplicate field names.
        projection = pack_evidence(projection)
        prompt = prompts.render(filename, {"A5_FACT_SNAPSHOT": projection})
    if len(prompt) > PROMPT_TARGET:
        packed = pack_string_dictionary(projection)
        candidate = prompts.render(filename, {"A5_FACT_SNAPSHOT": packed})
        if len(candidate) < len(prompt):
            prompt = candidate
    diagnostics = {
        "prompt_chars": len(prompt), "unpacked_prompt_chars": original_chars,
        "limit_chars": PROMPT_LIMIT, "target_chars": PROMPT_TARGET,
        "input_hash": original_projection.get("input_hash"),
        "section_chars": {key: json_size(value) for key, value in projection.items()},
    }
    if len(prompt) > PROMPT_LIMIT:
        raise A5ReviewError("A5_MODEL_CONTEXT_TOO_LARGE", diagnostics=diagnostics)
    return prompt, diagnostics


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
