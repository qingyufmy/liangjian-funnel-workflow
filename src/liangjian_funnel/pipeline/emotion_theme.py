"""Bind daily attention members to frozen, strategy-owned taxonomy links.

Popularity is a source, not a theme. Membership does not prove a catalyst or
an early-cycle stage; those remain separate A2 judgments.
"""
from collections.abc import Mapping
from typing import Any

from .deterministic import _membership_map
from .feature_store import content_hash
from .mature_theme_registry import taxonomy_is_business_related


def bind_emotion_themes(output: Mapping[str, Any], snapshot: Mapping[str, Any],
                       symbols: set[str]) -> dict[str, dict[str, Any]]:
    links = output.get("taxonomy_links", ())
    if not isinstance(links, (list, tuple)):
        links = ()
    index: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in links:
        if not isinstance(row, Mapping):
            continue
        # Model-generated monthly aliases may duplicate or drift from the
        # canonical registry. Only the validated stable mapping owns identity.
        if row.get("match_method") != "MATURE_THEME_REGISTRY_EXACT_NAME":
            continue
        theme, node = str(row.get("theme_id") or ""), str(row.get("node_id") or "")
        taxonomy, code = str(row.get("taxonomy") or "").upper(), str(row.get("taxonomy_code") or "").upper()
        if not theme or not node or taxonomy not in {"INDUSTRY", "CONCEPT"} or not code:
            continue
        if not taxonomy_is_business_related(theme, taxonomy, str(row.get("taxonomy_name") or "")):
            continue
        index.setdefault((taxonomy, code), []).append(dict(row))
    memberships = {
        kind: _membership_map(snapshot.get(key), taxonomy=kind)
        for kind, key in (("INDUSTRY", "THS_INDUSTRY_MEMBERSHIP"), ("CONCEPT", "THS_CONCEPT_MEMBERSHIP"))
    }
    result = {}
    for symbol in sorted(symbols):
        matches = []
        for kind, by_symbol in memberships.items():
            for member in by_symbol.get(symbol, ()):
                code = str(member.get("taxonomy_code") or "").upper()
                matches.extend(index.get((kind, code), ()))
        matches = sorted({content_hash(row): row for row in matches}.values(),
                         key=lambda row: (str(row["theme_id"]), str(row["node_id"]), str(row["taxonomy_code"])))
        # A unique industry binding precedes optional concept associations.
        # Multiple industry themes cannot be resolved by an unrelated concept.
        industry = [row for row in matches if row["taxonomy"] == "INDUSTRY"]
        preferred = industry or matches
        pairs = {(str(row["theme_id"]), str(row["node_id"])) for row in preferred}
        resolved = len(pairs) == 1
        theme, node = next(iter(pairs)) if resolved else ("UNMAPPED", "UNMAPPED")
        result[symbol] = {
            "schema_version": "emotion-theme-binding/1.0.0",
            "resolved": resolved, "theme_id": theme, "node_id": node,
            "reason_code": "OK" if resolved else "EMOTION_THEME_AMBIGUOUS" if pairs else "EMOTION_THEME_MEMBERSHIP_MISSING",
            "source": "FROZEN_A1_TAXONOMY_LINKS_AND_SYMBOL_MEMBERSHIP",
            "selection_basis": "UNIQUE_INDUSTRY" if industry else "UNIQUE_CONCEPT",
            "matches": matches, "source_hash": content_hash({"symbol": symbol, "matches": matches}),
            "confirms_catalyst": False, "confirms_theme_stage": False,
        }
    return result
