"""Read-only full taxonomy and frozen A1 lineage audit; writes only audit artifacts."""
from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from liangjian_funnel.data.rotation_theme import (
    load_rotation_theme_config, load_membership_snapshot, membership_matches_codes,
    validate_board_identity, _content_hash,
)
from liangjian_funnel.pipeline.deterministic import _membership_map, screen_a1
from liangjian_funnel.pipeline.mature_theme_registry import resolve_mature_theme_registry, taxonomy_is_business_related
from liangjian_funnel.reporting import atomic_write_json
from liangjian_funnel.workflow import _main_business_evidence


def build_audit(*, config, catalog, memberships, snapshot, research, now, business_projection=None):
    data = snapshot.get("data", snapshot)
    a1 = next(row["output"] for row in research["stages"] if row["stage"] == "A1")
    expected = a1.get("envelope", {}).get("input_snapshot_ids", [])
    if expected and snapshot.get("snapshot_id") not in expected:
        raise ValueError("A1_MAPPING_AUDIT_WRONG_SOURCE_SNAPSHOT")
    pools = {key: {row["symbol"] for row in a1.get(key, ())} for key in (
        "active_research_pool", "monitor_pool", "rejected_candidates"
    )}
    symbols = set(data.get("g0_symbols", ()))
    industry = _membership_map(data.get("THS_INDUSTRY_MEMBERSHIP"), taxonomy="INDUSTRY")
    concept = _membership_map(data.get("THS_CONCEPT_MEMBERSHIP"), taxonomy="CONCEPT")
    previous_business = data.get("MAIN_BUSINESS_EVIDENCE", {})
    projected_business = business_projection if business_projection is not None else _main_business_evidence(data.get("DISCLOSURE_EVENTS", {}), sorted(symbols))
    member_union = set()
    boards = []
    issues = []
    for theme in config.themes:
        member = memberships.get(theme.theme_id, {})
        errors = []
        if error := validate_board_identity(theme, catalog):
            errors.append(error)
        if not theme.eastmoney_board_names:
            errors.append("BOARD_NAME_BINDING_MISSING")
        if not member.get("available"):
            errors.append(member.get("reason_code") or "MEMBERSHIP_MISSING")
        if not membership_matches_codes(member, theme.eastmoney_board_codes):
            errors.append("MEMBERSHIP_BOARD_DEFINITION_MISMATCH")
        members = {row["symbol"] for row in member.get("records", ())}
        member_union.update(members)
        boards.append({
            "theme_id": theme.theme_id, "name": theme.name, "kind": theme.kind,
            "parent": theme.parent, "codes": list(theme.eastmoney_board_codes),
            "vendor_names": dict(theme.eastmoney_board_names), "errors": errors,
            "member_count": len(members), "member_hash": member.get("content_hash"),
            "member_captured_at": member.get("captured_at"),
            "a1_active_count": len(members & pools["active_research_pool"]),
            "a1_monitor_count": len(members & pools["monitor_pool"]),
            "a1_rejected_count": len(members & pools["rejected_candidates"]),
            "outside_g0_count": len(members - symbols),
            "business_missing_count": sum(not projected_business.get(s, {}).get("available") for s in members & symbols),
            "a1_missing_symbols": sorted(members & symbols - pools["active_research_pool"]),
        })
        issues.extend({"theme_id": theme.theme_id, "reason": error} for error in errors)
    resolved = resolve_mature_theme_registry(data.get("A1_MATURE_THEME_REGISTRY", {}),
                                            data.get("THS_INDUSTRY_CATALOG"), data.get("THS_CONCEPT_CATALOG"))
    missing = []
    recovered = []
    for symbol in sorted(symbols):
        names = [r.get("taxonomy_name") for r in industry.get(symbol, ())]
        if not projected_business.get(symbol, {}).get("available"):
            missing.append({"symbol": symbol, "industries": names,
                            "in_active_a1": symbol in pools["active_research_pool"]})
        elif not previous_business.get(symbol, {}).get("available"):
            recovered.append({"symbol": symbol, "industries": names,
                              "evidence": projected_business[symbol]["evidence"]})
    rejected_links = [row for row in a1.get("taxonomy_links", ()) if not taxonomy_is_business_related(
        str(row.get("theme_id") or ""), str(row.get("taxonomy") or ""), str(row.get("taxonomy_name") or "")
    )]
    # Quant-only re-evaluation on the same frozen company facts and monthly
    # decisions. It is an impact check, never a published A1 or an LLM result.
    projected_data = dict(data)
    projected_data["MAIN_BUSINESS_EVIDENCE"] = projected_business
    gate = screen_a1(projected_data, a1)
    by_symbol = {row["symbol"]: row for row in gate.decisions}
    report = {
        "schema_version": "theme-mapping-audit/1.0.0", "captured_at": now.isoformat(),
        "snapshot_id": snapshot.get("snapshot_id"), "snapshot_hash": snapshot.get("snapshot_hash"),
        "taxonomy_hash": _content_hash(config.as_dict()),
        "mapping_status": "PASS" if not issues else "FAIL", "mapping_errors": issues,
        "boards": boards, "theme_count": len(boards), "member_union_count": len(member_union),
        "g0_count": len(symbols), "industry_covered_count": len(symbols & set(industry)),
        "concept_covered_count": len(symbols & set(concept)),
        "industry_stock_count_total": len(industry), "concept_stock_count_total": len(concept),
        "concept_symbols_without_industry": sorted(set(concept) - set(industry)),
        "frozen_a1_requires_refresh": bool(rejected_links or recovered),
        "production_updated": False,
        "invalid_business_links_removed": rejected_links,
        "quant_only_counts": dict(Counter(row["status"] for row in gate.decisions)),
        "recovered_business_quant_decisions": [{
            "symbol": row["symbol"], "status": by_symbol.get(row["symbol"], {}).get("status"),
            "reasons": by_symbol.get(row["symbol"], {}).get("reason_codes"),
            "half_year": by_symbol.get(row["symbol"], {}).get("half_year_support"),
        } for row in recovered],
        "industry_missing_symbols": sorted(symbols - set(industry)),
        "monthly_unresolved_names": resolved["unresolved"],
        "business_missing_count_before": sum(not previous_business.get(s, {}).get("available") for s in symbols),
        "business_missing_count_after": len(missing), "business_recovered": recovered,
        "business_missing": missing,
        "business_missing_by_industry": dict(Counter(name for row in missing for name in row["industries"])),
        "boundary": "Current membership cross-check of frozen A1, not a point-in-time replay; no pool promotion or publication.",
    }
    report["content_hash"] = _content_hash(report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("registry", "catalog", "memberships", "snapshot", "research", "output"):
        parser.add_argument(f"--{key}", required=True)
    parser.add_argument("--fact-cache", help="Current evidence impact check only; never a historical replay")
    args = parser.parse_args()
    config = load_rotation_theme_config(args.registry)
    now = datetime.now(ZoneInfo("Asia/Shanghai"))
    memberships = {t.theme_id: load_membership_snapshot(args.memberships, t.theme_id, now.date(), now=now) for t in config.themes}
    read = lambda p: json.loads(Path(p).read_text(encoding="utf-8"))
    snapshot = read(args.snapshot)
    projection = None
    updated = []
    if args.fact_cache:
        from liangjian_funnel.data.cninfo_pdf import CninfoPdfEvidence
        from liangjian_funnel.facts.cninfo import _pdf_payload, compact_cninfo_pdf_evidence
        from liangjian_funnel.pipeline.local_fact_cache import LocalFactCache
        events = snapshot["data"].get("DISCLOSURE_EVENTS", {}).get("by_symbol", {})
        cache = LocalFactCache(Path(args.fact_cache))
        records = cache.get_cached_results("CNINFO_PDF_EVIDENCE",
            [r["announcement_id"] for rows in events.values() for r in rows], fresh_at=now)
        projected = {}
        for symbol, rows in events.items():
            projected[symbol] = []
            for row in rows:
                cached = records.get(row["announcement_id"], {}).get("payload", {})
                replacement = row
                if cached.get("available"):
                    evidence = CninfoPdfEvidence.model_validate(cached)
                    if evidence.announcement_id == row["announcement_id"] and evidence.pdf_url == row.get("source_url"):
                        replacement = {**row, **_pdf_payload(compact_cninfo_pdf_evidence(evidence)),
                            "content_hash": evidence.pdf_sha256}
                        updated.append({"symbol": symbol, "announcement_id": evidence.announcement_id,
                            "fetched_at": evidence.fetched_at.isoformat(), "pdf_sha256": evidence.pdf_sha256,
                            "extraction_version": evidence.extraction_version})
                projected[symbol].append(replacement)
        projection = _main_business_evidence({"by_symbol": projected}, snapshot["data"]["g0_symbols"])
    report = build_audit(config=config, catalog=read(args.catalog), memberships=memberships,
                         snapshot=snapshot, research=read(args.research), now=now, business_projection=projection)
    report["current_cache_projection"] = {"enabled": bool(args.fact_cache), "updated_documents": updated}
    report.pop("content_hash")
    report["content_hash"] = _content_hash(report)
    atomic_write_json(Path(args.output), report)
    print(json.dumps({k: v for k, v in report.items() if k not in {
        "boards", "business_recovered", "business_missing", "business_missing_by_industry", "monthly_unresolved_names",
        "invalid_business_links_removed", "recovered_business_quant_decisions", "current_cache_projection"
    }}, ensure_ascii=False))
    return 0 if report["mapping_status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
