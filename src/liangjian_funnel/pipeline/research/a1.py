from __future__ import annotations
from .common import *

def _run_v2_a1_review(
    self,
    *,
    lane_id: str,
    model: str,
    snapshot: FrozenInputSnapshot,
    g0: set[str],
    bundle: PromptBundle,
    run_id: str,
    discovery: StageAudit,
    discovery_output: Mapping[str, Any],
    discovery_context: Mapping[str, Any],
    gate: DeterministicGateResult,
) -> StageAudit:
    batches = _chunk_symbol_sets(gate.review_symbols, self.settings.research_a1_batch_size)
    self._emit_progress(
        run_id=run_id,
        lane=lane_id,
        model=model,
        stage="A1_LLM_REVIEW",
        completed=0,
        total=len(batches),
        status="RUNNING",
        attempts=0,
        processed_symbols=0,
        total_symbols=len(gate.review_symbols),
    )
    request_audits: list[StageAudit] = []
    valid_audits: list[StageAudit] = []
    split_count = 0
    blocked: StageAudit | None = None
    if batches:
        request_audits, valid_audits, split_count, blocked, _ = self._execute_batch_plan(
            batches=batches,
            lane_id=lane_id,
            model=model,
            stage="A1",
            progress_stage="A1_LLM_REVIEW",
            run_id=run_id,
            snapshot_id=snapshot.snapshot_id,
            runner=lambda batch: self._run_stage_with_checkpoint(
                lane_id=lane_id,
                model=model,
                stage="A1",
                snapshot=snapshot,
                upstream_output=None,
                upstream_symbols=batch,
                bundle=bundle,
                run_id=run_id,
                projection_symbols=batch,
                a1_discovery_context=discovery_context,
            ),
            splittable=_a1_batch_is_splittable,
        )
    if blocked is not None:
        self._emit_progress(
            run_id=run_id,
            lane=lane_id,
            model=model,
            stage="A1_LLM_REVIEW",
            completed=len(valid_audits),
            total=len(batches),
            status="FAILED",
            attempts=sum(item.attempts for item in request_audits),
            processed_symbols=sum(len(item.symbols) for item in valid_audits),
            total_symbols=len(gate.review_symbols),
        )
        return StageAudit(
            lane=lane_id,
            model=model,
            stage="A1",
            status="BLOCKED",
            snapshot_id=snapshot.snapshot_id,
            prompt_hash=_combined_digest(item.prompt_hash for item in (discovery, *request_audits)),
            input_hash=_combined_digest(item.input_hash for item in (discovery, *request_audits)),
            output_hash=None,
            latency_ms=sum(item.latency_ms or 0 for item in (discovery, *request_audits)),
            attempts=sum(item.attempts for item in (discovery, *request_audits)),
            thinking_variant=_common_variant((discovery, *request_audits)),
            symbols=(),
            reason_codes=tuple(f"A1_BATCH_BLOCKED:{reason}" for reason in blocked.reason_codes) or ("A1_BATCH_BLOCKED",),
            diagnostics={"pipeline_mode": DETERMINISTIC_PIPELINE_MODE, "local_screen": gate.summary},
        )
    outputs = [discovery_output, *(audit.output for audit in valid_audits if isinstance(audit.output, Mapping))]
    merged = _merge_a1_outputs(outputs)
    if isinstance(discovery_output.get("monthly_industry_decisions"), list):
        # Monthly industry decisions belong to the one macro-discovery
        # call.  Company mapping batches may repeat the prompt schema but
        # cannot rewrite the frozen monthly ranking decisions.
        merged["monthly_industry_decisions"] = [
            dict(item)
            for item in discovery_output["monthly_industry_decisions"]
            if isinstance(item, Mapping)
        ]
    merged["taxonomy_links"] = list(gate.taxonomy_links)
    merged["active_research_pool"] = _deduplicate_stage_items(
        "active_research_pool",
        [*merged.get("active_research_pool", []), *local_active_items(gate)],
    )
    merged["monitor_pool"] = _deduplicate_stage_items(
        "monitor_pool",
        [*merged.get("monitor_pool", []), *local_monitor_items(gate)],
    )
    merged["rejected_candidates"] = _deduplicate_stage_items(
        "rejected_candidates",
        [*merged.get("rejected_candidates", []), *local_rejected_items(gate)],
    )
    # selection_basis is a server-owned provenance label.  Model batches
    # may return the same symbol first during de-duplication, so reattach
    # the frozen local decision after the merge instead of trusting or
    # requiring the model to echo this field.
    basis_by_symbol = {
        str(item.get("symbol")): str(item.get("selection_basis"))
        for item in gate.decisions
        if item.get("symbol") and item.get("selection_basis")
    }
    gate_by_symbol = {
        str(item.get("symbol")): item
        for item in gate.decisions
        if item.get("symbol")
    }
    for partition in ("active_research_pool", "monitor_pool", "rejected_candidates"):
        rows = merged.get(partition)
        if not isinstance(rows, list):
            continue
        enriched: list[Any] = []
        for raw in rows:
            if not isinstance(raw, Mapping):
                enriched.append(raw)
                continue
            item = dict(raw)
            symbol = _first_symbol(item)
            if symbol in basis_by_symbol:
                item["selection_basis"] = basis_by_symbol[symbol]
            local_decision = gate_by_symbol.get(symbol)
            if isinstance(local_decision, Mapping):
                for field in _A1_SERVER_OWNED_FACT_FIELDS:
                    if field in local_decision:
                        value = local_decision[field]
                        item[field] = dict(value) if isinstance(value, Mapping) else (
                            list(value)
                            if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))
                            else value
                        )
            enriched.append(item)
        merged[partition] = enriched
    basis_counts = {
        basis: 0
        for basis in (
            "LLM_REVIEWED",
            "DETERMINISTIC_SCORE",
            "QUOTA_FILL",
            "BROKER_GOLD_DIRECT",
            "FUNDAMENTAL_BASELINE",
            "HALF_YEAR_FUNDAMENTAL",
        )
    }
    for item in merged.get("active_research_pool", ()):
        if not isinstance(item, Mapping):
            continue
        basis = str(item.get("selection_basis") or "")
        if basis in basis_counts:
            basis_counts[basis] += 1
    summary = dict(merged.get("analysis_summary")) if isinstance(merged.get("analysis_summary"), Mapping) else {}
    summary["selection_basis_counts"] = basis_counts
    summary["selection_basis_total"] = sum(basis_counts.values())
    merged["analysis_summary"] = summary
    institutional_rows = [
        {
            "symbol": decision.get("symbol"),
            "company_name": decision.get("name"),
            "coverage_origin": decision.get("coverage_origin"),
            "local_partition": decision.get("status"),
            "autonomous_partition": decision.get("autonomous_status"),
            "theme_id": decision.get("theme_id"),
            "industry_chain_node": decision.get("node_id"),
            "reason_codes": list(decision.get("reason_codes") or ()),
            "institutional_coverage": decision.get("institutional_coverage"),
        }
        for decision in gate.decisions
        if decision.get("coverage_origin") == "BROKER_GOLD_T2"
    ]
    merged["institutional_coverage_pool"] = institutional_rows
    raw_a1_targets = snapshot.data.get("A1_POOL_TARGETS")
    strict_monthly_chain = (
        isinstance(raw_a1_targets, Mapping)
        and raw_a1_targets.get("monthly_chain_only") is True
    )
    merged["institutional_coverage_summary"] = {
        "symbol_count": len(institutional_rows),
        "active_count": sum(
            row.get("local_partition") in {"LOCAL_ACTIVE_CANDIDATE", "REVIEW_CANDIDATE"}
            for row in institutional_rows
        ),
        "monitor_count": sum(
            row.get("local_partition") in {"LOCAL_MONITOR", "OUTSIDE_THEME", "OUTSIDE_G0"}
            for row in institutional_rows
        ),
        "rejected_count": sum(row.get("local_partition") == "HARD_REJECT" for row in institutional_rows),
        "direct_research_entry": not strict_monthly_chain,
        "direct_approval_forbidden": strict_monthly_chain,
        "autonomous_benchmark_remains_independent": True,
    }
    merged["local_screen_summary"] = gate.summary
    merged = _refresh_analysis_counts(merged, "A1")
    merged = _annotate_a1_pool_target(merged, snapshot.data)
    # The A1 research universe is G0 plus verified institutional direct
    # rows.  Rows outside G0 remain downstream_trade_eligible=false and A2
    # rejects them deterministically; treating them as a partition error
    # here would contradict the direct-research contract.
    a1_research_universe = {
        str(item.get("symbol"))
        for item in gate.decisions
        if item.get("symbol")
    }
    reasons = _validate_output(
        merged,
        stage="A1",
        model=model,
        snapshot_id=snapshot.snapshot_id,
        upstream_symbols=a1_research_universe,
        snapshot_data=snapshot.data,
    )
    approved_symbols = tuple(sorted(_approved_symbols(merged, "A1")))
    stage_status, outcome_reasons = _classify_stage_outcome(
        "A1", merged, reasons=reasons, gate=gate
    )
    reasons = list(dict.fromkeys([*reasons, *outcome_reasons]))
    self._emit_progress(
        run_id=run_id,
        lane=lane_id,
        model=model,
            stage="A1_LLM_REVIEW",
            completed=len(valid_audits),
            total=len(batches),
            status=_progress_status_for_stage_status(stage_status),
            attempts=sum(item.attempts for item in request_audits),
            processed_symbols=len(gate.review_symbols),
            total_symbols=len(gate.review_symbols),
            selected_symbols=len(approved_symbols),
            reason_codes=reasons,
            outcome=stage_status,
    )
    audits = (discovery, *request_audits)
    return StageAudit(
        lane=lane_id,
        model=model,
        stage="A1",
        status=stage_status,
        snapshot_id=snapshot.snapshot_id,
        prompt_hash=_combined_digest(item.prompt_hash for item in audits),
        input_hash=_combined_digest(item.input_hash for item in audits),
        output_hash=_sha256_json(merged),
        latency_ms=sum(item.latency_ms or 0 for item in audits),
        attempts=sum(item.attempts for item in audits),
        thinking_variant=_common_variant(audits),
        symbols=approved_symbols,
        reason_codes=tuple(reasons),
        output=merged,
        diagnostics={
            "pipeline_mode": DETERMINISTIC_PIPELINE_MODE,
            "local_screen": gate.summary,
            "monthly_strategy_status": (
                discovery_context.get("monthly_strategy_context", {}).get("status")
                if isinstance(discovery_context.get("monthly_strategy_context"), Mapping)
                else None
            ),
            "batch_count": len(batches),
            "completed_batches": len(valid_audits),
            "split_count": split_count,
            "pool_counts": _stage_pool_counts(merged, "A1"),
        },
    )


def _run_a1_batched(
    self,
    *,
    lane_id: str,
    model: str,
    snapshot: FrozenInputSnapshot,
    g0: set[str],
    bundle: PromptBundle | None,
    run_id: str,
) -> StageAudit:
    size = self.settings.research_a1_batch_size
    batches = _build_a1_node_batches(g0, snapshot.data, size)
    discovery_output: Mapping[str, Any] = {}
    frozen_discovery_context: Mapping[str, Any] | None = None
    audits: list[StageAudit] = []
    if snapshot.data.get("A1_DRIVER_LINEAGE_REQUIRED") is True:
        discovery = self._run_stage(
            lane_id=lane_id,
            model=model,
            stage="A1",
            snapshot=snapshot,
            upstream_output=None,
            upstream_symbols=set(),
            bundle=bundle,
            run_id=run_id,
            projection_symbols=set(),
            a1_discovery_context={"mode": "POLICY_MACRO_DISCOVERY"},
        )
        discovery_output = discovery.output if isinstance(discovery.output, Mapping) else {}
        if discovery.status != "VALIDATED" or not _valid_a1_discovery_output(discovery_output):
            reasons = tuple(
                f"A1_DISCOVERY_BLOCKED:{reason}"
                for reason in (discovery.reason_codes or ("A1_DISCOVERY_OUTPUT_INVALID",))
            )
            return StageAudit(
                lane=lane_id,
                model=model,
                stage="A1",
                status=STATUS_BLOCKED_MODEL,
                snapshot_id=snapshot.snapshot_id,
                prompt_hash=discovery.prompt_hash,
                input_hash=discovery.input_hash,
                output_hash=None,
                latency_ms=discovery.latency_ms,
                attempts=discovery.attempts,
                thinking_variant=discovery.thinking_variant,
                symbols=(),
                reason_codes=reasons,
                diagnostics={"discovery_output_shape": _output_shape(discovery.output)},
            )
        frozen_discovery_context = {
            "mode": "COMPANY_MAPPING",
            "structural_themes": discovery_output["structural_themes"],
            "industry_chain_graph": discovery_output["industry_chain_graph"],
        }
        audits.append(discovery)
    batch_audits, valid_audits, split_count, blocked, _total_batches = self._execute_batch_plan(
        batches=batches,
        lane_id=lane_id,
        model=model,
        stage="A1",
        run_id=run_id,
        snapshot_id=snapshot.snapshot_id,
        runner=lambda batch: self._run_stage_with_checkpoint(
            lane_id=lane_id,
            model=model,
            stage="A1",
            snapshot=snapshot,
            upstream_output=None,
            upstream_symbols=batch,
            bundle=bundle,
            run_id=run_id,
            projection_symbols=batch,
            a1_discovery_context=frozen_discovery_context,
        ),
        splittable=_a1_batch_is_splittable,
    )
    audits.extend(batch_audits)
    if blocked is not None:
        reasons = tuple(f"A1_BATCH_BLOCKED:{reason}" for reason in blocked.reason_codes)
        return StageAudit(
            lane=lane_id,
            model=model,
            stage="A1",
            status="BLOCKED",
            snapshot_id=snapshot.snapshot_id,
            prompt_hash=_combined_digest(item.prompt_hash for item in audits),
            input_hash=_combined_digest(item.input_hash for item in audits),
            output_hash=None,
            latency_ms=sum(item.latency_ms or 0 for item in audits),
            attempts=sum(item.attempts for item in audits),
            thinking_variant=_common_variant(audits),
            symbols=(),
            reason_codes=reasons or ("A1_BATCH_BLOCKED",),
            diagnostics={
                "batch_count": len(batches),
                "completed_batches": len(valid_audits),
                "request_groups": len(audits),
                "split_count": split_count,
                "blocked_batch_output_shape": _output_shape(blocked.output),
                "blocked_batch_diagnostics": blocked.diagnostics,
            },
        )

    merge_outputs = [
        audit.output for audit in valid_audits if isinstance(audit.output, Mapping)
    ]
    if discovery_output:
        merge_outputs.insert(0, discovery_output)
    merged = _merge_a1_outputs(merge_outputs)
    merged = _annotate_a1_pool_target(merged, snapshot.data)
    reasons = _validate_output(
        merged,
        stage="A1",
        model=model,
        snapshot_id=snapshot.snapshot_id,
        upstream_symbols=g0,
        snapshot_data=snapshot.data,
    )
    symbols = tuple(sorted(_approved_symbols(merged, "A1")))
    diagnostics = {
        "batch_count": len(batches),
        "completed_batches": len(valid_audits),
        "request_groups": len(audits),
        "split_count": split_count,
        "pool_counts": _stage_pool_counts(merged, "A1"),
    }
    if discovery_output:
        diagnostics.update({
            "discovery_theme_count": len(discovery_output.get("structural_themes", ())),
            "discovery_node_count": len(discovery_output.get("industry_chain_graph", ())),
        })
    return StageAudit(
        lane=lane_id,
        model=model,
        stage="A1",
        status="VALIDATED" if not reasons else "BLOCKED",
        snapshot_id=snapshot.snapshot_id,
        prompt_hash=_combined_digest(item.prompt_hash for item in audits),
        input_hash=_combined_digest(item.input_hash for item in audits),
        output_hash=_sha256_json(merged),
        latency_ms=sum(item.latency_ms or 0 for item in audits),
        attempts=sum(item.attempts for item in audits),
        thinking_variant=_common_variant(audits),
        symbols=symbols,
        reason_codes=tuple(reasons),
        output=merged,
        diagnostics=diagnostics,
    )


def _coerce_active_a1_payload(active_a1: Mapping[str, Any] | Any | None) -> dict[str, Any] | None:
    """Normalize an A1 registry record or payload for pipeline consumption."""

    if active_a1 is None:
        return None
    if isinstance(active_a1, Mapping):
        raw = dict(active_a1)
    else:
        raw_payload = getattr(active_a1, "payload", None)
        if not isinstance(raw_payload, Mapping):
            return None
        raw = dict(raw_payload)
        generation_id = getattr(active_a1, "generation_id", None)
        if generation_id and "generation_id" not in raw:
            raw["generation_id"] = str(generation_id)
    nested = raw.get("payload")
    if isinstance(nested, Mapping) and not isinstance(raw.get("lanes"), Mapping):
        payload = dict(nested)
        for key in ("generation_id", "snapshot_id", "snapshot_hash", "as_of", "manifest"):
            if key in raw and key not in payload:
                payload[key] = raw[key]
        raw = payload
    lanes = raw.get("lanes")
    if isinstance(lanes, Sequence) and not isinstance(lanes, (str, bytes, bytearray)):
        normalized_lanes: dict[str, Any] = {}
        for item in lanes:
            if not isinstance(item, Mapping):
                continue
            lane_id = str(item.get("lane") or item.get("lane_id") or "").strip()
            if lane_id:
                normalized_lanes[lane_id] = dict(item)
        lanes = normalized_lanes
    if not isinstance(lanes, Mapping):
        # A direct one-lane payload is useful for small integrations and tests.
        if any(key in raw for key in ("active_research_pool", "monitor_pool", "rejected_candidates")):
            return {"lanes": {"lane_1": {"model": raw.get("model"), "output": raw}}}
        return None
    normalized = dict(raw)
    normalized["lanes"] = {str(key): value for key, value in lanes.items() if isinstance(value, Mapping)}
    return normalized if normalized["lanes"] else None


def _active_a1_lane(payload: Mapping[str, Any], lane_id: str, model: str) -> Mapping[str, Any] | None:
    lanes = payload.get("lanes")
    if not isinstance(lanes, Mapping):
        return None
    item = lanes.get(lane_id)
    if isinstance(item, Mapping):
        return item
    # A production registry intentionally contains one canonical primary A1
    # lane. Optional A2/A3 comparison models must consume that same upstream
    # pool; otherwise enabling comparisons would force A1 to run again and
    # violate the cadence boundary.
    if len(lanes) == 1:
        only = next(iter(lanes.values()))
        if isinstance(only, Mapping):
            return only
    # Multi-lane legacy registries still require an exact model match.
    for candidate in lanes.values():
        if not isinstance(candidate, Mapping):
            continue
        candidate_model = str(candidate.get("model") or "").strip()
        if candidate_model and candidate_model == model:
            return candidate
    return None


def _filter_a1_output_to_symbols(
    output: Mapping[str, Any],
    symbols: set[str],
) -> dict[str, Any]:
    """Drop stale active-A1 rows before the current run enters A2."""

    allowed = {str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}
    result = dict(output)
    for partition in ("active_research_pool", "monitor_pool", "rejected_candidates"):
        rows = output.get(partition)
        if not isinstance(rows, (list, tuple)):
            continue
        result[partition] = [
            dict(item)
            for item in rows
            if isinstance(item, Mapping)
            and bool(_scan_symbols(item).intersection(allowed))
        ]
    return result


def _build_a1_node_batches(
    symbols: set[str],
    snapshot_data: Mapping[str, Any],
    batch_size: int,
) -> list[set[str]]:
    """Pack deterministic industry-node groups into bounded A1 calls."""

    if batch_size < 1:
        raise ResearchPipelineError("A1_BATCH_SIZE_INVALID")
    node_by_symbol = _a1_node_by_symbol(snapshot_data, symbols)
    raw_liquidity = snapshot_data.get("LIQUIDITY_SNAPSHOT")
    liquidity = raw_liquidity if isinstance(raw_liquidity, Mapping) else {}
    groups: dict[str, list[str]] = {}
    for symbol in symbols:
        groups.setdefault(node_by_symbol.get(symbol, f"UNMAPPED:{symbol}"), []).append(symbol)

    def turnover(symbol: str) -> float:
        value = liquidity.get(symbol)
        return _safe_float(value.get("turnover")) if isinstance(value, Mapping) else 0.0

    for members in groups.values():
        members.sort(key=lambda symbol: (-turnover(symbol), symbol))
    ordered_nodes = sorted(
        groups,
        key=lambda node: (-sum(turnover(symbol) for symbol in groups[node]), node),
    )
    batches: list[set[str]] = []
    current: list[str] = []
    for node in ordered_nodes:
        members = groups[node]
        for offset in range(0, len(members), batch_size):
            chunk = members[offset:offset + batch_size]
            if current and len(current) + len(chunk) > batch_size:
                batches.append(set(current))
                current = []
            current.extend(chunk)
            if len(current) == batch_size:
                batches.append(set(current))
                current = []
    if current:
        batches.append(set(current))
    return batches


def _a1_node_by_symbol(
    snapshot_data: Mapping[str, Any],
    symbols: set[str],
) -> dict[str, str]:
    raw_membership = snapshot_data.get("THS_INDUSTRY_MEMBERSHIP")
    if not isinstance(raw_membership, Mapping):
        return {}
    records = raw_membership.get("records")
    if not isinstance(records, list):
        return {}
    result: dict[str, str] = {}
    for record in records:
        if not isinstance(record, Mapping):
            continue
        symbol = str(record.get("thscode") or record.get("symbol") or "")
        memberships = record.get("memberships")
        if symbol not in symbols or not isinstance(memberships, list):
            continue
        valid = [
            item
            for item in memberships
            if isinstance(item, Mapping) and str(item.get("industry_thscode") or "")
        ]
        if not valid:
            continue
        specific = max(
            valid,
            key=lambda item: (
                str(item.get("industry_thscode") or "").startswith("884"),
                str(item.get("industry_thscode") or ""),
            ),
        )
        result[symbol] = str(specific.get("industry_thscode"))
    return result


def _merge_a1_outputs(outputs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not outputs:
        return {}
    merged: dict[str, Any] = {}
    list_values: dict[str, list[Any]] = {}
    for output in outputs:
        for key, value in output.items():
            if key == "envelope":
                if key not in merged and isinstance(value, Mapping):
                    merged[key] = dict(value)
                elif isinstance(value, Mapping) and isinstance(merged.get(key), dict):
                    if value.get("status") == "DEGRADED":
                        merged[key]["status"] = "DEGRADED"
                continue
            if isinstance(value, list):
                list_values.setdefault(str(key), []).extend(value)
            elif key not in merged:
                merged[str(key)] = value
    for key, values in list_values.items():
        merged[key] = _deduplicate_stage_items(key, values)

    active = merged.get("active_research_pool")
    if isinstance(active, list):
        normalized_active = [_normalize_pool_symbol(item) for item in active]
        merged["active_research_pool"] = sorted(
            normalized_active,
            key=lambda item: (
                -_safe_float(item.get("structural_score")) if isinstance(item, Mapping) else 0.0,
                _first_symbol(item) if isinstance(item, Mapping) else "",
            ),
        )
    monitor = merged.get("monitor_pool")
    if isinstance(monitor, list):
        rejected_symbols = _scan_symbols(merged.get("rejected_candidates", ()))
        active_symbols = _scan_symbols(merged.get("active_research_pool", ()))
        normalized_monitor = [_normalize_pool_symbol(item) for item in monitor]
        normalized_monitor = [
            item
            for item in normalized_monitor
            if not _scan_symbols(item).intersection(rejected_symbols | active_symbols)
        ]
        merged["monitor_pool"] = sorted(
            normalized_monitor,
            key=lambda item: _first_symbol(item) if isinstance(item, Mapping) else _canonical_json(item),
        )
    if isinstance(merged.get("rejected_candidates"), list):
        merged["rejected_candidates"] = [_normalize_pool_symbol(item) for item in merged["rejected_candidates"]]
    # A provider summary describes only one transport batch and becomes
    # misleading after merge (for example, "all five candidates"). Publish a
    # deterministic aggregate summary instead; the normalized pools and their
    # combined hashes remain the authoritative merged result.
    merged["analysis_summary"] = {
        "outcome": "A1_BATCHES_MERGED",
        "batch_count": len(outputs),
        "approved_count": len(merged.get("active_research_pool", ())),
        "monitor_count": len(merged.get("monitor_pool", ())),
        "rejected_count": len(merged.get("rejected_candidates", ())),
    }
    return merged


def _valid_a1_discovery_output(output: Mapping[str, Any]) -> bool:
    """Check the structural shape shared by discovery and company mapping.

    Target counts and canonical industry mapping completeness are evaluated by
    ``_monthly_discovery_reasons`` because that function has the frozen
    monthly context.  Keeping this predicate structural preserves compatibility
    with small pre-v3 fixtures and old read-only audit files.
    """

    validation = validate_discovery_output(
        output,
        require_targets=False,
        canonical_decisions=None,
    )
    return not any(
        reason in {
            "A1_DISCOVERY_THEMES_MISSING",
            "A1_DISCOVERY_CHAIN_NODES_MISSING",
            "A1_DISCOVERY_THEME_ID_INVALID",
            "A1_DISCOVERY_NODE_ID_INVALID",
            "A1_DISCOVERY_NODE_INVALID",
            "A1_DISCOVERY_NODE_THEME_LINK_INVALID",
        }
        for reason in validation.reason_codes
    )


def _a1_discovery_context_reasons(
    output: Mapping[str, Any],
    context: Mapping[str, Any],
) -> list[str]:
    mode = str(context.get("mode") or "")
    if mode == "POLICY_MACRO_DISCOVERY":
        return [] if _valid_a1_discovery_output(output) else ["A1_DISCOVERY_OUTPUT_INVALID"]
    if mode != "COMPANY_MAPPING":
        return []
    frozen_themes = context.get("structural_themes")
    frozen_nodes = context.get("industry_chain_graph")
    allowed_theme_ids = {
        str(item.get("theme_id") or "").strip()
        for item in frozen_themes
        if isinstance(item, Mapping) and str(item.get("theme_id") or "").strip()
    } if isinstance(frozen_themes, list) else set()
    allowed_node_ids = {
        str(item.get("node_id") or "").strip()
        for item in frozen_nodes
        if isinstance(item, Mapping) and str(item.get("node_id") or "").strip()
    } if isinstance(frozen_nodes, list) else set()
    output_theme_ids = {
        str(item.get("theme_id") or "").strip()
        for item in output.get("structural_themes", ())
        if isinstance(item, Mapping) and str(item.get("theme_id") or "").strip()
    } if isinstance(output.get("structural_themes"), list) else set()
    output_node_ids = {
        str(item.get("node_id") or "").strip()
        for item in output.get("industry_chain_graph", ())
        if isinstance(item, Mapping) and str(item.get("node_id") or "").strip()
    } if isinstance(output.get("industry_chain_graph"), list) else set()
    reasons: list[str] = []
    if output_theme_ids.difference(allowed_theme_ids):
        reasons.append("A1_BATCH_THEME_OUTSIDE_DISCOVERY")
    if output_node_ids.difference(allowed_node_ids):
        reasons.append("A1_BATCH_NODE_OUTSIDE_DISCOVERY")
    return reasons


def _a1_reviewed_hypothesis_coverage_reasons(
    output: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
) -> list[str]:
    """Require an explicit disposition for every active reviewed A1 hypothesis.

    Reviewed public research is a T3 question plane, not a stock selector and
    not primary evidence.  The model may map a hypothesis to an independently
    evidenced structural theme, keep it under observation, or reject it.  It
    may not silently omit the hypothesis: that previously allowed bank/insurance
    and AI-application research questions to disappear even though they were
    present in the frozen A1 packet.

    Dispositions live in ``unresolved_questions`` so the discovery top-level
    contract stays backward compatible.  Production packets identify each row
    by the exact ``document_id`` and exact hypothesis text supplied by the
    server; semantic aliases are deliberately not guessed here.
    """

    contract = snapshot_data.get("REVIEWED_PUBLIC_RESEARCH_LEADS")
    if not isinstance(contract, Mapping) or contract.get("available") is not True:
        return []
    documents = contract.get("documents")
    if not isinstance(documents, list):
        return []

    expected: set[tuple[str, str]] = set()
    for document in documents:
        if not isinstance(document, Mapping):
            continue
        document_id = str(document.get("document_id") or "").strip()
        hypotheses = document.get("theme_hypotheses")
        if not document_id or not isinstance(hypotheses, list):
            continue
        for hypothesis in hypotheses:
            if not isinstance(hypothesis, Mapping):
                continue
            theme = str(hypothesis.get("theme") or "").strip()
            if theme:
                expected.add((document_id, theme))
    if not expected:
        return []

    theme_ids = {
        str(item.get("theme_id") or "").strip()
        for item in output.get("structural_themes", ())
        if isinstance(item, Mapping) and str(item.get("theme_id") or "").strip()
    } if isinstance(output.get("structural_themes"), list) else set()
    questions = output.get("unresolved_questions")
    questions = questions if isinstance(questions, list) else []
    observed: dict[tuple[str, str], Mapping[str, Any]] = {}
    reasons: list[str] = []
    for raw in questions:
        if not isinstance(raw, Mapping):
            continue
        document_id = str(raw.get("document_id") or "").strip()
        hypothesis_theme = str(raw.get("hypothesis_theme") or "").strip()
        if not document_id or not hypothesis_theme:
            continue
        key = (document_id, hypothesis_theme)
        if key in observed:
            reasons.append("A1_REVIEWED_HYPOTHESIS_DISPOSITION_DUPLICATE")
            continue
        observed[key] = raw
        if key not in expected:
            reasons.append("A1_REVIEWED_HYPOTHESIS_DISPOSITION_UNKNOWN")
            continue
        disposition = str(raw.get("disposition") or "").strip().upper()
        if disposition not in {"MAPPED", "MONITOR", "REJECTED"}:
            reasons.append("A1_REVIEWED_HYPOTHESIS_DISPOSITION_INVALID")
            continue
        mapped = raw.get("matched_theme_ids")
        mapped_ids = {
            str(value).strip()
            for value in mapped
            if isinstance(value, str) and value.strip()
        } if isinstance(mapped, list) else set()
        if disposition == "MAPPED" and (
            not mapped_ids or not mapped_ids.issubset(theme_ids)
        ):
            reasons.append("A1_REVIEWED_HYPOTHESIS_THEME_MAPPING_INVALID")
        if not str(raw.get("reason") or "").strip():
            reasons.append("A1_REVIEWED_HYPOTHESIS_REASON_MISSING")

    if expected.difference(observed):
        reasons.append("A1_REVIEWED_HYPOTHESIS_COVERAGE_INCOMPLETE")
    return list(dict.fromkeys(reasons))


def _a1_discovery_evidence_required(context: Mapping[str, Any]) -> bool:
    """Apply the strict packet evidence gate only to full discovery runs.

    Tiny pre-v2 fixtures and historical checkpoints may exercise the discovery
    shape without carrying a monthly packet. They remain readable, while any
    real/full-market run (or a context with canonical monthly decisions) must
    use the immutable packet/snapshot intersection and fail closed on gaps.
    """

    monthly = context.get("monthly_strategy_context")
    source = monthly if isinstance(monthly, Mapping) else context
    return (
        _safe_int(source.get("g0_symbol_count")) >= 500
        or bool(source.get("monthly_industry_decisions"))
    )


def _a1_discovery_evidence_reasons(
    output: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
    authorized_source_refs: Sequence[str] | None = None,
) -> list[str]:
    # The discovery request and its final validator must share the exact same
    # immutable allowlist.  A caller that has rendered the compact packet passes
    # the packet/snapshot intersection; the fallback is retained for old
    # read-only callers that only have a snapshot and is intentionally limited to
    # the discovery-source projection below.
    valid_refs = {
        str(value)
        for value in (
            authorized_source_refs
            if authorized_source_refs is not None
            else _snapshot_discovery_evidence_refs(snapshot_data)
        )
        if isinstance(value, str) and value.strip()
    }
    reasons: list[str] = []
    for field, reason in (
        ("structural_themes", "A1_DISCOVERY_THEME_EVIDENCE_INVALID"),
        ("industry_chain_graph", "A1_DISCOVERY_NODE_EVIDENCE_INVALID"),
    ):
        records = output.get(field)
        if not isinstance(records, list):
            continue
        for record in records:
            refs, malformed = _discovery_record_source_refs(record)
            if malformed or not refs or not refs.issubset(valid_refs):
                reasons.append(reason)
                break
    mappings = output.get("industry_theme_mappings")
    if isinstance(mappings, list):
        for mapping in mappings:
            if not isinstance(mapping, Mapping):
                reasons.append("A1_INDUSTRY_THEME_MAPPING_EVIDENCE_INVALID")
                break
            if str(mapping.get("mapping_status") or "").strip().upper() != "MAPPED":
                continue
            raw_refs = mapping.get("supporting_source_refs")
            if not isinstance(raw_refs, list) or not raw_refs:
                reasons.append("A1_INDUSTRY_THEME_MAPPING_EVIDENCE_INVALID")
                break
            refs = {
                value.strip()
                for value in raw_refs
                if isinstance(value, str) and value.strip()
            }
            if len(refs) != len(raw_refs) or not refs.issubset(valid_refs):
                reasons.append("A1_INDUSTRY_THEME_MAPPING_EVIDENCE_INVALID")
                break
    return reasons


def _a1_discovery_validation_issues(
    output: Mapping[str, Any], authorized_source_refs: Sequence[str],
) -> list[dict[str, Any]]:
    """Locate invalid citations/links without guessing or granting evidence."""

    allowed = set(authorized_source_refs)
    themes = output.get("structural_themes")
    theme_ids = {
        str(row.get("theme_id") or "").strip() for row in themes
        if isinstance(row, Mapping) and str(row.get("theme_id") or "").strip()
    } if isinstance(themes, list) else set()
    issues: list[dict[str, Any]] = []
    for field, identity in (("structural_themes", "theme_id"), ("industry_chain_graph", "node_id"),
                            ("industry_theme_mappings", "industry_thscode")):
        rows = output.get(field)
        if not isinstance(rows, list):
            continue
        for index, row in enumerate(rows):
            if not isinstance(row, Mapping):
                continue
            mapped = field == "industry_theme_mappings"
            if mapped and str(row.get("mapping_status") or "").strip().upper() != "MAPPED":
                continue
            key = "supporting_source_refs" if mapped else "source_refs"
            raw = row.get(key, row.get("source_ref") if not mapped else None)
            refs, malformed = _discovery_record_source_refs({"source_refs": raw})
            if mapped and not isinstance(raw, list):
                malformed = True
            if malformed or not refs or not refs.issubset(allowed):
                issues.append({"path": f"{field}[{index}].{key}", "record_id": row.get(identity),
                               "kind": "INVALID_EVIDENCE", "observed": raw,
                               "malformed": malformed, "unauthorized_refs": sorted(refs - allowed)})
            link_key = "mapped_theme_ids" if mapped else "theme_ids" if field == "industry_chain_graph" else None
            if link_key:
                links = row.get(link_key)
                if (not isinstance(links, list) or not links
                        or any(not isinstance(v, str) or v not in theme_ids for v in links)):
                    issues.append({"path": f"{field}[{index}].{link_key}", "record_id": row.get(identity),
                                   "kind": "INVALID_THEME_LINK", "observed": links,
                                   "allowed_theme_ids": sorted(theme_ids)})
    return sanitize(issues)


def _normalize_a1_discovery_source_refs(
    output: Mapping[str, Any],
    authorized_source_refs: Sequence[str],
) -> tuple[dict[str, Any], int]:
    """Normalize only authorized scalar refs; never invent discovery evidence."""

    authorized = frozenset(
        str(value)
        for value in authorized_source_refs
        if isinstance(value, str) and value.strip()
    )
    result = dict(output)
    changed = 0
    for field in ("structural_themes", "industry_chain_graph"):
        rows = result.get(field)
        if not isinstance(rows, list):
            continue
        normalized_rows: list[Any] = []
        for raw in rows:
            if not isinstance(raw, Mapping):
                normalized_rows.append(raw)
                continue
            item = dict(raw)
            if "source_refs" in item and isinstance(item.get("source_refs"), str):
                value = item["source_refs"]
                if value == value.strip() and value in authorized:
                    item["source_refs"] = [value]
                    changed += 1
            elif "source_ref" in item and isinstance(item.get("source_ref"), str):
                value = item["source_ref"]
                if value == value.strip() and value in authorized:
                    # Retain the original singular field for losslessness while
                    # supplying the canonical plural field expected by v3.
                    item["source_refs"] = [value]
                    changed += 1
            normalized_rows.append(item)
        result[field] = normalized_rows
    return result, changed


def _a1_batch_is_splittable(reasons: Sequence[str]) -> bool:
    retryable_prefixes = (
        "MODEL_PROMPT_TOO_LARGE",
        "OUTPUT_BUDGET_",
        "NETWORK_",
        "MODEL_TOTAL_DEADLINE_",
        "STRICT_JSON_",
        "STREAM_",
        "RESPONSE_",
        "JSON_",
        "ENVELOPE_",
        "STAGE_ID_",
        "MODEL_NAME_",
        "SNAPSHOT_LINEAGE_",
        "APPROVED_POOL_",
        "A1_POOL_",
    )
    return any(str(reason).startswith(retryable_prefixes) for reason in reasons)


def _a1_missing_mapping_codes(
    output: Mapping[str, Any],
    context: Mapping[str, Any],
) -> tuple[str, ...]:
    """Return exact missing INCLUDE codes for a field-level semantic retry."""

    expected = context.get("monthly_industry_decisions")
    if not isinstance(expected, list) or not isinstance(output.get("industry_theme_mappings"), list):
        return ()
    validation = validate_discovery_output(
        output,
        canonical_decisions=expected,
        require_targets=False,
    )
    return tuple(validation.missing_industry_codes)


def _canonicalize_a1_driver_context(
    output: Mapping[str, Any],
    discovery_context: Mapping[str, Any],
) -> tuple[dict[str, Any], int]:
    """Keep company batches read-only with respect to frozen A1 drivers."""

    result = dict(output)
    changed = 0
    for field in ("structural_themes", "industry_chain_graph"):
        frozen = discovery_context.get(field)
        if not isinstance(frozen, list):
            continue
        canonical = [dict(item) if isinstance(item, Mapping) else item for item in frozen]
        if result.get(field) != canonical:
            result[field] = canonical
            changed += 1
    return result, changed


def _canonicalize_a1_local_candidate_facts(
    output: Mapping[str, Any],
    discovery_context: Mapping[str, Any],
) -> tuple[dict[str, Any], int]:
    """Restore frozen A1 candidate facts before applying model thresholds.

    ``COMPANY_MAPPING`` responses are allowed to provide semantic conclusions,
    status and reason codes, but the deterministic A1 screen owns the monthly
    direction, sector membership and financial facts used by the threshold
    policy.  Company batches historically received those facts in their
    prompt but could omit or echo stale values; the final A1 merge repaired
    them too late, after an otherwise valid ACTIVE row had already been moved
    to MONITOR.  This helper only enriches rows whose symbol is present in the
    frozen ``local_candidates`` map.  It never adds rows or changes model-owned
    conclusions, status or reasons.
    """

    if (
        not isinstance(discovery_context, Mapping)
        or discovery_context.get("mode") != "COMPANY_MAPPING"
    ):
        return dict(output), 0
    raw_candidates = discovery_context.get("local_candidates")
    if not isinstance(raw_candidates, Mapping) or not raw_candidates:
        return dict(output), 0

    candidates_by_symbol: dict[str, Mapping[str, Any]] = {}
    for raw_key, raw_candidate in raw_candidates.items():
        if not isinstance(raw_candidate, Mapping):
            continue
        symbol = _first_symbol(raw_candidate.get("symbol")) or _first_symbol(raw_key)
        if symbol:
            candidates_by_symbol[symbol] = raw_candidate
    if not candidates_by_symbol:
        return dict(output), 0

    def copy_fact(value: Any) -> Any:
        if isinstance(value, Mapping):
            return {key: copy_fact(item) for key, item in value.items()}
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            return [copy_fact(item) for item in value]
        return value

    result = dict(output)
    changed = 0
    for partition in ("active_research_pool", "monitor_pool", "rejected_candidates"):
        rows = result.get(partition)
        if not isinstance(rows, list):
            continue
        normalized: list[Any] = []
        for raw_item in rows:
            if not isinstance(raw_item, Mapping):
                normalized.append(raw_item)
                continue
            item = dict(raw_item)
            candidate = candidates_by_symbol.get(_first_symbol(item.get("symbol")))
            if isinstance(candidate, Mapping):
                for field in _A1_SERVER_OWNED_FACT_FIELDS:
                    if field not in candidate:
                        continue
                    value = copy_fact(candidate[field])
                    if item.get(field) != value:
                        item[field] = value
                        changed += 1
                for output_field, source_fields in _A1_SERVER_OWNED_FACT_ALIASES.items():
                    source_field = next((field for field in source_fields if field in candidate), None)
                    if source_field is None:
                        continue
                    value = copy_fact(candidate[source_field])
                    if item.get(output_field) != value:
                        item[output_field] = value
                        changed += 1
            normalized.append(item)
        result[partition] = normalized
    return result, changed


def _a1_business_evidence_reasons(
    item: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
) -> list[str]:
    if "MAIN_BUSINESS_EVIDENCE" not in snapshot_data:
        # Backward-compatible replay/test snapshots predate the deterministic
        # evidence contract. New production snapshots always include it.
        return []
    symbol = _first_symbol(item)
    raw_evidence = snapshot_data.get("MAIN_BUSINESS_EVIDENCE")
    symbol_evidence = raw_evidence.get(symbol) if isinstance(raw_evidence, Mapping) else None
    if not isinstance(symbol_evidence, Mapping) or symbol_evidence.get("available") is not True:
        return ["A1_MAIN_BUSINESS_EVIDENCE_MISSING"]

    exposure = item.get("business_exposure")
    if not isinstance(exposure, Mapping):
        return ["A1_REVENUE_EXPOSURE_UNCONFIRMED", "A1_BUSINESS_SOURCE_REF_INVALID"]
    revenue_exposure = _safe_float(exposure.get("revenue_exposure_pct"))
    reasons: list[str] = []
    if revenue_exposure <= 0 or revenue_exposure > 100:
        reasons.append("A1_REVENUE_EXPOSURE_UNCONFIRMED")

    valid_refs = {
        str(evidence.get("source_ref"))
        for evidence in symbol_evidence.get("evidence", ())
        if isinstance(evidence, Mapping) and evidence.get("source_ref")
    }
    if str(exposure.get("source_ref") or "") not in valid_refs:
        reasons.append("A1_BUSINESS_SOURCE_REF_INVALID")
    return reasons


def _a1_structural_lineage_reasons(
    item: Mapping[str, Any],
    output: Mapping[str, Any],
    valid_refs: set[str],
) -> list[str]:
    """Reject free-form A1 narratives that are not bound to frozen evidence."""

    themes = output.get("structural_themes")
    nodes = output.get("industry_chain_graph")
    if not isinstance(themes, list):
        return ["A1_STRUCTURAL_THEME_LINEAGE_MISSING", "A1_CHAIN_NODE_LINEAGE_MISSING"]
    if not isinstance(nodes, list):
        return ["A1_CHAIN_NODE_LINEAGE_MISSING"]
    primary_theme = str(item.get("primary_theme") or "").strip()
    chain_node = str(item.get("industry_chain_node") or "").strip()
    matched_theme = next((
        theme for theme in themes
        if isinstance(theme, Mapping)
        and primary_theme in {
            str(theme.get("theme_id") or "").strip(),
            str(theme.get("display_name") or "").strip(),
        }
    ), None)
    matched_node = next((
        node for node in nodes
        if isinstance(node, Mapping) and chain_node == str(node.get("node_id") or "").strip()
    ), None)
    reasons: list[str] = []
    if matched_theme is None:
        reasons.append("A1_STRUCTURAL_THEME_LINEAGE_MISSING")
    if matched_node is None:
        reasons.append("A1_CHAIN_NODE_LINEAGE_MISSING")
    if matched_theme is not None and matched_node is not None:
        theme_id = str(matched_theme.get("theme_id") or "").strip()
        node_theme_ids = matched_node.get("theme_ids")
        if not isinstance(node_theme_ids, list) or theme_id not in {
            str(value).strip() for value in node_theme_ids if isinstance(value, str)
        }:
            reasons.append("A1_CHAIN_NODE_THEME_LINK_INVALID")
    for matched, reason in (
        (matched_theme, "A1_THEME_DRIVER_EVIDENCE_INVALID"),
        (matched_node, "A1_CHAIN_NODE_EVIDENCE_INVALID"),
    ):
        if matched is None:
            continue
        raw_refs = matched.get("source_refs")
        refs = {
            str(value).strip()
            for value in raw_refs
            if isinstance(value, str) and value.strip()
        } if isinstance(raw_refs, list) else set()
        if not refs.intersection(valid_refs):
            reasons.append(reason)
    return reasons


def _a1_score_breakdown_reasons(
    item: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
) -> list[str]:
    """Ensure the model used configured A1 weights instead of an ad-hoc score."""

    raw_weights = snapshot_data.get("SCORE_WEIGHTS")
    if not isinstance(raw_weights, Mapping) or not raw_weights:
        return []
    weights = {
        str(key): _safe_float(value)
        for key, value in raw_weights.items()
        if isinstance(key, str) and 0 < _safe_float(value) <= 1
    }
    if not weights or abs(sum(weights.values()) - 1.0) > 1e-6:
        return ["A1_SCORE_WEIGHTS_INVALID"]
    breakdown = item.get("score_breakdown")
    if not isinstance(breakdown, Mapping):
        return ["A1_SCORE_BREAKDOWN_MISSING"]
    if set(breakdown) != set(weights):
        return ["A1_SCORE_BREAKDOWN_INVALID"]
    values: dict[str, float] = {}
    for key in weights:
        raw_value = breakdown.get(key)
        value = _safe_float(raw_value)
        if isinstance(raw_value, bool) or raw_value is None or value < 0 or value > 100:
            return ["A1_SCORE_BREAKDOWN_INVALID"]
        values[key] = value
    computed = sum(values[key] * weights[key] for key in weights)
    if abs(computed - _safe_float(item.get("structural_score"))) > 0.51:
        return ["A1_STRUCTURAL_SCORE_MISMATCH"]
    return []


def _annotate_a1_pool_target(
    output: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
) -> dict[str, Any]:
    """Expose A1 research coverage gaps without changing the classification."""

    result = dict(output)
    raw_targets = snapshot_data.get("A1_POOL_TARGETS")
    targets = raw_targets if isinstance(raw_targets, Mapping) else {}
    active_min, active_max = _target_pair(targets.get("active_research_target"), (200, 800))
    clue_min, clue_max = _target_pair(targets.get("clue_pool_target"), (300, 800))
    active_count = len(result.get("active_research_pool")) if isinstance(result.get("active_research_pool"), list) else 0
    monitor_count = len(result.get("monitor_pool")) if isinstance(result.get("monitor_pool"), list) else 0
    clue_count = active_count + monitor_count
    summary = dict(result.get("analysis_summary")) if isinstance(result.get("analysis_summary"), Mapping) else {}
    reason_codes = summary.get("reason_codes") if isinstance(summary.get("reason_codes"), list) else []
    if active_count < active_min:
        reason_codes = [*reason_codes, "A1_ACTIVE_TARGET_UNDERFILLED"]
    if clue_count < clue_min:
        reason_codes = [*reason_codes, "A1_CLUE_TARGET_UNDERFILLED"]
    summary.update({
        "institutional_pool_role": "P1_INSTITUTIONAL_DIRECT_RESEARCH",
        "active_research_count": active_count,
        "clue_pool_count": clue_count,
        "active_research_target": {"minimum": active_min, "maximum": active_max},
        "clue_pool_target": {"minimum": clue_min, "maximum": clue_max},
        "quota_fill_enabled": bool(targets.get("quota_fill_enabled", False)),
        "quota_fill_observation": str(
            targets.get("quota_fill_observation") or "COHORT_OBSERVATION_ONLY"
        ),
        "reason_codes": list(dict.fromkeys(reason_codes)),
    })
    result["analysis_summary"] = summary
    return result


def _validate_a1_partition(output: Mapping[str, Any], upstream_symbols: set[str]) -> list[str]:
    reasons: list[str] = []
    pools: dict[str, set[str]] = {}
    for key in ("active_research_pool", "monitor_pool", "rejected_candidates"):
        value = output.get(key)
        if not isinstance(value, list):
            reasons.append("A1_POOL_SCHEMA_INVALID")
            pools[key] = set()
            continue
        declared: set[str] = set()
        for item in value:
            if not isinstance(item, Mapping):
                reasons.append("A1_POOL_ITEM_INVALID")
                continue
            scanned = _scan_symbols(item.get("symbol"))
            if len(scanned) != 1:
                reasons.append("A1_POOL_SYMBOL_INVALID")
                continue
            declared.update(scanned)
        pools[key] = declared
    if any(pools[left].intersection(pools[right]) for left, right in (
        ("active_research_pool", "monitor_pool"),
        ("active_research_pool", "rejected_candidates"),
        ("monitor_pool", "rejected_candidates"),
    )):
        reasons.append("A1_POOL_PARTITION_OVERLAP")
    covered = set().union(*pools.values())
    if covered != upstream_symbols:
        reasons.append("A1_POOL_PARTITION_INCOMPLETE")
    return reasons


_MOVED_NAMES = ('_run_v2_a1_review', '_run_a1_batched', '_coerce_active_a1_payload', '_active_a1_lane', '_filter_a1_output_to_symbols', '_build_a1_node_batches', '_a1_node_by_symbol', '_merge_a1_outputs', '_valid_a1_discovery_output', '_a1_discovery_context_reasons', '_a1_reviewed_hypothesis_coverage_reasons', '_a1_discovery_evidence_required', '_a1_discovery_evidence_reasons', '_a1_discovery_validation_issues', '_normalize_a1_discovery_source_refs', '_a1_batch_is_splittable', '_a1_missing_mapping_codes', '_canonicalize_a1_driver_context', '_canonicalize_a1_local_candidate_facts', '_a1_business_evidence_reasons', '_a1_structural_lineage_reasons', '_a1_score_breakdown_reasons', '_annotate_a1_pool_target', '_validate_a1_partition')
_METHOD_BINDINGS = {'_run_v2_a1_review': 'ResearchPipeline._run_v2_a1_review', '_run_a1_batched': 'ResearchPipeline._run_a1_batched'}
