from __future__ import annotations
from .common import *

def _run_a2_batched(
    self,
    *,
    lane_id: str,
    model: str,
    snapshot: FrozenInputSnapshot,
    upstream_output: Mapping[str, Any],
    upstream_symbols: set[str],
    bundle: PromptBundle | None,
    run_id: str,
) -> StageAudit:
    """Run A2 in theme-preserving transport batches, then rank globally."""

    batches = _build_a2_theme_batches(
        upstream_output,
        upstream_symbols,
        min(self.settings.research_a2_batch_size, _A2_MAX_TRANSPORT_BATCH_SIZE),
        snapshot_data=snapshot.data,
    )
    audits, valid_audits, split_count, blocked, _total_batches = self._execute_batch_plan(
        batches=batches,
        lane_id=lane_id,
        model=model,
        stage="A2",
        run_id=run_id,
        snapshot_id=snapshot.snapshot_id,
        runner=lambda batch: self._run_stage_with_checkpoint(
            lane_id=lane_id,
            model=model,
            stage="A2",
            snapshot=snapshot,
            upstream_output=upstream_output,
            upstream_symbols=batch,
            bundle=bundle,
            run_id=run_id,
            projection_symbols=batch,
        ),
        splittable=_a2_batch_is_splittable,
    )
    if blocked is not None:
        return StageAudit(
            lane=lane_id,
            model=model,
            stage="A2",
            status="BLOCKED",
            snapshot_id=snapshot.snapshot_id,
            prompt_hash=_combined_digest(item.prompt_hash for item in audits),
            input_hash=_combined_digest(item.input_hash for item in audits),
            output_hash=None,
            latency_ms=sum(item.latency_ms or 0 for item in audits),
            attempts=sum(item.attempts for item in audits),
            thinking_variant=_common_variant(audits),
            symbols=(),
            reason_codes=tuple(f"A2_BATCH_BLOCKED:{reason}" for reason in blocked.reason_codes),
            diagnostics={
                "batch_count": len(batches),
                "completed_batches": len(valid_audits),
                "request_groups": len(audits),
                "split_count": split_count,
                "blocked_batch_diagnostics": blocked.diagnostics,
            },
        )

    merged = _merge_a2_outputs([
        audit.output for audit in valid_audits if isinstance(audit.output, Mapping)
    ])
    merged, canonicalized_a2_semantics = _canonicalize_a2_contract_semantics(merged)
    merged, canonicalized_lineage = _canonicalize_stage_lineage(
        merged,
        "A2",
        upstream_output,
        snapshot.data,
    )
    merged, a2_llm_reject_demotions = _demote_a2_llm_rejects(merged, snapshot.data)
    merged, canonicalized_scores = _canonicalize_stage_scores(merged, "A2", snapshot.data)
    merged, canonicalized_bottleneck_scores = _canonicalize_a2_bottleneck_scorecards(
        merged, snapshot.data
    )
    merged, threshold_demotions = _apply_stage_threshold_policy(merged, "A2", snapshot.data)
    if snapshot.data.get("STRICT_AGENT_RULES") is True:
        merged, lineage_demotions = _apply_a2_lineage_policy(
            merged, upstream_output, snapshot.data
        )
    else:
        lineage_demotions = 0
    merged, post_policy_lineage = _canonicalize_stage_lineage(
        merged,
        "A2",
        upstream_output,
        snapshot.data,
    )
    canonicalized_lineage += post_policy_lineage
    merged, enriched_decision_facts = _enrich_a2_decision_facts(merged, snapshot.data)
    merged = _annotate_a2_pool_target(merged, snapshot.data)
    merged = _refresh_analysis_counts(merged, "A2")
    reasons = _validate_output(
        merged,
        stage="A2",
        model=model,
        snapshot_id=snapshot.snapshot_id,
        upstream_symbols=upstream_symbols,
        snapshot_data=snapshot.data,
    )
    return StageAudit(
        lane=lane_id,
        model=model,
        stage="A2",
        status="VALIDATED" if not reasons else "BLOCKED",
        snapshot_id=snapshot.snapshot_id,
        prompt_hash=_combined_digest(item.prompt_hash for item in audits),
        input_hash=_combined_digest(item.input_hash for item in audits),
        output_hash=_sha256_json(merged),
        latency_ms=sum(item.latency_ms or 0 for item in audits),
        attempts=sum(item.attempts for item in audits),
        thinking_variant=_common_variant(audits),
        symbols=tuple(sorted(_approved_symbols(merged, "A2"))),
        reason_codes=tuple(reasons),
        output=merged,
        diagnostics={
            "batch_count": len(batches),
            "completed_batches": len(valid_audits),
            "request_groups": len(audits),
            "split_count": split_count,
            "canonicalized_score_items": canonicalized_scores,
            "canonicalized_bottleneck_scorecards": canonicalized_bottleneck_scores,
            "canonicalized_a2_semantics": canonicalized_a2_semantics,
            "enriched_decision_facts": enriched_decision_facts,
            "canonicalized_lineage": canonicalized_lineage,
            "a2_llm_reject_demotions": a2_llm_reject_demotions,
            "policy_demotions": threshold_demotions + lineage_demotions,
            "pool_counts": _stage_pool_counts(merged, "A2"),
        },
    )


def _project_a2_theme_metrics(
    value: Any,
    symbols: set[str] | None,
    snapshot_data: Mapping[str, Any],
    *,
    global_theme_limit: int = 2,
) -> Any:
    """Project full-market A2 theme metrics to batch themes plus a benchmark."""

    if not isinstance(value, Mapping) or symbols is None:
        return value
    raw_metrics = value.get("theme_metrics")
    if not isinstance(raw_metrics, Mapping):
        return dict(value)
    relevant_codes = _a2_prompt_rotation_codes(snapshot_data, symbols)
    if not relevant_codes:
        for taxonomy in ("industry", "concept"):
            relevant_codes.update(
                f"{taxonomy.upper()}:{code}"
                for code in _membership_codes_for_symbols(snapshot_data, taxonomy, symbols)
            )
    selected_board = snapshot_data.get("SELECTED_BOARD_SNAPSHOT")
    selected_by_symbol = selected_board.get("by_symbol") if isinstance(selected_board, Mapping) else None
    if isinstance(selected_by_symbol, Mapping):
        for symbol in symbols:
            rows = selected_by_symbol.get(symbol)
            if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
                continue
            for row in rows:
                if not isinstance(row, Mapping):
                    continue
                for key in ("theme_id", "board_code", "taxonomy_code"):
                    code = str(row.get(key) or "").strip().upper()
                    if code:
                        relevant_codes.add(code.upper())

    def rank(item: tuple[Any, Any]) -> tuple[float, float, float, str]:
        key, raw = item
        row = raw if isinstance(raw, Mapping) else {}
        return (
            _safe_float(row.get("score")),
            _safe_float(row.get("weekly_confirmation_score")),
            _safe_float(row.get("breadth")),
            str(key),
        )

    globally_ranked = sorted(raw_metrics.items(), key=rank, reverse=True)
    global_keys = {
        str(key)
        for key, _item in globally_ranked[: max(0, int(global_theme_limit))]
    }
    selected_keys = {key.upper() for key in relevant_codes.union(global_keys)}
    result = dict(value)
    result["theme_metrics"] = {
        str(key): item
        for key, item in raw_metrics.items()
        if str(key).upper() in selected_keys
    }
    result["prompt_projection"] = {
        "symbol_count": len(symbols),
        "prompt_theme_count": len(result["theme_metrics"]),
        "full_theme_count": len(raw_metrics),
        "batch_linked_theme_count": sum(
            1 for key in result["theme_metrics"] if key.upper() in relevant_codes
        ),
        "global_benchmark_limit": max(0, int(global_theme_limit)),
        "full_snapshot_retained_for_audit": True,
    }
    return result


def _a2_prompt_rotation_codes(
    snapshot_data: Mapping[str, Any],
    symbols: set[str],
) -> set[str]:
    """Return only deterministic rotation/taxonomy codes needed by this batch."""

    result: set[str] = set()
    contexts = snapshot_data.get("A2_BOTTLENECK_CONTEXT")
    if isinstance(contexts, Mapping):
        for symbol in symbols:
            row = contexts.get(symbol)
            if not isinstance(row, Mapping):
                continue
            for field in ("rotation_direction_id", "theme_id"):
                code = str(row.get(field) or "").strip().upper()
                if code:
                    result.add(code.removeprefix("SELECTED_BOARD:"))
            binding = row.get("a2_taxonomy_binding")
            if isinstance(binding, Mapping):
                taxonomy = str(binding.get("taxonomy") or "").strip().upper()
                code = str(binding.get("taxonomy_code") or "").strip().upper()
                if code:
                    result.add(f"{taxonomy}:{code}" if taxonomy else code)
    selected = snapshot_data.get("SELECTED_BOARD_SNAPSHOT")
    by_symbol = selected.get("by_symbol") if isinstance(selected, Mapping) else None
    if isinstance(by_symbol, Mapping):
        for symbol in symbols:
            rows = by_symbol.get(symbol)
            if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
                continue
            for row in rows:
                if not isinstance(row, Mapping):
                    continue
                for field in ("theme_id", "board_code", "taxonomy_code"):
                    code = str(row.get(field) or "").strip().upper()
                    if code:
                        result.add(code)
    return result


def _project_a2_bottleneck_context(value: Any, symbols: set[str] | None) -> Any:
    """Build the single compact, server-owned fact row used by A2."""

    if not isinstance(value, Mapping) or symbols is None:
        return value
    result: dict[str, Any] = {}
    for symbol in sorted(symbols):
        raw = value.get(symbol)
        if not isinstance(raw, Mapping):
            continue
        row = {
            "a1_theme_id": raw.get("theme_id"),
            "deterministic_status": raw.get("deterministic_status"),
            "top_rotation_theme": raw.get("top_rotation_theme") is True,
            "quant_score": raw.get("deterministic_score"),
            "rotation_direction_id": raw.get("rotation_direction_id"),
            "rotation_rank": raw.get("theme_rotation_rank"),
            "rotation_score": raw.get("theme_rotation_score"),
            "channel": raw.get("a2_pool_channel"),
            "market_role": raw.get("deterministic_market_role"),
            "behavior_type": raw.get("stock_behavior_type"),
            "research_route_qualifications": raw.get("research_route_qualifications", {}),
            "strong_trend_observation": raw.get("strong_trend_observation") is True,
            "research_observation_scope": raw.get("research_observation_scope"),
            "identifiability": raw.get("identifiability_score"),
            "data_state": raw.get("data_sufficiency_state"),
            "emotion_eligible": raw.get("emotion_core_eligible") is True,
            "execution_permission": raw.get("execution_permission"),
            "research_only_reason": raw.get("research_only_reason"),
            "emotion_theme_binding": raw.get("emotion_theme_binding"),
            "trend_eligible": raw.get("trend_core_eligible") is True,
            "rotation_reserve_eligible": raw.get("rotation_reserve_eligible") is True,
            "rotation_reserve_scope": raw.get("rotation_reserve_scope"),
            "rotation_reserve_boards": [
                {key: board.get(key) for key in ("board_name", "board_code", "rotation_reserve_rank", "strength", "main_net_inflow_cny")}
                for board in raw.get("rotation_reserve_boards", []) if isinstance(board, Mapping)
            ],
        }
        routes = raw.get("eligible_routes")
        if isinstance(routes, Sequence) and not isinstance(routes, (str, bytes, bytearray)):
            row["eligible_routes"] = [str(item) for item in routes[:2]]
        reasons = raw.get("deterministic_reason_codes")
        if isinstance(reasons, Sequence) and not isinstance(reasons, (str, bytes, bytearray)):
            row["reason_codes"] = [str(item) for item in reasons[:3]]
        factors = raw.get("a2_factor_scores")
        if isinstance(factors, Mapping):
            row["factor_scores"] = {
                str(name): factor.get("score")
                for name, factor in factors.items()
                if isinstance(factor, Mapping)
                and factor.get("score") is not None
            }
        board = raw.get("selected_board")
        if isinstance(board, Mapping):
            row["selected_board"] = {
                key: board.get(key)
                for key in (
                    "eligible", "selected_for_rotation", "board_code", "board_name", "theme_id", "parent_theme_id",
                    "primary_rank", "strength", "main_net_inflow_cny",
                )
                if key in board
            }
        hot = raw.get("eastmoney_hot100")
        if isinstance(hot, Mapping):
            row["hot100"] = {
                key: hot.get(key)
                for key in ("eligible", "rank")
                if key in hot
            }
        emotion = raw.get("market_emotion_cycle")
        if isinstance(emotion, Mapping):
            row["emotion_cycle"] = {
                key: emotion.get(key)
                for key in ("stage", "new_entry_policy")
                if key in emotion
            }
        behavior = raw.get("behavior_type_decision")
        if isinstance(behavior, Mapping):
            row["behavior"] = {
                key: behavior.get(key)
                for key in (
                    "confidence", "evidence_state",
                )
                if key in behavior
            }
        result[symbol] = row
    scope: dict[str, list[str]] = {}
    for symbol, row in result.items():
        direction = row.get("rotation_direction_id")
        if (direction and row.get("trend_eligible")
                and row.get("deterministic_status") == "REVIEW_CANDIDATE"
                and row.get("top_rotation_theme") is True
                and row.get("selected_board", {}).get("selected_for_rotation") is True):
            scope.setdefault(str(direction), []).append(symbol)
    if scope:
        result["_rotation_review_scope"] = scope
    theme_scope: dict[str, list[str]] = {}
    for symbol in sorted(symbols):
        row = result.get(symbol, {})
        if row.get("a1_theme_id"):
            theme_scope.setdefault(str(row["a1_theme_id"]), []).append(symbol)
    if theme_scope:
        result["_theme_review_scope"] = theme_scope
    return result


def _a2_candidate_pool_max(snapshot_data: Mapping[str, Any]) -> int:
    raw = snapshot_data.get("A2_POOL_TARGETS")
    if isinstance(raw, Mapping):
        return max(0, _safe_int(raw.get("pool_max")) or 60)
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)) and len(raw) >= 2:
        return max(0, _safe_int(raw[1]) or 60)
    return 60


def _canonicalize_a2_complete_partition(
    output: Mapping[str, Any],
    gate: DeterministicGateResult,
) -> tuple[dict[str, Any], int]:
    """Make the final A2 partition mutually exclusive and complete.

    The model reviews only ``gate.review_symbols`` while deterministic rows are
    appended afterwards.  A reviewed daily-emotion row can therefore still be
    present in the derived outside-rotation pool, and the bounded effective
    watch pool can leave otherwise valid local-monitor rows unmaterialized.
    The server owns the complete partition: keep the most actionable pool by
    priority, remove later duplicates, then place every remaining gate row in
    the non-actionable outside-rotation audit pool.  This never promotes a row
    into focus/watch and cannot bypass a deterministic hard reject.
    """

    result = dict(output)
    priority = (
        "focus_pool",
        "watch_only_pool",
        "crowded_pool",
        "low_identity_pool",
        "rejected_candidates",
        "outside_rotation_pool",
    )
    seen: set[str] = set()
    changes = 0
    for pool in priority:
        raw_rows = result.get(pool)
        rows = raw_rows if isinstance(raw_rows, list) else []
        retained: list[Any] = []
        for row in rows:
            symbol = _first_symbol(row) if isinstance(row, Mapping) else ""
            if symbol and symbol in seen:
                changes += 1
                continue
            retained.append(row)
            if symbol:
                seen.add(symbol)
        result[pool] = retained

    outside = list(result.get("outside_rotation_pool") or [])
    for decision in gate.decisions:
        symbol = _first_symbol(decision)
        if not symbol or symbol in seen:
            continue
        item = _gate_item_from_decision(decision, "A2", "OUTSIDE_ROTATION")
        reason_codes = item.get("reason_codes")
        reasons = list(reason_codes) if isinstance(reason_codes, list) else []
        reasons.append("A2_NOT_IN_EFFECTIVE_POOL")
        item["reason_codes"] = list(dict.fromkeys(str(reason) for reason in reasons if str(reason)))
        outside.append(item)
        seen.add(symbol)
        changes += 1
    result["outside_rotation_pool"] = _deduplicate_stage_items(
        "outside_rotation_pool",
        outside,
    )
    return result, changes


def _move_a2_hard_rejects_to_rejected(
    output: Mapping[str, Any],
    gate: DeterministicGateResult,
) -> tuple[dict[str, Any], int]:
    """Repair a provider partition that placed deterministic hard rejects elsewhere."""

    hard_symbols = {str(symbol) for symbol in gate.rejected_symbols}
    if not hard_symbols:
        return dict(output), 0
    gate_by_symbol = {
        str(decision.get("symbol")): decision
        for decision in gate.decisions
        if str(decision.get("symbol")) in hard_symbols
    }
    result = dict(output)
    moved: list[Any] = []
    changed = 0
    for pool in ("focus_pool", "watch_only_pool", "crowded_pool", "low_identity_pool"):
        values = result.get(pool)
        if not isinstance(values, list):
            continue
        retained: list[Any] = []
        for raw in values:
            symbol = _first_symbol(raw.get("symbol")) if isinstance(raw, Mapping) else ""
            if symbol not in hard_symbols:
                retained.append(raw)
                continue
            row = dict(raw) if isinstance(raw, Mapping) else {"symbol": symbol}
            decision = gate_by_symbol.get(symbol)
            authoritative = _gate_item_from_decision(decision, "A2", "REJECTED") if decision else {}
            merged = {**row, **authoritative}
            existing_reasons = row.get("reason_codes") if isinstance(row.get("reason_codes"), list) else []
            reasons = list(dict.fromkeys([
                *existing_reasons,
                *list(authoritative.get("reason_codes") or ()),
                "A2_DETERMINISTIC_HARD_REJECT",
            ]))
            merged["reason_codes"] = reasons
            moved.append(merged)
            changed += 1
        result[pool] = retained
    if moved:
        result["rejected_candidates"] = _deduplicate_stage_items(
            "rejected_candidates",
            [*(result.get("rejected_candidates") or []), *moved],
        )
    return result, changed


def _build_a2_theme_batches(
    upstream_output: Mapping[str, Any],
    symbols: set[str],
    batch_size: int,
    *,
    snapshot_data: Mapping[str, Any] | None = None,
) -> list[set[str]]:
    """Pack candidates by deterministic daily rotation direction.

    A1's monthly theme is only a fallback. Mixing daily boards in one request
    makes lifecycle comparison unstable and invites the model to collapse the
    TOP5 contract into one preferred narrative.
    """

    if batch_size < 1:
        raise ResearchPipelineError("A2_BATCH_SIZE_INVALID")
    groups: dict[str, list[tuple[float, str]]] = {}
    contexts = snapshot_data.get("A2_BOTTLENECK_CONTEXT") if isinstance(snapshot_data, Mapping) else None
    contexts = contexts if isinstance(contexts, Mapping) else {}
    active = upstream_output.get("active_research_pool")
    if isinstance(active, list):
        for item in active:
            if not isinstance(item, Mapping):
                continue
            scanned = _scan_symbols(item.get("symbol"))
            if len(scanned) != 1:
                continue
            symbol = next(iter(scanned))
            if symbol not in symbols:
                continue
            context = contexts.get(symbol)
            context = context if isinstance(context, Mapping) else {}
            if context.get("emotion_core_eligible") is True:
                hot = context.get("eastmoney_hot100")
                hot = hot if isinstance(hot, Mapping) else {}
                theme = f"EMOTION:{context.get('theme_id') or item.get('primary_theme') or 'UNMAPPED'}"
                score = -_safe_float(hot.get("rank"))
            else:
                theme = str(
                    context.get("rotation_direction_id")
                    or item.get("primary_theme")
                    or "UNMAPPED"
                ).strip() or "UNMAPPED"
                score = _safe_float(
                    context.get("theme_rotation_score") or item.get("structural_score")
                )
            groups.setdefault(theme, []).append((score, symbol))
    assigned = {symbol for members in groups.values() for _score, symbol in members}
    for symbol in sorted(symbols.difference(assigned)):
        groups.setdefault("UNMAPPED", []).append((0.0, symbol))

    ordered_groups = sorted(
        groups.items(),
        key=lambda entry: (-max((score for score, _symbol in entry[1]), default=0.0), entry[0]),
    )
    batches: list[set[str]] = []
    for _theme, members in ordered_groups:
        ordered_members = [symbol for _score, symbol in sorted(members, key=lambda item: (-item[0], item[1]))]
        for offset in range(0, len(ordered_members), batch_size):
            chunk = ordered_members[offset:offset + batch_size]
            batches.append(set(chunk))
    return batches


def _merge_a2_outputs(outputs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Merge theme-preserving A2 batches into one globally ranked partition."""

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
    for pool in ("focus_pool", "watch_only_pool", "rejected_candidates"):
        values = merged.get(pool)
        if isinstance(values, list):
            merged[pool] = [_normalize_pool_symbol(item) for item in values]

    focus_symbols = _scan_symbols(merged.get("focus_pool", ()))
    if isinstance(merged.get("watch_only_pool"), list):
        merged["watch_only_pool"] = [
            item for item in merged["watch_only_pool"]
            if not _scan_symbols(item).intersection(focus_symbols)
        ]
    selected_symbols = focus_symbols | _scan_symbols(merged.get("watch_only_pool", ()))
    if isinstance(merged.get("rejected_candidates"), list):
        merged["rejected_candidates"] = [
            item for item in merged["rejected_candidates"]
            if not _scan_symbols(item).intersection(selected_symbols)
        ]

    def ranking(item: Any) -> tuple[float, float, str]:
        return (
            -_safe_float(item.get("theme_score")) if isinstance(item, Mapping) else 0.0,
            -_safe_float(item.get("identifiability_score")) if isinstance(item, Mapping) else 0.0,
            _first_symbol(item) if isinstance(item, Mapping) else _canonical_json(item),
        )

    for pool in ("focus_pool", "watch_only_pool"):
        if isinstance(merged.get(pool), list):
            merged[pool] = sorted(merged[pool], key=ranking)
    merged["analysis_summary"] = {
        "outcome": "A2_BATCHES_MERGED",
        "batch_count": len(outputs),
        **_stage_pool_counts(merged, "A2"),
        "pool_counts": _stage_pool_counts(merged, "A2"),
    }
    return merged


def _a2_batch_is_splittable(reasons: Sequence[str]) -> bool:
    retryable_prefixes = (
        "MODEL_PROMPT_TOO_LARGE",
        "OUTPUT_BUDGET_",
        "NETWORK_",
        "MODEL_TOTAL_DEADLINE_",
        "MODEL_WALL_CLOCK_",
        "UPSTREAM_5XX_",
        "RATE_LIMIT_",
        "STRICT_JSON_",
        "STREAM_",
        "RESPONSE_",
        "JSON_",
        "ENVELOPE_",
        "STAGE_ID_",
        "MODEL_NAME_",
        "SNAPSHOT_LINEAGE_",
        "APPROVED_POOL_",
        "A2_POOL_",
    )
    return any(str(reason).startswith(retryable_prefixes) for reason in reasons)


def _project_a2_research_hypotheses(value: Any) -> Any:
    return _compact_viewpoint_contract(value, public_lead=False)


def _a2_rejection_has_hard_fact_veto(
    item: Mapping[str, Any],
    context: Mapping[str, Any],
) -> bool:
    reason_values: list[str] = []
    # Only server/deterministic context can constitute a hard factual veto.
    # A model must not prevent its own rejection from being demoted simply by
    # echoing a hard-looking reason code in the response.
    for source in (context,):
        for key in ("reason_codes", "deterministic_reason_codes", "hard_reason_codes"):
            values = source.get(key)
            if isinstance(values, Sequence) and not isinstance(values, (str, bytes, bytearray)):
                reason_values.extend(str(value).strip().upper() for value in values if str(value).strip())
    return any(
        any(marker in reason for marker in _A2_LLM_REJECT_HARD_MARKERS)
        for reason in reason_values
    )


def _demote_a2_llm_rejects(
    output: Mapping[str, Any],
    snapshot_data: Mapping[str, Any] | None,
) -> tuple[dict[str, Any], int]:
    """Keep soft model rejects visible as A2 watch-only rows.

    Deterministic hard rejects and locally created rows are left untouched.
    This separates a model opinion from a fact-level veto while preserving the
    original model reasons for later review.
    """

    frozen = snapshot_data if isinstance(snapshot_data, Mapping) else {}
    contexts = _lineage_context_rows(frozen, "A2_BOTTLENECK_CONTEXT")
    result = dict(output)
    rejected = result.get("rejected_candidates")
    if not isinstance(rejected, list):
        return result, 0
    watch = list(result.get("watch_only_pool")) if isinstance(result.get("watch_only_pool"), list) else []
    retained: list[Any] = []
    changed = 0
    for raw in rejected:
        if not isinstance(raw, Mapping):
            retained.append(raw)
            continue
        symbol = _first_symbol(raw.get("symbol"))
        context = contexts.get(symbol, {})
        deterministic_status = str(
            context.get("deterministic_status")
            or context.get("status")
            or context.get("local_partition")
            or ""
        ).strip().upper()
        if (
            symbol
            and raw.get("local_decision") is not True
            and deterministic_status in {"REVIEW_CANDIDATE", "LOCAL_MONITOR"}
            and not _a2_rejection_has_hard_fact_veto(raw, context)
        ):
            demoted = dict(raw)
            reasons = demoted.get("reason_codes") if isinstance(demoted.get("reason_codes"), list) else []
            demoted["reason_codes"] = list(dict.fromkeys([*reasons, "A2_LLM_REJECT_DEMOTED_TO_WATCH"]))
            demoted["status"] = "WATCH_ONLY"
            demoted["local_partition"] = "LLM_REJECT_DEMOTED_TO_WATCH"
            demoted["llm_decision"] = "REJECT"
            watch.append(demoted)
            changed += 1
        else:
            retained.append(raw)
    if changed:
        result["watch_only_pool"] = _deduplicate_stage_items("watch_only_pool", watch)
        result["rejected_candidates"] = _deduplicate_stage_items("rejected_candidates", retained)
    return result, changed


def _canonicalize_a2_bottleneck_scorecards(
    output: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
) -> tuple[dict[str, Any], int]:
    """Recompute scorecards only for the strict supply-chain route.

    MARKET_CORE is a market-structure route and must not be rejected merely
    because it has no scarcity scorecard.  When a provider omits ``a2_route``,
    the server may fill it only from the frozen deterministic route context.
    """

    result = dict(output)
    changed = 0
    for pool in ("focus_pool", "watch_only_pool"):
        raw_items = result.get(pool)
        if not isinstance(raw_items, list):
            continue
        normalized: list[Any] = []
        for raw_item in raw_items:
            if not isinstance(raw_item, Mapping):
                normalized.append(raw_item)
                continue
            item = dict(raw_item)
            symbol = _first_symbol(item)
            route = _a2_item_route(item, snapshot_data, symbol)
            if route and item.get("a2_route") != route:
                item["a2_route"] = route
                changed += 1
            if route == MARKET_CORE_ROUTE:
                if item.get("bottleneck_status") != "NOT_REQUIRED_FOR_MARKET_CORE":
                    item["bottleneck_status"] = "NOT_REQUIRED_FOR_MARKET_CORE"
                    changed += 1
                normalized.append(item)
                continue
            scorecard, reasons = canonicalize_model_scorecard(item.get("bottleneck_scorecard"))
            if scorecard is not None:
                if item.get("bottleneck_scorecard") != scorecard:
                    changed += 1
                item["bottleneck_scorecard"] = scorecard
                item["bottleneck_score"] = scorecard["final_score"]
            elif reasons:
                existing = item.get("reason_codes") if isinstance(item.get("reason_codes"), list) else []
                item["reason_codes"] = list(dict.fromkeys([*existing, *reasons]))
            normalized.append(item)
        result[pool] = normalized
    return result, changed


def _a2_compact_theme_identity_reasons(output, snapshot_data, expected_symbols):
    """Do not silently merge different board scores into one monthly theme."""
    reviews = output.get("theme_reviews") if isinstance(output, Mapping) else None
    contexts = _lineage_context_rows(snapshot_data, "A2_BOTTLENECK_CONTEXT")
    allowed = {str(contexts.get(symbol, {}).get("theme_id") or "") for symbol in expected_symbols}
    allowed.discard("")
    if not isinstance(reviews, list) or not allowed:
        return []
    reasons, seen = [], set()
    for row in reviews:
        if not isinstance(row, Mapping):
            continue
        theme = str(row.get("theme_id") or "")
        if theme not in allowed:
            reasons.append("A2_THEME_REVIEW_ID_NOT_CANONICAL:" + theme)
        elif theme in seen:
            reasons.append("A2_THEME_REVIEW_ID_DUPLICATED:" + theme)
        seen.add(theme)
    return reasons


def _expand_a2_compact_output(
    output: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
    expected_symbols: set[str],
) -> dict[str, Any]:
    """Expand compact A2 promotions/vetoes into the complete review partition.

    The quant gate owns the review domain. DeepSeek only names FOCUS
    promotions and explicit REJECT proposals; every unmentioned supplied
    symbol is a deterministic WATCH. This prevents a malformed or truncated
    provider response from silently deleting candidates.
    """

    if not isinstance(output, Mapping):
        return dict(output) if isinstance(output, dict) else {}
    reviews = output.get("theme_reviews")
    focus_decisions = output.get("focus_decisions")
    reject_decisions = output.get("reject_decisions")
    if not isinstance(reviews, list):
        return dict(output)
    if focus_decisions is None:
        focus_decisions = []
    if reject_decisions is None:
        reject_decisions = []
    if not isinstance(focus_decisions, list) or not isinstance(reject_decisions, list):
        return dict(output)

    expected = {
        _first_symbol(symbol)
        for symbol in expected_symbols
        if _first_symbol(symbol)
    }

    history = snapshot_data.get("SECTOR_CYCLE_SNAPSHOT")
    history_metrics = history.get("history_metrics") if isinstance(history, Mapping) else None
    overlap = (
        _safe_float(history_metrics.get("top3_daily_overlap"))
        if isinstance(history_metrics, Mapping) and history_metrics.get("available") is True
        else 0.0
    )

    def codes(value: Any, *, limit: int) -> list[str]:
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
            return []
        return [str(item).strip() for item in value[:limit] if str(item).strip()]

    themes: list[dict[str, Any]] = []
    for raw in reviews:
        if not isinstance(raw, Mapping):
            continue
        penalty_points = _safe_float(raw.get("penalty_points"))
        penalty_codes = codes(raw.get("penalty_codes"), limit=3)
        themes.append({
            "theme_id": raw.get("theme_id"),
            "stage": raw.get("stage"),
            "new_entry_policy": raw.get("new_entry_policy"),
            "theme_score": 0.0,
            "score_breakdown": dict(raw.get("score_breakdown") or {}),
            "penalties": ([{
                "points": penalty_points,
                "reason_codes": penalty_codes,
            }] if penalty_points or penalty_codes else []),
            "supporting_evidence": codes(raw.get("support_codes"), limit=2),
            "contradicting_evidence": codes(raw.get("risk_codes"), limit=2),
            "rotation_overlap_ratio": overlap,
        })
        # Compact transport has no lifecycle-onset field. Record the dated
        # review and its exact quant inputs instead of inventing stage_since.
        context = snapshot_data.get("A2_BOTTLENECK_CONTEXT")
        context = context if isinstance(context, Mapping) else {}
        theme_inputs = {str(key): value for key, value in context.items()
                        if isinstance(value, Mapping)
                        and value.get("theme_id") == raw.get("theme_id")}
        reference = snapshot_data.get("A2_MARKET_REFERENCE")
        observed = str(reference.get("market_trade_date") or "") if isinstance(reference, Mapping) else ""
        if theme_inputs and re.fullmatch(r"\d{4}-\d{2}-\d{2}", observed):
            themes[-1]["stage_observed_as_of"] = observed
            themes[-1]["stage_evidence_hash"] = _sha256_json({"review": dict(raw), "inputs": theme_inputs, "as_of": observed})
            themes[-1]["source_refs"] = [f"A2_BOTTLENECK_CONTEXT:{key}" for key in sorted(theme_inputs)]
        # Preserve richer evidence if supplied by an older transport.
        for field in ("stage_since", "source_refs"):
            if raw.get(field):
                themes[-1][field] = raw[field]

    focus_by_symbol: dict[str, dict[str, Any]] = {}
    reject_by_symbol: dict[str, dict[str, Any]] = {}
    for raw in focus_decisions:
        if not isinstance(raw, Mapping):
            continue
        symbol = _first_symbol(raw.get("symbol"))
        if symbol not in expected or symbol in focus_by_symbol:
            continue
        support = codes(raw.get("support_codes"), limit=2)
        risk = codes(raw.get("risk_codes"), limit=2)
        focus_by_symbol[symbol] = {
            "symbol": symbol,
            "selection_reasons": support or ["A2_LLM_FOCUS_PROMOTION"],
            "risk_reasons": risk,
            "risk_flags": risk,
        }
    for raw in reject_decisions:
        if not isinstance(raw, Mapping):
            continue
        symbol = _first_symbol(raw.get("symbol"))
        if symbol not in expected or symbol in reject_by_symbol:
            continue
        risk = codes(raw.get("risk_codes"), limit=3)
        if not risk:
            continue
        reject_by_symbol[symbol] = {"symbol": symbol, "reason_codes": risk}

    # When the model contradicts itself, the conservative classification wins.
    # The later server policy still demotes unsupported vetoes back to WATCH.
    for symbol in reject_by_symbol:
        focus_by_symbol.pop(symbol, None)
    classified = set(focus_by_symbol).union(reject_by_symbol)
    watch = [
        {
            "symbol": symbol,
            "selection_reasons": ["A2_QUANT_ELIGIBLE_LLM_WATCH"],
            "risk_reasons": ["A2_LLM_NOT_PROMOTED_TO_FOCUS"],
        }
        for symbol in sorted(expected.difference(classified))
    ]

    return {
        "envelope": output.get("envelope"),
        "analysis_summary": {
            "reason_codes": codes(output.get("reason_codes"), limit=4),
            "compact_transport_expanded": True,
        },
        "rotation_reviews": output.get("rotation_reviews", []),
        "active_themes": themes,
        "focus_pool": [focus_by_symbol[symbol] for symbol in sorted(focus_by_symbol)],
        "watch_only_pool": watch,
        "rejected_candidates": [
            reject_by_symbol[symbol] for symbol in sorted(reject_by_symbol)
        ],
    }


def _canonicalize_a2_contract_semantics(
    output: Mapping[str, Any],
) -> tuple[dict[str, Any], int]:
    """Normalize provider-owned A2 vocabulary without changing selection facts.

    A2 requests are sent in transport batches, but the capacity target belongs
    to the globally merged result.  Providers sometimes apply that target to
    one batch and emit ``POOL_CAPACITY_FULL`` as if it were a symbol-level
    veto.  Remove that false reason and leave an explicit server audit reason;
    the row stays in its original partition and the global policy remains the
    only code allowed to apply capacity.

    ``COOLING`` is a weekly momentum state, not a theme lifecycle stage.  A
    provider response that puts it in ``stage``/``theme_stage`` is repaired to
    ``DIVERGENCE`` while preserving the cooling state for downstream
    explanation.  That is the nearest non-terminal lifecycle state: it keeps
    the candidate observable but cannot misrepresent weakening momentum as a
    fresh confirmation.  Other invalid lifecycle values are intentionally
    left for the normal validator rather than guessed.
    """

    result = dict(output)
    changed = 0

    def append_reason(item: dict[str, Any], reason: str) -> None:
        nonlocal changed
        raw = item.get("reason_codes")
        if isinstance(raw, str):
            reasons = [raw]
        elif isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
            reasons = [value for value in raw]
        else:
            reasons = []
        if reason in reasons:
            return
        item["reason_codes"] = [*reasons, reason]
        changed += 1

    def normalize_reason_codes(item: dict[str, Any]) -> None:
        nonlocal changed
        raw = item.get("reason_codes")
        if isinstance(raw, str):
            values = [raw]
        elif isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
            values = list(raw)
        else:
            return
        ignored = [
            value for value in values
            if isinstance(value, str) and value.strip().upper() in _A2_CAPACITY_REASON_CODES
        ]
        if not ignored:
            return
        retained = [
            value for value in values
            if not (isinstance(value, str) and value.strip().upper() in _A2_CAPACITY_REASON_CODES)
        ]
        item["reason_codes"] = retained
        changed += 1
        append_reason(item, _A2_CAPACITY_REASON_IGNORED)

    def normalize_stage(item: dict[str, Any], field: str) -> None:
        nonlocal changed
        raw_stage = item.get(field)
        stage = str(raw_stage or "").strip().upper()
        if stage != "COOLING":
            return
        if item.get("weekly_momentum_state") != "COOLING":
            item["weekly_momentum_state"] = "COOLING"
            changed += 1
        if item.get(field) != _A2_COOLING_FALLBACK_STAGE:
            item[field] = _A2_COOLING_FALLBACK_STAGE
            changed += 1
        append_reason(item, _A2_COOLING_STAGE_NORMALIZED)

    active_themes = result.get("active_themes")
    if isinstance(active_themes, list):
        normalized_themes: list[Any] = []
        for raw_theme in active_themes:
            if not isinstance(raw_theme, Mapping):
                normalized_themes.append(raw_theme)
                continue
            theme = dict(raw_theme)
            normalize_stage(theme, "stage")
            normalize_reason_codes(theme)
            normalized_themes.append(theme)
        result["active_themes"] = normalized_themes

    for pool in _STAGE_LINEAGE_POOLS["A2"]:
        raw_items = result.get(pool)
        if not isinstance(raw_items, list):
            continue
        normalized_items: list[Any] = []
        for raw_item in raw_items:
            if not isinstance(raw_item, Mapping):
                normalized_items.append(raw_item)
                continue
            item = dict(raw_item)
            normalize_stage(item, "theme_stage")
            # Accept the provider's occasional row-level alias as well.  The
            # canonical A2 row field remains theme_stage; normalizing stage
            # here prevents a non-contract COOLING value from leaking through.
            normalize_stage(item, "stage")
            normalize_reason_codes(item)
            normalized_items.append(item)
        result[pool] = normalized_items

    summary = result.get("analysis_summary")
    if isinstance(summary, Mapping):
        normalized_summary = dict(summary)
        normalize_reason_codes(normalized_summary)
        result["analysis_summary"] = normalized_summary
    return result, changed


def _enrich_a2_decision_facts(
    output: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
) -> tuple[dict[str, Any], int]:
    """Attach explicit A2 fact states to model pool rows.

    The deterministic gate already has symbol-scoped factor results, but the
    provider response historically exposed only a subset of them.  That made
    the workbench report several facts as ``missing`` even when the server had
    an observed value (or had an explicit unavailable state).  These facts are
    server-owned, so copy the complete deterministic record even when a model
    returned a smaller object with the same alias.  Optional facts remain
    visibly unavailable instead of being represented by a fabricated zero.
    """

    result = dict(output)
    raw_context = snapshot_data.get("A2_BOTTLENECK_CONTEXT")
    contexts = raw_context if isinstance(raw_context, Mapping) else {}
    if not contexts:
        return result, 0

    changed = 0
    factor_aliases: Mapping[str, tuple[str, ...]] = {
        "capital_flow": ("capital_flow", "fund_flow", "capital_score", "capital_flow_score"),
        "tier_structure": ("tier_structure", "tier_position", "tier", "ladder", "tier_table"),
        "leader_structure": ("leader_structure", "leader_subtype", "leader_role", "identifiability_score"),
        "index_chain_resonance": (
            "index_chain_resonance",
            "index_chain_resonance_score",
            "chain_resonance_score",
        ),
    }

    def has_fact(item: Mapping[str, Any], names: Sequence[str]) -> bool:
        return any(_a2_fact_value_present(item.get(name)) for name in names)

    for pool in ("focus_pool", "watch_only_pool"):
        raw_items = result.get(pool)
        if not isinstance(raw_items, list):
            continue
        normalized: list[Any] = []
        for raw_item in raw_items:
            if not isinstance(raw_item, Mapping):
                normalized.append(raw_item)
                continue
            item = dict(raw_item)
            symbol = _first_symbol(item)
            context = contexts.get(symbol) if symbol else None
            if not isinstance(context, Mapping):
                normalized.append(item)
                continue

            factors = context.get("a2_factor_scores")
            factors = factors if isinstance(factors, Mapping) else {}
            weak_fields = _a2_fact_name_list(item.get("weak_evidence_fields"))
            deterministic_role = str(
                context.get("deterministic_market_role")
                or context.get("market_role")
                or ""
            ).strip().upper()
            if deterministic_role and item.get("market_role") != deterministic_role:
                # Market role is a deterministic classification, not a model
                # vote. Leaving a historical generic LEADER in place would
                # shadow TREND_LEADER/EMOTION_LEADER and route a non-ladder
                # stock into the A3 leader strategy.
                item["market_role"] = deterministic_role
                changed += 1
            for name, aliases in factor_aliases.items():
                factor = factors.get(name)
                if isinstance(factor, Mapping):
                    available = factor.get("available") is True and _a2_number_present(factor.get("score"))
                    # The model-facing schema historically kept only
                    # available/score/source and silently discarded fields
                    # such as ladder_height and availability_state.  A3 needs
                    # the complete point-in-time fact contract; the server copy
                    # is authoritative and must not be shadowed by that lossy
                    # projection.
                    if item.get(name) != factor:
                        item[name] = dict(factor)
                        changed += 1
                    if name == "capital_flow" and "capital_flow_available" not in item:
                        # A boolean availability flag is safe to expose even
                        # when the score itself is intentionally null.
                        item["capital_flow_available"] = available
                        changed += 1
                    if not available:
                        weak_fields.append(name)

            route = str(
                context.get("preferred_route")
                or context.get("deterministic_route")
                or context.get("route")
                or item.get("a2_route")
                or ""
            ).strip().upper()
            # MARKET_CORE is intentionally not a supply-chain scarcity claim.
            # Still expose an explicit state so the UI can distinguish
            # "not required for this route" from an omitted model field.
            if route == MARKET_CORE_ROUTE and not _a2_fact_value_present(item.get("supply_chain_role")):
                item["supply_chain_role"] = {
                    "available": False,
                    "score": None,
                    "source": "A2_BOTTLENECK_CONTEXT",
                    "reason_code": "NOT_REQUIRED_FOR_MARKET_CORE",
                }
                changed += 1
                weak_fields.append("supply_chain_role")

            if not has_fact(item, ("crowding", "crowding_score", "chase_risk_level", "crowding_flags")):
                item["crowding"] = _a2_unavailable_crowding_fact(snapshot_data)
                changed += 1
                weak_fields.append("crowding")

            coverage = context.get("factor_coverage")
            if isinstance(coverage, Mapping) and not _a2_fact_value_present(item.get("factor_coverage")):
                item["factor_coverage"] = dict(coverage)
                changed += 1
            missing_optional = context.get("missing_optional_factors")
            if isinstance(missing_optional, Sequence) and not isinstance(missing_optional, (str, bytes, bytearray)):
                deterministic_missing = [str(value) for value in missing_optional if str(value).strip()]
                if deterministic_missing:
                    weak_fields.extend(deterministic_missing)
                    existing_missing = _a2_fact_name_list(item.get("missing_optional_factors"))
                    merged_missing = list(dict.fromkeys([*existing_missing, *deterministic_missing]))
                    if item.get("missing_optional_factors") != merged_missing:
                        item["missing_optional_factors"] = merged_missing
                        changed += 1

            deterministic_state = context.get("data_sufficiency_state")
            if deterministic_state and item.get("deterministic_data_sufficiency_state") != deterministic_state:
                item["deterministic_data_sufficiency_state"] = deterministic_state
                changed += 1
            for field in ("gate_results", "first_blocking_gate", "all_failed_gates"):
                expected = context.get(field)
                if item.get(field) == expected:
                    continue
                if isinstance(expected, Mapping):
                    item[field] = dict(expected)
                elif isinstance(expected, Sequence) and not isinstance(expected, (str, bytes, bytearray)):
                    item[field] = list(expected)
                else:
                    item[field] = expected
                changed += 1
            normalized_weak = list(dict.fromkeys(weak_fields))
            if normalized_weak and item.get("weak_evidence_fields") != normalized_weak:
                item["weak_evidence_fields"] = normalized_weak
                changed += 1
            normalized.append(item)
        result[pool] = normalized
    block_counts: dict[str, int] = {}
    for context in contexts.values():
        if not isinstance(context, Mapping):
            continue
        for gate_name in context.get("all_failed_gates") or ():
            name = str(gate_name)
            if name:
                block_counts[name] = block_counts.get(name, 0) + 1
    summary = dict(result.get("analysis_summary")) if isinstance(result.get("analysis_summary"), Mapping) else {}
    summary["gate_block_counts"] = dict(sorted(block_counts.items()))
    result["analysis_summary"] = summary
    return result, changed


def _a2_fact_value_present(value: Any) -> bool:
    """Whether a row already contains an explicit, non-empty fact value."""

    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (Mapping, Sequence)) and not isinstance(value, (str, bytes, bytearray)):
        return bool(value)
    return True


def _a2_number_present(value: Any) -> bool:
    if value is None or isinstance(value, bool):
        return False
    try:
        return bool(float(value) == float(value))
    except (TypeError, ValueError, OverflowError):
        return False


def _a2_fact_name_list(value: Any) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _a2_unavailable_crowding_fact(snapshot_data: Mapping[str, Any]) -> dict[str, Any]:
    """Return an explicit optional-crowding state without inventing a score."""

    raw = snapshot_data.get("CROWDING_SNAPSHOT")
    source = raw.get("source") if isinstance(raw, Mapping) else None
    reason = raw.get("reason_code") if isinstance(raw, Mapping) else None
    return {
        "available": False,
        "score": None,
        "source": str(source or "CROWDING_SNAPSHOT"),
        "reason_code": "A2_CROWDING_OPTIONAL_UNAVAILABLE",
        "upstream_reason_code": str(reason or "SOURCE_UNAVAILABLE"),
    }


def _a2_available_theme_weights(
    snapshot_data: Mapping[str, Any],
    weights: Mapping[str, float],
) -> tuple[dict[str, float], float, tuple[str, ...]]:
    """Return A2 weights that are factually available for theme scoring."""

    available = dict(weights)
    unavailable: list[str] = []
    capital_flow = snapshot_data.get("CAPITAL_FLOW_SNAPSHOT")
    if "capital_flow" in available and (
        not isinstance(capital_flow, Mapping) or capital_flow.get("available") is not True
    ):
        available.pop("capital_flow", None)
        unavailable.append("capital_flow")
    return available, sum(available.values()), tuple(unavailable)


def _a2_relative_top5_market_core_exception(
    item: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
) -> bool:
    """Return whether the A2 score line is advisory for this focus row.

    ``screen_a2`` chooses up to five themes from frozen market facts before
    the model sees them.  The old post-model policy treated ``MIN_THEME_SCORE``
    as an unconditional veto, so a relative fourth/fifth theme was silently
    removed even when its MARKET_CORE route was valid.  This helper keeps the
    exception narrow: the frozen context must identify the row as a TOP5
    theme, the row must have passed the local review route, and no deterministic
    identity/tradability/data veto may be present.  A missing context never
    creates an exception for a new v2 snapshot.
    """

    symbol = _first_symbol(item)
    raw_contexts = snapshot_data.get("A2_BOTTLENECK_CONTEXT")
    context = raw_contexts.get(symbol) if isinstance(raw_contexts, Mapping) and symbol else None

    # Production v2 rows are authoritative only when their symbol-scoped
    # deterministic context is present.  Legacy direct callers can still use
    # the explicit item fields, preserving replay compatibility without making
    # a missing v2 context look like evidence.
    if isinstance(raw_contexts, Mapping):
        if not isinstance(context, Mapping):
            return False
        top_rotation_theme = context.get("top_rotation_theme") is True
        emotion_core_eligible = context.get("emotion_core_eligible") is True
        local_status = str(
            context.get("deterministic_status")
            or context.get("status")
            or context.get("local_partition")
            or ""
        ).strip().upper()
        if local_status != "REVIEW_CANDIDATE":
            return False
        eligible_routes = context.get("eligible_routes")
        route_values = {
            str(value).strip().upper()
            for value in eligible_routes
            if isinstance(value, str) and value.strip()
        } if isinstance(eligible_routes, Sequence) and not isinstance(
            eligible_routes, (str, bytes, bytearray)
        ) else set()
        if MARKET_CORE_ROUTE not in route_values:
            return False
        route_eligibility = context.get("route_eligibility")
        if isinstance(route_eligibility, Mapping):
            market_route = route_eligibility.get(MARKET_CORE_ROUTE)
            if not isinstance(market_route, Mapping) or market_route.get("eligible") is not True:
                return False
        context_reasons: list[str] = []
        for key in ("deterministic_reason_codes", "hard_reason_codes", "reason_codes"):
            values = context.get(key)
            if isinstance(values, str):
                values = [values]
            if isinstance(values, Sequence) and not isinstance(values, (str, bytes, bytearray)):
                context_reasons.extend(
                    str(value).strip().upper()
                    for value in values
                    if isinstance(value, str) and value.strip()
                )
        failed_gates = context.get("all_failed_gates")
        if isinstance(failed_gates, str):
            failed_gates = [failed_gates]
        if isinstance(failed_gates, Sequence) and not isinstance(failed_gates, (str, bytes, bytearray)):
            # THEME_SCORE_MIN is intentionally observation-only in the
            # deterministic gate.  It is the one score check this helper is
            # allowed to bypass; every other effective failed gate remains a
            # hard veto.
            hard_failed_gates = {
                str(value).strip().upper()
                for value in failed_gates
                if isinstance(value, str) and value.strip()
            } - {
                "THEME_SCORE_MIN",
                # Frozen pre-fix contexts may still list this comparison as
                # a failed gate.  Identifiability is now ordering/risk
                # evidence only and cannot invalidate a fact-qualified TOP5
                # market-core row during replay.
                "IDENTIFIABILITY_MIN",
            }
            if hard_failed_gates:
                return False
        if any(
            any(marker in reason for marker in _A2_RELATIVE_TOP5_HARD_VETO_MARKERS)
            for reason in context_reasons
        ):
            return False
    else:
        top_rotation_theme = item.get("top_rotation_theme") is True
        emotion_core_eligible = item.get("emotion_core_eligible") is True
        eligible_routes = item.get("eligible_routes")
        route_values = {
            str(value).strip().upper()
            for value in eligible_routes
            if isinstance(value, str) and value.strip()
        } if isinstance(eligible_routes, Sequence) and not isinstance(
            eligible_routes, (str, bytes, bytearray)
        ) else set()
        explicit_route = str(
            item.get("a2_route") or item.get("route") or item.get("selection_route") or ""
        ).strip().upper()
        if explicit_route == MARKET_CORE_ROUTE:
            route_values.add(MARKET_CORE_ROUTE)
        if MARKET_CORE_ROUTE not in route_values:
            return False

    if not top_rotation_theme and not emotion_core_eligible and not _is_rotation_reserve(context if isinstance(context, Mapping) else item):
        return False

    # Model-owned fields may explain or veto a candidate, but cannot turn a
    # deterministic hard fact into a pass.  The score/reference-line and
    # optional-fact reasons are deliberately excluded from this marker check.
    item_reasons = item.get("reason_codes")
    if isinstance(item_reasons, str):
        item_reasons = [item_reasons]
    if isinstance(item_reasons, Sequence) and not isinstance(item_reasons, (str, bytes, bytearray)):
        for raw_reason in item_reasons:
            reason = str(raw_reason).strip().upper()
            if any(marker in reason for marker in _A2_RELATIVE_TOP5_HARD_VETO_MARKERS):
                return False
    review_status = str(item.get("review_status") or "").strip().upper()
    if review_status in {"VETO", "REJECT", "REJECTED"}:
        return False
    return True


def _a2_bottleneck_reasons(
    item: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
) -> list[str]:
    """Require a source-backed scarce-layer thesis for every A2 focus item."""

    reasons: list[str] = []
    symbol = _first_symbol(item)
    raw_context = snapshot_data.get("A2_BOTTLENECK_CONTEXT")
    # Pre-v2/legacy snapshots remain replayable. New deterministic-v2 runs
    # always attach this context after the local A2 gate.
    if not isinstance(raw_context, Mapping):
        return reasons
    context = raw_context.get(symbol) if isinstance(raw_context, Mapping) else None
    if not isinstance(context, Mapping):
        reasons.append("A2_BOTTLENECK_CONTEXT_MISSING")
    route = _a2_item_route(item, snapshot_data, symbol)
    if route == MARKET_CORE_ROUTE:
        if str(item.get("bottleneck_status") or "").strip().upper() != "NOT_REQUIRED_FOR_MARKET_CORE":
            reasons.append("A2_MARKET_CORE_STATUS_INVALID")
        return list(dict.fromkeys(reasons))
    if route != SUPPLY_CHAIN_ALPHA_ROUTE:
        reasons.append("A2_ROUTE_MISSING_OR_INVALID")
        return list(dict.fromkeys(reasons))
    role = str(item.get("supply_chain_role") or "").strip()
    if role not in SUPPLY_CHAIN_ROLES or role == "STORY_ONLY":
        reasons.append("A2_SUPPLY_CHAIN_ROLE_NOT_FOCUS_ELIGIBLE")
    if not str(item.get("scarce_layer") or "").strip():
        reasons.append("A2_SCARCE_LAYER_MISSING")
    if not str(item.get("value_chain_position") or "").strip():
        reasons.append("A2_VALUE_CHAIN_POSITION_MISSING")
    scorecard, scorecard_reasons = canonicalize_model_scorecard(item.get("bottleneck_scorecard"))
    reasons.extend(scorecard_reasons)
    if scorecard is not None and role in {"CONTROLS_SCARCE_LAYER", "SUPPLIES_SCARCE_LAYER"}:
        factors = scorecard["factors"]
        if (
            _safe_float(factors.get("chokepoint_severity")) < 3.0
            or max(
                _safe_float(factors.get("supplier_concentration")),
                _safe_float(factors.get("expansion_difficulty")),
            ) < 2.0
        ):
            reasons.append("A2_SCARCE_LAYER_SCORE_UNSUPPORTED")

    evidence = item.get("bottleneck_evidence")
    evidence = evidence if isinstance(evidence, list) else []
    allowed_refs = _snapshot_primary_evidence_refs(snapshot_data)
    if isinstance(context, Mapping):
        allowed_refs.update(
            str(value).strip()
            for value in context.get("source_refs", ())
            if isinstance(value, str) and value.strip()
        )
    allowed_refs.update(
        str(value).strip()
        for value in item.get("source_refs", ())
        if isinstance(value, str) and value.strip()
    ) if isinstance(item.get("source_refs"), list) else None
    valid_evidence = 0
    stronger_evidence = 0
    for raw in evidence:
        if not isinstance(raw, Mapping):
            continue
        strength = str(raw.get("strength") or "").strip().upper()
        source_ref = str(raw.get("source_ref") or "").strip()
        claim = str(raw.get("claim") or "").strip()
        if not claim or strength not in EVIDENCE_STRENGTHS or not source_ref:
            continue
        if allowed_refs and source_ref not in allowed_refs:
            continue
        valid_evidence += 1
        if strength in {"STRONG", "MEDIUM"}:
            stronger_evidence += 1
    if valid_evidence < 2:
        reasons.append("A2_BOTTLENECK_EVIDENCE_INSUFFICIENT")
    if stronger_evidence < 1:
        reasons.append("A2_BOTTLENECK_STRONG_EVIDENCE_MISSING")
    if not str(item.get("missing_proof") or "").strip():
        reasons.append("A2_BOTTLENECK_MISSING_PROOF_UNDECLARED")
    kill_switches = item.get("kill_switches")
    if not isinstance(kill_switches, list) or not any(str(value).strip() for value in kill_switches):
        reasons.append("A2_BOTTLENECK_KILL_SWITCH_MISSING")
    return list(dict.fromkeys(reasons))


def _a2_item_route(
    item: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
    symbol: str,
) -> str | None:
    """Return the server-owned A2 route for one symbol.

    A deterministic-v2 snapshot carries ``eligible_routes`` and a preferred
    route in ``A2_BOTTLENECK_CONTEXT``.  Those fields are authoritative and
    take precedence over a model response.  An empty v2 route list means that
    no route is available; it must never be silently inferred as
    ``SUPPLY_CHAIN_ALPHA``.  Explicit item routes are retained only for old
    snapshots that predate the route context contract.
    """

    raw_context = snapshot_data.get("A2_BOTTLENECK_CONTEXT")
    context = raw_context.get(symbol) if isinstance(raw_context, Mapping) else None
    if isinstance(context, Mapping):
        preferred = str(
            context.get("preferred_route")
            or context.get("deterministic_route")
            or context.get("route")
            or ""
        ).strip().upper()
        eligible = context.get("eligible_routes")
        if isinstance(eligible, Sequence) and not isinstance(eligible, (str, bytes, bytearray)):
            normalized = [str(value).strip().upper() for value in eligible if str(value).strip()]
            if preferred in normalized:
                return preferred
            if MARKET_CORE_ROUTE in normalized:
                return MARKET_CORE_ROUTE
            if SUPPLY_CHAIN_ALPHA_ROUTE in normalized:
                return SUPPLY_CHAIN_ALPHA_ROUTE
            # v2 explicitly says there is no usable route.  Do not inspect
            # model-supplied fields below this return.
            return None
        # A legacy snapshot may explicitly mark the route as such.  This is
        # intentionally opt-in for any context that has a route vocabulary;
        # merely having a v2 context mapping is no longer enough to infer the
        # supply-chain route.
        legacy_route = str(
            context.get("legacy_route")
            or context.get("legacy_selection_route")
            or ""
        ).strip().upper()
        if legacy_route in {MARKET_CORE_ROUTE, SUPPLY_CHAIN_ALPHA_ROUTE}:
            return legacy_route
        if context.get("legacy_route_inference") is True:
            return SUPPLY_CHAIN_ALPHA_ROUTE
        if "eligible_routes" not in context and not context.get("route_context_schema"):
            # Old bottleneck snapshots exposed only source references and had
            # no route vocabulary at all.  Their shape is the explicit legacy
            # compatibility signal.  A v2 snapshot always writes the schema
            # and an eligible_routes list (including an empty list).
            return SUPPLY_CHAIN_ALPHA_ROUTE
        return None

    # Pre-v2 callers/checkpoints may carry only an item-level route.  Keep this
    # compatibility path explicit and deterministic; it is not used by the
    # production v2 context created in ``_with_a2_bottleneck_context``.
    explicit = str(item.get("a2_route") or item.get("selection_route") or item.get("route") or "").strip().upper()
    if explicit in {MARKET_CORE_ROUTE, SUPPLY_CHAIN_ALPHA_ROUTE}:
        return explicit
    return None


def _apply_a2_lineage_policy(
    output: Mapping[str, Any],
    upstream_output: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
) -> tuple[dict[str, Any], int]:
    """Demote A2 focus items that rewrite A1 themes or invent missing facts."""

    result = dict(output)
    upstream_themes = upstream_output.get("structural_themes")
    allowed_theme_ids: set[str] = set()
    if isinstance(upstream_themes, list):
        for theme in upstream_themes:
            if not isinstance(theme, Mapping):
                continue
            theme_id = str(theme.get("theme_id") or "").strip()
            if theme_id:
                allowed_theme_ids.add(theme_id)

    # A1 deliberately has multiple entry routes.  Fundamental-baseline and
    # broker-gold rows may carry a real THS industry theme that is not one of
    # the monthly macro discovery IDs.  A2 may rotate among those themes, but
    # only when the theme is already attached to an actual A1 ACTIVE row or
    # was produced by the server-owned deterministic A2 context for that row.
    # This expands lineage authority, not the candidate universe.
    upstream_symbols: set[str] = set()
    upstream_pool = upstream_output.get("active_research_pool")
    if isinstance(upstream_pool, list):
        for raw_item in upstream_pool:
            if not isinstance(raw_item, Mapping):
                continue
            symbol = _first_symbol(raw_item)
            if symbol:
                upstream_symbols.add(symbol)
            for key in ("primary_theme", "theme_id"):
                theme_id = str(raw_item.get(key) or "").strip()
                if theme_id and theme_id.upper() != "UNMAPPED":
                    allowed_theme_ids.add(theme_id)
    raw_contexts = snapshot_data.get("A2_BOTTLENECK_CONTEXT")
    if isinstance(raw_contexts, Mapping):
        for raw_symbol, raw_context in raw_contexts.items():
            symbol = _first_symbol(raw_symbol)
            if symbol not in upstream_symbols or not isinstance(raw_context, Mapping):
                continue
            theme_id = str(raw_context.get("theme_id") or "").strip()
            if theme_id and theme_id.upper() != "UNMAPPED":
                allowed_theme_ids.add(theme_id)

    board_strategy_ids: dict[str, str] = {}
    selected_snapshot = snapshot_data.get("SELECTED_BOARD_SNAPSHOT")
    selected_boards = (
        selected_snapshot.get("boards")
        if isinstance(selected_snapshot, Mapping)
        else None
    )
    if isinstance(selected_boards, Sequence) and not isinstance(
        selected_boards, (str, bytes, bytearray)
    ):
        for board in selected_boards:
            if not isinstance(board, Mapping):
                continue
            board_id = str(
                board.get("board_code") or board.get("theme_id") or ""
            ).strip()
            strategy_id = str(
                board.get("strategy_theme_id") or board_id
            ).strip()
            if board_id and strategy_id:
                board_strategy_ids[board_id] = strategy_id

    active_themes = result.get("active_themes")
    valid_active_themes: set[str] = set()
    if isinstance(active_themes, list):
        normalized_themes: list[Any] = []
        for raw_theme in active_themes:
            if not isinstance(raw_theme, Mapping):
                normalized_themes.append(raw_theme)
                continue
            theme = dict(raw_theme)
            theme_id = str(theme.get("theme_id") or "").strip()
            canonical_theme_id = board_strategy_ids.get(theme_id)
            if (
                theme_id not in allowed_theme_ids
                and canonical_theme_id in allowed_theme_ids
            ):
                theme["rotation_board_id"] = theme_id
                theme["theme_id"] = canonical_theme_id
                theme_id = canonical_theme_id
                existing = (
                    theme.get("reason_codes")
                    if isinstance(theme.get("reason_codes"), list)
                    else []
                )
                theme["reason_codes"] = list(dict.fromkeys([
                    *existing,
                    "A2_BOARD_THEME_CANONICALIZED",
                ]))
            theme_reasons = _a2_theme_reasons(theme, snapshot_data)
            if theme_id not in allowed_theme_ids:
                theme_reasons.append("A2_THEME_OUTSIDE_A1")
            if theme_reasons:
                existing = theme.get("reason_codes") if isinstance(theme.get("reason_codes"), list) else []
                theme["reason_codes"] = list(dict.fromkeys([*existing, *theme_reasons]))
            else:
                valid_active_themes.add(theme_id)
            normalized_themes.append(theme)
        result["active_themes"] = normalized_themes

    focus = result.get("focus_pool")
    watch = list(result.get("watch_only_pool")) if isinstance(result.get("watch_only_pool"), list) else []
    if not isinstance(focus, list):
        return result, 0
    retained: list[Any] = []
    changed = 0
    for raw_item in focus:
        if not isinstance(raw_item, Mapping):
            retained.append(raw_item)
            continue
        item = dict(raw_item)
        reasons: list[str] = []
        theme_id = str(item.get("theme_id") or "").strip()
        if theme_id not in valid_active_themes:
            reasons.append("A2_THEME_LINEAGE_INVALID")
        role = str(item.get("market_role") or "").strip()
        if role not in A2_FOCUS_ROLES:
            reasons.append("A2_MARKET_ROLE_NOT_FOCUS_ELIGIBLE")
        reasons.extend(_a2_bottleneck_reasons(item, snapshot_data))
        active_theme = next((
            theme for theme in result.get("active_themes", ())
            if isinstance(theme, Mapping) and str(theme.get("theme_id") or "").strip() == theme_id
        ), None)
        if isinstance(active_theme, Mapping):
            if abs(_safe_float(item.get("theme_score")) - _safe_float(active_theme.get("theme_score"))) > 0.51:
                reasons.append("A2_THEME_SCORE_LINEAGE_MISMATCH")
            # A2 is the broad rotation/research funnel.  Theme-stage and
            # new-entry policy remain visible on the active theme, but they
            # are consumed as A3/A4 risk context rather than erasing a TOP5
            # market-core candidate before daily technical evaluation.
        if not reasons:
            retained.append(item)
            continue
        existing = item.get("reason_codes") if isinstance(item.get("reason_codes"), list) else []
        item["reason_codes"] = list(dict.fromkeys([*existing, *reasons]))
        watch.append(item)
        changed += 1
    result["focus_pool"] = retained
    result["watch_only_pool"] = _deduplicate_stage_items("watch_only_pool", watch)
    params = snapshot_data.get("REGIME_PARAM_SET")
    agent = params.get("agent_2") if isinstance(params, Mapping) else None
    if isinstance(agent, Mapping):
        focus_max = max(0, _safe_int(agent.get("focus_pool_max", len(retained))))
        ordered_focus = sorted(
            retained,
            key=lambda candidate: (
                -_safe_float(candidate.get("theme_score")) if isinstance(candidate, Mapping) else 0.0,
                -_safe_float(candidate.get("identifiability_score")) if isinstance(candidate, Mapping) else 0.0,
                _first_symbol(candidate) if isinstance(candidate, Mapping) else _canonical_json(candidate),
            ),
        )
        retained = ordered_focus[:focus_max]
        for raw_item in ordered_focus[focus_max:]:
            if not isinstance(raw_item, Mapping):
                watch.append(raw_item)
                continue
            item = dict(raw_item)
            existing = item.get("reason_codes") if isinstance(item.get("reason_codes"), list) else []
            item["reason_codes"] = list(dict.fromkeys([*existing, "A2_GLOBAL_FOCUS_LIMIT"]))
            watch.append(item)
            changed += 1
        result["focus_pool"] = retained
        result["watch_only_pool"] = _deduplicate_stage_items("watch_only_pool", watch)
    if changed:
        result["analysis_summary"] = _policy_summary(result, "A2", changed)
    return result, changed


def _annotate_a2_pool_target(
    output: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
) -> dict[str, Any]:
    """Report A2 research capacity without manufacturing candidates.

    ``focus_pool`` is deliberately narrow: it represents the strongest names
    in the leading rotation directions.  A2's usable downstream research
    scope also includes ``watch_only_pool``; A3 applies its own deterministic
    eligibility checks before technical review.  Treating focus alone as the
    A2 result made a healthy 3 + 99 partition appear to contain only 3 stocks.
    """

    result = dict(output)
    raw_targets = snapshot_data.get("A2_POOL_TARGETS")
    targets = raw_targets if isinstance(raw_targets, Mapping) else {}
    minimum = max(0, _safe_int(targets.get("pool_min", 20)))
    maximum = max(minimum, _safe_int(targets.get("pool_max", 60)))
    focus_count = len(result.get("focus_pool")) if isinstance(result.get("focus_pool"), list) else 0
    watch_rows = result.get("watch_only_pool") if isinstance(result.get("watch_only_pool"), list) else []
    watch_count = len(watch_rows)
    effective_watch_rows = [
        item for item in watch_rows
        if isinstance(item, Mapping) and _a2_watch_row_research_eligible(item)
    ]
    rejected_count = len(result.get("rejected_candidates")) if isinstance(result.get("rejected_candidates"), list) else 0
    effective_count = focus_count + len(effective_watch_rows)
    a3_candidate_count = focus_count + sum(
        1 for item in effective_watch_rows if _a3_watch_only_candidate_eligible(item)
    )
    direction_ids: set[str] = set()
    for item in result.get("focus_pool") or ():
        if not isinstance(item, Mapping):
            continue
        direction_id = str(
            item.get("rotation_direction_id")
            or item.get("theme_id")
            or item.get("primary_theme")
            or ""
        ).strip()
        if direction_id:
            direction_ids.add(direction_id)
    summary = dict(result.get("analysis_summary")) if isinstance(result.get("analysis_summary"), Mapping) else {}
    reason_codes = summary.get("reason_codes") if isinstance(summary.get("reason_codes"), list) else []
    reason_codes = [str(code) for code in reason_codes if str(code) != "POOL_TARGET_UNDERFILLED"]
    if effective_count < minimum:
        reason_codes = list(dict.fromkeys([*reason_codes, "POOL_TARGET_UNDERFILLED"]))
    notes = summary.get("notes") if isinstance(summary.get("notes"), list) else []
    summary["notes"] = [note for note in notes if str(note).strip() != "POOL_TARGET_UNDERFILLED"]
    summary.update({
        "focus_pool_count": focus_count,
        "watch_only_pool_count": watch_count,
        "effective_research_pool_count": effective_count,
        "a3_candidate_count": a3_candidate_count,
        "rejected_candidate_count": rejected_count,
        "rotation_direction_count": len(direction_ids),
        "pool_target": {"minimum": minimum, "maximum": maximum, "quota_forbidden": True},
        "pool_target_underfilled_by": max(0, minimum - effective_count),
        "reason_codes": reason_codes,
    })
    result["analysis_summary"] = summary
    return result


def _a2_watch_row_research_eligible(item: Mapping[str, Any]) -> bool:
    """Return whether an A2 watch row remains part of the effective pool."""

    if (item.get("top_rotation_theme") is False and not _is_rotation_reserve(item)
            and not (item.get("strong_trend_observation") is True
                     and item.get("research_observation_scope") == "REQUIRES_A3_A4_CONFIRMATION")
            and item.get("emotion_core_eligible") is not True):
        return False
    status = str(item.get("status") or "").strip().upper()
    if status in {"REJECTED", "HARD_REJECT", "DATA_GAP"}:
        return False
    sufficiency = str(item.get("data_sufficiency_state") or "").strip().upper()
    if sufficiency in {"INSUFFICIENT", "UNAVAILABLE", "MISSING", "DATA_GAP"}:
        return False
    return not bool(_output_reason_codes({"watch_only_pool": [item]}).intersection(_A2_EVIDENCE_GAP_REASONS))


def _a2_theme_reasons(theme: Mapping[str, Any], snapshot_data: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    if str(theme.get("stage") or "") not in _A2_THEME_LIFECYCLE_STAGES:
        reasons.append("A2_THEME_STAGE_INVALID")
    if str(theme.get("new_entry_policy") or "") not in {
        "ALLOW", "PROBE_ONLY", "WATCH_ONLY", "NO_NEW_ENTRY",
    }:
        reasons.append("A2_NEW_ENTRY_POLICY_INVALID")
    for field, reason in (
        ("supporting_evidence", "A2_SUPPORTING_EVIDENCE_MISSING"),
        ("contradicting_evidence", "A2_CONTRADICTING_EVIDENCE_MISSING"),
    ):
        value = theme.get(field)
        if not isinstance(value, list) or not value:
            reasons.append(reason)
    raw_weights = snapshot_data.get("THEME_SCORE_WEIGHTS")
    breakdown = theme.get("score_breakdown")
    if isinstance(raw_weights, Mapping) and raw_weights:
        weights = {str(key): _safe_float(value) for key, value in raw_weights.items()}
        if not isinstance(breakdown, Mapping):
            reasons.append("A2_SCORE_BREAKDOWN_MISSING")
        elif set(breakdown) != set(weights):
            reasons.append("A2_SCORE_BREAKDOWN_INVALID")
        else:
            values = {key: _safe_float(breakdown.get(key)) for key in weights}
            if any(value < 0 or value > 100 for value in values.values()):
                reasons.append("A2_SCORE_BREAKDOWN_INVALID")
            else:
                penalties = theme.get("penalties")
                penalty_points = sum(
                    _safe_float(item.get("points"))
                    for item in penalties
                    if isinstance(item, Mapping)
                ) if isinstance(penalties, list) else 0.0
                score_weights, available_weight, _ = _a2_available_theme_weights(snapshot_data, weights)
                weighted_total = sum(values[key] * score_weights.get(key, 0.0) for key in weights)
                normalized_total = weighted_total / available_weight if available_weight > 0 else 0.0
                computed = max(0.0, min(100.0, normalized_total + penalty_points))
                if abs(computed - _safe_float(theme.get("theme_score"))) > 0.51:
                    reasons.append("A2_THEME_SCORE_MISMATCH")
                capital_flow = snapshot_data.get("CAPITAL_FLOW_SNAPSHOT")
                if (
                    isinstance(capital_flow, Mapping)
                    and capital_flow.get("available") is not True
                    and values.get("capital_flow", 0.0) != 0.0
                ):
                    reasons.append("A2_CAPITAL_FLOW_SCORE_INVENTED")
    history = snapshot_data.get("SECTOR_CYCLE_SNAPSHOT")
    metrics = history.get("history_metrics") if isinstance(history, Mapping) else None
    if isinstance(metrics, Mapping) and metrics.get("available") is True:
        expected_overlap = _safe_float(metrics.get("top3_daily_overlap"))
        if abs(_safe_float(theme.get("rotation_overlap_ratio")) - expected_overlap) > 0.001:
            reasons.append("A2_ROTATION_OVERLAP_MISMATCH")
    return reasons


def _compact_a2_upstream_candidate(item: Mapping[str, Any]) -> dict[str, Any]:
    """Expose only A1 facts that are not repeated by the A2 gate context."""

    result = {
        "symbol": item.get("symbol") or item.get("code"),
        "name": item.get("company_name") or item.get("name"),
        "a1_theme": item.get("theme_id") or item.get("primary_theme"),
        "node": item.get("industry_chain_node"),
        "a1_score": item.get("structural_score"),
        "financial_quality": item.get("financial_quality_score"),
        "data_quality": item.get("data_quality_score"),
        "evidence_confidence": item.get("evidence_confidence"),
        "business_match": item.get("disclosed_business_match"),
    }
    fundamental = item.get("fundamental_support")
    if isinstance(fundamental, Mapping):
        result["fundamental_supported"] = fundamental.get("supported")
        latest = fundamental.get("latest_half_year")
        if isinstance(latest, Mapping):
            result["half_year_revenue_yoy"] = latest.get("operating_income_yoy_pct")
            result["half_year_profit_yoy"] = latest.get("parent_holder_net_profit_yoy_pct")
    return {key: value for key, value in result.items() if value is not None}


def _a2_rotation_retry_feedback(output, snapshot_data, upstream_symbols):
    contexts = _lineage_context_rows(snapshot_data, "A2_BOTTLENECK_CONTEXT")
    projected = _project_a2_bottleneck_context(contexts, upstream_symbols)
    scope = projected.get("_rotation_review_scope", {})
    focus_by_direction: dict[str, list[str]] = {}
    for row in output.get("focus_pool", []):
        symbol = _first_symbol(row.get("symbol")) if isinstance(row, Mapping) else ""
        direction = str(contexts.get(symbol, {}).get("rotation_direction_id") or "")
        if direction in scope:
            focus_by_direction.setdefault(direction, []).append(symbol)
    return {"already_focused_directions": focus_by_direction,
        "required_no_focus_if_focus_unchanged": {
            key: values for key, values in scope.items() if key not in focus_by_direction},
        "validation_errors": _validate_a2_rotation_focus_coverage(output, snapshot_data, upstream_symbols)}


def _validate_a2_rotation_focus_coverage(
    output: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
    upstream_symbols: set[str],
) -> list[str]:
    """Require a model focus decision or an explicit, attributable NO_FOCUS.

    Coverage is review completeness, never a permission to promote a row.
    The reported representative must belong to the frozen direction.
    """

    contexts = _lineage_context_rows(snapshot_data, "A2_BOTTLENECK_CONTEXT")
    expected: set[str] = set()
    for symbol in upstream_symbols:
        context = contexts.get(symbol)
        if not isinstance(context, Mapping):
            continue
        selected = context.get("selected_board")
        selected = selected if isinstance(selected, Mapping) else {}
        if (
            str(context.get("deterministic_status") or "").upper() != "REVIEW_CANDIDATE"
            or context.get("trend_core_eligible") is not True
            or context.get("top_rotation_theme") is not True
            or selected.get("selected_for_rotation") is not True
        ):
            continue
        direction = str(
            context.get("rotation_direction_id")
            or selected.get("board_code")
            or selected.get("theme_id")
            or ""
        ).strip()
        if direction:
            expected.add(direction)

    focused: set[str] = set()
    for item in output.get("focus_pool", ()):
        if not isinstance(item, Mapping):
            continue
        symbol = _first_symbol(item.get("symbol"))
        context = contexts.get(symbol, {})
        selected = context.get("selected_board")
        selected = selected if isinstance(selected, Mapping) else {}
        direction = str(
            context.get("rotation_direction_id")
            or selected.get("board_code")
            or selected.get("theme_id")
            or ""
        ).strip()
        if direction:
            focused.add(direction)

    explained: set[str] = set()
    reviews = output.get("rotation_reviews", [])
    if not isinstance(reviews, list):
        return ["A2_ROTATION_REVIEW_SCHEMA_INVALID"]
    errors: list[str] = []
    seen: set[str] = set()
    for review in reviews:
        if not isinstance(review, Mapping):
            errors.append("A2_ROTATION_REVIEW_SCHEMA_INVALID")
            continue
        direction = str(review.get("rotation_direction_id") or "").strip()
        representatives = review.get("reviewed_symbols")
        reasons = review.get("reason_codes")
        explanation = review.get("explanation")
        if (
            direction not in expected
            or direction in seen
            or direction in focused
            or review.get("decision") != "NO_FOCUS"
            or not isinstance(representatives, list)
            or not representatives
            or not isinstance(reasons, list)
            or not reasons
            or any(not isinstance(code, str) or not code.strip() for code in reasons)
            or not isinstance(explanation, str)
            or len(explanation.strip()) < 10
        ):
            invalid_fields = []
            if direction not in expected:
                invalid_fields.append("UNKNOWN_DIRECTION")
            if direction in seen:
                invalid_fields.append("DUPLICATE_DIRECTION")
            if direction in focused:
                invalid_fields.append("ALREADY_FOCUSED")
            if review.get("decision") != "NO_FOCUS":
                invalid_fields.append("DECISION_MUST_BE_NO_FOCUS")
            if not isinstance(representatives, list) or not representatives:
                invalid_fields.append("REPRESENTATIVES_REQUIRED")
            if not isinstance(reasons, list) or not reasons:
                invalid_fields.append("REASON_CODES_REQUIRED")
            if not isinstance(explanation, str) or len(explanation.strip()) < 10:
                invalid_fields.append("EXPLANATION_REQUIRED")
            errors.append("A2_ROTATION_REVIEW_INVALID:" + direction + ":" + ",".join(invalid_fields or ["REASON_CODE_INVALID"]))
            continue
        seen.add(direction)
        # A2 priority and A4 entry permission are separate. Optional emotion
        # factors cannot become a blanket rejection of a TREND direction.
        non_selection_reasons = {
            "MARKET_RISK_OFF_RETREAT", "MARKET_RISK_OFF", "RISK_OFF_RETREAT",
            "EMOTION_DIVERGENCE_NO_NEW_ENTRY", "EMOTION_DIVERGENCE",
            "NO_NEW_ENTRY", "NO_NEW_ENTRY_REGIME", "TIER_STRUCTURE_ZERO",
            "TIER_STRUCTURE_ABSENT", "ROTATION_RANK_4", "ROTATION_RANK_5",
        }
        if all(code.strip().upper() in non_selection_reasons for code in reasons):
            errors.append("A2_ROTATION_REVIEW_USES_ENTRY_GATE:" + direction)
            continue
        valid = True
        for representative in representatives:
            symbol = _first_symbol(representative)
            context = contexts.get(symbol, {})
            selected = context.get("selected_board") or {}
            bound = str(context.get("rotation_direction_id") or selected.get("board_code") or selected.get("theme_id") or "").strip()
            if symbol not in upstream_symbols or bound != direction:
                valid = False
                break
        if not valid:
            errors.append("A2_ROTATION_REVIEW_REPRESENTATIVE_INVALID:" + direction)
            continue
        explained.add(direction)
    return errors + [
        f"A2_ROTATION_FOCUS_COVERAGE_MISSING:{direction}"
        for direction in sorted(expected.difference(focused).difference(explained))
    ]


def _with_a2_bottleneck_context(
    snapshot: FrozenInputSnapshot,
    gate: DeterministicGateResult,
) -> FrozenInputSnapshot:
    """Attach the server-computed A2 scorecard inputs to the model view."""

    context: dict[str, dict[str, Any]] = {}
    for item in gate.decisions:
        symbol = str(item.get("symbol") or "")
        if not symbol or not isinstance(item.get("bottleneck_context"), Mapping):
            continue
        context[symbol] = {
            **dict(item.get("bottleneck_context") or {}),
            # These fields are emitted by the deterministic gate and are
            # authoritative for every later A2/A3 model response.
            "company_name": item.get("name"),
            "theme_id": item.get("theme_id"),
            "rotation_direction_id": item.get("rotation_direction_id"),
            "industry_chain_node": item.get("node_id"),
            "upstream_candidate_id": item.get("upstream_candidate_id"),
            "deterministic_status": item.get("status"),
            "independent_strategy_review": item.get("independent_strategy_review") is True,
            "research_route_qualifications": dict(item.get("research_route_qualifications") or {}),
            "strong_trend_observation": item.get("strong_trend_observation") is True,
            "research_observation_scope": item.get("research_observation_scope"),
            "deterministic_route": item.get("route"),
            "deterministic_score": item.get("score"),
            "theme_rotation_rank": item.get("theme_rotation_rank"),
            "theme_rotation_score": item.get("theme_rotation_score"),
            "rotation_strength_source": item.get("rotation_strength_source"),
            "top_rotation_theme": item.get("top_rotation_theme"),
            "rotation_reserve_eligible": item.get("rotation_reserve_eligible") is True,
            "rotation_reserve_scope": item.get("rotation_reserve_scope"),
            "rotation_reserve_boards": list(item.get("rotation_reserve_boards") or []),
            "a2_taxonomy_binding": dict(item.get("a2_taxonomy_binding") or {}),
            "a2_pool_channel": item.get("a2_pool_channel"),
            "a1_formal_member": item.get("a1_formal_member") is not False,
            "monthly_a1_member": item.get("monthly_a1_member"),
            "daily_verified_increment": dict(item.get("daily_verified_increment") or {}),
            "emotion_core_eligible": item.get("emotion_core_eligible") is True,
            "research_only_reason": item.get("research_only_reason"),
            "execution_permission": item.get("execution_permission"),
            "emotion_theme_binding": item.get("emotion_theme_binding"),
            "trend_core_eligible": item.get("trend_core_eligible") is True,
            "eastmoney_hot100": dict(item.get("eastmoney_hot100") or {}),
            "selected_board": dict(item.get("selected_board") or {}),
            "selected_board_binding": item.get("selected_board_binding"),
            "selected_board_theme_match": item.get("selected_board_theme_match") is True,
            "deterministic_market_role": item.get("role"),
            "stock_behavior_type": item.get("stock_behavior_type"),
            "route_permission": list(item.get("route_permission") or ()),
            "behavior_type_decision": dict(item.get("behavior_type_decision") or {}),
            "market_emotion_cycle": dict(item.get("market_emotion_cycle") or {}),
            "decision_id": item.get("decision_id"),
            "as_of": item.get("as_of"),
            "identifiability_score": item.get("identifiability_score"),
            "identifiability_breakdown": dict(item.get("role_breakdown") or {}),
            "role_breakdown": dict(item.get("role_breakdown") or {}),
            "a2_factor_scores": dict(item.get("a2_factor_scores") or {}),
            "factor_coverage": dict(item.get("factor_coverage") or {}),
            "critical_factor_coverage": dict(item.get("critical_factor_coverage") or {}),
            "data_sufficiency_state": item.get("data_sufficiency_state"),
            "missing_optional_factors": list(item.get("missing_optional_factors") or ()),
            "deterministic_reason_codes": list(item.get("reason_codes") or ()),
            "eligible_routes": list(item.get("eligible_routes") or ()),
            "preferred_route": item.get("route"),
            "route_eligibility": dict(item.get("route_eligibility") or {}),
            "gate_results": dict(item.get("gate_results") or {}),
            "first_blocking_gate": item.get("first_blocking_gate"),
            "all_failed_gates": list(item.get("all_failed_gates") or ()),
            "route_context_schema": "a2-route-lineage/2",
        }
    overlay_hash = _sha256_json({
        "base_snapshot_hash": snapshot.snapshot_hash,
        "stage": "A2_BOTTLENECK_CONTEXT",
        "context": context,
    })
    data = dict(snapshot.data)
    data["A2_BOTTLENECK_CONTEXT"] = context
    return FrozenInputSnapshot(
        snapshot_id=f"{snapshot.snapshot_id}:a2-bottleneck:{overlay_hash[:12]}",
        data=data,
        snapshot_hash=overlay_hash,
        as_of=snapshot.as_of,
    )


def _a2_optional_gap_only(views: Sequence[Mapping[str, Any]]) -> bool:
    """Return whether all explicit output gap markers are optional enrichment.

    This narrow exception preserves the existing contract that missing
    optional capital-flow data alone is not a critical A2 evidence failure.
    An unqualified DEGRADED/INSUFFICIENT marker is treated conservatively as a
    real gap because its cause cannot be established from the output.
    """

    reasons: set[str] = set()
    for view in views:
        reasons.update(_output_reason_codes(view, include_rejected=False))
    return bool(reasons) and reasons.issubset(_A2_OPTIONAL_EVIDENCE_GAP_REASONS)


def _a2_gate_optional_gap_only(
    gate: Any | None,
    summary: Mapping[str, Any],
) -> bool:
    """Recognize a gate DEGRADED state caused only by optional enrichment."""

    reasons = _gate_reason_codes(gate, include_hard_rejects=False)
    reasons.update(_output_reason_codes(summary))
    if reasons and not reasons.issubset(_A2_OPTIONAL_EVIDENCE_GAP_REASONS):
        return False
    missing = summary.get("optional_missing_factors")
    if isinstance(missing, Sequence) and not isinstance(missing, (str, bytes, bytearray)):
        names = {str(item).strip().lower() for item in missing if str(item).strip()}
        if names and names.issubset({"capital_flow", "capital-flow", "capitalflow"}):
            return True
    # An explicit optional-only reason set is sufficient.  With no reason at
    # all we cannot prove the degraded state is optional, so fail closed.
    return bool(reasons)


_MOVED_NAMES = ('_run_a2_batched', '_project_a2_theme_metrics', '_a2_prompt_rotation_codes', '_project_a2_bottleneck_context', '_a2_candidate_pool_max', '_canonicalize_a2_complete_partition', '_move_a2_hard_rejects_to_rejected', '_build_a2_theme_batches', '_merge_a2_outputs', '_a2_batch_is_splittable', '_project_a2_research_hypotheses', '_a2_rejection_has_hard_fact_veto', '_demote_a2_llm_rejects', '_canonicalize_a2_bottleneck_scorecards', '_a2_compact_theme_identity_reasons', '_expand_a2_compact_output', '_canonicalize_a2_contract_semantics', '_enrich_a2_decision_facts', '_a2_fact_value_present', '_a2_number_present', '_a2_fact_name_list', '_a2_unavailable_crowding_fact', '_a2_available_theme_weights', '_a2_relative_top5_market_core_exception', '_a2_bottleneck_reasons', '_a2_item_route', '_apply_a2_lineage_policy', '_annotate_a2_pool_target', '_a2_watch_row_research_eligible', '_a2_theme_reasons', '_compact_a2_upstream_candidate', '_a2_rotation_retry_feedback', '_validate_a2_rotation_focus_coverage', '_with_a2_bottleneck_context', '_a2_optional_gap_only', '_a2_gate_optional_gap_only')
_METHOD_BINDINGS = {'_run_a2_batched': 'ResearchPipeline._run_a2_batched'}
