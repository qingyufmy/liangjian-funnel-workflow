from __future__ import annotations
from .common import *

def _run_a3_batched(
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
    ordered = sorted(upstream_symbols)
    batches = [set(ordered[offset:offset + _A3_BATCH_SIZE]) for offset in range(0, len(ordered), _A3_BATCH_SIZE)]
    audits, valid_audits, _split_count, blocked, _total_batches = self._execute_batch_plan(
        batches=batches,
        lane_id=lane_id,
        model=model,
        stage="A3",
        run_id=run_id,
        snapshot_id=snapshot.snapshot_id,
        runner=lambda batch: self._run_stage_with_checkpoint(
            lane_id=lane_id,
            model=model,
            stage="A3",
            snapshot=snapshot,
            upstream_output=upstream_output,
            upstream_symbols=batch,
            bundle=bundle,
            run_id=run_id,
            projection_symbols=batch,
        ),
        # A3 transport failures are intentionally not split: the stage's
        # fixed technical projection is already bounded and its semantic
        # contract is fail-closed.
        splittable=lambda _reasons: False,
    )
    if blocked is not None:
        return StageAudit(
            lane=lane_id,
            model=model,
            stage="A3",
            status="BLOCKED",
            snapshot_id=snapshot.snapshot_id,
            prompt_hash=_combined_digest(item.prompt_hash for item in audits),
            input_hash=_combined_digest(item.input_hash for item in audits),
            output_hash=None,
            latency_ms=sum(item.latency_ms or 0 for item in audits),
            attempts=sum(item.attempts for item in audits),
            thinking_variant=_common_variant(audits),
            symbols=(),
            reason_codes=tuple(f"A3_BATCH_BLOCKED:{reason}" for reason in blocked.reason_codes),
            diagnostics={
                "batch_count": len(batches),
                "completed_batches": len(valid_audits),
                "blocked_batch_diagnostics": blocked.diagnostics,
            },
        )
    merged = _merge_stage_outputs(
        "A3",
        [audit.output for audit in valid_audits if isinstance(audit.output, Mapping)],
    )
    merged, canonicalized_lineage = _canonicalize_stage_lineage(
        merged,
        "A3",
        upstream_output,
        snapshot.data,
    )
    merged = _refresh_analysis_counts(merged, "A3")
    merged, _ = _apply_a3_pool_limits(merged, snapshot.data)
    merged, post_limit_lineage = _canonicalize_stage_lineage(
        merged,
        "A3",
        upstream_output,
        snapshot.data,
    )
    canonicalized_lineage += post_limit_lineage
    merged = _refresh_analysis_counts(merged, "A3")
    reasons = _validate_output(
        merged,
        stage="A3",
        model=model,
        snapshot_id=snapshot.snapshot_id,
        upstream_symbols=upstream_symbols,
        snapshot_data=snapshot.data,
    )
    return StageAudit(
        lane=lane_id,
        model=model,
        stage="A3",
        status="VALIDATED" if not reasons else "BLOCKED",
        snapshot_id=snapshot.snapshot_id,
        prompt_hash=_combined_digest(item.prompt_hash for item in audits),
        input_hash=_combined_digest(item.input_hash for item in audits),
        output_hash=_sha256_json(merged),
        latency_ms=sum(item.latency_ms or 0 for item in audits),
        attempts=sum(item.attempts for item in audits),
        thinking_variant=_common_variant(audits),
        symbols=tuple(sorted(_approved_symbols(merged, "A3"))),
        reason_codes=tuple(reasons),
        output=merged,
        diagnostics={
            "batch_count": len(batches),
            "completed_batches": len(valid_audits),
            "canonicalized_lineage": canonicalized_lineage,
            "pool_counts": _stage_pool_counts(merged, "A3"),
        },
    )


def _project_a3_deterministic_context(value: Any, symbols: set[str] | None) -> Any:
    """Send each route's independent verdict once; keep frozen evidence intact.

    The full strategy checks repeat the same daily facts, price contract, and
    market regime for every route. The model already receives the selected
    route's authoritative fields here and the daily factors/price levels in
    their own placeholders. This projection is model-only: validation and the
    attempt ledger still read the original full deterministic context.
    """

    scoped = _filter_symbol_mapping(value, symbols)
    if not isinstance(scoped, Mapping):
        return scoped
    result: dict[str, Any] = {}
    for symbol, raw in scoped.items():
        if not isinstance(raw, Mapping):
            result[str(symbol)] = raw
            continue
        row = {key: raw[key] for key in _A3_REVIEW_TOP_FIELDS if key in raw}
        checks = raw.get("strategy_checks")
        if isinstance(checks, Mapping):
            row["strategy_checks"] = {
                str(route): {
                    **{key: check[key] for key in _A3_ROUTE_REVIEW_FIELDS if key in check},
                    **({"gate_results": _project_a3_gate_results(check["gate_results"])}
                       if isinstance(check.get("gate_results"), Mapping) else {}),
                    **({"strategy_facts": {
                        key: facts[key] for key in _A3_REVIEW_FACT_FIELDS if key in facts
                    }} if isinstance(facts := check.get("strategy_facts"), Mapping) else {}),
                }
                if isinstance(check, Mapping) else check
                for route, check in checks.items()
            }
        facts = raw.get("strategy_facts")
        if isinstance(facts, Mapping):
            row["strategy_facts"] = {
                key: facts[key] for key in _A3_REVIEW_FACT_FIELDS if key in facts
            }
        result[str(symbol)] = row
    return result


def _project_a3_gate_results(value: Mapping[str, Any]) -> dict[str, Any]:
    """Compress repeated success envelopes, never hide failed gate evidence."""

    return {
        str(gate): True if isinstance(detail, Mapping)
        and detail.get("available") is True
        and detail.get("met") is True
        and detail.get("reason") == "OK"
        else detail
        for gate, detail in value.items()
    }


def _build_a3_candidate_domain(
    a2_output: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, str]]:
    """Build the server-owned A3 domain from every routeable A2 result.

    Focus and watch are A2 priority labels, not A3 technical gates.  Current
    deterministic-v2 rows therefore use one uniform contract: the behavior
    must already be EMOTION or TREND, the corresponding A4 strategy family
    must be permitted, and no immutable data/tradability veto may be present.
    Historical rows without the behavior contract retain the former
    focus/watch compatibility path so frozen replays remain readable.

    This is a domain expansion, not a quota: every eligible row is retained and
    A3 itself decides which of the three technical setups is actually ready.
    """

    focus_rows = a2_output.get("focus_pool")
    watch_rows = a2_output.get("watch_only_pool")
    focus_rows = focus_rows if isinstance(focus_rows, list) else []
    watch_rows = watch_rows if isinstance(watch_rows, list) else []

    selected: dict[str, dict[str, Any]] = {}
    origins: dict[str, str] = {}

    for raw in focus_rows:
        if not isinstance(raw, Mapping):
            continue
        symbols = _scan_symbols(raw.get("symbol"))
        if len(symbols) != 1:
            continue
        symbol = next(iter(symbols))
        if not _a3_candidate_eligible(raw, origin="FOCUS"):
            continue
        item = dict(raw)
        item["symbol"] = symbol
        item["candidate_origin"] = "FOCUS"
        selected[symbol] = item
        origins[symbol] = "FOCUS"

    for raw in watch_rows:
        if not isinstance(raw, Mapping):
            continue
        symbols = _scan_symbols(raw.get("symbol"))
        if len(symbols) != 1:
            continue
        symbol = next(iter(symbols))
        if symbol in selected:
            # A symbol in focus wins deterministically if a provider repeated
            # it in both A2 partitions.
            continue
        if not _a3_candidate_eligible(raw, origin="WATCH_ONLY"):
            continue
        item = dict(raw)
        item["symbol"] = symbol
        item["candidate_origin"] = "WATCH_ONLY"
        selected[symbol] = item
        origins[symbol] = "WATCH_ONLY"

    # The A3 candidate domain deliberately contains both A2 focus candidates
    # and server-qualified WATCH_ONLY candidates.  Preserve candidate_origin
    # on each row so neither the model nor operators can mistake this for the
    # narrower A2 focus pool.
    rows = [selected[symbol] for symbol in sorted(selected)]
    projected = dict(a2_output)
    projected["focus_pool"] = rows
    projected["watch_only_pool"] = []
    return projected, origins


def _a3_watch_only_candidate_eligible(item: Mapping[str, Any]) -> bool:
    """Backward-compatible public helper for the A2-watch to A3 contract."""

    return _a3_candidate_eligible(item, origin="WATCH_ONLY")


def _a3_candidate_eligible(item: Mapping[str, Any], *, origin: str) -> bool:
    behavior_value = str(
        item.get("stock_behavior_type")
        or item.get("behavior_type")
        or ""
    ).strip().upper()
    behavior_contract_present = bool(behavior_value)
    allowed_permissions = _A3_BEHAVIOR_ROUTE_PERMISSIONS.get(behavior_value)

    # A current A2 row is routeable only after A2 has resolved its execution
    # behavior.  UNRESOLVED/NONE rows are audit evidence, not A3 candidates.
    if behavior_contract_present and allowed_permissions is None:
        return False

    # The selected-board top-five requirement belongs only to the trend
    # channel.  Emotion rows are sourced independently from Eastmoney Hot100.
    if behavior_value != "EMOTION" and item.get("top_rotation_theme") is False and not _is_rotation_reserve(item):
        return False

    raw_permission = item.get("route_permission")
    permission_values = {
        str(value).strip().upper()
        for value in raw_permission
        if str(value).strip()
    } if isinstance(raw_permission, Sequence) and not isinstance(
        raw_permission, (str, bytes, bytearray)
    ) else set()
    if behavior_contract_present:
        # A present-but-empty permission list is an explicit A2 route veto.
        if not permission_values.intersection(allowed_permissions or frozenset()):
            return False

    role = str(
        item.get("market_role")
        or item.get("role")
        or item.get("a2_role")
        or item.get("deterministic_market_role")
        or ""
    ).strip().upper()
    if not behavior_contract_present and origin == "WATCH_ONLY" and role not in _A3_WATCH_ONLY_ROLES:
        return False

    # Only server-owned reasons may remove an A2 row from A3's deterministic
    # technical review domain.  Model-facing priority/identity wording is
    # advisory; otherwise A2 ranking language would become an undocumented A3
    # technical gate.  Explicit hard facts remain blocking.
    reason_values: list[str] = []
    reason_keys = ["deterministic_reason_codes", "hard_reason_codes"]
    if item.get("local_decision") is True:
        reason_keys.append("reason_codes")
    for key in reason_keys:
        values = item.get(key)
        if isinstance(values, Sequence) and not isinstance(values, (str, bytes, bytearray)):
            reason_values.extend(
                str(value).strip().upper()
                for value in values
                if isinstance(value, str) and str(value).strip()
            )
    if any(
        any(marker in reason for marker in _A3_WATCH_ONLY_HARD_REASON_MARKERS)
        for reason in reason_values
    ):
        return False

    sufficiency = str(item.get("data_sufficiency_state") or "").strip().upper()
    if sufficiency in {"INSUFFICIENT", "UNAVAILABLE", "MISSING", "DATA_GAP"}:
        return False

    # Existing A2 payloads use either trade_eligible or tradable.  Missing is
    # not treated as false here because the A3 technical gate will re-check the
    # immutable TRADABILITY_FLAGS contract before model review.
    for key in ("trade_eligible", "tradable", "is_tradable"):
        if key in item and item.get(key) is False:
            return False

    explicit_route = str(
        item.get("a2_route") or item.get("route") or item.get("selection_route") or ""
    ).strip().upper()
    eligible_routes = item.get("eligible_routes")
    route_values = {
        str(value).strip().upper()
        for value in eligible_routes
        if isinstance(value, str) and value.strip()
    } if isinstance(eligible_routes, Sequence) and not isinstance(eligible_routes, (str, bytes, bytearray)) else set()
    # Some A2 model rows carry a valid role but omit the route alias.  That is
    # not itself a hard data gap; A3 will still enforce its own deterministic
    # factor/price/tradability contract.  Reject only an explicit route veto
    # or an explicitly populated route list with no usable route.
    if explicit_route in {"NO_ROUTE", "NO_ROUTE_READY", "A2_NO_ROUTE_READY", "INVALID", "UNAVAILABLE"}:
        return False
    if route_values and not route_values.intersection({
        MARKET_CORE_ROUTE,
        SUPPLY_CHAIN_ALPHA_ROUTE,
    }):
        return False
    return True


def _apply_a3_candidate_origin_policy(
    output: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
) -> tuple[dict[str, Any], int]:
    """Preserve A2 provenance without overriding A3 technical qualification.

    ``WATCH_ONLY`` is an upstream A2 confidence label, not a permanent
    execution veto. When the server-owned A3 strategy context is QUALIFIED,
    the row may remain in the executable core pool, but its risk unit is
    conservatively capped at ``PROBE``. The model can still veto the row for
    an independently evidenced technical, event or tradability risk.
    """

    result = dict(output)
    raw_origins = snapshot_data.get("A3_CANDIDATE_ORIGIN")
    origins = {
        _first_symbol(symbol): str(origin).strip().upper()
        for symbol, origin in raw_origins.items()
        if _first_symbol(symbol) and str(origin).strip().upper() in {"FOCUS", "WATCH_ONLY"}
    } if isinstance(raw_origins, Mapping) else {}
    core = list(output.get("core_watch_pool")) if isinstance(output.get("core_watch_pool"), list) else None
    secondary = (
        list(output.get("secondary_watch_pool"))
        if isinstance(output.get("secondary_watch_pool"), list)
        else None
    )
    if core is None and secondary is None:
        return result, 0

    changed = 0
    contexts = snapshot_data.get("A2_BOTTLENECK_CONTEXT")
    contexts = contexts if isinstance(contexts, Mapping) else {}
    def normalize(raw_item: Any, pool: str) -> Any:
        nonlocal changed
        if not isinstance(raw_item, Mapping):
            return raw_item
        item = dict(raw_item)
        symbol = _first_symbol(item)
        if contexts.get(symbol, {}).get("rotation_reserve_eligible") is True:
            item["rotation_reserve_eligible"] = True
            item["rotation_reserve_scope"] = "RESEARCH_ONLY_NO_AUTOMATIC_ENTRY"
            item["rotation_reserve_boards"] = list(contexts[symbol].get("rotation_reserve_boards") or [])
        if contexts.get(symbol, {}).get("strong_trend_observation") is True:
            item["strong_trend_observation"] = True
            item["research_observation_scope"] = contexts[symbol].get("research_observation_scope", "RESEARCH_ONLY_NO_AUTOMATIC_ENTRY")
        from ..a2_role_logic import route_execution_permission
        permission = route_execution_permission(contexts.get(symbol, {}), str(item.get("strategy_profile") or ""))
        if permission is not None:
            item["execution_permission"] = permission
            item["research_only_reason"] = contexts[symbol].get("research_only_reason") if permission == "BLOCKED" else None
        origin = origins.get(symbol) or str(item.get("candidate_origin") or "FOCUS").strip().upper()
        # Unknown/malformed origins are not allowed to become a new routing
        # class.  Treat old responses without the additive field as FOCUS;
        # the deterministic candidate map remains authoritative in production.
        if origin not in {"FOCUS", "WATCH_ONLY"}:
            origin = "FOCUS"
        if item.get("candidate_origin") != origin:
            item["candidate_origin"] = origin
            changed += 1
        if origin == "WATCH_ONLY" and pool == "core_watch_pool":
            if item.get("risk_unit") != "PROBE":
                item["risk_unit"] = "PROBE"
                changed += 1
            existing = item.get("reason_codes") if isinstance(item.get("reason_codes"), list) else []
            codes = list(dict.fromkeys([*existing, "A3_WATCH_ONLY_TECHNICALLY_QUALIFIED_PROBE"]))
            if codes != existing:
                item["reason_codes"] = codes
                changed += 1
        elif origin == "WATCH_ONLY" and pool == "secondary_watch_pool":
            # A secondary row remains non-executable. The origin alone does
            # not explain why it is secondary; review_status/reason_codes must
            # carry an independent veto or data-gap reason.
            if item.get("risk_unit") != "NO_ENTRY" and item.get("risk_unit") != "PROBE":
                item["risk_unit"] = "PROBE"
                changed += 1
        if pool == "secondary_watch_pool" and (
            item.get("status") == "WATCH_ONLY" or item.get("execution_permission") == "BLOCKED"
        ) and item.get("risk_unit") != "NO_ENTRY":
            item["risk_unit"] = "NO_ENTRY"
            changed += 1
        return item

    if core is not None:
        result["core_watch_pool"] = [normalize(item, "core_watch_pool") for item in core]
    if secondary is not None:
        result["secondary_watch_pool"] = [normalize(item, "secondary_watch_pool") for item in secondary]
    # Research coverage is not automatic execution permission. Keep the
    # completed technical/model conclusions, but publish no reserve orders.
    reserve = [item for item in result.get("core_watch_pool", []) if isinstance(item, Mapping) and _is_rotation_reserve(item)]
    if reserve:
        result["core_watch_pool"] = [item for item in result["core_watch_pool"] if item not in reserve]
        for item in reserve:
            item["risk_unit"] = "NO_ENTRY"
            reason = "A3_STRONG_TREND_OBSERVATION_ONLY" if item.get("strong_trend_observation") else "A3_ROTATION_RESERVE_RESEARCH_ONLY"
            item["reason_codes"] = list(dict.fromkeys([*item.get("reason_codes", []), reason]))
        result["secondary_watch_pool"] = [*result.get("secondary_watch_pool", []), *reserve]
        changed += len(reserve)
    return result, changed


def _a3_origin_only_veto_reasons(
    output: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
) -> list[str]:
    """Reject model vetoes that merely repeat an A2 WATCH_ONLY label.

    A3 is the technical planning authority. A model may veto a deterministic
    QUALIFIED setup only with an independent risk/data reason; otherwise a
    bounded retry is required instead of silently reporting no setup.
    """

    raw_contexts = snapshot_data.get("A3_DETERMINISTIC_CONTEXT")
    contexts = raw_contexts if isinstance(raw_contexts, Mapping) else {}
    raw_origins = snapshot_data.get("A3_CANDIDATE_ORIGIN")
    origins = raw_origins if isinstance(raw_origins, Mapping) else {}
    origin_only_codes = {
        "A2_LLM_REJECT_DEMOTED_TO_WATCH",
        "A3_WATCH_ONLY_CORE_DEMOTED",
        "A3_WATCH_ONLY_NON_EXECUTABLE",
        "CANDIDATE_ORIGIN_WATCH_ONLY_NON_EXECUTABLE",
        "WATCH_ONLY",
    }
    reasons: list[str] = []
    for pool_name in ("secondary_watch_pool", "rejected_candidates"):
        raw_items = output.get(pool_name)
        if not isinstance(raw_items, list):
            continue
        for raw_item in raw_items:
            if not isinstance(raw_item, Mapping):
                continue
            symbol = _first_symbol(raw_item)
            context = contexts.get(symbol)
            context = context if isinstance(context, Mapping) else {}
            origin = str(origins.get(symbol) or raw_item.get("candidate_origin") or "").strip().upper()
            eligibility = str(context.get("eligibility") or raw_item.get("eligibility") or "").strip().upper()
            if origin != "WATCH_ONLY" or eligibility != "QUALIFIED":
                continue
            review_status = str(raw_item.get("review_status") or "VETO").strip().upper()
            if review_status not in {"VETO", "DATA_GAP"}:
                continue
            raw_codes = raw_item.get("reason_codes")
            codes = {
                str(code).strip().upper()
                for code in raw_codes
                if isinstance(code, str) and str(code).strip()
            } if isinstance(raw_codes, list) else set()
            independent = {
                code
                for code in codes
                if code not in origin_only_codes and "WATCH_ONLY" not in code
            }
            if not independent:
                reasons.append("A3_ORIGIN_ONLY_VETO_CONTRADICTS_TECHNICAL_QUALIFICATION")
    return list(dict.fromkeys(reasons))


def _a3_prior_market_only_veto_reasons(
    output: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
) -> list[str]:
    """Reject A3 vetoes based only on prior-day market/pool switches.

    A3 owns closed-daily technical qualification.  The previous session's
    regime controls position guidance and priority, while A4 owns the next
    session's live new-entry permission.  A veto-only model therefore cannot
    demote a server-qualified plan solely because an obsolete regime override
    says the model pool is zero or yesterday was risk-off.
    """

    raw_contexts = snapshot_data.get("A3_DETERMINISTIC_CONTEXT")
    contexts = raw_contexts if isinstance(raw_contexts, Mapping) else {}
    context_only_codes = {
        "POOL_CAPACITY_LIMIT",
        "POOL_CAPACITY_FULL",
        "SYSTEM_CORE_WATCH_MAX_ZERO",
        "AGENT3_GENERATE_BUY_PLANS_FALSE",
        "MARKET_RISK_OFF",
        "RISK_OFF_RETREAT",
        "NO_NEW_ENTRY",
        "PRIOR_MARKET_NO_NEW_ENTRY",
        "PRIOR_DAY_NO_NEW_ENTRY",
    }
    reasons: list[str] = []
    for pool_name in ("secondary_watch_pool", "rejected_candidates"):
        raw_items = output.get(pool_name)
        if not isinstance(raw_items, list):
            continue
        for raw_item in raw_items:
            if not isinstance(raw_item, Mapping):
                continue
            symbol = _first_symbol(raw_item)
            context = contexts.get(symbol)
            context = context if isinstance(context, Mapping) else {}
            eligibility = str(
                context.get("eligibility") or raw_item.get("eligibility") or ""
            ).strip().upper()
            if eligibility != "QUALIFIED":
                continue
            review_status = str(raw_item.get("review_status") or "VETO").strip().upper()
            if review_status not in {"VETO", "DATA_GAP"}:
                continue
            raw_codes = raw_item.get("reason_codes")
            codes = {
                str(code).strip().upper()
                for code in raw_codes
                if isinstance(code, str) and str(code).strip()
            } if isinstance(raw_codes, list) else set()
            independent = {
                code
                for code in codes
                if code not in context_only_codes
                and not code.startswith("SYSTEM_CORE_WATCH_MAX_")
                and not code.startswith("AGENT3_GENERATE_BUY_PLANS_")
            }
            if codes and not independent:
                reasons.append("A3_PRIOR_MARKET_ONLY_VETO_CONTRADICTS_TECHNICAL_AUTHORITY")
    return list(dict.fromkeys(reasons))


def _a3_semantic_price_reasons(
    output: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
) -> list[str]:
    """Reject model demotions that confuse A3 reference geometry with A4.

    Price levels are canonicalized after this check, so harmless decimal
    rounding remains accepted.  A qualified daily route may carry a weak
    reference-close reward/risk or stop observation, but that observation is
    deferred to A4's actual confirmation price and cannot by itself move the
    row out of A3 core.
    """

    raw_levels = snapshot_data.get("PRICE_LEVELS")
    if not isinstance(raw_levels, Mapping):
        return []
    minimum_reward_risk = _safe_float(snapshot_data.get("MIN_REWARD_RISK", 2.0))
    maximum_stop = _safe_float(snapshot_data.get("MAX_STOP_DISTANCE", 0.06))
    raw_contexts = snapshot_data.get("A3_DETERMINISTIC_CONTEXT")
    contexts = raw_contexts if isinstance(raw_contexts, Mapping) else {}
    reasons: list[str] = []
    for pool_name in ("core_watch_pool", "secondary_watch_pool", "rejected_candidates"):
        raw_items = output.get(pool_name)
        if not isinstance(raw_items, list):
            continue
        for raw_item in raw_items:
            if not isinstance(raw_item, Mapping):
                continue
            symbol = _first_symbol(raw_item)
            expected = raw_levels.get(symbol)
            if not symbol or not isinstance(expected, Mapping) or expected.get("available") is not True:
                continue
            codes = raw_item.get("reason_codes")
            codes = {
                str(value).strip().upper()
                for value in codes
                if isinstance(value, str) and value.strip()
            } if isinstance(codes, Sequence) and not isinstance(codes, (str, bytes, bytearray)) else set()
            expected_reward_risk = _safe_float(expected.get("reward_risk"))
            expected_stop = _safe_float(expected.get("stop_distance_pct"))
            reward_veto = any(
                "REWARD_RISK" in code
                and any(marker in code for marker in ("BELOW", "OUTSIDE", "MINIMUM", "INSUFFICIENT"))
                for code in codes
            )
            stop_veto = any(
                "STOP_DISTANCE" in code
                and any(marker in code for marker in ("OUTSIDE", "ABOVE", "LIMIT", "MAXIMUM"))
                for code in codes
            )
            context = contexts.get(symbol)
            context = context if isinstance(context, Mapping) else {}
            qualified_daily_route = (
                str(context.get("eligibility") or "").upper() == "QUALIFIED"
                and str(context.get("strategy_profile") or "").upper()
                in {"LEADER_INTRADAY", "MA520_SWING", "TREND_MA5"}
                and str(context.get("route_permission") or "ALLOW_A4").upper()
                == "ALLOW_A4"
            )
            model_demoted = (
                pool_name != "core_watch_pool"
                or str(raw_item.get("review_status") or "").upper()
                in {"VETO", "DATA_GAP"}
                or str(raw_item.get("risk_unit") or "").upper() == "NO_ENTRY"
            )
            if qualified_daily_route and model_demoted and (reward_veto or stop_veto):
                reasons.append("A3_LIVE_GEOMETRY_PREMATURE_VETO")
                continue
            if reward_veto and expected_reward_risk >= minimum_reward_risk:
                reasons.append("A3_REWARD_RISK_REJECTION_CONTRADICTS_FROZEN_FACTS")
            if stop_veto and 0 < expected_stop <= maximum_stop:
                reasons.append("A3_STOP_REJECTION_CONTRADICTS_FROZEN_FACTS")
    return list(dict.fromkeys(reasons))


def _a3_secondary_probe_contract_reasons(
    output: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
) -> list[str]:
    """A3 secondary rows are intentionally non-executable in v3.

    The model no longer repairs or supplies a weighted-score contract.  The
    threshold policy below forces every secondary row to ``NO_ENTRY``.
    """

    del output, snapshot_data
    return []


def _apply_a3_pool_limits(
    output: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
) -> tuple[dict[str, Any], int]:
    del snapshot_data
    # Pool sizes remain observable/advisory.  A valid deterministic strategy
    # plan must never disappear because an arbitrary core/watch capacity was
    # reached; execution risk is handled later by the simulator/governor.
    return dict(output), 0


def _validate_a3_provenance(output: Mapping[str, Any], snapshot_data: Mapping[str, Any]) -> list[str]:
    raw_levels = snapshot_data.get("PRICE_LEVELS")
    # Lightweight/offline callers may not provide computed price levels.  In
    # the production workflow the key is always present; only validate exact
    # A3 lineage when that deterministic evidence set was supplied.
    if not isinstance(raw_levels, Mapping):
        return []
    levels = raw_levels
    raw_contexts = snapshot_data.get("A3_DETERMINISTIC_CONTEXT")
    contexts = raw_contexts if isinstance(raw_contexts, Mapping) else {}
    reasons: list[str] = []
    for pool_name in ("core_watch_pool", "secondary_watch_pool"):
        pool = output.get(pool_name)
        if not isinstance(pool, list) or not pool:
            continue
        for item in pool:
            if not isinstance(item, Mapping):
                continue
            risk_unit = str(item.get("risk_unit") or "").upper()
            # Secondary shadow/NO_ENTRY rows are intentionally non-executable;
            # they remain auditable without pretending to have a frozen plan.
            # A secondary PROBE, however, has exactly the same evidence and
            # numeric provenance obligations as a core plan.
            if pool_name == "secondary_watch_pool" and risk_unit != "PROBE":
                continue
            scanned = _scan_symbols(item.get("symbol"))
            if len(scanned) != 1:
                continue
            symbol = next(iter(scanned))
            strategy_context = contexts.get(symbol)
            strategy_context = strategy_context if isinstance(strategy_context, Mapping) else {}
            if isinstance(raw_contexts, Mapping) and not strategy_context:
                reasons.append("A3_STRATEGY_CONTEXT_MISSING")
            elif strategy_context:
                if str(item.get("strategy_profile") or "").upper() != str(
                    strategy_context.get("strategy_profile") or ""
                ).upper():
                    reasons.append("A3_STRATEGY_PROFILE_PROVENANCE_MISMATCH")
                if str(item.get("stock_behavior_type") or "").upper() != str(
                    strategy_context.get("stock_behavior_type") or ""
                ).upper():
                    reasons.append("A3_BEHAVIOR_TYPE_PROVENANCE_MISMATCH")
                if str(item.get("route_permission") or "").upper() != str(
                    strategy_context.get("route_permission") or ""
                ).upper():
                    reasons.append("A3_ROUTE_PERMISSION_PROVENANCE_MISMATCH")
                if pool_name == "core_watch_pool" and str(
                    strategy_context.get("eligibility") or ""
                ).upper() != "QUALIFIED":
                    reasons.append("A3_CORE_ELIGIBILITY_PROVENANCE_MISMATCH")
            expected = levels.get(symbol)
            if not isinstance(expected, Mapping) or expected.get("available") is not True:
                reasons.append("A3_PRICE_LEVELS_UNAVAILABLE")
                continue
            if risk_unit not in {"PROBE", "STANDARD", "NO_ENTRY"}:
                reasons.append("A3_RISK_UNIT_INVALID")
            if (
                pool_name == "secondary_watch_pool"
                and risk_unit == "PROBE"
                and snapshot_data.get("STRICT_AGENT_RULES") is True
            ):
                reasons.append("A3_SECONDARY_PROBE_NOT_ALLOWED")
            actual_zone = item.get("trigger_zone")
            expected_zone = expected.get("trigger_zone")
            if not isinstance(actual_zone, Mapping) or not isinstance(expected_zone, Mapping):
                reasons.append("A3_TRIGGER_ZONE_PROVENANCE_MISMATCH")
            else:
                for key in ("low", "high"):
                    if not _same_number(actual_zone.get(key), expected_zone.get(key)):
                        reasons.append("A3_TRIGGER_ZONE_PROVENANCE_MISMATCH")
                        break
            for actual_key, expected_key, reason in (
                ("invalidation_level", "invalidation", "A3_INVALIDATION_PROVENANCE_MISMATCH"),
                ("stop_distance_pct", "stop_distance_pct", "A3_STOP_DISTANCE_PROVENANCE_MISMATCH"),
                ("reward_risk", "reward_risk", "A3_REWARD_RISK_PROVENANCE_MISMATCH"),
            ):
                if not _same_number(item.get(actual_key), expected.get(expected_key)):
                    reasons.append(reason)
            # Price-discovery trends legitimately have no historic overhead
            # resistance.  In every other case the frozen value must match.
            if not (
                expected.get("price_discovery") is True
                and item.get("first_resistance") is None
                and expected.get("first_resistance") is None
            ) and not _same_number(item.get("first_resistance"), expected.get("first_resistance")):
                reasons.append("A3_RESISTANCE_PROVENANCE_MISMATCH")
            if snapshot_data.get("STRICT_AGENT_RULES") is True:
                reasons.extend(_a3_factor_contract_reasons(symbol, snapshot_data))
                scenarios = item.get("scenarios")
                required_scenarios = {
                    "normal_open_plan", "weak_open_plan", "high_gap_no_chase_plan", "invalidation_plan"
                }
                if not isinstance(scenarios, Mapping) or not required_scenarios.issubset(str(key) for key in scenarios):
                    reasons.append("A3_SCENARIO_SET_INCOMPLETE")
    return reasons


def _a3_factor_contract_reasons(symbol: str, snapshot_data: Mapping[str, Any]) -> list[str]:
    factors = snapshot_data.get("FACTOR_SNAPSHOT")
    factor = factors.get(symbol) if isinstance(factors, Mapping) else None
    return a3_factor_contract_reasons(factor)


def _canonicalize_a3_price_fields(
    output: Mapping[str, Any],
    snapshot_data: Mapping[str, Any],
) -> tuple[dict[str, Any], int, int]:
    """Replace model-rounded A3 prices with the frozen deterministic values."""

    result = dict(output)
    levels = snapshot_data.get("PRICE_LEVELS")
    raw_contexts = snapshot_data.get("A3_DETERMINISTIC_CONTEXT")
    contexts = raw_contexts if isinstance(raw_contexts, Mapping) else {}
    if not isinstance(levels, Mapping):
        return result, 0, 0
    count = 0
    trend_veto_count = 0
    for pool_name in ("core_watch_pool", "secondary_watch_pool"):
        pool = output.get(pool_name)
        if not isinstance(pool, list):
            continue
        canonical_pool: list[Any] = []
        for raw_item in pool:
            if not isinstance(raw_item, Mapping):
                canonical_pool.append(raw_item)
                continue
            item = dict(raw_item)
            symbols = _scan_symbols(item.get("symbol"))
            symbol = next(iter(symbols)) if len(symbols) == 1 else ""
            expected = levels.get(symbol)
            strategy_context = contexts.get(symbol)
            strategy_context = strategy_context if isinstance(strategy_context, Mapping) else {}
            if not isinstance(expected, Mapping) or expected.get("available") is not True:
                canonical_pool.append(item)
                continue
            replacements = {
                "trigger_zone": expected.get("trigger_zone"),
                "invalidation_level": expected.get("invalidation"),
                "stop_distance_pct": expected.get("stop_distance_pct"),
                "first_resistance": expected.get("first_resistance"),
                "reward_risk": expected.get("reward_risk"),
            }
            if strategy_context.get("scenario_contract_version") == "a3-scenarios/1":
                replacements["scenarios"] = _a3_contract_scenarios(strategy_context)
            for key in (
                "strategy_profile",
                "strategy_version",
                "eligibility",
                "candidate_origin",
                "market_role",
                "stock_behavior_type",
                "route_permission",
                "expected_holding_sessions",
                "time_stop_sessions",
                "setup_pattern",
                "cycle_alignment",
                "emotion_cycle_stage",
                "market_environment",
                "behavior_risk",
                "market_funding_state",
                "decision_id",
                "as_of",
                "market_regime",
                "theme_stage",
                "monthly_state",
                "monthly_partial_observation",
                "weekly_closed_state",
                "weekly_partial_observation",
                "daily_state",
                "daily_ma",
                "daily_macd",
                "daily_volume_state",
                "relative_strength",
                "entry_reference_zone",
                "no_chase_price",
                "price_discovery",
                "daily_invalidation",
                "plan_premises",
                "a4_deferred_conditions",
                "a4_required_entry_rules",
                "a4_exit_rules",
                "scenarios",
                "plan_mode",
                "plan_priority",
                "priority_reasons",
                "reference_price",
                "reference_price_as_of",
                "pressure_reduce_price",
                "pressure_basis",
                "plan_expiry",
                "required_conditions",
                "met_conditions",
                "unmet_conditions",
                "veto_conditions",
                "gate_results",
                "first_blocking_gate",
                "all_failed_gates",
                "publication_state",
                "A3_ABLATION_MODE",
                "a3_ablation_mode",
                "ablation_gates",
                "ablation_shadow_eligibility",
                "strategy_facts",
            ):
                if key in strategy_context:
                    replacements[key] = strategy_context.get(key)
            if strategy_context.get("entry_reference_zone") is not None:
                replacements["trigger_zone"] = strategy_context.get("entry_reference_zone")
            if strategy_context.get("daily_invalidation") is not None:
                replacements["invalidation_level"] = strategy_context.get("daily_invalidation")
            if strategy_context.get("strategy_profile") is not None:
                for key in ("research_state", "strategy_checks", "execution_permission", "research_only_reason"):
                    if key in strategy_context:
                        replacements[key] = strategy_context[key]
                replacements["setup_type"] = strategy_context.get("strategy_profile")
            if strategy_context.get("a4_required_entry_rules") is not None:
                replacements["confirmation_conditions"] = strategy_context.get(
                    "a4_required_entry_rules"
                )
            factor_snapshot = snapshot_data.get("FACTOR_SNAPSHOT")
            factor = factor_snapshot.get(symbol) if isinstance(factor_snapshot, Mapping) else None
            if isinstance(factor, Mapping):
                replacements["ma_analysis"] = _canonical_ma_analysis(factor)
                replacements["factor_snapshot_hash"] = _sha256_json(factor)
            for key, value in replacements.items():
                item[key] = value
            # Publication receives the immutable base snapshot, while these
            # stage-specific technical levels are computed by the A3 overlay.
            # Persist a server-generated content hash with the canonical
            # fields so the final persistence boundary can verify that an
            # executable secondary PROBE passed this code path without having
            # to retain the complete per-stage snapshot in memory.
            item["server_price_levels_hash"] = _sha256_json({
                "trigger_zone": replacements["trigger_zone"],
                "invalidation_level": replacements["invalidation_level"],
                "stop_distance_pct": replacements["stop_distance_pct"],
                "first_resistance": replacements["first_resistance"],
                "reward_risk": replacements["reward_risk"],
            })
            if _major_trend_repair_required(symbol, snapshot_data):
                item["risk_unit"] = "NO_ENTRY"
                codes = item.get("reason_codes") if isinstance(item.get("reason_codes"), list) else []
                item["reason_codes"] = list(dict.fromkeys([*codes, "MAJOR_TREND_REPAIR_REQUIRED"]))
                scenarios = item.get("scenarios")
                if isinstance(scenarios, Mapping):
                    suspended: dict[str, Any] = {}
                    for name, raw_scenario in scenarios.items():
                        if not isinstance(raw_scenario, Mapping) or name == "invalidation_plan":
                            suspended[str(name)] = raw_scenario
                            continue
                        scenario = dict(raw_scenario)
                        scenario["action"] = "NO_ENTRY"
                        if "risk_unit" in scenario:
                            scenario["risk_unit"] = "NO_ENTRY"
                        suspended[str(name)] = scenario
                    item["scenarios"] = suspended
                trend_veto_count += 1
            canonical_pool.append(item)
            count += 1
        result[pool_name] = canonical_pool
    return result, count, trend_veto_count


def _with_a3_candidate_context(
    snapshot: FrozenInputSnapshot,
    origins: Mapping[str, str],
) -> FrozenInputSnapshot:
    """Attach the immutable A3 candidate-origin map to a stage snapshot."""

    normalized = {
        str(symbol): str(origin).upper()
        for symbol, origin in origins.items()
        if str(symbol) and str(origin).upper() in {"FOCUS", "WATCH_ONLY"}
    }
    overlay_hash = _sha256_json({
        "base_snapshot_hash": snapshot.snapshot_hash,
        "stage": "A3_CANDIDATE_CONTEXT",
        "origins": normalized,
    })
    data = dict(snapshot.data)
    data["A3_CANDIDATE_ORIGIN"] = dict(sorted(normalized.items()))
    data["A3_CANDIDATE_SCOPE"] = sorted(normalized)
    return FrozenInputSnapshot(
        snapshot_id=f"{snapshot.snapshot_id}:a3-candidates:{overlay_hash[:12]}",
        data=data,
        snapshot_hash=overlay_hash,
        as_of=snapshot.as_of,
    )


def _a3_contract_scenarios(decision: Mapping[str, Any]) -> dict[str, Any]:
    """Display the existing execution contract, never invent another trigger."""
    base = {
        "source": "DETERMINISTIC_A3_CONTRACT",
        "schema_version": "a3-scenarios/1",
        "strategy_profile": decision.get("strategy_profile"),
        "entry_reference_zone": decision.get("entry_reference_zone"),
        "no_chase_price": decision.get("no_chase_price"),
        "invalidation_level": decision.get("daily_invalidation"),
        "required_entry_rules": decision.get("a4_required_entry_rules") or [],
        "risk_unit": decision.get("plan_mode"),
    }
    return {
        "normal_open_plan": {**base, "action": "WAIT", "description": "等待A4按冻结策略确认，不因正常开盘自动买入。"},
        "weak_open_plan": {**base, "action": "WAIT", "description": "等待盘中结构恢复并通过原策略确认，不左侧抄底；触及失效位取消计划。"},
        "high_gap_no_chase_plan": {**base, "action": "NO_ENTRY", "description": "超过禁止追价上限不得追买；回归允许范围后仍须通过原策略确认。"},
        "invalidation_plan": {**base, "action": "CANCEL_PLAN", "risk_unit": "NO_ENTRY", "description": "入场前触及失效位取消计划；已有持仓的退出由A4及T+1可卖数量规则处理。"},
    }


def _with_a3_deterministic_context(
    snapshot: FrozenInputSnapshot,
    gate: DeterministicGateResult,
) -> FrozenInputSnapshot:
    """Persist A3's server-owned strategy and condition evidence."""

    keys = (
        "symbol",
        "status",
        "strategy_profile",
        "strategy_version",
        "eligibility",
        "candidate_origin",
        "market_role",
        "stock_behavior_type",
        "route_permission",
        "expected_holding_sessions",
        "time_stop_sessions",
        "setup_pattern",
        "cycle_alignment",
        "emotion_cycle_stage",
        "market_environment",
        "behavior_risk",
        "market_funding_state",
        "decision_id",
        "as_of",
        "market_regime",
        "theme_stage",
        "monthly_state",
        "monthly_partial_observation",
        "weekly_closed_state",
        "weekly_partial_observation",
        "daily_state",
        "daily_ma",
        "daily_macd",
        "daily_volume_state",
        "relative_strength",
        "entry_reference_zone",
        "no_chase_price",
        "price_discovery",
        "daily_invalidation",
        "plan_premises",
        "a4_deferred_conditions",
        "a4_required_entry_rules",
        "a4_exit_rules",
        "plan_mode",
        "plan_priority",
        "priority_reasons",
        "reference_price",
        "reference_price_as_of",
        "pressure_reduce_price",
        "pressure_basis",
        "plan_expiry",
        "required_conditions",
        "met_conditions",
        "unmet_conditions",
        "veto_conditions",
        "gate_results",
        "first_blocking_gate",
        "all_failed_gates",
        "publication_state",
        "research_state",
        "strategy_checks",
        "execution_permission",
        "research_only_reason",
        "A3_ABLATION_MODE",
        "a3_ablation_mode",
        "ablation_gates",
        "ablation_shadow_eligibility",
        "strategy_facts",
        "reward_risk",
        "stop_distance_pct",
        "minimum_reward_risk",
        "maximum_stop_distance_pct",
        "price_levels_hash",
        "factor_snapshot_hash",
        "reason_codes",
    )
    context = {
        str(item.get("symbol")): {
            **{key: item.get(key) for key in keys},
            # Inputs already include every source value. Do not repeat four
            # derived copies in the model prompt; materialize after review.
            "scenario_contract_version": "a3-scenarios/1",
        }
        for item in gate.decisions
        if str(item.get("symbol") or "")
    }
    overlay_hash = _sha256_json({
        "base_snapshot_hash": snapshot.snapshot_hash,
        "stage": "A3_DETERMINISTIC_CONTEXT",
        "context": context,
    })
    data = dict(snapshot.data)
    raw_factors = data.get("FACTOR_SNAPSHOT")
    if isinstance(raw_factors, Mapping):
        bounded_factors: dict[str, Any] = {}
        for symbol, raw_factor in raw_factors.items():
            if not isinstance(raw_factor, Mapping):
                continue
            factor = dict(raw_factor)
            frames = raw_factor.get("timeframes")
            if isinstance(frames, Mapping):
                factor["timeframes"] = {
                    name: value
                    for name, value in frames.items()
                    if str(name) in {"monthly", "weekly", "daily"}
                }
            summary = raw_factor.get("technical_summary")
            if isinstance(summary, Mapping):
                summary_copy = dict(summary)
                summary_frames = summary.get("timeframes")
                if isinstance(summary_frames, Mapping):
                    summary_copy["timeframes"] = {
                        name: value
                        for name, value in summary_frames.items()
                        if str(name) in {"monthly", "weekly", "daily"}
                    }
                factor["technical_summary"] = summary_copy
            bounded_factors[str(symbol)] = factor
        data["FACTOR_SNAPSHOT"] = bounded_factors
    data["A3_DETERMINISTIC_CONTEXT"] = context
    return FrozenInputSnapshot(
        snapshot_id=f"{snapshot.snapshot_id}:a3-gate:{overlay_hash[:12]}",
        data=data,
        snapshot_hash=overlay_hash,
        as_of=snapshot.as_of,
    )


def _a3_decision_evidence_gaps(
    output: Mapping[str, Any],
    gate: Any | None,
) -> set[str]:
    """Return per-symbol decision gaps that invalidate a pure no-setup claim."""

    reasons: set[str] = set()
    rows: list[Mapping[str, Any]] = []
    for pool in ("secondary_watch_pool", "rejected_candidates"):
        value = output.get(pool)
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            rows.extend(item for item in value if isinstance(item, Mapping))
    decisions = getattr(gate, "decisions", ()) if gate is not None else ()
    rows.extend(item for item in decisions if isinstance(item, Mapping))
    for row in rows:
        eligibility = str(
            row.get("deterministic_eligibility")
            or row.get("eligibility")
            or row.get("status")
            or row.get("local_partition")
            or ""
        ).strip().upper()
        if eligibility not in {"DATA_GAP", "EVIDENCE_PENDING"}:
            continue
        raw = row.get("reason_codes") or row.get("deterministic_reason_codes") or ()
        if isinstance(raw, str):
            raw = [raw]
        if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
            reasons.update(
                str(item).strip().upper()
                for item in raw
                if str(item).strip().upper() in _A3_DECISION_EVIDENCE_GAP_REASONS
            )
        if not reasons:
            reasons.add("A3_DECISION_EVIDENCE_GAP")
    return reasons


_MOVED_NAMES = ('_run_a3_batched', '_project_a3_deterministic_context', '_project_a3_gate_results', '_build_a3_candidate_domain', '_a3_watch_only_candidate_eligible', '_a3_candidate_eligible', '_apply_a3_candidate_origin_policy', '_a3_origin_only_veto_reasons', '_a3_prior_market_only_veto_reasons', '_a3_semantic_price_reasons', '_a3_secondary_probe_contract_reasons', '_apply_a3_pool_limits', '_validate_a3_provenance', '_a3_factor_contract_reasons', '_canonicalize_a3_price_fields', '_with_a3_candidate_context', '_a3_contract_scenarios', '_with_a3_deterministic_context', '_a3_decision_evidence_gaps')
_METHOD_BINDINGS = {'_run_a3_batched': 'ResearchPipeline._run_a3_batched'}
