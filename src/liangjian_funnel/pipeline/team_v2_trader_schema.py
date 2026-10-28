"""Claude 3.0.2 A4 schema; invalid/unknown is never an executable signal."""
from collections.abc import Mapping
from copy import deepcopy
import json
import re

from .team_v2_contract import TeamValidation, validate_team_output

_ROOT = frozenset("envelope schema_version decision_id minute_snapshot_id signals position_actions handoff_notes what_would_change_my_mind".split())
_SIGNAL = frozenset("plan_id symbol action reason_code supporting_refs opposing_refs tech_reference_review what_would_change_my_mind handoff_to_a6".split())
_POSITION = frozenset("symbol action reason_code supporting_refs opposing_refs distance_to_stop_note".split())
_ACTIONS = frozenset("NO_ACTION START_CONFIRMATION BUY_SIGNAL ADD_SIGNAL SELL_SIGNAL REDUCE_SIGNAL CANCEL_SIGNAL BLOCK".split())
_ORDERS = frozenset("BUY_SIGNAL ADD_SIGNAL SELL_SIGNAL REDUCE_SIGNAL".split())


def _text(value):
    return isinstance(value, str) and bool(value.strip()) and len(value) <= 2000


def _texts(value):
    return isinstance(value, list) and 1 <= len(value) <= 2 and all(_text(x) for x in value)


def _same(left, right):
    try:
        return json.dumps(left, sort_keys=True, allow_nan=False) == json.dumps(right, sort_keys=True, allow_nan=False)
    except (TypeError, ValueError):
        return False


def project_trader_references(output, tech_reference):
    """Keep the raw vote immutable; server-owned facts are a separate projection.

    Only a known item in its own plan can receive its authoritative status.
    0995 permits dropping well-formed unknown items with their full original
    explanation in the audit. Required items, duplicates, actions and identity
    still pass through the strict validator unchanged.
    """
    projected = deepcopy(output)
    changes = []
    if not isinstance(projected, Mapping) or not isinstance(tech_reference, Mapping):
        return projected, changes
    signals = projected.get('signals')
    if not isinstance(signals, list):
        return projected, changes
    for index, row in enumerate(signals):
        if not isinstance(row, Mapping):
            continue
        pid = row.get('plan_id')
        if not isinstance(pid, str) or not isinstance(tech_reference.get(pid), Mapping):
            continue
        reviews = row.get('tech_reference_review')
        if not isinstance(reviews, list):
            continue
        retained = []
        for ordinal, ref in enumerate(reviews):
            if (isinstance(ref, dict) and set(ref) in (
                    {'item', 'my_read'}, {'item', 'status', 'my_read'})
                    and isinstance(ref.get('item'), str) and _text(ref['item'])
                    and ref['item'] not in tech_reference[pid] and _text(ref.get('my_read'))
                    and ('status' not in ref or (isinstance(ref['status'],str) and ref['status'] in {'MET','UNMET'}))):
                changes.append(dict(code='UNKNOWN_REFERENCE_ITEM_DROPPED',
                    path=f'signals.{index}.tech_reference_review.{ordinal}',
                    item=ref['item'],original=deepcopy(ref),my_read=ref['my_read']))
                continue
            retained.append(ref)
            if (not isinstance(ref, dict) or set(ref) not in (
                    {'item', 'my_read'}, {'item', 'status', 'my_read'})
                    or not isinstance(ref.get('item'), str) or ref['item'] not in tech_reference[pid]
                    or ('status' in ref and (not isinstance(ref['status'],str) or ref['status'] not in {'MET','UNMET'}))):
                continue
            status = tech_reference[pid][ref['item']]
            if status not in {'MET', 'UNMET'}:
                continue
            if ref.get('status') != status:
                changes.append(dict(path=f'signals.{index}.tech_reference_review.{ordinal}.status',
                    item=ref['item'], original_present='status' in ref,
                    original=ref.get('status'), authoritative=status))
            ref['status'] = status
        row['tech_reference_review'] = retained
    return projected, changes


def validate_trader_output(output, catalog, *, required_envelope, decision_id, minute_snapshot_id,
                           plans, held_symbols, tech_reference,candidate_actions=None,position_candidate_actions=None):
    if candidate_actions is not None and (set(candidate_actions)!=set(plans)
            or any(value not in {'BUY_SIGNAL','ADD_SIGNAL'} for value in candidate_actions.values())):
        raise ValueError('TRADER_REVIEW_CANDIDATE_CONTEXT_INVALID')
    if position_candidate_actions is not None and (set(position_candidate_actions)!=set(held_symbols)
            or any(value not in {'SELL_SIGNAL','REDUCE_SIGNAL'} for value in position_candidate_actions.values())):
        raise ValueError('TRADER_REVIEW_POSITION_CONTEXT_INVALID')
    if (not isinstance(plans, Mapping) or any(not _text(k) or not _text(v) for k, v in plans.items())
            or not isinstance(held_symbols, (set, frozenset)) or any(not _text(x) for x in held_symbols)
            or not isinstance(tech_reference, Mapping) or set(tech_reference) != set(plans)
            or any(not isinstance(ref, Mapping) or any(not _text(k) or not isinstance(v, str) or v not in {"MET", "UNMET"}
                for k, v in ref.items()) for ref in tech_reference.values())
            or not _text(decision_id) or not _text(minute_snapshot_id)):
        raise ValueError("TRADER_SERVER_CONTEXT_INVALID")
    output, _ = project_trader_references(output, tech_reference)
    common = validate_team_output("A4", output, catalog, expected_ids=set(plans))
    issues = list(common.issues)
    def issue(code):
        issues.append("TRADER_" + code)
    if not isinstance(output, Mapping):
        return TeamValidation(False, tuple(dict.fromkeys(issues)), common.judgments, common.sentinel_count)
    if set(output) != _ROOT: issue("ROOT_FIELDS_INVALID")
    if output.get("schema_version") != "a4-trader/1.0.0": issue("SCHEMA_VERSION_INVALID")
    if not _same(output.get("envelope"), required_envelope): issue("ENVELOPE_BINDING_MISMATCH")
    if output.get("decision_id") != decision_id: issue("DECISION_BINDING_MISMATCH")
    if output.get("minute_snapshot_id") != minute_snapshot_id: issue("SNAPSHOT_BINDING_MISMATCH")
    notes = output.get("handoff_notes")
    if not isinstance(notes, Mapping) or set(notes) != {"to_a6", "to_a3"}: issue("HANDOFF_RECIPIENT_INVALID")
    signals = output.get("signals")
    ordered = set()
    if not isinstance(signals, list):
        issue("SIGNAL_POOL_INVALID")
    else:
        for row in signals:
            if (not isinstance(row, Mapping) or not _SIGNAL.issubset(row)
                    or set(row) - (_SIGNAL | {"waiting_for", "opposing_search_scope"})):
                issue("SIGNAL_FIELDS_INVALID"); continue
            pid, symbol, action = row.get("plan_id"), row.get("symbol"), row.get("action")
            if not isinstance(pid, str) or pid not in plans or plans[pid] != symbol:
                issue("PLAN_SYMBOL_MISMATCH"); continue
            if not isinstance(action, str) or action not in _ACTIONS:
                issue("ACTION_INVALID"); continue
            if candidate_actions is not None and action not in {candidate_actions[pid],'NO_ACTION'}:
                issue('CANDIDATE_ACTION_ESCALATION')
            if action in {"NO_ACTION", "START_CONFIRMATION"} and not _text(row.get("waiting_for")):
                issue("WAITING_FOR_REQUIRED")
            if action in _ORDERS: ordered.add(symbol)
            if not isinstance(row.get("reason_code"), str) or not re.fullmatch(r"[A-Z][A-Z0-9_]{1,79}", row["reason_code"]):
                issue("REASON_CODE_INVALID")
            if not _texts(row.get("what_would_change_my_mind")): issue("SIGNAL_FALSIFIER_REQUIRED")
            handoff = row.get("handoff_to_a6")
            if (not isinstance(handoff, Mapping) or set(handoff) != {"risk_unit_hint", "batches_hint", "holding_horizon", "note"}
                    or not isinstance(handoff.get("risk_unit_hint"), str) or handoff["risk_unit_hint"] not in {"STANDARD", "PROBE"}
                    or type(handoff.get("batches_hint")) is not int or handoff["batches_hint"] < 1
                    or not isinstance(handoff.get("holding_horizon"), str) or handoff["holding_horizon"] not in {"INTRADAY", "T1", "T3", "SWING"}
                    or not _text(handoff.get("note"))):
                issue("A6_HANDOFF_INVALID")
            reviews = row.get("tech_reference_review")
            reviewed = set()
            if not isinstance(reviews, list):
                issue("TECH_REFERENCE_INVALID")
            else:
                for ref in reviews:
                    if (not isinstance(ref, Mapping) or set(ref) != {"item", "status", "my_read"}
                            or not isinstance(ref.get("item"), str) or ref["item"] not in tech_reference[pid]
                            or ref["item"] in reviewed or ref.get("status") != tech_reference[pid][ref["item"]]
                            or not _text(ref.get("my_read"))):
                        issue("TECH_REFERENCE_INVALID"); continue
                    reviewed.add(ref["item"])
                if not {k for k, v in tech_reference[pid].items() if v == "UNMET"}.issubset(reviewed):
                    issue("UNMET_REFERENCE_UNEXPLAINED")
    actions = output.get("position_actions")
    seen = set()
    if not isinstance(actions, list):
        issue("POSITION_POOL_INVALID")
    else:
        for row in actions:
            if (not isinstance(row, Mapping) or not _POSITION.issubset(row)
                    or set(row) - (_POSITION | {"opposing_search_scope"})):
                issue("POSITION_FIELDS_INVALID"); continue
            symbol, action = row.get("symbol"), row.get("action")
            if not isinstance(symbol, str) or symbol not in held_symbols or symbol in seen:
                issue("POSITION_DOMAIN_INVALID"); continue
            seen.add(symbol)
            if not isinstance(action, str) or action not in {"HOLD", "REDUCE_SIGNAL", "SELL_SIGNAL", "ADD_SIGNAL"}:
                issue("POSITION_ACTION_INVALID"); continue
            if position_candidate_actions is not None and action not in {position_candidate_actions[symbol],'HOLD'}:
                issue('POSITION_CANDIDATE_ACTION_ESCALATION')
            if action in _ORDERS and symbol in ordered: issue("DUPLICATE_ORDER_ACTION")
            if (not isinstance(row.get("reason_code"), str) or not re.fullmatch(r"[A-Z][A-Z0-9_]{1,79}", row["reason_code"])
                    or not _text(row.get("distance_to_stop_note"))): issue("POSITION_REASON_INVALID")
    if position_candidate_actions is not None and seen!=set(held_symbols):issue('REVIEW_POSITION_DOMAIN_MISSING')
    return TeamValidation(not issues, tuple(dict.fromkeys(issues)), common.judgments, common.sentinel_count)
