from copy import deepcopy
import pytest

from liangjian_funnel.pipeline.team_v2_trader_schema import validate_trader_output

CAT = {"categories": ["MARKET", "STOCK_TECH"], "items": [
    {"id": "M1", "category": "MARKET"}, {"id": "T1", "category": "STOCK_TECH"}]}
ENV = {"stage_id": "A4", "run_id": "fixture"}
CONTEXT = dict(required_envelope=ENV, decision_id="d1", minute_snapshot_id="minute-1000",
    plans={"p1": "000001.SZ"}, held_symbols={"000002.SZ"},
    tech_reference={"p1": {"PRESSURE": "UNMET", "VWAP": "MET"}})


def output(action="BUY_SIGNAL"):
    return dict(envelope=deepcopy(ENV), schema_version="a4-trader/1.0.0", decision_id="d1",
        minute_snapshot_id="minute-1000", signals=[dict(plan_id="p1", symbol="000001.SZ",
            action=action, reason_code="RIGHT_SIDE_EVIDENCE", waiting_for="next closed reversal",
            supporting_refs=["M1"], opposing_refs=["T1"],
            tech_reference_review=[dict(item="PRESSURE", status="UNMET", my_read="alternative evidence M1")],
            what_would_change_my_mind=["structure reverses"],
            handoff_to_a6=dict(risk_unit_hint="PROBE", batches_hint=1, holding_horizon="T1", note="risk first"))],
        position_actions=[], handoff_notes={"to_a6": ["check capacity"], "to_a3": []},
        what_would_change_my_mind=["market reverses"])


def checked(raw):
    return validate_trader_output(raw, CAT, **CONTEXT)


def test_unmet_reference_explained_is_not_an_extra_and_hard_gate():
    assert checked(output()).valid


@pytest.mark.parametrize("kind", ["envelope", "decision", "snapshot", "extra_root", "missing_plan",
    "duplicate_plan", "outside_symbol", "bad_action", "shares", "missing_refs", "missing_explanation",
    "unknown_item", "duplicate_item", "bad_handoff", "bool_batches", "no_falsifier", "no_wait", "unheld_exit"])
def test_malformed_or_unbound_votes_never_repaired_into_pass(kind):
    raw=output(); row=raw["signals"][0]
    if kind=="envelope": raw["envelope"]={}
    elif kind=="decision": raw["decision_id"]="another"
    elif kind=="snapshot": raw["minute_snapshot_id"]="another"
    elif kind=="extra_root": raw["shares"]=1000
    elif kind=="missing_plan": raw["signals"]=[]
    elif kind=="duplicate_plan": raw["signals"].append(deepcopy(row))
    elif kind=="outside_symbol": row["symbol"]="600000.SH"
    elif kind=="bad_action": row["action"]="BUY_NOW"
    elif kind=="shares": row["shares"]=1000
    elif kind=="missing_refs": row["supporting_refs"]=[]
    elif kind=="missing_explanation": row["tech_reference_review"]=[]
    elif kind=="unknown_item": row["tech_reference_review"][0]["item"]="UNKNOWN_ITEM"
    elif kind=="duplicate_item": row["tech_reference_review"].append(deepcopy(row["tech_reference_review"][0]))
    elif kind=="bad_handoff": row["handoff_to_a6"]["risk_unit_hint"]="MAX"
    elif kind=="bool_batches": row["handoff_to_a6"]["batches_hint"]=True
    elif kind=="no_falsifier": row["what_would_change_my_mind"]=[]
    elif kind=="no_wait": row.update(action="NO_ACTION",waiting_for="")
    elif kind=="unheld_exit": raw["position_actions"]=[dict(symbol="600000.SH",action="SELL_SIGNAL",
        reason_code="EXIT",supporting_refs=["M1"],opposing_refs=["T1"],distance_to_stop_note="break")]
    assert not checked(raw).valid


def test_reference_status_is_server_owned_and_raw_action_is_unchanged():
    from liangjian_funnel.pipeline.team_v2_trader_schema import project_trader_references
    raw=output('NO_ACTION');raw['signals'][0]['tech_reference_review'][0]['status']='MET'
    original=deepcopy(raw)
    assert checked(raw).valid
    projected,changes=project_trader_references(raw,CONTEXT['tech_reference'])
    assert raw==original and projected['signals'][0]['action']=='NO_ACTION'
    assert projected['signals'][0]['tech_reference_review'][0]['status']=='UNMET'
    assert changes[0]['original']=='MET' and changes[0]['authoritative']=='UNMET'
    del raw['signals'][0]['tech_reference_review'][0]['status']
    assert checked(raw).valid


def test_malformed_nested_types_are_invalid_not_exceptions():
    for value in (None, [], {"signals": 3}, output() | {"signals": [None]}):
        assert not checked(value).valid
    for field in ("action", "reason_code", "handoff_to_a6", "tech_reference_review"):
        raw=output(); raw["signals"][0][field]={"invalid":[]}
        assert not checked(raw).valid


def test_duplicate_order_in_signal_and_position_action_is_not_two_dispatches():
    raw=output("SELL_SIGNAL")
    raw["position_actions"]=[dict(symbol="000001.SZ",action="SELL_SIGNAL",reason_code="EXIT",
        supporting_refs=["M1"],opposing_refs=["T1"],distance_to_stop_note="risk")]
    assert not validate_trader_output(raw,CAT,**(CONTEXT | {"held_symbols":{"000001.SZ"}})).valid


def test_opposing_sentinel_requires_server_catalog_search_scope():
    raw=output(); raw["signals"][0]["opposing_refs"]=["NO_OPPOSING_EVIDENCE_FOUND"]
    assert not checked(raw).valid
    raw["signals"][0]["opposing_search_scope"]=["MARKET","STOCK_TECH"]
    assert checked(raw).valid
