from copy import deepcopy
from liangjian_funnel.data.rotation_policy import authorized_rotation_policy, positive_rotation_flow


def snapshot():
    rows = [{"board_code": str(i), "theme_id": str(i), "theme_level": "PRIMARY",
             "strength": 100-i, "main_net_inflow_cny": 1 if i == 9 else -1,
             "member_snapshot_complete": True, "factor_coverage": 1,
             "effective_fund_coverage": 1, "price_coverage": 1,
             "constituents": [str(i)], "selected_for_rotation": False} for i in range(1, 10)]
    return {"available": True, "trade_date": "2026-10-13", "boards": rows, "by_symbol": {}}


def test_top_eight_precedes_flow_gate_no_ninth_refill():
    source = snapshot(); before = deepcopy(source)
    result = authorized_rotation_policy(source)
    assert not result["selected_primary_boards"]
    assert source == before


def test_top8_admission_does_not_expand_existing_top5_numeric_exception():
    from liangjian_funnel.pipeline.research.a2 import _a2_relative_top5_market_core_exception
    row={"symbol":"000001.SZ"}
    context={"deterministic_status":"REVIEW_CANDIDATE","top_rotation_theme":True,
             "theme_rotation_rank":3,"eligible_routes":["MARKET_CORE"]}
    data={"A2_AUTHORIZED_ROTATION_POLICY":"authorized","A2_BOTTLENECK_CONTEXT":{"000001.SZ":context}}
    assert _a2_relative_top5_market_core_exception(row,data)
    context["theme_rotation_rank"]=6
    assert not _a2_relative_top5_market_core_exception(row,data)
    data.pop("A2_AUTHORIZED_ROTATION_POLICY")
    assert _a2_relative_top5_market_core_exception(row,data)


def test_three_day_flow_requires_verified_complete_sessions():
    row = {"main_net_inflow_cny": -1, "main_net_inflow_3d_mean_cny": 3}
    assert not positive_rotation_flow(row)
    row["fund_flow_history_evidence"] = {"consecutive_sessions": 3, "hash_verified": True,
                                         "same_basket": True}
    assert positive_rotation_flow(row)


def test_selected_by_symbol_and_rank_agree_with_positive_three_day_branch():
    source = snapshot(); row = source["boards"][6]
    row.update(main_net_inflow_3d_mean_cny=3, eastmoney_main_net_inflow_cny=-2, fund_flow_history_evidence={
        "consecutive_sessions": 3, "hash_verified": True, "same_basket": True})
    result = authorized_rotation_policy(source)
    assert [x["board_code"] for x in result["selected_primary_boards"]] == ["7"]
    assert result["by_symbol"]["7"][0]["primary_rank"] == 7


def test_real_cross_source_sign_conflict_remains_blocked():
    source=snapshot();source["boards"][0].update(main_net_inflow_cny=3,eastmoney_main_net_inflow_cny=-2)
    assert not authorized_rotation_policy(source)["boards"][0]["selected_for_rotation"]


def test_unavailable_source_not_promoted():
    source = snapshot(); source["available"] = False
    assert authorized_rotation_policy(source) == source


def test_empty_rotation_gate_is_global_not_only_no_candidate_membership_match():
    from liangjian_funnel.data.rotation_policy import authorized_rotation_gate_empty
    source = authorized_rotation_policy(snapshot())
    assert authorized_rotation_gate_empty(source)
    # An admitted board outside all current A1 candidates is still not empty.
    source['boards'][0]['selected_for_rotation'] = True
    source['by_symbol'] = {}
    assert not authorized_rotation_gate_empty(source)
    source['boards'][0]['selected_for_rotation'] = False
    source['selected_primary_boards'] = [{'selected_for_rotation': True}]
    assert not authorized_rotation_gate_empty(source)
    source['available'] = False
    source['selected_primary_boards'] = []
    assert not authorized_rotation_gate_empty(source)


def test_family_summary_does_not_omit_a_positive_parent_with_negative_stronger_child():
    source=snapshot()
    source["boards"]=[dict(source["boards"][0],main_net_inflow_cny=5,strength=80),
        dict(source["boards"][1],theme_level="CHILD",parent_theme_id="1",strength=90)]
    result=authorized_rotation_policy(source)
    assert [row["board_code"] for row in result["selected_primary_boards"]]==["1"]
    assert result["by_symbol"]["1"][0]["selected_for_rotation"] is True
    assert "constituents" not in result["by_symbol"]["1"][0]
    import json,hashlib
    body=dict(result);expected=body.pop("content_hash")
    assert hashlib.sha256(json.dumps(body,ensure_ascii=False,sort_keys=True,separators=(",", ":")).encode()).hexdigest()==expected


def test_three_session_mean_only_uses_hash_valid_closed_same_basket_history(tmp_path):
    import json
    from datetime import date
    from liangjian_funnel.data.rotation_theme import _apply_history_factors,_content_hash,ROTATION_THEME_SCHEMA
    for day,flow in [("2026-10-13",10),("2026-10-12",7)]:
        body={"schema_version":ROTATION_THEME_SCHEMA,"trade_date":day,"captured_at":day+"T15:10:00+08:00",
              "boards":[{"theme_id":"TEST","theme_level":"PRIMARY","component_board_codes":["C"],
                         "tencent_main_net_inflow_cny":flow,"member_snapshot_complete":True,"effective_fund_coverage":1}]}
        (tmp_path/('rotation-theme-'+day+'.json')).write_text(json.dumps({**body,"content_hash":_content_hash(body)}),encoding="utf-8")
    row={"theme_id":"TEST","component_board_codes":["C"],"tencent_main_net_inflow_cny":-2}
    _apply_history_factors([row],tmp_path,date(2026,10,14))
    assert row["main_net_inflow_3d_mean_cny"]==5
    assert row["fund_flow_history_evidence"]["trade_dates"]==["2026-10-14","2026-10-13","2026-10-12"]
    path=tmp_path/'rotation-theme-2026-10-13.json'
    body=json.loads(path.read_text());body.pop("content_hash");body["captured_at"]="2026-10-15T15:00:00+08:00"
    path.write_text(json.dumps({**body,"content_hash":_content_hash(body)}),encoding="utf-8")
    _apply_history_factors([row],tmp_path,date(2026,10,14))
    assert "main_net_inflow_3d_mean_cny" not in row
