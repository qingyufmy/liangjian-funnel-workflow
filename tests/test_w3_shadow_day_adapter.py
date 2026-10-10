import copy

import pytest

from liangjian_funnel.evaluation.ablation.strategy_accumulation import canonical_sha256 as sha
from liangjian_funnel.evaluation.ablation.shadow_day_adapter import (
    build_shadow_day, production_equivalence, render_shadow_day,
)

DAY = "2026-10-12"
AT = DAY + "T15:30:00+08:00"


def source():
    baseline = {"plan_id":"p1", "symbol":"600000.SH", "minute":DAY+"T10:00:00+08:00",
        "action":"WAIT", "first_cause":"NO_PULLBACK", "event_id":"e1", "event_sha256":"a"*64,
        "field_status":"OK"}
    plan = {"plan_id":"p1", "symbol":"600000.SH", "strategy_profile":"TREND_MA5",
        "strategy_version":"trend-ma5/2", "activated_at":DAY+"T09:32:00+08:00",
        "expires_at":DAY+"T15:00:00+08:00", "entry_zone_lower":10, "entry_zone_upper":11}
    census = {"schema":"shadow-day-census/1", "trade_date":DAY, "source_ref":"fixture:census",
        "variant_set_version":"a4-shadow-variants/1", "plans":[plan],
        "expected_windows":[{"plan_id":"p1", "minute":baseline["minute"]}]}
    minute = {"minute":baseline["minute"], "status":"OK", "variant_set_version":"a4-shadow-variants/1",
        "requested_variant_count":6, "evaluated_variant_count":6, "evaluated_plan_count":1,
        "shadow_budget_exceeded_count":0, "baseline_records":[baseline]}
    signals = []
    for i in range(1,7):
        signals.append({"id":f"s{i}", "source_kind":"REALTIME_SHADOW", "event_kind":"INITIAL_STATE",
            "signal":{"variant_id":f"V{i}", "variant_set_version":"a4-shadow-variants/1",
                "plan_id":"p1", "symbol":"600000.SH", "profile":"TREND_MA5",
                "minute":baseline["minute"], "status":"OK", "variant_action":"WAIT",
                "variant_conditions":{"reason_codes":["NO_PULLBACK"]}}, "outcome":None})
    view = {"schema":"shadow-evidence/1", "signals":signals, "minutes":[minute],
        "price_limit_evidence":[{"plan_id":"p1", "trade_date":DAY, "captured_at":DAY+"T09:26:00+08:00",
            "original_evidence":{"symbol":"600000.SH"}, "limits":{"status":"KNOWN"}}],
        "events":[], "account_pnl":None, "source_authenticated":False}
    view["snapshot_canonical_sha256"] = sha(view)
    view.update(ok=True, hash_chain_status="MATCHED")
    return view,census,[baseline]


def seal(view):
    view["snapshot_canonical_sha256"] = sha({k:v for k,v in view.items()
        if k not in {"snapshot_canonical_sha256","ok","hash_chain_status"}})
    return view


def test_daily_zero_is_proven_only_by_complete_window_census():
    view,census,production = source()
    before = copy.deepcopy((view,census,production))
    result = build_shadow_day(view,census,production,as_of=AT)
    assert (view,census,production)==before
    assert result["production_equivalence"]["status"]=="MATCHED"
    assert result["groups"]["V1"]["TREND_MA5"]["first_trigger_count"]==0
    assert result["groups"]["V1"]["TREND_MA5"]["fill_count"]==0
    assert len(result["day_envelopes"])==6
    assert result["customer_notifications"]=="NOT_AUDITED"
    assert "非账户收益" in render_shadow_day(result)


@pytest.mark.parametrize("mutation", ["missing_minute","limited_input","budget","missing_identity"])
def test_coverage_gap_is_unknown_not_zero(mutation):
    view,census,production = source()
    if mutation=="missing_minute": view["minutes"]=[]
    if mutation=="limited_input": view["signals"][0]["signal"]["status"]="DATA_LIMITED"
    if mutation=="budget": view["minutes"][0]["status"]="SHADOW_BUDGET_EXCEEDED"
    if mutation=="missing_identity": view["price_limit_evidence"]=[]
    result = build_shadow_day(seal(view),census,production,as_of=AT)
    assert result["status"]=="DATA_LIMITED"
    assert result["groups"]["V1"]["TREND_MA5"]["first_trigger_count"] is None


def test_duplicate_first_trigger_is_not_double_counted_and_rejected():
    view,census,production = source()
    view["signals"][0]["event_kind"]="FIRST_TRIGGER"
    view["signals"].append(copy.deepcopy(view["signals"][0]))
    with pytest.raises(ValueError,match="DUPLICATE"):
        build_shadow_day(seal(view),census,production,as_of=AT)


def test_pending_fill_not_filled_and_missing_return_null_not_zero():
    view,census,production = source()
    view["signals"][0].update(event_kind="FIRST_TRIGGER",outcome={"fill_status":"PENDING_NEXT_COMPLETE_MINUTE"})
    result = build_shadow_day(seal(view),census,production,as_of=AT)
    group = result["groups"]["V1"]["TREND_MA5"]
    assert group["first_trigger_count"]==1 and group["fill_count"]==0
    assert group["returns"]["T1"]["mean"] is None
    assert group["returns"]["T1"]["sample_count"]==0


def test_filled_missing_tn_has_partial_coverage_and_no_winning_claim():
    view,census,production = source()
    view["signals"][0].update(event_kind="FIRST_TRIGGER",outcome={"fill_status":"FILLED",
        "fill_price":10.2,"fill_bar_end":DAY+"T10:02:00+08:00", "t1_close_return":.1,
        "t3_close_return":None,"t5_close_return":None, "mae":-.01,"mfe":.2,
        "available_at":"2026-10-13T15:10:00+08:00"})
    group = build_shadow_day(seal(view),census,production,as_of="2026-10-13T15:30:00+08:00")["groups"]["V1"]["TREND_MA5"]
    assert group["fill_count"]==1
    assert group["returns"]["T1"]["observed_mean"]==.1
    assert group["returns"]["T1"]["win_rate"] is None
    assert group["returns"]["T3"]["missing_count"]==1


def test_same_day_t1_claim_cannot_become_forward_return():
    view,census,production = source()
    view["signals"][0].update(event_kind="FIRST_TRIGGER",outcome={"fill_status":"FILLED",
        "fill_price":10.2,"fill_bar_end":DAY+"T10:02:00+08:00", "t1_close_return":.1,
        "available_at":DAY+"T15:10:00+08:00"})
    with pytest.raises(ValueError,match="HORIZON_NOT_CLOSED"):
        build_shadow_day(seal(view),census,production,as_of=AT)


def test_daily_equivalence_missing_or_empty_is_not_zero_diff_pass():
    _,_,records = source()
    assert production_equivalence(records,[]) ["status"]=="DATA_LIMITED"
    assert production_equivalence([],[]) ["status"]=="DATA_LIMITED"
    altered = copy.deepcopy(records);altered[0]["first_cause"]="DATA_BLOCK"
    assert production_equivalence(records,altered)["status"]=="MISMATCH"


def test_real_w1_field_status_shape_is_consumed_not_relabelled_missing():
    _,_,records = source()
    shadow = copy.deepcopy(records)
    shadow[0]["field_status"] = {"outer_baseline":"PRESENT", "action":"PRESENT",
        "event_id":"PRESENT", "event_sha256":"PRESENT", "reason":"PRESENT"}
    assert production_equivalence(records, shadow)["status"]=="MATCHED"


def test_requested_variant_counts_without_initial_states_cannot_claim_complete():
    view,census,production = source()
    view["signals"] = []
    result = build_shadow_day(seal(view),census,production,as_of=AT)
    assert result["status"]=="DATA_LIMITED"
    assert result["groups"]["V1"]["TREND_MA5"]["first_trigger_count"] is None


def test_expected_window_outside_plan_validity_is_rejected():
    view,census,production = source()
    census["expected_windows"][0]["minute"] = DAY+"T09:31:00+08:00"
    with pytest.raises(ValueError,match="VALIDITY"):
        build_shadow_day(view,census,production,as_of=AT)


def price_archive(census):
    from datetime import timedelta
    from liangjian_funnel.runtime.simulation import _first_complete_bar_end
    from datetime import datetime
    plan = census["plans"][0]
    clock = datetime.fromisoformat(plan["activated_at"])
    end = datetime.fromisoformat(plan["expires_at"])
    bars = []
    while (clock := _first_complete_bar_end(clock)) is not None and clock <= end:
        bars.append({"symbol":plan["symbol"],"bar_end":clock.isoformat(),
            "captured_at":(clock+timedelta(seconds=3)).isoformat(),"interval":"1m",
            "adjust_mode":"none","open":10.,"high":12.,"low":9.,"close":11.,
            "source_ref":"fixture:minute-original","source_input_sha256":"b"*64})
    material = {"schema":"shadow-validity-price-archive/1","trade_date":census["trade_date"],
        "source_ref":"fixture:full-validity-archive","bars":bars}
    return {**material,"sha256":sha(material)}


def test_opportunity_cost_uses_full_validity_untriggered_plan_observations():
    view,census,production=source()
    archive = price_archive(census)
    before=copy.deepcopy(archive)
    result=build_shadow_day(view,census,production,as_of=AT,price_archive=archive)
    group=result["groups"]["V1"]["TREND_MA5"]
    assert archive==before
    assert group["opportunity_cost"]["observed_sum"]==pytest.approx(12/11-1)
    assert group["opportunity_cost"]["sample_count"]==group["opportunity_cost"]["eligible_count"]==1
    assert group["opportunity_cost"]["mean"] is None
    assert group["opportunity_status"]=="INSUFFICIENT_EVIDENCE"
    assert result["day_envelopes"][0]["coverage"]["strategies"]["TREND_MA5"]["opportunity_cost"]=="COMPLETE"


def test_missing_validity_bar_does_not_fill_gap_or_zero_opportunity():
    view,census,production=source();archive=price_archive(census)
    archive["bars"].pop(20)
    archive["sha256"]=sha({k:v for k,v in archive.items() if k!="sha256"})
    group=build_shadow_day(view,census,production,as_of=AT,price_archive=archive)["groups"]["V1"]["TREND_MA5"]
    assert group["opportunity_cost"]["mean"] is None
    assert group["opportunity_cost"]["sample_count"]==0
    assert group["opportunity_cost"]["eligible_count"]==1
    assert group["opportunity_status"]=="UNKNOWN"


@pytest.mark.parametrize("mutation",["hash","future_arrival","duplicate","wrong_symbol"])
def test_opportunity_price_evidence_rejects_unbound_or_invalid_rows(mutation):
    view,census,production=source();archive=price_archive(census)
    if mutation=="hash":archive["sha256"]="0"*64
    if mutation=="future_arrival":archive["bars"][0]["captured_at"]="2026-10-13T09:00:00+08:00"
    if mutation=="duplicate":archive["bars"].append(copy.deepcopy(archive["bars"][0]))
    if mutation=="wrong_symbol":archive["bars"][0]["symbol"]="other"
    if mutation!="hash":archive["sha256"]=sha({k:v for k,v in archive.items() if k!="sha256"})
    with pytest.raises(ValueError):
        build_shadow_day(view,census,production,as_of=AT,price_archive=archive)


@pytest.mark.parametrize("mutation", ["source_hash","wrong_cohort","future_day","unknown_plan"])
def test_source_identity_and_clock_fail_closed(mutation):
    view,census,production = source()
    if mutation=="source_hash": view["snapshot_canonical_sha256"]="0"*64
    if mutation=="wrong_cohort": view["signals"][0]["source_kind"]="REALTIME_PAPER_LEDGER";seal(view)
    if mutation=="future_day": census["trade_date"]="2026-10-13"
    if mutation=="unknown_plan": view["signals"][0]["signal"]["plan_id"]="other";seal(view)
    with pytest.raises(ValueError): build_shadow_day(view,census,production,as_of=AT)
