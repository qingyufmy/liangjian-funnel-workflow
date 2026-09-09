from collections import defaultdict
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo
import json

import pytest

from liangjian_funnel.pipeline.deterministic import _matched_links, _business_theme_match
from liangjian_funnel.review.daily import _a2_projection, _model_fact_projection
from liangjian_funnel.review.verification import counterexample_drop_stage
from liangjian_funnel.review.context import pack_string_dictionary
from liangjian_funnel.runtime.entry_contract import freeze_entry_contract
from liangjian_funnel.runtime.entry_contract import next_entry_minute
from liangjian_funnel.runtime.indicator_history import prepare_520_history
from liangjian_funnel.runtime.simulation import PaperBroker, SimulationAction
from liangjian_funnel.runtime.state import RuntimeStore
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
from liangjian_funnel.evaluation.outcome_labels import backfill_forward_returns
from liangjian_funnel.data.mootdx import MinuteBar
from liangjian_funnel.data.cache import MinuteBarStore

TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 9, 9, 16, tzinfo=TZ)


def test_industry_priority_survives_dedup_and_business_existence_is_not_match():
    links = [dict(node_id="AAA", theme_id="AGRICULTURE", taxonomy="CONCEPT", taxonomy_code="C1", taxonomy_name="乡村振兴", confidence=1),
             dict(node_id="ZZZ", theme_id="GOLD", taxonomy="INDUSTRY", taxonomy_code="I1", taxonomy_name="贵金属", confidence=1),
             dict(node_id="ZZZ", theme_id="GOLD", taxonomy="CONCEPT", taxonomy_code="C2", taxonomy_name="黄金概念", confidence=1)]
    by_code = defaultdict(list)
    for row in links:
        by_code[row["taxonomy"], row["taxonomy_code"]].append(row)
    result = _matched_links(links, by_code)
    assert result[0]["theme_id"] == "GOLD"
    facts = [{"business_name": "黄金", "revenue_exposure_pct": 94.69}]
    assert _business_theme_match(facts, links, "GOLD")
    assert not _business_theme_match(facts, links, "AGRICULTURE")
    assert _matched_links(list(reversed(links)), by_code) == result


def test_full_a2_lineage_over_300_and_group_roundtrip():
    rows = [{"symbol": f"{i:06}.SZ", "name": str(i), "reason_codes": ["A2_OUTSIDE_ROTATION"], "theme_id": "GOLD"} for i in range(827)]
    audit = {"stages": [{"stage": "A2", "output": {"focus_pool": rows[:8], "watch_only_pool": rows[8:81],
        "outside_rotation_pool": rows[81:], "local_screen_summary": {"evaluated_count": 827, "sent_to_llm_count": 81}}}]}
    projection, missing = _a2_projection(audit)
    assert not missing and projection["lineage_complete"] and len(projection["candidates"]) == 827
    assert projection["counts"]["OUTSIDE_ROTATION"] == 746
    assert counterexample_drop_stage("000100.SZ", "OUTSIDE_ROTATION", [], {}) == "A2_QUANT_FILTERED"
    assert counterexample_drop_stage("000100.SZ", "OUTSIDE_ROTATION", ["000100.SZ"], {}) == "A4_NO_EFFECTIVE_SIGNAL"
    grouped = _model_fact_projection({"a2": projection})["a2"]["candidates"]
    restored = [{**group["common"], **row} for group in grouped["groups"] for row in group["stocks"]]
    for row in restored:
        row.setdefault("reason_codes", row["selection_reasons"])
    assert sorted(restored, key=lambda r: r["symbol"]) == sorted(projection["candidates"], key=lambda r: r["symbol"])


def test_dictionary_roundtrip_escapes_reserved_keys():
    text = "this is a long repeated evidence reason value"
    value = [{"x": text, "nil": None, "flag": False, "$a5_string": 999} for _ in range(20)]
    packed = pack_string_dictionary(value)
    def decode(node):
        if isinstance(node, list):
            return [decode(child) for child in node]
        if isinstance(node, dict):
            if "$a5_object" in node:
                return {key: decode(child) for key, child in node["$a5_object"].items()}
            if "$a5_string" in node:
                return packed["dictionary"][node["$a5_string"]]
            return {key: decode(child) for key, child in node.items()}
        return node
    assert decode(packed["data"]) == value


def minute(*, opening=10, low=9.9, high=10.1, at=None, interval="1m"):
    return MinuteBar(symbol="600001.SH", interval=interval, bar_end=at or NOW.replace(hour=10, minute=1),
                     open=opening, close=opening, high=high, low=low, volume=100000, amount=1000000,
                     source_id="TEST")


@pytest.mark.parametrize("limit,expected", [(9.8, None), (10.0, 10.0), (10.3, 10.0)])
def test_limit_is_ceiling_and_allows_price_improvement(tmp_path, limit, expected):
    broker = PaperBroker(RuntimeStore(tmp_path / "state.db"), model="test")
    action = SimulationAction(account_id=broker.account_id, signal_id="s1", symbol="600001.SH", action="BUY",
        signal_bar_end=NOW.replace(hour=10, minute=0), entry_reference=limit, stop_level=9,
        order_type="LIMIT", limit_price=limit)
    assert broker._adverse_price(action, minute()) == expected


def test_limit_touch_alone_not_fill_and_legacy_replay_unchanged(tmp_path):
    broker = PaperBroker(RuntimeStore(tmp_path / "state.db"), model="test")
    action = SimulationAction(account_id=broker.account_id, signal_id="s1", symbol="600001.SH", action="BUY",
        signal_bar_end=NOW.replace(hour=10, minute=0), entry_reference=9.9, stop_level=9, order_type="LIMIT", limit_price=9.9)
    assert broker._adverse_price(action, minute()) is None
    old = action.model_copy(update={"order_type": "LEGACY_REFERENCE", "entry_reference": 10.5})
    assert broker._adverse_price(old, minute()) is None


def test_frozen_entry_ignores_old_zone_and_honors_ceiling():
    plan = {"trigger_low": 17.878, "stop_level": 17.5, "no_chase_price": 18.01}
    contract = freeze_entry_contract("002826.SZ", plan, {"reference_price": 18.02})
    assert contract["signal_reference"] == 18.02 and contract["limit_price"] == 18.01
    plan["no_chase_price"] = 25
    assert contract["limit_price"] == 18.01
    assert freeze_entry_contract("002826.SZ", plan, {})["status"] == "INVALID"


def test_new_limit_fill_keeps_t1_and_stop_protection(tmp_path):
    store = RuntimeStore(tmp_path / "state.db")
    broker = PaperBroker(store, model="test")
    action = SimulationAction(account_id=broker.account_id, signal_id="s1", symbol="600001.SH", action="BUY",
        signal_bar_end=NOW.replace(hour=10, minute=0), entry_reference=10.05, stop_level=9.9, order_type="LIMIT", limit_price=10.05)
    outcome = broker.apply(action, minute())
    assert outcome.status == "FILLED"
    assert store.get_position(broker.account_id, "600001.SH")["sellable_qty"] == 0
    assert broker.apply(action, minute()).status == "DUPLICATE"
    exit_action = action.model_copy(update={"signal_id": "s2", "action": action.action.__class__("SELL"), "order_type": "LEGACY_REFERENCE"})
    assert broker.apply(exit_action, minute()).reason_code == "BLOCKED_T1"


def test_signal_returns_differ_per_event_and_missing_session_not_shifted(tmp_path):
    store = RuntimeStore(tmp_path / "state.db")
    for run, price in [("event1", 10), ("event2", 20)]:
        store.record_outcome_labels([dict(run_id=run, lane_id="lane_1", trade_date="2026-09-09", stage="A4",
            symbol="600001.SH", decision="PASSED", reason_codes=["OK"], selection_basis="A4_EXECUTION_SIGNAL",
            snapshot_id="s", config_hash="c", metadata={"performance_basis": "SIGNAL_REFERENCE_NOT_FILL", "signal_price": price})])
    prices = [{"symbol": "600001.SH", "trade_date": day, "close": close, "high": close, "low": close}
              for day, close in [("2026-09-09", 12), ("2026-09-11", 15), ("2026-09-14", 16)]]
    backfill_forward_returns(store, as_of_date="2026-09-14", price_source=prices)
    rows = {r["run_id"]: r for r in store.list_outcome_labels()}
    assert rows["event1"]["signal_return_1d"] is None  # Sep 10 missing, not shifted to Sep 11.
    assert rows["event1"]["signal_return_3d"] == pytest.approx(.6)
    assert rows["event2"]["signal_return_3d"] == pytest.approx(-.2)
    assert rows["event1"]["fwd_return_1d"] == rows["event2"]["fwd_return_1d"]  # Preserved legacy close basis.
    backfill_forward_returns(store, as_of_date="2026-09-14", price_source=prices)


def test_prepare_history_fetches_three_sessions_and_never_future(tmp_path):
    store = MinuteBarStore(tmp_path)
    calendar = ExchangeTradingCalendar()
    bars = []
    for day in (7, 8, 9):
        for hour, start_min in ((9, 30), (13, 0)):
            for n in range(1, 25):
                at = NOW.replace(day=day, hour=hour, minute=start_min) + timedelta(minutes=5*n)
                bars.append(minute(at=at, interval="5m"))
    calls = []
    class Provider:
        def fetch_bars(self, symbol, interval, required, *, as_of):
            calls.append(as_of)
            return SimpleNamespace(bars=bars)
    plan = {"plan_id": "p", "symbol": "600001.SH", "expires_at": "2026-09-10T15:00:00+08:00",
            "payload": {"strategy_profile": "MA520_SWING"}}
    args = dict(plans=[plan], now=NOW, calendar=calendar, minute_store=store, provider=Provider())
    result = prepare_520_history(**args)
    assert result["status"] == "READY" and result["plans"][0]["available_5m_count"] == 144
    assert prepare_520_history(**args)["status"] == "READY" and len(calls) == 1
    assert calls[0] <= NOW


def test_next_minute_contract_skips_lunch_not_entire_afternoon(tmp_path):
    signal_at = NOW.replace(hour=11, minute=30)
    assert next_entry_minute(signal_at) == NOW.replace(hour=13, minute=1)
    broker = PaperBroker(RuntimeStore(tmp_path / "state.db"), model="test")
    action = SimulationAction(account_id=broker.account_id, signal_id="s1", symbol="600001.SH", action="BUY",
        signal_bar_end=signal_at, entry_reference=10, stop_level=9.9, order_type="LIMIT", limit_price=10)
    assert broker.apply(action, minute(at=NOW.replace(hour=13, minute=2))).reason_code == "ENTRY_NEXT_BAR_MISSED"


def test_standalone_signal_card_all_pages_correct_time_and_idempotency(tmp_path):
    from liangjian_funnel.runtime.lark_notifications import WorkflowLarkPublisher
    from liangjian_funnel.runtime.lark import LarkDeliveryResult
    store = RuntimeStore(tmp_path / "state.db")
    publisher = WorkflowLarkPublisher(store, "https://open.larksuite.com/open-apis/bot/v2/hook/test")
    calls = []
    class Notifier:
        enabled = True
        def send(self, title, body, color):
            calls.append((title, body))
            return LarkDeliveryResult(True, "LARK_SENT", 200, 1)
    publisher.notifier = Notifier()
    rows = [{"symbol": f"{i:06}.SZ", "name": str(i), "entry_audit": {"signal_at": "2026-09-09T09:51:00+08:00",
             "signal_reference_price": 18.02, "fill_summary": "未成交"}, "performance_summary": "信号后+0.39%"} for i in range(23)]
    facts = {"review_kind": "POST_CLOSE", "trade_date": "2026-09-09", "signal_stock_reviews": rows}
    assert len(publisher.publish_signal_day_review(facts, now=NOW)) == 3
    assert len(publisher.publish_signal_day_review(facts, now=NOW)) == 3 and len(calls) == 3
    assert "09:51" in str(calls) and "18.02" in str(calls)
    assert publisher.publish_signal_day_review({**facts, "signal_stock_reviews": []}, now=NOW) == []


def test_cached_monthly_mapping_repair_preserves_membership_and_emotion():
    from liangjian_funnel.pipeline.deterministic import repair_cached_primary_mapping
    row = {"symbol": "002155.SZ", "primary_theme": "AGRICULTURE", "selection_basis": "HALF_YEAR_FUNDAMENTAL",
           "business_exposure": {"business_name": "黄金"}, "taxonomy_matches": [
           dict(node_id="A", theme_id="AGRICULTURE", taxonomy="CONCEPT", taxonomy_code="C1", taxonomy_name="乡村振兴", confidence=1),
           dict(node_id="Z", theme_id="GOLD", taxonomy="INDUSTRY", taxonomy_code="I1", taxonomy_name="贵金属", confidence=1)]}
    corrected = repair_cached_primary_mapping(row)
    assert corrected["primary_theme"] == "GOLD" and row["primary_theme"] == "AGRICULTURE"
    assert corrected["selection_basis"] == row["selection_basis"]
    overlay = {**row, "selection_basis": "DAILY_EMOTION_OVERLAY"}
    assert repair_cached_primary_mapping(overlay) == overlay


def test_verified_findings_cannot_be_cleared_by_healthy_model_prose():
    from liangjian_funnel.review.daily import A5ReviewReport, _enforce_verified_findings
    report = A5ReviewReport.model_validate(dict(schema_version="a5-daily-review/1.0.0",
        review_kind="POST_CLOSE", trade_date="2026-09-09", overall_verdict="HEALTHY",
        executive_summary="全部正常", **{f"{layer}_review": {"verdict": "HEALTHY", "summary": "正常"} for layer in ("a2", "a3", "a4")},
        unresolved_questions=[dict(question="样本足够吗", reason="INSUFFICIENT_SAMPLE", resolution="继续记录")]))
    facts = {"a2": {"lineage_complete": False}, "metrics": {"a4_m15_macd_applicable_plan_count": 1,
        "a4_monitor_observation_count": 240, "a4_m15_macd_warmed_plan_count": 0},
        "independent_verification": {"a4": {"plans": [{"evidence_id": "A5V:A4:P", "cross_source_status": "MISMATCH", "observation_coverage": 1}]},
                                     "a3": {"not_verified_fields": ["MACD"]}}}
    _enforce_verified_findings(report, facts)
    assert report.overall_verdict == report.a4_review.verdict == "NEEDS_ATTENTION"
    assert len(report.core_defects) == 3 and report.a3_review.data_limitations
    assert all(item.evidence_ids for item in report.core_defects)
    A5ReviewReport.model_validate(report.model_dump())


def test_new_entry_and_add_have_separate_tn_identity_and_frozen_price(tmp_path):
    from liangjian_funnel.workflow import WorkflowApplication
    from liangjian_funnel.runtime.state import PlanStatus, MonitorAction
    store = RuntimeStore(tmp_path / "state.db")
    store.record_workflow_run(run_id="source", lane_id="lane_1", trade_date="2026-09-08", slot="CLOSE",
        model="test", status="PUBLISHED", snapshot_hash="snapshot", config_hash="config")
    plan = store.create_execution_plan("p", "lane_1", "600001.SH", status=PlanStatus.ACTIVE_TODAY,
        payload={"source_run_id": "source", "strategy_profile": "TREND_MA5"})
    events = []
    for n, (action, price) in enumerate([(MonitorAction.BUY_SIGNAL, 10), (MonitorAction.ADD_SIGNAL, 11)]):
        event, _ = store.record_monitor_event(event_key=f"event:{n}", lane_id="lane_1", minute_end=NOW.replace(hour=10, minute=n),
            action=action, effective=True, payload={"plan_id": "p", "symbol": "600001.SH",
                "entry_contract": {"version": "a4-entry/1", "signal_reference": price}})
        events.append(event)
    app = SimpleNamespace(store=store)
    assert WorkflowApplication._record_a4_outcomes(app, events, {"p": plan})["status"] == "READY"
    assert WorkflowApplication._record_a4_outcomes(app, events, {"p": plan})["status"] == "READY"
    rows = store.list_outcome_labels(stage="A4")
    assert len(rows) == 2 and len({row["run_id"] for row in rows}) == 2
    assert {json.loads(row["metadata_json"])["signal_price"] for row in rows} == {10, 11}


@pytest.mark.parametrize("reference,opening,low,high,expected", [
    (18.02, 18.02, 17.99, 18.02, 18.02),
    (20.50, 20.51, 20.50, 20.54, None),
    (9.28, 9.27, 9.26, 9.27, 9.27),
])
def test_three_signal_next_bars_limit_matching_only(tmp_path, reference, opening, low, high, expected):
    # Frozen Sep 9 next-bar OHLC; this isolates price matching, not complete
    # account/strategy replay, queue certainty or a historical production fill.
    broker = PaperBroker(RuntimeStore(tmp_path / "state.db"), model="test")
    action = SimulationAction(account_id=broker.account_id, signal_id="test", symbol="600001.SH", action="BUY",
        signal_bar_end=NOW.replace(hour=10, minute=0), entry_reference=reference, stop_level=reference*.95,
        order_type="LIMIT", limit_price=reference)
    assert broker._adverse_price(action, minute(opening=opening, low=low, high=high)) == expected


def test_indicator_preparation_failure_is_visible_not_a_plan_rejection(tmp_path):
    class Failing:
        def fetch_bars(self, *args, **kwargs):
            raise TimeoutError("fixture")
    plan = {"plan_id": "p", "symbol": "600001.SH", "expires_at": "2026-09-10T15:00:00+08:00",
            "payload": {"strategy_profile": "MA520_SWING"}}
    result = prepare_520_history([plan], now=NOW, calendar=ExchangeTradingCalendar(),
                                minute_store=MinuteBarStore(tmp_path), provider=Failing())
    assert result["status"] == "DATA_LIMITED" and result["plans"][0]["error_type"] == "TimeoutError"
    assert plan["payload"] == {"strategy_profile": "MA520_SWING"}


@pytest.mark.parametrize("stock_score,sector_score,met", [(86, 30, True), (20, 90, False)])
def test_stock_trend_classification_does_not_use_sector_weekly_score(stock_score, sector_score, met):
    from liangjian_funnel.pipeline.deterministic import _a2_behavior_evidence
    row = _a2_behavior_evidence(item={}, factor_scores={
        "trend_strength_proxy": {"available": True, "score": stock_score, "source": "LOCAL_POINT_IN_TIME_DAILY_BARS"},
        "weekly_confirmation": {"available": True, "score": sector_score, "source": "A1_BOUND_TAXONOMY_AGGREGATE"}},
        identifiability=80, minimum_identifiability_score=60, relative=80, liquidity=80, legacy_role="TREND_LEADER", as_of=NOW.isoformat())
    assert row["medium_term_trend"]["met"] is met
    assert row["medium_term_trend"]["value"]["source_factor"] == "trend_strength_proxy"


def test_trend_v2_requires_separate_closed_reversal_and_volume_confirmation():
    from datetime import timedelta
    from dataclasses import replace
    from liangjian_funnel.runtime.strategies import _Bar, _trend_entry_sequence, _evaluate_trend
    def bar(n, opening, close, low, volume):
        return _Bar("600001.SH", NOW.replace(hour=10, minute=0) + timedelta(minutes=n*5),
                    opening, max(opening, close)+.1, low, close, volume, close*volume)
    bars = [bar(0, 10.4, 10.3, 10.2, 200), bar(1, 10.3, 10.1, 10, 100),
            bar(2, 10.1, 10.2, 10.05, 120), bar(3, 10.2, 10.4, 10.1, 180)]
    assert all(_trend_entry_sequence(bars, 10.2).values())
    assert not all(_trend_entry_sequence(bars[:-1], 10.2).values())
    assert not _trend_entry_sequence([*bars[:-1], replace(bars[-1], volume=100)], 10.2)["TREND_SUBSEQUENT_5M_CONFIRMATION"]
    assert not _trend_entry_sequence([bars[0], replace(bars[1], volume=250), *bars[2:]], 10.2)["TREND_PULLBACK_VOLUME_CONTRACTION"]
    # A red 15m bar that no longer makes lower lows is not automatically a
    # sell-pressure veto. Old plans retain the exact previous interpretation.
    fifteen = [bar(0, 10.3, 10.2, 9.9, 300), bar(3, 10.3, 10.2, 10, 300)]
    plan = {"trigger_low": 10, "trigger_high": 11, "trend_entry_rule_version": "trend-ma5/2"}
    new = _evaluate_trend(plan, bars, fifteen, bars[-1], {}, False, False)
    old = _evaluate_trend({**plan, "trend_entry_rule_version": "legacy"}, bars, fifteen, bars[-1], {}, False, False)
    assert new["action"] == "BUY_SIGNAL"
    assert old["action"] != "BUY_SIGNAL"


def test_rotation_reserve_enters_technical_research_but_never_execution_core():
    from liangjian_funnel.pipeline.research import _build_a3_candidate_domain, _apply_a3_candidate_origin_policy
    reserve = {"symbol": "002155.SZ", "stock_behavior_type": "TREND", "route_permission": ["TREND_MA5", "MA520_SWING"],
               "route": "MARKET_CORE", "top_rotation_theme": False, "rotation_reserve_eligible": True,
               "rotation_reserve_scope": "RESEARCH_ONLY_NO_AUTOMATIC_ENTRY", "data_sufficiency_state": "SUFFICIENT"}
    domain, origins = _build_a3_candidate_domain({"focus_pool": [], "watch_only_pool": [reserve]})
    assert origins == {"002155.SZ": "WATCH_ONLY"}
    assert len(domain["focus_pool"]) == 1
    output, _ = _apply_a3_candidate_origin_policy({"core_watch_pool": [{"symbol": "002155.SZ", "eligibility": "QUALIFIED", "review_status": "PASS"}]},
        {"A3_CANDIDATE_ORIGIN": origins, "A2_BOTTLENECK_CONTEXT": {"002155.SZ": reserve}})
    assert output["core_watch_pool"] == []
    assert output["secondary_watch_pool"][0]["risk_unit"] == "NO_ENTRY"
    assert output["secondary_watch_pool"][0]["review_status"] == "PASS"
    rejected = {**reserve, "data_sufficiency_state": "INSUFFICIENT"}
    assert not _build_a3_candidate_domain({"watch_only_pool": [rejected]})[1]


def test_repeated_reason_list_encoding_roundtrip_and_reserved_keys():
    from liangjian_funnel.review.context import pack_structure_dictionary, json_size
    reasons = ["A2_REPEATED_LONG_REASON_" + str(i) for i in range(12)]
    original = [{"symbol": f"{i:06}.SZ", "reasons": reasons, "nil": None,
                 "$a5_value": 77, "$a5_value_object": {"x": False}} for i in range(100)]
    original.append({"$a5_value": 999, "one_off": [None, False]})
    packed = pack_structure_dictionary(original)
    def decode(node):
        if isinstance(node, list):
            return [decode(item) for item in node]
        if isinstance(node, dict):
            if "$a5_value_object" in node:
                return {key:decode(child) for key,child in node["$a5_value_object"].items()}
            if "$a5_value" in node:
                return packed["dictionary"][node["$a5_value"]]
            return {key:decode(child) for key,child in node.items()}
        return node
    assert decode(packed["data"]) == original
    assert json_size(packed) < json_size(original) / 2


def test_engineering_validation_is_not_forced_to_ten_shadow_days():
    from liangjian_funnel.review.daily import A5Proposal
    payload = dict(proposal_id="fix", type="DATA_FIX", target="A4", hypothesis="data",
                   proposed_change="verify", validation_method="replay", success_criteria="match",
                   falsification_criteria="mismatch", min_shadow_days=0, risk="none")
    assert A5Proposal.model_validate(payload).min_shadow_days == 0
    with pytest.raises(ValueError):
        A5Proposal.model_validate({**payload, "type": "SHADOW_TEST"})


def test_a5_embedded_states_keep_chinese_execution_meaning():
    from liangjian_funnel.runtime.lark_notifications import _display_text
    rendered = _display_text('TREND_MA5 BUY_SIGNAL UNFILLED；OUTSIDE_ROTATION；OPEN/HIGH/LOW/VOLUME/AMOUNT',limit=500)
    assert rendered == '趋势 5 日线 买入触发 未成交；轮动范围外；开盘价/最高价/最低价/成交量/成交额'
    assert _display_text('HIGH') == '较高'
    assert _display_text('LOW') == '较低'
    assert '系统内部状态' not in _display_text('UNRECOGNIZED_FUTURE_REASON')
    assert '阶段追溯完整' in _display_text('lineage_complete=true')


def test_close_time_cannot_emit_unexecutable_buy_notification(tmp_path):
    from liangjian_funnel.runtime.monitor import MonitorEngine
    store=RuntimeStore(tmp_path/'monitor.db')
    engine=MonitorEngine(store)
    plan={'plan_id':'p','symbol':'600001.SH','payload_json':json.dumps({'stop_level':9.0}),
          'expires_at':'2026-09-09T15:00:00+08:00'}
    strategy={'action':'BUY_SIGNAL','live_entry_price':10.0,'live_stop_level':9.0}
    event=engine._emit_effective('lane_1',plan,NOW.replace(hour=15), 'closed-snapshot','BUY_SIGNAL','STRATEGY_ENTRY_READY',strategy_result=strategy)
    assert event.effective is False and event.action=='NO_ACTION'
    assert event.reason_code in {'ENTRY_NO_NEXT_TRADING_MINUTE','ENTRY_NEXT_MINUTE_AFTER_PLAN_EXPIRY'}
    assert not any(row['effective'] for row in store.list_monitor_events())
    contract=freeze_entry_contract('600001.SH',{'stop_level':9.0,'expires_at':plan['expires_at']},strategy,at=NOW.replace(hour=14,minute=59))
    assert contract['status']=='READY' and contract['eligible_bar_end']=='2026-09-09T15:00:00+08:00'
