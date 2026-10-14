from datetime import datetime
import json, hashlib
import pytest
from liangjian_funnel.runtime.card_archive import archive_card
from liangjian_funnel.review.business_audit import business_audit

NOW=datetime.fromisoformat("2026-10-14T16:00:00+08:00")


def test_archive_only_redacts_copy_and_preserves_binding(tmp_path):
    card={"body":"sk-abcdefghijklmnop https://open.larksuite.com/open-apis/bot/v2/hook/private", "api_key":"private"}
    original=json.dumps(card)
    result=archive_card(directory=tmp_path,card=card,delivery_key="one",source_run_id="run-one",plan_ids=["plan-one"],trade_date="2026-10-14",observed_at=NOW)
    archived=json.loads(open(result["card_evidence_path"],encoding="utf-8").read())
    assert "private" not in json.dumps(archived)
    assert json.dumps(card)==original
    raw=json.dumps(archived["card"],ensure_ascii=False,sort_keys=True,separators=(",",":")).encode()
    assert hashlib.sha256(raw).hexdigest()==result["sanitized_card_sha256"]
    with pytest.raises(ValueError,match="BINDING_CONFLICT"):
        archive_card(directory=tmp_path,card=card,delivery_key="one",source_run_id="wrong-run",plan_ids=["plan-one"],trade_date="2026-10-14",observed_at=NOW)


def test_a5_events_are_not_delivery_rows_and_missing_is_unknown():
    event={"event_id":"1","action":"BUY_SIGNAL","effective":True,"reason_code":"READY","payload_json":json.dumps({"plan_id":"p"})}
    result=business_audit(events=[event,event],plans=[{"plan_id":"p","strategy_profile":"TREND_MA5"}],delivery_counts={"total":7,"statuses":{"SENT":7},"truncated":False},operational=[{"kind":"JOB_TERMINATED","run_id":"node-one","evidence_id":"incident"}],replay={"trigger_windows":[{"classification":"MISSING_EXECUTION_RECORD","evidence_id":"missing","minute_end":NOW.isoformat()}]})
    assert result["unique_buy_signal_events"]==1
    assert result["event_rows"]==2 and result["notification_delivery_rows"]==7
    assert result["first_reason_by_strategy"]=={"TREND_MA5":{"READY":2}}
    assert result["gap_cause_evidence"][0]["cause"]=="TASK_SIGTERM"
    assert result["missing_execution_records"][0]["cause"]=="UNKNOWN"
    assert result["gap_cause_evidence"][0]["gap_proven"] is False


def test_a5_new_counts_are_compact_and_citable_without_losing_raw_ids():
    from liangjian_funnel.review.business_audit import compact_business_audit
    from liangjian_funnel.review.daily import _evidence_ids, _projection_evidence_ids
    rows=[{"evidence_id":"missing:"+str(i),"plan_id":"p","cause":"UNKNOWN","minute_end":str(i)} for i in range(1000)]
    facts={"evidence_id":"A5:BUSINESS_AUDIT", "missing_execution_records":rows,"gap_cause_evidence":[]}
    compact=compact_business_audit(facts)
    assert compact["missing_execution_record_count"]==1000
    assert len(compact["missing_execution_record_groups"])==1
    assert len(json.dumps(compact))<1500 and len(rows)==1000
    assert "A5:BUSINESS_AUDIT" in _evidence_ids({"business_audit":facts})
    assert compact["missing_execution_record_groups"][0]["evidence_id"] in _projection_evidence_ids({"business_audit":compact})
