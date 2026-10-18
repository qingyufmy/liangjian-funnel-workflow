"""Opening evidence is not activation, a research retry or a price rebase."""
from datetime import datetime
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.runtime.opening_evidence import capture_opening_evidence

TZ = ZoneInfo("Asia/Shanghai")
AT = datetime(2026, 10, 16, 9, 25, 5, tzinfo=TZ)


def settings(tmp_path):
    database = tmp_path / "state.sqlite3"
    with sqlite3.connect(database) as conn:
        conn.execute("CREATE TABLE execution_plans(plan_id,lane_id,symbol,status,expires_at,payload_json)")
        conn.execute("INSERT INTO execution_plans VALUES (?,?,?,?,?,?)", (
            "run:lane_1:p", "lane_1", "600000.SH", "PENDING_MORNING_REVIEW",
            "2026-10-16T15:00:00+08:00", '{}'))
    return SimpleNamespace(state_db_path=database, workflow_output_dir=tmp_path / "outputs",
                           research_primary_lane_id="lane_1")


class Calendar:
    def is_trading_day(self, day): return day == AT.date()


def quote(stamp="2026-10-16T09:25:00+08:00", **extra):
    return {"600000.SH": {"symbol":"600000.SH", "source_id":"TENCENT:qt.gtimg.cn",
        "latest_price":10., "quote_time":stamp, "no_reported_trades":False, **extra}}


def test_collects_once_in_window_with_original_provider_time_and_no_db_mutation(tmp_path):
    config = settings(tmp_path)
    before = config.state_db_path.read_bytes()
    calls = []
    def fetch(symbols):
        calls.append(symbols)
        return quote()
    result = capture_opening_evidence(config, calendar=Calendar(), now=AT,
        clock=lambda: AT, fetch=fetch)
    assert result["status"] == "CAPTURED"
    frozen = json.loads(Path(result["evidence_path"]).read_text(encoding="utf-8"))
    assert frozen["by_symbol"]["600000.SH"]["quote_time"] == "2026-10-16T09:25:00+08:00"
    assert frozen["by_symbol"]["600000.SH"]["auction_final_price_claimed"] is True
    assert frozen["execution_authority"] is False
    assert config.state_db_path.read_bytes() == before
    second = capture_opening_evidence(config, calendar=Calendar(), now=AT,
        clock=lambda: AT, fetch=fetch)
    assert second["reason_code"] == "OPENING_EVIDENCE_ALREADY_CLAIMED"
    assert calls == [["600000.SH"]]


@pytest.mark.parametrize("stamp,extra", [
    ("2026-10-15T09:25:00+08:00", {}),
    ("2026-10-16T09:24:59+08:00", {}),
    ("2026-10-16T09:26:00+08:00", {}),
    ("2026-10-16T09:25:01+08:00", {}),
    ("2026-10-16T09:25:00+08:00", {"symbol":"000001.SZ"}),
    ("2026-10-16T09:25:00+08:00", {"latest_price":float("nan")}),
    ("2026-10-16T09:25:00+08:00", {"no_reported_trades":True}),
])
def test_non_final_or_invalid_quotes_preserved_but_never_claimed(tmp_path, stamp, extra):
    result = capture_opening_evidence(settings(tmp_path), calendar=Calendar(), now=AT,
        clock=lambda: AT, fetch=lambda symbols:quote(stamp, **extra))
    frozen = json.loads(Path(result["evidence_path"]).read_text(encoding="utf-8"))
    assert frozen["by_symbol"]["600000.SH"]["auction_final_price_claimed"] is False
    assert frozen["by_symbol"]["600000.SH"]["reason_codes"]


@pytest.mark.parametrize("current", [AT.replace(minute=24,second=59),
                                     AT.replace(second=31), AT.replace(minute=26)])
def test_outside_real_window_does_not_fetch_or_claim(tmp_path, current):
    def forbidden(symbols): raise AssertionError("NETWORK_NOT_ALLOWED")
    config = settings(tmp_path)
    result = capture_opening_evidence(config, calendar=Calendar(), now=current,
        clock=lambda:current, fetch=forbidden)
    assert result["reason_code"] == "OPENING_EVIDENCE_WINDOW_MISSED"
    assert not config.workflow_output_dir.exists()


def test_delayed_receive_is_not_backdated_as_auction_final(tmp_path):
    result = capture_opening_evidence(settings(tmp_path), calendar=Calendar(), now=AT,
        clock=lambda:AT.replace(second=31), fetch=lambda symbols:quote())
    frozen = json.loads(Path(result["evidence_path"]).read_text(encoding="utf-8"))
    assert frozen["by_symbol"]["600000.SH"]["auction_final_price_claimed"] is False
    assert frozen["received_at"].endswith("09:25:31+08:00")


def test_source_error_is_frozen_once_and_not_retried(tmp_path):
    def failed(symbols): raise OSError("not copied into receipt")
    config = settings(tmp_path)
    result = capture_opening_evidence(config, calendar=Calendar(), now=AT,
        clock=lambda:AT, fetch=failed)
    assert result["status"] == "DATA_LIMITED"
    assert result["reason_code"] == "OPENING_EVIDENCE_SOURCE_FAILED"
    assert "not copied" not in Path(result["evidence_path"]).read_text(encoding="utf-8")


def test_cli_dispatch_never_constructs_workflow_application(tmp_path, monkeypatch, capsys):
    import liangjian_funnel.cli as cli
    import liangjian_funnel.runtime.opening_evidence as evidence
    def forbidden(*args, **kwargs): raise AssertionError("WORKFLOW_CONSTRUCTION_FORBIDDEN")
    monkeypatch.setattr(cli, "WorkflowApplication", forbidden)
    monkeypatch.setattr(evidence, "capture_scheduled_opening_evidence", lambda settings: {
        "status":"NOOP", "reason_code":"OPENING_EVIDENCE_WINDOW_MISSED"})
    assert cli.main(["run-auction-evidence"], settings=settings(tmp_path)) == 0
    assert json.loads(capsys.readouterr().out)["reason_code"] == "OPENING_EVIDENCE_WINDOW_MISSED"


def test_missing_scope_has_no_source_call_or_plan_side_effect(tmp_path):
    config = settings(tmp_path)
    with sqlite3.connect(config.state_db_path) as connection:
        connection.execute("UPDATE execution_plans SET expires_at='2026-10-15T15:00:00+08:00'")
    def forbidden(symbols): raise AssertionError("NO_SOURCE_FOR_STALE_SCOPE")
    result = capture_opening_evidence(config, calendar=Calendar(), now=AT,
        clock=lambda:AT, fetch=forbidden)
    assert result["reason_code"] == "OPENING_EVIDENCE_SCOPE_EMPTY"
    assert not config.workflow_output_dir.exists()


def test_four_fixed_polls_are_exclusive_per_slot_with_original_prints(tmp_path):
    from liangjian_funnel.runtime.opening_evidence import capture_scheduled_opening_evidence
    config=settings(tmp_path);before=config.state_db_path.read_bytes();seen=[]
    for second in (3,8,15,24):
        at=AT.replace(second=second)
        result=capture_scheduled_opening_evidence(config,calendar=Calendar(),clock=lambda:at,
            fetch=lambda symbols:(seen.append(second) or quote()))
        assert result['status']=='CAPTURED'
        raw=json.loads(Path(result['evidence_path']).read_text(encoding='utf-8'))
        assert raw['poll_slot_second']==second and raw['source_attempts']==1
        repeated=capture_scheduled_opening_evidence(config,calendar=Calendar(),clock=lambda:at,
            fetch=lambda symbols:pytest.fail('DUPLICATE_SLOT_NETWORK'))
        assert repeated['reason_code']=='OPENING_EVIDENCE_ALREADY_CLAIMED'
    assert seen==[3,8,15,24] and before==config.state_db_path.read_bytes()


@pytest.mark.parametrize('second',[0,2,5,7,10,14,17,23,26,30,31])
def test_missed_poll_is_not_caught_up_or_claimed(tmp_path,second):
    from liangjian_funnel.runtime.opening_evidence import capture_scheduled_opening_evidence
    config=settings(tmp_path)
    result=capture_scheduled_opening_evidence(config,calendar=Calendar(),clock=lambda:AT.replace(second=second),
        fetch=lambda symbols:pytest.fail('LATE_POLL_NETWORK'))
    assert result['status']=='NOOP' and not config.workflow_output_dir.exists()


def test_poll_receive_after_deadline_is_retained_but_not_claimed(tmp_path):
    from liangjian_funnel.runtime.opening_evidence import capture_scheduled_opening_evidence
    times=iter((AT.replace(second=24),AT.replace(second=24),AT.replace(second=31)))
    result=capture_scheduled_opening_evidence(settings(tmp_path),calendar=Calendar(),
        clock=lambda:next(times),fetch=lambda symbols:quote())
    body=json.loads(Path(result['evidence_path']).read_text(encoding='utf-8'))
    assert body['status']=='DATA_LIMITED' and body['poll_slot_second']==24
    assert body['received_at'].endswith('09:25:31+08:00') and body['claimed_symbol_count']==0


def test_later_poll_does_not_replace_an_earlier_failure_or_its_original_quote(tmp_path):
    from liangjian_funnel.runtime.opening_evidence import capture_scheduled_opening_evidence
    config=settings(tmp_path)
    bad=capture_scheduled_opening_evidence(config,calendar=Calendar(),clock=lambda:AT.replace(second=3),
        fetch=lambda symbols:quote('2026-10-16T09:24:59+08:00'))
    before=Path(bad['evidence_path']).read_bytes()
    good=capture_scheduled_opening_evidence(config,calendar=Calendar(),clock=lambda:AT.replace(second=8),
        fetch=lambda symbols:quote())
    assert bad['status']=='DATA_LIMITED' and good['status']=='CAPTURED'
    assert Path(bad['evidence_path']).read_bytes()==before and bad['evidence_path']!=good['evidence_path']
