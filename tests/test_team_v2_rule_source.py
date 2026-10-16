from pathlib import Path
from types import SimpleNamespace
from datetime import datetime,timedelta
from zoneinfo import ZoneInfo
import json
import shutil

import pytest

from liangjian_funnel.runtime.team_v2_rule_source import prepare_daily_rule_source
from liangjian_funnel.runtime.shadow_rule_preflight import load_approved_rule_archive

ROOT=Path(__file__).resolve().parents[1]
NOW=datetime(2026,10,16,8,33,tzinfo=ZoneInfo('Asia/Shanghai'))
CAL=SimpleNamespace(is_trading_day=lambda _:True)


def fixture(tmp_path):
    relative='artifacts/wp5-20261010/official-rules-20261010'
    shutil.copytree(ROOT/relative,tmp_path/relative)
    return SimpleNamespace(root=tmp_path),load_approved_rule_archive(tmp_path/relative)


def test_three_actual_formal_parser_inputs_frozen_once_no_recall_or_source_pin_changes(tmp_path):
    settings,approved=fixture(tmp_path);seen=[]
    def fetch(url,remaining):
        seen.append(url)
        return SimpleNamespace(url=url,status_code=200,content=next(d.raw_response for d in approved.documents if d.url==url))
    first=prepare_daily_rule_source(settings,clock=lambda:NOW,calendar=CAL,fetch_response=fetch)
    assert first['status']=='RULE_VERSION_CONFIRMED' and first['source_requests_started']==3 and len(seen)==3
    raw=Path(first['receipt_path']).read_bytes()
    second=prepare_daily_rule_source(settings,clock=lambda:NOW+timedelta(minutes=1),calendar=CAL,fetch_response=fetch)
    assert second['status']=='RULE_VERSION_CONFIRMED' and second['reused'] and second['source_requests_started']==0
    assert len(seen)==3 and Path(first['receipt_path']).read_bytes()==raw
    receipt=json.loads(raw)
    assert receipt['rule_constant_modified'] is False and receipt['latest_revision_absence_proven'] is False


def test_missing_archive_does_not_call_any_source_or_make_a_fake_confirmed_file(tmp_path):
    seen=[];settings=SimpleNamespace(root=tmp_path)
    result=prepare_daily_rule_source(settings,clock=lambda:NOW,calendar=CAL,fetch_response=lambda *a:seen.append(a))
    assert result['reason_code']=='RULE_APPROVED_ARCHIVE_UNAVAILABLE' and not seen
    assert not (tmp_path/'state/team_v2/rules/2026-10-16.json').exists()


@pytest.mark.parametrize('failure',['changed','redirect','transport','interrupted','tampered'])
def test_unapproved_or_interrupted_attempt_stays_blocked_and_is_not_silently_retried(tmp_path,failure):
    settings,approved=fixture(tmp_path);directory=tmp_path/'state/team_v2/rules';seen=[]
    def fetch(url,remaining):
        seen.append(url)
        if failure=='transport':raise OSError('PRIVATE_NEVER_LOG')
        raw=next(d.raw_response for d in approved.documents if d.url==url)
        return SimpleNamespace(url=url if failure!='redirect' else 'https://example.com/',
            status_code=200,content=raw if failure!='changed' else b'unknown-version')
    if failure=='interrupted':
        directory.mkdir(parents=True);(directory/'2026-10-16.claim.json').write_text('{}')
    result=prepare_daily_rule_source(settings,clock=lambda:NOW,calendar=CAL,fetch_response=fetch)
    if failure=='tampered':
        assert result['status']=='RULE_VERSION_CONFIRMED'
        (directory/'2026-10-16.json').write_text('{}');seen.clear()
    count=len(seen)
    again=prepare_daily_rule_source(settings,clock=lambda:NOW,calendar=CAL,fetch_response=fetch)
    assert again['status']=='DATA_LIMITED' and len(seen)==count
    assert 'PRIVATE_NEVER_LOG' not in json.dumps(again)


def test_after0926_and_nontrading_do_not_issue_requests(tmp_path):
    settings=SimpleNamespace(root=tmp_path);seen=[]
    for at,calendar in ((NOW.replace(hour=9,minute=26),CAL),(NOW,SimpleNamespace(is_trading_day=lambda _:False))):
        result=prepare_daily_rule_source(settings,clock=lambda:at,calendar=calendar,fetch_response=lambda *a:seen.append(a))
        assert result['status']=='DATA_LIMITED' and not seen


def test_monday_is_blocked_by_existing_planned_due_not_silently_extended(tmp_path):
    settings,approved=fixture(tmp_path);seen=[]
    result=prepare_daily_rule_source(settings,clock=lambda:NOW.replace(day=19),calendar=CAL,
        fetch_response=lambda *a:seen.append(a))
    assert result['status']=='DATA_LIMITED' and not seen
    assert result['reason_codes']==['RULE_REVALIDATION_PLAN_WINDOW_EXCEEDED']


def test_actual_morning_engine_calls_preflight_once_without_confusing_planning_with_entry(tmp_path,monkeypatch):
    from test_team_v2_morning import fixture as morning_fixture,source_bytes,CAL as morning_calendar
    from liangjian_funnel.runtime.team_v2_morning import run_morning_stage
    import liangjian_funnel.runtime.team_v2_rule_source as module
    settings,clock=morning_fixture(tmp_path);seen=[];before=source_bytes(settings)
    def unavailable(*args,**kwargs):
        seen.append(kwargs['clock']())
        return dict(status='DATA_LIMITED',reason_code='RULE_REVALIDATION_PLAN_WINDOW_EXCEEDED')
    monkeypatch.setattr(module,'prepare_daily_rule_source',unavailable)
    first=run_morning_stage(settings,'engine',clock=lambda:clock.now,calendar=morning_calendar)
    second=run_morning_stage(settings,'engine',clock=lambda:clock.now,calendar=morning_calendar)
    assert first==second and first['status']=='COMPLETE' and len(seen)==1
    assert first['rule_preflight']['status']=='DATA_LIMITED'
    assert first['authority']=='PLANNING_ONLY_NOT_EXECUTION_PERMISSION' and source_bytes(settings)==before
