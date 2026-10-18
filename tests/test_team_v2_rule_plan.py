from copy import deepcopy
from datetime import timedelta,date
import json
from pathlib import Path
from types import SimpleNamespace
import shutil
import time

import pytest

from test_team_v2_rule_source import fixture,ROOT,NOW,CAL
from liangjian_funnel.runtime.team_v2_rule_plan import load_plan,validate_plan_receipt
from liangjian_funnel.runtime.team_v2_rule_source import prepare_daily_rule_source
from liangjian_funnel.runtime.shadow_rule_preflight import PLANNED_DUE,confirm_rule_version
from liangjian_funnel.evaluation.ablation.price_limits import RULE_REVIEWED_THROUGH


def setup(tmp_path):
    settings,archive=fixture(tmp_path);(tmp_path/'config').mkdir()
    shutil.copyfile(ROOT/'config/rule_revalidation_plan.yaml',tmp_path/'config/rule_revalidation_plan.yaml')
    return settings,archive,load_plan(tmp_path/'config/rule_revalidation_plan.yaml')


def test_monday_three_originals_finite_renewal_replay_and_unchanged_old_contract(tmp_path):
    settings,archive,plan=setup(tmp_path);seen=[];now=NOW.replace(day=19)
    def fetch(url,remaining):
        seen.append(url)
        return SimpleNamespace(url=url,status_code=200,content=next(d.raw_response for d in archive.documents if d.url==url))
    result=prepare_daily_rule_source(settings,clock=lambda:now,calendar=CAL,fetch_response=fetch)
    assert result['status']=='RULE_VERSION_CONFIRMED' and len(seen)==3
    assert result['annotation_only'] and not result['blocks_new_entries']
    receipt=json.loads(Path(result['receipt_path']).read_bytes())
    assert validate_plan_receipt(archive,receipt,plan=plan,at=now)['status']=='RULE_VERSION_CONFIRMED'
    again=prepare_daily_rule_source(settings,clock=lambda:now+timedelta(minutes=1),calendar=CAL,fetch_response=fetch)
    assert again['reused'] and len(seen)==3
    assert PLANNED_DUE==date(2026,10,16) and RULE_REVIEWED_THROUGH==date(2026,10,10)
    assert confirm_rule_version(archive,[],trade_date='2026-10-19',known_at=now)['reason_codes']==['RULE_REVALIDATION_PLAN_WINDOW_EXCEEDED']


@pytest.mark.parametrize('defect',['body','transport','redirect','late','expired'])
def test_only_actual_changed_body_blocks_not_unavailable_or_expired_plan(tmp_path,defect):
    settings,archive,plan=setup(tmp_path);seen=[]
    now=NOW.replace(day=19) if defect!='expired' else NOW.replace(month=11,day=2)
    def fetch(url,remaining):
        seen.append(url)
        if defect=='transport':raise OSError('DO_NOT_EXPOSE_PRIVATE')
        return SimpleNamespace(url=url if defect!='redirect' else 'https://unapproved.example/',status_code=200,
            content=next(d.raw_response for d in archive.documents if d.url==url)+(b'changed' if defect=='body' else b''))
    if defect=='late':now=now.replace(hour=9,minute=26)
    result=prepare_daily_rule_source(settings,clock=lambda:now,calendar=CAL,fetch_response=fetch)
    assert result['status']==('BLOCK' if defect=='body' else 'DATA_LIMITED')
    assert result.get('blocks_new_entries',False)==(defect=='body')
    count=len(seen)
    repeated=prepare_daily_rule_source(settings,clock=lambda:now,calendar=CAL,fetch_response=fetch)
    assert repeated['status']==result['status'] and len(seen)==count
    assert 'DO_NOT_EXPOSE_PRIVATE' not in json.dumps(repeated)
    if defect in ('late','expired'):assert not seen


def test_plan_cannot_silently_extend_or_receipt_rewrite(tmp_path):
    settings,archive,plan=setup(tmp_path);path=tmp_path/'config/rule_revalidation_plan.yaml'
    path.write_text(path.read_text().replace('2026-10-30','2026-11-30'))
    with pytest.raises(ValueError,match='RULE_RENEWAL_AUTHORIZATION_INVALID'):load_plan(path)
    seen=[];result=prepare_daily_rule_source(settings,clock=lambda:NOW.replace(day=19),calendar=CAL,
        fetch_response=lambda *a:seen.append(a))
    assert result['reason_code']=='RULE_RENEWAL_AUTHORIZATION_INVALID' and not seen


def test_uncooperative_request_cannot_continue_or_publish_late(tmp_path):
    settings,archive,plan=setup(tmp_path);seen=[]
    def slow(url,remaining):
        seen.append(url);time.sleep(.06)
        return SimpleNamespace(url=url,status_code=200,content=archive.documents[0].raw_response)
    result=prepare_daily_rule_source(settings,clock=lambda:NOW.replace(day=19),calendar=CAL,
        fetch_response=slow,budget_seconds=.01)
    assert result['status']=='DATA_LIMITED'
    time.sleep(.09)
    assert len(seen)==1 and not (tmp_path/'state/team_v2/rules/2026-10-19.json').exists()


def test_first_changed_body_is_frozen_and_alerted_without_waiting_for_failed_second(tmp_path):
    settings,archive,plan=setup(tmp_path);seen=[]
    def fetch(url,remaining):
        seen.append(url)
        if len(seen)>1:raise OSError('later source unavailable')
        return SimpleNamespace(url=url,status_code=200,content=archive.documents[0].raw_response+b'changed')
    result=prepare_daily_rule_source(settings,clock=lambda:NOW.replace(day=19),calendar=CAL,fetch_response=fetch)
    assert result['status']=='BLOCK' and len(seen)==1
    root=tmp_path/'state/team_v2/rules'
    receipt=json.loads((root/'2026-10-19.observation.json').read_bytes())
    alert=json.loads((root/'2026-10-19.alert.json').read_bytes())
    assert receipt['rows'][0]['raw_response_base64'] and alert['delivery_status']=='PENDING'
    assert alert['audience']=='INTERNAL_ONLY' and alert['receipt_sha256']==receipt['receipt_sha256']


@pytest.mark.parametrize('changed_body',[False,True])
def test_unavailable_annotation_does_not_block_control_but_changed_body_does(tmp_path,monkeypatch,changed_body):
    from test_team_v2_monitor_dispatch import prepared
    from liangjian_funnel.runtime.team_v2_monitor import run_monitor_control
    from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore,canonical
    import sqlite3
    app,clock,seen,api,proof=prepared(tmp_path,monkeypatch)
    path=app.settings.root/'state/team_v2/team.db'
    with sqlite3.connect(path) as db,db:
        raw=json.loads(db.execute('SELECT body_json FROM morning_engine').fetchone()[0])
        assert raw['rule_preflight']['status']=='DATA_LIMITED'
        if changed_body:
            raw['rule_preflight']={'status':'BLOCK','blocks_new_entries':True,
                'reason_codes':['RULE_VERSION_CHANGED_NO_AUTO_ACCEPT']}
            db.execute('UPDATE morning_engine SET body_json=?',(canonical(raw),))
    result=run_monitor_control(app,now=clock.now,clock=lambda:clock.now,client=api,tradability_loader=proof)
    if changed_body:
        # 0308: an ineligible engine envelope is not sent to the model at all.
        assert result['result']['trader']['model_validated'] is None and not seen
        assert result['result']['trader']['admissions']==[]
        with sqlite3.connect(app.settings.root/'state/team_v2/paper.db') as db:
            assert db.execute('SELECT count(*) FROM paper_orders').fetchone()[0]==0
    else:
        assert result['result']['trader']['model_validated'] and len(seen)==1
        assert result['result']['trader']['admissions'][0]['dispatched']
