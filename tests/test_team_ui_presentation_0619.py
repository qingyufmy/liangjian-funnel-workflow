"""Reader-only UI evidence: local SQLite fixtures, no VM/network/model calls."""
from contextlib import closing
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / 'scripts'
sys.path.insert(0, str(SCRIPTS))
from team_ui_presentation import apply, condition_basis, decorate_names, digest, receipt
from team_ui_readonly import connect


def fixture(tmp_path):
    root = tmp_path.resolve(); (root / 'state/team_v2').mkdir(parents=True)
    source = dict(id='plan-270', table='execution_plans', sha256='a' * 64)
    original = dict(name='华明装备', stop_level='7.30')
    with closing(sqlite3.connect(root / 'state/workflow.sqlite3')) as db, db:
        db.execute('CREATE TABLE execution_plans(plan_id PRIMARY KEY,symbol,status,payload_json)')
        db.execute('INSERT INTO execution_plans VALUES(?,?,?,?)', ('plan-270', '002270.SZ', 'EXPIRED', json.dumps(original)))
        db.execute('CREATE TABLE monitor_events(event_id PRIMARY KEY,minute_end,payload_json)')
        conditions = {f'COND_{i}': dict(status='MET' if i == 0 else 'UNMET') for i in range(7)}
        conditions['ENTRY_RR'] = dict(status='MET', value=47.49999999999887,
            source_as_of='2026-10-22T14:55:00+08:00', threshold=dict(stop=7.30, target=8.27, minimum=2.5))
        body = dict(plan_id='plan-354', strategy=dict(as_of='2026-10-22T14:56:00+08:00',
            closed_5m_end='2026-10-22T14:55:00+08:00', entry_confirmation=dict(current_entry_close=7.32,
                required_conditions=list(conditions)[:7], conditions=conditions)))
        db.execute('INSERT INTO monitor_events VALUES(?,?,?)', ('event-354', '2026-10-22T14:57:00+08:00', json.dumps(body)))
    body = dict(schema='team-ui-presentation-source/0619-v1', producer='PINNED_READ_ONLY_OPS', trade_date='2026-10-22',
        security_names={'002270.SZ': dict(name='华明装备', source=source, identity_only=True)},
        plan_data_status={'600201.SH': dict(status='PARTIAL', label='缺少前日5分钟历史', source=source),
            '002354.SZ': dict(status='PARTIAL', label='等待条件；历史覆盖待核', source=source),
            '300556.SZ': dict(status='AVAILABLE', label='等待条件（当轮行情原件）', source=source)},
        risk_limits=dict(values=dict(max_new_entries=3,max_cluster_signals=2,min_cash_reserve_pct='0.10',
            max_risk_pct='0.05',standard_risk_pct='0.03'),source=source),
        market=dict(status='FROZEN_CLOSE_OBSERVATION_NOT_REALTIME',
            emotion=dict(stage='ICE_POINT',stage_label='冰点期',observed_at='2026-10-22T15:48:37.984000+08:00',source=source,
                         advances=535,declines=4998,limit_up_count=33,limit_down_count=55),
            indices=[dict(key='SSE',value=None,status='NOT_INTEGRATED'),dict(key='LIMIT_UP',value=33,source=source)]))
    path = root / 'state/team_v2/ui_presentation_source.json'
    path.write_text(json.dumps(dict(body=body,body_sha256=digest(body)),ensure_ascii=False),encoding='utf-8')
    return root, body, path


def test_expired_plan_identity_is_not_eligibility_and_all_rows_keep_name_provenance(tmp_path):
    root, _, _ = fixture(tmp_path)
    rows = [dict(plan_id='plan-270',symbol='002270.SZ',status='FILLED'),
            dict(plan_id='plan-270',code='002270.SZ',qty=0)]
    decorate_names(root, rows, connect)
    assert all(r['name']=='华明装备' and r['name_source']['id']=='plan-270' for r in rows)
    assert rows[0]['status']=='FILLED' and rows[1]['qty']==0
    assert all('eligible' not in r for r in rows)


def test_different_security_cannot_borrow_plan_name(tmp_path):
    root, _, _ = fixture(tmp_path); rows=[dict(symbol='600201.SH',plan_id='plan-270')]
    decorate_names(root,rows,connect)
    assert rows[0]['name'] is None and rows[0]['name_missing_reason']=='NO_BOUND_PLAN_IDENTITY'


def test_confirmation_and_rr_are_same_original_event_not_day_best_or_display_rounding(tmp_path):
    root, _, _ = fixture(tmp_path)
    plan=dict(id='plan-354',rr=47.49999999999887,conditions=[dict(code='ENTRY_RR',source_id='event-354')])
    condition_basis(root,'2026-10-22',plan,connect)
    assert plan['rr']==47.49999999999887
    assert plan['rr_basis']['price']==7.32 and plan['rr_basis']['stop']==7.30 and plan['rr_basis']['target']==8.27
    assert plan['rr_basis']['observed_at']=='2026-10-22T14:55:00+08:00'
    assert plan['confirmation_basis']['decision_at']=='2026-10-22T14:57:00+08:00'
    assert plan['confirmation_basis']['met_count']==1 and plan['confirmation_basis']['required_count']==7


def test_bound_metadata_adds_limits_names_and_partial_data_without_changing_raw_research_flags(tmp_path):
    root, _, _ = fixture(tmp_path)
    plans=[dict(id='plan-'+s,code=s,data_limits=dict(data_limited=None)) for s in ('600201.SH','002354.SZ','300556.SZ')]
    out=apply(root,'2026-10-22',dict(sections={'plans':{'rows':plans},'orders':{'rows':[dict(symbol='002270.SZ',plan_id='plan-270')]}},admissions=[],risk={}),connect)
    assert [p['data_status']['status'] for p in plans]==['PARTIAL','PARTIAL','AVAILABLE']
    assert all(p['data_limits']['data_limited'] is None for p in plans)
    assert out['risk']['max_open']==3 and out['risk']['max_cluster']==2
    assert out['risk']['min_cash_pct']==10 and out['risk']['max_risk_pct']==5 and out['risk']['standard_risk_pct']==3
    assert out['market']['emotion']['stage_label']=='冰点期' and out['market']['indices'][0]['value'] is None
    assert out['sections']['orders']['rows'][0]['name']=='华明装备'


@pytest.mark.parametrize('kind',['missing','sha','wrong-day','unknown-source'])
def test_missing_or_mismatched_receipt_never_infers_complete_or_index_quote(tmp_path,kind):
    root,body,path=fixture(tmp_path)
    if kind=='missing':path.unlink()
    else:
        if kind=='wrong-day':body['trade_date']='2026-10-21'
        if kind=='unknown-source':body['plan_data_status']['600201.SH']['source']={}
        path.write_text(json.dumps(dict(body=body,body_sha256='0'*64 if kind=='sha' else digest(body))),encoding='utf-8')
    plan=dict(id='missing',code='600201.SH',status='ACTIVE_TODAY')
    result=apply(root,'2026-10-22',dict(sections={'plans':dict(rows=[plan])},risk={}),connect)
    assert plan['data_status']['status']=='UNPROVEN'
    if kind!='unknown-source':
        assert result['market']['emotion'] is None
        assert all(x['value'] is None for x in result['market']['indices'])


def test_reader_leaves_database_bytes_and_directory_entries_unchanged(tmp_path):
    root, _, _ = fixture(tmp_path)
    before={p.relative_to(root).as_posix():digest(p.read_bytes()) for p in root.rglob('*') if p.is_file()}
    plan=dict(id='plan-354',code='002354.SZ',conditions=[dict(source_id='event-354')])
    apply(root,'2026-10-22',dict(sections={'plans':dict(rows=[plan])},risk={}),connect)
    after={p.relative_to(root).as_posix():digest(p.read_bytes()) for p in root.rglob('*') if p.is_file()}
    assert before==after
