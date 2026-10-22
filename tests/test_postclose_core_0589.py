from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import timedelta
import json
import sqlite3
import pytest

from liangjian_funnel.runtime.shared_json import merge_shared_json
from liangjian_funnel.runtime.integrity import verify_integrity,marker_path
from liangjian_funnel.runtime.state import RuntimeStore
from liangjian_funnel.runtime.provider_health import observe_http
from liangjian_funnel.settings import Settings
from test_entry_confirmation import b_rows,plan,run,history,bar,DAY,daily


def test_shared_state_short_owned_update_keeps_all_other_long_fields(tmp_path):
    path=tmp_path/'state.json';path.write_text(json.dumps({'claude':'x'*10000,'codex_monitor':{'old':'y'*10000}}))
    merge_shared_json(path,{'codex_monitor':{'ok':True}},allowed_prefix='codex_')
    assert json.loads(path.read_bytes())=={'claude':'x'*10000,'codex_monitor':{'ok':True}}
    assert not list(tmp_path.glob('*.tmp')) and not list(tmp_path.glob('*.lock'))


def test_shared_state_cooperating_writers_keep_each_others_fields(tmp_path):
    path=tmp_path/'state.json';path.write_text('{}')
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda i:merge_shared_json(path,{'codex_'+str(i):i},allowed_prefix='codex_'),range(20)))
    assert len(json.loads(path.read_bytes()))==20
    before=path.read_bytes()
    with pytest.raises(ValueError,match='UNOWNED'):merge_shared_json(path,{'claude':{}},allowed_prefix='codex_')
    assert before==path.read_bytes()
    path.write_bytes(b'{}tail')
    with pytest.raises(ValueError):merge_shared_json(path,{'codex_x':1})
    assert path.read_bytes()==b'{}tail'


def test_health_hot_path_never_scans_and_schema_change_requires_maintenance(tmp_path,monkeypatch):
    path=tmp_path/'workflow.db';store=RuntimeStore(path)
    import liangjian_funnel.runtime.integrity as module
    monkeypatch.setattr(module,'verify_integrity',lambda *a,**kw:pytest.fail('full scan in hot path'))
    assert RuntimeStore(path).healthy
    with sqlite3.connect(path) as db:db.execute('CREATE TABLE changed(x)')
    assert not RuntimeStore(path).healthy
    monkeypatch.undo();verify_integrity(path)
    assert RuntimeStore(path).healthy
    marker_path(path).unlink()
    assert not RuntimeStore(path).healthy


def test_b_no_breakout_is_waiting_not_missing_but_true_prefix_gap_stays_blocked():
    rows=b_rows();p=plan(low=100.2,high=100.8)
    facts=daily();facts['previous_day_high']['value']=101.8
    # Actual prices never exceed the effective key; all required source rows exist.
    for row in rows[20:]:row.update(open=101,high=101.2,low=100.9,close=101.1,volume=100)
    out=run(p,rows,facts=facts)
    assert out['pattern']=='B_BREAKOUT_RETEST' and out['status']=='WAITING_CONDITION'
    assert out['conditions']['B_EFFECTIVE_BREAKOUT']['status']=='UNMET'
    for k in ('B_RETEST','B_RETEST_VOLUME','B_ACCEPTED','B_STOP_ONLY_RAISED','ENTRY_RR'):
        assert out['conditions'][k]['status']=='WAITING_CONDITION'
    cold=run(p,[bar(DAY+timedelta(minutes=5*(i+1)),101.1,100) for i in range(24)],facts=facts)
    assert cold['status']=='DATA_LIMITED' and cold['conditions']['B_ATTEMPT_PREFIX']['status']=='UNPROVEN'


def test_provider_failures_isolated_and_alerted_once_per_episode(tmp_path,monkeypatch):
    from liangjian_funnel.runtime import provider_health as health
    settings=Settings.from_env({},root=tmp_path);alerts=[]
    monkeypatch.setattr(health,'queue_internal',lambda *a,**kw:alerts.append(kw))
    def emit(role,status):return observe_http(settings,model='deepseek-v4.1-flash',role=role,status=status)
    assert emit('A2',403)['consecutive_failures']==1 and not alerts
    assert emit('A4',200)['consecutive_failures']==0
    emit('A2',403);emit('A2',503);assert len(alerts)==1
    emit('A2',200);emit('A2',429);emit('A2',429);assert len(alerts)==2
    assert all('sk-' not in row['text'] for row in alerts)


@pytest.mark.parametrize('dies_after_exit',[False,True])
def test_release_checks_lifetime_after_unit_exit_not_its_exit_code(dies_after_exit):
    from liangjian_funnel.runtime.release_lifecycle import verify_after_unit_exit
    clock=[0.];observations=[]
    def observe():
        observations.append(clock[0])
        return dict(node_pid=123 if not dies_after_exit or clock[0]<20 else None,
                    health_status=200,business_hashes={'plans':'unchanged'})
    kwargs=dict(unit_active=lambda:clock[0]<5,observe=observe,before={'plans':'unchanged'},
        sleep=lambda n:clock.__setitem__(0,clock[0]+n),monotonic=lambda:clock[0])
    if dies_after_exit:
        with pytest.raises(ValueError,match='POST_UNIT'):verify_after_unit_exit(**kwargs)
    else:
        result=verify_after_unit_exit(**kwargs)
        assert result['post_unit_elapsed_seconds']>=60 and observations[-1]>=65
