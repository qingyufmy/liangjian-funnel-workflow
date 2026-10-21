from datetime import datetime
from types import SimpleNamespace as NS
import json
import pytest
from liangjian_funnel.runtime.state import RuntimeStore,StateTransitionError
from liangjian_funnel.runtime.close_regeneration import bind_close_regeneration,require_raw_research_pass
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar

NOW=datetime.fromisoformat('2026-10-21T23:00:00+08:00')
EXP=datetime.fromisoformat('2026-10-22T15:00:00+08:00')
HEAD='a'*40


def row(key,run='old'):
    return dict(plan_id=key,lane_id='lane_1',symbol='002354.SZ',status='PENDING_MORNING_REVIEW',
        expires_at=EXP,payload={'source_run_id':run,'risk_unit':'PROBE','trigger_low':10,'trigger_high':11,'stop_level':9})


def app_fixture(tmp_path):
    store=RuntimeStore(tmp_path/'state.db');store.publish_plan_batch([row(str(i)) for i in range(3)])
    out=tmp_path/'outputs';(out/'runs').mkdir(parents=True)
    source=dict(run_id='old',market_trade_date='2026-10-21',status='READY',slot='close',
        plan_publication={'created':['0','1','2']})
    (out/'runs/old.json').write_text(json.dumps(source))
    generation=NS(as_of=NOW.replace(hour=18),snapshot_id='fresh-a1',snapshot_hash='b'*64)
    app=NS(store=store,settings=NS(workflow_output_dir=out,research_primary_lane_id='lane_1'),
        trading_calendar=ExchangeTradingCalendar(),a1_registry=NS(require_active=lambda **_:generation))
    return app,generation,source


def test_one_transaction_retires_exact_old_batch_with_audit_not_old_payload_mutation(tmp_path):
    app,_,_=app_fixture(tmp_path);proof=bind_close_regeneration(app,'old',now=NOW,head=HEAD)
    before=app.store.get_execution_plan('0')['payload_json']
    new=[row('new1',proof.run_id),row('new2',proof.run_id)]
    result=app.store.publish_plan_batch(new,supersede_pending_plan_ids=proof.old_plan_ids,
        supersession_run_id=proof.run_id)
    assert len(result)==2
    assert all(app.store.get_execution_plan(str(i))['status']=='SUPERSEDED' for i in range(3))
    assert app.store.get_execution_plan('0')['payload_json']==before
    assert {r['plan_id'] for r in app.store.list_execution_plans(status='PENDING_MORNING_REVIEW')}=={'new1','new2'}
    with app.store._connect() as c:
        assert [r['reason'] for r in c.execute('SELECT reason FROM plan_supersessions')]==['post-fix regeneration']*3


def test_concurrent_pending_scope_change_rolls_back_old_and_new(tmp_path):
    app,_,_=app_fixture(tmp_path);proof=bind_close_regeneration(app,'old',now=NOW,head=HEAD)
    app.store.publish_plan_batch([row('concurrent','another')])
    with pytest.raises(StateTransitionError,match='REGEN_PENDING_SCOPE_CHANGED'):
        app.store.publish_plan_batch([row('new',proof.run_id)],supersede_pending_plan_ids=proof.old_plan_ids,
            supersession_run_id=proof.run_id)
    assert app.store.get_execution_plan('new') is None
    assert all(app.store.get_execution_plan(str(i))['status']=='PENDING_MORNING_REVIEW' for i in range(3))


def test_empty_batch_never_retires_existing_three(tmp_path):
    app,_,_=app_fixture(tmp_path)
    with pytest.raises(StateTransitionError,match='REGEN_BATCH_CONTRACT_INVALID'):
        app.store.publish_plan_batch([],supersede_pending_plan_ids=['0','1','2'],supersession_run_id='regen')
    assert len(app.store.list_execution_plans(status='PENDING_MORNING_REVIEW'))==3


def test_new_batch_identity_conflict_keeps_all_old(tmp_path):
    app,_,_=app_fixture(tmp_path);app.store.publish_plan_batch([row('new','first')])
    with pytest.raises(StateTransitionError,match='PLAN_ID_CONTENT_CONFLICT'):
        app.store.publish_plan_batch([row('new','second')],supersede_pending_plan_ids=['0','1','2'],supersession_run_id='second')
    assert all(app.store.get_execution_plan(str(i))['status']=='PENDING_MORNING_REVIEW' for i in range(3))


def test_old_a1_or_cross_day_or_changed_batch_cannot_start_regeneration(tmp_path):
    app,generation,source=app_fixture(tmp_path)
    generation.as_of=NOW.replace(day=16)
    with pytest.raises(ValueError,match='REGEN_FRESH_A1_REQUIRED'):bind_close_regeneration(app,'old',now=NOW,head=HEAD)
    generation.as_of=NOW.replace(hour=18)
    midnight=bind_close_regeneration(app,'old',now=NOW.replace(day=22,hour=0),head=HEAD)
    assert midnight.trade_date=='2026-10-21' and midnight.target_trade_date=='2026-10-22'
    assert midnight.wall_clock.date().isoformat()=='2026-10-22'
    with pytest.raises(ValueError,match='CLOSE_RESEARCH_PUBLICATION_DEADLINE_REACHED'):
        bind_close_regeneration(app,'old',now=NOW.replace(day=22,hour=6,minute=30),head=HEAD)
    app.store.publish_plan_batch([row('extra')])
    with pytest.raises(ValueError,match='REGEN_SOURCE_PENDING_SCOPE_CHANGED'):bind_close_regeneration(app,'old',now=NOW,head=HEAD)


@pytest.mark.parametrize('history,expected',[([['A2_ROTATION_DOMAIN_MISMATCH']],False),([],False),([[]],True),([['bad'],[]],True)])
def test_normalized_a2_stage_is_not_a_raw_pass(history,expected):
    stage=NS(stage='A2',status='VALIDATED',diagnostics={'provider_validation_history':history})
    result=NS(status='READY',lanes=[NS(lane='lane_1',status='READY',stages=[stage])])
    assert require_raw_research_pass(result,'lane_1') is expected
