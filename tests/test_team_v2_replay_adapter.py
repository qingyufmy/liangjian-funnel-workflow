from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
import hashlib
import json
import sqlite3
from types import SimpleNamespace

import pytest

from liangjian_funnel.evaluation.team_v2_replay_adapter import FrozenReplayAdapter
from liangjian_funnel.evaluation.team_v2_replay_materializer import FrozenMinuteMaterializer
from liangjian_funnel.evaluation.team_v2_replay_source import sha
from liangjian_funnel.runtime.team_v2_sources import stamp
from test_team_v2_replay_materializer import source


def fixture(tmp_path):
    path,_,rows,batch=source(tmp_path)
    raw=dict(symbol='000001.SZ',cluster_label='BANK',stop_level=9,trigger_low=9.8,
        trigger_high=10.1,no_chase=10.5)
    row=rows['p']
    row.update(payload_json=json.dumps(raw),status='ACTIVE_TODAY',
        valid_from='2026-10-15T09:26:00+08:00',expires_at='2026-10-15T15:00:00+08:00')
    with sqlite3.connect(path) as db:
        db.execute('ALTER TABLE execution_plans ADD COLUMN status TEXT')
        db.execute('ALTER TABLE execution_plans ADD COLUMN valid_from TEXT')
        db.execute('ALTER TABLE execution_plans ADD COLUMN expires_at TEXT')
        db.execute('UPDATE execution_plans SET payload_json=?,status=?,valid_from=?,expires_at=?',
            tuple(row[key] for key in ('payload_json','status','valid_from','expires_at')))
    frame=batch['source_frames'][0];frame['original_plan_row_sha256']=sha(row)
    frame['frame_sha256']=sha({k:v for k,v in frame.items() if k!='frame_sha256'})
    pin=hashlib.sha256(path.read_bytes()).hexdigest()
    reader=FrozenMinuteMaterializer(path,expected_sha256=pin,plans=rows,settings=SimpleNamespace())
    clock=[stamp(batch['decision_at'])];positions=[]
    broker=SimpleNamespace(clock=lambda:clock[0],get_positions=lambda:tuple(positions))
    store=object()
    adapter=FrozenReplayAdapter(reader,broker=broker,store=store,clock=lambda:clock[0],account_id='paper')
    return reader,batch,adapter,broker,store,clock,positions,path,pin


def test_real_ro_source_to_typed_coordinator_no_meeting_no_model_no_source_change(tmp_path):
    reader,batch,adapter,b,s,_,_,path,pin=fixture(tmp_path)
    with reader:
        result=adapter(batch,b,s)
    plan=result['session_kwargs']['all_plans']['p']
    assert plan.price==Decimal('10') and plan.cluster=='BANK'
    assert plan.status=='ACTIVE_TODAY' and result['closed_bars']['000001.SZ'].tradability_known
    assert result['model_enabled'] is False and 'envelopes' not in result['session_kwargs']
    assert result['source_audit']['model_edge']=='UNAVAILABLE_NO_CALL'
    assert hashlib.sha256(path.read_bytes()).hexdigest()==pin


def test_terminal_source_not_reactivated_and_unknown_geometry_stays_limited(tmp_path):
    reader,batch,adapter,b,s,*_=fixture(tmp_path)
    batch['entry_source_frames']=[]
    seen=[]
    adapter.session_factory=lambda context:seen.append(context) or dict(
        session_kwargs=dict(active_plan_ids=set(),regime='DATA_BLOCK',live_market_ready=False,
            trade_date=batch['trade_date'],envelopes={}),model_enabled=False)
    with reader:
        with pytest.raises(ValueError,match='ENVELOPE_BINDING'):adapter(batch,b,s)
    assert seen[0]['active_plan_ids']==set()  # no invented entry permission


def edge(context,day):
    from liangjian_funnel.runtime.risk_engine import RiskEnvelope
    envelopes={pid:RiskEnvelope(pid,p.symbol,day,False,'NONE',Decimal(0),1,0,
        ('REPLAY_PM_AND_A6_UNAVAILABLE',),()) for pid,p in context['all_plans'].items()}
    return dict(session_kwargs=dict(active_plan_ids=context['active_plan_ids'],
        trade_date=day,regime='DATA_BLOCK',live_market_ready=False,envelopes=envelopes),model_enabled=False)


@pytest.mark.parametrize('change',['clock','broker','active','regime','live','day','envelope','permission','override'])
def test_factory_cannot_upgrade_source_or_rebind_account(tmp_path,change):
    reader,batch,adapter,b,s,clock,*_=fixture(tmp_path)
    def factory(context):
        value=edge(context,batch['trade_date']);args=value['session_kwargs']
        if change=='active':args['active_plan_ids']={'invented'}
        elif change=='regime':args['regime']='TREND_MAINLINE'
        elif change=='live':args['live_market_ready']=True
        elif change=='day':args['trade_date']='2026-10-16'
        elif change=='envelope':args['envelopes']['p']=replace(args['envelopes']['p'],symbol='000002.SZ')
        elif change=='permission':value['model_enabled']=1
        elif change=='override':args['all_plans']={}
        return value
    adapter.session_factory=factory
    if change=='clock':clock[0]=stamp('2026-10-15T09:34:00+08:00')
    if change=='broker':b=SimpleNamespace(clock=lambda:clock[0])
    with reader,pytest.raises(ValueError,match='REPLAY_ADAPTER_'):adapter(batch,b,s)


def test_lost_current_held_source_keeps_owner_without_borrowing_old_bar_or_price(tmp_path):
    reader,batch,adapter,b,s,_,positions,*_=fixture(tmp_path)
    with reader:
        adapter(batch,b,s)
        positions.append(dict(plan_id='p',symbol='000001.SZ'))
        empty=deepcopy(batch);empty['source_frames']=[];empty['entry_source_frames']=[]
        empty['absent_original_plan_ids']=['p']
        result=adapter(empty,b,s)
    plan=result['session_kwargs']['all_plans']['p']
    assert not plan.data_ready and not plan.tradable and not result['closed_bars']
    assert result['source_audit']['limited'][-1]['reason_codes']==['REPLAY_HELD_CURRENT_SOURCE_MISSING']


def test_unknown_held_owner_blocks_model_factory_but_preserves_other_source(tmp_path):
    reader,batch,adapter,b,s,_,positions,*_=fixture(tmp_path)
    positions.append(dict(plan_id='foreign-held',symbol='000002.SZ'))
    adapter.session_factory=lambda context:pytest.fail('Missing owned geometry must not reach model edge')
    with reader:result=adapter(batch,b,s)
    assert result['source_audit']['missing_held_plan_ids']==['foreign-held']
    assert result['closed_bars'] and result['model_enabled'] is False


def test_valid_factory_only_connects_explicit_edge_does_not_grant_model_permission(tmp_path):
    reader,batch,adapter,b,s,*_=fixture(tmp_path)
    adapter.session_factory=lambda context:edge(context,batch['trade_date'])
    with reader:result=adapter(batch,b,s)
    assert result['session_kwargs']['active_plan_ids']=={'p'} and not result['model_enabled']
    assert result['session_kwargs']['envelopes']['p'].eligible is False
