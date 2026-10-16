from copy import deepcopy
from datetime import datetime,timedelta
import json
from pathlib import Path
import yaml
import pytest

from test_team_v2_morning import fixture,CAL
from liangjian_funnel.runtime.team_v2_morning import run_morning_stage
from liangjian_funnel.runtime.team_v2_external import assess_external,read_external,FIELDS
from liangjian_funnel.data.open_macro import _finalize_contract

NOW=datetime.fromisoformat('2026-10-19T08:33:00+08:00')
POLICY=yaml.safe_load((Path(__file__).resolve().parents[1]/'config/team_v2_deployment.yaml').read_text())['external']


def snapshot():
    return dict(available=True,as_of='2026-10-19T08:29:00+08:00',market_observations={key:
        dict(value=10 if key in ('vix','usd_cnh') else 0,as_of='2026-10-19T08:29:00+08:00',source_ref='FROZEN_TEST:'+key)
        for key in FIELDS})


def test_missing_external_is_minus_ten_pp_not_neutral(tmp_path):
    settings,clock=fixture(tmp_path)
    result=run_morning_stage(settings,'engine',clock=lambda:clock.now,calendar=CAL)
    assert result['baseline_range']==[40,65]
    assert result['engine_utilization_cap']==65
    assert result['external_adjustment']['reason_code']=='EXTERNAL_SNAPSHOT_MISSING'
    assert result['external_adjustment']['value_pp']==-10


@pytest.mark.parametrize('key,value,expected',[('nasdaq_change_pct',-1.5,-10),('a50_futures_change_pct',-1,-10),
    ('nasdaq_change_pct',-1.49,0),('a50_futures_change_pct',-0.99,0),('nasdaq_change_pct',5,0)])
def test_only_approved_contraction_no_invented_expansion(key,value,expected):
    data=snapshot();data['market_observations'][key]['value']=value
    assert assess_external(data,now=NOW,policy=POLICY)['value_pp']==expected


@pytest.mark.parametrize('kind',['absent','old_outer','old_item','missing_item','future','post0830','nan','bool','zero_vix','no_ref','wrong_type'])
def test_missing_stale_or_unproven_is_never_zero_adjustment(kind):
    data=snapshot()
    if kind=='absent':data=None
    elif kind=='old_outer':data['as_of']='2026-10-18T20:32:59+08:00'
    elif kind=='old_item':data['market_observations']['vix']['as_of']='2026-10-18T20:32:59+08:00'
    elif kind=='missing_item':del data['market_observations']['usd_cnh']
    elif kind=='future':data['as_of']='2026-10-19T09:00:00+08:00'
    elif kind=='post0830':data['as_of']='2026-10-19T08:31:00+08:00'
    elif kind=='nan':data['market_observations']['vix']['value']=float('nan')
    elif kind=='bool':data['market_observations']['vix']['value']=True
    elif kind=='zero_vix':data['market_observations']['vix']['value']=0
    elif kind=='no_ref':data['market_observations']['vix']['source_ref']=''
    elif kind=='wrong_type':data['market_observations']=[]
    result=assess_external(data,now=NOW,policy=POLICY)
    assert result['value_pp']==-10 and result['reason_code']=='EXTERNAL_SNAPSHOT_MISSING'


def test_twelve_hours_exact_is_allowed_and_missing_fields_of_actual_legacy_contract_not_guessed():
    data=snapshot();data['as_of']=(NOW-timedelta(hours=12)).isoformat()
    assert assess_external(data,now=NOW,policy=POLICY)['value_pp']==0
    legacy=dict(available=True,as_of='2026-10-19T08:29:00+08:00',
        values=dict(usd_momentum_percentile=0.5,fed_easing_probability_percentile=0.6,us_rate=4,cn_rate=2))
    assert assess_external(legacy,now=NOW,policy=POLICY)['value_pp']==-10


@pytest.mark.parametrize('tamper',[False,True])
def test_existing_cache_binding_rechecked_and_no_file_rewrite_or_collection(tmp_path,tamper):
    settings,_=fixture(tmp_path);directory=settings.open_macro_cache_dir;directory.mkdir(parents=True)
    body=_finalize_contract(dict(as_of='2026-10-19T08:29:00+08:00',cache_status='LIVE',GLOBAL_MACRO_SNAPSHOT=_finalize_contract(snapshot())))
    if tamper:body['GLOBAL_MACRO_SNAPSHOT']['market_observations']['vix']['value']=999
    path=directory/'open-macro-20261019.json';path.write_text(json.dumps(body),encoding='utf-8')
    before=path.read_bytes();result=read_external(settings,now=NOW,policy=POLICY)
    assert result['value_pp']==(-10 if tamper else 0) and result['source_requests_started']==0
    assert path.read_bytes()==before
