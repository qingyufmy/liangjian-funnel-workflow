"""D1 geometry, new publication proof and frozen v1 compatibility."""
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
import json
from pathlib import Path
import pytest
from liangjian_funnel.pipeline.team_v2_plan_geometry import (
    project_team_plan_geometry,project_stop_rr_geometry,GEOMETRY_FIELD,STOP_PUBLICATION_CONTRACT)

def test_minimum_geometry_widens_only_stop_and_derives_one_rr():
    raw=dict(symbol='002920.SZ',trigger_zone=dict(low=77.58,high=78.3558),stop_level=77.38,
             invalidation_level=77.38,first_resistance=90,minimum_reward_risk=2.5,reward_risk=999)
    before=deepcopy(raw);out=project_stop_rr_geometry(raw)
    p=out['payload'];assert out['status']=='PUBLISHABLE' and raw==before
    low,high,stop,target=map(lambda x:Decimal(str(x)),(77.58,78.3558,p['stop_level'],90))
    assert stop<=low*Decimal('.99') and stop<=high*Decimal('.98')
    assert p['stop_level']==p['invalidation_level'] and p['first_resistance']==90
    assert p['reward_risk']==float((target-high)/(high-stop))
    assert p[GEOMETRY_FIELD]['original_reward_risk']==999

def test_new_stop_cannot_invent_target_to_pass_rr():
    raw=dict(trigger_zone=dict(low=100,high=101),stop_level=99.9,
             first_resistance=105,minimum_reward_risk=2.5)
    assert project_team_plan_geometry(raw)['status']=='PUBLISHABLE'
    out=project_stop_rr_geometry(raw)
    assert out['status']=='OBSERVATION_ONLY' and out['payload'] is None
    assert out['audit']['reason_code']=='PLAN_MINIMUM_STOP_RR_INFEASIBLE'

def test_version_two_receipt_and_pm_recompute_and_reject_tampering():
    from test_team_v2_plan_geometry import projection_inputs
    from test_wp5_publication_receipt import build,bound
    from liangjian_funnel.runtime.publication_projection import capture_publication_projection
    from liangjian_funnel.runtime.team_v2_pm_geometry import pm_geometry_domain
    old=projection_inputs(20)
    # The unchanged v1 reader must still authenticate an old frozen publication.
    assert build(old)['evidence_complete']
    original=dict(old.normalized_payload.value,source_run_id=old.run_id)
    p=project_stop_rr_geometry(original)['payload']
    inputs=capture_publication_projection(replace(old,publication_contract=STOP_PUBLICATION_CONTRACT),
        shadow_projection=None,submitted_payload=bound(p),published_payload=bound(deepcopy(p)))
    receipt=build(inputs);assert receipt['evidence_complete'],receipt
    eligible,_=pm_geometry_domain([dict(plan_id='p1',symbol=p['symbol'],payload_json=json.dumps(p))])
    assert eligible=={'p1'}
    mutated=deepcopy(p);mutated['reward_risk']=999
    bad=capture_publication_projection(inputs,shadow_projection=None,
        submitted_payload=bound(mutated),published_payload=bound(deepcopy(mutated)))
    assert not build(bad)['evidence_complete']

def test_actual_today_originals_immutable_and_derived_display_synced():
    f=Path('D:/dev_A股/liangjian_team_v2_20261016/artifacts/a4-monitor-20261021/C2_TRACK2_1800_ORIGINALS_RO_20261029.json')
    before=f.read_bytes();facts=json.loads(before)['result']['A5_original_contexts'][0]['body']['facts']
    rows=facts['plan_causal_chains']['by_plan'];assert len(rows)==25
    symbols=set()
    for row in rows.values():
        original=row['original_frozen_plan'];symbols.add(original['symbol'].split('.')[0])
        source=deepcopy(original);source.pop(GEOMETRY_FIELD,None)
        out=project_stop_rr_geometry(source,minimum_reward_risk=2.5)
        if out['status']=='PUBLISHABLE':
            p=out['payload'];a=out['audit'];stop=Decimal(str(p['stop_level']))
            low,high,target=(Decimal(a[k]) for k in ('trigger_low','effective_trigger_high','target'))
            assert stop<=low*Decimal('.99') and stop<=high*Decimal('.98')
            assert p['reward_risk']==float((target-high)/(high-stop))
    assert {'002920','300454','300488'}<=symbols
    assert f.read_bytes()==before

def test_a3_price_levels_opt_in_preserves_legacy_and_uses_new_floor():
    from test_technical_aggregates import _daily_bar,_snapshot
    from liangjian_funnel.pipeline.technical_aggregates import build_price_levels
    snapshot=_snapshot(daily=[_daily_bar(i) for i in range(70)])
    old=build_price_levels(snapshot);new=build_price_levels(snapshot,minimum_stop_geometry=True)
    assert 'stop_geometry_policy' not in old
    assert new['stop_geometry_policy']['total_distance_denominator']=='trigger_zone.high'
    low,high=new['trigger_zone'].values();stop=new['invalidation']
    assert Decimal(str(stop))<=Decimal(str(low))*Decimal('.99')
    assert Decimal(str(stop))<=Decimal(str(high))*Decimal('.98')
