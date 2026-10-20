import json
from copy import deepcopy

import pytest

from liangjian_funnel.pipeline.team_v2_plan_geometry import project_team_plan_geometry
from liangjian_funnel.runtime.team_v2_pm_geometry import pm_geometry_domain


def row(pid='p1',**changes):
    body=dict(symbol='000333.SZ',trigger_low=10,trigger_high=11,stop_level=9,
        first_resistance=14,minimum_reward_risk=2.5)
    body.update(changes)
    return dict(plan_id=pid,symbol=body['symbol'],payload_json=json.dumps(body))


def test_existing_projector_and_frozen_facts_only():
    rows=[row(),row('empty',first_resistance=10),row('unknown',first_resistance=None)]
    before=deepcopy(rows)
    ids,audit=pm_geometry_domain(rows)
    assert ids==frozenset({'p1'}) and rows==before
    assert audit['counts']=={'PARTIAL':1,'EMPTY':1,'UNPROVEN':1}
    assert audit['plan_writes']==audit['source_fetches']==0


def test_new_geometry_plan_rechecked_not_blindly_trusted():
    original=json.loads(row()['payload_json'])
    plan=project_team_plan_geometry(original)['payload']
    r=dict(plan_id='new',symbol=original['symbol'],payload_json=json.dumps(plan))
    assert pm_geometry_domain([r])[0]==frozenset({'new'})
    plan['first_resistance']=9
    r['payload_json']=json.dumps(plan)
    assert pm_geometry_domain([r])[0]==frozenset()


def test_missing_threshold_is_not_guessed():
    r=row();p=json.loads(r['payload_json']);del p['minimum_reward_risk']
    r['payload_json']=json.dumps(p)
    assert pm_geometry_domain([r])[0]==frozenset()


@pytest.mark.parametrize('change',[lambda r:r.update(symbol='other'),
    lambda r:r.update(payload_json='[]'),lambda r:r.update(plan_id='')])
def test_invalid_ownership_rejected(change):
    r=row();change(r)
    with pytest.raises(ValueError):pm_geometry_domain([r])


def test_duplicate_owner_rejected():
    with pytest.raises(ValueError):pm_geometry_domain([row(),row()])
