from copy import deepcopy
import hashlib
import json

import pytest

from liangjian_funnel.runtime.team_v2_sources import meeting_plan_facts


def row():
    raw=dict(symbol='000001.SZ',source_run_id='original-run',primary_theme='FINANCE',
        trigger_low=10,trigger_high=11,stop_level=9,no_chase=12,plan_expiry='2026-10-14T15:00:00+08:00',
        handoff_notes={'to_a4':['original risk path']},what_would_change_my_mind=['original falsifier'],
        a4_exit_rules={'rule':'original exit'},supporting_refs=['original-ref'],
        deterministic_technical_evidence={'unused_archive':'X'*100000})
    return dict(plan_id='p1',lane_id='lane_1',symbol=raw['symbol'],status='PENDING_MORNING_REVIEW',
        valid_from=None,expires_at='2026-10-15T15:00:00+08:00',payload_json=json.dumps(raw))


def test_compact_meeting_projection_preserves_original_rules_dates_and_hashes_without_repair():
    original=row();before=deepcopy(original);facts=meeting_plan_facts([original])[0]
    assert original==before and facts['plan_id']=='p1'
    assert facts['publication_facts']['plan_expiry']=='2026-10-14T15:00:00+08:00'
    assert facts['expires_at']=='2026-10-15T15:00:00+08:00' and facts['valid_from'] is None
    for key in ('handoff_notes','what_would_change_my_mind','a4_exit_rules','supporting_refs','stop_level'):
        assert facts['publication_facts'][key]==json.loads(original['payload_json'])[key]
    assert facts['original_payload_sha256']==hashlib.sha256(original['payload_json'].encode()).hexdigest()
    assert facts['missing_fields']==['plan_priority_rank']
    assert 'payload_json' not in facts and 'deterministic_technical_evidence' not in facts['publication_facts']
    assert len(json.dumps(facts))<len(original['payload_json'])/10


@pytest.mark.parametrize('kind',['duplicate','wrong_symbol','missing_run'])
def test_meeting_projection_never_hides_a_malformed_original_domain(kind):
    original=row();rows=[original]
    if kind=='duplicate':rows.append(original)
    else:
        body=json.loads(original['payload_json'])
        if kind=='wrong_symbol':body['symbol']='000002.SZ'
        else:body.pop('source_run_id')
        original['payload_json']=json.dumps(body)
    with pytest.raises(ValueError,match='MEETING_PLAN_SOURCE_BINDING_INVALID'):meeting_plan_facts(rows)


def test_historical_planning_cannot_see_later_activation_or_invalidation():
    from liangjian_funnel.runtime.team_v2_sources import historical_meeting_plan_facts
    original=row();original.update(status='INVALIDATED',valid_from='2026-10-15T09:26:00+08:00')
    before=deepcopy(original)
    facts=historical_meeting_plan_facts([original])[0]
    assert original==before
    assert 'status' not in facts and 'valid_from' not in facts
    assert facts['lifecycle_evidence_status']=='UNAVAILABLE_AT_HISTORICAL_PLANNING_CUTOFF'
    assert facts['publication_facts']==meeting_plan_facts([original])[0]['publication_facts']
    assert facts['source_row_sha256']==meeting_plan_facts([original])[0]['source_row_sha256']
    assert meeting_plan_facts([original])[0]['status']=='INVALIDATED'  # live path unchanged
