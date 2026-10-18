from copy import deepcopy

import pytest

from liangjian_funnel.evaluation import team_v2_replay_timeline as timeline
from test_team_v2_replay_timeline import fixture,batches


def test_carried_wait_is_not_a_new_model_judgment_denominator(tmp_path,monkeypatch):
    args,_=fixture(tmp_path)
    root=dict(model_validated=None,admissions=[],reason_codes=[],shards=[],scope=dict(decisions=[
        dict(plan_id='p',reason_code='CARRIED_WAIT',selected_for_model=False,model_judgment_denominator=False)]))
    monkeypatch.setattr(timeline,'run_control_minute',lambda **kwargs:dict(fills=[],hard_exits=[],trader=deepcopy(root)))
    result=timeline.execute_timeline(batches(),**args)
    assert result['legal_rate_denominator']==0 and result['model_batch_legal_rate'] is None
    assert sum(d['counts']['carried_wait_plan_ticks'] for d in result['days'])==1440


def test_actual_shard_claims_not_selected_or_deferred_plans_define_model_rate():
    root=dict(model_validated=False,shards=[
        dict(source_call_id='a',plan_ids=['a1','a2'],model_validated=True),
        dict(source_call_id='b',plan_ids=['b1'],model_validated=False),
        dict(source_call_id='not-sent',plan_ids=['c1'],model_validated=False)],scope=dict(decisions=[
            dict(plan_id='a1',reason_code='ENGINE_EVENT',selected_for_model=True,model_judgment_denominator=True),
            dict(plan_id='a2',reason_code='ENGINE_EVENT',selected_for_model=True,model_judgment_denominator=True),
            dict(plan_id='b1',reason_code='ENGINE_EVENT',selected_for_model=True,model_judgment_denominator=True),
            dict(plan_id='c1',reason_code='ENGINE_EVENT',selected_for_model=True,model_judgment_denominator=False),
            dict(plan_id='deferred',reason_code='DEFERRED_CAPACITY',selected_for_model=False,model_judgment_denominator=False),
            dict(plan_id='wait',reason_code='CARRIED_WAIT',selected_for_model=False,model_judgment_denominator=False)]))
    value=timeline.model_evidence_counts(root,None)
    assert value['attempted_model_calls']==2 and value['validated_model_calls']==1
    assert value['model_attempted_plan_ticks']==3 and value['model_legal_plan_ticks']==2
    assert value['selected_not_sent_plan_ticks']==1 and value['deferred_capacity_plan_ticks']==1
    assert value['carried_wait_plan_ticks']==1


def test_legacy_single_session_requires_real_call_claim_not_a_status_label():
    class Journal:
        def has_call_claim(self,identity):return identity=='real'
    value=timeline.model_evidence_counts(dict(source_call_id='real',model_validated=False),Journal())
    assert value['attempted_model_calls']==1 and value['validated_model_calls']==0
    value=timeline.model_evidence_counts(dict(source_call_id='fabricated',model_validated=True),Journal())
    assert value['attempted_model_calls']==0
