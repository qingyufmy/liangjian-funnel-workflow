from copy import deepcopy
import pytest

from test_team_v2_review_command import fixture,NOW
from test_team_v2_review import hashed
from liangjian_funnel.pipeline.team_v2_judgments import ModelJudgmentJournal
from liangjian_funnel.runtime.team_v2_review_facts import build_team_review_facts
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore


@pytest.mark.parametrize('value',[30,None,True,-1,'30'])
def test_original_explicit_a2_counts_reach_metrics_without_model_arithmetic_or_coercion(tmp_path,value):
    settings,_,_,legacy=fixture(tmp_path)
    legacy['a2']=dict(llm_reviewed_count=value,quant_evaluated_count=1453)
    legacy=hashed({k:v for k,v in legacy.items() if k!='input_hash'});before=deepcopy(legacy)
    owned=settings.root/'state/team_v2'
    full=build_team_review_facts(legacy_facts=legacy,store=TeamMeetingStore(owned/'team.db'),
        journal=ModelJudgmentJournal(owned/'model.db'),paper_snapshot={'status':'UNAVAILABLE'},settlement=None,
        account_id='test-paper',trade_date='2026-10-19',review_kind='POST_CLOSE',observed_at=NOW)
    assert full['metrics']['a2_quant_evaluated_count']==1453
    assert full['metrics']['a2_llm_reviewed_count']==(value if type(value) is int and value>=0 else None)
    assert legacy==before


def test_observation_denominators_and_approved_targets_are_explicit_and_missing_stays_null(tmp_path):
    settings,_,_,legacy=fixture(tmp_path)
    legacy['metrics']=dict(a4_monitor_observation_count=360,a4_action_counts={
        'START_CONFIRMATION':356,'PLAN_INVALIDATED':3,'EMPTY_SCOPE':1})
    legacy['business_audit']={'target_buy_signals':{'minimum':2,'maximum':5}}
    legacy=hashed({k:v for k,v in legacy.items() if k!='input_hash'});before=deepcopy(legacy)
    owned=settings.root/'state/team_v2'
    full=build_team_review_facts(legacy_facts=legacy,store=TeamMeetingStore(owned/'team.db'),
        journal=ModelJudgmentJournal(owned/'model.db'),paper_snapshot={'status':'UNAVAILABLE'},settlement=None,
        account_id='test-paper',trade_date='2026-10-19',review_kind='POST_CLOSE',observed_at=NOW)
    assert full['metrics']['observation_counts']==dict(observed_minutes=360,start_confirmation=356,plan_invalidated=3,empty_scope=1,
        denominator='ORIGINAL_MONITOR_OBSERVATION_ROWS_NOT_DISTINCT_WALL_CLOCK_MINUTES')
    assert full['metrics']['targets']['status']=='FROZEN_APPROVED_TARGET' and legacy==before
