from copy import deepcopy
import json
from pathlib import Path
from liangjian_funnel.runtime.team_v2_review_facts import pm_deployment_target_metrics


def test_actual_pm_target_is_a5_metric_not_an_execution_authority():
    row={'meeting_id':'pm:2026-10-19','result_json':json.dumps({'status':'READY',
        'deployment_target':{'target_pct':40,'baseline_range':[40,65]}})}
    before=deepcopy(row);out=pm_deployment_target_metrics([row])
    assert row==before and out['status']=='AVAILABLE'
    assert out['observations'][0]['target_pct']=='40'
    assert out['observations'][0]['execution_authority'] is False
    assert out['observations'][0]['source']=='FROZEN_PM_DECISION_NOT_A6_VOTE'


def test_invalid_or_unavailable_target_not_repaired_or_invented():
    for value in (True,'nan',-1,101):
        row={'result_json':json.dumps({'status':'READY','deployment_target':{
            'target_pct':value,'baseline_range':[40,65]}})}
        assert pm_deployment_target_metrics([row])['status']=='UNAVAILABLE'
    assert pm_deployment_target_metrics([])['observations']==[]


def test_real_1019_original_red_then_exact_original_pm_metric_green():
    from liangjian_funnel.pipeline.team_v2_review_schema import validate_daily_review
    f=json.loads((Path(__file__).parent/'fixtures/team_v2_a5_midday_original_20261019.json').read_bytes())
    args=dict(required_envelope=f['required_envelope'],trade_date='2026-10-19',review_kind='MIDDAY',
        signal_domain=f['signal_domain'],counterexample_domain=set(f['counterexample_domain']),
        confirmed_defect_symbols=frozenset(f['confirmed_defect_symbols']))
    before=json.dumps(f['output'],sort_keys=True)
    old=validate_daily_review(f['output'],f['catalog'],metrics=f['metrics'],**args)
    assert not old.valid and old.issues==('A5_SCORECARD_NUMBER_NOT_IN_METRICS',)
    metrics={**f['metrics'],'pm_deployment_targets':pm_deployment_target_metrics(f['pm_decisions'])}
    assert metrics['pm_deployment_targets']['observations'][0]['target_pct']=='40'
    new=validate_daily_review(f['output'],f['catalog'],metrics=metrics,**args)
    assert new.valid and not new.issues
    assert json.dumps(f['output'],sort_keys=True)==before
    fabricated=deepcopy(f['output'])
    fabricated['team_scorecard']['utilization_vs_target']+=';made_up_target=777'
    assert 'A5_SCORECARD_NUMBER_NOT_IN_METRICS' in validate_daily_review(
        fabricated,f['catalog'],metrics=metrics,**args).issues
