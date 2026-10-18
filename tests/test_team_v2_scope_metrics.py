import json
from liangjian_funnel.runtime.team_v2_review_facts import trader_scope_metrics,candidate_review_metrics


def test_deferred_ratio_excludes_carried_wait_and_child_capture_rows():
    rows=[dict(decision_id='root',result_json=json.dumps(dict(scope=dict(decisions=[
        dict(reason_code='ENGINE_STATE_TRANSITION',model_judgment_denominator=True),
        dict(reason_code='DEFERRED_CAPACITY',model_judgment_denominator=False),
        dict(reason_code='CARRIED_WAIT',model_judgment_denominator=False)])))),
        dict(decision_id='root:S001',result_json=json.dumps(dict(capture_only=True,model_validated=True)))]
    result=trader_scope_metrics(rows)
    assert result['deferred_capacity_rate']=='0.5' and result['scheduled_candidate_plan_ticks']==2
    assert result['scope_plan_ticks']==3 and result['carried_wait_model_judgments']==0
    assert result['scope_batches']==1
    assert trader_scope_metrics([])['deferred_capacity_rate'] is None


def test_review_rates_use_actual_sent_objects_and_unknown_timeout_stays_unknown():
    review=dict(candidate_count=5,selected_count=2,actual_model_objects=2,confirmed_count=0,
        veto_count=0,failed_review_count=2,engine_exit_fallback_count=1,timeout_count=None)
    rows=[dict(decision_id='root',result_json=json.dumps({'candidate_review':review})),
        dict(decision_id='root:REVIEW',result_json=json.dumps({'capture_only':True}))]
    result=candidate_review_metrics(rows)
    assert result['candidate_count']==5 and result['actual_model_objects']==2
    assert result['unclassified_failure_objects']==2 and result['timeout_rate'] is None
    review.update(confirmed_count=1,veto_count=1,failed_review_count=0,timeout_count=0)
    rows[0]['result_json']=json.dumps({'candidate_review':review})
    assert candidate_review_metrics(rows)['confirmation_rate']=='0.5'
    assert candidate_review_metrics([])['confirmation_rate'] is None
