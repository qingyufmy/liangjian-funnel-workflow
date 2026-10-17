"""0258: event IDs, not ticker/profile/lifecycle triples, own the domain."""
from copy import deepcopy

import pytest

from test_team_v2_review import report,check,REFS


DOMAIN={key:dict(symbol='000001.SZ',strategy_profile='TREND_MA5',lifecycle_status='REJECTED',plan_id='p1')
    for key in ('A4:original-one','A4:original-two')}


def test_original_counterexample_and_nested_candidate_are_citable_without_alias_repair():
    from liangjian_funnel.runtime.team_v2_review_command import counterexample_citations
    rows=[dict(symbol='000001.SZ',evidence_id='A5V:MISS:000001.SZ',
        a3_candidate=dict(evidence_id='A3:CANDIDATE:000001.SZ',verdict='WAIT'))]
    before=deepcopy(rows);items=counterexample_citations(rows)
    assert rows==before and [r['id'] for r in items]==['A5V:MISS:000001.SZ','A3:CANDIDATE:000001.SZ']
    for item in items:
        fact={'counterexamples':rows}
        for key in item['input_ref']:fact=fact[key]
        from liangjian_funnel.runtime.team_v2_review_command import _hash
        assert item['sha256']==_hash(fact)
    with pytest.raises(ValueError,match='CITATION_CONFLICT'):
        counterexample_citations(rows+rows)


def test_sharded_model_admissions_bind_actual_child_call_not_aggregate_session_id():
    import json
    from liangjian_funnel.runtime.team_v2_review_command import review_events
    calls=[];admissions=[]
    for pid in ('p1','p2'):
        call='A4:parent:'+pid
        judgment=dict(action='BUY_SIGNAL',plan_id=pid,symbol='000001.SZ')
        calls.append(dict(call_id=call,validated=1,judgment_json=json.dumps(judgment),decision_id='decision:'+pid))
        admissions.append(dict(source_call_id=call,binding=judgment,judgment={'status':'APPROVED'}))
    facts=dict(legacy_facts={'a4':{'events':[]},'a3':{'plans':[]}},model_judgments={'rows':{'judgments':calls}},
        a6={'rows':{'trader_sessions':[dict(source_call_id='A4:parent',result_json=json.dumps({'admissions':admissions}))]}})
    domain,events=review_events(facts)
    assert set(domain)=={'decision:p1','decision:p2'} and len(events)==2
    broken=deepcopy(facts)
    raw=json.loads(broken['a6']['rows']['trader_sessions'][0]['result_json'])
    raw['admissions'][0].pop('source_call_id')
    broken['a6']['rows']['trader_sessions'][0]['result_json']=json.dumps(raw)
    with pytest.raises(ValueError,match='JUDGMENT_BINDING_UNPROVEN'):review_events(broken)


def response():
    raw=report({})
    raw['signal_reviews']=[dict(decision_id=key,**values,assessment='逐事件依据不足',attribution='DATA_LIMITED',**REFS)
        for key,values in DOMAIN.items()]
    return raw


def test_two_same_ticker_and_status_events_require_two_original_ids():
    assert check(response(),signal_domain=DOMAIN).valid
    raw=response();raw['signal_reviews'][0].pop('plan_id')
    assert check(raw,signal_domain=DOMAIN).valid


@pytest.mark.parametrize('fault',['missing_id','missing_event','duplicate','outside','symbol','profile','lifecycle','plan','list_id'])
def test_event_binding_rejects_missing_duplicate_outside_and_copied_wrong_metadata(fault):
    raw=response()
    if fault=='missing_id':raw['signal_reviews'][0].pop('decision_id')
    elif fault=='missing_event':raw['signal_reviews'].pop()
    elif fault=='duplicate':raw['signal_reviews'][1]=deepcopy(raw['signal_reviews'][0])
    elif fault=='outside':raw['signal_reviews'][0]['decision_id']='A4:outside'
    elif fault=='symbol':raw['signal_reviews'][0]['symbol']='000002.SZ'
    elif fault=='profile':raw['signal_reviews'][0]['strategy_profile']='LEADER_INTRADAY'
    elif fault=='lifecycle':raw['signal_reviews'][0]['lifecycle_status']='FILLED'
    elif fault=='plan':raw['signal_reviews'][0]['plan_id']='p2'
    elif fault=='list_id':raw['signal_reviews'][0]['decision_id']=[]
    assert not check(raw,signal_domain=DOMAIN).valid


def test_old_triple_domain_is_a_server_contract_error_not_silent_accept():
    with pytest.raises(ValueError,match='SERVER_CONTEXT_INVALID'):
        check(response(),signal_domain={('000001.SZ','TREND_MA5','REJECTED')})
