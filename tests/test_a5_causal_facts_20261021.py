from copy import deepcopy
from liangjian_funnel.runtime.team_v2_review_causal import plan_causal_projection


def test_each_plan_keeps_original_identity_all_sample_states_and_accounts_separate():
    source={'a3':{'plans':[{'plan_id':'p','symbol':'002270.SZ'}]},'paper_broker':{'snapshot':{'qty':10500}},
        'legacy_facts':{'a3_plan_replay':{'frozen_plans':{'p':{'stop_level':18.66}},'plans':[{'plan_id':'p','status':'DATA_LIMITED'}]},
            'a4':{'events':[{'plan_id':'p','action':'NO_ACTION','reason_code':'WAIT','minute_end':t,'evidence_id':t}
                for t in ('10:00','10:30')], 'lifecycles':[{'plan_id':'p','entry_qty':2100}]}}}
    before=deepcopy(source);result=plan_causal_projection(source)
    plan=result['by_plan']['p'];assert plan['original_frozen_plan']=={'stop_level':18.66}
    assert plan['condition_samples'][0]['count']==2
    assert plan['legacy_lifecycle'][0]['entry_qty']==2100
    assert result['team_paper_account']['snapshot']['qty']==10500
    assert 'NOT_CONTINUOUS' in result['coverage'] and source==before
    assert not any(key in plan for key in ('mfe','mae','R'))


def test_missing_sources_do_not_create_plans_or_fabricate_results():
    result=plan_causal_projection({})
    assert result['by_plan']=={} and result['team_paper_account'] is None
