"""Original quant exclusions -> one research sample -> actual model journal."""
from copy import deepcopy
from dataclasses import replace
from contextlib import closing
import json

import pytest

from test_team_v2_a2_research import fixture


def add_filtered(args,symbol='000004.SZ',**changes):
    data=deepcopy(dict(args['snapshot'].data))
    row=deepcopy(data['A2_BOTTLENECK_CONTEXT']['000001.SZ'])
    row.update(company_name='FILTERED',theme_id='T',upstream_candidate_id='a1:'+symbol,
        deterministic_status='LOCAL_MONITOR',deterministic_reason_codes=['A2_OUTSIDE_ROTATION_TOP_THEMES'],
        role_breakdown={'relative_strength':85},selected_board_theme_match=True)
    row.update(changes);data['A2_BOTTLENECK_CONTEXT'][symbol]=row
    args['snapshot']=replace(args['snapshot'],data=data)


def nominate(raw,_):
    raw['model_nominations']=[dict(symbol='000004.SZ',theme_id='T',nomination_thesis='Frozen sector evidence',
        quant_filter_reason='A2_OUTSIDE_ROTATION_TOP_THEMES',why_i_disagree='Disclosed catalyst',falsifiers=['Catalyst withdrawn'],
        supporting_refs=['A2:NOMINATION:000004.SZ'],opposing_refs=['A2:NEWS'])]


def test_natural_filtered_context_becomes_bounded_source_not_an_extra_a3_candidate(tmp_path):
    from liangjian_funnel.pipeline.research.common import _build_a3_candidate_domain
    app,args,seen=fixture(tmp_path,nominate);add_filtered(args)
    original=deepcopy(dict(args['snapshot'].data))
    result=app._run_stage(**args)
    assert result.status=='VALIDATED',result.reason_codes
    assert len(seen)==1 and 'A2_OUTSIDE_ROTATION_TOP_THEMES' in seen[0]['messages'][0]['content']
    _,domain=_build_a3_candidate_domain(result.output)
    assert '000004.SZ' not in domain and result.output['model_nominations'][0]['symbol']=='000004.SZ'
    assert float(result.output['active_themes'][0]['theme_score'])==70
    with closing(app.team_research_adapter.journal._connect()) as db:
        assert db.execute("SELECT symbol FROM model_judgments WHERE judgment_path='model_nominations[0]'").fetchall()==[('000004.SZ',)]
    source=result.diagnostics['nomination_source']
    assert source['eligible_before_cap']==1 and source['maximum_sample_size']==30
    assert args['snapshot'].data==original


def test_unproven_top_level_sample_cannot_invent_a_quant_exclusion(tmp_path):
    app,args,seen=fixture(tmp_path,nominate)
    args['snapshot']=replace(args['snapshot'],data={**args['snapshot'].data,
        'A2_QUANT_FILTERED_SAMPLE':[dict(symbol='000004.SZ',theme_id='T',
            quant_filter_reason='A2_OUTSIDE_ROTATION_TOP_THEMES',source_refs=['unproven'])]})
    result=app._run_stage(**args)
    assert result.status=='BLOCKED' and not seen


@pytest.mark.parametrize('change,reason',[
    (lambda r:r.update(theme_id='FORGED_THEME'),'A2_NOMINATION_THEME_BINDING_MISMATCH'),
    (lambda r:r.update(quant_filter_reason='INVENTED_REASON'),'A2_NOMINATION_FILTER_REASON_MISMATCH'),
    (lambda r:r.update(supporting_refs=['A2:MARKET']),'A2_NOMINATION_ORIGINAL_FACT_REFERENCE_REQUIRED')])
def test_model_nomination_must_bind_original_theme_reason_and_own_quant_fact(tmp_path,change,reason):
    def reply(raw,n):nominate(raw,n);change(raw['model_nominations'][0])
    app,args,seen=fixture(tmp_path,reply);add_filtered(args)
    result=app._run_stage(**args)
    assert result.status=='BLOCKED' and result.reason_codes==(reason,) and len(seen)==3
    assert result.diagnostics['validation_history']==[[reason]]*3
    assert all(reason in request['messages'][0]['content'] for request in seen[1:])
    with closing(app.team_research_adapter.journal._connect()) as db:
        assert db.execute('SELECT validated FROM model_calls').fetchall()==[(0,)]*3
    with closing(app.team_research_adapter.store._connect()) as db:
        rows=db.execute("SELECT body_json FROM control_minute_context WHERE decision_id LIKE 'a2-research-source:%'").fetchall()
    frozen=[json.loads(row[0])['values'] for row in rows]
    assert len(frozen)==3
    assert all(values['A2_QUANT_FILTERED_SAMPLE']==frozen[0]['A2_QUANT_FILTERED_SAMPLE'] for values in frozen)
    assert [row['symbol'] for row in frozen[0]['A2_QUANT_FILTERED_SAMPLE']]==['000004.SZ']
    for values in frozen:
        feedback=values['A2_BOTTLENECK_CONTEXT']['_rotation_review_contract'].get('retry_feedback')
        if feedback is not None:
            assert feedback['validation_errors']==[reason]
            assert feedback['legal_values']['nomination_scope']==['000004.SZ']
            assert [row['code'] for row in feedback['violations']]==[reason]


@pytest.mark.parametrize('changes',[
    {'deterministic_status':'HARD_REJECT'}, {'deterministic_status':'DATA_GAP'},
    {'deterministic_status':'REVIEW_CANDIDATE'}, {'top_rotation_theme':True},
    {'stock_behavior_type':'EMOTION','eastmoney_hot100':{'rank':1}},
    {'deterministic_reason_codes':[]}, {'theme_id':''},
    {'role_breakdown':{},'selected_board_theme_match':False}])
def test_ineligible_or_existing_technical_watch_rows_do_not_enter_nomination_sample(tmp_path,changes):
    app,args,_=fixture(tmp_path);add_filtered(args,**changes)
    snapshot=app.team_research_adapter.prepare_a2_snapshot(args['snapshot'],args['upstream_symbols'])
    assert snapshot.data['A2_QUANT_FILTERED_SAMPLE']==[]


def test_cap_and_order_are_global_deterministic_and_selected_source_is_immutable(tmp_path):
    app,args,_=fixture(tmp_path)
    for i in range(4,45):add_filtered(args,f'{i:06d}.SZ',role_breakdown={'relative_strength':i})
    before=deepcopy(dict(args['snapshot'].data));adapter=app.team_research_adapter
    first=adapter.prepare_a2_snapshot(args['snapshot'],args['upstream_symbols'])
    sample=first.data['A2_QUANT_FILTERED_SAMPLE']
    assert len(sample)==30 and sample[0]['symbol']=='000044.SZ' and sample[-1]['symbol']=='000015.SZ'
    assert first.data['_TEAM_A2_NOMINATION_FREEZE']['eligible_before_cap']==41
    assert args['snapshot'].data==before
    assert adapter.prepare_a2_snapshot(first,{'000002.SZ'}).data['A2_QUANT_FILTERED_SAMPLE']==sample
    data=deepcopy(dict(args['snapshot'].data))
    data['A2_BOTTLENECK_CONTEXT']=dict(reversed(list(data['A2_BOTTLENECK_CONTEXT'].items())))
    reordered=adapter.prepare_a2_snapshot(replace(args['snapshot'],data=data),args['upstream_symbols'])
    assert reordered.data['_TEAM_A2_NOMINATION_FREEZE']==first.data['_TEAM_A2_NOMINATION_FREEZE']
    first.data['A2_BOTTLENECK_CONTEXT']['000044.SZ']['a2_factor_scores']['capital_flow']['score']=1
    with pytest.raises(ValueError,match='NOMINATION_SOURCE_MISMATCH'):
        adapter.prepare_a2_snapshot(first,args['upstream_symbols'])


def test_later_transport_batch_cannot_get_another_nomination_quota(tmp_path):
    app,args,seen=fixture(tmp_path);add_filtered(args)
    args['snapshot']=app.team_research_adapter.prepare_a2_snapshot(args['snapshot'],args['upstream_symbols'])
    args['projection_symbols']={'000002.SZ','000003.SZ'}
    # The old model mock focuses symbol 1, intentionally invalid in this batch;
    # inspect the actual pre-model source, not a repaired mock response.
    result=app._run_stage(**args)
    assert result.status=='BLOCKED' and result.reason_codes==(
        'A2_SYMBOL_OUTSIDE_FROZEN_DOMAIN:focus_decisions',
        'TEAM_SUPPORTING_REFS_INVALID:theme_reviews[0]',
        'TEAM_SUPPORTING_REFS_INVALID:focus_decisions[0]')
    assert len(seen)==3
    assert all('A2:NOMINATION:000004.SZ' not in request['messages'][0]['content'] for request in seen)
    assert all('A2_SYMBOL_OUTSIDE_FROZEN_DOMAIN:focus_decisions' in request['messages'][0]['content'] for request in seen[1:])
    with closing(app.team_research_adapter.store._connect()) as db:
        rows=db.execute("SELECT body_json FROM control_minute_context WHERE decision_id LIKE 'a2-research-source:%'").fetchall()
    assert len(rows)==3
    for row in rows:
        values=json.loads(row[0])['values']
        assert values['A2_QUANT_FILTERED_SAMPLE']==[]
        feedback=values['A2_BOTTLENECK_CONTEXT']['_rotation_review_contract'].get('retry_feedback')
        if feedback is not None:
            assert feedback['legal_values']['nomination_scope']==[]
