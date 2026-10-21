"""Closed domains are sent before the first answer and bind every correction."""
from copy import deepcopy
from pathlib import Path
import hashlib,json

import pytest

from liangjian_funnel.pipeline.team_v2_a2_schema import a2_output_contract,a2_field_violations,validate_a2_response
from liangjian_funnel.pipeline.team_v2_a2_recovery import validation_feedback
from liangjian_funnel.pipeline.team_v2_a2_score import POLICY,penalty_code_domain
from test_team_v2_a2_schema import packet,CATALOG,REFS,ENVELOPE


def originals():return json.loads((Path(__file__).parent/'fixtures/a2_penalty_0515_originals.json').read_text(encoding='utf-8'))['rounds']


def context(row):
    return dict(required_envelope=row['envelope'],candidate_ids=set(row['candidate_ids']),
        nomination_scope=set(row['nomination_scope']),symbol_themes=row['symbol_themes'],
        allowed_theme_ids=set(row['allowed_theme_ids']),direction_symbols={d:set(v) for d,v in row['direction_symbols'].items()},
        engine_emotion_cycle=row['engine_emotion_cycle'],penalty_codes=row['penalty_points'].keys(),
        nomination_facts=row['nomination_sample'])


@pytest.mark.parametrize('index',[0,1])
def test_two_real_penalty_originals_rejected_unchanged_with_each_exact_row_index(index):
    row=originals()[index];raw=json.loads(row['captured']['output_json']);before=deepcopy(raw)
    assert hashlib.sha256(row['captured']['output_json'].encode()).hexdigest()==row['captured']['output_hash']
    checked=validate_a2_response(raw,row['catalog'],**context(row))
    assert 'A2_SCORE_PENALTY_CODE_UNKNOWN' in checked.issues
    contract=a2_output_contract(catalog=row['catalog'],**context(row))
    feedback=validation_feedback(raw,row['catalog'],checked.issues,envelope=row['envelope'],
        symbols=set(row['candidate_ids']),themes=set(row['allowed_theme_ids']),directions=context(row)['direction_symbols'],
        nomination_scope=set(row['nomination_scope']),output_contract=contract)
    bad=[v for v in feedback['violations'] if v['code']=='A2_SCORE_PENALTY_CODE_UNKNOWN']
    expected=[f'theme_reviews[{i}].penalty_codes[{j}]' for i,r in enumerate(raw['theme_reviews'])
        for j,code in enumerate(r['penalty_codes']) if code not in row['penalty_points']]
    assert [v['field'] for v in bad]==expected and expected
    assert all(v['actual']=='A2_CAPITAL_FLOW_UNAVAILABLE' for v in bad)
    assert all(v['allowed']['values']==sorted(POLICY['penalty_points']) for v in bad)
    assert raw==before


def baseline():
    raw=packet();raw['envelope']=dict(ENVELOPE,market_regime='ROTATION')
    kwargs=dict(required_envelope=raw['envelope'],candidate_ids={'000001.SZ','000002.SZ'},nomination_scope={'000003.SZ'},
        symbol_themes={'000001.SZ':'T','000002.SZ':'U'},allowed_theme_ids={'T','U'},direction_symbols={'D':{'000001.SZ','000002.SZ'}},
        catalog=deepcopy(CATALOG),nomination_facts=[dict(symbol='000003.SZ',theme_id='V',quant_filter_reason='EXACT_SOURCE_REASON')])
    kwargs['catalog']['items'].append(dict(id='A2:NOMINATION:000003.SZ',category='NEWS'))
    return raw,kwargs


def review():return dict(rotation_direction_id='D',decision='NO_FOCUS',reviewed_symbols=['000002.SZ'],
    reason_codes=['LOW_CONVICTION'],explanation='Original frozen evidence lacks conviction.',**REFS)


@pytest.mark.parametrize('case,path',[
    ('envelope','envelope.run_id'),('regime','market_read.regime'),('emotion','market_read.emotion_cycle'),
    ('dominant','market_read.dominant_themes[0]'),('theme','theme_reviews[0].theme_id'),
    ('stage','theme_reviews[0].stage'),('policy','theme_reviews[0].new_entry_policy'),
    ('penalty','theme_reviews[0].penalty_codes[0]'),('penalty-case','theme_reviews[0].penalty_codes[0]'),
    ('symbol','focus_decisions[0].symbol'),('ref','focus_decisions[0].supporting_refs[0]'),
    ('sentinel-support','focus_decisions[0].supporting_refs[0]'),('sentinel-scope','focus_decisions[0].opposing_search_scope'),
    ('nom-theme','model_nominations[0].theme_id'),('nom-reason','model_nominations[0].quant_filter_reason'),
    ('nom-ref','model_nominations[0].supporting_refs'),('rot-direction','rotation_reviews[0].rotation_direction_id'),
    ('rot-member','rotation_reviews[0].reviewed_symbols[0]'),('rot-decision','rotation_reviews[0].decision'),
    ('rot-reason','rotation_reviews[0].reason_codes'),('rot-explanation','rotation_reviews[0].explanation')])
def test_all_closed_fields_use_the_same_contract_and_exact_actual_path(case,path):
    raw,kw=baseline()
    if case=='envelope':raw['envelope']={**raw['envelope'],'run_id':'FOREIGN'}
    elif case=='regime':raw['market_read']['regime']='OTHER'
    elif case=='emotion':raw['market_read']['emotion_cycle']='STARTUP'
    elif case=='dominant':raw['market_read']['dominant_themes']=['OUTSIDE']
    elif case=='theme':raw['theme_reviews'][0]['theme_id']='OUTSIDE'
    elif case=='stage':raw['theme_reviews'][0]['stage']='NOT_A_STAGE'
    elif case=='policy':raw['theme_reviews'][0]['new_entry_policy']='PROBE_ONLY'
    elif case=='penalty':raw['theme_reviews'][0]['penalty_codes']=['RISK_JUDGMENT_NOT_PENALTY']
    elif case=='penalty-case':raw['theme_reviews'][0]['penalty_codes']=['HIGH_CROWDING']
    elif case=='symbol':raw['focus_decisions'][0]['symbol']='OUTSIDE'
    elif case in ('ref','sentinel-support'):raw['focus_decisions'][0]['supporting_refs']=['OUTSIDE' if case=='ref' else 'NO_OPPOSING_EVIDENCE_FOUND']
    elif case=='sentinel-scope':raw['focus_decisions'][0].update(opposing_refs=['NO_OPPOSING_EVIDENCE_FOUND'],opposing_search_scope=['NEWS'])
    elif case.startswith('nom-'):
        raw['model_nominations']=[dict(symbol='000003.SZ',theme_id='V',quant_filter_reason='EXACT_SOURCE_REASON',
            nomination_thesis='Evidence judgment',why_i_disagree='Frozen source judgment',falsifiers=['Original facts reverse'],
            supporting_refs=['A2:NOMINATION:000003.SZ'],opposing_refs=['N'])]
        raw['model_nominations'][0][{'nom-theme':'theme_id','nom-reason':'quant_filter_reason','nom-ref':'supporting_refs'}[case]] = ['M'] if case=='nom-ref' else 'WRONG'
    elif case.startswith('rot-'):
        raw['focus_decisions']=[];raw['rotation_reviews']=[review()]
        key={'rot-direction':'rotation_direction_id','rot-member':'reviewed_symbols','rot-decision':'decision','rot-reason':'reason_codes','rot-explanation':'explanation'}[case]
        raw['rotation_reviews'][0][key]={'rot-direction':'FOREIGN','rot-member':['FOREIGN'],'rot-decision':'FOCUS','rot-reason':[],'rot-explanation':'short'}[case]
    original=deepcopy(raw);contract=a2_output_contract(**kw)
    violations=a2_field_violations(raw,contract)
    assert any(v['field']==path for v in violations),(case,violations)
    cat=kw.pop('catalog');assert not validate_a2_response(raw,cat,**kw).valid
    assert raw==original


def test_open_reason_support_risk_are_not_mistaken_for_penalty_enums_and_sentinel_is_exclusive():
    raw,kw=baseline();raw['reason_codes']=['FREE_JUDGMENT'];raw['theme_reviews'][0]['risk_codes']=['A2_CAPITAL_FLOW_UNAVAILABLE']
    raw['focus_decisions'][0]['support_codes']=['FREE_REASON'];raw['focus_decisions'][0].update(
        opposing_refs=['NO_OPPOSING_EVIDENCE_FOUND'],opposing_search_scope=['MARKET','NEWS'])
    cat=kw.pop('catalog');assert validate_a2_response(raw,cat,**kw).valid
    raw['focus_decisions'][0]['opposing_refs'].append('N')
    assert not validate_a2_response(raw,cat,**kw).valid


def test_exact_policy_penalty_domain_is_shared_with_score_not_a_fresh_alias_set():
    assert penalty_code_domain(POLICY,{**POLICY['penalty_points'],'INVENTED':-99})==tuple(sorted(POLICY['penalty_points']))
    assert len(penalty_code_domain(POLICY,POLICY['penalty_points']))==6


def test_real_a2_mock_last_user_has_penalty_contract_first_then_exact_correction(tmp_path):
    from test_team_v2_a2_research import fixture
    def answer(raw,attempt):
        raw['theme_reviews'][0]['penalty_codes']=['A2_CAPITAL_FLOW_UNAVAILABLE'] if attempt==1 else []
    app,args,seen=fixture(tmp_path,answer)
    result=app._run_stage(**args)
    assert result.status=='VALIDATED' and len(seen)==2,result.reason_codes
    users=[json.loads(r['messages'][-1]['content'].removeprefix('RUNTIME_INPUT\n')) for r in seen]
    contract=users[0]['server_output_contract']
    assert contract['domains']['penalty_codes']==sorted(POLICY['penalty_points'])
    assert contract['field_contracts']['theme_reviews[*].penalty_codes']['case_sensitive'] is True
    assert users[1]['server_output_contract']==contract
    correction=users[1]['validation_feedback']['violations'][0]
    assert correction['field']=='theme_reviews[0].penalty_codes[0]'
    assert correction['actual']=='A2_CAPITAL_FLOW_UNAVAILABLE'
    assert correction['allowed']['values']==contract['domains']['penalty_codes']
    with app.team_research_adapter.journal._connect() as db:
        captured=db.execute('select validated,output_json from model_calls order by rowid').fetchall()
    assert [r[0] for r in captured]==[0,1]
    assert json.loads(captured[0][1])['theme_reviews'][0]['penalty_codes']==['A2_CAPITAL_FLOW_UNAVAILABLE']
