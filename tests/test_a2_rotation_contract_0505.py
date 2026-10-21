"""Exact existing third answer is a counterexample, not a new model call."""
from copy import deepcopy
import hashlib,json
from pathlib import Path

import pytest

from liangjian_funnel.pipeline.team_v2_a2_schema import validate_a2_response
from liangjian_funnel.pipeline.team_v2_a2_recovery import validation_feedback,bind_a2_retry_feedback
from liangjian_funnel.pipeline.team_v2_prompts import with_runtime_contract
from liangjian_funnel.pipeline.team_v2_rotation_scope import rotation_output_contract,rotation_reason_contract,ENTRY_GATE_ONLY_REASON_CODES
from liangjian_funnel.pipeline.research.common import _validate_a2_rotation_focus_coverage
from test_team_v2_a2_retry_user_feedback import request,runtime

PATH=Path(__file__).parent/'fixtures/a2_postpolicy_0505_original.json'
DIRECTION='SELECTED_BOARD:AI_COMPUTE_INFRASTRUCTURE'


def original():return json.loads(PATH.read_text(encoding='utf-8'))


def validate(raw,fixture):
    frozen=fixture['frozen']
    return validate_a2_response(raw,frozen['catalog'],required_envelope=frozen['envelope'],
        candidate_ids=set(frozen['candidate_ids']),nomination_scope=set(frozen['nomination_scope']),
        symbol_themes=frozen['symbol_themes'],allowed_theme_ids=set(frozen['allowed_theme_ids']),
        direction_symbols={d:set(s) for d,s in frozen['direction_symbols'].items()})


def feedback(raw,fixture,issues):
    frozen=fixture['frozen']
    return validation_feedback(raw,frozen['catalog'],issues,envelope=frozen['envelope'],
        symbols=set(frozen['candidate_ids']),themes=set(frozen['allowed_theme_ids']),
        directions={d:set(s) for d,s in frozen['direction_symbols'].items()},nomination_scope=set(frozen['nomination_scope']))


def test_exact_original_raw_unchanged_now_rejected_before_legacy_postpolicy():
    fixture=original();captured=fixture['captured'];before=captured['output_json']
    assert hashlib.sha256(before.encode()).hexdigest()==captured['output_hash']
    assert captured['validated']==1 and captured['issues_json']=='[]'
    raw=json.loads(before)
    checked=validate(raw,fixture)
    assert checked.issues==('A2_ROTATION_REASON_CODES_INVALID:'+DIRECTION,)
    assert json.loads(before)==raw and fixture['captured']['output_json']==before


def test_same_projection_last_user_contract_and_exact_field_feedback():
    fixture=original();raw=json.loads(fixture['captured']['output_json'])
    contract=rotation_output_contract(fixture['frozen']['direction_symbols'])
    before=request();first=with_runtime_contract(before,contract)
    rule=runtime(first)['server_output_contract']['rotation_reason_codes_by_direction'][DIRECTION]
    assert rule['minItems']==1 and rule['positive_enum'] is None
    issue=validate(raw,fixture).issues
    review=feedback(raw,fixture,issue)
    assert review['violations']==[dict(code=issue[0],field='rotation_reviews[0].reason_codes',actual=[],allowed=rule)]
    corrected=bind_a2_retry_feedback(first,review,correction_attempt=2)
    assert corrected.messages[0] is first.messages[0]
    assert runtime(corrected)['server_output_contract']==contract
    assert runtime(corrected)['validation_feedback']['violations'][0]['actual']==[]
    assert corrected.input_hash!=first.input_hash


def test_original_posterior_two_errors_have_one_cause_and_controlled_model_reason_clears_both(monkeypatch):
    fixture=original();frozen=fixture['frozen'];post=deepcopy(fixture['postpolicy_exact_projection'])
    # This seam reuses the exact frozen server direction projection. It does
    # not reconstruct missing input or run any provider/whole research entry.
    monkeypatch.setitem(_validate_a2_rotation_focus_coverage.__globals__,'_project_a2_bottleneck_context',
        lambda *args,**kwargs:{'_rotation_review_scope':frozen['direction_symbols']})
    issues=_validate_a2_rotation_focus_coverage(post,{'A2_BOTTLENECK_CONTEXT':{}},set(frozen['candidate_ids']))
    assert issues==fixture['postpolicy_reason_codes']
    assert issues==['A2_ROTATION_REVIEW_INVALID:'+DIRECTION+':REASON_CODES_REQUIRED',
        'A2_ROTATION_FOCUS_COVERAGE_MISSING:'+DIRECTION]
    original_raw=json.loads(fixture['captured']['output_json']);corrected=deepcopy(original_raw)
    # Declared controlled model judgment only. Not a server-generated reason,
    # not a newly captured provider answer, and not a positive code enum.
    corrected['rotation_reviews'][0]['reason_codes']=['LOW_CONVICTION']
    assert validate(corrected,fixture).valid
    post['rotation_reviews'][0]['reason_codes']=['LOW_CONVICTION']
    assert _validate_a2_rotation_focus_coverage(post,{'A2_BOTTLENECK_CONTEXT':{}},set(frozen['candidate_ids']))==[]
    assert original_raw['rotation_reviews'][0]['reason_codes']==[]


@pytest.mark.parametrize('codes',[[],[''],['  '],['A','A'],['A']*5,None,42,[{}]])
def test_selected_board_reason_invalid_shape_is_field_specific(codes):
    fixture=original();raw=json.loads(fixture['captured']['output_json'])
    raw['rotation_reviews'][0]['reason_codes']=codes
    checked=validate(raw,fixture)
    assert 'A2_ROTATION_REASON_CODES_INVALID:'+DIRECTION in checked.issues
    review=feedback(raw,fixture,checked.issues)
    row=next(r for r in review['violations'] if r['field']=='rotation_reviews[0].reason_codes')
    assert row['actual']==codes and row['allowed']['minItems']==1


def test_same_existing_entry_gate_exclusion_not_new_reason_enum():
    fixture=original();raw=json.loads(fixture['captured']['output_json'])
    raw['rotation_reviews'][0]['reason_codes']=['NO_NEW_ENTRY']
    checked=validate(raw,fixture)
    assert 'A2_ROTATION_REASON_CODES_ENTRY_GATE_ONLY:'+DIRECTION in checked.issues
    rule=rotation_reason_contract(fixture['frozen']['direction_symbols'])[DIRECTION]
    assert set(rule['forbidden_as_only_reasons'])==ENTRY_GATE_ONLY_REASON_CODES
    raw['rotation_reviews'][0]['reason_codes']=['NO_NEW_ENTRY','LOW_CONVICTION']
    assert validate(raw,fixture).valid  # Existing posterior rejects ONLY-all gate codes.


def test_posterior_feedback_resolves_colon_direction_to_original_exact_field():
    fixture=original();raw=json.loads(fixture['captured']['output_json'])
    review=feedback(raw,fixture,fixture['postpolicy_reason_codes'])
    assert len(review['violations'])==2
    assert all(row['field']=='rotation_reviews[0].reason_codes' and row['actual']==[] for row in review['violations'])


def test_independent_missing_direction_is_not_misreported_as_bad_reason_code():
    fixture=original();raw=json.loads(fixture['captured']['output_json']);raw['rotation_reviews']=[]
    review=feedback(raw,fixture,['A2_ROTATION_FOCUS_COVERAGE_MISSING:'+DIRECTION])
    assert not any(row['field'].endswith('.reason_codes') for row in review['violations'])
    assert review['violations'][0]['field']=='rotation_reviews[0]'
    assert review['violations'][0]['actual'] is None
    assert review['violations'][0]['allowed']['rotation_direction_id']==DIRECTION
    assert DIRECTION in review['required_no_focus_if_focus_unchanged']


def test_independent_short_explanation_feedback_has_actual_field_and_unchanged_threshold():
    fixture=original();raw=json.loads(fixture['captured']['output_json'])
    raw['rotation_reviews'][0].update(reason_codes=['LOW_CONVICTION'],explanation='short')
    review=feedback(raw,fixture,['A2_ROTATION_REVIEW_INVALID:'+DIRECTION+':EXPLANATION_REQUIRED'])
    assert review['violations'][0]['field']=='rotation_reviews[0].explanation'
    assert review['violations'][0]['actual']=='short'
    assert review['violations'][0]['allowed']['minLength']==10


def test_actual_a2_request_retry_sees_exact_contract_and_keeps_first_capture_invalid(tmp_path):
    from test_team_v2_a2_research import fixture
    def model_answer(raw,attempt):
        raw['rotation_reviews']=[dict(rotation_direction_id=DIRECTION,decision='NO_FOCUS',
            reviewed_symbols=['000002.SZ'],reason_codes=[] if attempt==1 else ['LOW_CONVICTION'],
            explanation='Frozen candidates have insufficient relative conviction.',
            supporting_refs=['A2:QUANT:000002.SZ'],opposing_refs=['A2:NEWS'])]
    app,args,seen=fixture(tmp_path,model_answer,rotation=True)
    for row in args['snapshot'].data['A2_BOTTLENECK_CONTEXT'].values():
        row['rotation_direction_id']=DIRECTION
    args['snapshot'].data['A2_BOTTLENECK_CONTEXT']['000001.SZ']['top_rotation_theme']=False
    result=app._run_stage(**args)
    assert result.status=='VALIDATED' and len(seen)==2,result.reason_codes
    first=json.loads(seen[0]['messages'][-1]['content'].removeprefix('RUNTIME_INPUT\n'))
    second=json.loads(seen[1]['messages'][-1]['content'].removeprefix('RUNTIME_INPUT\n'))
    contract=first['server_output_contract']['rotation_reason_codes_by_direction'][DIRECTION]
    assert contract['minItems']==1
    field=second['validation_feedback']['violations'][0]
    assert field['field']=='rotation_reviews[0].reason_codes' and field['actual']==[]
    assert field['allowed']==contract
    with app.team_research_adapter.journal._connect() as db:
        rows=db.execute('select validated,output_json from model_calls order by rowid').fetchall()
    assert [row[0] for row in rows]==[0,1]
    assert json.loads(rows[0][1])['rotation_reviews'][0]['reason_codes']==[]
