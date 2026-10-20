"""0417: real sample facts need their real namespace, never an invented alias."""
from copy import deepcopy
from contextlib import closing
import hashlib
import json

import pytest

from test_team_v2_a2_research import fixture
from test_team_v2_nomination_source import add_filtered
from liangjian_funnel.pipeline.team_v2_contract import validate_team_output


def sha(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
        separators=(',', ':'), allow_nan=False).encode()).hexdigest()


@pytest.mark.parametrize('symbol', ['603276.SH', '002674.SZ', '603978.SH'])
def test_actual_0417_unknown_quant_ref_is_still_rejected_despite_real_nomination_fact(symbol):
    # These exact ID pairs are captured in run225716. This small pure schema
    # counterexample is not a claim that its fabricated text is a provider reply.
    catalog = dict(categories=['NEWS', 'QUANT'], items=[
        dict(id='A2:NEWS', category='NEWS', sha256='n'),
        dict(id='A2:NOMINATION:' + symbol, category='QUANT', sha256='f')])
    raw = dict(handoff_notes={'to_a3': ['Frozen']}, what_would_change_my_mind=['Evidence changes'],
        theme_reviews=[dict(theme_id='T', supporting_refs=['A2:NEWS'],
            opposing_refs=['A2:QUANT:' + symbol])])
    result = validate_team_output('A2', raw, catalog)
    assert result.issues == ('TEAM_OPPOSING_REFS_INVALID:theme_reviews[0]',)
    assert raw['theme_reviews'][0]['opposing_refs'] == ['A2:QUANT:' + symbol]


def test_model_input_sample_has_unique_real_id_with_original_fact_hash_and_no_source_mutation(tmp_path):
    app, args, seen = fixture(tmp_path)
    add_filtered(args)
    before = deepcopy(dict(args['snapshot'].data))
    result = app._run_stage(**args)
    assert result.status == 'VALIDATED'
    with closing(app.team_research_adapter.store._connect()) as db:
        frozen = json.loads(db.execute("SELECT body_json FROM control_minute_context "
            "WHERE decision_id LIKE 'a2-research-source:%'").fetchone()[0])
    row = frozen['values']['A2_QUANT_FILTERED_SAMPLE'][0]
    assert not {'evidence_ref', 'evidence_role', 'source_fact_sha256'}.intersection(row)
    binding = frozen['request_transport_projection']['input_projection']['nomination_evidence_projection']['bindings'][0]
    assert binding['evidence_ref'] == 'A2:NOMINATION:000004.SZ'
    assert binding['evidence_role'] == 'FILTERED_NOMINATION_ONLY'
    assert binding['source_fact_sha256'] == sha(row)
    catalog = frozen['values']['EVIDENCE_CATALOG']
    assert next(item['sha256'] for item in catalog['items'] if item['id'] == binding['evidence_ref']) == sha(row)
    assert not any(item['id'] == 'A2:QUANT:000004.SZ' for item in catalog['items'])
    assert binding['evidence_ref'] in seen[0]['messages'][0]['content']
    assert 'evidence_ref' in seen[0]['messages'][0]['content']
    assert dict(args['snapshot'].data) == before
    from liangjian_funnel.pipeline.team_v2_a2_references import prepare_nomination_evidence_values, strip_nomination_evidence
    from liangjian_funnel.pipeline.team_v2_a2_projection import prepare_a2_transport_values, expand_a2_transport_values
    model_values, binding_audit = prepare_nomination_evidence_values(frozen['values'])
    compact, _ = prepare_a2_transport_values(model_values)
    expanded = expand_a2_transport_values(compact)
    assert expanded == model_values and binding_audit['original_values_sha256'] == sha(frozen['values'])
    expanded['A2_QUANT_FILTERED_SAMPLE'] = strip_nomination_evidence(expanded['A2_QUANT_FILTERED_SAMPLE'], catalog)
    assert expanded == frozen['values']  # Explicit two-layer inverse, no false original-fact identity.
    assert 'source_fact_sha256' not in model_values['A2_QUANT_FILTERED_SAMPLE'][0]
    replay = app.team_research_adapter.repository.prepare_messages('A2', compact,
        required_envelope=frozen['envelope'], session='CLOSE_REVIEW')
    assert [dict(message) for message in replay.messages] == seen[0]['messages']
    assert replay.input_hash == frozen['request_transport_projection']['input_hash']


def test_explicit_same_fact_feedback_corrects_mock_next_reply_but_never_provider_capture(tmp_path):
    def reply(raw, attempt):
        raw['theme_reviews'][0]['opposing_refs'] = ['A2:QUANT:000004.SZ']
        if attempt > 1:
            runtime = json.loads(seen[-1]['messages'][1]['content'].split('\n', 1)[1])
            suggestions = runtime['validation_feedback']['reference_feedback']['same_fact_namespace_suggestions']
            suggestion = next(row for row in suggestions if row['invalid_reference_id'] == 'A2:QUANT:000004.SZ')
            assert suggestion['field'] == 'theme_reviews[0].opposing_refs'
            assert suggestion['source_fact_sha256'] and suggestion['evidence_role'] == 'FILTERED_NOMINATION_ONLY'
            raw['theme_reviews'][0]['opposing_refs'] = [suggestion['legal_reference_id']]
    app, args, seen = fixture(tmp_path, reply)
    add_filtered(args)
    result = app._run_stage(**args)
    assert result.status == 'VALIDATED' and len(seen) == 2
    with closing(app.team_research_adapter.journal._connect()) as db:
        captures = db.execute('SELECT validated,output_json FROM model_calls ORDER BY captured_at').fetchall()
    assert [row[0] for row in captures] == [0, 1]
    assert json.loads(captures[0][1])['theme_reviews'][0]['opposing_refs'] == ['A2:QUANT:000004.SZ']
    assert json.loads(captures[1][1])['theme_reviews'][0]['opposing_refs'] == ['A2:NOMINATION:000004.SZ']
    assert result.output['model_nominations'] == []  # Reference suggestion is not nomination or execution permission.


@pytest.mark.parametrize('symbol', ['603276.SH', '002674.SZ', '603978.SH'])
def test_model_ignoring_real_namespace_suggestion_stays_blocked_three_times(tmp_path, symbol):
    def reply(raw, attempt):
        raw['theme_reviews'][0]['opposing_refs'] = ['A2:QUANT:' + symbol]
    app, args, seen = fixture(tmp_path, reply)
    add_filtered(args, symbol=symbol)
    result = app._run_stage(**args)
    assert result.status == 'BLOCKED' and len(seen) == 3
    assert result.reason_codes == ('TEAM_OPPOSING_REFS_INVALID:theme_reviews[0]',)
    for request in seen[1:]:
        runtime = json.loads(request['messages'][1]['content'].split('\n', 1)[1])
        suggestion = runtime['validation_feedback']['reference_feedback']['same_fact_namespace_suggestions'][0]
        assert suggestion['invalid_reference_id'] == 'A2:QUANT:' + symbol
        assert suggestion['legal_reference_id'] == 'A2:NOMINATION:' + symbol
    with closing(app.team_research_adapter.journal._connect()) as db:
        captures = db.execute('SELECT validated,output_json FROM model_calls').fetchall()
    assert [row[0] for row in captures] == [0, 0, 0]
    assert all(json.loads(row[1])['theme_reviews'][0]['opposing_refs'] == ['A2:QUANT:' + symbol]
        for row in captures)


@pytest.mark.parametrize('change', ['unknown', 'mismatch_sha', 'duplicate', 'wrong_ref'])
def test_same_fact_suggestion_never_proposes_unproven_namespace(change):
    from liangjian_funnel.pipeline.team_v2_a2_references import bind_nomination_evidence, same_fact_namespace_suggestions
    sample = [dict(symbol='000004.SZ', theme_id='T', quant_filter_reason='ORIGINAL')]
    catalog = dict(categories=['QUANT'], items=[dict(id='A2:NOMINATION:000004.SZ', category='QUANT', sha256=sha(sample[0]))])
    annotated = bind_nomination_evidence(sample, catalog)
    before = deepcopy(annotated)
    if change == 'unknown': invalid = 'A2:QUANT:999999.SZ'
    else: invalid = 'A2:QUANT:000004.SZ'
    if change == 'mismatch_sha': annotated[0]['source_fact_sha256'] = '0' * 64
    if change == 'duplicate': annotated.append(deepcopy(annotated[0]))
    if change == 'wrong_ref': annotated[0]['evidence_ref'] = 'A2:NOMINATION:999999.SZ'
    raw = {'theme_reviews': [{'opposing_refs': [invalid]}]}
    assert same_fact_namespace_suggestions(raw, catalog, ['TEAM_OPPOSING_REFS_INVALID:theme_reviews[0]'], annotated) == []
    if change == 'unknown': assert annotated == before


def test_binding_fails_instead_of_overwriting_reserved_or_unbound_source_values():
    from liangjian_funnel.pipeline.team_v2_a2_references import bind_nomination_evidence
    sample = [dict(symbol='000004.SZ', theme_id='T', quant_filter_reason='ORIGINAL')]
    catalog = dict(categories=['QUANT'], items=[dict(id='A2:NOMINATION:000004.SZ', category='QUANT', sha256=sha(sample[0]))])
    for key in ('evidence_ref', 'evidence_role', 'source_fact_sha256'):
        invalid = deepcopy(sample)
        invalid[0][key] = 'FOREIGN'
        with pytest.raises(ValueError, match='NOMINATION_EVIDENCE'):
            bind_nomination_evidence(invalid, catalog)
    with pytest.raises(ValueError, match='NOMINATION_EVIDENCE'):
        bind_nomination_evidence(sample, dict(categories=['QUANT'], items=[]))
