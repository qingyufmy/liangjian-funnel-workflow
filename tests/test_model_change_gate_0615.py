"""0615 opt-in normalization proof. All inputs below are explicit offline mocks."""
from copy import deepcopy
from contextlib import closing
import json
import sqlite3
from pathlib import Path

import pytest

from liangjian_funnel.runtime import model_change_gate as m
from test_model_change_gate_0589 import quality_fixture, put, hashed, canonical, update
from test_team_v2_a2_research import fixture


def proof_fixture(tmp_path, *, contradiction=False, demotion=False):
    def wrong(raw, _):
        raw['focus_decisions'][0]['symbol'] = '000001.SH'
        if contradiction:
            raw['rotation_reviews'] = [dict(rotation_direction_id='D', decision='NO_FOCUS',
                reviewed_symbols=['000002.SZ'], reason_codes=['OBSERVE'],
                explanation='Declared offline contradictory explanatory row',
                supporting_refs=['A2:QUANT:000002.SZ'], opposing_refs=['A2:NEWS'])]
    app, kwargs, seen = fixture(tmp_path / 'pipeline', wrong, rotation=contradiction or demotion)
    if demotion:
        for context in kwargs['snapshot'].data['A2_BOTTLENECK_CONTEXT'].values():
            context['a2_factor_scores']['leader_structure']['available'] = False
            context['a2_factor_scores']['breadth']['available'] = False
    result = app._run_stage(**kwargs)
    assert result.status == 'VALIDATED' and len(seen) == 1, result.reason_codes
    with closing(app.team_research_adapter.journal._connect()) as db:
        db.row_factory = sqlite3.Row
        row = dict(db.execute('select * from model_calls').fetchone())
        evidence = dict(db.execute('select * from model_call_evidence').fetchone())
    f = quality_fixture(tmp_path / 'quality', source_trade_date=row['source_trade_date'],
        execution_trade_date=row['execution_trade_date'], models={r: row['model'] for r in m.ROLES},
        role_run_ids={r: row['run_id'] if r == 'A2' else 'DECLARED_' + r for r in m.ROLES})
    original = f['originals']['A2']; old_id = original['journal_row']['call_id']
    original['journal_row'] = row; original['model_call_evidence'] = evidence
    ref = next(r for r in f['aggregate']['calls'] if r['quality_role'] == 'A2')
    ref['call_id'] = row['call_id']
    wire = next(w for w in f['ledger']['dispatches'] if w['call_id'] == old_id)
    wire.update({k: row[k] for k in ('call_id', 'run_id', 'input_hash', 'catalog_hash', 'prompt_hash', 'model')})
    # This synthetic wire fixture already owns a fixed time sequence; no claim
    # that its synthetic dispatch times describe a real provider is made.
    update(f, role='A2', ledger=True)
    adapter = app.team_research_adapter
    audit = result.diagnostics['canonicalization_audits'][0]
    identity = row['call_id'].removeprefix('A2:CLOSE_REVIEW:')
    def context(key):
        body = adapter.store.read_control_context(key, source_sha256=row['input_hash'])
        return put(tmp_path / (key.split(':')[0] + '.json'),
                   dict(source_sha256=row['input_hash'], body=body))
    proof = dict(f['common'], schema_version='model-quality-a2-normalization/0615-v1',
        approval_sha256=m.NORMALIZATION_APPROVAL_0615, source_call_id=row['call_id'],
        input_hash=row['input_hash'], catalog_hash=row['catalog_hash'], prompt_hash=row['prompt_hash'],
        provider_raw_sha256=row['output_hash'], canonical_sha256=audit['canonical_sha256'],
        postpolicy_sha256=hashed(result.output), request_ref=put(tmp_path / 'request.json',
            dict(capture_kind='CAPTURED_MESSAGES', messages=seen[0]['messages'])),
        canonical_context_ref=context('a2-canonicalization:' + identity),
        postpolicy_context_ref=context('a2-research-postpolicy:' + identity))
    if demotion:
        proof['policy_context_ref'] = context('a2-research-policy-demotion:' + identity)
    f.update(proof=proof, cid=row['call_id'], proof_path=tmp_path / 'proof.json')
    return f


def validate(f, *, approved=True):
    mapping = {f['cid']: put(f['proof_path'], f['proof'])} if approved else None
    return m.validate_model_change_gate(f['ref']['path'], expected_sha256=f['ref']['sha256'],
                                       approved_normalizations=mapping, **f['common'])


@pytest.mark.parametrize('contradiction,demotion', [(False, False), (True, False), (False, True), (True, True)])
def test_existing_mechanical_contracts_pass_only_explicit_proof(tmp_path, contradiction, demotion):
    f = proof_fixture(tmp_path, contradiction=contradiction, demotion=demotion)
    original = deepcopy(f['originals']['A2'])
    with pytest.raises(ValueError, match='BATCH_FINAL_RAW_NOT_VALIDATED'):
        validate(f, approved=False)
    assert validate(f)['actual_http'] == 4
    assert f['originals']['A2'] == original and original['journal_row']['validated'] == 0
    assert json.loads(original['journal_row']['issues_json'])


@pytest.mark.parametrize('field,value', [('source_call_id', 'OTHER'), ('input_hash', 'a' * 64),
    ('catalog_hash', 'b' * 64), ('provider_raw_sha256', 'c' * 64), ('prompt_hash', 'd' * 64),
    ('canonical_sha256', 'e' * 64), ('postpolicy_sha256', 'f' * 64), ('approval_sha256', '0' * 64)])
def test_cross_call_catalog_input_output_approval_rejected(tmp_path, field, value):
    f = proof_fixture(tmp_path); f['proof'][field] = value
    with pytest.raises(ValueError, match='MODEL_GATE_'):
        validate(f)


@pytest.mark.parametrize('change', ['focus', 'score', 'unknown-rule', 'no-actions', 'missing-postpolicy',
                                    'postpolicy-error', 'cross-context', 'changed-request', 'fake-pass'])
def test_claimed_audit_does_not_authorize_other_changes(tmp_path, change):
    f = proof_fixture(tmp_path)
    key = 'canonical_context_ref'; ref = f['proof'][key]
    body = json.loads(Path(ref['path']).read_bytes())
    if change == 'focus': body['body']['canonical_raw']['focus_decisions'] = []
    if change == 'score': body['body']['canonical_raw']['theme_reviews'][0]['theme_score'] = 100
    if change == 'unknown-rule': body['body']['actions'][0]['rule_id'] = 'INVENTED_APPROVAL'
    if change == 'no-actions': body['body']['actions'] = []
    if change == 'cross-context': body['source_sha256'] = '0' * 64
    if change == 'fake-pass': body['body'] = {'status': 'PASSED', 'source_call_id': f['cid']}
    f['proof'][key] = put(Path(ref['path']), body)
    if change == 'missing-postpolicy': del f['proof']['postpolicy_context_ref']
    if change == 'postpolicy-error':
        ref = f['proof']['postpolicy_context_ref']; body = json.loads(Path(ref['path']).read_bytes())
        body['body']['reason_codes'] = ['REAL_POSTPOLICY_FAILURE']; f['proof']['postpolicy_context_ref'] = put(Path(ref['path']), body)
    if change == 'changed-request':
        ref = f['proof']['request_ref']; body = json.loads(Path(ref['path']).read_bytes())
        body['messages'][1]['content'] += ' '; f['proof']['request_ref'] = put(Path(ref['path']), body)
    with pytest.raises(ValueError, match='MODEL_GATE_'):
        validate(f)


def test_non_a2_and_unreferenced_normalization_proof_rejected(tmp_path):
    f = proof_fixture(tmp_path)
    ref = put(f['proof_path'], f['proof'])
    for cid in ('DECLARED_CALL_A6', 'UNKNOWN_CALL'):
        with pytest.raises(ValueError, match='NORMALIZATION_UNUSED_OR_NON_A2_PROOF'):
            m.validate_model_change_gate(f['ref']['path'], expected_sha256=f['ref']['sha256'],
                approved_normalizations={cid: ref}, **f['common'])


@pytest.mark.parametrize('mutate', [None, 'missing', 'wrong-user', 'wrong-source-call', 'wrong-catalog'])
def test_hash_verified_reconstruction_requires_original_frozen_source(tmp_path, mutate):
    f = proof_fixture(tmp_path)
    ref = f['proof']['request_ref']; request = json.loads(Path(ref['path']).read_bytes())
    runtime = json.loads(request['messages'][1]['content'].removeprefix('RUNTIME_INPUT\n'))
    source = dict(decision_id='a2-research-source:' + f['cid'].removeprefix('A2:CLOSE_REVIEW:'),
        source_sha256=f['proof']['input_hash'], body=dict(envelope=runtime['required_envelope'],
            values={'EVIDENCE_CATALOG': json.loads(f['originals']['A2']['model_call_evidence']['catalog_json'])},
            request_transport_projection=dict(input_hash=f['proof']['input_hash'], prompt_sha256=f['proof']['prompt_hash'],
                user_message=request['messages'][1]['content'])))
    request['capture_kind'] = 'HASH_VERIFIED_RECONSTRUCTION_FROM_ORIGINAL_CONTEXT'
    if mutate == 'wrong-user': source['body']['request_transport_projection']['user_message'] += ' '
    if mutate == 'wrong-source-call': source['decision_id'] += 'OTHER'
    if mutate == 'wrong-catalog': source['body']['values']['EVIDENCE_CATALOG'] = {}
    if mutate != 'missing': request['source_context_ref'] = put(tmp_path / 'frozen-source.json', source)
    f['proof']['request_ref'] = put(Path(ref['path']), request)
    if mutate:
        with pytest.raises(ValueError, match='MODEL_GATE_'): validate(f)
    else:
        assert validate(f)['actual_http'] == 4


def test_ambiguous_suffix_never_becomes_an_approved_mechanical_action():
    from liangjian_funnel.pipeline.team_v2_symbol_suffix import canonicalize_a2_symbols
    raw = dict(focus_decisions=[{'symbol': '000001.BJ'}])
    derived, actions = canonicalize_a2_symbols(raw, candidate_ids={'000001.SH', '000001.SZ'},
        nomination_scope=set(), direction_symbols={}, catalog={'items': []})
    assert derived == raw and actions == []


@pytest.mark.parametrize('kind', ['missing', 'invented-rule', 'changed-explanation', 'changed-focus'])
def test_server_demotion_explanation_requires_exact_existing_helper_audit(tmp_path, kind):
    f = proof_fixture(tmp_path, demotion=True)
    if kind == 'missing':
        del f['proof']['policy_context_ref']
    else:
        key = 'policy_context_ref' if kind == 'invented-rule' else 'postpolicy_context_ref'
        ref = f['proof'][key]; context = json.loads(Path(ref['path']).read_bytes())
        if kind == 'invented-rule': context['body']['rule_id'] = 'INVENTED_PERMISSION'
        if kind == 'changed-explanation':
            context['body']['output']['rotation_reviews'][0]['explanation'] = 'Invented server judgment'
        if kind == 'changed-focus': context['body']['output']['focus_pool'] = [{'symbol': '999999.SZ'}]
        if key == 'postpolicy_context_ref': f['proof']['postpolicy_sha256'] = hashed(context['body']['output'])
        f['proof'][key] = put(Path(ref['path']), context)
    with pytest.raises(ValueError, match='MODEL_GATE_'): validate(f)
