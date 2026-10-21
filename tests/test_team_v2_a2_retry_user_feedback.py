"""B1 correction data must reach the actual last user and its durable binding."""
from contextlib import closing
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
from pathlib import Path
from types import MappingProxyType

import pytest

from liangjian_funnel.pipeline.team_v2_a2_recovery import bind_a2_retry_feedback, validation_feedback
from liangjian_funnel.pipeline.team_v2_prompts import TeamMessageRequest, TeamPromptRequest
from test_team_v2_a2_research import fixture


def message_hash(messages):
    return hashlib.sha256(json.dumps([dict(row) for row in messages], ensure_ascii=False,
        sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def request(role='A2', session='CLOSE_REVIEW'):
    system = 'frozen system 字节'
    prompt = TeamPromptRequest(role, 'shared', 'role-version', system,
        hashlib.sha256(system.encode()).hexdigest(), MappingProxyType({'original.txt': 'a'*64}))
    runtime = dict(required_envelope={'stage_id': role}, session=session, catalog_sha256='b'*64,
        inputs={'UNCHANGED_FACTS': [1, 2, 3]})
    user = 'RUNTIME_INPUT\n'+json.dumps(runtime, ensure_ascii=False, sort_keys=True,
        separators=(',', ':'), allow_nan=False)
    messages = (MappingProxyType({'role': 'system', 'content': system}),
        MappingProxyType({'role': 'user', 'content': user}))
    return TeamMessageRequest(prompt, messages, message_hash(messages), 'b'*64)


def feedback():
    return dict(validation_errors=['A2_STAGE_POLICY_MISMATCH'], violations=[
        dict(code='A2_STAGE_POLICY_MISMATCH', field='theme_reviews[0].stage', actual='LATENT')])


def runtime(request):
    return json.loads(request.messages[1]['content'].removeprefix('RUNTIME_INPUT\n'))


def test_first_request_is_identical_without_feedback():
    original = request()
    assert bind_a2_retry_feedback(original, None, correction_attempt=1) is original


def test_retry_user_carries_feedback_without_rewriting_any_original_input_or_system():
    original = request(); source_feedback = feedback(); before = deepcopy(source_feedback)
    result = bind_a2_retry_feedback(original, source_feedback, correction_attempt=2, engine_emotion_cycle='LATENT')
    assert result.prompt is original.prompt
    assert result.messages[0] is original.messages[0] and len(result.messages) == 2
    assert result.evidence_catalog_hash == original.evidence_catalog_hash
    for key, value in runtime(original).items():
        assert runtime(result)[key] == value
    value = runtime(result)
    assert value['validation_feedback'] == source_feedback and value['correction_attempt'] == 2
    assert 'LATENT' in value['field_contracts']['market_read.emotion_cycle']['allowed_values']
    assert 'LATENT' in value['field_contracts']['theme_reviews[*].stage']['allowed_values']
    assert result.input_hash == message_hash(result.messages) != original.input_hash
    assert source_feedback == before and 'validation_feedback' not in runtime(original)
    source_feedback['violations'][0]['actual'] = 'MUTATED_AFTER_BINDING'
    assert runtime(result)['validation_feedback']['violations'][0]['actual'] == 'LATENT'
    third = bind_a2_retry_feedback(original, before, correction_attempt=3, engine_emotion_cycle='LATENT')
    assert third.input_hash != result.input_hash


@pytest.mark.parametrize('role,session,attempt', [
    ('A3', 'CLOSE_REVIEW', 2), ('A4', 'INTRADAY', 2), ('A2', 'INTRADAY_RADAR', 2),
    ('A2', 'CLOSE_REVIEW', 1), ('A2', 'CLOSE_REVIEW', 4), ('A2', 'CLOSE_REVIEW', True),
])
def test_only_a2_close_review_semantic_retries_may_bind(role, session, attempt):
    with pytest.raises(ValueError, match='FEEDBACK_SCOPE_INVALID'):
        bind_a2_retry_feedback(request(role, session), feedback(), correction_attempt=attempt)


def test_old_input_hash_cannot_be_silently_repaired_by_the_projection():
    original = request()
    forged = replace(original, messages=(original.messages[0], {'role': 'user', 'content': 'different'}))
    with pytest.raises(ValueError, match='MESSAGE_BINDING_MISMATCH'):
        bind_a2_retry_feedback(forged, feedback(), correction_attempt=2)
    derived = bind_a2_retry_feedback(original, feedback(), correction_attempt=2)
    with pytest.raises(ValueError, match='ALREADY_BOUND'):
        bind_a2_retry_feedback(derived, feedback(), correction_attempt=3)


def test_feedback_cannot_replace_envelope_catalog_or_other_runtime_fields():
    original = request(); supplied = feedback()
    supplied.update(required_envelope={'stage_id': 'FORGED'}, catalog_sha256='FORGED', session='FORGED')
    derived = bind_a2_retry_feedback(original, supplied, correction_attempt=2)
    assert runtime(derived)['required_envelope'] == runtime(original)['required_envelope']
    assert runtime(derived)['catalog_sha256'] == original.evidence_catalog_hash
    assert runtime(derived)['session'] == 'CLOSE_REVIEW'


def test_all_thirteen_bad_theme_stages_have_precise_fields_actual_values_and_fixed_legal_table():
    raw = dict(focus_decisions=[], theme_reviews=[dict(theme_id=f'T{i}', stage='INVENTED_STAGE',
        new_entry_policy='WATCH_ONLY') for i in range(13)])
    result = validation_feedback(raw, {'items': []}, ['A2_STAGE_POLICY_MISMATCH'],
        envelope={'market_regime': 'ROTATION'}, symbols=set(), themes=set(), directions={},
        nomination_scope=set(), engine_emotion_cycle='LATENT')
    assert len(result['violations']) == 13
    for index, row in enumerate(result['violations']):
        assert row['field'] == f'theme_reviews[{index}].stage' and row['actual'] == 'INVENTED_STAGE'
        assert row['actual_new_entry_policy'] == 'WATCH_ONLY'
        assert len(row['allowed']['values']) == 10 and 'LATENT' in row['allowed']['values']
        assert row['allowed']['stage_policy']['IGNITION'] == 'PROBE_ONLY'
    raw['theme_reviews'] = [dict(stage='CONFIRMATION', new_entry_policy='WATCH_ONLY')]
    bad_policy = validation_feedback(raw, {'items': []}, ['A2_STAGE_POLICY_MISMATCH'],
        envelope={'market_regime': 'ROTATION'}, symbols=set(), themes=set(), directions={}, nomination_scope=set())
    assert bad_policy['violations'][0]['field'] == 'theme_reviews[0].new_entry_policy'
    assert bad_policy['violations'][0]['allowed']['values'] == ['ALLOW']


def test_actual_mock_transport_receives_last_user_correction_and_journal_binds_exact_messages(tmp_path):
    def mutate(raw, attempt):
        raw['market_read']['emotion_cycle'] = 'LATENT'
        if attempt == 1:
            raw['theme_reviews'][0].update(stage='INVENTED_STAGE', new_entry_policy='WATCH_ONLY')
    app, args, seen = fixture(tmp_path, mutate)
    args['snapshot'] = replace(args['snapshot'], data={**args['snapshot'].data,
        'MARKET_EMOTION_SNAPSHOT': {'emotion_cycle_stage': 'LATENT'}})
    original_requests = []
    prepare = app.team_research_adapter.repository.prepare_messages
    def capture(*positional, **keywords):
        prepared = prepare(*positional, **keywords); original_requests.append(prepared)
        return prepared
    app.team_research_adapter.repository.prepare_messages = capture
    result = app._run_stage(**args)
    assert result.status == 'VALIDATED' and len(seen) == 2
    from liangjian_funnel.pipeline.team_v2_prompts import with_runtime_contract
    first=with_runtime_contract(original_requests[0],result.diagnostics['rotation_output_contract'])
    assert seen[0]['messages'] == [dict(row) for row in first.messages]
    correction = json.loads(seen[1]['messages'][1]['content'].removeprefix('RUNTIME_INPUT\n'))
    assert correction['validation_feedback']['violations'][0]['field'] == 'theme_reviews[0].stage'
    assert correction['validation_feedback']['violations'][0]['actual'] == 'INVENTED_STAGE'
    assert correction['correction_attempt'] == 2
    assert seen[1]['messages'][0]['content'] == original_requests[1].messages[0]['content']
    assert original_requests[0].prompt.source_hashes == original_requests[1].prompt.source_hashes
    with closing(app.team_research_adapter.journal._connect()) as db:
        captured = db.execute('SELECT input_hash,prompt_hash,output_json,validated FROM model_calls ORDER BY captured_at').fetchall()
    assert [row[0] for row in captured] == [message_hash(body['messages']) for body in seen]
    assert [row[1] for row in captured] == [hashlib.sha256(body['messages'][0]['content'].encode()).hexdigest() for body in seen]
    assert not captured[0][3] and json.loads(captured[0][2])['theme_reviews'][0]['stage'] == 'INVENTED_STAGE'
    assert captured[1][3] and result.output['market_read']['emotion_cycle'] == 'LATENT'
    with closing(app.team_research_adapter.store._connect()) as db:
        contexts = [json.loads(row[0]) for row in db.execute(
            "SELECT body_json FROM control_minute_context WHERE decision_id LIKE 'a2-research-source:%'")]
    assert len(contexts) == 2
    for context in contexts:
        projected = context['request_transport_projection']
        from liangjian_funnel.pipeline.team_v2_a2_projection import prepare_a2_transport_values
        compact,audit=prepare_a2_transport_values(context['values'])
        assert audit['source_values_sha256']==projected['input_projection']['source_values_sha256']
        assert audit['transport_values_sha256']==projected['input_projection']['transport_values_sha256']
        original = prepare('A2', compact, required_envelope=context['envelope'], session='CLOSE_REVIEW')
        messages = [dict(original.messages[0]), {'role': 'user', 'content': projected['user_message']}]
        assert message_hash(messages) == projected['input_hash']
        assert hashlib.sha256(messages[0]['content'].encode()).hexdigest() == projected['prompt_sha256']


def test_all_three_real_isolated_replies_have_precise_final_user_theme_correction_without_mapping():
    path = Path(__file__).parent/'fixtures/team_v2_a2_isolated_invalid_stages_20261019.json'
    assert hashlib.sha256(path.read_bytes()).hexdigest() == '0ec9fa50a28c5fa82099aabf6683ce8ab1abe4eebb247441af9a692e2669dc09'
    originals = json.loads(path.read_text(encoding='utf-8'))['provider_originals']
    assert len(originals) == 3
    for captured in originals:
        raw = captured['provider_raw']; before = deepcopy(raw)
        issues = json.loads(captured['issues_json'])
        assert 'A2_STAGE_POLICY_MISMATCH' in issues
        assert set(issues) <= {'A2_STAGE_POLICY_MISMATCH', 'A2_ROTATION_DOMAIN_MISMATCH'}
        rows = raw['theme_reviews']; assert len(rows) == 13
        actual = validation_feedback(raw, {'items': []}, ['A2_STAGE_POLICY_MISMATCH'],
            envelope={'market_regime': raw['envelope']['market_regime']},
            symbols=set(), themes={row['theme_id'] for row in rows}, directions={},
            nomination_scope=set(), engine_emotion_cycle='LATENT')
        # Historical raw/journal keeps its old issues, but the newly unified
        # contract accepts all thirteen LATENT+WATCH_ONLY theme opinions.
        assert actual['violations'] == []
        derived = bind_a2_retry_feedback(request(), actual, correction_attempt=2, engine_emotion_cycle='LATENT')
        last = runtime(derived)
        from liangjian_funnel.pipeline.emotion_vocabulary import POLICIES
        assert all(POLICIES[row['stage']]==row['new_entry_policy'] for row in rows)
        assert last['field_contracts']['market_read.emotion_cycle']['engine_fact'] == 'LATENT'
        assert len(last['field_contracts']['theme_reviews[*].stage']['allowed_values'])==10
        assert '十阶段' in last['field_contracts']['market_theme_boundary']
        assert raw == before and derived.input_hash == message_hash(derived.messages)


def test_projection_removes_only_redundant_per_stock_emotion_and_keeps_engine_snapshot_permissions(tmp_path):
    from liangjian_funnel.pipeline.research.common import _project_a2_bottleneck_context
    app, args, seen = fixture(tmp_path)
    data = deepcopy(dict(args['snapshot'].data))
    market = {'emotion_cycle_stage': 'LATENT', 'new_long_permission': 'WATCH_ONLY', 'breadth': 0.421}
    data['MARKET_EMOTION_SNAPSHOT'] = deepcopy(market)
    for row in data['A2_BOTTLENECK_CONTEXT'].values():
        row['market_emotion_cycle'] = dict(stage='LATENT', new_entry_policy='WATCH_ONLY',
            new_long_permission='WATCH_ONLY')
    args['snapshot'] = replace(args['snapshot'], data=data)
    before = deepcopy(dict(args['snapshot'].data))
    projected = _project_a2_bottleneck_context(data['A2_BOTTLENECK_CONTEXT'], args['upstream_symbols'])
    for symbol in args['upstream_symbols']:
        assert 'emotion_cycle' not in projected[symbol]
        assert 'market_emotion_cycle' not in projected[symbol]
        assert before['A2_BOTTLENECK_CONTEXT'][symbol]['market_emotion_cycle']['stage'] == 'LATENT'
    result = app._run_stage(**args)
    assert result.status == 'VALIDATED' and len(seen) == 1
    assert dict(args['snapshot'].data) == before
    assert args['snapshot'].data['MARKET_EMOTION_SNAPSHOT'] == market
    with closing(app.team_research_adapter.store._connect()) as db:
        frozen = json.loads(db.execute(
            "SELECT body_json FROM control_minute_context WHERE decision_id LIKE 'a2-research-source:%'").fetchone()[0])
    values = frozen['values']; rows = values['A2_BOTTLENECK_CONTEXT']
    catalog = {row['id']: row['sha256'] for row in values['EVIDENCE_CATALOG']['items']}
    for symbol in args['upstream_symbols']:
        assert 'emotion_cycle' not in rows[symbol]
        expected = hashlib.sha256(json.dumps(rows[symbol], sort_keys=True, ensure_ascii=False,
            separators=(',', ':'), allow_nan=False).encode()).hexdigest()
        assert catalog['A2:QUANT:'+symbol] == expected
    assert values['MARKET_EMOTION_SNAPSHOT'] == market
    assert 'validation_feedback' not in json.loads(
        seen[0]['messages'][1]['content'].removeprefix('RUNTIME_INPUT\n'))
