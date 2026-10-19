"""Bounded A2 feedback and audited, non-decision rotation coverage projections.

Neither function changes the provider capture or the original validator.  Only
explicitly authorized NO_FOCUS coverage noise can enter a derived projection.
"""
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
from types import MappingProxyType

from .team_v2_a2_schema import _ENGINE_EMOTION_STAGES, _FIELDS, _OPTIONAL, _POLICIES, _ROOT
from .team_v2_contract import validate_team_output


def _rows(raw, field):
    value = raw.get(field)
    return [row for row in value if isinstance(row, Mapping)] if isinstance(value, list) else []


def _message_input_hash(messages):
    # Exactly the serialization used by TeamPromptRepository.prepare_messages
    # and the call_team_model message-binding guard, including UTF-8 and NaN.
    text = json.dumps([dict(message) for message in messages], ensure_ascii=False,
        sort_keys=True, separators=(',', ':'), allow_nan=False)
    return hashlib.sha256(text.encode()).hexdigest()


def bind_a2_retry_feedback(request, feedback, *, correction_attempt, engine_emotion_cycle=None):
    """Bind existing correction data to the last user JSON, without a new role.

    No prompt source bytes, system text, catalog, envelope, validation rule or
    model response are rewritten.  The derived message bytes own a new binding.
    """
    if feedback is None:
        return request
    if (request.prompt.role != 'A2' or type(correction_attempt) is not int
            or correction_attempt not in (2, 3) or not isinstance(feedback, Mapping)
            or not feedback.get('validation_errors')):
        raise ValueError('TEAM_A2_RETRY_FEEDBACK_SCOPE_INVALID')
    if (len(request.messages) != 2 or request.messages[0].get('role') != 'system'
            or request.messages[1].get('role') != 'user'
            or _message_input_hash(request.messages) != request.input_hash
            or hashlib.sha256(request.messages[0]['content'].encode()).hexdigest() != request.prompt.sha256):
        raise ValueError('TEAM_MESSAGE_BINDING_MISMATCH')
    user = request.messages[1]['content']
    if not user.startswith('RUNTIME_INPUT\n'):
        raise ValueError('TEAM_A2_RETRY_FEEDBACK_SCOPE_INVALID')
    runtime = json.loads(user.removeprefix('RUNTIME_INPUT\n'))
    if (not isinstance(runtime, Mapping) or runtime.get('session') != 'CLOSE_REVIEW'
            or not isinstance(runtime.get('required_envelope'), Mapping)
            or runtime['required_envelope'].get('stage_id') != 'A2'):
        raise ValueError('TEAM_A2_RETRY_FEEDBACK_SCOPE_INVALID')
    if {'validation_feedback', 'correction_attempt', 'field_contracts'}.intersection(runtime):
        raise ValueError('TEAM_A2_RETRY_FEEDBACK_ALREADY_BOUND')
    emotion_values = list(_POLICIES)
    if (isinstance(engine_emotion_cycle, str) and engine_emotion_cycle in _ENGINE_EMOTION_STAGES
            and engine_emotion_cycle not in emotion_values):
        emotion_values.append(engine_emotion_cycle)
    runtime = deepcopy(dict(runtime))
    # Never merge feedback into runtime: untrusted previous-answer values must
    # not replace the envelope, session, evidence catalog hash or input fields.
    runtime['validation_feedback'] = deepcopy(dict(feedback))
    runtime['correction_attempt'] = correction_attempt
    runtime['field_contracts'] = {
        'market_theme_boundary': 'LATENT/STARTUP/ICE_POINT 是 MARKET_EMOTION_SNAPSHOT 的市场阶段，不是题材阶段；theme_reviews[*].stage 只能从下面八个题材阶段选择，不做市场阶段到题材阶段的语义映射。',
        'market_read.emotion_cycle': dict(kind='MARKET_FACT_OR_EXISTING_A2_STAGE',
            engine_fact=engine_emotion_cycle, allowed_values=emotion_values),
        'theme_reviews[*].stage': dict(kind='THEME_JUDGMENT_NOT_MARKET_FACT', allowed_values=list(_POLICIES)),
        'theme_reviews[*].new_entry_policy': dict(allowed_by_stage=dict(_POLICIES)),
    }
    content = 'RUNTIME_INPUT\n'+json.dumps(runtime, ensure_ascii=False, sort_keys=True,
        separators=(',', ':'), allow_nan=False)
    messages = (request.messages[0], MappingProxyType({'role': 'user', 'content': content}))
    return replace(request, messages=messages, input_hash=_message_input_hash(messages))


def validation_feedback(raw, catalog, issues, *, envelope, symbols, themes, directions, nomination_scope,
                        engine_emotion_cycle=None):
    """Return concrete offending values plus the unchanged frozen legal domains."""
    source = raw if isinstance(raw, Mapping) else {}
    focused = {r.get('symbol') for r in _rows(source, 'focus_decisions') if isinstance(r.get('symbol'), str)}
    expected = {d: sorted(ss) for d, ss in directions.items() if not focused.intersection(ss)}
    known_refs = sorted(row['id'] for row in catalog['items'])
    emotion_values = list(_POLICIES)
    if (isinstance(engine_emotion_cycle, str) and engine_emotion_cycle in _ENGINE_EMOTION_STAGES
            and engine_emotion_cycle not in emotion_values):
        emotion_values.append(engine_emotion_cycle)
    laws = dict(emotion_cycle=emotion_values, engine_emotion_cycle=engine_emotion_cycle, stage_policy=dict(_POLICIES),
        candidate_ids=sorted(symbols), allowed_theme_ids=sorted(themes),
        nomination_scope=sorted(nomination_scope), evidence_reference_ids=known_refs,
        required_envelope=envelope, required_root_fields=sorted(_ROOT),
        required_item_fields={key: sorted(fields) for key, fields in _FIELDS.items()},
        required_no_focus_if_focus_unchanged=expected,
        rotation_rule='Already FOCUS directions must not appear in rotation_reviews; each remaining frozen direction requires exactly one NO_FOCUS row with 1-3 unique in-domain symbols.')
    fields = {
        'A2_RESPONSE_OBJECT_REQUIRED': '$', 'A2_RESPONSE_ROOT_FIELDS_INVALID': '$',
        'A2_ENVELOPE_BINDING_MISMATCH': 'envelope', 'A2_REASON_CODES_INVALID': 'reason_codes',
        'A2_UNRESOLVED_QUESTIONS_INVALID': 'unresolved_questions',
        'A2_HANDOFF_RECIPIENT_INVALID': 'handoff_notes', 'TEAM_HANDOFF_NOTES_INVALID': 'handoff_notes',
        'TEAM_FALSIFIERS_INVALID': 'what_would_change_my_mind',
        'A2_MARKET_READ_INVALID': 'market_read', 'A2_MARKET_REGIME_BINDING_MISMATCH': 'market_read',
        'A2_FOCUS_CODES_INVALID': 'focus_decisions', 'A2_REJECT_CODES_INVALID': 'reject_decisions',
        'A2_NOMINATION_FIELDS_INVALID': 'model_nominations', 'TEAM_NOMINATION_DOMAIN_INVALID': 'model_nominations',
        'A2_DUPLICATE_SYMBOL_DECISION': '$', 'A2_THEME_OUTSIDE_FROZEN_DOMAIN': 'theme_reviews',
        'A2_STAGE_POLICY_MISMATCH': 'theme_reviews', 'A2_THEME_FIELDS_INVALID': 'theme_reviews',
        'A2_FOCUS_THEME_COVERAGE_MISMATCH': 'theme_reviews',
        'A2_SCORE_PENALTY_CODE_UNKNOWN': 'theme_reviews',
        'A2_NOMINATION_FILTER_REASON_MISMATCH': 'model_nominations',
        'A2_NOMINATION_THEME_BINDING_MISMATCH': 'model_nominations',
        'A2_NOMINATION_ORIGINAL_FACT_REFERENCE_REQUIRED': 'model_nominations',
    }
    violations = []
    for code in issues:
        prefix, _, path = code.partition(':')
        if prefix == 'A2_STAGE_POLICY_MISMATCH':
            for index, row in enumerate(_rows(source, 'theme_reviews')):
                stage = row.get('stage'); policy = row.get('new_entry_policy')
                if not isinstance(stage, str) or stage not in _POLICIES:
                    violations.append(dict(code=code, field=f'theme_reviews[{index}].stage',
                        actual=deepcopy(stage), allowed=dict(values=list(_POLICIES), stage_policy=dict(_POLICIES)),
                        actual_new_entry_policy=deepcopy(policy)))
                elif _POLICIES[stage] != policy:
                    violations.append(dict(code=code, field=f'theme_reviews[{index}].new_entry_policy',
                        actual=deepcopy(policy), stage=stage,
                        allowed=dict(values=[_POLICIES[stage]], stage_policy=dict(_POLICIES))))
            continue
        field = path or fields.get(prefix, 'rotation_reviews' if prefix.startswith('A2_ROTATION_') else '$')
        actual = raw if field == '$' else source.get(field)
        if '[' in field:
            pool, _, index = field.partition('[')
            try:
                actual = source.get(pool, [])[int(index.rstrip(']'))]
            except (TypeError, ValueError, IndexError, KeyError):
                actual = source.get(pool)
        # The full legal frozen domains appear once, not once per violation:
        # error feedback must not multiply a large catalog into an oversized prompt.
        allowed = {'contract': 'See legal_values and the unchanged role schema'}
        if field == 'market_read':
            allowed = dict(emotion_cycle=emotion_values, regime=envelope['market_regime'],
                dominant_themes=sorted(themes))
        elif field == 'rotation_reviews':
            allowed = dict(rotation_rule=laws['rotation_rule'], required_no_focus_if_focus_unchanged=expected)
        elif field == 'theme_reviews':
            allowed = dict(stage_policy=dict(_POLICIES), allowed_theme_ids=sorted(themes))
        violations.append(dict(code=code, field=field, actual=deepcopy(actual), allowed=allowed))
    return dict(validation_errors=list(issues), violations=violations, legal_values=laws,
        unchanged_focus_symbols=sorted(focused), required_no_focus_if_focus_unchanged=expected,
        already_focused_directions_forbidden=sorted(set(directions)-set(expected)),
        authority='0361_REPLY_B_ALL_VALIDATION_FEEDBACK_MAX_THREE')


def drop_focused_rotation(raw, catalog, directions):
    """Drop safe NO_FOCUS rows already covered by a provider FOCUS decision.

    Strictly inspect *all* rows before deletion: the original schema intentionally
    skips the rest of a row once its direction is not expected.  That skip must
    not conceal a foreign stock, bad decision, invalid evidence or duplicates.
    """
    if not isinstance(raw, Mapping):
        return None
    rows = raw.get('rotation_reviews')
    if not isinstance(rows, list):
        return None
    focused = {r.get('symbol') for r in _rows(raw, 'focus_decisions') if isinstance(r.get('symbol'), str)}
    expected = {d for d, ss in directions.items() if not focused.intersection(ss)}
    seen = set()
    for row in rows:
        if (not isinstance(row, Mapping) or not _FIELDS['rotation_reviews'].issubset(row)
                or not set(row).issubset(_FIELDS['rotation_reviews'] | _OPTIONAL)):
            return None
        direction = row.get('rotation_direction_id'); symbols = row.get('reviewed_symbols')
        if (not isinstance(direction, str) or direction not in directions or direction in seen
                or row.get('decision') != 'NO_FOCUS' or not isinstance(symbols, list)
                or not 1 <= len(symbols) <= 3 or any(not isinstance(s, str) for s in symbols)
                or len(set(symbols)) != len(symbols) or not set(symbols).issubset(directions[direction])
                or not isinstance(row.get('reason_codes'), list) or len(row['reason_codes']) > 4
                or any(not isinstance(code, str) or not code.strip() for code in row['reason_codes'])
                or len(set(row['reason_codes'])) != len(row['reason_codes'])
                or not isinstance(row.get('explanation'), str) or not row['explanation'].strip()):
            return None
        seen.add(direction)
    # The original contract owns references and malformed extra fields as well.
    reference_projection = {key: raw.get(key) for key in ('handoff_notes', 'what_would_change_my_mind')}
    reference_projection['rotation_reviews'] = rows
    if not validate_team_output('A2', reference_projection, catalog).valid:
        return None
    result = deepcopy(raw); result['rotation_reviews'] = []; actions = []
    for index, row in enumerate(rows):
        if row['rotation_direction_id'] in expected:
            result['rotation_reviews'].append(deepcopy(row))
        else:
            actions.append(dict(action='SERVER_DROPPED_FOCUSED_DIRECTION', original_index=index,
                rotation_direction_id=row['rotation_direction_id'], original_row=deepcopy(row)))
    if not actions:
        return None
    return result, dict(actions=actions,
        provider_capture_unchanged=True, purpose='MECHANICAL_COVERAGE_NOT_MODEL_JUDGMENT')
