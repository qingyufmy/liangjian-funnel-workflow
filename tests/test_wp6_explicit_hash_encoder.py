"""The legacy C encoder remains an explicit option, never an env guess."""
import hashlib

import pytest

from liangjian_funnel.pipeline.a1_contract import canonical_json, stable_digest


@pytest.mark.parametrize('value', [
    {'text': '中文', 'number': 1.25},
    {1: 'integer', 2: 'second'},
    {'nan': float('nan'), 'pos': float('inf'), 'neg': float('-inf')},
    {'nested': [None, True, 1e-300, -0.0, ('tuple', 1)]},
])
def test_explicit_encoders_match_legacy_bytes(value):
    expected = hashlib.sha256(canonical_json(value).encode('utf-8')).hexdigest()
    assert stable_digest(value, encoder='C') == expected
    assert stable_digest(value, encoder='STREAM') == expected
    assert stable_digest(value) == expected


@pytest.mark.parametrize('value, error', [
    ({'text': '\ud800'}, UnicodeEncodeError),
    ({1: 'integer', 'text': 'mixed'}, TypeError),
])
def test_explicit_encoders_preserve_invalid_input_error(value, error):
    for encoder in ('C', 'STREAM'):
        with pytest.raises(error):
            stable_digest(value, encoder=encoder)


@pytest.mark.parametrize('encoder', [None, 'auto', 'c', '', True])
def test_encoder_is_explicit_not_environment_or_coercion(encoder):
    with pytest.raises(ValueError, match='CANONICAL_HASH_ENCODER_REQUIRED'):
        stable_digest({}, encoder=encoder)
