"""Local equivalence and allocation checks; retain the full fixture objects."""
import hashlib
import json
import math
import tracemalloc
from time import perf_counter
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from liangjian_funnel.pipeline import a1_contract as contract
from liangjian_funnel.pipeline import a1_packet as packet_module


def legacy_json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), default=str)


def legacy_digest(value):
    return hashlib.sha256(legacy_json(value).encode('utf-8')).hexdigest()


def legacy_tokens(text):
    ascii_chars = sum(ord(char) < 128 for char in text)
    return max(1, math.ceil(ascii_chars / 4 + (len(text) - ascii_chars) / 1.5) + 8)


def legacy_diagnostics(packet):
    sections = {str(key): len(legacy_json(value)) for key, value in packet.items()
                if str(key) not in {'packet_hash', 'diagnostics'}}
    total = len(legacy_json({key: value for key, value in packet.items() if key != 'diagnostics'}))
    tokens = legacy_tokens(legacy_json({key: value for key, value in packet.items() if key != 'diagnostics'}))
    largest = sorted(sections.items(), key=lambda item: (-item[1], item[0]))[:12]
    return {'packet_chars': total, 'estimated_input_tokens': tokens, 'section_chars': sections,
            'largest_sections': [{'section': key, 'chars': chars} for key, chars in largest],
            'raw_history_keys_excluded': ['MACRO_ECONOMIC_DATA.series', 'INDUSTRY_ACTIVITY_DATA.items'],
            'full_snapshot_retained': True}


VALUES = [None, True, 123, -0.0, [float('nan'), float('inf'), -float('inf')],
          {'z': '中文🙂é\u2028\u2029', 'a': '\"\\\n\r\t\b\f\x00'},
          {'date': datetime(2026, 10, 10, tzinfo=timezone.utc), 'decimal': Decimal('1.20'),
           'path': Path('A/B'), 'tuple': ('中', 2), 'bytes': b'ab'},
          {'long': '中🙂a\n' * 6000}, {2: 'b', 1: 'a'}, [1.2e-200, 1.2e200, 0.000001]]
VALUES.append({'controls': ''.join(chr(i) for i in range(32)),
               'unicode': ''.join(chr(i) for i in (0x2028, 0x2029, 0x1F642, 0xE9)),
               'quoted_key': '"slash\\'})


@pytest.mark.parametrize('value', VALUES)
def test_old_json_bytes_hash_lengths_and_character_counts_exact(value):
    expected = legacy_json(value)
    assert contract.canonical_json(value) == expected  # string API retained
    chunks = list(contract.canonical_json_chunks(value))
    assert ''.join(chunks).encode('utf-8') == expected.encode('utf-8')
    assert contract.canonical_json_length(value) == len(expected)
    assert contract.stable_digest(value) == legacy_digest(value)
    measured = contract.measure_canonical_json(value)
    assert measured.total_chars == len(expected)
    assert measured.ascii_chars == sum(ord(c) < 128 for c in expected)
    assert measured.non_ascii_chars == sum(ord(c) >= 128 for c in expected)
    assert packet_module._estimate_tokens(expected) == legacy_tokens(expected)


def test_digest_does_not_request_full_canonical_string(monkeypatch):
    monkeypatch.setattr(contract, 'canonical_json', lambda _: pytest.fail('whole canonical string'))
    assert contract.stable_digest({'事实': [1, 2, 3]}) == legacy_digest({'事实': [1, 2, 3]})


def test_diagnostics_do_not_request_whole_strings(monkeypatch):
    packet = {'b': ['中', 'a'], 'packet_hash': 'abc', 'diagnostics': {'old': 'excluded'}}
    expected = legacy_diagnostics(packet)
    monkeypatch.setattr(packet_module, 'canonical_json', lambda _: pytest.fail('whole diagnostic string'))
    assert packet_module.packet_diagnostics(packet) == expected


def test_section_lengths_do_not_scan_ascii_counts(monkeypatch):
    packet = {'records': [{'事实': '中文🙂'}], 'scope': 'fixture', 'packet_hash': 'h'}
    expected = legacy_diagnostics(packet)
    original = contract.measure_canonical_json
    calls = []
    def total_only(value):
        assert value == packet, 'sections must request length only, not ASCII scans'
        calls.append(value)
        return original(value)
    monkeypatch.setattr(packet_module, 'measure_canonical_json', total_only)
    assert packet_module.packet_diagnostics(packet) == expected
    assert len(calls) == 1


def test_total_measurement_once_global_ceil_not_chunk_ceil(monkeypatch):
    packet = {str(i): chr(65 + i) for i in range(15)}
    packet['packet_hash'] = 'hash-in-total-only'
    packet['diagnostics'] = {'not_in_total': 'x' * 1000}
    expected = legacy_diagnostics(packet)
    calls = []
    original = contract.measure_canonical_json
    def measure(value):
        calls.append(value)
        return original(value)
    monkeypatch.setattr(packet_module, 'measure_canonical_json', measure)
    actual = packet_module.packet_diagnostics(packet)
    assert actual == expected
    total_calls = [v for v in calls if isinstance(v, dict) and 'packet_hash' in v]
    assert len(total_calls) == 1
    assert 'diagnostics' not in total_calls[0]
    assert 'packet_hash' not in actual['section_chars']
    assert len(actual['largest_sections']) == 12
    assert list(actual['section_chars']) == [str(i) for i in range(15)]


@pytest.mark.parametrize('packet', [
    {}, {'b': 'same', 'a': 'same', 'packet_hash': 'x', 'diagnostics': {'excluded': 1}},
    {2: '中', 1: '🙂'}, {'outer': {'x': [None, True, -0.0, float('nan')]}, 'hash': 'same'},
])
def test_all_diagnostics_fields_and_order_match(packet):
    assert packet_module.packet_diagnostics(packet) == legacy_diagnostics(packet)


def capture_error(function, value):
    try:
        function(value)
    except Exception as error:
        return type(error)
    pytest.fail('expected error')


def circular():
    value = []
    value.append(value)
    return value


@pytest.mark.parametrize('value', [circular(), {1: 1, 'a': 2}, {(1, 2): 'bad-key'}])
def test_streaming_and_old_encoder_raise_same_error_class(value):
    kind = capture_error(legacy_json, value)
    assert capture_error(lambda v: ''.join(contract.canonical_json_chunks(v)), value) is kind
    assert capture_error(contract.measure_canonical_json, value) is kind
    assert capture_error(contract.canonical_json_length, value) is kind
    assert capture_error(contract.stable_digest, value) is kind


def test_surrogate_digest_error_and_character_measurement_compatibility():
    value = {'text': '\ud800'}
    assert contract.canonical_json(value) == legacy_json(value)
    assert capture_error(contract.stable_digest, value) is UnicodeEncodeError
    assert capture_error(legacy_digest, value) is UnicodeEncodeError
    assert contract.measure_canonical_json(value).total_chars == len(legacy_json(value))


def peak(action):
    # Payload construction is outside this window. No gc/delete/shrinking.
    tracemalloc.start()
    try:
        started = perf_counter()
        result = action()
        elapsed = perf_counter() - started
        _, maximum = tracemalloc.get_traced_memory()
        return result, maximum, elapsed
    finally:
        tracemalloc.stop()


def test_medium_records_peak_reduces_without_deleting_fixture(record_property):
    records = [{'i': i, 'facts': '事实🙂ASCII\n' * 80, 'source': f'frozen:{i}',
                'numbers': [i, i + .5, None]} for i in range(1500)]
    packet = {'records': records, 'scope': 'FULL_FIXTURE', 'packet_hash': 'h' * 64}
    old_hash, old_hash_peak, old_hash_elapsed = peak(lambda: legacy_digest(packet))
    new_hash, new_hash_peak, new_hash_elapsed = peak(lambda: contract.stable_digest(packet))
    old_diagnostic, old_diagnostic_peak, old_diagnostic_elapsed = peak(lambda: legacy_diagnostics(packet))
    new_diagnostic, new_diagnostic_peak, new_diagnostic_elapsed = peak(lambda: packet_module.packet_diagnostics(packet))
    assert old_hash == new_hash
    assert old_diagnostic == new_diagnostic
    assert len(records) == 1500 and len(packet['records']) == 1500
    assert new_hash_peak < old_hash_peak * .5
    assert new_diagnostic_peak < old_diagnostic_peak * .5
    for key, value in {'record_count': len(records), 'old_hash_peak_bytes': old_hash_peak,
                       'new_hash_peak_bytes': new_hash_peak, 'old_diagnostics_peak_bytes': old_diagnostic_peak,
                       'new_diagnostics_peak_bytes': new_diagnostic_peak,
                       'input_packet_chars': old_diagnostic['packet_chars'],
                       'input_canonical_sha256': old_hash, 'same_hash': old_hash == new_hash,
                       'old_hash_traced_elapsed_seconds': old_hash_elapsed,
                       'new_hash_traced_elapsed_seconds': new_hash_elapsed,
                       'old_diagnostics_traced_elapsed_seconds': old_diagnostic_elapsed,
                       'new_diagnostics_traced_elapsed_seconds': new_diagnostic_elapsed}.items():
        record_property(key, value)


def test_entire_packet_and_budget_error_match_old_diagnostics(monkeypatch):
    from test_a1_packet import _snapshot, _context, AS_OF
    snapshot, context = _snapshot(), _context()
    new_packet = packet_module.build_a1_research_packet(snapshot, as_of=AS_OF,
                                                        monthly_strategy_context=context)
    with pytest.raises(packet_module.A1PacketSizeError) as new_error:
        packet_module.build_a1_research_packet(snapshot, as_of=AS_OF,
            monthly_strategy_context=context, max_estimated_tokens=1, raise_on_budget=True)
    monkeypatch.setattr(packet_module, 'stable_digest', legacy_digest)
    monkeypatch.setattr(packet_module, 'packet_diagnostics', legacy_diagnostics)
    old_packet = packet_module.build_a1_research_packet(snapshot, as_of=AS_OF,
                                                        monthly_strategy_context=context)
    assert new_packet == old_packet  # includes budget fixed point/hash/coverage
    with pytest.raises(packet_module.A1PacketSizeError) as old_error:
        packet_module.build_a1_research_packet(snapshot, as_of=AS_OF,
            monthly_strategy_context=context, max_estimated_tokens=1, raise_on_budget=True)
    assert str(new_error.value) == str(old_error.value)
    assert new_error.value.__dict__ == old_error.value.__dict__
