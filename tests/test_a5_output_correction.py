import copy
import hashlib
import json
import time
from datetime import datetime

import pytest

from test_a5_daily_review import ROOT, TZ, _report, _seed
from liangjian_funnel.pipeline.model_client import ModelCallResult
from liangjian_funnel.pipeline.prompts import PromptRepository
from liangjian_funnel.review.context import A5ReviewError
from liangjian_funnel.review.daily import A5DailyReviewService, A5ReviewKind
from liangjian_funnel.runtime.state import RuntimeStore


class SequenceModel:
    def __init__(self, outputs):
        self.outputs = outputs
        self.calls = []

    def complete(self, model, messages, **kwargs):
        self.calls.append((messages, kwargs))
        return ModelCallResult(model=model, output=copy.deepcopy(self.outputs[len(self.calls) - 1]),
            prompt_hash=kwargs['prompt_hash'], input_hash=kwargs['input_hash'],
            latency_ms=10, attempts=1, thinking_variant='test')


def setup_service(tmp_path, outputs):
    store = RuntimeStore(tmp_path / 'state.sqlite3')
    _seed(store, tmp_path)
    model = SequenceModel(outputs)
    return A5DailyReviewService(store=store, prompts=PromptRepository(ROOT / 'prompts'),
        model_client=model, output_dir=tmp_path, lane_id='lane_1', model='deepseek-v4-pro'), model


def invalid_report():
    report = copy.deepcopy(_report())
    report['a4_review']['evidence_ids'] = ['A5V:SUMMARY']
    return report


def test_unknown_evidence_requires_new_model_output_and_preserves_original(tmp_path):
    original = invalid_report()
    service, model = setup_service(tmp_path, [original, _report()])
    result = service.run(review_kind=A5ReviewKind.MIDDAY, now=datetime(2026, 9, 3, 11, 35, tzinfo=TZ))
    assert result['created'] and len(model.calls) == 2
    first, second = [kwargs for _, kwargs in model.calls]
    assert first['input_hash'] == second['input_hash']
    assert first['prompt_hash'] != second['prompt_hash']
    assert 0 < second['timeout_seconds'] <= 180
    request = model.calls[0][0][0]['content']
    assert 'citation_catalog' in request
    target = tmp_path / 'a5/2026-09-03'
    raw = json.loads(next(target.glob('*-model-*.json')).read_text(encoding='utf-8'))
    assert raw['output'] == original and raw['validation_status'] == 'RAW_NOT_APPROVED'
    correction = json.loads(next(target.glob('*-repair-model-*.json')).read_text(encoding='utf-8'))
    assert correction['parent_output_hash'] == raw['output_hash']
    stored = service.store.list_a5_reviews()[0]
    assert stored['attempts'] == 2
    assert any('一次纠正' in note for note in json.loads(stored['report_json'])['fact_reconciliation'])


def test_repeated_invalid_output_stops_after_one_correction_without_review(tmp_path):
    service, model = setup_service(tmp_path, [invalid_report(), invalid_report()])
    with pytest.raises(A5ReviewError, match='A5_OUTPUT_EVIDENCE_INVALID'):
        service.run(review_kind=A5ReviewKind.MIDDAY, now=datetime(2026, 9, 3, 11, 35, tzinfo=TZ))
    assert len(model.calls) == 2 and not service.store.list_a5_reviews()
    assert list((tmp_path / 'a5/2026-09-03').glob('*-repair-validation-*.json'))


@pytest.mark.parametrize('mode', ['archive', 'deadline', 'identity', 'oversize'])
def test_correction_does_not_bypass_archive_deadline_identity_or_size(tmp_path, mode):
    service, model = setup_service(tmp_path, [_report()])
    output = invalid_report()
    if mode == 'identity':
        output['trade_date'] = '2026-09-04'
    if mode == 'archive':
        model.archived_response_source = 'immutable.json'
    result = ModelCallResult(model='deepseek-v4-pro', output=output, prompt_hash='original',
        input_hash='input', latency_ms=1, attempts=1, thinking_variant='test')
    expected = 'A5_OUTPUT_IDENTITY_MISMATCH' if mode == 'identity' else 'A5_OUTPUT_EVIDENCE_INVALID'
    with pytest.raises(A5ReviewError, match=expected):
        service._validate_or_repair(result, prompt='x' * (250_000 if mode == 'oversize' else 1),
            facts={}, allowed_projection_evidence=set(), review_kind=A5ReviewKind.MIDDAY,
            trade_date=datetime(2026, 9, 3).date(), target_dir=tmp_path, artifact_stem='test',
            model_deadline=time.monotonic() + (-1 if mode == 'deadline' else 600))
    assert model.calls == []


def test_schema_correction_is_validated_and_bound_to_original_input(tmp_path):
    bad = copy.deepcopy(_report())
    bad['overall_verdict'] = 'LOOKS_GOOD'
    service, model = setup_service(tmp_path, [bad, _report()])
    service.run(review_kind=A5ReviewKind.MIDDAY, now=datetime(2026, 9, 3, 11, 35, tzinfo=TZ))
    content = model.calls[1][0][0]['content']
    assert 'A5_OUTPUT_SCHEMA_INVALID' in content
    assert hashlib.sha256(content.encode()).hexdigest() == model.calls[1][1]['prompt_hash']


def test_correction_consumes_remaining_budget_without_reset(monkeypatch, tmp_path):
    service, model = setup_service(tmp_path, [_report()])
    monkeypatch.setattr('liangjian_funnel.review.daily.time.monotonic', lambda: 1000)
    bad = ModelCallResult(model='deepseek-v4-pro', output=invalid_report(), prompt_hash='original',
        input_hash='input', latency_ms=1, attempts=1, thinking_variant='test')
    signal_id = _report()['signal_reviews'][0]['evidence_ids'][0]
    service._validate_or_repair(bad, prompt='input', facts={'input_hash': 'input'},
        allowed_projection_evidence={signal_id}, review_kind=A5ReviewKind.MIDDAY,
        trade_date=datetime(2026, 9, 3).date(), target_dir=tmp_path, artifact_stem='budget', model_deadline=1010)
    assert model.calls[0][1]['timeout_seconds'] == 10
