"""Render every archived 10-27 customer card from its original inputs.

Artifacts are release evidence. Runtime DBs remain wrapper-owned tmp_path.
No model, data source, notification transport or browser is invoked.
"""
from copy import deepcopy
from datetime import datetime
import difflib
import hashlib
import json
from pathlib import Path
import socket

from liangjian_funnel.review.daily import (A5ReviewReport,
    _canonicalize_report_output, _enforce_verified_findings, _markdown,
    _model_fact_projection, _projection_evidence_ids)
from liangjian_funnel.review.fact_guard import reconcile_report
from liangjian_funnel.runtime.lark import LarkNotifier, LarkDeliveryResult
from liangjian_funnel.runtime.lark_notifications import WorkflowLarkPublisher
from liangjian_funnel.runtime.state import RuntimeStore

OPS = Path('D:/dev_A股/liangjian_team_v2_20261016/artifacts/a4-monitor-20261021')


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':')).encode()


def sha(value):
    return hashlib.sha256(value).hexdigest()


def text(card):
    value = card['card']
    return value['header']['title']['content'] + '\n' + '\n'.join(
        item.get('text', {}).get('content', '') for item in value['elements'])


class CapturingNotifier:
    enabled = True

    def __init__(self):
        self.cards = []

    def send(self, title, body, color):
        self.cards.append(LarkNotifier.build_card(title, body, color))
        return LarkDeliveryResult(True, 'LARK_SENT', 200, 1)


def test_every_real_customer_card_has_original_and_rerendered_snapshot(tmp_path, monkeypatch):
    monkeypatch.setattr(socket.socket, 'connect', lambda *a, **k: (_ for _ in ()).throw(
        AssertionError('D8_NETWORK_FORBIDDEN')))
    source = OPS / 'r0-0837-business-originals-20261027'
    originals = []
    for path in source.glob('*.json'):
        if path.stat().st_size > 15000:
            continue
        record = json.loads(path.read_bytes())
        if 'card' in record:
            originals.append((path, record))
    assert len(originals) == 13
    output = OPS / 'D8_REAL_OUTPUT_REPLAY_0837_20261027'
    output.mkdir(exist_ok=True)
    publisher = WorkflowLarkPublisher(RuntimeStore(tmp_path / 'd8.sqlite3'), None)
    notifier = CapturingNotifier()
    publisher.notifier = notifier
    exact_path = OPS / 'D8_EXACT_CARD_INPUTS_RO_0837_20261027.json'
    exact = json.loads(exact_path.read_bytes())
    plans = {r['plan_id']: r for r in exact['plans']}
    frozen_plans = deepcopy(plans)
    for pid, row in plans.items():
        assert sha(row['payload_json'].encode()) == exact['plan_payload_sha256'][pid]

    activation_path = OPS / 'ACTIVATION_0927_SUPPLEMENT_RO_0804_R2_20261027.json'
    activation = json.loads(activation_path.read_bytes())
    evidence = {}
    for row in activation['activation_evidence']:
        body = row['body']
        assert row['hash_valid']
        assert body['plan_payload_sha256'] == exact['plan_payload_sha256'][row['plan_id']]
        evidence[body['symbol']] = body['quote']
    assert len(evidence) == 14
    publisher.publish_premarket(list(plans.values()),
        reviewed_at=datetime.fromisoformat('2026-10-27T09:26:00+08:00'), evidence=evidence)
    assert len(notifier.cards) == 5
    new_by_title = {text(c).split('\n', 1)[0]: c for c in notifier.cards}
    for row in exact['events']:
        assert sha(row['payload_json'].encode()) == exact['event_payload_sha256'][row['event_id']]
        payload = json.loads(row['payload_json'])
        event = dict(row, plan_id=payload['plan_id'], symbol=payload['symbol'])
        count = len(notifier.cards)
        publisher.publish_a4_events([event], plans=plans,
            now=datetime.fromisoformat(row['minute_end']))
        assert len(notifier.cards) == count + 1
    assert len(notifier.cards) == 10
    a4_cards = notifier.cards[5:]
    for card in a4_cards:
        rendered = text(card)
        assert '未识别' not in rendered
        assert '计划区间' in rendered
        assert '不是交易信号' in rendered or '不是数据缺失或交易信号' in rendered

    a5_cards = {}
    a5_hashes = []
    for folder in ['a5-0825-originals-20261027', 'a5-close-0837-originals-20261027']:
        root = OPS / folder
        fact_path = next(root.glob('*-facts.json'))
        raw_path = next(root.glob('*-model-*.json'))
        facts = json.loads(fact_path.read_bytes())
        raw = json.loads(raw_path.read_bytes())
        allowed = _projection_evidence_ids(_model_fact_projection(facts)) | {
            'METRICS:DAILY', 'DATA_QUALITY:DAILY'}
        original_facts = canonical(facts)
        report = A5ReviewReport.model_validate(_canonicalize_report_output(
            deepcopy(raw['output']), allowed_evidence=allowed))
        _enforce_verified_findings(report, facts)
        report.fact_reconciliation.extend(reconcile_report(report, facts))
        markdown = _markdown(report, facts)
        assert '未识别' not in markdown and 'UTC01:' not in markdown and 'UTC00:' not in markdown
        assert canonical(facts) == original_facts
        kind = report.review_kind.value
        count = len(notifier.cards)
        publisher.publish_a5_review(dict(review_id='D8:' + kind,
            trade_date='2026-10-27', review_kind=kind,
            report=report.model_dump(mode='json'), fact_snapshot=facts,
            cutoff_at=facts.get('cutoff_at')),
            now=datetime.fromisoformat('2026-10-27T11:36:25+08:00' if kind == 'MIDDAY'
                                       else '2026-10-27T16:01:42+08:00'))
        assert len(notifier.cards) == count + 1
        a5_cards[kind] = notifier.cards[-1]
        (output / (kind + '.md')).write_text(markdown, encoding='utf8')
        a5_hashes.append(dict(kind=kind, facts_sha256=sha(fact_path.read_bytes()),
                            raw_sha256=sha(raw_path.read_bytes())))

    snapshots = []
    for old_path, record in sorted(originals, key=lambda item: item[1]['observed_at']):
        old = record['card']
        title = text(old).split('\n', 1)[0]
        old_text = text(old)
        if title.startswith('A股盘前计划｜'):
            new = new_by_title[title]
            scope = 'same immutable 14 authored plans and original activation quote'
        elif '盘中复盘' in title or '盘后复盘' in title:
            new = a5_cards['MIDDAY' if '盘中复盘' in title else 'POST_CLOSE']
            scope = 'same original A5 facts/raw reply; new deterministic reconciliation'
        elif record['plan_ids']:
            pid = record['plan_ids'][0]
            event = next(row for row in exact['events']
                         if json.loads(row['payload_json'])['plan_id'] == pid)
            index = exact['events'].index(event)
            new = a4_cards[index]
            scope = 'same immutable minute event and authored plan; no Team binding fabricated'
        else:
            # Actual 08:30 no-plans push predates CAS. The new publisher
            # suppresses it before cutoff, then sends the actual CAS count.
            assert record['observed_at'].startswith('2026-10-27T08:30:')
            before = len(notifier.cards)
            waiting = publisher.publish_a3_premarket_status(
                analyzed_at=datetime.fromisoformat(record['observed_at']),
                reason_code='DAILY_PLAN_NOT_READY')
            assert waiting['status'] == 'NOOP'
            assert waiting['reason_code'] == 'PREMARKET_AWAITING_NATURAL_CAS'
            assert len(notifier.cards) == before
            new = {'suppressed': True, 'reason': 'WAIT_FOR_ACTUAL_CAS_NOT_ZERO_PLANS'}
            scope = 'same 08:30 instant; no final no-plan message before CAS'
        stem = old_path.name[:16]
        new_text = text(new) if 'card' in new else json.dumps(new, ensure_ascii=False)
        assert '未识别' not in new_text and 'UTC01:' not in new_text
        (output / (stem + '-before.json')).write_bytes(canonical(record))
        (output / (stem + '-after.json')).write_bytes(canonical(new))
        (output / (stem + '.diff')).write_text('\n'.join(difflib.unified_diff(
            old_text.splitlines(), new_text.splitlines(), fromfile='original-sanitized',
            tofile='candidate-render', lineterm='')), encoding='utf8')
        snapshots.append(dict(source=str(old_path), source_sha256=sha(old_path.read_bytes()),
            old_sanitized_not_wire_bytes=record['sanitized_not_wire_bytes'],
            before_sha256=sha(canonical(record)), after_sha256=sha(canonical(new)), scope=scope))
    assert len(snapshots) == 13 and plans == frozen_plans
    publisher.publish_batch_switch_status(trade_date='2026-10-27', scope_id='bd_c3b92f336a0a5b86ac624f5d28bc',
        plan_count=14, body_sha256='eb8c66ee4b3937e18568ed577fe55dc5b814a1edc05b13c80372d2e207e5d85f',
        observed_at=datetime.fromisoformat('2026-10-27T08:38:18.982182+08:00'))
    (output / 'BATCH_SWITCH_0838-new.json').write_bytes(canonical(notifier.cards[-1]))
    (output / 'RESULT.json').write_bytes(canonical(dict(
        status='13_CUSTOMER_ORIGINALS_RERENDERED_NOT_COMPLETE_RELEASE_ACCEPTANCE',
        snapshots=snapshots, a5_inputs=a5_hashes, source_http=0, model_http=0,
        notification_http=0, production_writes=0,
        limitation='A7 per-number provenance and live UI/Team-A5 output are separate gates; no new real A5 call.')))
