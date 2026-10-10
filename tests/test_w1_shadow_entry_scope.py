"""Original whole-round baseline versus independently derived entry scope."""
from copy import deepcopy
from datetime import timedelta
import json
import sqlite3
import uuid

import pytest

from test_shadow_session import AT, session, write_completion


def change_event(state, *, pid='p0', action='PLAN_INVALIDATED', reason='TERMINAL', effective=1,
                 terminal=True, inner=False):
    with sqlite3.connect(state) as db:
        old = db.execute('SELECT payload_json FROM monitor_events WHERE payload_json LIKE ?',
                         ('%"plan_id": "'+pid+'"%',)).fetchone()[0]
        payload = json.loads(old)
        if not inner:
            payload.pop('strategy', None)
        key = (f'effective:lane_1:{pid}:{action}' if effective else
               f'internal:lane_1:{pid}:{AT.isoformat()}:{action}:{reason}')
        db.execute('UPDATE monitor_events SET event_id=?,event_key=?,action=?,reason_code=?,effective=?,payload_json=? WHERE payload_json=?',
                   (str(uuid.uuid5(uuid.NAMESPACE_URL, 'liangjian-monitor:'+key)), key, action,
                    reason, effective, json.dumps(payload), old))
        if terminal:
            db.execute('UPDATE execution_plans SET status="INVALIDATED",updated_at=? WHERE plan_id=?',
                       (AT.isoformat(), pid))


def run(service, state):
    write_completion(service.source.monitor_latest, state)
    return service.poll(observed_at=AT+timedelta(seconds=1))


def test_same_minute_terminal_without_inner_or_bars_retains_whole_baseline(tmp_path):
    service, engine, ledger, state, minute = session(tmp_path, count=2)
    change_event(state)
    with sqlite3.connect(minute) as db:
        db.execute('DELETE FROM minute_decision_snapshots WHERE symbol="600000.SH"')
    result = run(service, state)
    assert result['scope_status'] == 'COMPLETE'
    assert result['full_plan_count'] == 2 and result['entry_plan_count'] == 1
    assert [i['plan_id'] for i in engine.calls[0][0]] == ['p1']
    assert result['exclusion_counts'] == {'TERMINAL': 1, 'INACTIVE_STATUS': 1}
    records = {r['plan_id']:r for r in ledger.minutes[0]['baseline_records']}
    assert records['p0']['action'] == 'PLAN_INVALIDATED'
    assert records['p0']['first_cause'] == 'TERMINAL'
    assert records['p0']['inner_baseline_sha256'] is None
    assert len(records['p0']['event_sha256']) == 64


@pytest.mark.parametrize('symbol,entry_count', [('600000.SH', 1), ('000001.SZ', 2)])
def test_held_symbol_only_excludes_corresponding_entry(tmp_path, symbol, entry_count):
    service, engine, ledger, state, _ = session(tmp_path, count=2)
    with sqlite3.connect(state) as db:
        db.execute('INSERT INTO virtual_positions VALUES("paper:lane_1",?,100)', (symbol,))
    result = run(service, state)
    assert result['scope_status'] == 'COMPLETE'
    assert result['full_plan_count'] == 2 and result['entry_plan_count'] == entry_count
    assert len(engine.calls[0][0]) == entry_count
    assert len(ledger.minutes[0]['baseline_records']) == 2
    assert result['exclusion_counts'].get('HELD', 0) == 2-entry_count


def test_first_buy_original_baseline_preserved_when_fill_created_position(tmp_path):
    service, engine, ledger, state, _ = session(tmp_path)
    change_event(state, action='BUY_SIGNAL', reason='ORIGINAL_FIRST_BUY', terminal=False, inner=True)
    with sqlite3.connect(state) as db:
        db.execute('INSERT INTO virtual_positions VALUES("paper:lane_1","600000.SH",100)')
    result = run(service, state)
    assert result['scope_status'] == 'COMPLETE' and result['entry_plan_count'] == 0
    assert not engine.calls
    assert ledger.minutes[0]['baseline_records'][0]['action'] == 'BUY_SIGNAL'
    assert ledger.minutes[0]['baseline_records'][0]['first_cause'] == 'ORIGINAL_FIRST_BUY'


@pytest.mark.parametrize('case', ['identity', 'event_id', 'completion_hash'])
def test_terminal_cannot_hide_full_round_identity_or_fence_conflict(tmp_path, case):
    service, engine, ledger, state, _ = session(tmp_path, count=2)
    change_event(state)
    with sqlite3.connect(state) as db:
        if case == 'identity':
            db.execute('UPDATE execution_plans SET symbol="000001.SZ" WHERE plan_id="p0"')
        elif case == 'event_id':
            db.execute('UPDATE monitor_events SET event_id="wrong" WHERE action="PLAN_INVALIDATED"')
    write_completion(service.source.monitor_latest, state)
    if case == 'completion_hash':
        payload = json.loads(service.source.monitor_latest.read_bytes())
        payload['observability']['decision_hash'] = '0'*64
        service.source.monitor_latest.write_text(json.dumps(payload), encoding='utf-8')
    result = service.poll(observed_at=AT+timedelta(seconds=1))
    assert result['status'] == 'DATA_LIMITED' and not engine.calls and not ledger.minutes


def test_entry_subset_strict_equality_and_updated_at_cannot_restore_terminal(tmp_path):
    service, engine, _, state, _ = session(tmp_path, count=2)
    change_event(state)
    write_completion(service.source.monitor_latest, state)
    items, _ = service.source.read_round(AT, observed_at=AT+timedelta(seconds=1))
    from liangjian_funnel.runtime.shadow_session import _entry_cohort, _verify_entry_subset
    entries, counts = _entry_cohort(items)
    assert [i['plan_id'] for i in entries] == ['p1'] and counts['TERMINAL'] == 1
    assert _verify_entry_subset(items, entries) is None
    for altered in ([], items, entries+entries, [deepcopy(entries[0]) | {'plan_id':'unknown'}]):
        with pytest.raises(ValueError, match='ENTRY_COHORT_STRICT_EQUALITY_REQUIRED'):
            _verify_entry_subset(items, altered)
    assert items[0]['entry_admission']['status'] == 'EXCLUDED'
    assert not engine.calls


def test_unwired_pit_cannot_emit_or_commit_ok_first_buy(tmp_path):
    service, engine, ledger, state, _ = session(tmp_path)
    stored, committed = [], []
    original = engine.evaluate_minute
    def buying(items, **kwargs):
        result = original(items, **kwargs)
        result['signals'] = [{'plan_id':'p0', 'variant_id':'V1', 'status':'OK',
                              'variant_action':'BUY_SIGNAL', 'event_kind':'FIRST_TRIGGER'}]
        return result
    engine.evaluate_minute = buying
    engine.commit_tentative = lambda result, **kw:(committed.append(kw) or {'ok':True})
    ledger.record_signal = lambda signal, plan, **kw:(stored.append(signal) or {'ok':True, 'stored':True})
    result = run(service, state)
    assert result['status'] == 'DATA_LIMITED'
    assert stored[0]['status'] == 'DATA_LIMITED' and stored[0]['variant_action'] is None
    assert stored[0]['evaluation_variant_action'] == 'BUY_SIGNAL'
    assert not committed and result['pit_blocked_signal_count'] == 1


def test_holding_other_paper_account_does_not_exclude_lane_entry(tmp_path):
    service, engine, _, state, _ = session(tmp_path)
    with sqlite3.connect(state) as db:
        db.execute('INSERT INTO virtual_positions VALUES("paper:other_lane","600000.SH",100)')
    result = run(service, state)
    assert result['entry_plan_count'] == 1 and len(engine.calls) == 1
    assert result['exclusion_counts'] == {}


def test_terminal_even_current_active_flag_cannot_be_restored_by_updated_at(tmp_path):
    service, engine, ledger, state, _ = session(tmp_path)
    change_event(state)
    with sqlite3.connect(state) as db:
        db.execute('UPDATE execution_plans SET status="ACTIVE_TODAY",updated_at=?', (AT.isoformat(),))
    result = run(service, state)
    assert result['scope_status'] == 'COMPLETE' and result['entry_plan_count'] == 0
    assert result['exclusion_counts'] == {'TERMINAL':1} and not engine.calls
    assert ledger.minutes[0]['baseline_records'][0]['action'] == 'PLAN_INVALIDATED'


def test_terminal_revision_after_original_event_still_blocks_full_round(tmp_path):
    service, engine, ledger, state, _ = session(tmp_path)
    change_event(state)
    with sqlite3.connect(state) as db:
        db.execute('UPDATE execution_plans SET updated_at=?', ((AT+timedelta(seconds=1)).isoformat(),))
    result = run(service, state)
    assert result['gap_codes'] == ['SOURCE_TIME_OR_PLAN_REVISION_UNPROVEN']
    assert result['scope_status'] == 'INCOMPLETE' and not engine.calls and not ledger.minutes


def test_engine_cannot_emit_excluded_plan_and_passive_receipt_is_retained(tmp_path):
    service, engine, ledger, state, _ = session(tmp_path, count=2)
    change_event(state)
    original = engine.evaluate_minute
    def extra(*args, **kwargs):
        result = original(*args, **kwargs)
        result['signals'] = [{'plan_id':'p0','variant_id':'V1','status':'OK','variant_action':'BUY_SIGNAL'}]
        return result
    engine.evaluate_minute = extra
    result = run(service, state)
    assert result['gap_codes'] == ['ENTRY_COHORT_STRICT_EQUALITY_REQUIRED']
    assert result['passive_baseline_coverage_count'] == len(result['baseline_records']) == 2
    assert not ledger.minutes


def test_real_ledger_records_passive_first_buy_with_zero_entry_without_outcome(tmp_path):
    from liangjian_funnel.runtime.shadow_evidence import ShadowEvidenceLedger, read_shadow_evidence
    service, engine, _, state, _ = session(tmp_path)
    ledger = ShadowEvidenceLedger(tmp_path/'shadow.db', tmp_path/'shadow.jsonl')
    service.ledger = ledger
    change_event(state, action='BUY_SIGNAL', reason='ORIGINAL_FIRST_BUY', terminal=False, inner=True)
    with sqlite3.connect(state) as db:
        db.execute('INSERT INTO virtual_positions VALUES("paper:lane_1","600000.SH",100)')
    result = run(service, state)
    stored = read_shadow_evidence(ledger.db_path, ledger.jsonl_path)
    assert result['state_commit']['status'] == 'NO_ENTRY_COHORT' and not engine.calls
    assert stored['ok'] and stored['hash_chain_status'] == 'MATCHED'
    assert stored['signals'] == []
    assert stored['minutes'][0]['full_plan_count'] == 1
    assert stored['minutes'][0]['baseline_records'][0]['action'] == 'BUY_SIGNAL'


def test_conflicting_explicit_plan_identity_is_full_round_failure_not_passive_exception(tmp_path):
    service, engine, ledger, state, _ = session(tmp_path)
    change_event(state)
    with sqlite3.connect(state) as db:
        raw = db.execute('SELECT payload_json FROM execution_plans').fetchone()[0]
        plan = json.loads(raw); plan['plan_id']='different-original-id'
        db.execute('UPDATE execution_plans SET payload_json=?', (json.dumps(plan),))
    result = run(service, state)
    assert result['gap_codes'] == ['EXECUTION_PLAN_IDENTITY_MISMATCH']
    assert not engine.calls and not ledger.minutes


def test_optional_payload_id_not_invented_as_evidence_row_event_chain_remains_explicit(tmp_path):
    service, engine, _, state, _ = session(tmp_path)
    with sqlite3.connect(state) as db:
        raw=db.execute('SELECT payload_json FROM execution_plans').fetchone()[0]
        plan=json.loads(raw); plan.pop('plan_id')
        db.execute('UPDATE execution_plans SET payload_json=?',(json.dumps(plan),))
    result=run(service,state)
    assert result['entry_plan_count']==1 and len(engine.calls)==1
    binding=result['source_bindings']['p0']
    assert binding['payload_plan_id_status']=='MISSING'
    assert binding['plan_identity_basis']=='ORIGINAL_ROW_PRIMARY_KEY_AND_EVENT_UUID_KEY'
