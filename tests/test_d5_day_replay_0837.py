"""10-27 frozen ledger and every observed Team reply; no invented decisions."""
from pathlib import Path
from collections import Counter
from datetime import datetime,timedelta
import json,hashlib
import pytest
from liangjian_funnel.pipeline.team_v2_trader_schema import validate_trader_output,project_trader_references
from liangjian_funnel.runtime.team_v2_review_latch import ReviewDayLatch
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
F=Path(__file__).parent/'fixtures/postclose_20261027'
ROWS=json.loads((F/'all_a4_originals.json').read_bytes())['result']['rows']

@pytest.mark.parametrize('row',ROWS,ids=lambda r:r['call_id'])
def test_every_observed_original_keeps_no_action_with_authoritative_same_tick_flags(row):
    raw=json.loads(row['output_json']);catalog=json.loads(row['catalog']['catalog_json'])
    assert hashlib.sha256(row['output_json'].encode()).hexdigest()==row['output_hash']
    assert hashlib.sha256(row['catalog']['catalog_json'].encode()).hexdigest()==row['catalog']['catalog_sha256']
    assert raw['envelope']['execution_trade_date']==row['execution_trade_date']=='2026-10-27'
    assert raw['envelope']['run_id']==row['run_id']
    plans={r['plan_id']:r['symbol'] for r in raw['signals']}
    refs={r['plan_id']:{**{k:'MET' for k in json.loads(r['met'] or '[]')},**{k:'UNMET' for k in json.loads(r['unmet'] or '[]')}}
        for r in row['same_tick_original_flags'] if r['plan_id'] in plans}
    assert set(refs)==set(plans)
    checked=validate_trader_output(raw,catalog,required_envelope=raw['envelope'],decision_id=raw['decision_id'],
        minute_snapshot_id=raw['minute_snapshot_id'],plans=plans,held_symbols=set(),tech_reference=refs,
        candidate_actions={pid:'BUY_SIGNAL' for pid in plans},position_candidate_actions={})
    assert checked.valid,checked.issues
    projected,_=project_trader_references(raw,refs)
    assert [r['action'] for r in projected['signals']]==[r['action'] for r in raw['signals']]
    assert all(r['action']=='NO_ACTION' for r in raw['signals'])


def test_real_day_is_complete_and_no_degradation_admission_is_invented(tmp_path):
    day=json.loads((F/'full_day_runtime_rows.json').read_bytes())
    start=datetime.fromisoformat('2026-10-27T09:32:00+08:00')
    morning=[(start+timedelta(minutes=i)).isoformat() for i in range(119)]
    start=datetime.fromisoformat('2026-10-27T13:01:00+08:00')
    afternoon=[(start+timedelta(minutes=i)).isoformat() for i in range(120)]
    expected={(p['plan_id'],m) for p in day['plans'] for m in morning+afternoon}
    events=[r for r in day['minute_events'] if r['plan_id']]
    counts=Counter((r['plan_id'],r['minute_end']) for r in events)
    assert len(day['plans'])==14 and len(events)==3346 and set(counts)==expected and max(counts.values())==1
    assert Counter(r['reason_code'] for r in events if r['technical_action']=='BUY_SIGNAL')=={'MONITOR_OVERRUN':74,'LLM_UNAVAILABLE':4}
    assert not day['orders'] and not day['lots'] and not day['engine_period_admissions']
    assert all(not r['admissions'] for r in day['sessions_after1406'])
    assert len(ROWS)==11 and sum(r['validated'] for r in ROWS)==6
    assert sum(len(json.loads(r['output_json'])['signals']) for r in ROWS)==13
    # The new classifier on the observed votes does not reproduce the old echo
    # error latch. This is not a claim about unobserved post-14:06 model outputs.
    latch=ReviewDayLatch(TeamMeetingStore(tmp_path/'team.db'))
    for row in sorted(ROWS,key=lambda r:r['captured_at']):
        state=latch.record(account_id='local-paper',trade_date='2026-10-27',decision_id=row['call_id'],
            valid=True,counts_as_failure=False,observed_at=datetime.fromisoformat(row['captured_at']))
        assert not state['engine_only']
    assert latch.read('local-paper','2026-10-28')=={'consecutive_failures':0,'engine_only':False}


def test_actual_premarket_cas_activation_and_daily_reader_are_separate_stages():
    morning=json.loads((F/'PREMARKET_0840_RESULT_0804_20261027.json').read_bytes())
    activate=json.loads((F/'PREMARKET_0927_RESULT_0804_20261027.json').read_bytes())
    monitor=json.loads((F/'PREMARKET_0935_RESULT_0804_20261027.json').read_bytes())
    cas=morning['cas'][0]
    assert cas['old_plan_ids_count']==0 and cas['new_plan_ids_count']==14
    assert cas['switched_at'].startswith('2026-10-27T08:38:18.')
    assert activate['active']==14 and monitor['plans_active']==14 and monitor['source_rejection']==0
    assert monitor['B_daily_found']==14
    assert monitor['second_entrypoint']['result']['activated']==[]
    assert 'no second fresh activation/source decision' in monitor['second_entrypoint']['limitation']
