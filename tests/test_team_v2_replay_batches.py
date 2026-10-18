from datetime import datetime

import pytest

from liangjian_funnel.evaluation.team_v2_replay_batches import chronological_batches
from liangjian_funnel.evaluation.team_v2_replay_source import SIX_DAYS,sha


def source():
    plan=dict(plan_id='p',symbol='000001.SZ',valid_from='2026-10-08T09:30:00+08:00',expires_at='2026-10-08T15:00:00+08:00')
    manifests=[dict(kind='day',trade_date=day,observed_plan_ids=['p'] if day==SIX_DAYS[0] else []) for day in SIX_DAYS]
    def frame(minute,action='START_CONFIRMATION',status='SOURCE_BOUND_RULES_PENDING'):
        return dict(kind='plan_minute',trade_date=SIX_DAYS[0],decision_at=SIX_DAYS[0]+'T'+minute+':00+08:00',
            lane_id='lane_1',plan_id='p',original_action=action,status=status,reason_codes=['ORIGINAL_MISSING'] if status=='DATA_LIMITED' else [],
            original_plan_row_sha256=sha(plan))
    return manifests,plan,frame


def test_all_minutes_and_empty_days_preserved_without_lunch_or_synthetic_models():
    records,plan,frame=source();records.append(frame('09:36'))
    batches=list(chronological_batches(records,{'p':plan}))
    # Existing source schedule ends 11:30/15:00; its closed bars end one
    # minute earlier. 15:01 close-bar review is a separate coordinator path.
    assert len(batches)==1440 and sum(r['model_tick'] for r in batches)==276
    assert all(not datetime.fromisoformat(r['decision_at']).hour==12 for r in batches)
    first=next(r for r in batches if r['decision_at'].endswith('09:36:00+08:00') and r['trade_date']==SIX_DAYS[0])
    assert len(first['entry_source_frames'])==1 and first['model_tick']
    assert sum(r['status']=='EMPTY_SCOPE' for r in batches)==1200
    assert all(r['quote_basis']=='REPLAY_BAR_DERIVED' and not r['leader_open'] for r in batches)


def test_terminal_and_missing_data_not_reactivated_but_retained_for_exit_protection():
    records,plan,frame=source();records.extend([frame('09:36','PLAN_INVALIDATED'),frame('09:37'),frame('09:38',status='DATA_LIMITED')])
    rows=[r for r in chronological_batches(records,{'p':plan}) if r['source_frames']]
    assert not any(r['entry_source_frames'] for r in rows)
    assert [len(r['exit_only_source_frames']) for r in rows]==[1,1,0]
    assert len(rows[-1]['data_limited_source_frames'])==1


def test_duplicate_or_source_row_mutation_cannot_be_counted_as_another_minute():
    records,plan,frame=source();records.extend([frame('09:36'),frame('09:36')])
    with pytest.raises(ValueError,match='DUPLICATE_PLAN_MINUTE'):list(chronological_batches(records,{'p':plan}))
    records=records[:-1];plan['expires_at']='2026-10-08T16:00:00+08:00'
    with pytest.raises(ValueError,match='PLAN_HASH_INVALID'):list(chronological_batches(records,{'p':plan}))


def test_missing_day_manifest_not_treated_as_a_complete_empty_trading_day():
    records,plan,_=source()
    with pytest.raises(ValueError,match='DAY_MANIFEST_MISSING'):list(chronological_batches(records[:-1],{'p':plan}))
