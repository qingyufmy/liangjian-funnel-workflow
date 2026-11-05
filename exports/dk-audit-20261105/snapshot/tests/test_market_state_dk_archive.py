from copy import deepcopy
from dataclasses import replace
from datetime import datetime,time,timedelta
from decimal import Decimal
from pathlib import Path
import sys

import pytest

from liangjian_funnel.market_state.bars15m import Bar15m,ENDS,TZ
from liangjian_funnel.market_state.calendar import market_calendar
from liangjian_funnel.market_state.dk_archive import archive_evaluation,bar_identity,merge_bars
from liangjian_funnel.market_state.ssl import CANONICAL_SOURCE

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from track2_archive_dk import decode_bar

CAL=market_calendar()


def day_bars(day):
    day=datetime.fromisoformat(day).date();result=[]
    for slot in ENDS:
        end=datetime.combine(day,time.fromisoformat(slot),tzinfo=TZ)
        parts=tuple(end-timedelta(minutes=n) for n in (10,5,0))
        result.append(Bar15m('000001.SH',end,'CLOSED',parts,parts,open=Decimal(100),close=Decimal(100),
            high=Decimal(101),low=Decimal(99),source_id=CANONICAL_SOURCE,adjust_mode='none'))
    return result


def capture(day):return datetime.fromisoformat(day+'T16:00:00+08:00')


def merge(old,new,day='2026-10-30',sha='a'*64):
    return merge_bars(old,new,captured_at=capture(day),source_sha256=sha,observed_at=capture(day),calendar=CAL)


def test_two_actual_capture_days_are_not_three_retrospective_bar_days_and_first_seen_is_preserved():
    rows=day_bars('2026-10-28')+day_bars('2026-10-29')
    first=merge([],rows,day='2026-10-29');before=deepcopy(first)
    second=merge(first,rows[-16:]+day_bars('2026-10-30'),sha='b'*64)
    assert first==before and len(second)==48
    assert second[:32]==first
    assert second[-1]['first_seen_at']==capture('2026-10-30').isoformat()
    result=archive_evaluation(second,observed_at=capture('2026-10-30'),calendar=CAL)
    assert result['actual_capture_dates']==['2026-10-29','2026-10-30']
    assert result['five_day_shadow_acceptance']=='NOT_AUTOMATICALLY_ACCEPTED'
    assert all(result['incremental_equal'].values())


@pytest.mark.parametrize('case',['revision','gap','symbol','duplicate','future_first_seen'])
def test_revision_gap_wrong_identity_and_invalid_first_seen_are_rejected(case):
    rows=day_bars('2026-10-29');first=merge([],rows,day='2026-10-29')
    incoming=rows[-2:]+day_bars('2026-10-30')
    if case=='revision':incoming[0]=replace(incoming[0],close=Decimal('100.1'))
    elif case=='gap':incoming.pop(6)
    elif case=='symbol':incoming[-1]=replace(incoming[-1],symbol='000300.SH')
    elif case=='duplicate':incoming.append(incoming[-1])
    else:first[0]['first_seen_at']='2026-11-02T16:00:00+08:00'
    with pytest.raises(ValueError):merge(first,incoming)


def test_json_round_trip_preserves_bar_semantics_for_next_capture():
    for bar in day_bars('2026-10-30'):
        assert decode_bar(bar_identity(bar))==bar


def test_later_evaluation_cannot_make_a_bar_closed_at_an_earlier_capture():
    with pytest.raises(ValueError,match='DK_ARCHIVE_BAR_AFTER_CAPTURE'):
        merge_bars([],day_bars('2026-10-30'),captured_at=datetime.fromisoformat('2026-10-30T15:00:30+08:00'),
            source_sha256='a'*64,observed_at=capture('2026-10-30'),calendar=CAL)


def test_missing_capture_day_is_retained_as_gap_not_filled_from_bar_dates():
    first=merge([],day_bars('2026-10-28'),day='2026-10-28')
    second=merge(first,day_bars('2026-10-29')+day_bars('2026-10-30'))
    result=archive_evaluation(second,observed_at=capture('2026-10-30'),calendar=CAL)
    assert result['capture_dates_consecutive'] is False
    assert result['actual_capture_days']==2
