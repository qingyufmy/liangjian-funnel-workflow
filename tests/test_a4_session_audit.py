from datetime import datetime

from scripts.replay_a4_session import market_at, session_minutes


def test_session_has_exactly_240_closed_minutes_and_no_lunch():
    rows=session_minutes('2026-09-07')
    assert len(rows)==len(set(rows))==240
    assert rows[119].strftime('%H:%M')=='11:30'
    assert rows[120].strftime('%H:%M')=='13:01'
    assert rows[-1].strftime('%H:%M')=='15:00'


def test_replay_does_not_backfill_future_market_snapshot():
    now=datetime.fromisoformat('2026-09-07T13:11:00+08:00')
    snap={'1310.json':{'as_of':'2026-09-07T13:13:00+08:00','status':'READY'}}
    assert market_at(snap,now)==({},True)
    state,assumed=market_at(snap,now,True)
    assert assumed and state['source'].startswith('TEST_ONLY')
    assert state['as_of']==now.isoformat()


def test_replay_retains_actual_market_block_not_allow_override():
    now=datetime.fromisoformat('2026-09-07T13:14:00+08:00')
    state={'as_of':'2026-09-07T13:13:00+08:00','entry_permission':'BLOCK_NEW_ENTRY'}
    assert market_at({'1310.json':state},now,True)==(state,False)
