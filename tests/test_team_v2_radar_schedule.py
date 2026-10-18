from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timedelta
from pathlib import Path
import sqlite3

from liangjian_funnel.runtime.team_v2_radar_schedule import RadarTriggerStore
from liangjian_funnel.runtime.team_v2_regime import load_rules
from test_team_v2_regime import sector,emotion


class Calendar:
    def is_trading_day(self,day):return day.weekday()<5


def at(value,day='2026-10-19'):return datetime.fromisoformat(day+'T'+value+':00+08:00')


def observe(store,value,on=True,missing=False,day='2026-10-19'):
    tick=at(value,day);em=emotion();em.update(as_of=tick.isoformat(),limit_up_count=40 if on else 19)
    facts=dict(market=dict(status='READY',fallback=False,as_of=tick.isoformat(),breadth=.8),
        emotion={} if missing else em,sector_cycle=sector(),sector_cycle_known_at=at('09:00',day).isoformat())
    store.store.record_live_regime(facts=facts,tick=tick,observed_at=tick+timedelta(seconds=10),
        prior='ROTATION_NO_MAINLINE',rules=load_rules(Path(__file__).resolve().parents[1]/'config/regime_live.yaml'))


def claim(store,clock,day='2026-10-19'):
    return store.claim_due(account_id='team',observed_at=at(clock,day)+timedelta(seconds=20),calendar=Calendar())


def test_fixed_trigger_missing_emotion_stays_blocked_and_restart_cannot_recall(tmp_path):
    path=tmp_path/'team.db';store=RadarTriggerStore(path);observe(store,'10:00',missing=True)
    first=claim(store,'10:01')
    assert first['status']=='CLAIMED' and first['request']['market']['emotion_state']=='DATA_BLOCK'
    assert first['request']['trigger_reasons']==['FIXED_1000']
    assert claim(RadarTriggerStore(path),'10:02')['status']=='ALREADY_CLAIMED_NO_RECALL'
    assert claim(store,'10:06')['status']=='DATA_BLOCK'
    with sqlite3.connect(path) as db:
        assert db.execute('SELECT count(*) FROM intraday_radar_requests').fetchone()==(1,)
        assert not db.execute("SELECT name FROM sqlite_master WHERE name IN ('execution_plans','model_calls','orders')").fetchall()


def test_only_persisted_two_bar_off_to_on_can_claim_extra_once(tmp_path):
    store=RadarTriggerStore(tmp_path/'team.db')
    for clock,on in [('09:40',False),('09:45',False),('09:50',True)]:observe(store,clock,on)
    assert claim(store,'09:51')['reason_code']=='RADAR_NO_AUTHORIZED_TRIGGER'
    observe(store,'09:55',True)
    assert claim(store,'09:56')['request']['trigger_reasons']==['CONFIRMED_OFF_TO_ON']
    observe(store,'10:00',True);assert claim(store,'10:01')['status']=='CLAIMED'
    for clock,on in [('10:05',False),('10:10',False),('10:15',True),('10:20',True)]:observe(store,clock,on)
    assert claim(store,'10:21')['reason_code']=='RADAR_SWITCH_TRIGGER_ALREADY_CLAIMED'
    observe(store,'13:30',True);assert claim(store,'13:31')['status']=='CLAIMED'
    for clock,on in [('13:35',False),('13:40',False),('13:45',True),('13:50',True)]:observe(store,clock,on)
    assert claim(store,'13:51')['reason_code']=='RADAR_DAILY_THREE_ALREADY_CLAIMED'


def test_initial_on_and_missing_or_old_state_never_create_switch_trigger(tmp_path):
    store=RadarTriggerStore(tmp_path/'team.db')
    observe(store,'09:40');observe(store,'09:45')
    assert claim(store,'09:46')['reason_code']=='RADAR_NO_AUTHORIZED_TRIGGER'
    assert claim(store,'10:01')['reason_code']=='RADAR_CURRENT_G5_OBSERVATION_MISSING'


def test_claims_are_atomic_and_day_cap_resets_without_reusing_yesterday_permission(tmp_path):
    path=tmp_path/'team.db';store=RadarTriggerStore(path);observe(store,'10:00')
    with ThreadPoolExecutor(max_workers=6) as pool:
        results=list(pool.map(lambda _:claim(store,'10:01'),range(6)))
    assert sum(r['status']=='CLAIMED' for r in results)==1
    assert claim(store,'10:01','2026-10-20')['reason_code']=='RADAR_CURRENT_G5_OBSERVATION_MISSING'
    observe(store,'10:00',day='2026-10-20')
    assert claim(store,'10:01','2026-10-20')['status']=='CLAIMED'
    assert claim(store,'10:01','2026-10-17')['reason_code']=='NON_TRADING_DAY'
