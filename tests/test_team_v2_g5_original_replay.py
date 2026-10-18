from datetime import datetime
import importlib.util
from pathlib import Path

from liangjian_funnel.runtime.team_v2_regime import frozen_candidates,load_rules

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('g5_replay',ROOT/'scripts/replay_team_g5_five_day.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)


def test_missing_original_emotion_never_becomes_a_proved_off_switch():
    tick=datetime.fromisoformat('2026-10-14T10:00:00+08:00')
    observed=datetime.fromisoformat('2026-10-14T10:01:00+08:00')
    raw=dict(status='READY',fallback=False,as_of=tick.isoformat(),breadth=.2)
    facts,reasons=module.original_facts((raw,'frozen-original'),tick=tick,observed_at=observed)
    primary,emotion=frozen_candidates(facts,tick=tick,observed_at=observed,rules=load_rules(ROOT/'config/regime_live.yaml'))
    assert primary=='RISK_OFF_RETREAT' and emotion=='DATA_BLOCK'
    assert 'ORIGINAL_EMOTION_FULL_UNIVERSE_COUNTS_NOT_ARCHIVED' in reasons


def test_original_cache_observed_after_its_decision_is_not_used_retroactively():
    tick=datetime.fromisoformat('2026-10-14T10:00:00+08:00')
    observed=datetime.fromisoformat('2026-10-14T10:01:00+08:00')
    raw=dict(status='READY',fallback=False,as_of='2026-10-14T10:02:00+08:00',breadth=.2)
    facts,reasons=module.original_facts((raw,'frozen-original'),tick=tick,observed_at=observed)
    assert frozen_candidates(facts,tick=tick,observed_at=observed,rules=load_rules(ROOT/'config/regime_live.yaml'))==('DATA_BLOCK','DATA_BLOCK')
    assert 'ORIGINAL_MARKET_NOT_YET_KNOWN_AT_DECISION' in reasons
