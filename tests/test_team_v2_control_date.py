"""0338: Monday RECORD is not the first CONTROL trading day."""
from datetime import datetime
from pathlib import Path

from liangjian_funnel.runtime.risk_engine import RiskLimits
from liangjian_funnel.runtime.team_v2_first_day import POLICY, first_day_limits, load_first_day_policy


def test_first_control_tightening_is_bound_to_tuesday_not_monday():
    policy = load_first_day_policy(Path(__file__).resolve().parents[1] / 'config/risk_limits.yaml')
    assert policy == POLICY and policy['activation_trade_date'] == '2026-10-20'
    limits = RiskLimits()
    monday = datetime.fromisoformat('2026-10-19T09:36:00+08:00')
    tuesday = datetime.fromisoformat('2026-10-20T09:36:00+08:00')
    assert first_day_limits(limits, policy, now=monday) == limits
    assert first_day_limits(limits, policy, now=tuesday).max_new_entries == 2
    assert policy['probe_only'] is True
