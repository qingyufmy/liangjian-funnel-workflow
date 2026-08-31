from __future__ import annotations

from pathlib import Path

import yaml

from liangjian_funnel.workflow import _prompt_parameters


def test_prompt_parameters_are_derived_from_versioned_funnel_config() -> None:
    config_path = Path(__file__).parents[1] / "config" / "funnel_config_v2.yaml"
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))

    parameters = _prompt_parameters(config)

    assert parameters["PRIOR_CONTRIBUTION_CAP"] == config["theme_registry"]["prior_contribution_cap"]
    assert parameters["THEME_EXPIRY_DAYS"] == config["theme_registry"]["theme_expiry_without_confirmation_days"]
    assert parameters["POLICY_CALENDAR_HORIZON_DAYS"] == config["agent_1"]["policy_research"]["policy_calendar_horizon_days"]
    assert parameters["MIN_SECTOR_COVERAGE"] == config["data_policy"]["minimum_sector_coverage"]
    assert parameters["ROTATION_LOOKBACK_DAYS"] == config["market_regime"]["rotation_lookback_days"]
    assert parameters["LEADER_MIN_CRITERIA"] == config["agent_2"]["stock_selection"]["leader_min_criteria"]
    assert parameters["CLIMAX_NEW_ENTRY_POLICY"] == config["agent_2"]["climax_new_entry_policy"]
    assert parameters["A3_STRATEGY_VERSION"] == config["agent_3"]["strategy_version"]
    assert parameters["A3_ALLOWED_STRATEGIES"] == config["agent_3"]["allowed_strategy_profiles"]
    assert parameters["A3_DECISION_TIMEFRAMES"] == config["agent_3"]["decision_timeframes"]
    assert "MIN_TECHNICAL_SCORE" not in parameters
    assert "TECHNICAL_SCORE_WEIGHTS" not in parameters
    assert "REQUIRED_CONFIRMATIONS" not in parameters
