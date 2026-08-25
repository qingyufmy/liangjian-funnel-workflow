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
    assert parameters["MAX_MA_BIAS"] == config["agent_3"]["moving_average_system"]["max_ma_bias_pct"]
    assert parameters["REQUIRED_CONFIRMATIONS"] == config["agent_3"]["required_confirmations"]
    assert parameters["TECHNICAL_SCORE_WEIGHTS"] == config["agent_3"]["score_weights"]
