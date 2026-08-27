from liangjian_funnel.pipeline.macro_regime import build_macro_asset_quadrant


def test_macro_quadrant_uses_momentum_flow_and_bounded_macro_tilts():
    result = build_macro_asset_quadrant({
        "ASSET_ROTATION_SNAPSHOT": {
            "available": True,
            "assets": {
                "EQUITY": {"momentum_20d_percentile": 90, "momentum_60d_percentile": 85, "fund_flow_percentile": 80},
                "GOLD": {"momentum_20d_percentile": 70, "momentum_60d_percentile": 75, "fund_flow_percentile": 65},
                "BOND": {"momentum_20d_percentile": 40, "momentum_60d_percentile": 45, "fund_flow_percentile": 50},
                "CASH": {"momentum_20d_percentile": 30, "momentum_60d_percentile": 35, "fund_flow_percentile": 40},
            },
        },
        "GLOBAL_MACRO_SNAPSHOT": {
            "available": True,
            "fed_easing_probability_percentile": 60,
            "usd_momentum_percentile": 45,
        },
        "MACRO_ECONOMIC_DATA": {
            "available": True,
            "credit_impulse_percentile": 75,
            "m1_m2_gap_percentile": 70,
        },
    })
    assert result["status"] == "READY"
    assert result["leading_asset"] == "EQUITY"
    assert result["quadrant"] == "RISK_ON_GROWTH"
    assert result["rules"]["llm_override_forbidden"] is True


def test_macro_quadrant_does_not_turn_missing_data_into_zero():
    result = build_macro_asset_quadrant({
        "ASSET_ROTATION_SNAPSHOT": {
            "available": False,
            "assets": {"EQUITY": {"momentum_20d_percentile": 80}},
        }
    })
    assert result["status"] == "UNAVAILABLE"
    assert result["asset_scores"] == {}
    assert result["leading_asset"] is None
