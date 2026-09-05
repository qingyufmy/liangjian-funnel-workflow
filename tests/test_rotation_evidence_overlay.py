import importlib.util
from datetime import datetime
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location("rotation_overlay", Path(__file__).resolve().parents[1] / "scripts/build_rotation_evidence_overlay.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def inputs():
    evidence = {"schema_version": module.OBSERVATION_SCHEMA, "trade_date": "2026-09-03",
                "observed_as_of": "2026-09-03T15:00:00+08:00", "retrospective_validation_only": True,
                "rotation_theme_count": 1, "observations": [{"theme_id": "INSURANCE",
                "membership_board_codes": ["BK0474"], "strength": 2356,
                "main_net_inflow_cny": 899000000, "primary_rank": 1, "occupies_primary_slot": True}]}
    membership = {"schema_version": module.ROTATION_SCHEMA, "trade_date": "2026-09-04",
                  "captured_at": "2026-09-04T22:00:00+08:00", "boards": [{
                  "theme_id": "INSURANCE", "membership_board_codes": ["BK0474"],
                  "constituents": ["601318.SH"]}]}
    membership["content_hash"] = module._canonical_hash(membership)
    return evidence, membership


def test_overlay_preserves_observation_and_actual_capture_separately():
    evidence, membership = inputs()
    result = module.build_overlay(evidence, membership)
    assert result["observed_as_of"] == evidence["observed_as_of"]
    assert datetime.fromisoformat(result["captured_at"]) >= datetime.fromisoformat(membership["captured_at"])
    assert result["quality"]["membership_known_after_target_date"] is True
    assert result["quality"]["production_publish_forbidden"] is True
    assert result["content_hash"] == module._canonical_hash(result)


def test_overlay_rejects_broad_member_substitution():
    evidence, membership = inputs()
    membership["boards"][0]["membership_board_codes"] = ["BK1203"]
    membership["content_hash"] = module._canonical_hash(membership)
    with pytest.raises(SystemExit, match="ROTATION_EXACT_MEMBERSHIP_REQUIRED"):
        module.build_overlay(evidence, membership)
