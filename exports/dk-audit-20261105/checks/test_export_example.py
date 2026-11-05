"""Packaging checks use unchanged original controlled fixtures, not market data."""
from pathlib import Path
import hashlib
import importlib.util
import json
import sys
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "snapshot" / "tests"))
from test_market_state_ssl import bars, clock
from liangjian_funnel.market_state.dk_archive import bar_identity

spec = importlib.util.spec_from_file_location("dk_export_example", ROOT / "examples" / "replay_closed_bars.py")
example = importlib.util.module_from_spec(spec)
spec.loader.exec_module(example)

def args(path):
    return ["--input", str(path), "--sha256", hashlib.sha256(path.read_bytes()).hexdigest(),
            "--observed-at", "2026-11-02T16:00:00+08:00"]

def test_hash_mismatch_rejected_before_any_calculation(tmp_path):
    path = tmp_path / "empty.json"
    path.write_text("[]")
    values = args(path)
    values[3] = "0" * 64
    with pytest.raises(ValueError, match="INPUT_SHA_MISMATCH"):
        example.main(values)

def test_empty_data_not_synthesized(tmp_path):
    path = tmp_path / "empty.json"
    path.write_text("[]")
    with pytest.raises(ValueError, match="REAL_INPUT_REQUIRED"):
        example.main(args(path))

def test_existing_controlled_fixture_can_cross_example_boundary(tmp_path, capsys):
    source = bars(130)  # Existing controlled test generator copied unchanged.
    path = tmp_path / "controlled-original-test-fixture.json"
    path.write_text(json.dumps([bar_identity(bar) for bar in source]), encoding="utf-8")
    result = example.main(args(path))
    assert result["authority"] == "SHADOW_ONLY" and result["enabled"] is False
    assert result["evaluation"]["coverage_status"] == "HISTORY_INSUFFICIENT_20_TRADE_DAYS"
    assert result["evaluation"]["event_count"] > 0
