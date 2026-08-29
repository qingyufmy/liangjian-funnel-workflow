from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "scripts" / "check_critical_coverage.py"
SPEC = importlib.util.spec_from_file_location("critical_coverage_gate", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def test_coverage_gate_separates_global_lines_from_critical_branches(tmp_path, monkeypatch) -> None:
    source = tmp_path / "critical.py"
    source.write_text(
        "def decision(value):\n"
        "    if value:\n"
        "        return 1\n"
        "    return 0\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        MODULE,
        "CRITICAL_GROUPS",
        {"decision_contract": (MODULE.CriticalTarget("critical.py", ("decision",)),)},
    )
    report = {
        "totals": {"covered_lines": 8, "num_statements": 10},
        "files": {
            "critical.py": {
                "executed_lines": [1, 2, 3],
                "missing_lines": [4],
                "executed_branches": [[2, 3]],
                "missing_branches": [[2, 4]],
            }
        },
    }
    rows, failures = MODULE.evaluate_coverage(
        report,
        root=tmp_path,
        overall_minimum=80.0,
        critical_minimum=90.0,
    )
    assert rows[0]["line_percent"] == 80.0
    assert rows[1]["branch_percent"] == 50.0
    assert failures == ["decision_contract line coverage 75.00% < 90.00%", "decision_contract branch coverage 50.00% < 90.00%"]


def test_coverage_gate_accepts_fully_exercised_critical_contract(tmp_path, monkeypatch) -> None:
    source = tmp_path / "critical.py"
    source.write_text(
        "def decision(value):\n"
        "    if value:\n"
        "        return 1\n"
        "    return 0\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        MODULE,
        "CRITICAL_GROUPS",
        {"decision_contract": (MODULE.CriticalTarget("critical.py", ("decision",)),)},
    )
    report = {
        "totals": {"covered_lines": 10, "num_statements": 10},
        "files": {
            "critical.py": {
                "executed_lines": [1, 2, 3, 4],
                "missing_lines": [],
                "executed_branches": [[2, 3], [2, 4]],
                "missing_branches": [],
            }
        },
    }
    _, failures = MODULE.evaluate_coverage(
        report,
        root=tmp_path,
        overall_minimum=80.0,
        critical_minimum=90.0,
    )
    assert failures == []
