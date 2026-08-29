#!/usr/bin/env python3
"""Enforce line coverage globally and branch coverage on critical contracts.

Coverage.py's single percentage combines statements and branches.  The
remediation contract requires two different gates instead: at least 80% of
all executable lines, and at least 90% line *and* branch coverage for the
small set of functions that own safety-critical workflow semantics.
"""

from __future__ import annotations

import argparse
import ast
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class CriticalTarget:
    file: str
    functions: tuple[str, ...]


CRITICAL_GROUPS: dict[str, tuple[CriticalTarget, ...]] = {
    "generation_activation_and_run_binding": (
        CriticalTarget(
            "src/liangjian_funnel/pipeline/feature_store.py",
            (
                "ResearchFeatureStore.validate_feature_generation",
                "ResearchFeatureStore.seal_generation",
                "ResearchFeatureStore.activate_generation",
                "ResearchFeatureStore.publish_feature_generation",
                "ResearchFeatureStore.bind_run_feature_generation",
                "ResearchFeatureStore.bind_run_to_active_generation",
                "ResearchFeatureStore.get_run_feature_binding",
            ),
        ),
    ),
    "outcome_contract_and_reducers": (
        CriticalTarget(
            "src/liangjian_funnel/pipeline/outcome_contract.py",
            ("contract_hash", "normalize_stage", "normalize_lane", "normalize_run", "validate_contract"),
        ),
        CriticalTarget(
            "src/liangjian_funnel/pipeline/outcomes.py",
            (
                "stage_outcome_from_legacy",
                "aggregate_lane_outcome",
                "aggregate_run_outcome",
                "aggregate_workflow_acceptance",
                "cli_exit_code",
            ),
        ),
    ),
    "a2_coverage_and_zero_result": (
        CriticalTarget(
            "src/liangjian_funnel/pipeline/a2_features.py",
            ("build_a2_feature_snapshot", "_dataset_observation", "_factor"),
        ),
        CriticalTarget(
            "src/liangjian_funnel/pipeline/deterministic.py",
            (
                "screen_a2",
                "_critical_factor_coverage",
                "_capital_flow_unavailable",
                "_capital_flow_available",
            ),
        ),
    ),
    "point_in_time_guards": (
        CriticalTarget(
            "src/liangjian_funnel/evaluation/replay_window.py",
            ("_check_context_identity", "evaluate_replay_window"),
        ),
        CriticalTarget(
            "src/liangjian_funnel/evaluation/broker_gold.py",
            ("_apply_as_of", "normalize_broker_gold_rows"),
        ),
    ),
    "primary_comparison_publication": (
        CriticalTarget(
            "src/liangjian_funnel/workflow.py",
            (
                "WorkflowApplication._create_comparison_request",
                "WorkflowApplication._claim_comparison_request",
                "WorkflowApplication._update_comparison_request",
                "WorkflowApplication.run_comparison",
            ),
        ),
    ),
    "paper_broker_state_transitions": (
        CriticalTarget(
            "src/liangjian_funnel/runtime/simulation.py",
            (
                "PaperBroker.start_trading_day",
                "PaperBroker.calculate_quantity",
                "PaperBroker.apply",
                "PaperBroker._adverse_price",
            ),
        ),
    ),
}


def _function_ranges(path: Path) -> dict[str, tuple[int, int]]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    ranges: dict[str, tuple[int, int]] = {}

    def visit(nodes: Iterable[ast.stmt], prefix: str = "") -> None:
        for node in nodes:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                name = f"{prefix}.{node.name}" if prefix else node.name
                ranges[name] = (node.lineno, node.end_lineno or node.lineno)
            elif isinstance(node, ast.ClassDef):
                name = f"{prefix}.{node.name}" if prefix else node.name
                visit(node.body, name)

    visit(tree.body)
    return ranges


def _coverage_entry(files: dict[str, Any], path: str) -> dict[str, Any]:
    candidates = (path, path.replace("/", "\\"), path.replace("\\", "/"))
    for candidate in candidates:
        value = files.get(candidate)
        if isinstance(value, dict):
            return value
    raise KeyError(f"coverage JSON does not contain {path}")


def _percentage(covered: int, total: int) -> float:
    return 100.0 if total == 0 else covered * 100.0 / total


def evaluate_coverage(
    report: dict[str, Any],
    *,
    root: Path,
    overall_minimum: float,
    critical_minimum: float,
) -> tuple[list[dict[str, Any]], list[str]]:
    files = report.get("files")
    if not isinstance(files, dict):
        raise ValueError("coverage report has no files mapping")
    totals = report.get("totals") if isinstance(report.get("totals"), dict) else {}
    statements = int(totals.get("num_statements") or 0)
    covered_statements = int(totals.get("covered_lines") or 0)
    overall_line = _percentage(covered_statements, statements)
    rows: list[dict[str, Any]] = [
        {
            "group": "overall",
            "line_covered": covered_statements,
            "line_total": statements,
            "line_percent": overall_line,
            "branch_covered": None,
            "branch_total": None,
            "branch_percent": None,
        }
    ]
    failures: list[str] = []
    if overall_line + 1e-9 < overall_minimum:
        failures.append(f"overall line coverage {overall_line:.2f}% < {overall_minimum:.2f}%")

    for group, targets in CRITICAL_GROUPS.items():
        all_statements: set[tuple[str, int]] = set()
        executed_statements: set[tuple[str, int]] = set()
        all_branches: set[tuple[str, int, int]] = set()
        executed_branches: set[tuple[str, int, int]] = set()
        for target in targets:
            entry = _coverage_entry(files, target.file)
            ranges = _function_ranges(root / target.file)
            executed_lines = {int(value) for value in entry.get("executed_lines", [])}
            missing_lines = {int(value) for value in entry.get("missing_lines", [])}
            executed_arcs = {tuple(map(int, value)) for value in entry.get("executed_branches", [])}
            missing_arcs = {tuple(map(int, value)) for value in entry.get("missing_branches", [])}
            for function in target.functions:
                if function not in ranges:
                    raise ValueError(f"critical function not found: {target.file}:{function}")
                start, end = ranges[function]
                for line in executed_lines | missing_lines:
                    if start <= line <= end:
                        all_statements.add((target.file, line))
                        if line in executed_lines:
                            executed_statements.add((target.file, line))
                for origin, destination in executed_arcs | missing_arcs:
                    if start <= origin <= end:
                        arc = (target.file, origin, destination)
                        all_branches.add(arc)
                        if (origin, destination) in executed_arcs:
                            executed_branches.add(arc)
        line_percent = _percentage(len(executed_statements), len(all_statements))
        branch_percent = _percentage(len(executed_branches), len(all_branches))
        rows.append(
            {
                "group": group,
                "line_covered": len(executed_statements),
                "line_total": len(all_statements),
                "line_percent": line_percent,
                "branch_covered": len(executed_branches),
                "branch_total": len(all_branches),
                "branch_percent": branch_percent,
            }
        )
        if line_percent + 1e-9 < critical_minimum:
            failures.append(f"{group} line coverage {line_percent:.2f}% < {critical_minimum:.2f}%")
        if branch_percent + 1e-9 < critical_minimum:
            failures.append(f"{group} branch coverage {branch_percent:.2f}% < {critical_minimum:.2f}%")
    return rows, failures


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--coverage-json", default="coverage.json")
    parser.add_argument("--root", default=".")
    parser.add_argument("--overall-minimum", type=float, default=80.0)
    parser.add_argument("--critical-minimum", type=float, default=90.0)
    args = parser.parse_args()
    report = json.loads(Path(args.coverage_json).read_text(encoding="utf-8"))
    rows, failures = evaluate_coverage(
        report,
        root=Path(args.root).resolve(),
        overall_minimum=args.overall_minimum,
        critical_minimum=args.critical_minimum,
    )
    for row in rows:
        branch = "n/a" if row["branch_percent"] is None else f'{row["branch_percent"]:.2f}%'
        print(f'{row["group"]}: line={row["line_percent"]:.2f}% branch={branch}')
    if failures:
        for failure in failures:
            print(f"COVERAGE_GATE_FAILED: {failure}")
        return 1
    print("COVERAGE_GATE_PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
