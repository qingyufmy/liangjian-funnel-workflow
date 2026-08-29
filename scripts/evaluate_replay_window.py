#!/usr/bin/env python3
"""Evaluate an offline A1-A3 point-in-time replay window.

The command only reads persisted run summaries/lane audits and writes a JSON
and Markdown report.  It does not call models, providers, real trading APIs,
or the runtime scheduler.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from liangjian_funnel.evaluation.replay_window import (
    DEFAULT_MINIMUM_DAYS,
    ReplayWindowContractError,
    evaluate_replay_window,
)


SHANGHAI = ZoneInfo("Asia/Shanghai")


def _default_outputs(runs_dir: Path) -> tuple[Path, Path]:
    root = runs_dir.resolve().parent / "evaluation"
    stamp = datetime.now(SHANGHAI).strftime("%Y%m%d-%H%M%S")
    return root / f"replay-window-{stamp}.json", root / f"replay-window-{stamp}.md"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs-dir", default="outputs/runs", help="directory containing persisted run-summary JSON files")
    parser.add_argument("--audit-dir", default="outputs/research", help="directory containing lane audit JSON files")
    parser.add_argument("--broker-gold-dir", help="optional offline broker-gold benchmark directory")
    parser.add_argument("--minimum-days", type=int, default=DEFAULT_MINIMUM_DAYS)
    parser.add_argument("--primary-lane", default="lane_1")
    parser.add_argument("--cutoff", help="optional timezone-aware ISO cutoff; defaults to current Asia/Shanghai time")
    parser.add_argument("--output-json", help="JSON output path; defaults to outputs/evaluation")
    parser.add_argument("--output-md", help="Markdown output path; defaults to outputs/evaluation")
    args = parser.parse_args(argv)

    runs_dir = Path(args.runs_dir).expanduser()
    default_json, default_md = _default_outputs(runs_dir)
    output_json = Path(args.output_json).expanduser() if args.output_json else default_json
    output_md = Path(args.output_md).expanduser() if args.output_md else default_md
    if bool(args.output_json) != bool(args.output_md):
        parser.error("--output-json and --output-md must be supplied together")
    try:
        report = evaluate_replay_window(
            runs_dir,
            audit_dir=args.audit_dir,
            broker_gold_dir=args.broker_gold_dir,
            minimum_days=args.minimum_days,
            primary_lane_id=args.primary_lane,
            cutoff=args.cutoff,
            output_json=output_json,
            output_markdown=output_md,
        )
    except ReplayWindowContractError as exc:
        print(json.dumps({"status": "REPLAY_CONTRACT_ERROR", "reason_code": exc.reason_code}, ensure_ascii=False), file=sys.stderr)
        return 4
    print(
        json.dumps(
            {
                "status": report["status"],
                "json": str(output_json.resolve()),
                "markdown": str(output_md.resolve()),
                "summary": report["summary"],
                "future_data_rejected": report["future_data_rejected"],
            },
            ensure_ascii=False,
        )
    )
    return 0 if report["status"] == "READY" else 2


if __name__ == "__main__":
    raise SystemExit(main())
