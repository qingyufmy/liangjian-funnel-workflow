from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Sequence

from .contracts import CapabilityStatus
from .probes.hithink import HithinkProbe
from .probes.models import ModelProbe
from .probes.mootdx import MootdxProbe
from .reporting import write_capability_report
from .settings import Settings, load_yaml
from .workflow import WorkflowApplication, WorkflowError


_SAFE_REASON_CODE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="liangjian-funnel")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("doctor", help="offline configuration and directory check")
    sub.add_parser("probe-hithink", help="run the real Hithink capability gate")
    sub.add_parser("probe-models", help="run model strict-JSON and thinking gates")
    sub.add_parser("probe-mootdx", help="run mootdx minute-history and cross-source gates")
    sub.add_parser("probe-all", help="run all external capability gates")
    snapshot = sub.add_parser("prepare-snapshot", help="freeze a real full-market research snapshot")
    snapshot.add_argument("--max-candidates", type=int, default=None)
    research = sub.add_parser("run-research", help="run three isolated A1-A2-A3 model lanes")
    research.add_argument("--slot", choices=("morning", "close"), required=True)
    research.add_argument("--max-candidates", type=int, default=None)
    sub.add_parser("monitor-once", help="run one A4 minute and paper-simulation cycle")
    sub.add_parser("run-due", help="dispatch only work due at the current Shanghai time")
    sub.add_parser("status", help="show redacted local workflow state")
    return parser


def main(argv: Sequence[str] | None = None, *, settings: Settings | None = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="strict")
    args = build_parser().parse_args(argv)
    active = settings or Settings.from_env()
    if args.command == "doctor":
        return _doctor(active)
    if args.command in {"prepare-snapshot", "run-research", "monitor-once", "run-due", "status"}:
        return _workflow_command(args, active)
    reports = []
    if args.command in {"probe-hithink", "probe-all"}:
        reports.append(HithinkProbe(active).run())
    if args.command in {"probe-models", "probe-all"}:
        reports.append(ModelProbe(active).run())
    if args.command in {"probe-mootdx", "probe-all"}:
        reports.append(MootdxProbe(active).run())
    active.output_dir.mkdir(parents=True, exist_ok=True)
    for report in reports:
        paths = write_capability_report(report, active.output_dir)
        print(json.dumps({"provider": report.provider, "status": report.overall_status.value, "reports": [str(path) for path in paths]}, ensure_ascii=False))
    return 0 if reports and all(report.overall_status is CapabilityStatus.PASS for report in reports) else 2


def _doctor(settings: Settings) -> int:
    checks: dict[str, object] = settings.safe_summary()
    required = [
        settings.root / "config" / "runtime.yaml",
        settings.root / "config" / "capability_specs.yaml",
        settings.root / "FINAL_IMPLEMENTATION_PLAN.md",
    ]
    checks["files"] = {str(path): path.is_file() for path in required}
    try:
        runtime = load_yaml(required[0])
        exact_models = tuple(runtime["models"]["research"]) == settings.research_models and runtime["models"]["monitor"] == settings.monitor_model
        runtime_contract = (
            runtime.get("schema_version") == "liangjian-runtime/1.0.0"
            and runtime.get("mode") in {"PHASE0_CAPABILITY_ONLY", "SIMULATION_WORKFLOW"}
            and runtime.get("timezone") == "Asia/Shanghai"
            and runtime.get("research_slots") == {"morning": "09:25", "close": "15:10"}
            and runtime.get("monitor", {}).get("cadence_seconds") == 60
            and runtime.get("permissions") == {
                "external_orders": False,
                "gm_fallback": False,
                "live_trading": False,
                "fast_track": False,
            }
        )
    except (OSError, ValueError, KeyError, TypeError):
        exact_models = False
        runtime_contract = False
    checks["exact_model_config"] = exact_models
    checks["runtime_contract"] = runtime_contract
    checks["phase0_ready_for_live_probe"] = bool(
        settings.hithink_api_key and settings.model_api_key and exact_models and runtime_contract
    )
    print(json.dumps(checks, ensure_ascii=False, indent=2, sort_keys=True))
    files_ok = all(checks["files"].values())  # type: ignore[union-attr]
    return 0 if files_ok and exact_models and runtime_contract else 2


def entrypoint() -> None:
    raise SystemExit(main())


def _workflow_command(args: argparse.Namespace, settings: Settings) -> int:
    try:
        application = WorkflowApplication(settings)
        if args.command == "prepare-snapshot":
            payload = application.prepare_snapshot(max_candidates=args.max_candidates).as_dict()
        elif args.command == "run-research":
            payload = application.run_research(args.slot, max_candidates=args.max_candidates)
        elif args.command == "monitor-once":
            payload = application.monitor_once()
        elif args.command == "run-due":
            payload = application.run_due()
        else:
            payload = {
                "state_db": str(settings.state_db_path),
                "state_healthy": application.store.healthy,
                "accounts": application.store.list_accounts(),
                "plan_counts": {
                    status: len(application.store.list_execution_plans(status=status))
                    for status in (
                        "DRAFT_CLOSE",
                        "PENDING_MORNING_REVIEW",
                        "ACTIVE_TODAY",
                        "INVALIDATED",
                        "CANCELLED",
                        "EXPIRED",
                    )
                },
                "effective_event_count": len(application.store.list_monitor_events(effective_only=True)),
            }
    except WorkflowError as exc:
        print(json.dumps({"status": "BLOCKED", "reason_code": exc.reason_code}, ensure_ascii=False))
        return 2
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(json.dumps({"status": "FAILED", "reason_code": type(exc).__name__}, ensure_ascii=False))
        return 2
    except Exception as exc:
        try:
            candidate = getattr(exc, "reason_code", None)
        except Exception:
            candidate = None
        reason_code = (
            candidate
            if isinstance(candidate, str) and _SAFE_REASON_CODE.fullmatch(candidate)
            else type(exc).__name__
        )
        if not _SAFE_REASON_CODE.fullmatch(reason_code):
            reason_code = "UNEXPECTED_RUNTIME_ERROR"
        print(json.dumps({"status": "FAILED", "reason_code": reason_code}, ensure_ascii=False))
        return 2
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    if args.command == "run-research" and payload.get("status") == "BLOCKED":
        return 2
    if args.command == "run-due":
        dispatch = payload.get("dispatch", []) if isinstance(payload, dict) else []
        if any(
            isinstance(record, dict) and record.get("status") in {"FAILED", "MISSED"}
            for record in dispatch
        ):
            return 2
    return 0
