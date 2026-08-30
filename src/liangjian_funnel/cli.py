from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Mapping, Sequence
from zoneinfo import ZoneInfo

from .contracts import CapabilityStatus
from .pipeline.outcomes import aggregate_workflow_acceptance, cli_exit_code
from .evaluation.broker_gold import import_broker_gold
from .probes.hithink import HithinkProbe
from .probes.models import ModelProbe
from .probes.mootdx import MootdxProbe
from .reporting import write_capability_report
from .reporting import atomic_write_json
from .runtime.storage_governance import (
    StorageGovernanceError,
    backup_sqlite,
    storage_audit,
    storage_cleanup_plan,
)
from .settings import Settings, load_yaml
from .workflow import WorkflowApplication, WorkflowError


_SAFE_REASON_CODE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
def _latest_workflow_acceptance(
    workflow_runs: Sequence[dict[str, object]],
    *,
    expected_lanes: int = 3,
    required_lane_ids: Sequence[str] = ("lane_1",),
) -> dict[str, object]:
    """Summarise the newest run through the canonical outcome reducer.

    The returned shape intentionally remains the pre-v2 CLI contract.  The
    four-axis result is available to new callers through
    :func:`aggregate_workflow_acceptance`.
    """

    return aggregate_workflow_acceptance(
        workflow_runs,
        expected_lanes=expected_lanes,
        required_lane_ids=required_lane_ids,
    ).to_legacy_acceptance()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="liangjian-funnel")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("doctor", help="offline configuration and directory check")
    sub.add_parser("probe-hithink", help="run the real Hithink capability gate")
    sub.add_parser("probe-models", help="run model strict-JSON and thinking gates")
    sub.add_parser("probe-mootdx", help="run mootdx minute-history and cross-source gates")
    sub.add_parser("probe-all", help="run all external capability gates")
    sub.add_parser("prepare-snapshot", help="freeze a real full-market research snapshot")
    broker_gold = sub.add_parser("import-broker-gold", help="validate and persist monthly broker gold benchmark data")
    broker_gold.add_argument("source", help="strict CSV or JSON benchmark file")
    broker_gold.add_argument("--as-of", help="point-in-time cutoff; defaults to now in Asia/Shanghai")
    sub.add_parser("sync-data", help="bootstrap or incrementally refresh the local fact cache")
    maintain = sub.add_parser(
        "maintain-features",
        help="rebuild the local feature generation from the latest verified snapshot",
    )
    maintain.add_argument(
        "--full",
        action="store_true",
        help="force a full staging rebuild; Saturday defaults to full automatically",
    )
    research = sub.add_parser("run-research", help="run three isolated A1-A2-A3 model lanes")
    research.add_argument("--slot", choices=("morning", "close"), required=True)
    research.add_argument(
        "--as-of",
        default=None,
        help="historical ISO-8601 cutoff with timezone; keeps the current simulation trading day",
    )
    research.add_argument(
        "--snapshot-id",
        default=None,
        help="verified persisted snapshot id for an offline historical replay",
    )
    research.add_argument(
        "--primary-only",
        action="store_true",
        help="run only the configured primary research model and enqueue optional comparisons",
    )
    sub.add_parser("monitor-once", help="run one A4 minute and paper-simulation cycle")
    sub.add_parser("run-due", help="dispatch only work due at the current Shanghai time")
    sub.add_parser("run-morning", help="dispatch only the due 09:26 morning review")
    sub.add_parser("run-close", help="dispatch only the due 15:10 close workflow")
    sub.add_parser("run-monitor", help="dispatch only the current due A4 minute")
    comparison = sub.add_parser(
        "run-comparison",
        help="resume one durable optional-model comparison request without rerunning the primary",
    )
    comparison.add_argument("--parent-run-id", default=None, help="optional primary run id to process")
    sub.add_parser("status", help="show redacted local workflow state")
    storage_audit_parser = sub.add_parser(
        "storage-audit",
        help="read-only disk, SQLite integrity, and retention-reference audit",
    )
    storage_audit_parser.add_argument(
        "--path",
        default=None,
        help="filesystem path whose disk watermarks are measured (defaults to project root)",
    )
    storage_audit_parser.add_argument(
        "--feature-db",
        default=None,
        help="Feature Store SQLite path used for active/previous/run-bound reference scanning",
    )
    storage_audit_parser.add_argument(
        "--snapshot-root",
        action="append",
        default=[],
        help="snapshot/manifest root to scan for generation references; repeatable",
    )
    storage_backup_parser = sub.add_parser(
        "storage-backup",
        help="verified SQLite online backup with SHA-256 manifest and restore check",
    )
    storage_backup_parser.add_argument(
        "source_path",
        nargs="?",
        help="source SQLite database (also accepted through --source)",
    )
    storage_backup_parser.add_argument(
        "--source",
        dest="source_option",
        default=None,
        help="source SQLite database",
    )
    storage_backup_parser.add_argument(
        "--destination",
        "--output",
        dest="destination",
        default=None,
        help="new backup file; an existing file is never overwritten",
    )
    storage_backup_parser.add_argument(
        "--manifest",
        dest="manifest",
        default=None,
        help="optional new manifest path (defaults to <backup>.manifest.json)",
    )
    storage_cleanup_parser = sub.add_parser(
        "storage-cleanup",
        help="produce a reference-aware dry-run cleanup plan; never deletes in this build",
    )
    storage_cleanup_parser.add_argument(
        "--feature-db",
        default=None,
        help="Feature Store SQLite path",
    )
    storage_cleanup_parser.add_argument(
        "--snapshot-root",
        action="append",
        default=[],
        help="snapshot/manifest root to scan for generation references; repeatable",
    )
    storage_cleanup_parser.add_argument(
        "--execute",
        action="store_true",
        help="reserved for a future reviewed implementation; this build refuses execution",
    )
    return parser


def main(argv: Sequence[str] | None = None, *, settings: Settings | None = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="strict")
    args = build_parser().parse_args(argv)
    active = settings or Settings.from_env()
    if args.command == "doctor":
        return _doctor(active)
    if args.command in {"storage-audit", "storage-backup", "storage-cleanup"}:
        return _storage_command(args, active)
    if args.command in {"prepare-snapshot", "import-broker-gold", "sync-data", "maintain-features", "run-research", "run-comparison", "monitor-once", "run-due", "run-morning", "run-close", "run-monitor", "status"}:
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
        settings.source_config_path,
        settings.exchange_rules_path,
        settings.root / "FINAL_IMPLEMENTATION_PLAN.md",
    ]
    checks["files"] = {str(path): path.is_file() for path in required}
    try:
        runtime = load_yaml(required[0])
        funnel = load_yaml(settings.source_config_path)
        exchange_rules = load_yaml(settings.exchange_rules_path)
        exact_models = tuple(runtime["models"]["research"]) == settings.research_models and runtime["models"]["monitor"] == settings.monitor_model
        runtime_contract = (
            runtime.get("schema_version") == "liangjian-runtime/1.0.0"
            and runtime.get("mode") in {"PHASE0_CAPABILITY_ONLY", "SIMULATION_WORKFLOW"}
            and runtime.get("timezone") == "Asia/Shanghai"
            and runtime.get("research_slots") == {"morning": "09:26", "close": "15:10"}
            and runtime.get("monitor", {}).get("cadence_seconds") == 60
            and runtime.get("permissions") == {
                "external_orders": False,
                "gm_fallback": False,
                "live_trading": False,
                "fast_track": False,
            }
        )
        authority = funnel.get("data_sources", {}).get("authority", {})
        source_contract = (
            funnel.get("schema_version") == "astock-agent-funnel-v2"
            and funnel.get("runtime", {}).get("simulation_only") is True
            and funnel.get("runtime", {}).get("order_permission") == "DISABLED"
            and authority.get("realtime_quote") == "hithink_ths"
            and authority.get("minute_bars") == "mootdx"
            and funnel.get("universe_gate", {}).get("require_trading_calendar") is True
        )
        exchange_rule_contract = (
            exchange_rules.get("schema_version") == "liangjian-exchange-rules/1.0.0"
            and exchange_rules.get("simulation_only") is True
            and exchange_rules.get("external_orders") is False
            and exchange_rules.get("t_plus_one") is True
            and exchange_rules.get("lot_size") == 100
            and set(exchange_rules.get("sources", {})) == {"sse", "szse", "bse"}
        )
    except (OSError, ValueError, KeyError, TypeError):
        exact_models = False
        runtime_contract = False
        source_contract = False
        exchange_rule_contract = False
    checks["exact_model_config"] = exact_models
    checks["runtime_contract"] = runtime_contract
    checks["source_contract"] = source_contract
    checks["exchange_rule_contract"] = exchange_rule_contract
    checks["phase0_ready_for_live_probe"] = bool(
        settings.hithink_api_key
        and settings.model_api_key
        and exact_models
        and runtime_contract
        and source_contract
        and exchange_rule_contract
    )
    print(json.dumps(checks, ensure_ascii=False, indent=2, sort_keys=True))
    files_ok = all(checks["files"].values())  # type: ignore[union-attr]
    return 0 if files_ok and exact_models and runtime_contract and source_contract and exchange_rule_contract else 2


def entrypoint() -> None:
    raise SystemExit(main())


def _storage_command(args: argparse.Namespace, settings: Settings) -> int:
    """Run storage governance commands without constructing the workflow.

    Keeping these commands outside ``WorkflowApplication`` means a read-only
    audit cannot acquire scheduler leases or initialize a business database.
    The cleanup branch intentionally refuses ``--execute`` until a separately
    reviewed implementation exists.
    """

    try:
        if args.command == "storage-audit":
            root = Path(args.path).resolve() if args.path else settings.root
            feature_db = Path(args.feature_db).resolve() if args.feature_db else settings.feature_store_db_path
            database_paths = tuple(
                dict.fromkeys(
                    (
                        feature_db,
                        settings.fact_cache_db_path,
                        settings.state_db_path,
                    )
                )
            )
            snapshot_roots = tuple(
                Path(item).resolve() for item in (args.snapshot_root or [str(settings.snapshot_dir)])
            )
            payload = storage_audit(
                root,
                database_paths=database_paths,
                feature_store_db=feature_db,
                snapshot_roots=snapshot_roots,
            )
            print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str))
            return 0 if payload.get("status") in {"OK", "WARNING"} else 2

        if args.command == "storage-backup":
            source_raw = args.source_option or args.source_path
            if not source_raw:
                print(json.dumps({"status": "FAILED", "reason_code": "SQLITE_SOURCE_REQUIRED"}, ensure_ascii=False))
                return 2
            source = Path(source_raw).resolve()
            if args.destination:
                destination = Path(args.destination).resolve()
            else:
                stamp = datetime.now(ZoneInfo(settings.timezone)).strftime("%Y%m%dT%H%M%S%z")
                destination = settings.root / "storage" / "backups" / f"{source.stem}-{stamp}.sqlite3"
            payload = backup_sqlite(
                source,
                destination,
                manifest_path=args.manifest,
                verify_restore=True,
            )
            print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str))
            return 0

        # ``storage-cleanup`` is a proof-producing dry-run only.  Even an
        # explicit --execute is rejected rather than being interpreted by a
        # future caller as authorization to delete data.
        feature_db = Path(args.feature_db).resolve() if args.feature_db else settings.feature_store_db_path
        snapshot_roots = tuple(Path(item).resolve() for item in args.snapshot_root)
        payload = storage_cleanup_plan(feature_db, snapshot_roots=snapshot_roots)
        if args.execute:
            payload = {
                **payload,
                "status": "BLOCKED",
                "reason_code": "STORAGE_CLEANUP_EXECUTION_NOT_IMPLEMENTED",
                "dry_run": True,
                "deletion_allowed": False,
            }
            print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str))
            return 2
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str))
        return 0
    except StorageGovernanceError as exc:
        reason_code = str(exc).split(":", 1)[0]
        if not _SAFE_REASON_CODE.fullmatch(reason_code):
            reason_code = "STORAGE_GOVERNANCE_FAILED"
        print(json.dumps({"status": "FAILED", "reason_code": reason_code}, ensure_ascii=False))
        return 3
    except (OSError, ValueError, TypeError) as exc:
        print(json.dumps({"status": "FAILED", "reason_code": type(exc).__name__}, ensure_ascii=False))
        return 3


def _workflow_command(args: argparse.Namespace, settings: Settings) -> int:
    try:
        if args.command == "maintain-features":
            from .pipeline.feature_maintenance import run_feature_maintenance

            payload = run_feature_maintenance(
                settings,
                full=bool(args.full),
                now=datetime.now(ZoneInfo(settings.timezone)),
            )
            print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str))
            return 0 if payload.get("status") in {"PUBLISHED", "NOOP"} else 2
        application = WorkflowApplication(settings)
        if args.command == "import-broker-gold":
            cutoff = datetime.fromisoformat(args.as_of) if args.as_of else datetime.now(ZoneInfo(settings.timezone))
            if cutoff.tzinfo is None or cutoff.utcoffset() is None:
                cutoff = cutoff.replace(tzinfo=ZoneInfo(settings.timezone))
            dataset = import_broker_gold(Path(args.source), as_of=cutoff)
            by_month: dict[str, list[dict[str, object]]] = defaultdict(list)
            for record in dataset.records:
                by_month[record.month].append(record.as_dict())
            written: list[str] = []
            for month, records in sorted(by_month.items()):
                target = settings.broker_gold_dir / f"{month}.json"
                atomic_write_json(target, {"records": records})
                written.append(str(target))
            payload = {
                "status": "IMPORTED" if written else "EMPTY",
                "benchmark_not_runtime_input": True,
                "record_count": len(dataset.records),
                "excluded_future_count": len(dataset.excluded_future),
                "duplicate_count": dataset.duplicate_count,
                "months": list(dataset.months),
                "written": written,
            }
        elif args.command == "prepare-snapshot":
            payload = application.prepare_snapshot().as_dict()
        elif args.command == "sync-data":
            payload = application.sync_data_cache()
        elif args.command == "run-research":
            historical_as_of = datetime.fromisoformat(args.as_of) if args.as_of else None
            research_kwargs = {
                "as_of": historical_as_of,
                "historical_replay": historical_as_of is not None,
                "snapshot_id": args.snapshot_id,
            }
            if args.primary_only:
                research_kwargs.update(
                    primary_only=True,
                    schedule_comparison=settings.comparison_enabled,
                )
            payload = application.run_research(args.slot, **research_kwargs)
        elif args.command == "run-comparison":
            payload = application.run_comparison(parent_run_id=args.parent_run_id)
        elif args.command == "monitor-once":
            payload = application.monitor_once()
        elif args.command == "run-due":
            payload = application.run_due()
        elif args.command == "run-morning":
            from .runtime.scheduler import ScheduleKind

            payload = application.run_scheduled(ScheduleKind.MORNING_0925)
        elif args.command == "run-close":
            from .runtime.scheduler import ScheduleKind

            payload = application.run_scheduled(ScheduleKind.CLOSE_1510)
        elif args.command == "run-monitor":
            from .runtime.scheduler import ScheduleKind

            payload = application.run_scheduled(ScheduleKind.MONITOR)
        else:
            workflow_runs = application.store.list_workflow_runs(limit=12)
            workflow_outcome = aggregate_workflow_acceptance(
                workflow_runs,
                expected_lanes=3,
                required_lane_ids=(settings.research_primary_lane_id,),
            )
            workflow_acceptance = workflow_outcome.to_legacy_acceptance()
            configuration_ready = bool(
                application.store.healthy
                and settings.hithink_api_key
                and settings.model_api_key
                and settings.exchange_rules_path.is_file()
            )
            payload = {
                "state_db": str(settings.state_db_path),
                "state_healthy": application.store.healthy,
                "fact_cache": application.fact_cache.get_coverage(),
                "accounts": application.store.list_accounts(),
                "positions": {
                    account["account_id"]: application.store.list_positions(str(account["account_id"]))
                    for account in application.store.list_accounts()
                },
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
                "latest_workflow_runs": workflow_runs,
                "scheduler_leases": application.store.list_leases(),
                "configuration_ready": configuration_ready,
                "latest_workflow_acceptance": workflow_acceptance,
                "latest_workflow_outcome_v2": workflow_outcome.as_dict(),
                "deployment_ready": bool(
                    configuration_ready
                    and workflow_acceptance["status"] in {"READY", "READY_DEGRADED"}
                ),
                "deployment_blockers": [
                    reason
                    for blocked, reason in (
                        (not application.store.healthy, "STATE_DB_UNHEALTHY"),
                        (settings.hithink_api_key is None, "HITHINK_API_KEY_MISSING"),
                        (settings.model_api_key is None, "MODEL_API_KEY_MISSING"),
                        (not settings.exchange_rules_path.is_file(), "EXCHANGE_RULE_SNAPSHOT_MISSING"),
                        (
                            workflow_acceptance["status"] not in {"READY", "READY_DEGRADED"},
                            "LATEST_WORKFLOW_NOT_READY",
                        ),
                    )
                    if blocked
                ],
            }
    except WorkflowError as exc:
        print(json.dumps({"status": "BLOCKED", "reason_code": exc.reason_code}, ensure_ascii=False))
        return 2
    except KeyboardInterrupt:
        print(json.dumps({"status": "CANCELLED", "reason_code": "RUN_CANCELLED"}, ensure_ascii=False))
        return 130
    except (ValueError, KeyError, TypeError) as exc:
        print(json.dumps({"status": "FAILED", "reason_code": type(exc).__name__}, ensure_ascii=False))
        return 4
    except OSError as exc:
        print(json.dumps({"status": "FAILED", "reason_code": type(exc).__name__}, ensure_ascii=False))
        return 3
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
        return 3
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    if args.command == "run-research":
        outcome = payload.get("outcome_v2") if isinstance(payload, Mapping) else None
        return cli_exit_code(outcome if isinstance(outcome, Mapping) else payload)
    if args.command in {"run-due", "run-morning", "run-close", "run-monitor"}:
        dispatch = payload.get("dispatch", []) if isinstance(payload, dict) else []
        if any(
            isinstance(record, dict) and record.get("status") in {"FAILED", "MISSED"}
            for record in dispatch
        ):
            return 2
    if args.command == "run-comparison" and isinstance(payload, Mapping) and payload.get("status") == "FAILED":
        return 2
    return 0
