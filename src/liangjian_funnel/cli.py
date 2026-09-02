from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Mapping, Sequence
from zoneinfo import ZoneInfo

from .contracts import CapabilityStatus
from .pipeline.outcomes import PublicationState, RunOutcome, aggregate_workflow_acceptance, cli_exit_code
from .pipeline.a1_registry import A1RegistryError, DEFAULT_A1_DEGRADED_AFTER, DEFAULT_A1_MAX_AGE
from .evaluation.broker_gold import import_broker_gold
from .evaluation.outcome_labels import OutcomeLabelError, backfill_forward_returns
from .evaluation.replay_window import layer_attribution
from .probes.hithink import HithinkProbe
from .probes.models import ModelProbe
from .probes.mootdx import MootdxProbe
from .reporting import write_capability_report
from .reporting import atomic_write_json
from .runtime.storage_governance import (
    RETENTION_DEFAULT_KEEP_DAYS,
    StorageGovernanceError,
    backup_sqlite,
    storage_audit,
    storage_cleanup_execute,
    storage_cleanup_plan,
)
from .runtime.state import PlanStatus, RuntimeStateError, RuntimeStore
from .settings import Settings, load_yaml
from .workflow import WorkflowApplication, WorkflowError


_SAFE_REASON_CODE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _primary_workflow_publishable(outcome: RunOutcome) -> bool:
    """Return whether the required primary lane is safe to publish.

    Optional comparison lanes deliberately remain visible through
    ``comparison_status`` and the legacy acceptance projection, but they must
    not turn a production-ready primary lane into a deployment blocker.
    """

    return outcome.publication_state in {
        PublicationState.READY,
        PublicationState.PUBLISHED,
    }


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


def _monitor_plan_projection(plan: Mapping[str, object]) -> dict[str, object]:
    try:
        payload = json.loads(str(plan.get("payload_json") or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        payload = {}
    if not isinstance(payload, Mapping):
        payload = {}
    reasons = payload.get("selection_reasons")
    if not isinstance(reasons, list):
        reasons = payload.get("reason_codes") if isinstance(payload.get("reason_codes"), list) else []
    plan_id = str(plan.get("plan_id") or "")
    lane_id = str(plan.get("lane_id") or "")
    source_run_id = payload.get("source_run_id")
    lineage_marker = f":{lane_id}:" if lane_id else ""
    if not source_run_id and lineage_marker and lineage_marker in plan_id:
        source_run_id = plan_id.split(lineage_marker, 1)[0]
    return {
        "plan_id": plan_id,
        "lane_id": lane_id,
        "symbol": str(plan.get("symbol") or ""),
        "name": str(payload.get("name") or ""),
        "status": str(plan.get("status") or ""),
        "valid_from": plan.get("valid_from"),
        "expires_at": plan.get("expires_at"),
        "strategy_profile": payload.get("strategy_profile"),
        "eligibility": payload.get("eligibility"),
        "setup_type": payload.get("setup_type"),
        "trigger_low": payload.get("trigger_low"),
        "trigger_high": payload.get("trigger_high"),
        "stop_level": payload.get("stop_level"),
        "risk_unit": payload.get("risk_unit"),
        "no_chase_price": payload.get("no_chase_price", payload.get("max_chase_price")),
        "required_conditions": payload.get("required_conditions") if isinstance(payload.get("required_conditions"), list) else [],
        "met_conditions": payload.get("met_conditions") if isinstance(payload.get("met_conditions"), list) else [],
        "unmet_conditions": payload.get("unmet_conditions") if isinstance(payload.get("unmet_conditions"), list) else [],
        "veto_conditions": payload.get("veto_conditions") if isinstance(payload.get("veto_conditions"), list) else [],
        "source_run_id": source_run_id,
        "selection_reasons": [str(item)[:500] for item in reasons[:6]],
    }


def _monitor_event_projection(
    event: Mapping[str, object],
    *,
    plans: Mapping[str, Mapping[str, object]],
    fills: Mapping[str, Mapping[str, object]],
) -> dict[str, object]:
    try:
        payload = json.loads(str(event.get("payload_json") or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        payload = {}
    if not isinstance(payload, Mapping):
        payload = {}
    plan_id = str(payload.get("plan_id") or "")
    plan = plans.get(plan_id, {})
    strategy = payload.get("strategy")
    strategy = strategy if isinstance(strategy, Mapping) else {}
    event_key = str(event.get("event_key") or "")
    fill = fills.get(event_key)
    return {
        "event_id": event.get("event_id"),
        "event_key": event_key,
        "minute_end": event.get("minute_end"),
        "lane_id": event.get("lane_id"),
        "plan_id": plan_id or None,
        "symbol": payload.get("symbol") or plan.get("symbol"),
        "name": plan.get("name"),
        "action": event.get("action"),
        "reason_code": event.get("reason_code"),
        "diagnostic_code": payload.get("diagnostic_code"),
        "strategy_profile": strategy.get("strategy_profile") or plan.get("strategy_profile"),
        "eligibility": plan.get("eligibility"),
        "reason_codes": strategy.get("reason_codes") if isinstance(strategy.get("reason_codes"), list) else [],
        "met_conditions": strategy.get("met_conditions") if isinstance(strategy.get("met_conditions"), list) else [],
        "unmet_conditions": strategy.get("unmet_conditions") if isinstance(strategy.get("unmet_conditions"), list) else [],
        "veto_conditions": strategy.get("veto_conditions") if isinstance(strategy.get("veto_conditions"), list) else [],
        "closed_5m_end": strategy.get("closed_5m_end"),
        "closed_15m_end": strategy.get("closed_15m_end"),
        "effective": bool(event.get("effective")),
        "llm_veto": bool(payload.get("llm_veto")),
        "plan": plan or None,
        "simulation": (
            {
                "status": "FILLED",
                "action": fill.get("action"),
                "qty": fill.get("qty"),
                "price": fill.get("price"),
                "fee": fill.get("fee"),
                "bar_end": fill.get("bar_end"),
            }
            if fill is not None
            else None
        ),
    }


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
    a1_maintenance = sub.add_parser(
        "run-a1-maintenance",
        help="run the due 18:00 monthly-full or weekly-incremental A1 maintenance",
    )
    a1_maintenance.add_argument(
        "--mode",
        choices=("full", "incremental"),
        default=None,
        help="explicit maintenance mode; omit to apply the exchange-calendar schedule",
    )
    a1_maintenance.add_argument(
        "--as-of",
        default=None,
        help="timezone-aware execution timestamp; defaults to current Asia/Shanghai time",
    )
    a1_maintenance.add_argument(
        "--snapshot-id",
        default=None,
        help="reuse one verified same-day frozen snapshot after a model-only A1 failure",
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
    activate_a3 = sub.add_parser(
        "activate-latest-a3-for-a4",
        help="explicitly bind the latest published A3 plans to the current A4 session",
    )
    activate_a3.add_argument(
        "--as-of",
        default=None,
        help="timezone-aware operator timestamp; defaults to current Asia/Shanghai time",
    )
    sub.add_parser("run-due", help="dispatch only work due at the current Shanghai time")
    sub.add_parser("run-premarket", help="dispatch only the due 08:30 A3 premarket analysis")
    sub.add_parser("run-morning", help="dispatch only the due 09:26 morning review")
    sub.add_parser("run-close", help="dispatch only the due 15:10 close workflow")
    sub.add_parser(
        "run-next-session-prep",
        help="prepare one clean close A1-A3 run for the nearest next trading session",
    )
    sub.add_parser("run-monitor", help="dispatch only the current due A4 minute")
    outcomes = sub.add_parser(
        "label-outcomes",
        help="backfill deterministic forward outcome labels from local prices",
    )
    outcomes.add_argument(
        "--as-of",
        required=True,
        help="latest local market date allowed for forward-return backfill (YYYY-MM-DD)",
    )
    outcomes.add_argument(
        "--price-source",
        default=None,
        help="local JSON/CSV or SQLite daily-bar source; defaults to the fact cache",
    )
    attribution = sub.add_parser(
        "layer-attribution",
        help="calculate deterministic funnel-layer outcome attribution",
    )
    attribution.add_argument("--from", dest="from_date", required=True, help="first trade date (YYYY-MM-DD)")
    attribution.add_argument("--to", dest="to_date", required=True, help="last trade date (YYYY-MM-DD)")
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
        help="plan or execute a guarded gzip-archive retention pass",
    )
    storage_cleanup_parser.add_argument(
        "--root",
        default=None,
        help="explicit project root (required with --execute)",
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
        "--manifest",
        default=None,
        help="plan manifest to write in dry-run or read during --execute",
    )
    storage_cleanup_parser.add_argument(
        "--policy",
        default=None,
        help="retention policy identifier; required with --execute",
    )
    storage_cleanup_parser.add_argument(
        "--cutoff-hours",
        type=float,
        default=None,
        help=f"age cutoff in hours (dry-run default: {RETENTION_DEFAULT_KEEP_DAYS * 24})",
    )
    storage_cleanup_parser.add_argument(
        "--keep-days",
        type=float,
        default=None,
        help="compatibility alias for --cutoff-hours/24",
    )
    storage_cleanup_parser.add_argument(
        "--protected-pattern",
        action="append",
        default=[],
        help="case-insensitive filename/path pattern to protect; repeatable",
    )
    storage_cleanup_parser.add_argument(
        "--protected-run-id",
        action="append",
        default=[],
        help="run id whose files must be protected; repeatable",
    )
    storage_cleanup_parser.add_argument(
        "--confirm-token",
        "--confirmation-token",
        dest="confirm_token",
        default=None,
        help="exact plan_id returned by the dry-run manifest",
    )
    storage_cleanup_parser.add_argument(
        "--confirm-manifest",
        default=None,
        help="optional explicit confirmation JSON containing plan_id and confirmed=true",
    )
    storage_cleanup_parser.add_argument(
        "--purge",
        action="store_true",
        help="reserved; permanent deletion is intentionally unavailable",
    )
    storage_cleanup_parser.add_argument(
        "--execute",
        action="store_true",
        help="execute a previously written plan after explicit root/policy/token confirmation",
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
    if args.command in {"label-outcomes", "layer-attribution"}:
        return _evaluation_command(args, active)
    if args.command in {"prepare-snapshot", "import-broker-gold", "sync-data", "maintain-features", "run-a1-maintenance", "run-research", "run-comparison", "monitor-once", "activate-latest-a3-for-a4", "run-due", "run-premarket", "run-morning", "run-close", "run-next-session-prep", "run-monitor", "status"}:
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
            and runtime.get("research_slots") == {"premarket": "08:30", "morning": "09:26", "close": "15:10"}
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
    Cleanup is deliberately kept outside ``WorkflowApplication``.  Planning
    reads only the configured state/progress files, while execution consumes a
    previously written plan and never opens a writable SQLite connection.
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

        if args.purge:
            print(json.dumps({
                "status": "BLOCKED",
                "reason_code": "STORAGE_CLEANUP_PURGE_NOT_IMPLEMENTED",
            }, ensure_ascii=False))
            return 2
        if args.execute:
            missing = (
                "STORAGE_CLEANUP_ROOT_REQUIRED" if not args.root else
                "STORAGE_CLEANUP_POLICY_REQUIRED" if not args.policy else
                "STORAGE_CLEANUP_MANIFEST_REQUIRED" if not args.manifest else
                "STORAGE_CLEANUP_CONFIRMATION_REQUIRED"
                if not args.confirm_token and not args.confirm_manifest else None
            )
            if missing:
                print(json.dumps({"status": "BLOCKED", "reason_code": missing}, ensure_ascii=False))
                return 2
            payload = storage_cleanup_execute(
                args.manifest,
                root=Path(args.root),
                policy=args.policy,
                confirmation_token=args.confirm_token,
                confirmation_manifest=args.confirm_manifest,
            )
            print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str))
            return 0 if payload.get("status") in {"EXECUTED", "IDEMPOTENT", "NOOP"} else 2

        feature_db = Path(args.feature_db).resolve() if args.feature_db else settings.feature_store_db_path
        # Keep the raw root for storage-governance symlink checks; resolving it
        # here would hide a symlink before the safety boundary sees it.
        root = Path(args.root) if args.root else settings.root
        snapshot_roots = tuple(
            Path(item).resolve() for item in (args.snapshot_root or [str(root / "storage" / "snapshots")])
        )
        cutoff_hours = args.cutoff_hours
        if args.keep_days is not None:
            keep_cutoff = float(args.keep_days) * 24.0
            if cutoff_hours is not None and float(cutoff_hours) != keep_cutoff:
                print(json.dumps({"status": "FAILED", "reason_code": "STORAGE_RETENTION_CUTOFF_CONFLICT"}, ensure_ascii=False))
                return 3
            cutoff_hours = keep_cutoff
        payload = storage_cleanup_plan(
            feature_db,
            root=root,
            snapshot_roots=snapshot_roots,
            workflow_progress_path=settings.workflow_progress_path,
            cutoff_hours=cutoff_hours,
            policy=args.policy,
            protected_patterns=tuple(args.protected_pattern),
            protected_run_ids=tuple(args.protected_run_id),
            manifest_path=args.manifest,
        )
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


def _evaluation_command(args: argparse.Namespace, settings: Settings) -> int:
    """Run deterministic outcome evaluation without constructing the workflow.

    These commands intentionally have no ``WorkflowApplication`` path: they
    only read/write the local SQLite outcome ledger and local price source.
    In particular, they cannot acquire a scheduler lease, call a model, or
    create a research/monitoring plan as a side effect.
    """

    try:
        store = RuntimeStore(settings.state_db_path)
        if args.command == "label-outcomes":
            source = (
                Path(args.price_source).expanduser().resolve()
                if args.price_source
                else settings.fact_cache_db_path
            )
            payload = backfill_forward_returns(
                store,
                as_of_date=args.as_of,
                price_source=source,
            )
            print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str))
            return 0 if not payload.get("source_errors") else 2

        try:
            from_date = date.fromisoformat(str(args.from_date))
            to_date = date.fromisoformat(str(args.to_date))
        except ValueError:
            print(json.dumps({"status": "FAILED", "reason_code": "DATE_INVALID"}, ensure_ascii=False))
            return 3
        if from_date > to_date:
            print(json.dumps({"status": "FAILED", "reason_code": "DATE_RANGE_INVALID"}, ensure_ascii=False))
            return 3
        labels = tuple(
            row
            for row in store.list_outcome_labels(labeled_only=False)
            if from_date <= date.fromisoformat(str(row["trade_date"])) <= to_date
        )
        runs = tuple(
            row
            for row in store.list_workflow_runs(limit=200)
            if _date_in_range(row.get("trade_date"), from_date, to_date)
        )
        payload = layer_attribution(runs, labels)
        payload = {
            **payload,
            "from_date": from_date.isoformat(),
            "to_date": to_date.isoformat(),
            "label_count": len(labels),
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str))
        return 0 if payload.get("status") == "READY" else 2
    except OutcomeLabelError as exc:
        reason_code = exc.reason_code if _SAFE_REASON_CODE.fullmatch(exc.reason_code) else "OUTCOME_EVALUATION_FAILED"
        print(json.dumps({"status": "FAILED", "reason_code": reason_code}, ensure_ascii=False))
        return 3
    except RuntimeStateError as exc:
        reason_code = exc.reason_code if _SAFE_REASON_CODE.fullmatch(exc.reason_code) else "RUNTIME_STATE_FAILED"
        print(json.dumps({"status": "FAILED", "reason_code": reason_code}, ensure_ascii=False))
        return 3
    except (OSError, ValueError, TypeError) as exc:
        print(json.dumps({"status": "FAILED", "reason_code": type(exc).__name__}, ensure_ascii=False))
        return 3


def _date_in_range(value: object, start: date, end: date) -> bool:
    try:
        parsed = date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return False
    return start <= parsed <= end


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
        if args.command == "run-a1-maintenance":
            maintenance_at = datetime.fromisoformat(args.as_of) if args.as_of else None
            if maintenance_at is not None and (maintenance_at.tzinfo is None or maintenance_at.utcoffset() is None):
                maintenance_at = maintenance_at.replace(tzinfo=ZoneInfo(settings.timezone))
            payload = application.run_a1_maintenance(
                now=maintenance_at,
                mode=args.mode,
                snapshot_id=args.snapshot_id,
            )
            print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str))
            return 0 if payload.get("status") in {"PUBLISHED", "NOOP", "NOT_DUE"} else 2
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
        elif args.command == "activate-latest-a3-for-a4":
            activation_at = datetime.fromisoformat(args.as_of) if args.as_of else None
            if activation_at is not None and (activation_at.tzinfo is None or activation_at.utcoffset() is None):
                activation_at = activation_at.replace(tzinfo=ZoneInfo(settings.timezone))
            payload = application.activate_latest_a3_for_a4(now=activation_at)
        elif args.command == "run-due":
            payload = application.run_due()
        elif args.command == "run-premarket":
            from .runtime.scheduler import ScheduleKind

            payload = application.run_scheduled(ScheduleKind.PREMARKET_0830)
        elif args.command == "run-morning":
            from .runtime.scheduler import ScheduleKind

            payload = application.run_scheduled(ScheduleKind.MORNING_0925)
        elif args.command == "run-close":
            from .runtime.scheduler import ScheduleKind

            payload = application.run_scheduled(ScheduleKind.CLOSE_1510)
        elif args.command == "run-next-session-prep":
            payload = application.run_next_session_prep()
        elif args.command == "run-monitor":
            from .runtime.scheduler import ScheduleKind

            payload = application.run_scheduled(ScheduleKind.MONITOR)
        else:
            workflow_runs = application.store.list_workflow_runs(limit=12)
            expected_workflow_lanes = (
                len(settings.research_models)
                if settings.comparison_enabled
                else 1
            )
            workflow_outcome = aggregate_workflow_acceptance(
                workflow_runs,
                expected_lanes=expected_workflow_lanes,
                required_lane_ids=(settings.research_primary_lane_id,),
            )
            workflow_acceptance = workflow_outcome.to_legacy_acceptance()
            primary_workflow_publishable = _primary_workflow_publishable(workflow_outcome)
            configuration_ready = bool(
                application.store.healthy
                and settings.hithink_api_key
                and settings.model_api_key
                and settings.exchange_rules_path.is_file()
            )
            monitor_plans = tuple(
                _monitor_plan_projection(plan)
                for status in (PlanStatus.ACTIVE_TODAY, PlanStatus.PENDING_MORNING_REVIEW)
                for plan in application.store.list_execution_plans(status=status)
            )
            plan_by_id = {str(plan["plan_id"]): plan for plan in monitor_plans}
            recent_fills = tuple(application.store.list_fills())[-100:]
            fill_by_signal = {str(fill.get("signal_id") or ""): fill for fill in recent_fills}
            recent_effective_events = tuple(
                _monitor_event_projection(event, plans=plan_by_id, fills=fill_by_signal)
                for event in application.store.list_monitor_events(effective_only=True)[-100:]
            )
            a1_registry = getattr(application, "a1_registry", None)
            status_now = datetime.now(ZoneInfo(settings.timezone))
            active_a1 = None
            a1_reason_code = None
            try:
                active_a1 = a1_registry.get_active_generation() if a1_registry is not None else None
                if a1_registry is not None and active_a1 is not None:
                    a1_registry.require_active(as_of=status_now, max_age=DEFAULT_A1_MAX_AGE)
            except A1RegistryError as exc:
                a1_reason_code = exc.reason_code
            a1_age_seconds = (
                max(0, int((status_now - active_a1.as_of).total_seconds()))
                if active_a1 is not None
                else None
            )
            a1_ready = active_a1 is not None and active_a1.is_sealed and a1_reason_code is None
            raw_a1_delta = (
                active_a1.manifest.get("delta")
                if active_a1 is not None and isinstance(active_a1.manifest.get("delta"), Mapping)
                else {}
            )
            safe_a1_delta = {
                key: raw_a1_delta.get(key)
                for key in (
                    "processed_count",
                    "added_count",
                    "changed_count",
                    "theme_affected_count",
                    "removed_count",
                    "unchanged_count",
                    "macro_revalidation_count",
                    "global_input_changed",
                )
                if key in raw_a1_delta
            }
            a1_generation = (
                {
                    "status": "ACTIVE" if a1_ready else "BLOCKED",
                    "generation_id": active_a1.generation_id,
                    "mode": active_a1.mode,
                    "as_of": active_a1.as_of.isoformat(),
                    "activated_at": active_a1.activated_at.isoformat() if active_a1.activated_at else None,
                    "age_seconds": a1_age_seconds,
                    "degraded": bool(
                        a1_age_seconds is not None
                        and a1_age_seconds > int(DEFAULT_A1_DEGRADED_AFTER.total_seconds())
                    ),
                    "reason_code": a1_reason_code,
                    "snapshot_id": active_a1.snapshot_id,
                    "base_generation_id": active_a1.base_generation_id,
                    "delta": safe_a1_delta,
                }
                if active_a1 is not None
                else {
                    "status": "MISSING" if a1_reason_code is None else "BLOCKED",
                    "generation_id": None,
                    "reason_code": a1_reason_code or "A1_ACTIVE_MISSING",
                }
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
                "monitor_plans": monitor_plans,
                "recent_effective_events": recent_effective_events,
                "recent_notifications": application.store.list_notification_deliveries(limit=50),
                "recent_fills": recent_fills,
                "latest_workflow_runs": workflow_runs,
                "scheduler_leases": application.store.list_leases(),
                "a1_generation": a1_generation,
                "configuration_ready": configuration_ready,
                "latest_workflow_acceptance": workflow_acceptance,
                "latest_workflow_outcome_v2": workflow_outcome.as_dict(),
                "deployment_ready": bool(
                    configuration_ready
                    and primary_workflow_publishable
                    and a1_ready
                ),
                "deployment_blockers": [
                    reason
                    for blocked, reason in (
                        (not application.store.healthy, "STATE_DB_UNHEALTHY"),
                        (settings.hithink_api_key is None, "HITHINK_API_KEY_MISSING"),
                        (settings.model_api_key is None, "MODEL_API_KEY_MISSING"),
                        (not settings.exchange_rules_path.is_file(), "EXCHANGE_RULE_SNAPSHOT_MISSING"),
                        (not a1_ready, "A1_ACTIVE_MISSING"),
                        (
                            not primary_workflow_publishable,
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
    if args.command in {"run-research", "run-next-session-prep"}:
        outcome = payload.get("outcome_v2") if isinstance(payload, Mapping) else None
        return cli_exit_code(outcome if isinstance(outcome, Mapping) else payload)
    if args.command in {"run-due", "run-premarket", "run-morning", "run-close", "run-monitor"}:
        dispatch = payload.get("dispatch", []) if isinstance(payload, dict) else []
        if any(
            isinstance(record, dict) and record.get("status") in {"FAILED", "MISSED"}
            for record in dispatch
        ):
            return 2
    if args.command == "activate-latest-a3-for-a4" and isinstance(payload, Mapping) and payload.get("status") == "BLOCKED":
        return 2
    if args.command == "run-comparison" and isinstance(payload, Mapping) and payload.get("status") == "FAILED":
        return 2
    return 0
