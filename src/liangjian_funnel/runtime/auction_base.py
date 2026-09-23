"""Separate slow, immutable morning evidence preparation from auction deltas.

No plan publication or model call occurs here. Cached source dates are retained;
the marker is an index, never a substitute for snapshot/hash validation.
"""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime, time, timedelta
import json
import signal
import threading
from zoneinfo import ZoneInfo

from ..reporting import atomic_write_json, atomic_write_json_streaming

TZ = ZoneInfo("Asia/Shanghai")


def marker_path(app, day):
    return app.settings.workflow_output_dir / "auction_base" / f"{day}.json"


def run_auction_base(app, *, now=None):
    from ..workflow import WorkflowError, _A1_MAX_AGE, _active_a1_downstream_scope

    current = (now or datetime.now(TZ)).astimezone(TZ)
    if not app.trading_calendar.is_trading_day(current.date()):
        return {"status": "NOOP", "reason_code": "NON_TRADING_DAY"}
    if not time(7) <= current.time().replace(tzinfo=None) < time(7, 15):
        raise WorkflowError("AUCTION_BASE_START_WINDOW_MISSED")
    lease, owner, key = "scheduler:auction-base", "auction-base", f"auction-base:{current.date()}"
    if not app.store.acquire_lease(lease, owner, now=current, ttl_seconds=3900, dispatch_key=key):
        return {"status": "NOOP", "reason_code": "AUCTION_BASE_ALREADY_DISPATCHED"}
    path = marker_path(app, current.date())
    receipt = {"schema_version": "auction-base/1", "status": "RUNNING", "started_at": current.isoformat(),
               "execution_publication": "UNCHANGED", "model_calls": 0}
    atomic_write_json(path, receipt)
    def record_failure(reason):
        receipt.update(status="BLOCKED", reason_code=reason, finished_at=datetime.now(TZ).isoformat())
        atomic_write_json(path, receipt)
        app.store.release_lease(lease, owner)

    def terminated(signum, frame):
        record_failure("AUCTION_BASE_PROCESS_TERMINATED")
        raise WorkflowError("AUCTION_BASE_PROCESS_TERMINATED")

    previous_handler = None
    if threading.current_thread() is threading.main_thread():
        previous_handler = signal.signal(signal.SIGTERM, terminated)
    try:
        generation = app.a1_registry.require_active(as_of=current, max_age=_A1_MAX_AGE)
        scope = _active_a1_downstream_scope(generation.payload)
        if not scope:
            raise WorkflowError("A1_ACTIVE_DOWNSTREAM_SCOPE_EMPTY")
        prepared = app.prepare_snapshot(as_of=current, candidate_symbols=scope,
                                        materialize_feature_source=False)
        finished = datetime.now(TZ)
        if finished.date() != current.date() or finished.time().replace(tzinfo=None) >= time(8, 15):
            raise WorkflowError("AUCTION_BASE_FINISH_DEADLINE_EXCEEDED")
        receipt.update(status="READY", finished_at=finished.isoformat(),
                       a1_generation_id=generation.generation_id, scope_symbols=list(scope),
                       prepared_snapshot=prepared.as_dict())
        atomic_write_json(path, receipt)
        app.store.complete_lease(lease, owner, dispatch_key=key, now=finished)
        return receipt
    except BaseException as exc:
        record_failure(getattr(exc, "reason_code", "AUCTION_BASE_FAILED"))
        raise
    finally:
        if previous_handler is not None:
            signal.signal(signal.SIGTERM, previous_handler)


def load_auction_base(app, *, current, generation, scope):
    from ..workflow import WorkflowError

    try:
        marker = json.loads(marker_path(app, current.date()).read_text(encoding="utf-8"))
        if marker.get("schema_version") != "auction-base/1" or marker.get("status") != "READY":
            raise ValueError()
        started = datetime.fromisoformat(marker["started_at"])
        finished = datetime.fromisoformat(marker["finished_at"])
        if (started.tzinfo is None or finished.tzinfo is None or started.date() != current.date()
                or not timedelta(0) <= current - started <= timedelta(hours=3)
                or not started <= finished <= current):
            raise ValueError()
        if marker["a1_generation_id"] != generation.generation_id or set(marker["scope_symbols"]) != set(scope):
            raise WorkflowError("AUCTION_BASE_A1_GENERATION_MISMATCH")
        raw = marker["prepared_snapshot"]
        prepared = app._load_research_snapshot_by_id(raw["snapshot_id"], expected_date=current.date().isoformat())
        if prepared.snapshot.snapshot_hash != raw["snapshot_hash"]:
            raise WorkflowError("AUCTION_BASE_HASH_MISMATCH")
        if not started <= prepared.snapshot.as_of <= finished:
            raise ValueError()
        if not set(scope) <= set(prepared.snapshot.data.get("g0_symbols", [])):
            raise WorkflowError("AUCTION_BASE_A1_COVERAGE_INCOMPLETE")
        return prepared
    except WorkflowError:
        raise
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
        raise WorkflowError("AUCTION_BASE_NOT_READY_OR_STALE") from exc


def project_auction_delta(base, *, hot, boards, quotes, observed_at):
    """Change only observed market fields; never redate company/daily evidence."""
    from ..workflow import WorkflowError

    day = observed_at.date().isoformat()
    rows = hot.get("records") or []
    if (hot.get("available") is not True or hot.get("trade_date") != day or hot.get("record_count") != 100
            or len(rows) != 100 or len({r.get("symbol") for r in rows}) != 100
            or {r.get("rank") for r in rows} != set(range(1, 101))):
        raise WorkflowError("AUCTION_HOT100_UNAVAILABLE")
    if boards.get("available") is not True or boards.get("trade_date") != day:
        raise WorkflowError("AUCTION_CURRENT_ROTATION_UNAVAILABLE")
    if quotes.get("available") is not True or quotes.get("trade_date") != day:
        raise WorkflowError("AUCTION_QUOTE_COVERAGE_INCOMPLETE")
    data = dict(base.snapshot.data)
    scope = set(data.get("g0_symbols", []))
    new_hot = sorted({row["symbol"] for row in hot.get("records", [])} - scope)
    context = {
        "schema_version": "auction-delta/1", "observed_at": observed_at.isoformat(),
        "base_snapshot_id": base.snapshot.snapshot_id, "base_snapshot_hash": base.snapshot.snapshot_hash,
        "base_evidence_as_of": base.snapshot.as_of.isoformat(),
        "daily_market_data_as_of": data.get("MARKET_DATA_AS_OF"),
        "refreshed_fields": ["EASTMONEY_HOT100_SNAPSHOT", "SELECTED_BOARD_SNAPSHOT", "AUCTION_SNAPSHOT"],
        "company_and_macro_evidence_reused_without_redating": True,
        "post_base_disclosures_not_requeried": True,
        "new_hot_symbols_deferred_to_full_research": new_hot,
        "execution_publication": "UNCHANGED",
    }
    data.update(EASTMONEY_HOT100_SNAPSHOT=hot, SELECTED_BOARD_SNAPSHOT=boards,
                AUCTION_SNAPSHOT={**quotes, "research_evidence_context": context},
                AUCTION_REFRESH_CONTEXT=context)
    data["MARKET_CONTEXT"] = {**data.get("MARKET_CONTEXT", {}), "auction_refresh": context}
    data["snapshot_manifest"] = {**data.get("snapshot_manifest", {}),
                                "as_of": observed_at.isoformat(), "auction_refresh": context}
    # Raw facts, their hash/path, daily feature dates and risk-event evidence
    # remain those of the verified base. Consumers can distinguish the delta.
    return data


def prepare_auction_delta(app, *, current, generation, scope):
    from ..workflow import (WorkflowError, ResearchSnapshot, _hash_json, load_yaml,
                            collect_eastmoney_hot100, collect_rotation_theme_snapshot)
    from .auction_refresh import collect_fresh_quotes

    base = load_auction_base(app, current=current, generation=generation, scope=scope)
    settings = app.settings
    hot = collect_eastmoney_hot100(as_of=current, expected_trade_date=current.date(),
                                  cache_dir=settings.fact_store_dir / "eastmoney_hot100" / "auction",
                                  force_refresh=True)
    if not hot.get("available"):
        raise WorkflowError("AUCTION_HOT100_UNAVAILABLE")
    config = load_yaml(settings.source_config_path)
    a2 = config.get("agent_2", {})
    boards = collect_rotation_theme_snapshot(
        as_of=current, expected_trade_date=current.date(),
        registry_path=settings.rotation_theme_registry_path,
        snapshot_dir=settings.fact_store_dir / "rotation_theme",
        rotation_theme_count=int(a2.get("rotation_theme_count", 5)),
        membership_refresh_days=settings.rotation_membership_refresh_days,
        warn_age_days=settings.rotation_membership_warn_age_days,
        max_age_days=settings.rotation_membership_max_age_days,
        fund_coverage_minimum=settings.rotation_fund_coverage_minimum,
        price_coverage_minimum=settings.rotation_price_coverage_minimum,
        workers=settings.rotation_collection_workers)
    # Capture quotes last; a slow board endpoint must not age an earlier quote
    # before it is handed to research. Source collectors retain actual clocks.
    quotes = collect_fresh_quotes(scope, as_of=current)
    observed = datetime.now(TZ)
    if (observed - current).total_seconds() > 180:
        raise WorkflowError("AUCTION_DELTA_CAPTURE_DEADLINE_EXCEEDED")
    data = project_auction_delta(base, hot=hot, boards=boards, quotes=quotes, observed_at=observed)
    hashed = _hash_json(data)
    identity = f"snapshot-{observed.strftime('%Y%m%dT%H%M%S%z')}-{hashed[:12]}"
    snapshot = ResearchSnapshot(snapshot_id=identity, snapshot_hash=hashed, as_of=observed, data=data)
    path = settings.snapshot_dir / f"{identity}.json"
    atomic_write_json_streaming(path, {"snapshot_id": identity, "snapshot_hash": hashed,
                                      "as_of": observed.isoformat(), "data": data})
    return replace(base, snapshot=snapshot, path=path, feature_source=None)
