"""Post-auction research, isolated from activation and minute execution."""

from __future__ import annotations

from datetime import datetime, time
import math
from typing import Any
from zoneinfo import ZoneInfo

from ..data.rotation_theme import _default_tencent_quote_batch_fetch
from ..reporting import atomic_write_json

SHANGHAI = ZoneInfo("Asia/Shanghai")


def collect_fresh_quotes(symbols, *, as_of: datetime, fetch=None, clock=None) -> dict[str, Any]:
    from ..workflow import WorkflowError

    requested = sorted(set(symbols))
    if not requested:
        raise WorkflowError("AUCTION_QUOTE_SCOPE_EMPTY")
    quotes = (fetch or _default_tencent_quote_batch_fetch)(requested)
    captured = (clock or (lambda: datetime.now(SHANGHAI)))()
    accepted = {}
    for symbol in requested:
        quote = quotes.get(symbol, {})
        stamp = quote.get("quote_time")
        try:
            stamp = datetime.fromisoformat(stamp) if isinstance(stamp, str) else stamp
            if stamp.tzinfo is None:
                continue
            stamp = stamp.astimezone(SHANGHAI)
            age = (captured - stamp).total_seconds()
            price = float(quote.get("latest_price") or 0)
            if (stamp.date() != as_of.date() or stamp.time().replace(tzinfo=None) < time(9, 25)
                    or not 0 <= age <= 180 or not math.isfinite(price) or price <= 0):
                continue
        except (AttributeError, TypeError, ValueError):
            continue
        accepted[symbol] = {**quote, "quote_time": stamp.isoformat(),
                            "trade_date": stamp.date().isoformat()}
    coverage = len(accepted) / len(requested)
    if coverage < .95:
        raise WorkflowError("AUCTION_QUOTE_COVERAGE_INCOMPLETE")
    return {
        "available": True, "source_id": "TENCENT:qt.gtimg.cn",
        "observation_kind": "POST_AUCTION_QUOTES", "as_of": captured.isoformat(),
        "trade_date": as_of.date().isoformat(), "by_symbol": accepted,
        "coverage": coverage, "missing_symbols": sorted(set(requested) - set(accepted)),
        "closed_daily_bar_substitution_forbidden": True,
        "auction_final_price_claimed": False,
    }


def run_auction_refresh(app, *, now=None):
    """One durable research attempt per trading day; never mutate A4 plans.

    This can finish after the open. Source timestamps are retained and the
    resulting batch explicitly has no execution publication. Existing A4
    continues through its independent 09:26 quote review and minute worker.
    """
    from ..workflow import WorkflowError

    current = now or datetime.now(SHANGHAI)
    current = current.astimezone(SHANGHAI)
    if not app.trading_calendar.is_trading_day(current.date()):
        return {"status": "NOOP", "reason_code": "NON_TRADING_DAY"}
    if not time(9, 26) <= current.time().replace(tzinfo=None) < time(9, 30):
        raise WorkflowError("AUCTION_REFRESH_START_WINDOW_MISSED")
    key = f"auction-refresh:{current.date()}"
    lease, owner = "scheduler:auction-refresh", "auction-refresh"
    if not app.store.acquire_lease(lease, owner, now=current, ttl_seconds=2400, dispatch_key=key):
        return {"status": "NOOP", "reason_code": "AUCTION_REFRESH_ALREADY_DISPATCHED"}
    path = app.settings.workflow_output_dir / "runs" / f"{current.date()}-auction-refresh.json"
    receipt = {"started_at": current.isoformat(), "a1_reused": True,
               "execution_publication": "UNCHANGED", "status": "RUNNING"}
    atomic_write_json(path, receipt)
    try:
        result = app.run_research(
            "morning", as_of=current, primary_only=True, from_active_a1=True,
            publish_plans=False, schedule_comparison=False, reuse_resume_snapshot=False,
            auction_refresh=True,
            run_id_override=f"{current.date()}-auction-refresh-{current.strftime('%H%M%S')}",
        )
        receipt.update(status=result.get("status", "BLOCKED"), run_id=result.get("run_id"),
                       finished_at=datetime.now(SHANGHAI).isoformat(),
                       snapshot=result.get("snapshot"), plan_publication=result.get("plan_publication"))
        atomic_write_json(path, receipt)
        if receipt["status"] not in {"READY", "READY_DEGRADED"}:
            raise WorkflowError("AUCTION_REFRESH_RESEARCH_NOT_READY")
        app.store.complete_lease(lease, owner, dispatch_key=key, now=datetime.now(SHANGHAI))
        return receipt
    except Exception as exc:
        receipt.update(status="BLOCKED", reason_code=getattr(exc, "reason_code", "AUCTION_REFRESH_FAILED"),
                       finished_at=datetime.now(SHANGHAI).isoformat())
        atomic_write_json(path, receipt)
        app.store.release_lease(lease, owner)
        raise
