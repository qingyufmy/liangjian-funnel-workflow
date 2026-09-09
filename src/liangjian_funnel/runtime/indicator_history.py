"""Prepare closed pre-session history without changing A3 eligibility."""
import json
from datetime import datetime, timedelta
from collections.abc import Mapping


def prepare_520_history(plans, *, now, calendar, minute_store, provider):
    results = []
    for plan in plans:
        payload = plan.get("payload") or plan.get("payload_json") or {}
        if isinstance(payload, str):
            payload = json.loads(payload)
        if payload.get("strategy_profile") != "MA520_SWING":
            continue
        symbol = str(plan["symbol"])
        target = datetime.fromisoformat(str(plan["expires_at"])).date()
        prior = calendar.previous_trading_day(target)
        cutoff = now.replace(year=prior.year, month=prior.month, day=prior.day, hour=15, minute=0, second=0, microsecond=0)
        row = {"plan_id": plan["plan_id"], "symbol": symbol, "target_trade_date": target.isoformat(),
               "history_cutoff": cutoff.isoformat(), "status": "DATA_LIMITED", "required_5m_count": 144}
        if cutoff > now:
            row["reason_code"] = "HISTORY_CUTOFF_NOT_CLOSED"
            results.append(row)
            continue
        expected = set()
        day = prior
        for _ in range(3):
            base = cutoff.replace(year=day.year, month=day.month, day=day.day)
            for hour in (9, 13):
                start = base.replace(hour=hour, minute=30 if hour == 9 else 0)
                expected.update(start + timedelta(minutes=5 * n) for n in range(1, 25))
            day = calendar.previous_trading_day(day)
        try:
            bars = minute_store.load_latest(symbol, "5m", limit=360, before=cutoff + timedelta(seconds=1))
            missing = expected - {bar.bar_end for bar in bars}
            if missing:
                fetched = provider.fetch_bars(symbol, "5m", 360, as_of=cutoff)
                fetched_bars = getattr(fetched, "bars", ()) if not isinstance(fetched, Mapping) else fetched.get("bars", ())
                valid = [bar for bar in fetched_bars if bar.symbol == symbol and bar.interval == "5m" and bar.bar_end in expected]
                if valid:
                    minute_store.write_live(valid, as_of=now, snapshot_id=f"indicator-preparation:{plan['plan_id']}")
                bars = minute_store.load_latest(symbol, "5m", limit=360, before=cutoff + timedelta(seconds=1))
                missing = expected - {bar.bar_end for bar in bars}
            row.update(status="READY" if not missing else "DATA_LIMITED",
                       reason_code="OK" if not missing else "M15_HISTORY_INCOMPLETE",
                       available_5m_count=len(expected) - len(missing),
                       missing_bar_ends=[value.isoformat() for value in sorted(missing)])
        except Exception as exc:
            row["reason_code"] = "HISTORY_PREPARATION_FAILED"
            row["error_type"] = type(exc).__name__
        results.append(row)
    return {"schema_version": "a4-indicator-preparation/1", "plans": results,
            "status": "READY" if all(row["status"] == "READY" for row in results) else "DATA_LIMITED"}
