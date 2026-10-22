"""Prepare closed pre-session history without changing A3 eligibility."""
import json
import hashlib
from datetime import datetime, timedelta
from collections.abc import Mapping


def prepare_pattern_history(plans, *, now, calendar, minute_store, provider):
    """Native closed previous-session 48 bars; all-or-none before strict insert.

    A full previous session also supplies ATR20/V6 for the opening attempt
    prefix. Later current-day bars must never substitute for that prefix.
    """
    results=[];memo={}
    for plan in plans:
        target=datetime.fromisoformat(str(plan['expires_at'])).date()
        prior=calendar.previous_trading_day(target);symbol=str(plan['symbol']);key=(symbol,prior)
        if key not in memo:
            cutoff=now.replace(year=prior.year,month=prior.month,day=prior.day,hour=15,minute=0,second=0,microsecond=0)
            expected=set()
            for hour in (9,13):
                start=cutoff.replace(hour=hour,minute=30 if hour==9 else 0)
                expected.update(start+timedelta(minutes=5*n) for n in range(1,25))
            row=dict(symbol=symbol,status='DATA_LIMITED',required_5m_count=48,
                source_trade_date=prior.isoformat(),history_cutoff=cutoff.isoformat(),requests=0)
            present=set();selected=[]
            def native(bar):
                return (bar.symbol==symbol and bar.interval=='5m' and bar.bar_end in expected
                    and getattr(bar,'evidence_kind',None)=='MARKET_BAR'
                    and not any(x in str(bar.source_id).lower() for x in ('synthetic','aggregate','derived',':risk_only')))
            try:
                if cutoff>now:raise ValueError('PATTERN_HISTORY_CUTOFF_NOT_CLOSED')
                cached=minute_store.load_latest(symbol,'5m',limit=96,before=cutoff+timedelta(seconds=1))
                selected=[b for b in cached if native(b)]
                present={b.bar_end for b in selected}
                if not expected<=present:
                    row['requests']=1
                    fetched=provider.fetch_bars(symbol,'5m',48,as_of=cutoff)
                    bars=list(fetched.get('bars',()) if isinstance(fetched,Mapping) else getattr(fetched,'bars',()))
                    complete=fetched.get('complete') if isinstance(fetched,Mapping) else getattr(fetched,'complete',False)
                    valid=(complete is True and len(bars)==48 and len({b.bar_end for b in bars})==48
                           and all(native(b) for b in bars))
                    if not valid:raise ValueError('PATTERN_HISTORY_48_INCOMPLETE')
                    # Strict write rolls back conflicts; no partial insert or
                    # revision of an existing frozen observation is permitted.
                    minute_store.write(bars)
                    present|={b.bar_end for b in bars}
                    selected=bars
                evidence=[b.model_dump(mode='json') for b in sorted(selected,key=lambda b:b.bar_end)]
                row.update(status='READY',reason_code='OK',available_5m_count=48,opening_prefix_ready=True,
                    native_rowset_sha256=hashlib.sha256(json.dumps(evidence,sort_keys=True,separators=(',',':')).encode()).hexdigest(),
                    source_ids=sorted({b.source_id for b in selected}))
            except Exception as exc:
                row.update(reason_code=str(exc) if str(exc).startswith('PATTERN_HISTORY_') else 'PATTERN_HISTORY_PREPARATION_FAILED',
                    available_5m_count=len(expected & present),opening_prefix_ready=False)
            memo[key]=row
        results.append(dict(memo[key],plan_id=plan['plan_id'],target_trade_date=target.isoformat()))
    return dict(schema_version='entry-pattern-history/1',plans=results,
        status='NOT_REQUESTED' if not results else 'READY' if all(r['status']=='READY' for r in results) else 'DATA_LIMITED')


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
                fetched = provider.fetch_bars(symbol, "5m", len(expected), as_of=cutoff)
                row["fetch_reason_code"] = getattr(fetched, "reason_code", None)
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
