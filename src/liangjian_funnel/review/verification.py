"""Independent, read-only verification facts for the daily A5 reviewer.

This module deliberately does not import A2/A3 ranking code.  It recomputes a
small set of auditable invariants from raw daily/minute bars and compares the
production A4 decision trace with an alternate market-data family.  A mismatch
is review evidence only; it never changes a candidate, plan or signal.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, time, timedelta
from statistics import median
from typing import Any
from pathlib import Path
from zoneinfo import ZoneInfo
from ..runtime.calendar import ExchangeTradingCalendar
from .indicator_evidence import audit_event_indicators, daily_macd_check
from .evidence_archive import archive_observation


SHANGHAI = ZoneInfo("Asia/Shanghai")
_ACTIONABLE = frozenset({"BUY_SIGNAL", "ADD_SIGNAL", "SELL_SIGNAL", "REDUCE_SIGNAL", "FORCED_RISK_EXIT"})
_LEGITIMATE_SUPPRESSIONS = frozenset({
    "SIGNAL_ALREADY_EMITTED", "DUPLICATE_EFFECTIVE_STATE", "POSITION_ALREADY_OPEN",
    "ADD_WITHOUT_POSITION", "EXIT_WITHOUT_POSITION", "BUY_TIME_CUTOFF", "LLM_VETO",
    "ENTRY_NO_NEXT_TRADING_MINUTE", "ENTRY_NEXT_MINUTE_AFTER_PLAN_EXPIRY",
})


def counterexample_drop_stage(symbol: str, pool: str, plan_symbols: Sequence[str], technical_candidates: Mapping[str, Any]) -> str:
    # Actual downstream evidence outranks an A2 display-pool label.
    if symbol in plan_symbols:
        return "A4_NO_EFFECTIVE_SIGNAL"
    if symbol in technical_candidates:
        return "A3_NOT_PLANNED"
    if pool in {"A1_MONITOR", "A1_REJECTED"}:
        return "A1_NOT_ACTIVE"
    if pool == "A1_ACTIVE":
        return "A2_NOT_EVALUATED"
    if pool == "REJECTED":
        return "A2_REJECTED"
    if pool in {"OUTSIDE_ROTATION", "CROWDED", "LOW_IDENTITY"}:
        return "A2_QUANT_FILTERED"
    return "UNRESOLVED_A3_LINEAGE_MISSING"


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _bar_value(row: Mapping[str, Any], key: str) -> float | None:
    payload = _mapping(row.get("payload"))
    for source in (payload, row):
        for name in (key, f"{key}_value", f"{key}_price"):
            if (value := _number(source.get(name))) is not None:
                return value
    return None


def _bar_time(row: Mapping[str, Any]) -> datetime | None:
    for key in ("bar_end", "bar_timestamp", "timestamp", "time"):
        value = row.get(key)
        if value is None:
            continue
        try:
            parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            continue
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            parsed = parsed.replace(tzinfo=SHANGHAI)
        return parsed.astimezone(SHANGHAI)
    return None


def _bar_dict(bar: Any) -> dict[str, Any]:
    if hasattr(bar, "model_dump"):
        value = bar.model_dump(mode="json")
        return dict(value) if isinstance(value, Mapping) else {}
    return dict(bar) if isinstance(bar, Mapping) else {}


def _digest(rows: Sequence[Mapping[str, Any]]) -> str:
    compact = [
        [str(row.get("bar_end") or row.get("bar_timestamp") or ""), *[_bar_value(row, key) for key in ("open", "high", "low", "close", "volume")]]
        for row in rows
    ]
    return hashlib.sha256(json.dumps(compact, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def _mean(values: Sequence[float]) -> float | None:
    return round(sum(values) / len(values), 8) if values else None


def _moving_average(values: Sequence[float], length: int) -> float | None:
    return _mean(values[-length:]) if len(values) >= length else None


def _relative_difference(left: float | None, right: float | None) -> float | None:
    if left is None or right is None or abs(right) < 1e-12:
        return None
    return abs(left - right) / abs(right)


def _expected_minutes(start: datetime, cutoff: datetime) -> int:
    day = cutoff.date()
    windows = ((time(9, 31), time(11, 30)), (time(13, 1), time(15, 0)))
    count = 0
    for begin_clock, end_clock in windows:
        begin = datetime.combine(day, begin_clock, tzinfo=SHANGHAI)
        end = min(datetime.combine(day, end_clock, tzinfo=SHANGHAI), cutoff)
        begin = max(begin, start)
        if end >= begin:
            count += int((end - begin).total_seconds() // 60) + 1
    return count


def _field_comparison(left: Mapping[str, Mapping[str, Any]], right: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    result = {}
    for field in ("open", "high", "low", "close", "volume", "amount"):
        compared = 0
        missing = 0
        mismatches = []
        for stamp in sorted(set(left) & set(right)):
            a, b = left[stamp], right[stamp]
            comparable = (field not in {"volume", "amount"}
                          or (field == "volume" and a.get("volume_unit") == b.get("volume_unit") == "shares")
                          or (field == "amount" and a.get("amount_kind") == b.get("amount_kind") == "reported"))
            x, y = _bar_value(a, field), _bar_value(b, field)
            if not comparable or x is None or y is None:
                missing += 1
                continue
            compared += 1
            # Price tolerance keeps the existing audit contract. Volumes use
            # one-share rounding only; source disagreements remain visible.
            tolerance = max(0.01, abs(y) * 0.005) if field in {"open", "high", "low", "close"} else 1.0
            if abs(x - y) > tolerance + 1e-9:
                mismatches.append({"bar_end": stamp, "left": x, "right": y})
        result[field.upper()] = {"status": "MISMATCH" if mismatches else "MATCH" if compared and not missing else "DATA_LIMITED",
                                "compared_count": compared, "not_comparable_count": missing,
                                "mismatch_count": len(mismatches), "mismatch_samples": mismatches[:5]}
    return result


class A5IndependentVerifier:
    """Build independent A2/A3/A4 acceptance evidence without mutating runtime."""

    def __init__(self, *, daily_cache: Any, minute_store: Any, tencent: Any, mootdx: Any, workers: int = 12, quote_fetch: Any = None, evidence_dir: Path | None = None):
        self.daily_cache = daily_cache
        self.minute_store = minute_store
        self.tencent = tencent
        self.mootdx = mootdx
        self.workers = max(1, min(int(workers), 24))
        self.quote_fetch = quote_fetch
        self.evidence_dir = evidence_dir

    def verify(
        self,
        *,
        a2: Mapping[str, Any],
        market_universe: Sequence[Mapping[str, Any]] = (),
        plan_rows: Sequence[Mapping[str, Any]],
        event_rows: Sequence[Mapping[str, Any]],
        cutoff_at: datetime,
    ) -> dict[str, Any]:
        cutoff = cutoff_at.astimezone(SHANGHAI)
        candidates = [row for row in a2.get("candidates", ()) if isinstance(row, Mapping)]
        price_candidates = [row for row in candidates if row.get("llm_reviewed") is not False]
        plan_symbols = tuple(dict.fromkeys(str(row.get("symbol") or "") for row in plan_rows if str(row.get("symbol") or "")))
        market_cross_section = self._daily_market_cross_section(market_universe, cutoff)
        expected_symbols = {str(row.get("symbol") or "") for row in market_universe} - {""}
        missing = expected_symbols - {str(row["symbol"]) for row in market_cross_section}
        # Recover only absent end-of-day prices. A prior close must come from
        # the previous trading session, never from today's opening minute.
        recovered_tencent = self._fetch_many(self.tencent, tuple(sorted(missing)), "1m", 300, cutoff) if cutoff.time() >= time(15) else {}
        recovery = self._recover_cross_section(market_universe, missing, recovered_tencent, cutoff)
        if self.quote_fetch is not None and cutoff.time() >= time(15):
            metadata = {str(r.get("symbol")): r for r in market_universe}
            for symbol in sorted(missing - {r["symbol"] for r in recovery}):
                try:
                    quote = self.quote_fetch(symbol)
                    price, baseline = _number(quote.get("latest_price")), _number(quote.get("previous_close"))
                    stamp = quote.get("quote_time")
                    if isinstance(stamp,str): stamp = datetime.fromisoformat(stamp)
                    if (quote.get("symbol") == symbol and quote.get("no_reported_trades") is True
                            and quote.get("turnover_cny") == 0 and price is not None and price > 0
                            and price == baseline and isinstance(stamp,datetime) and stamp.tzinfo
                            and stamp.date() == cutoff.date() and stamp.time() >= time(15)):
                        recovery.append({**metadata.get(symbol,{}), "symbol":symbol,"return":0.0,
                            "return_basis":"NO_TRADE_QUOTE_REFERENCE_MARK", "ranking_eligible":False,
                            "trading_activity":"NO_REPORTED_TRADES", "close":price,"previous_close":baseline,
                            "price_at":stamp.isoformat(),"source_ids":[quote.get("source_id")],
                            "verification_fetched_at":datetime.now(SHANGHAI).isoformat(),
                            "evidence_scope":"POST_HOC_QUOTE_NOT_A_TRADE_OR_A_SUSPENSION_RULING"})
                except Exception:
                    continue
        market_cross_section.extend(recovery)
        market_cross_section.sort(key=lambda row: (-float(row["return"]), str(row["symbol"])))
        # The deterministic top percentile is a discovery index, not a funnel
        # capacity.  It avoids thousands of duplicate public minute requests;
        # every A1-universe member is still ranked before the alternate-source
        # confirmation set is chosen.
        confirmation_count = max(20, math.ceil(len(market_cross_section) * 0.01)) if market_cross_section else 0
        confirmation_symbols = tuple(str(row["symbol"]) for row in market_cross_section[:confirmation_count])
        symbols = tuple(dict.fromkeys([
            *(str(row.get("symbol") or "") for row in price_candidates if str(row.get("symbol") or "")),
            *plan_symbols,
            *confirmation_symbols,
        ]))

        # One complete A-share session has 240 one-minute closes.  Request a
        # little more so the independent A2 return can use the previous close
        # instead of silently dropping the overnight gap.
        tencent_rows = {**recovered_tencent, **self._fetch_many(self.tencent, tuple(s for s in symbols if s not in recovered_tencent), "1m", 300, cutoff)}
        tdx_1m = self._fetch_many(self.mootdx, plan_symbols, "1m", 240, cutoff)
        tdx_5m = self._fetch_many(self.mootdx, plan_symbols, "5m", 320, cutoff)
        local_1m = self._load_local_minutes(plan_symbols, cutoff)
        daily = self._daily_windows(plan_symbols, cutoff)

        a2_check = self._verify_a2(
            price_candidates, a2, plan_rows, event_rows, tencent_rows, cutoff,
            market_cross_section=market_cross_section,
            confirmation_symbols=set(confirmation_symbols),
        )
        expected_symbols = {str(row.get("symbol") or "") for row in market_universe} - {""}
        covered_symbols = {str(row.get("symbol") or "") for row in market_cross_section}
        a2_check["market_universe_expected_count"] = len(expected_symbols)
        a2_check["market_cross_section_missing_symbols"] = sorted(expected_symbols - covered_symbols)
        a2_check["market_cross_section_coverage"] = (
            round(len(expected_symbols & covered_symbols) / len(expected_symbols), 6)
            if expected_symbols else None
        )
        a2_check["market_cross_section_recovery"] = recovery
        a2_check["market_cross_section_status"] = "READY" if expected_symbols and not (expected_symbols - covered_symbols) else "DATA_LIMITED"
        if expected_symbols - covered_symbols and cutoff.time() >= time(15) and a2_check.get("status") == "READY":
            a2_check["status"] = "DEGRADED"
        a3_check = self._verify_a3(plan_rows, daily, tdx_5m, cutoff)
        a4_check = self._verify_a4(plan_rows, event_rows, tencent_rows, tdx_1m, local_1m, cutoff)
        # Only signal stocks need minute paths for deterministic performance.
        # Keep frozen decision-family observations separate from alternate checks.
        signal_symbols = {str(_mapping(e.get("payload_json")).get("symbol") or "")
                          for e in event_rows if e.get("effective") and e.get("action") in _ACTIONABLE}
        signal_market = {}
        previous_session = ExchangeTradingCalendar().previous_trading_day(cutoff.date())
        for symbol in sorted(signal_symbols):
            selected = local_1m.get(symbol, {})
            previous = [b for b in daily.get(symbol, []) if _bar_time(b) and _bar_time(b).date() < cutoff.date()]
            previous.sort(key=lambda b: _bar_time(b))
            signal_market[symbol] = {
                "source": selected.get("archive_basis", "UNAVAILABLE"),
                "previous_close": _bar_value(previous[-1], "close") if previous and _bar_time(previous[-1]).date() == previous_session else None,
                "expected_minutes": _expected_minutes(cutoff.replace(hour=9, minute=30, second=0, microsecond=0), cutoff),
                "bars": [{"bar_end": _bar_time(b).isoformat(), **{k: _bar_value(b, k) for k in ("open", "high", "low", "close")}}
                         for b in selected.get("bars", []) if _bar_time(b)],
            }
        sections = (a2_check, a3_check, a4_check)
        status = "READY" if all(item.get("status") == "READY" for item in sections) else (
            "DEGRADED" if any(item.get("status") in {"READY", "DEGRADED"} for item in sections) else "UNAVAILABLE"
        )
        archives = {name:{symbol:row.get("input_snapshot", {"status":"NOT_ARCHIVED"}) for symbol,row in source.items()}
                    for name,source in (("tencent_1m",tencent_rows),("tdx_1m",tdx_1m),("tdx_5m",tdx_5m))}
        if self.evidence_dir and any(item.get("status") == "ARCHIVE_FAILED" for source in archives.values() for item in source.values()):
            status = "DEGRADED"
        return {
            "schema_version": "a5-independent-verification/1.0.0",
            "status": status,
            "cutoff_at": cutoff.isoformat(),
            "independence_contract": {
                "a2": "A2模型复核候选的腾讯分钟价格广度对照，不等同于全市场板块强度或资金排名；全量量化去向另行保留",
                "a3": "从本地原始日线独立复算均线，并用通达信五分钟收盘交叉核价",
                "a4": "检查每分钟决策落盘覆盖、动作传递，并用通达信分钟线对照腾讯/本地行情",
                "production_mutation": False,
            },
            "a2": a2_check,
            "a3": a3_check,
            "a4": a4_check,
            "signal_market": signal_market,
            "market_data_evidence_archives": archives,
            "counterexamples": a2_check.get("counterexamples", []),
        }

    def _fetch_many(self, provider: Any, symbols: Sequence[str], interval: str, required: int, cutoff: datetime) -> dict[str, dict[str, Any]]:
        if provider is None or not callable(getattr(provider, "fetch_bars", None)) or not symbols:
            return {}

        def fetch(symbol: str) -> tuple[str, dict[str, Any]]:
            try:
                result = provider.fetch_bars(symbol, interval, required, as_of=cutoff)
                bars = [_bar_dict(item) for item in getattr(result, "bars", ())]
                bars = [row for row in bars if (_bar_time(row) is not None and _bar_time(row) <= cutoff)]
                observation = {
                    "reason_code": str(getattr(result, "reason_code", "UNKNOWN")),
                    "source_ids": sorted({str(row.get("source_id") or "") for row in bars if str(row.get("source_id") or "")}),
                    "bars": bars,
                    "verification_fetched_at": datetime.now(SHANGHAI).isoformat(),
                }
                if self.evidence_dir is not None:
                    try:
                        observation["input_snapshot"] = archive_observation(self.evidence_dir,
                            {**observation,"symbol":symbol,"interval":interval,"requested_bars":required,"cutoff_at":cutoff.isoformat()})
                    except (OSError,ValueError):
                        observation["input_snapshot"] = {"status":"ARCHIVE_FAILED"}
                return symbol, observation
            except Exception:
                return symbol, {"reason_code": "A5_VERIFICATION_PROVIDER_FAILED", "source_ids": [], "bars": []}

        with ThreadPoolExecutor(max_workers=min(self.workers, len(symbols)), thread_name_prefix="a5-verify") as executor:
            return dict(executor.map(fetch, symbols))

    def _daily_windows(self, symbols: Sequence[str], cutoff: datetime) -> dict[str, list[dict[str, Any]]]:
        if not symbols or self.daily_cache is None:
            return {}
        try:
            session_start = cutoff.replace(hour=0, minute=0, second=0, microsecond=0)
            raw = self.daily_cache.latest_daily_bar_windows_before(
                symbols, end=session_start, per_symbol_limit=800, adjust="none", as_of=cutoff,
            )
            return {str(symbol): [dict(row) for row in rows] for symbol, rows in raw.items()}
        except Exception:
            return {}

    def _daily_market_cross_section(
        self,
        universe: Sequence[Mapping[str, Any]],
        cutoff: datetime,
    ) -> list[dict[str, Any]]:
        if cutoff.time() < time(15, 0) or self.daily_cache is None or not universe:
            return []
        metadata = {
            str(row.get("symbol") or ""): dict(row)
            for row in universe if str(row.get("symbol") or "")
        }
        try:
            next_day = cutoff.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
            windows = self.daily_cache.latest_daily_bar_windows_before(
                tuple(metadata), end=next_day, per_symbol_limit=2, adjust="none",
            )
        except Exception:
            return []
        result = []
        for symbol, rows in windows.items():
            ordered = sorted((dict(row) for row in rows), key=lambda row: _bar_time(row) or cutoff)
            if (len(ordered) < 2 or (_bar_time(ordered[-1]) or cutoff).date() != cutoff.date()
                    or (_bar_time(ordered[-2]) or cutoff).date() != ExchangeTradingCalendar().previous_trading_day(cutoff.date())):
                continue
            previous_close = _bar_value(ordered[-2], "close")
            current_close = _bar_value(ordered[-1], "close")
            if previous_close is None or current_close is None or previous_close <= 0:
                continue
            item = metadata.get(str(symbol), {})
            result.append({
                **item,
                "symbol": str(symbol),
                "return": current_close / previous_close - 1.0,
                "return_basis": "LOCAL_DAILY_PREVIOUS_CLOSE",
                "daily_content_hash": ordered[-1].get("content_hash"),
                "daily_fetched_at": ordered[-1].get("fetched_at"),
            })
        result.sort(key=lambda row: (-float(row["return"]), str(row["symbol"])))
        return result

    def _recover_cross_section(self, universe, missing, fetched, cutoff):
        if cutoff.time() < time(15) or not missing:
            return []
        previous = self._daily_windows(tuple(sorted(missing)), cutoff)
        prior_day = ExchangeTradingCalendar().previous_trading_day(cutoff.date())
        close_time = cutoff.replace(hour=15, minute=0, second=0, microsecond=0)
        metadata = {str(r.get("symbol")): r for r in universe}
        result = []
        for symbol in sorted(missing):
            selected = fetched.get(symbol, {})
            bars = selected.get("bars", [])
            current = [r for r in bars if _bar_time(r) == close_time]
            prior = sorted([r for r in bars if _bar_time(r) and _bar_time(r).date() == prior_day], key=_bar_time)
            basis = "TENCENT_PREVIOUS_SESSION_CLOSE"
            # A minute baseline is valid only at the previous session close.
            prior = [r for r in prior if _bar_time(r).time() == time(15)]
            if not prior:
                prior = sorted([r for r in previous.get(symbol, []) if _bar_time(r) and _bar_time(r).date() == prior_day], key=_bar_time)
                basis = "LOCAL_DAILY_PREVIOUS_CLOSE_TENCENT_CLOSE"
            base = _bar_value(prior[-1], "close") if prior else None
            price = _bar_value(current[-1], "close") if current else None
            if base is None or price is None or base <= 0 or price <= 0:
                continue
            result.append({**metadata.get(symbol, {}), "symbol": symbol, "return": price / base - 1,
                           "return_basis": basis, "previous_close": base, "close": price,
                           "price_at": close_time.isoformat(), "source_ids": selected.get("source_ids", []),
                           "verification_fetched_at": selected.get("verification_fetched_at"),
                           "evidence_scope": "POST_HOC_VERIFICATION_NOT_ORIGINAL_DECISION_INPUT",
                           "input_digest": _digest([prior[-1], current[-1]])})
        return result

    def _load_local_minutes(self, symbols: Sequence[str], cutoff: datetime) -> dict[str, dict[str, Any]]:
        if self.minute_store is None:
            return {}
        result: dict[str, dict[str, Any]] = {}
        for symbol in symbols:
            selection = {}
            try:
                latest_selection = getattr(self.minute_store, "latest_decision_snapshot", None)
                if callable(latest_selection):
                    selection = latest_selection(symbol, "1m", as_of=cutoff)
                rows = [_bar_dict(item) for item in (
                    selection["bars"] if selection else self.minute_store.load_latest(symbol, "1m", limit=300)
                )]
            except Exception:
                rows = []
            rows = [
                row for row in rows
                if (stamp := _bar_time(row)) is not None
                and stamp.date() == cutoff.date()
                and stamp <= cutoff
            ]
            result[symbol] = {
                "bars": rows,
                "selection": {key: value for key, value in selection.items() if key != "bars"},
                "archive_basis": "LATEST_DECISION_SNAPSHOT" if selection else "LEGACY_FIRST_OBSERVATION",
                "source_ids": sorted({str(row.get("source_id") or "") for row in rows if str(row.get("source_id") or "")}),
            }
        return result

    @staticmethod
    def _verify_a2(
        candidates: Sequence[Mapping[str, Any]],
        a2: Mapping[str, Any],
        plan_rows: Sequence[Mapping[str, Any]],
        event_rows: Sequence[Mapping[str, Any]],
        market: Mapping[str, Mapping[str, Any]],
        cutoff: datetime,
        *,
        market_cross_section: Sequence[Mapping[str, Any]],
        confirmation_symbols: set[str],
    ) -> dict[str, Any]:
        by_theme: dict[str, list[dict[str, Any]]] = defaultdict(list)
        stock_performance: list[dict[str, Any]] = []
        covered = 0
        for candidate in candidates:
            symbol = str(candidate.get("symbol") or "")
            theme = str(candidate.get("theme_id") or "UNMAPPED")
            all_rows = [row for row in market.get(symbol, {}).get("bars", ()) if (_bar_time(row) or cutoff) <= cutoff]
            rows = [row for row in all_rows if (_bar_time(row) or cutoff).date() == cutoff.date()]
            if not rows:
                continue
            first_open = _bar_value(rows[0], "open")
            last_close = _bar_value(rows[-1], "close")
            previous = [row for row in all_rows if (_bar_time(row) or cutoff).date() < cutoff.date()]
            previous_close = _bar_value(previous[-1], "close") if previous else None
            reference = previous_close if previous_close is not None and previous_close > 0 else first_open
            if reference is None or last_close is None or reference <= 0:
                continue
            covered += 1
            item = {
                "symbol": symbol,
                "name": str(candidate.get("name") or ""),
                "theme_id": theme,
                "theme_name": str(candidate.get("theme_name") or ""),
                "pool": str(candidate.get("pool") or "UNKNOWN"),
                "return": last_close / reference - 1.0,
                "return_basis": "PREVIOUS_CLOSE" if previous_close is not None and previous_close > 0 else "FIRST_INTRADAY_OPEN",
                "selection_reasons": [str(value) for value in candidate.get("selection_reasons", ()) if value],
                "risk_reasons": [str(value) for value in candidate.get("risk_reasons", ()) if value],
            }
            stock_performance.append(item)
            by_theme[theme].append(item)
        ranking = []
        for theme, rows in by_theme.items():
            returns = [float(row["return"]) for row in rows]
            ranking.append({
                "evidence_id": f"A5V:A2:THEME:{theme}", "theme_id": theme,
                "sample_count": len(rows), "advance_ratio": round(sum(value > 0 for value in returns) / len(returns), 6),
                "median_return": round(median(returns), 8), "mean_return": _mean(returns),
            })
        ranking.sort(key=lambda row: (-float(row["median_return"]), -float(row["advance_ratio"]), -int(row["sample_count"]), str(row["theme_id"])))
        selected = {str(row.get("theme_id") or "") for row in a2.get("themes", ()) if isinstance(row, Mapping)}
        adequately_sampled = [row for row in ranking if int(row["sample_count"]) >= 3]
        top_n = 5
        independent_top = {str(row["theme_id"]) for row in (adequately_sampled or ranking)[:top_n]}
        plan_symbols = {
            str(row.get("symbol") or _mapping(row.get("payload_json")).get("symbol") or "")
            for row in plan_rows
        }
        effective_symbols = {
            str(_mapping(row.get("payload_json")).get("symbol") or "")
            for row in event_rows if bool(row.get("effective"))
        }
        stock_performance.sort(key=lambda row: (-float(row["return"]), str(row["symbol"])))
        production_candidate_by_symbol = {
            str(row.get("symbol") or ""): dict(row)
            for row in a2.get("candidates", candidates) if str(row.get("symbol") or "")
        }
        alternate_performance_by_symbol = {str(row.get("symbol") or ""): row for row in stock_performance}
        for cross_row in market_cross_section:
            symbol = str(cross_row.get("symbol") or "")
            if symbol not in confirmation_symbols or symbol in alternate_performance_by_symbol:
                continue
            all_rows = [row for row in market.get(symbol, {}).get("bars", ()) if (_bar_time(row) or cutoff) <= cutoff]
            current_rows = [row for row in all_rows if (_bar_time(row) or cutoff).date() == cutoff.date()]
            previous_rows = [row for row in all_rows if (_bar_time(row) or cutoff).date() < cutoff.date()]
            if not current_rows:
                continue
            previous_close = _bar_value(previous_rows[-1], "close") if previous_rows else None
            first_open = _bar_value(current_rows[0], "open")
            current_close = _bar_value(current_rows[-1], "close")
            reference = previous_close if previous_close is not None and previous_close > 0 else first_open
            if reference is None or current_close is None or reference <= 0:
                continue
            alternate_performance_by_symbol[symbol] = {
                "symbol": symbol,
                "return": current_close / reference - 1.0,
                "return_basis": "PREVIOUS_CLOSE" if previous_close is not None and previous_close > 0 else "FIRST_INTRADAY_OPEN",
            }
        audit_performance = [r for r in market_cross_section if r.get("ranking_eligible") is not False] if market_cross_section else stock_performance
        positive = [row for row in audit_performance if float(row["return"]) > 0]
        relative_limit = min(20, max(1, math.ceil(len(audit_performance) * 0.01))) if market_cross_section else min(20, max(1, math.ceil(len(audit_performance) * 0.10))) if audit_performance else 0
        counterexamples = []
        for rank, row in enumerate(positive[:relative_limit], start=1):
            symbol = str(row["symbol"])
            if symbol in effective_symbols:
                continue
            production_candidate = production_candidate_by_symbol.get(symbol)
            pool = str(production_candidate.get("pool") if production_candidate else row.get("pool") or "UNKNOWN")
            technical = {str(item.get("symbol")): item for item in a2.get("technical_candidates", ()) if isinstance(item, Mapping)}
            drop_stage = counterexample_drop_stage(symbol, pool, plan_symbols, technical)
            reason_source = production_candidate or row
            independent_confirmation = bool(market_cross_section) and symbol in confirmation_symbols
            alternate_return = (
                _number(alternate_performance_by_symbol.get(symbol, {}).get("return"))
                if independent_confirmation else None
            )
            local_return = float(row["return"])
            return_difference = abs(alternate_return - local_return) if alternate_return is not None else None
            counterexamples.append({
                "evidence_id": f"A5V:MISS:{symbol}",
                "symbol": symbol,
                "name": str(row.get("name") or ""),
                "theme_id": str(row.get("theme_id") or ""),
                "theme_name": str(row.get("theme_name") or ""),
                "source_pool": pool,
                "a3_candidate": technical.get(symbol),
                "intraday_return": round(local_return, 8),
                "return_basis": row["return_basis"],
                "performance_rank": rank,
                "performance_percentile_floor": round(1.0 - ((rank - 1) / max(1, len(audit_performance))), 6),
                "drop_stage": drop_stage,
                "has_a3_plan": symbol in plan_symbols,
                "has_effective_a4_event": False,
                "production_selection_reasons": [str(value) for value in reason_source.get("selection_reasons", ())][:8],
                "production_risk_reasons": [str(value) for value in reason_source.get("risk_reasons", ())][:8],
                "alternate_source_confirmation_requested": independent_confirmation,
                "alternate_source_confirmation_available": independent_confirmation and symbol in alternate_performance_by_symbol,
                "alternate_source_return": alternate_return,
                "alternate_source_return_absolute_difference": return_difference,
                "alternate_source_status": (
                    "MATCH" if return_difference is not None and return_difference <= 0.005
                    else "MISMATCH" if return_difference is not None
                    else "NOT_APPLICABLE" if not independent_confirmation
                    else "DATA_LIMITED"
                ),
                "interpretation_boundary": "客观强势反例，不等于当时必然存在合规买点",
            })
        ratio = covered / len(candidates) if candidates else 0.0
        reason_counts: dict[str, int] = defaultdict(int)
        for symbol in (str(row.get("symbol") or "") for row in candidates):
            reason_counts[str(market.get(symbol, {}).get("reason_code") or "NO_RESULT")] += 1
        return {
            "status": "READY" if ratio >= 0.8 else "DEGRADED" if covered else "UNAVAILABLE",
            "evidence_id": "A5V:A2:SUMMARY", "source_family": "TENCENT_MINUTE_PRICE_BREADTH",
            "scope": "A2模型复核候选的异源分钟价格抽验；全池量化去向不截断，不代表全市场板块排名",
            "quant_lineage_candidate_count": len(production_candidate_by_symbol),
            "candidate_count": len(candidates), "covered_count": covered, "coverage": round(ratio, 6),
            "provider_reason_counts": dict(sorted(reason_counts.items())),
            "independent_top5_theme_ids": [str(row["theme_id"]) for row in (adequately_sampled or ranking)[:top_n]],
            "ranking_basis": "CANDIDATE_PRICE_BREADTH_NOT_BOARD_STRENGTH_OR_NET_FLOW",
            "ranking_comparable_to_production": False,
            "selected_theme_overlap_count": len(selected & independent_top),
            "selected_theme_overlap_ratio": round(len(selected & independent_top) / max(1, len(independent_top)), 6),
            "theme_rankings": ranking,
            "adequately_sampled_theme_count": len(adequately_sampled),
            "counterexample_scope": (
                "有可用行情的A1研究样本盘后横截面，缺行情标的不参与排序；对顶部1%发起腾讯异源确认"
                if market_cross_section
                else "A1进入A2并在A2审计输出中可追踪的盘中候选域，不冒充全市场扫描"
            ),
            "market_universe_count": len(market_cross_section),
            "missing_cross_section_candidate_symbols": sorted(
                {str(row.get("symbol") or "") for row in candidates}
                - {str(row.get("symbol") or "") for row in market_cross_section}
            ) if market_cross_section else [],
            "alternate_confirmation_requested_count": len(confirmation_symbols),
            "counterexamples": counterexamples,
        }

    @staticmethod
    def _verify_a3(plan_rows: Sequence[Mapping[str, Any]], daily: Mapping[str, Sequence[Mapping[str, Any]]], tdx_5m: Mapping[str, Mapping[str, Any]], cutoff: datetime) -> dict[str, Any]:
        results = []
        ready = 0
        for row in plan_rows:
            payload = _mapping(row.get("payload_json"))
            symbol = str(row.get("symbol") or payload.get("symbol") or "")
            bars = list(daily.get(symbol, ()))
            closes = [value for item in bars if (value := _bar_value(item, "close")) is not None]
            latest_daily = closes[-1] if closes else None
            declared_daily = _mapping(_mapping(payload.get("ma_analysis")).get("daily"))
            strategy_facts = _mapping(payload.get("strategy_facts"))
            declared_daily = declared_daily or _mapping(strategy_facts.get("daily_moving_averages"))
            declared_macd = _mapping(payload.get("daily_macd")) or _mapping(strategy_facts.get("daily_macd_evidence"))
            if not declared_macd.get("input_hash"):
                declared_macd = {**declared_macd, "input_hash": _mapping(strategy_facts.get("daily_macd_evidence")).get("input_hash")}
            macd_check = daily_macd_check(declared_macd, closes)
            recomputed = {"ma5": _moving_average(closes, 5), "ma20": _moving_average(closes, 20), "ma60": _moving_average(closes, 60)}
            errors = {
                key: _relative_difference(
                    recomputed[key],
                    _number(declared_daily.get(key, declared_daily.get(key.upper()))),
                )
                for key in recomputed
            }
            comparable = [value for value in errors.values() if value is not None]
            formula_status = "MATCH" if comparable and max(comparable) <= 0.001 else "MISMATCH" if comparable else "DATA_LIMITED"
            tdx_days: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for item in tdx_5m.get(symbol, {}).get("bars", ()):
                stamp = _bar_time(item)
                if stamp is not None and stamp.date() < cutoff.date():
                    tdx_days[stamp.date().isoformat()].append(dict(item))
            tdx_previous = None
            if tdx_days:
                day = sorted(tdx_days)[-1]
                tdx_previous = _bar_value(tdx_days[day][-1], "close")
            close_diff = _relative_difference(tdx_previous, latest_daily)
            price_status = "MATCH" if close_diff is not None and close_diff <= 0.005 else "MISMATCH" if close_diff is not None else "DATA_LIMITED"
            behavior = str(payload.get("stock_behavior_type") or "").upper()
            strategy = str(payload.get("strategy_profile") or "").upper()
            route_ok = (behavior == "EMOTION" and strategy == "LEADER_INTRADAY") or (behavior == "TREND" and strategy in {"MA520_SWING", "TREND_MA5"})
            low, high = _number(payload.get("trigger_low")), _number(payload.get("trigger_high"))
            stop = _number(payload.get("stop_level", payload.get("invalidation_level")))
            no_chase = _number(payload.get("no_chase_price", payload.get("max_chase_price")))
            levels_ok = all(value is not None for value in (low, high, stop)) and bool(stop < low <= high) and (no_chase is None or high <= no_chase)
            if formula_status != "DATA_LIMITED":
                ready += 1
            results.append({
                "evidence_id": f"A5V:A3:PLAN:{row.get('plan_id')}", "plan_id": row.get("plan_id"),
                "symbol": symbol, "strategy_profile": strategy, "route_contract_match": route_ok,
                "price_levels_valid": levels_ok, "daily_bar_count": len(closes), "recomputed_ma": recomputed,
                "declared_ma_relative_errors": errors, "formula_status": formula_status,
                "daily_macd_verification": macd_check,
                "tdx_previous_close": tdx_previous, "local_daily_close": latest_daily,
                "cross_source_close_relative_difference": close_diff, "cross_source_price_status": price_status,
                "tdx_reason_code": tdx_5m.get(symbol, {}).get("reason_code"),
            })
        ratio = ready / len(plan_rows) if plan_rows else 1.0
        indicator_limited = any(r["daily_macd_verification"]["formula_status"] != "MATCH"
                                or r["daily_macd_verification"]["input_hash_status"] != "MATCH" for r in results)
        formula_mismatch = any(r["formula_status"] == "MISMATCH" for r in results)
        return {
            "status": "READY" if ratio >= 0.8 and not indicator_limited and not formula_mismatch else "DEGRADED" if ready or not plan_rows else "UNAVAILABLE",
            "evidence_id": "A5V:A3:SUMMARY", "plan_count": len(plan_rows),
            "verified_fields": ["MA5", "MA20", "MA60", "CLOSE", "ROUTE", "PRICE_LEVELS"] + (["MACD"] if results and all(r["daily_macd_verification"]["formula_status"] != "DATA_LIMITED" for r in results) else []),
            "not_verified_fields": ["KDJ", "VOLUME"] + (["MACD"] if any(r["daily_macd_verification"]["formula_status"] == "DATA_LIMITED" for r in results) else []),
            "macd_formula_covered_count": sum(r["daily_macd_verification"]["formula_status"] != "DATA_LIMITED" for r in results),
            "macd_mismatch_count": sum(r["daily_macd_verification"]["formula_status"] == "MISMATCH" or r["daily_macd_verification"]["input_hash_status"] == "MISMATCH" for r in results),
            "formula_covered_count": ready, "formula_coverage": round(ratio, 6), "plans": results,
        }

    def _verify_a4(
        self,
        plan_rows: Sequence[Mapping[str, Any]],
        event_rows: Sequence[Mapping[str, Any]],
        tencent: Mapping[str, Mapping[str, Any]],
        tdx: Mapping[str, Mapping[str, Any]],
        local: Mapping[str, Mapping[str, Any]],
        cutoff: datetime,
    ) -> dict[str, Any]:
        by_plan: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for row in event_rows:
            payload = _mapping(row.get("payload_json"))
            plan_id = str(payload.get("plan_id") or "")
            if plan_id:
                by_plan[plan_id].append(row)
        results = []
        covered = 0
        for plan_row in plan_rows:
            payload = _mapping(plan_row.get("payload_json"))
            plan_id = str(plan_row.get("plan_id") or "")
            symbol = str(plan_row.get("symbol") or payload.get("symbol") or "")
            events = by_plan.get(plan_id, [])
            actual_minutes = {str(row.get("minute_end") or "") for row in events if row.get("minute_end")}
            try:
                start = datetime.fromisoformat(str(plan_row.get("valid_from"))).astimezone(SHANGHAI)
            except (TypeError, ValueError):
                start = cutoff.replace(hour=9, minute=31, second=0, microsecond=0)
            expected = _expected_minutes(start, cutoff)
            never_activated = (not plan_row.get("valid_from") and str(plan_row.get("status")) == "INVALIDATED" and not events)
            if never_activated:
                expected = 0
            orchestration_omissions = []
            effective_actions = []
            for event in events:
                event_payload = _mapping(event.get("payload_json"))
                strategy = _mapping(event_payload.get("strategy"))
                candidate = str(strategy.get("action") or "")
                action = str(event.get("action") or "")
                reason = str(event.get("reason_code") or "")
                if bool(event.get("effective")):
                    effective_actions.append(action)
                if candidate in _ACTIONABLE and not bool(event.get("effective")) and reason not in _LEGITIMATE_SUPPRESSIONS:
                    orchestration_omissions.append({"minute_end": event.get("minute_end"), "candidate": candidate, "recorded_action": action, "reason_code": reason})

            left = [_bar_dict(row) for row in tencent.get(symbol, {}).get("bars", ()) if (_bar_time(row) or cutoff).date() == cutoff.date()]
            right = [_bar_dict(row) for row in tdx.get(symbol, {}).get("bars", ()) if (_bar_time(row) or cutoff).date() == cutoff.date()]
            archived = [_bar_dict(row) for row in local.get(symbol, {}).get("bars", ()) if (_bar_time(row) or cutoff).date() == cutoff.date()]
            left_by_time = {(_bar_time(row) or cutoff).isoformat(): row for row in left}
            right_by_time = {(_bar_time(row) or cutoff).isoformat(): row for row in right}
            archived_by_time = {(_bar_time(row) or cutoff).isoformat(): row for row in archived}
            overlap = sorted(set(left_by_time) & set(right_by_time))
            differences = [
                value for stamp in overlap
                if (value := _relative_difference(_bar_value(left_by_time[stamp], "close"), _bar_value(right_by_time[stamp], "close"))) is not None
            ]
            mismatch_points = [{"bar_end": stamp,
                "tencent_close": _bar_value(left_by_time[stamp], "close"),
                "tdx_close": _bar_value(right_by_time[stamp], "close"),
                "relative_difference": value}
                for stamp in overlap
                if (value := _relative_difference(_bar_value(left_by_time[stamp], "close"), _bar_value(right_by_time[stamp], "close"))) is not None
                and value > 0.005]
            archived_overlap = sorted(set(archived_by_time) & set(right_by_time))
            cross_fields = _field_comparison(left_by_time, right_by_time)
            archive_fields = _field_comparison(archived_by_time, right_by_time)
            archive_mismatch = any(v["status"] == "MISMATCH" for v in archive_fields.values())
            alternate_mismatch = any(v["status"] == "MISMATCH" for v in cross_fields.values())
            archived_differences = [
                value for stamp in archived_overlap
                if (value := _relative_difference(_bar_value(archived_by_time[stamp], "close"), _bar_value(right_by_time[stamp], "close"))) is not None
            ]
            if overlap:
                covered += 1
            low, high = _number(payload.get("trigger_low")), _number(payload.get("trigger_high"))
            stop = _number(payload.get("stop_level", payload.get("invalidation_level")))
            tdx_closes = [value for item in right if (value := _bar_value(item, "close")) is not None]
            tdx_lows = [value for item in right if (value := _bar_value(item, "low")) is not None]
            results.append({
                "evidence_id": f"A5V:A4:PLAN:{plan_id}", "plan_id": plan_id, "symbol": symbol,
                "expected_observation_minutes": expected, "recorded_observation_minutes": len(actual_minutes),
                "decision_scope": "INVALIDATED_BEFORE_ACTIVATION" if never_activated else "ACTIVE_WINDOW",
                "observation_coverage": round(len(actual_minutes) / expected, 6) if expected else 1.0,
                "effective_actions": effective_actions, "orchestration_omission_count": len(orchestration_omissions),
                "orchestration_omissions": orchestration_omissions[:20],
                "tencent_bar_count": len(left), "tdx_bar_count": len(right), "cross_source_overlap_count": len(overlap),
                "tencent_reason_code": tencent.get(symbol, {}).get("reason_code"),
                "tdx_reason_code": tdx.get(symbol, {}).get("reason_code"),
                "cross_source_max_close_difference": max(differences) if differences else None,
                "cross_source_difference_unit": "RELATIVE_RATIO",
                "cross_source_mismatch_points": mismatch_points,
                "cross_source_field_checks": cross_fields, "archived_tdx_field_checks": archive_fields,
                "discrepancy_class": "PRODUCTION_ARCHIVE_DIVERGENCE" if archive_mismatch else "ALTERNATE_SOURCE_DIVERGENCE" if alternate_mismatch else "NO_COMPARABLE_MISMATCH",
                "indicator_formula_audit": audit_event_indicators(events),
                "cross_source_status": "MATCH" if differences and max(differences) <= 0.005 else "MISMATCH" if differences else "DATA_LIMITED",
                "archived_bar_count": len(archived), "archived_tdx_overlap_count": len(archived_overlap),
                "archive_basis": local.get(symbol, {}).get("archive_basis", "LEGACY_FIRST_OBSERVATION"),
                "decision_snapshot": local.get(symbol, {}).get("selection", {}),
                "archived_tdx_max_close_difference": max(archived_differences) if archived_differences else None,
                "archived_tdx_status": "MATCH" if archived_differences and max(archived_differences) <= 0.005 else "MISMATCH" if archived_differences else "DATA_LIMITED",
                "tdx_trigger_zone_seen": bool(low is not None and high is not None and any(low <= value <= high for value in tdx_closes)),
                "tdx_stop_touched": bool(stop is not None and any(value <= stop for value in tdx_lows)),
                "tencent_digest": _digest(left) if left else None, "tdx_digest": _digest(right) if right else None,
                "archived_digest": _digest(archived) if archived else None,
            })
        ratio = covered / len(plan_rows) if plan_rows else 1.0
        needs_attention = any(row["discrepancy_class"] != "NO_COMPARABLE_MISMATCH" or row["cross_source_status"] == "MISMATCH" or row["archived_tdx_status"] == "MISMATCH"
                              or row["observation_coverage"] < 1 or row["orchestration_omission_count"]
                              or any(count for counts in row["indicator_formula_audit"]["counts"].values()
                                     for status, count in counts.items() if status != "MATCH") for row in results)
        return {
            "status": "READY" if ratio >= 0.8 and not needs_attention else "DEGRADED" if covered or not plan_rows else "UNAVAILABLE",
            "evidence_id": "A5V:A4:SUMMARY", "plan_count": len(plan_rows),
            "cross_source_verified_fields": [f for f in ("OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "AMOUNT") if results and all(r["cross_source_field_checks"][f]["status"] != "DATA_LIMITED" for r in results)],
            "cross_source_not_verified_fields": [f for f in ("OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "AMOUNT") if not results or any(r["cross_source_field_checks"][f]["status"] == "DATA_LIMITED" for r in results)],
            "cross_source_covered_count": covered, "cross_source_coverage": round(ratio, 6),
            "plans": results,
        }


__all__ = ["A5IndependentVerifier"]
