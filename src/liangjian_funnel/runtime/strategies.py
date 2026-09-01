"""Pure deterministic A4 strategy evaluators.

The module is deliberately independent from the monitor, persistence and LLM
layers.  An A3 plan is the only source of strategy identity and frozen daily
facts.  The evaluator consumes that plan and closed one-minute bars, derives
session-safe five/15-minute bars, and returns a JSON-ready decision.

There is intentionally no composite score here.  Each strategy is a separate
rule path and a plan can select exactly one of the three paths:

* ``LEADER_INTRADAY`` - theme/ladder strength and reseal or first-range break;
* ``MA520_SWING`` - frozen daily MA5/MA20 context plus intraday confirmation;
* ``TREND_MA5`` - frozen daily trend plus an intraday pullback/reversal.

The module does not place orders and does not call a model.  A caller may use
the returned action as a candidate, but still owns paper-broker settlement,
T+1 and any optional veto-only model call.
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from enum import StrEnum
from typing import Any, Literal, TypeAlias
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field


SHANGHAI = ZoneInfo("Asia/Shanghai")


class StrategyProfile(StrEnum):
    """The only strategy identities which can reach the A4 evaluator."""

    LEADER_INTRADAY = "LEADER_INTRADAY"
    MA520_SWING = "MA520_SWING"
    TREND_MA5 = "TREND_MA5"


StrategyProfileValue: TypeAlias = Literal[
    "LEADER_INTRADAY",
    "MA520_SWING",
    "TREND_MA5",
]


class A4Action(StrEnum):
    """Actions exposed by this module; no free-form action is accepted."""

    NO_ACTION = "NO_ACTION"
    START_CONFIRMATION = "START_CONFIRMATION"
    BUY_SIGNAL = "BUY_SIGNAL"
    ADD_SIGNAL = "ADD_SIGNAL"
    REDUCE_SIGNAL = "REDUCE_SIGNAL"
    SELL_SIGNAL = "SELL_SIGNAL"
    FORCED_RISK_EXIT = "FORCED_RISK_EXIT"
    DATA_BLOCK = "DATA_BLOCK"


class StrategyEvaluation(BaseModel):
    """Frozen, JSON-serializable result of one deterministic strategy path."""

    model_config = ConfigDict(frozen=True, extra="allow")

    strategy_profile: str | None = None
    symbol: str = ""
    state: str = "WAITING"
    action: str = A4Action.NO_ACTION.value
    reason_codes: tuple[str, ...] = ()
    met_conditions: tuple[str, ...] = ()
    unmet_conditions: tuple[str, ...] = ()
    veto_conditions: tuple[str, ...] = ()
    confirmation_results: dict[str, dict[str, Any]] = Field(default_factory=dict)
    all_failed_confirmations: tuple[str, ...] = ()
    sector_data_lag_s: float | None = None
    closed_5m_end: str | None = None
    closed_15m_end: str | None = None

    def __getitem__(self, key: str) -> Any:
        """Permit the same small mapping-style access used by JSON callers."""

        return getattr(self, key)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)


ActionValue: TypeAlias = Literal[
    "NO_ACTION",
    "START_CONFIRMATION",
    "BUY_SIGNAL",
    "ADD_SIGNAL",
    "REDUCE_SIGNAL",
    "SELL_SIGNAL",
    "FORCED_RISK_EXIT",
    "DATA_BLOCK",
]


STRATEGY_PROFILES: tuple[str, ...] = tuple(item.value for item in StrategyProfile)
ACTIONS: frozenset[str] = frozenset(item.value for item in A4Action)

_SESSION_OPEN = time(9, 30)
_MORNING_CLOSE = time(11, 30)
_AFTERNOON_OPEN = time(13, 0)
_SESSION_CLOSE = time(15, 0)
_EPSILON = 1e-9


@dataclass(frozen=True, slots=True)
class _Bar:
    symbol: str
    end: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    amount: float


class _BarError(ValueError):
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


def evaluate_a4_plan(
    plan: Mapping[str, Any],
    bars: Mapping[str, Any] | Iterable[Any],
    *,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    """Evaluate one A3 plan against one frozen intraday bar snapshot.

    ``bars`` must contain closed one-minute bars for one symbol and one
    trading day.  ``as_of`` is the current minute end; when omitted it is
    inferred from the latest supplied bar.  If any supplied bar is newer than
    ``as_of`` the whole plan is data-blocked rather than silently dropping the
    future bar.

    The result always contains the public contract fields ``state``,
    ``action``, ``reason_codes``, ``met_conditions``, ``unmet_conditions``,
    ``veto_conditions``, ``closed_5m_end`` and ``closed_15m_end``.  Additional
    fields are diagnostic only and contain no model-authored instruction.
    """

    raw_profile = _lookup(plan, ("strategy_profile",), ("strategyProfile",))
    profile = _parse_profile(raw_profile)
    base = _base_result(profile, plan)
    if profile is None:
        return _finish(
            base,
            state="DATA_BLOCKED",
            action=A4Action.DATA_BLOCK,
            reasons=["UNKNOWN_STRATEGY_PROFILE"],
            unmet=["STRATEGY_PROFILE"],
            veto=["UNKNOWN_STRATEGY_PROFILE"],
        )

    try:
        normalized = _normalize_bars(bars, plan)
    except _BarError as exc:
        return _finish(
            base,
            state="DATA_BLOCKED",
            action=A4Action.DATA_BLOCK,
            reasons=[exc.reason],
            unmet=["VALID_CLOSED_1M_BARS"],
            veto=[exc.reason],
        )

    if not normalized:
        return _finish(
            base,
            state="DATA_BLOCKED",
            action=A4Action.DATA_BLOCK,
            reasons=["NO_1M_BARS"],
            unmet=["CURRENT_CLOSED_1M"],
            veto=["NO_1M_BARS"],
        )

    current = _resolve_as_of(plan, normalized, as_of)
    if current is None:
        return _finish(
            base,
            state="DATA_BLOCKED",
            action=A4Action.DATA_BLOCK,
            reasons=["AS_OF_TIMESTAMP_MISSING"],
            unmet=["AS_OF_TIMESTAMP"],
            veto=["AS_OF_TIMESTAMP_MISSING"],
        )
    if any(item.end > current for item in normalized):
        return _finish(
            base,
            state="DATA_BLOCKED",
            action=A4Action.DATA_BLOCK,
            reasons=["FUTURE_BAR_DETECTED"],
            unmet=["NO_FUTURE_BARS"],
            veto=["FUTURE_BAR_DETECTED"],
        )

    expected_trade_date = _trade_date(plan)
    if expected_trade_date is not None and any(item.end.date() != expected_trade_date for item in normalized):
        return _finish(
            base,
            state="DATA_BLOCKED",
            action=A4Action.DATA_BLOCK,
            reasons=["TRADE_DATE_MISMATCH"],
            unmet=["SAME_TRADING_DATE"],
            veto=["TRADE_DATE_MISMATCH"],
        )
    if len({item.end.date() for item in normalized}) != 1:
        return _finish(
            base,
            state="DATA_BLOCKED",
            action=A4Action.DATA_BLOCK,
            reasons=["MIXED_TRADING_DATES"],
            unmet=["SAME_TRADING_DATE"],
            veto=["MIXED_TRADING_DATES"],
        )

    # A current minute is required only while the market is open.  At lunch
    # and after close the last closed bar remains a valid immutable snapshot;
    # no signal is generated outside an exchange session.
    session = _session_for(current)
    current_bar = next((item for item in reversed(normalized) if item.end == current), None)
    if session is not None and current_bar is None:
        return _finish(
            base,
            state="DATA_BLOCKED",
            action=A4Action.DATA_BLOCK,
            reasons=["STALE_1M"],
            unmet=["CURRENT_CLOSED_1M"],
            veto=["STALE_1M"],
            as_of=current,
        )

    bars_5m, bars_15m = _aggregate_sessions(normalized, as_of=current)
    base.update(
        {
            "as_of": current.isoformat(),
            "reference_price": current_bar.close if current_bar is not None else None,
            "closed_5m_end": _iso_end(bars_5m),
            "closed_15m_end": _iso_end(bars_15m),
            "closed_5m_count": len(bars_5m),
            "closed_15m_count": len(bars_15m),
        }
    )

    if session is None:
        return _finish(
            base,
            state="WAITING",
            action=A4Action.NO_ACTION,
            reasons=["OUTSIDE_SESSION"],
            unmet=[],
            veto=[],
        )

    # Hard-stop evaluation deliberately precedes the 5m/15m availability
    # checks.  An exit safety rule must remain effective even when the
    # contextual bars are temporarily incomplete.
    stop = _number(
        _lookup(
            plan,
            ("stop_level",),
            ("invalidation_level",),
            ("daily_invalidation",),
            ("risk", "stop_level"),
            ("risk", "invalidation_level"),
        )
    )
    position_open = _position_open(plan)
    if current_bar is not None and stop is not None and current_bar.low <= stop + _EPSILON:
        return _finish(
            base,
            state="FORCED_RISK_EXIT",
            action=A4Action.FORCED_RISK_EXIT,
            reasons=["HARD_STOP"],
            met=["CURRENT_1M_HARD_STOP"],
            veto=["HARD_STOP"],
        )

    explicit_gap = _plan_data_gap(plan)
    if explicit_gap is not None:
        return _finish(
            base,
            state="DATA_BLOCKED",
            action=A4Action.DATA_BLOCK,
            reasons=[explicit_gap],
            unmet=["PLAN_DATA_READY"],
            veto=[explicit_gap],
        )

    if not bars_5m:
        return _finish(
            base,
            state="DATA_BLOCKED",
            action=A4Action.DATA_BLOCK,
            reasons=["NO_CLOSED_5M"],
            unmet=["CLOSED_5M"],
            veto=["NO_CLOSED_5M"],
        )
    if not bars_15m:
        return _finish(
            base,
            state="DATA_BLOCKED",
            action=A4Action.DATA_BLOCK,
            reasons=["NO_CLOSED_15M"],
            unmet=["CLOSED_15M"],
            veto=["NO_CLOSED_15M"],
        )

    # One-minute safety is evaluated before any strategy-specific trigger and
    # cannot be vetoed by a model.  It deliberately uses the current 1m low,
    # never a 5m/15m low, so a hard stop is not delayed by aggregation.
    if _plan_invalidated(plan):
        return _exit_or_cancel(
            base,
            plan,
            reason="PLAN_INVALIDATED",
            position_open=position_open,
            action=A4Action.SELL_SIGNAL,
        )

    locked, upper_limit = _locked_limit_up(plan, current_bar, bars_5m[-1])
    if locked:
        # A locked limit-up bar has no executable price.  It is a veto for
        # entry only; an existing position still receives a risk/exit result
        # from strategy-specific adverse conditions.
        base["upper_limit"] = upper_limit
        base["locked_limit_up"] = True
    else:
        base["locked_limit_up"] = False

    context = _common_context(plan, current_bar, bars_5m, bars_15m)
    if profile is StrategyProfile.LEADER_INTRADAY:
        decision = _evaluate_leader(plan, bars_5m, bars_15m, current_bar, context, position_open, locked)
    elif profile is StrategyProfile.MA520_SWING:
        decision = _evaluate_520(plan, bars_5m, bars_15m, current_bar, context, position_open, locked)
    else:
        decision = _evaluate_trend(plan, bars_5m, bars_15m, current_bar, context, position_open, locked)
    base.update(decision)
    return _finish(base, as_of=current)


def evaluate_strategy(
    plan: Mapping[str, Any],
    bars: Mapping[str, Any] | Iterable[Any],
    *,
    now: datetime,
    position: Mapping[str, Any] | None = None,
    market_context: Mapping[str, Any] | None = None,
) -> StrategyEvaluation:
    """Public Pydantic entry point for one plan and one current minute.

    ``position`` and ``market_context`` are optional runtime overlays.  They
    are copied into a new payload and cannot mutate the persisted A3 plan.
    Only context keys that are absent from the plan are inherited, so a
    frozen plan remains authoritative for strategy identity and daily facts.
    """

    payload = dict(plan)
    if position is not None:
        payload["position"] = dict(position)
    if market_context is not None:
        for key in (
            "leader_context",
            "ladder_context",
            "sector_context",
            "theme_context",
            "weekly_state",
            "monthly_state",
            "market_shock",
            "sector_data_as_of",
            "sector_as_of",
        ):
            if (key not in payload or payload.get(key) is None) and key in market_context:
                payload[key] = market_context[key]
    result = evaluate_a4_plan(payload, bars, as_of=now)
    return StrategyEvaluation.model_validate(result)


# The alias makes the module convenient for callers that use the descriptive
# ``evaluate_a4`` name while keeping one strategy dispatch implementation.
evaluate_a4 = evaluate_a4_plan


def aggregate_closed_bars(
    bars: Mapping[str, Any] | Iterable[Any],
    *,
    as_of: datetime | None = None,
) -> dict[str, tuple[dict[str, Any], ...]]:
    """Return session-safe closed ``5m``/``15m`` bars for diagnostics/tests."""

    normalized = _normalize_bars(bars, {})
    current = as_of.astimezone(SHANGHAI) if as_of is not None else (normalized[-1].end if normalized else None)
    if current is None:
        return {"5m": (), "15m": ()}
    if any(item.end > current for item in normalized):
        raise ValueError("FUTURE_BAR_DETECTED")
    five, fifteen = _aggregate_sessions(normalized, as_of=current)
    return {"5m": tuple(_bar_dict(item, "5m") for item in five), "15m": tuple(_bar_dict(item, "15m") for item in fifteen)}


def _base_result(profile: StrategyProfile | None, plan: Mapping[str, Any]) -> dict[str, Any]:
    symbol = _lookup(plan, ("symbol",), ("stock_code",), ("code",))
    return {
        "strategy_profile": profile.value if profile is not None else str(_lookup(plan, ("strategy_profile",)) or ""),
        "symbol": str(symbol or ""),
        "state": "WAITING",
        "action": A4Action.NO_ACTION.value,
        "reason_codes": [],
        "met_conditions": [],
        "unmet_conditions": [],
        "veto_conditions": [],
        "confirmation_results": {},
        "all_failed_confirmations": [],
        "sector_data_lag_s": None,
        "_sector_data_timestamp": _sector_data_timestamp(plan),
        "closed_5m_end": None,
        "closed_15m_end": None,
    }


def _finish(
    result: dict[str, Any],
    *,
    state: str | None = None,
    action: str | A4Action | None = None,
    reasons: Sequence[str] | None = None,
    met: Sequence[str] | None = None,
    unmet: Sequence[str] | None = None,
    veto: Sequence[str] | None = None,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    if state is not None:
        result["state"] = state
    if action is not None:
        value = action.value if isinstance(action, A4Action) else str(action)
        if value not in ACTIONS:
            value = A4Action.DATA_BLOCK.value
            result["state"] = "DATA_BLOCKED"
            result["veto_conditions"] = ["INVALID_ACTION"]
        result["action"] = value
    for key, values in (
        ("reason_codes", reasons),
        ("met_conditions", met),
        ("unmet_conditions", unmet),
        ("veto_conditions", veto),
    ):
        if values is not None:
            result[key] = _unique(values)
    if as_of is not None:
        result["as_of"] = as_of.isoformat()
    _finalize_observability(result, as_of=as_of)
    return result


def _finalize_observability(result: dict[str, Any], *, as_of: datetime | None) -> None:
    """Project the conditions actually used by a strategy into an audit map.

    The strategy functions intentionally keep their existing three lists.  A
    confirmation entry is derived only from those lists, so a leader plan does
    not acquire MA520 requirements (and vice versa) merely because a global
    config mentions them.  This function is called for every return path,
    including data blocks and exits.
    """

    reasons = result.get("reason_codes") or []
    reason_list = [str(value) for value in reasons if str(value).strip()]
    confirmations: dict[str, dict[str, Any]] = {}
    order: list[str] = []

    def add(
        name: Any,
        *,
        met: bool,
        kind: str,
        reason: str | None = None,
    ) -> None:
        key = str(name)
        if not key:
            return
        if key not in confirmations:
            order.append(key)
        selected_reason = str(reason or _confirmation_reason(key, reason_list, met=met))
        confirmations[key] = {
            "met": bool(met),
            "reason": selected_reason or ("OK" if met else "NOT_MET"),
            "kind": kind,
            "available": _confirmation_available(
                selected_reason or ("OK" if met else "NOT_MET"),
                result,
                met=met,
            ),
        }

    for name in result.get("met_conditions") or []:
        add(name, met=True, kind="CONDITION")
    for name in result.get("unmet_conditions") or []:
        add(name, met=False, kind="CONDITION")
    for name in result.get("veto_conditions") or []:
        add(name, met=False, kind="VETO", reason=str(name))

    result["confirmation_results"] = {name: confirmations[name] for name in order}
    result["all_failed_confirmations"] = [
        name for name in order if not bool(confirmations[name].get("met"))
    ]

    timestamp = result.pop("_sector_data_timestamp", None)
    result["sector_data_lag_s"] = _sector_data_lag_seconds(timestamp, as_of)


def _confirmation_reason(name: str, reasons: Sequence[str], *, met: bool) -> str:
    if met:
        return "OK"
    normalized = name.upper()
    best: tuple[int, str] | None = None
    for reason in reasons:
        candidate = reason.upper()
        if candidate == normalized or normalized in candidate or candidate in normalized:
            return reason
        name_tokens = {token for token in normalized.split("_") if token not in {"NOT", "NO"}}
        reason_tokens = {token for token in candidate.split("_") if token not in {"NOT", "NO"}}
        overlap = len(name_tokens & reason_tokens)
        if overlap >= 2 and (best is None or overlap > best[0]):
            best = (overlap, reason)
    return best[1] if best is not None else "NOT_MET"


def _confirmation_available(reason: str, result: Mapping[str, Any], *, met: bool) -> bool:
    if met:
        return True
    if str(result.get("state") or "").upper() == "DATA_BLOCKED":
        return False
    token = str(reason or "").upper()
    return not any(
        marker in token
        for marker in ("MISSING", "UNAVAILABLE", "NO_1M", "NO_CLOSED", "STALE", "FUTURE", "DATA_GAP")
    )


def _sector_data_lag_seconds(timestamp: Any, as_of: datetime | None) -> float | None:
    """Return lag only for an explicit, timezone-aware sector timestamp."""

    if timestamp is None or as_of is None:
        return None
    parsed = _parse_timestamp(timestamp)
    if parsed is None:
        return None
    lag = (as_of.astimezone(SHANGHAI) - parsed).total_seconds()
    # A future-dated fact cannot be called a negative lag.  Keep it unknown so
    # callers never mistake a clock/data-quality problem for freshness.
    return round(lag, 3) if lag >= 0 else None


def _parse_profile(value: Any) -> StrategyProfile | None:
    try:
        return StrategyProfile(str(value).strip().upper())
    except (TypeError, ValueError):
        return None


def _normalize_bars(bars: Mapping[str, Any] | Iterable[Any], plan: Mapping[str, Any]) -> list[_Bar]:
    if isinstance(bars, Mapping):
        if any(key in bars for key in ("close", "bar_end", "end", "timestamp")):
            values: list[Any] = [bars]
        else:
            values = list(bars.values())
    else:
        values = list(bars)
    if not values:
        return []
    expected_symbol = _lookup(plan, ("symbol",), ("stock_code",), ("code",))
    normalized: list[_Bar] = []
    seen: set[tuple[str, datetime]] = set()
    for raw in values:
        symbol = _lookup(raw, ("symbol",), ("code",)) or expected_symbol
        if symbol is None:
            raise _BarError("SYMBOL_MISSING")
        interval = str(_lookup(raw, ("interval",), ("frequency",)) or "1m").lower()
        if interval in {"1min", "minute", "1minute"}:
            interval = "1m"
        if interval != "1m":
            raise _BarError("INPUT_NOT_1M")
        closed = _lookup(raw, ("closed",), ("is_closed",))
        if closed is False:
            raise _BarError("UNFINISHED_1M_BAR")
        end = _parse_datetime(_lookup(raw, ("bar_end",), ("end",), ("timestamp",), ("time",)))
        if end is None:
            raise _BarError("BAR_TIMESTAMP_MISSING")
        values_map = {
            name: _number(_lookup(raw, (name,)))
            for name in ("open", "high", "low", "close", "volume", "amount")
        }
        if any(value is None for value in values_map.values()):
            raise _BarError("BAR_VALUE_MISSING")
        if any(values_map[name] is None or values_map[name] <= 0 for name in ("open", "high", "low", "close")):
            raise _BarError("BAR_PRICE_INVALID")
        if values_map["volume"] < 0 or values_map["amount"] < 0:
            raise _BarError("BAR_VOLUME_INVALID")
        if values_map["low"] > min(values_map["open"], values_map["close"]) or values_map["high"] < max(values_map["open"], values_map["close"]):
            raise _BarError("BAR_OHLC_INVALID")
        canonical = _symbol_key(str(symbol))
        key = (canonical, end)
        if key in seen:
            raise _BarError("DUPLICATE_1M_BAR")
        seen.add(key)
        normalized.append(_Bar(symbol=canonical, end=end, **values_map))
    normalized.sort(key=lambda item: item.end)
    symbols = {item.symbol for item in normalized}
    if len(symbols) != 1:
        raise _BarError("MIXED_SYMBOLS")
    if expected_symbol and _symbol_key(str(expected_symbol)) != next(iter(symbols)):
        raise _BarError("SYMBOL_MISMATCH")
    return normalized


def _aggregate_sessions(bars: Sequence[_Bar], *, as_of: datetime) -> tuple[list[_Bar], list[_Bar]]:
    """Aggregate complete buckets, never crossing either session boundary."""

    by_end = {bar.end: bar for bar in bars if bar.end <= as_of}
    five: list[_Bar] = []
    fifteen: list[_Bar] = []
    days = sorted({bar.end.date() for bar in bars})
    for day in days:
        for origin, session_end in ((time(9, 30), _MORNING_CLOSE), (time(13, 0), _SESSION_CLOSE)):
            for period, output in ((5, five), (15, fifteen)):
                bucket_end = datetime.combine(day, origin, tzinfo=SHANGHAI) + timedelta(minutes=period)
                while bucket_end.time().replace(tzinfo=None) <= session_end:
                    ends = tuple(bucket_end - timedelta(minutes=offset) for offset in range(period - 1, -1, -1))
                    group = [by_end.get(end) for end in ends]
                    if all(item is not None for item in group) and bucket_end <= as_of:
                        output.append(_aggregate_group(tuple(item for item in group if item is not None), period))
                    bucket_end += timedelta(minutes=period)
    five.sort(key=lambda item: item.end)
    fifteen.sort(key=lambda item: item.end)
    return five, fifteen


def _aggregate_group(group: tuple[_Bar, ...], period: int) -> _Bar:
    return _Bar(
        symbol=group[-1].symbol,
        end=group[-1].end,
        open=group[0].open,
        high=max(item.high for item in group),
        low=min(item.low for item in group),
        close=group[-1].close,
        volume=sum(item.volume for item in group),
        amount=sum(item.amount for item in group),
    )


def _evaluate_leader(
    plan: Mapping[str, Any],
    five: Sequence[_Bar],
    fifteen: Sequence[_Bar],
    current: _Bar | None,
    context: Mapping[str, Any],
    position_open: bool,
    locked: bool,
) -> dict[str, Any]:
    met: list[str] = []
    unmet: list[str] = []
    reasons: list[str] = []
    veto: list[str] = []
    leader = _leader_context(plan)
    if leader is None:
        unmet.append("LEADER_CONTEXT")
        reasons.append("LEADER_CONTEXT_MISSING")
    else:
        if _context_expired(leader, context.get("as_of")):
            unmet.append("LEADER_CONTEXT_CURRENT")
            reasons.append("LEADER_CONTEXT_EXPIRED")
        else:
            met.append("LEADER_CONTEXT_CURRENT")
        stage = str(_lookup(leader, ("theme_stage",), ("stage",), ("emotion_stage",)) or "").upper()
        intact_value = _bool_value(_lookup(leader, ("ladder_intact",), ("ladder", "intact")))
        broken_value = _bool_value(_lookup(leader, ("ladder_broken",)))
        ladder_ok = intact_value if intact_value is not None else None if broken_value is None else not broken_value
        ladder_known = ladder_ok is not None or _lookup(leader, ("board_count",), ("consecutive_boards",), ("ladder_position",), ("ladder_height",), ("ladder", "height")) is not None
        stage_known = stage not in {"", "UNKNOWN", "NONE"} or _lookup(leader, ("theme_id",), ("sector",), ("theme",)) is not None
        if _bool_value(_lookup(leader, ("ladder_broken",))) is True or ladder_ok is False or stage in {"RETREAT", "DECAY", "DISTRIBUTION", "FADING", "退潮"}:
            reason = "LEADER_LADDER_BROKEN" if ladder_ok is False or _bool_value(_lookup(leader, ("ladder_broken",))) is True else "LEADER_THEME_RETREAT"
            reasons.append(reason)
            veto.append(reason)
            unmet.append("LEADER_CONTEXT_STRENGTH")
            return _exit_or_cancel_decision(position_open, reason, met, unmet, reasons, veto)
        if ladder_known and stage_known:
            met.append("LEADER_CONTEXT_STRENGTH")
        else:
            unmet.append("LEADER_CONTEXT_STRENGTH")
            reasons.append("LEADER_CONTEXT_INCOMPLETE")
    latest15 = fifteen[-1]
    prior15 = fifteen[-2] if len(fifteen) >= 2 else None
    stable = _fifteen_not_weak(latest15, prior15)
    if stable:
        met.append("LEADER_15M_NOT_WEAK")
    else:
        unmet.append("LEADER_15M_NOT_WEAK")
        reasons.append("LEADER_15M_WEAK")

    latest5 = five[-1]
    prior5 = five[-2] if len(five) >= 2 else None
    vwap = _vwap(five)
    divergence = _leader_divergence(five, vwap)
    reseal = _bool_value(_lookup(leader or {}, ("reseal",), ("re_close",), ("reclose",))) is True or _reseal_bar(latest5, prior5)
    breakout = _first_15m_breakout(five, fifteen)
    if divergence:
        met.append("LEADER_5M_DIVERGENCE_TO_STRENGTH")
    if reseal:
        met.append("LEADER_5M_RESEAL")
    if breakout:
        met.append("LEADER_FIRST_15M_BREAKOUT")
    if not (divergence or reseal or breakout):
        unmet.append("LEADER_5M_TRIGGER")
        reasons.append("LEADER_TRIGGER_NOT_MET")

    high_shadow = _high_volume_upper_shadow(latest5, five)
    if high_shadow:
        reasons.append("LEADER_HIGH_VOLUME_UPPER_SHADOW")
        veto.append("LEADER_HIGH_VOLUME_UPPER_SHADOW")
        if position_open:
            return _exit_or_cancel_decision(position_open, "LEADER_HIGH_VOLUME_UPPER_SHADOW", met, unmet, reasons, veto, action=A4Action.REDUCE_SIGNAL)

    board_count = _number(_lookup(leader or {}, ("board_count",), ("consecutive_boards",), ("ladder_position",), ("ladder_height",), ("ladder", "height")))
    if board_count is not None and board_count <= 1:
        unmet.append("LEADER_BOARD_CONFIRMATION")
        reasons.append("LEADER_FIRST_BOARD_OBSERVE")
    elif board_count is not None and board_count >= 4:
        unmet.append("LEADER_BOARD_RISK")
        reasons.append("LEADER_CLIMAX_NO_NEW_ENTRY")
        veto.append("LEADER_CLIMAX_NO_NEW_ENTRY")

    leader_valid = leader is not None and "LEADER_CONTEXT_CURRENT" in met and "LEADER_CONTEXT_STRENGTH" in met
    if locked:
        unmet.append("EXECUTABLE_NOT_LOCKED_LIMIT_UP")
        reasons.append("LOCKED_LIMIT_UP")
        veto.append("LOCKED_LIMIT_UP")
    elif leader_valid and stable and (divergence or reseal or breakout) and not any(item in veto for item in ("LEADER_CLIMAX_NO_NEW_ENTRY",)) and not (board_count is not None and board_count <= 1):
        if _price_zone_met(plan, latest5.close):
            met.append("A3_TRIGGER_ZONE")
            return _entry_decision(plan, met, unmet, reasons, veto)
        unmet.append("A3_TRIGGER_ZONE")
        reasons.append("TRIGGER_ZONE_NOT_MET")
    return _waiting_decision(met, unmet, reasons, veto, forced_action=A4Action.NO_ACTION if locked else None)


def _evaluate_520(
    plan: Mapping[str, Any],
    five: Sequence[_Bar],
    fifteen: Sequence[_Bar],
    current: _Bar | None,
    context: Mapping[str, Any],
    position_open: bool,
    locked: bool,
) -> dict[str, Any]:
    met: list[str] = []
    unmet: list[str] = []
    reasons: list[str] = []
    veto: list[str] = []
    daily = _daily_context(plan)
    ma5 = _number(_lookup(daily, ("ma5",), ("MA5",))) if daily else None
    ma20 = _number(_lookup(daily, ("ma20",), ("MA20",))) if daily else None
    daily_close = _number(_lookup(daily, ("close",), ("latest_close",), ("daily_close",))) if daily else None
    if ma5 is None or ma20 is None:
        unmet.append("DAILY_MA5_MA20_SNAPSHOT")
        reasons.append("MA520_DAILY_SNAPSHOT_MISSING")
    else:
        if ma5 >= ma20:
            met.append("DAILY_MA5_ABOVE_MA20")
        else:
            unmet.append("DAILY_MA5_ABOVE_MA20")
            reasons.append("MA520_DEATH_CROSS")
        if daily_close is None or daily_close > ma20:
            met.append("DAILY_CLOSE_ABOVE_MA20")
        else:
            unmet.append("DAILY_CLOSE_ABOVE_MA20")
            reasons.append("MA520_BELOW_MA20")

    latest15 = fifteen[-1]
    prior15 = fifteen[-2] if len(fifteen) >= 2 else None
    if _fifteen_not_weak(latest15, prior15):
        met.append("MA520_15M_STABLE")
    else:
        unmet.append("MA520_15M_STABLE")
        reasons.append("MA520_15M_WEAK")

    latest5 = five[-1]
    prior5 = five[-2] if len(five) >= 2 else None
    vwap = _vwap(five)
    higher_low = prior5 is not None and latest5.low + _EPSILON >= prior5.low
    reclaim = vwap is not None and latest5.close + _EPSILON >= vwap
    if higher_low:
        met.append("MA520_5M_HIGHER_LOW")
    else:
        unmet.append("MA520_5M_HIGHER_LOW")
        reasons.append("MA520_5M_NO_HIGHER_LOW")
    if reclaim:
        met.append("MA520_5M_VWAP_RECLAIM")
    else:
        unmet.append("MA520_5M_VWAP_RECLAIM")
        reasons.append("MA520_5M_VWAP_NOT_RECLAIMED")

    confirmations = _520_confirmations(five)
    if confirmations >= 2:
        met.append("MA520_TWO_CLOSED_5M_CONFIRMATIONS")
    else:
        unmet.append("MA520_TWO_CLOSED_5M_CONFIRMATIONS")
        reasons.append("MA520_CONFIRMATION_PENDING")
    volume_ok, volume_reason = _volume_not_overheated(plan, five)
    if volume_ok:
        met.append("MA520_VOLUME_NOT_OVERHEATED")
    else:
        unmet.append("MA520_VOLUME_NOT_OVERHEATED")
        reasons.append(volume_reason)
        veto.append(volume_reason)

    # A3 owns the daily MA520 route decision.  Daily averages alone describe
    # a left-side setup and must not become an executable A4 entry when an
    # older/partial plan did not carry the deterministic right-side evidence.
    # Keep this as a separate condition so the missing frozen field remains
    # visible in the per-plan DATA_BLOCK result and cannot be mistaken for a
    # live-data opportunity.
    right_side_confirmed = _ma520_right_side_confirmed(plan)
    if right_side_confirmed:
        met.append("A3_RIGHT_SIDE_CONFIRMATION")
    else:
        unmet.append("A3_RIGHT_SIDE_CONFIRMATION")
        reasons.append("A3_RIGHT_SIDE_CONFIRMATION_MISSING")
        veto.append("A3_RIGHT_SIDE_CONFIRMATION_MISSING")

    # A 5m MA5/MA20 supplied by a caller is intentionally ignored.  This
    # marker makes the separation auditable without turning intraday averages
    # into a hidden daily 520 signal.
    met.append("DAILY_MA5_MA20_ONLY")
    if locked:
        unmet.append("EXECUTABLE_NOT_LOCKED_LIMIT_UP")
        reasons.append("LOCKED_LIMIT_UP")
        veto.append("LOCKED_LIMIT_UP")
    if position_open and (ma5 is not None and ma20 is not None and (ma5 < ma20 or (daily_close is not None and daily_close <= ma20))):
        return _exit_or_cancel_decision(position_open, "MA520_DAILY_BREAKDOWN", met, unmet, reasons, veto, action=A4Action.SELL_SIGNAL)
    if not right_side_confirmed:
        return {
            "state": "DATA_BLOCKED",
            "action": A4Action.DATA_BLOCK.value,
            "reason_codes": _unique(reasons),
            "met_conditions": _unique(met),
            "unmet_conditions": _unique(unmet),
            "veto_conditions": _unique(veto),
        }
    if all(name in met for name in ("DAILY_MA5_ABOVE_MA20", "DAILY_CLOSE_ABOVE_MA20", "MA520_15M_STABLE", "MA520_5M_HIGHER_LOW", "MA520_5M_VWAP_RECLAIM", "MA520_TWO_CLOSED_5M_CONFIRMATIONS", "MA520_VOLUME_NOT_OVERHEATED", "A3_RIGHT_SIDE_CONFIRMATION")) and not locked:
        if _price_zone_met(plan, latest5.close):
            met.append("A3_TRIGGER_ZONE")
            return _entry_decision(plan, met, unmet, reasons, veto)
        unmet.append("A3_TRIGGER_ZONE")
        reasons.append("TRIGGER_ZONE_NOT_MET")
    return _waiting_decision(met, unmet, reasons, veto, forced_action=A4Action.NO_ACTION if locked else None)


def _evaluate_trend(
    plan: Mapping[str, Any],
    five: Sequence[_Bar],
    fifteen: Sequence[_Bar],
    current: _Bar | None,
    context: Mapping[str, Any],
    position_open: bool,
    locked: bool,
) -> dict[str, Any]:
    met: list[str] = []
    unmet: list[str] = []
    reasons: list[str] = []
    veto: list[str] = []
    daily = _daily_context(plan)
    ma5 = _number(_lookup(daily, ("ma5",), ("MA5",))) if daily else None
    ma10 = _number(_lookup(daily, ("ma10",), ("MA10",))) if daily else None
    ma20 = _number(_lookup(daily, ("ma20",), ("MA20",))) if daily else None
    ma60 = _number(_lookup(daily, ("ma60",), ("MA60",))) if daily else None
    daily_close = _number(_lookup(daily, ("close",), ("latest_close",), ("daily_close",))) if daily else None
    if all(value is not None for value in (ma5, ma10, ma20, ma60, daily_close)):
        if ma5 > ma10 > ma20 and daily_close > ma60:
            met.append("DAILY_MAIN_UPTREND")
        else:
            unmet.append("DAILY_MAIN_UPTREND")
            reasons.append("TREND_DAILY_NOT_MAIN_UPTREND")
    else:
        unmet.append("DAILY_MA5_MA10_MA20_MA60_SNAPSHOT")
        reasons.append("TREND_DAILY_SNAPSHOT_MISSING")
    if _bool_value(_lookup(daily, ("main_uptrend",), ("main_trend",))) is False or _bool_value(_lookup(plan, ("daily_trend_invalidated",), ("trend_invalidated",))) is True:
        unmet.append("DAILY_MAIN_UPTREND")
        reasons.append("TREND_DAILY_NOT_MAIN_UPTREND")
        veto.append("TREND_DAILY_NOT_MAIN_UPTREND")

    weekly = str(_lookup(plan, ("weekly_closed_state",), ("weekly_state",), ("weekly_trend",), ("higher_timeframe", "weekly")) or "").upper()
    monthly = str(_lookup(plan, ("monthly_state",), ("monthly_trend",), ("higher_timeframe", "monthly")) or "").upper()
    if weekly not in {"DOWN", "BEARISH", "DECLINING", "下行", "走弱"} and monthly not in {"DOWN", "BEARISH", "DECLINING", "下行", "走弱"}:
        met.append("MONTHLY_WEEKLY_NOT_DOWN")
    else:
        unmet.append("MONTHLY_WEEKLY_NOT_DOWN")
        reasons.append("TREND_HIGHER_TIMEFRAME_WEAK")

    latest15 = fifteen[-1]
    prior15 = fifteen[-2] if len(fifteen) >= 2 else None
    pressure_easing = _fifteen_not_weak(latest15, prior15)
    if pressure_easing:
        met.append("TREND_15M_PRESSURE_EASING")
    else:
        unmet.append("TREND_15M_PRESSURE_EASING")
        reasons.append("TREND_15M_PRESSURE_NOT_EASING")
    latest5 = five[-1]
    prior5 = five[-2] if len(five) >= 2 else None
    vwap = _vwap(five)
    reversal = prior5 is not None and latest5.close >= latest5.open and latest5.close >= prior5.close and latest5.low + _EPSILON >= prior5.low and (vwap is None or latest5.close >= vwap)
    if reversal:
        met.append("TREND_5M_REVERSAL_CONFIRMATION")
    else:
        unmet.append("TREND_5M_REVERSAL_CONFIRMATION")
        reasons.append("TREND_5M_REVERSAL_NOT_CONFIRMED")
    volume_ok, volume_reason = _volume_not_overheated(plan, five)
    if volume_ok:
        met.append("TREND_VOLUME_NOT_OVERHEATED")
    else:
        unmet.append("TREND_VOLUME_NOT_OVERHEATED")
        reasons.append(volume_reason)
        veto.append(volume_reason)

    setup = str(_lookup(plan, ("setup_type",), ("technical_setup",), ("a3_setup",)) or "").upper()
    if setup in {"PRICE_DISCOVERY_TREND", "INNOVATION_HIGH", "NEW_HIGH", "创新高"} or _bool_value(_lookup(plan, ("innovation_high",), ("price_discovery",))) is True:
        met.append("PRICE_DISCOVERY_TREND_ROUTE")
    else:
        met.append("STANDARD_TREND_ROUTE")

    requested = _requested_action(plan)
    if requested is A4Action.ADD_SIGNAL:
        profitable = _profitable_position(plan, current.close if current is not None else latest5.close)
        if profitable:
            met.append("PROFITABLE_POSITION_FOR_ADD")
        else:
            unmet.append("PROFITABLE_POSITION_FOR_ADD")
            reasons.append("TREND_ADD_REQUIRES_PROFIT")
            veto.append("TREND_AVERAGE_DOWN_FORBIDDEN")
            return _waiting_decision(met, unmet, reasons, veto)
    if locked:
        unmet.append("EXECUTABLE_NOT_LOCKED_LIMIT_UP")
        reasons.append("LOCKED_LIMIT_UP")
        veto.append("LOCKED_LIMIT_UP")
    valid_daily = "DAILY_MAIN_UPTREND" in met
    if valid_daily and pressure_easing and reversal and volume_ok and not locked:
        if _price_zone_met(plan, latest5.close):
            met.append("A3_PULLBACK_ZONE")
            return _entry_decision(plan, met, unmet, reasons, veto)
        unmet.append("A3_PULLBACK_ZONE")
        reasons.append("TREND_PULLBACK_ZONE_NOT_MET")
    return _waiting_decision(met, unmet, reasons, veto, forced_action=A4Action.NO_ACTION if locked else None)


def _entry_decision(
    plan: Mapping[str, Any],
    met: list[str],
    unmet: list[str],
    reasons: list[str],
    veto: list[str],
) -> dict[str, Any]:
    requested = _requested_action(plan)
    if requested not in {A4Action.BUY_SIGNAL, A4Action.ADD_SIGNAL}:
        requested = A4Action.BUY_SIGNAL
    return {
        "state": "SIGNAL_READY",
        "action": requested.value,
        "reason_codes": _unique(["DETERMINISTIC_STRATEGY_CONFIRMATION", *reasons]),
        "met_conditions": _unique(met),
        "unmet_conditions": _unique(unmet),
        "veto_conditions": _unique(veto),
    }


def _waiting_decision(
    met: list[str],
    unmet: list[str],
    reasons: list[str],
    veto: list[str],
    *,
    forced_action: A4Action | None = None,
) -> dict[str, Any]:
    state = "CONFIRMING" if met and unmet else "WAITING"
    return {
        "state": state,
        "action": forced_action.value if forced_action is not None else A4Action.START_CONFIRMATION.value if met and unmet else A4Action.NO_ACTION.value,
        "reason_codes": _unique(reasons or ["STRATEGY_TRIGGER_NOT_MET"]),
        "met_conditions": _unique(met),
        "unmet_conditions": _unique(unmet),
        "veto_conditions": _unique(veto),
    }


def _exit_or_cancel(
    result: dict[str, Any],
    plan: Mapping[str, Any],
    *,
    reason: str,
    position_open: bool,
    action: A4Action,
) -> dict[str, Any]:
    if position_open and _sellable(plan):
        result["state"] = "EXIT_READY"
        result["action"] = action.value
        result["reason_codes"] = [reason]
        result["met_conditions"] = ["POSITION_OPEN", reason]
        result["unmet_conditions"] = []
        result["veto_conditions"] = [reason]
    else:
        result["state"] = "CANCELLED"
        result["action"] = A4Action.NO_ACTION.value
        result["reason_codes"] = [reason, "NO_SELLABLE_POSITION"] if position_open else [reason]
        result["met_conditions"] = [reason]
        result["unmet_conditions"] = ["SELLABLE_POSITION"] if position_open else []
        result["veto_conditions"] = [reason]
    return result


def _exit_or_cancel_decision(
    position_open: bool,
    reason: str,
    met: list[str],
    unmet: list[str],
    reasons: list[str],
    veto: list[str],
    *,
    action: A4Action = A4Action.SELL_SIGNAL,
) -> dict[str, Any]:
    if position_open:
        return {
            "state": "EXIT_READY",
            "action": action.value,
            "reason_codes": _unique([*reasons, reason]),
            "met_conditions": _unique([*met, "POSITION_OPEN"]),
            "unmet_conditions": _unique(unmet),
            "veto_conditions": _unique([*veto, reason]),
        }
    return {
        "state": "CANCELLED",
        "action": A4Action.NO_ACTION.value,
        "reason_codes": _unique([*reasons, reason]),
        "met_conditions": _unique(met),
        "unmet_conditions": _unique(unmet),
        "veto_conditions": _unique([*veto, reason]),
    }


def _common_context(plan: Mapping[str, Any], current: _Bar | None, five: Sequence[_Bar], fifteen: Sequence[_Bar]) -> dict[str, Any]:
    return {
        "as_of": current.end if current is not None else (five[-1].end if five else None),
        "vwap": _vwap(five),
        "five": five,
        "fifteen": fifteen,
    }


def _daily_context(plan: Mapping[str, Any]) -> Mapping[str, Any]:
    # A3 publishes a compact ``daily_ma``/``strategy_facts`` shape, while
    # replay fixtures often use ``daily_indicators``.  Normalize both at the
    # boundary; the resulting map remains frozen input, not a recalculation.
    sources: list[Mapping[str, Any]] = []
    for path in (("strategy_facts",), ("daily_indicators",), ("daily_technical",), ("daily",), ("technical",), ("a3_daily",), ()):
        value = plan if not path else _lookup(plan, path)
        if isinstance(value, Mapping):
            sources.append(value)
    result: dict[str, Any] = {}
    for source in sources:
        result.update(source)
    for source in sources:
        for key in ("daily_ma", "daily_moving_averages", "moving_averages"):
            moving = source.get(key)
            if isinstance(moving, Mapping):
                result.update(moving)
    for source in sources:
        for key in ("daily_close", "latest_close"):
            if result.get("close") is None and source.get(key) is not None:
                result["close"] = source[key]
    return result


def _ma520_right_side_confirmed(plan: Mapping[str, Any]) -> bool:
    """Read an explicit frozen A3 right-side confirmation for MA520.

    The current A3 payload projects the computed paths under
    ``strategy_facts.ma520_right_side``.  ``ma520_setup.second_wave_restart``
    and ``trend_reversal_confirmed`` are also accepted as explicit markers
    for compatible plans.  No MA5/MA20 or intraday facts are inferred here:
    a missing marker is a data contract failure, not a negative trading
    signal that A4 may reinterpret.
    """

    facts = _lookup(plan, ("strategy_facts",))
    if isinstance(facts, Mapping):
        setup = facts.get("ma520_setup")
        if isinstance(setup, Mapping) and any(
            _bool_value(setup.get(key)) is True
            for key in ("second_wave_restart", "trend_reversal_confirmed")
        ):
            return True

        # This is the shape emitted by the current A3 pipeline.  ``confirmed``
        # is scoped to this right-side object; a generic top-level ``confirmed``
        # field is deliberately not accepted as an equivalent marker.
        right_side = facts.get("ma520_right_side")
        if isinstance(right_side, Mapping) and any(
            _bool_value(right_side.get(key)) is True
            for key in ("second_wave_restart", "trend_reversal_confirmed", "confirmed")
        ):
            return True

    # The current A3 schema does not project a top-level
    # ``trend_reversal_confirmed`` field.  Do not accept arbitrary flattened
    # markers here: adding such a compatibility path would let an old caller
    # bypass the frozen A3 contract without an explicitly supported schema.
    return False


def _leader_context(plan: Mapping[str, Any]) -> Mapping[str, Any] | None:
    facts = _lookup(plan, ("strategy_facts",))
    if isinstance(facts, Mapping):
        merged_facts = dict(facts)
        merged_facts.update(plan)
        if any(
            key in merged_facts
            for key in (
                "leader_identity",
                "leader_code",
                "theme_stage",
                "ladder_intact",
                "ladder_broken",
                "ladder",
                "board_count",
                "ladder_height",
            )
        ):
            return merged_facts
    # Some callers flatten the A3 decision and omit ``strategy_facts``.  A
    # bare market role is not sufficient context; require a theme or ladder
    # fact before accepting the flattened shape.
    if any(
        key in plan
        for key in (
            "leader_identity",
            "leader_code",
            "theme_stage",
            "ladder_intact",
            "ladder_broken",
            "ladder",
            "board_count",
            "ladder_height",
        )
    ):
        return plan
    for path in (("leader_context",), ("ladder_context",), ("sector_context",), ("theme_context",), ("a2_context",)):
        value = _lookup(plan, path)
        if isinstance(value, Mapping):
            # A sector context is accepted only when it carries an explicit
            # leader/ladder signal; arbitrary sector text is not enough.
            if not any(
                key in value
                for key in (
                    "leader_identity",
                    "leader_code",
                    "market_role",
                    "ladder_intact",
                    "ladder_broken",
                    "ladder",
                    "theme_stage",
                    "stage",
                    "board_count",
                    "consecutive_boards",
                    "ladder_height",
                )
            ):
                continue
            return value
    return None


def _context_expired(context: Mapping[str, Any], as_of: Any) -> bool:
    if _bool_value(_lookup(context, ("valid",), ("current",))) is False:
        return True
    limit = _parse_datetime(_lookup(context, ("valid_until",), ("expires_at",)))
    current = as_of if isinstance(as_of, datetime) else None
    return limit is not None and current is not None and current > limit


def _fifteen_not_weak(latest: _Bar, previous: _Bar | None) -> bool:
    if latest.close + _EPSILON < latest.open:
        return False
    return previous is None or latest.close + _EPSILON >= previous.close


def _leader_divergence(five: Sequence[_Bar], vwap: float | None) -> bool:
    if len(five) < 2:
        return False
    latest, previous = five[-1], five[-2]
    if latest.close + _EPSILON < latest.open or latest.close + _EPSILON < previous.close:
        return False
    if len(five) >= 3:
        prior = five[-3]
        return previous.close <= prior.close + _EPSILON and latest.close > previous.close + _EPSILON
    return vwap is None or latest.close >= vwap


def _reseal_bar(latest: _Bar, previous: _Bar | None) -> bool:
    return previous is not None and latest.close >= previous.high - _EPSILON and latest.close >= latest.open


def _first_15m_breakout(five: Sequence[_Bar], fifteen: Sequence[_Bar]) -> bool:
    if len(five) < 4 or not fifteen:
        return False
    first = fifteen[0]
    latest = five[-1]
    return latest.end > first.end and latest.close > first.high + _EPSILON


def _high_volume_upper_shadow(latest: _Bar, five: Sequence[_Bar]) -> bool:
    bar_range = latest.high - latest.low
    if bar_range <= _EPSILON:
        return False
    upper = latest.high - max(latest.open, latest.close)
    if upper / bar_range < 0.55:
        return False
    if len(five) < 4:
        return False
    baseline = _median(item.volume for item in five[:-1])
    return baseline is not None and baseline > 0 and latest.volume >= baseline * 2.0


def _520_confirmations(five: Sequence[_Bar]) -> int:
    if not five:
        return 0
    count = 0
    for item in reversed(five):
        if item.close + _EPSILON >= item.open:
            count += 1
        else:
            break
    return count


def _volume_not_overheated(plan: Mapping[str, Any], five: Sequence[_Bar]) -> tuple[bool, str]:
    explicit = _lookup(plan, ("volume_overheated",), ("intraday", "volume_overheated"))
    if _bool_value(explicit) is True:
        return False, "VOLUME_OVERHEATED"
    latest = five[-1]
    ratio = _number(_lookup(plan, ("volume_ratio",), ("intraday", "volume_ratio")))
    threshold = _number(_lookup(plan, ("volume_overheat_ratio",), ("intraday", "volume_overheat_ratio"))) or 2.5
    if ratio is not None and ratio > threshold:
        return False, "VOLUME_OVERHEATED"
    baseline = _median(item.volume for item in five[:-1])
    if baseline is not None and baseline > 0 and latest.volume > baseline * threshold:
        return False, "VOLUME_OVERHEATED"
    return True, "VOLUME_NOT_OVERHEATED"


def _price_zone_met(plan: Mapping[str, Any], price: float) -> bool:
    zone = _lookup(plan, ("trigger_zone",), ("entry_reference_zone",), ("entry_zone",), ("pullback_zone",), ("strategy_facts", "entry_reference_zone"))
    if not isinstance(zone, Mapping):
        low = _number(_lookup(plan, ("trigger_low",),))
        high = _number(_lookup(plan, ("trigger_high",),))
    else:
        low = _number(_lookup(zone, ("low",), ("min",)))
        high = _number(_lookup(zone, ("high",), ("max",)))
    return low is not None and high is not None and low <= price <= high


def _locked_limit_up(plan: Mapping[str, Any], current: _Bar | None, latest5: _Bar) -> tuple[bool, float | None]:
    explicit = _lookup(plan, ("locked_limit_up",), ("limit_up_locked",), ("is_locked_limit_up",), ("strategy_facts", "locked"))
    upper = _number(_lookup(plan, ("upper_limit",), ("limit_up",), ("price_limits", "upper"), ("price_limit", "up")))
    if _bool_value(explicit) is True:
        return True, upper
    if upper is None or current is None:
        return False, upper
    tolerance = max(0.01, upper * 0.001)
    locked = current.low >= upper - tolerance and current.high >= upper - tolerance and current.close >= upper - tolerance and latest5.low >= upper - tolerance
    return locked, upper


def _plan_invalidated(plan: Mapping[str, Any]) -> bool:
    return _bool_value(_lookup(plan, ("plan_invalidated",), ("invalidated",), ("cancelled",))) is True


def _position_open(plan: Mapping[str, Any]) -> bool:
    explicit = _lookup(plan, ("position_open",), ("has_position",), ("holding",), ("in_position",))
    if explicit is not None:
        return _bool_value(explicit) is True
    for path in (("position",), ("virtual_position",), ("current_position",)):
        value = _lookup(plan, path)
        if isinstance(value, Mapping):
            quantity = _number(_lookup(value, ("qty",), ("quantity",), ("sellable_qty",)))
            return quantity is not None and quantity > 0
    quantity = _number(_lookup(plan, ("position_qty",), ("quantity",), ("sellable_qty",)))
    return quantity is not None and quantity > 0


def _sellable(plan: Mapping[str, Any]) -> bool:
    value = _lookup(plan, ("sellable_qty",), ("position", "sellable_qty"), ("position", "quantity"), ("position_qty",))
    if value is None:
        return True
    quantity = _number(value)
    return quantity is not None and quantity > 0


def _profitable_position(plan: Mapping[str, Any], current: float) -> bool:
    explicit = _lookup(plan, ("position_profitable",), ("profitable",), ("position", "profitable"))
    if explicit is not None:
        return _bool_value(explicit) is True
    entry = _number(_lookup(plan, ("entry_price",), ("position", "entry_price"), ("position", "avg_price")))
    return entry is not None and current > entry + _EPSILON


def _requested_action(plan: Mapping[str, Any]) -> A4Action:
    raw = str(_lookup(plan, ("action",), ("requested_action",)) or A4Action.BUY_SIGNAL.value).upper()
    try:
        action = A4Action(raw)
    except ValueError:
        action = A4Action.BUY_SIGNAL
    if action in {A4Action.NO_ACTION, A4Action.START_CONFIRMATION, A4Action.DATA_BLOCK, A4Action.FORCED_RISK_EXIT}:
        return A4Action.BUY_SIGNAL
    return action


def _plan_data_gap(plan: Mapping[str, Any]) -> str | None:
    if _bool_value(
        _lookup(
            plan,
            ("A3_ABLATION_MODE",),
            ("a3_ablation_mode",),
            ("strategy_facts", "A3_ABLATION_MODE"),
            ("strategy_facts", "a3_ablation_mode"),
        )
    ) is True:
        return "A3_ABLATION_MODE"
    for path in (("data_gap",), ("data_unavailable",)):
        value = _lookup(plan, path)
        if _bool_value(value) is True:
            return "PLAN_DATA_GAP"
    for path in (("a4_data_ready",), ("data", "ready")):
        value = _lookup(plan, path)
        if value is False:
            return "PLAN_DATA_GAP"
    value = _lookup(plan, ("data_gap_reason",), ("data", "reason_code"))
    if value:
        return _safe_reason(str(value), "PLAN_DATA_GAP")
    return None


def _sector_data_timestamp(plan: Mapping[str, Any]) -> datetime | None:
    """Find an explicit sector-fact timestamp; never use the bar timestamp."""

    for path in (
        ("sector_data_as_of",),
        ("sector_as_of",),
        ("sector_context", "data_as_of"),
        ("sector_context", "fact_as_of"),
        ("sector_context", "as_of"),
        ("sector_context", "updated_at"),
        ("sector_context", "observed_at"),
        ("sector_context", "timestamp"),
        ("market_context", "sector_data_as_of"),
        ("market_context", "sector_as_of"),
        ("strategy_facts", "sector_data_as_of"),
        ("strategy_facts", "sector_as_of"),
    ):
        parsed = _parse_timestamp(_lookup(plan, path))
        if parsed is not None:
            return parsed
    return None


def _resolve_as_of(plan: Mapping[str, Any], bars: Sequence[_Bar], as_of: datetime | None) -> datetime | None:
    value = as_of if as_of is not None else _lookup(plan, ("current_minute",), ("minute_end",), ("as_of",), ("current_time",))
    parsed = _parse_datetime(value)
    if value is not None:
        return parsed
    return bars[-1].end if bars else None


def _trade_date(plan: Mapping[str, Any]) -> date | None:
    value = _lookup(plan, ("trade_date",), ("market_trade_date",), ("target_trade_date",))
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    try:
        return date.fromisoformat(str(value)) if value else None
    except ValueError:
        return None


def _session_for(value: datetime) -> str | None:
    clock = value.astimezone(SHANGHAI).time().replace(tzinfo=None)
    if _SESSION_OPEN < clock <= _MORNING_CLOSE:
        return "AM"
    if _AFTERNOON_OPEN < clock <= _SESSION_CLOSE:
        return "PM"
    return None


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            return None
        return value.astimezone(SHANGHAI).replace(second=0, microsecond=0)
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(SHANGHAI).replace(second=0, microsecond=0)


def _parse_timestamp(value: Any) -> datetime | None:
    """Parse an explicit timestamp while retaining seconds precision.

    Naive values are rejected because assigning a timezone would be a guess.
    Numeric epoch seconds are accepted only when they are unambiguously in a
    modern Unix timestamp range.
    """

    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            return None
        return value.astimezone(SHANGHAI)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if 1_000_000_000 <= number <= 4_000_000_000:
            return datetime.fromtimestamp(number, tz=SHANGHAI)
        if 1_000_000_000_000 <= number <= 4_000_000_000_000:
            return datetime.fromtimestamp(number / 1000.0, tz=SHANGHAI)
        return None
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(SHANGHAI)


def _bar_dict(bar: _Bar, interval: str) -> dict[str, Any]:
    return {
        "symbol": bar.symbol,
        "interval": interval,
        "bar_end": bar.end.isoformat(),
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
        "amount": bar.amount,
    }


def _iso_end(bars: Sequence[_Bar]) -> str | None:
    return bars[-1].end.isoformat() if bars else None


def _vwap(bars: Sequence[_Bar]) -> float | None:
    volume = sum(item.volume for item in bars)
    amount = sum(item.amount for item in bars)
    if volume <= 0:
        return None
    return amount / volume


def _median(values: Iterable[float]) -> float | None:
    ordered = sorted(float(value) for value in values if math.isfinite(float(value)))
    if not ordered:
        return None
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) else None


def _bool_value(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "yes", "y", "1", "是", "有效", "intact"}:
            return True
        if text in {"false", "no", "n", "0", "否", "无效", "broken"}:
            return False
    return None


def _lookup(root: Any, *paths: tuple[str, ...]) -> Any:
    for path in paths:
        value = root
        found = True
        for key in path:
            if isinstance(value, Mapping):
                if key not in value:
                    found = False
                    break
                value = value[key]
            else:
                if not hasattr(value, key):
                    found = False
                    break
                value = getattr(value, key)
        if found and value is not None:
            return value
    return None


def _symbol_key(value: str) -> str:
    text = value.strip().upper()
    prefix = re.match(r"^(SHSE|SZSE|BJSE)\.(\d{6})$", text)
    if prefix:
        return f"{prefix.group(2)}.{ {'SHSE': 'SH', 'SZSE': 'SZ', 'BJSE': 'BJ'}[prefix.group(1)] }"
    if "." in text:
        code, exchange = text.split(".", 1)
        exchange = {"XSHG": "SH", "XSHE": "SZ", "SHSE": "SH", "SZSE": "SZ", "BJSE": "BJ"}.get(exchange, exchange)
        return f"{code}.{exchange}"
    return text


def _unique(values: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(str(value) for value in values if value))


def _safe_reason(value: str, fallback: str) -> str:
    value = re.sub(r"[^A-Z0-9_\-]", "_", value.upper())[:80]
    return value or fallback


__all__ = [
    "A4Action",
    "ACTIONS",
    "ActionValue",
    "STRATEGY_PROFILES",
    "StrategyProfile",
    "StrategyProfileValue",
    "StrategyEvaluation",
    "aggregate_closed_bars",
    "evaluate_a4",
    "evaluate_a4_plan",
    "evaluate_strategy",
]
