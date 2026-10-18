"""A4 model signals meet unchanged hard guards before local-paper dispatch.

Technical-reference met/unmet flags are deliberately NOT accepted as hard
guards here. Their explanations belong to the authored model schema. This
module owns neither provider freshness thresholds nor durable signal counts:
the frozen-source adapter supplies READY and atomically reserved daily counts.
1m hard exits are independent and must never wait for this 5m/model boundary.
"""
from dataclasses import dataclass
from datetime import datetime, time
from zoneinfo import ZoneInfo

from .risk_engine import RiskDecision, adjudicate_signal

_ORDERS = frozenset({'BUY_SIGNAL', 'ADD_SIGNAL', 'SELL_SIGNAL', 'REDUCE_SIGNAL'})
_NON_ORDERS = frozenset({'NO_ACTION', 'START_CONFIRMATION', 'CANCEL_SIGNAL', 'BLOCK'})
_LOCAL = ZoneInfo('Asia/Shanghai')


@dataclass(frozen=True)
class TraderGuardrailResult:
    status: str
    dispatch_allowed: bool
    reason_codes: tuple[str, ...]
    risk_decision: RiskDecision | None = None


def _aware(value):
    return isinstance(value, datetime) and value.utcoffset() is not None


def is_closed_trader_tick(value: datetime) -> bool:
    if not _aware(value): return False
    local = value.astimezone(_LOCAL)
    return (local.second == 0 and local.microsecond == 0 and local.minute % 5 == 0
            and (time(9, 35) <= local.time().replace(tzinfo=None) <= time(11, 30)
                 or time(13, 5) <= local.time().replace(tzinfo=None) <= time(15)))


def guard_trader_signal(*, signal_id: str, action: str, signal_plan_id: str, signal_symbol: str,
                        plan, envelope, account, positions, limits, now: datetime,
                        closed_5m_end: datetime, regime: str, live_market_ready: bool,
                        snapshot_ready: bool, schema_valid: bool, mode: str,
                        cluster_signals_today: int, batches_executed: int, calendar) -> TraderGuardrailResult:
    if not isinstance(mode, str) or mode not in {'record', 'control'}: raise ValueError('TRADER_MODE_INVALID')
    def block(code): return TraderGuardrailResult('BLOCKED', False, (code,))
    if type(schema_valid) is not bool or not schema_valid: return block('TRADER_SCHEMA_INVALID')
    if signal_plan_id != plan.plan_id or signal_symbol != plan.symbol:
        return block('TRADER_PLAN_BINDING_MISMATCH')
    if not isinstance(action, str) or action not in _ORDERS | _NON_ORDERS: return block('TRADER_ACTION_INVALID')
    if action in _NON_ORDERS:
        # Cancellation needs its own order-ID-bound path, not an entry/exit.
        return TraderGuardrailResult('NO_ORDER_ACTION', False, ())
    if type(snapshot_ready) is not bool or not snapshot_ready: return block('TRADER_SNAPSHOT_NOT_READY')
    if not _aware(now) or not is_closed_trader_tick(closed_5m_end): return block('TRADER_TICK_UNPROVEN')
    current = now.astimezone(_LOCAL); closed = closed_5m_end.astimezone(_LOCAL)
    try: trading_day = calendar.is_trading_day(current.date())
    except Exception: return block('TRADING_DAY_UNPROVEN')
    if trading_day is not True: return block('NON_TRADING_DAY')
    minute = current.replace(second=0, microsecond=0).time().replace(tzinfo=None)
    if not (time(9, 30) <= minute <= time(11, 30) or time(13) <= minute <= time(15)):
        return block('NO_TRADING_TIME')
    if closed.date() != current.date() or closed > current: return block('TRADER_TICK_UNPROVEN')
    latest = current.replace(minute=current.minute-current.minute % 5, second=0, microsecond=0)
    if closed != latest: return block('TRADER_TICK_NOT_CURRENT')
    if action in {'BUY_SIGNAL', 'ADD_SIGNAL'}:
        if type(live_market_ready) is not bool or not live_market_ready: return block('LIVE_MARKET_NOT_READY')
        if type(batches_executed) is not int or batches_executed < 0:
            return block('TRADER_BATCH_COUNT_UNPROVEN')
        if batches_executed >= envelope.max_batches: return block('TRADER_BATCH_CAP')
    decision = adjudicate_signal(signal_id, action, plan, envelope, account, positions, limits,
        now=now, regime=regime, cluster_signals_today=cluster_signals_today)
    if decision.status == 'REJECTED':
        return TraderGuardrailResult('BLOCKED', False, decision.reason_codes, decision)
    return TraderGuardrailResult('RECORD_ONLY' if mode == 'record' else 'APPROVED_FOR_LOCAL_PAPER',
                                 mode == 'control', decision.reason_codes, decision)
