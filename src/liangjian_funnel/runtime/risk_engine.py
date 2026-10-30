"""Pure, Decimal-based local-paper risk arithmetic. No model, broker or storage I/O.

Caps do not grant an execution permission. The caller must persist one decision
per immutable signal and reserve cash atomically with its paper order.
"""
from __future__ import annotations

from dataclasses import dataclass, field, fields, replace
from datetime import datetime, time
from decimal import Decimal, ROUND_FLOOR
from pathlib import Path
from zoneinfo import ZoneInfo
from collections.abc import Mapping

SHANGHAI = ZoneInfo("Asia/Shanghai")
_ZERO = Decimal("0")
PROBE_ONLY_TAGS = frozenset({'A2_UNAVAILABLE', 'A2_REVIEW_DEGRADED', 'PATTERN_B', 'SERVER_FALLBACK',
                           'PM_UNAVAILABLE_SERVER_FALLBACK', 'DATA_LIMITED', 'NEWS_DATA_LIMITED'})


def _field(value, name, default=None):
    return value.get(name, default) if isinstance(value, Mapping) else getattr(value, name, default)


def plan_risk_fields(payload):
    """Only top-level publication authority; nested scenario suggestions cannot upgrade it."""
    for key in PROBE_ONLY_TAGS:
        if key in payload and type(payload[key]) is not bool:
            raise ValueError('RISK_PLAN_CAP_INVALID')
    given_tags = payload.get('degradation_tags', ())
    if not isinstance(given_tags, (tuple, list)) or any(
            not isinstance(tag,str) or tag not in PROBE_ONLY_TAGS for tag in given_tags):
        raise ValueError('RISK_PLAN_CAP_INVALID')
    reasons = payload.get('reason_codes', ())
    if not isinstance(reasons, (tuple, list)) or any(not isinstance(code, str) for code in reasons):
        raise ValueError('RISK_PLAN_CAP_INVALID')
    tags = set(given_tags)
    tags.update(key for key in PROBE_ONLY_TAGS if payload.get(key) is True)
    tags.update(set(reasons) & PROBE_ONLY_TAGS)
    return dict(risk_unit=payload.get('risk_unit', 'STANDARD'), degradation_tags=tuple(sorted(tags)))


def effective_risk_unit(plan, a6=None, pm=None, a4=None):
    """Single monotone hard cap shared by planning, allocation and final quantity admission."""
    units = [_field(plan, 'risk_unit', _field(plan, 'plan_risk_unit', 'STANDARD'))]
    if set(_field(plan, 'degradation_tags', ())) & PROBE_ONLY_TAGS:
        units.append('PROBE')
    for value in (a6, pm, a4):
        if value is not None:
            units.append(value if isinstance(value, str) else _field(value, 'risk_unit'))
    if any(unit not in {'NONE', 'PROBE', 'STANDARD'} for unit in units):
        raise ValueError('RISK_UNIT_CAP_INVALID')
    return min(units, key={'NONE': 0, 'PROBE': 1, 'STANDARD': 2}.__getitem__)


def clamp_entry_quantity(*,unit,nav,price,stop_price,risk_budget,symbol_value,
                         position_pct,other_caps):
    """Single PROBE nominal/risk/position clamp used at every sizing boundary.

    Existing inventory and queued BUY reservations consume nominal room. Never
    resize old lots or infer a sell from a newly tighter entry cap.
    """
    nav,price,stop_price,risk_budget,symbol_value,position_pct=map(_number,
        (nav,price,stop_price,risk_budget,symbol_value,position_pct))
    if unit not in {'NONE','PROBE','STANDARD'}:raise ValueError('RISK_UNIT_CAP_INVALID')
    distance=price-stop_price
    raw=risk_budget/distance if distance>0 else _ZERO
    risk_qty=_lots(raw)
    caps={key:max(0,int(value)) for key,value in other_caps.items()}
    caps['POSITION_CAP']=_lots(max(_ZERO,nav*position_pct-symbol_value)/price) if price>0 else 0
    probe=_lots(max(_ZERO,nav*Decimal('0.10')-symbol_value)/price) if price>0 else 0
    if unit=='PROBE':caps['PROBE_NOTIONAL_CAP']=probe
    qty=min(risk_qty,*caps.values()) if unit!='NONE' else 0
    binding=tuple(key for key,value in caps.items() if value==qty and value<risk_qty)
    audit=dict(nav=str(nav),price=str(price),stop_price=str(stop_price),risk_budget=str(risk_budget),
        symbol_value=str(symbol_value),position_pct=str(position_pct),risk_unit=unit,
        raw_risk_qty=str(raw),risk_round_lot_qty=risk_qty,cap20_qty=caps['POSITION_CAP'],
        probe10_qty=probe if unit=='PROBE' else None,final_qty=qty,binding_limits=list(binding),
        other_caps={key:int(value) for key,value in other_caps.items()})
    return qty,audit


def _cap_nominal(envelope,unit,*,plan=None,limits=None):
    basis=envelope.quantity_audit
    if not basis:return envelope  # historical projection; final admission rebuilds fresh below
    limits=limits or RiskLimits()
    qty,audit=clamp_entry_quantity(unit=unit,nav=basis['nav'],price=plan.price if plan else basis['price'],
        stop_price=plan.stop_price if plan else basis['stop_price'],risk_budget=envelope.risk_budget,
        symbol_value=basis['symbol_value'],position_pct=limits.max_position_pct,
        other_caps={**basis['other_caps'],'UPSTREAM_CAP':envelope.max_shares_cap})
    if (qty>=envelope.max_shares_cap and basis.get('risk_unit')==unit
            and _number(basis['price'])==_number(audit['price'])
            and _number(basis['stop_price'])==_number(audit['stop_price'])
            and _number(basis['risk_budget'])==envelope.risk_budget):
        return envelope
    audit={**basis,**audit}
    eligible=envelope.eligible and qty>0
    return replace(envelope,max_shares_cap=min(envelope.max_shares_cap,qty),quantity_audit=audit,
        eligible=eligible,risk_unit=envelope.risk_unit if eligible else 'NONE',
        blocked_reasons=envelope.blocked_reasons if qty>0 else tuple(dict.fromkeys(
            (*envelope.blocked_reasons,'PROBE_NOTIONAL_CAP' if unit=='PROBE' else 'ROUND_LOT_UNAVAILABLE'))),
        binding_limits=tuple(dict.fromkeys((*envelope.binding_limits,*audit['binding_limits']))))


def cap_risk_envelope(envelope, *, plan=None, a6=None, pm=None, a4=None, limits=None,
                      conservative_quantity=False):
    """Apply the common cap once, retaining provenance in the existing journal payloads."""
    authority = plan if plan is not None else dict(risk_unit=envelope.plan_risk_unit,
        degradation_tags=envelope.degradation_tags)
    unit = effective_risk_unit(authority, envelope.risk_unit, pm, a4)
    unit = effective_risk_unit(dict(risk_unit=envelope.plan_risk_unit,
        degradation_tags=envelope.degradation_tags), unit)
    if a6 is not None:
        unit = effective_risk_unit(authority, unit, a6)
    if unit == 'NONE':
        return replace(envelope,eligible=False,risk_unit='NONE',risk_budget=_ZERO,max_shares_cap=0,
            blocked_reasons=envelope.blocked_reasons or ('ENVELOPE_INELIGIBLE',))
    if unit != 'PROBE' or envelope.risk_unit != 'STANDARD':
        return _cap_nominal(envelope,unit,plan=plan,limits=limits)
    limits = limits or RiskLimits()
    factor = limits.probe_multiplier
    if envelope.high_conviction and not envelope.a6_tightened:
        factor *= limits.standard_risk_pct / limits.max_risk_pct
    budget = envelope.risk_budget * factor
    # Preserve every original quantity ceiling; only the risk-distance ceiling tightens.
    shares = _lots(Decimal(envelope.max_shares_cap) * factor)
    if plan is not None and plan.price > plan.stop_price:
        shares = min(shares if conservative_quantity else envelope.max_shares_cap,
                     _lots(budget / (plan.price-plan.stop_price)))
    sources = []
    if _field(authority, 'risk_unit', 'STANDARD') == 'PROBE': sources.append('plan.risk_unit')
    sources.extend('plan.'+tag for tag in sorted(_field(authority, 'degradation_tags', ())))
    if envelope.plan_risk_unit == 'PROBE': sources.append('envelope.plan_risk_unit')
    sources.extend('envelope.'+tag for tag in envelope.degradation_tags)
    for key, value in (('a6', a6), ('pm', pm), ('a4', a4)):
        if value is not None and (value if isinstance(value, str) else _field(value, 'risk_unit')) == 'PROBE':
            sources.append(key+'.risk_unit')
    eligible = envelope.eligible and shares > 0
    changed=replace(envelope, risk_unit='PROBE' if eligible else 'NONE', eligible=eligible,
        risk_budget=budget, max_shares_cap=shares if eligible else 0,
        risk_cap_audit=tuple(dict.fromkeys((*envelope.risk_cap_audit, 'CLAMPED_TO_PROBE', *sources))),
        blocked_reasons=envelope.blocked_reasons if shares else tuple(dict.fromkeys(
            (*envelope.blocked_reasons, 'ROUND_LOT_UNAVAILABLE'))))
    return _cap_nominal(changed,unit,plan=plan,limits=limits)


def _number(value) -> Decimal:
    if isinstance(value, bool):
        raise ValueError("RISK_NUMBER_INVALID")
    try:
        result = Decimal(str(value))
    except Exception as exc:
        raise ValueError("RISK_NUMBER_INVALID") from exc
    if not result.is_finite() or result < 0:
        raise ValueError("RISK_NUMBER_INVALID")
    return result


def _aware(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("RISK_TIMESTAMP_INVALID")
    return value.astimezone(SHANGHAI)


def _lots(value: Decimal) -> int:
    return max(0, int((value / 100).to_integral_value(rounding=ROUND_FLOOR)) * 100)


@dataclass(frozen=True)
class RiskLimits:
    initial_capital: Decimal = Decimal("1000000")
    standard_risk_pct: Decimal = Decimal("0.03")
    max_risk_pct: Decimal = Decimal("0.05")
    probe_multiplier: Decimal = Decimal("0.5")
    max_position_pct: Decimal = Decimal("0.20")
    max_cluster_pct: Decimal = Decimal("0.40")
    max_positions: int = 6
    max_new_entries: int = 3
    max_cluster_signals: int = 2
    daily_loss_halt_pct: Decimal = Decimal("0.02")
    drawdown_half_pct: Decimal = Decimal("0.08")
    drawdown_halt_pct: Decimal = Decimal("0.15")
    min_cash_reserve_pct: Decimal = Decimal("0.10")
    no_new_entry_after: str = "14:30"

    def __post_init__(self):
        for field in fields(self):
            value = getattr(self, field.name)
            if field.name in {"max_positions", "max_new_entries", "max_cluster_signals"}:
                if type(value) is not int or value <= 0:
                    raise ValueError("RISK_LIMIT_INVALID")
            elif field.name == "no_new_entry_after":
                try: time.fromisoformat(value)
                except (ValueError, TypeError) as exc: raise ValueError("RISK_LIMIT_INVALID") from exc
            else:
                amount = _number(value)
                if amount <= 0 or (field.name != "initial_capital" and amount > 1):
                    raise ValueError("RISK_LIMIT_INVALID")
                object.__setattr__(self, field.name, amount)
        if (self.standard_risk_pct > self.max_risk_pct or self.drawdown_half_pct >= self.drawdown_halt_pct
                or self.max_position_pct > self.max_cluster_pct):
            raise ValueError("RISK_LIMIT_ORDER_INVALID")

    @classmethod
    def from_yaml(cls, path: str | Path) -> "RiskLimits":
        import yaml
        data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        if isinstance(data,dict) and 'control_first_day' in data:
            from .team_v2_first_day import validate_first_day_policy
            validate_first_day_policy(data.pop('control_first_day'))
        if isinstance(data,dict) and 'external_adjustment' in data:
            from .team_v2_external_policy import validate_external_policy
            validate_external_policy(data.pop('external_adjustment'))
        if isinstance(data,dict) and 'exception_triggers' in data:
            from .team_v2_exception_policy import validate_exception_policy
            validate_exception_policy(data.pop('exception_triggers'))
        if not isinstance(data, dict) or set(data) != {f.name for f in fields(cls)}:
            raise ValueError("RISK_LIMIT_KEYS_INVALID")
        return cls(**data)


@dataclass(frozen=True)
class AccountRisk:
    nav: Decimal
    cash: Decimal
    day_open_nav: Decimal
    peak_nav: Decimal
    frozen_cash: Decimal = _ZERO
    new_entries_today: int = 0
    hard_halt: bool = False

    def __post_init__(self):
        for key in ("nav", "cash", "day_open_nav", "peak_nav", "frozen_cash"):
            object.__setattr__(self, key, _number(getattr(self, key)))
        if self.day_open_nav <= 0 or self.peak_nav <= 0 or type(self.new_entries_today) is not int or self.new_entries_today < 0:
            raise ValueError("RISK_ACCOUNT_INVALID")
        if type(self.hard_halt) is not bool:
            raise ValueError("RISK_ACCOUNT_INVALID")


@dataclass(frozen=True)
class PositionRisk:
    plan_id: str
    symbol: str
    cluster: str
    qty: int
    sellable_qty: int
    market_value: Decimal
    avg_cost: Decimal = _ZERO

    def __post_init__(self):
        if (type(self.qty) is not int or type(self.sellable_qty) is not int
                or not 0 <= self.sellable_qty <= self.qty):
            raise ValueError("RISK_POSITION_INVALID")
        for key in ("market_value", "avg_cost"):
            object.__setattr__(self, key, _number(getattr(self, key)))


@dataclass(frozen=True)
class PlanRisk:
    plan_id: str
    symbol: str
    cluster: str
    status: str
    valid_from: datetime | None
    expires_at: datetime
    price: Decimal
    stop_price: Decimal
    entry_low: Decimal
    entry_high: Decimal
    no_chase: Decimal
    invalidation_price: Decimal | None = None
    tradable: bool = True
    data_ready: bool = True
    max_batches: int = 1
    risk_unit: str = 'STANDARD'
    degradation_tags: tuple[str, ...] = ()
    daily_atr14: Decimal | None = None

    def __post_init__(self):
        if (self.risk_unit not in {'STANDARD', 'PROBE'} or not isinstance(self.degradation_tags,(tuple,list))
                or any(tag not in PROBE_ONLY_TAGS for tag in self.degradation_tags)):
            raise ValueError('RISK_PLAN_CAP_INVALID')
        object.__setattr__(self, 'degradation_tags', tuple(sorted(set(self.degradation_tags))))
        for key in ("price", "stop_price", "entry_low", "entry_high", "no_chase"):
            object.__setattr__(self, key, _number(getattr(self, key)))
        if self.invalidation_price is not None:
            object.__setattr__(self, "invalidation_price", _number(self.invalidation_price))
        if self.daily_atr14 is not None:
            object.__setattr__(self,'daily_atr14',_number(self.daily_atr14))
        if self.valid_from is not None:
            _aware(self.valid_from)
        elif self.status != 'PENDING_MORNING_REVIEW':
            raise ValueError('RISK_PLAN_ACTIVATION_TIME_MISSING')
        _aware(self.expires_at)
        if (type(self.max_batches) is not int or self.max_batches < 1
                or type(self.tradable) is not bool or type(self.data_ready) is not bool):
            raise ValueError("RISK_PLAN_INVALID")


@dataclass(frozen=True)
class RiskEnvelope:
    plan_id: str
    symbol: str
    trade_date: str
    eligible: bool
    risk_unit: str
    risk_budget: Decimal
    max_batches: int
    max_shares_cap: int
    blocked_reasons: tuple[str, ...]
    binding_limits: tuple[str, ...]
    a6_tightened: bool = False
    high_conviction: bool = False
    purpose: str = "EXECUTION"
    plan_risk_unit: str = 'STANDARD'
    degradation_tags: tuple[str, ...] = ()
    risk_cap_audit: tuple[str, ...] = ()
    quantity_audit: dict = field(default_factory=dict)

    def __post_init__(self):
        for key in ('blocked_reasons', 'binding_limits', 'degradation_tags', 'risk_cap_audit'):
            object.__setattr__(self, key, tuple(getattr(self, key)))
        if self.plan_risk_unit not in {'STANDARD', 'PROBE'} or any(
                tag not in PROBE_ONLY_TAGS for tag in self.degradation_tags):
            raise ValueError('RISK_ENVELOPE_PLAN_CAP_INVALID')


@dataclass(frozen=True)
class RiskDecision:
    signal_id: str
    plan_id: str
    status: str
    shares: int
    reason_codes: tuple[str, ...]
    risk_unit: str
    quantity_audit: dict = field(default_factory=dict)


def build_risk_envelope(plan: PlanRisk, account: AccountRisk, positions: tuple[PositionRisk, ...],
                        limits: RiskLimits, *, now: datetime, regime: str,
                        cluster_signals_today: int = 0, high_conviction: bool = False,
                        a6_tightened: bool = False, action: str = "BUY_SIGNAL") -> RiskEnvelope:
    return _build_envelope(plan, account, positions, limits, now=now, regime=regime,
        cluster_signals_today=cluster_signals_today, high_conviction=high_conviction,
        a6_tightened=a6_tightened, action=action, purpose="EXECUTION")


def build_planning_envelope(plan: PlanRisk, account: AccountRisk, positions: tuple[PositionRisk, ...],
                            limits: RiskLimits, *, now: datetime, regime: str,
                            cluster_signals_today: int = 0, high_conviction: bool = False,
                            a6_tightened: bool = False, window_authorization=None,
                            window_source=None) -> RiskEnvelope:
    """Prospective capacity for the morning meeting, NEVER a trading permission.

    No plan is activated or repriced. Reference-price size is provisional;
    execution must build a fresh current-quote envelope after morning review.
    """
    planning_deadline=None
    if window_authorization is not None:
        from .morning_window_authority import morning_deadline
        if window_source is None:raise ValueError('PLANNING_MORNING_AUTHORITY_SOURCE_REQUIRED')
        planning_deadline=morning_deadline(now.date().isoformat(),window_source['source_trade_date'],
            authorization=window_authorization,source=window_source,now=now)
    return _build_envelope(plan, account, positions, limits, now=now, regime=regime,
        cluster_signals_today=cluster_signals_today, high_conviction=high_conviction,
        a6_tightened=a6_tightened, action="BUY_SIGNAL", purpose="PLANNING",planning_deadline=planning_deadline)


def _build_envelope(plan, account, positions, limits, *, now, regime, cluster_signals_today,
                     high_conviction, a6_tightened, action, purpose,planning_deadline=None):
    if type(high_conviction) is not bool or type(a6_tightened) is not bool:
        raise ValueError("RISK_FLAG_INVALID")
    now = _aware(now); reasons: list[str] = []
    planning = purpose == "PLANNING"
    existing = [p for p in positions if p.symbol == plan.symbol and p.qty > 0]
    allowed_statuses = {"ACTIVE_TODAY", "PENDING_MORNING_REVIEW"} if planning else {"ACTIVE_TODAY"}
    if plan.status not in allowed_statuses: reasons.append("ENVELOPE_INELIGIBLE")
    if now >= _aware(plan.expires_at): reasons.append("PLAN_EXPIRED")
    if not planning and (plan.valid_from is None or now < _aware(plan.valid_from)): reasons.append("PLAN_NOT_YET_VALID")
    if _aware(plan.expires_at).date() != now.date(): reasons.append("PLAN_DATE_MISMATCH")
    if planning and plan.valid_from is not None and (_aware(plan.valid_from).date() != now.date() or plan.valid_from >= plan.expires_at):
        reasons.append("PLAN_DATE_MISMATCH")
    if planning and (now.time().replace(tzinfo=None) >= time(9, 30) if planning_deadline is None else now>=planning_deadline):
        reasons.append("PLANNING_WINDOW_CLOSED")
    if not plan.tradable: reasons.append("NOT_TRADABLE")
    if not plan.data_ready: reasons.append("DATA_NOT_READY")
    if not plan.cluster or plan.cluster == "UNKNOWN": reasons.append("CLUSTER_UNKNOWN")
    if (plan.invalidation_price is None or plan.price <= plan.stop_price
            or plan.price <= (plan.invalidation_price or _ZERO)
            or plan.entry_low > min(plan.entry_high, plan.no_chase)
            or (not planning and not plan.entry_low <= plan.price <= min(plan.entry_high, plan.no_chase))):
        reasons.append("PRICE_OUTSIDE_PLAN_GEOMETRY")
    if not planning and not (time(9, 30) <= now.time().replace(tzinfo=None) <= time(11, 30)
            or time(13) <= now.time().replace(tzinfo=None) < time.fromisoformat(limits.no_new_entry_after)):
        reasons.append("NO_NEW_ENTRY_TIME")
    if regime not in {"TREND_MAINLINE", "ROTATION_NO_MAINLINE", "REPAIR", "BEAR_RISK", "RISK_OFF_RETREAT", "ICE_POINT", "ICE_POINT_NO_NEW_ENTRY"}:
        reasons.append("MARKET_STATE_UNPROVEN")
    if regime in {"ICE_POINT", "ICE_POINT_NO_NEW_ENTRY"}: reasons.append("REGIME_NO_NEW_ENTRY")
    if account.hard_halt: reasons.append("DRAWDOWN_HALT")
    if account.nav <= account.day_open_nav * (1 - limits.daily_loss_halt_pct): reasons.append("DAILY_LOSS_HALT")
    if account.nav <= account.peak_nav * (1 - limits.drawdown_halt_pct): reasons.append("DRAWDOWN_HALT")
    if action not in {"BUY_SIGNAL", "ADD_SIGNAL"}: reasons.append("ENTRY_ACTION_INVALID")
    if action == "ADD_SIGNAL":
        if not existing: reasons.append("ADD_POSITION_MISSING")
        if any(p.avg_cost <= 0 or plan.price <= p.avg_cost for p in existing): reasons.append("LOSS_ADD_FORBIDDEN")
    if action == "BUY_SIGNAL" and existing: reasons.append("POSITION_ALREADY_OPEN")
    if not existing and account.new_entries_today >= limits.max_new_entries: reasons.append("DAILY_ENTRY_CAP")
    if not existing and len({p.symbol for p in positions if p.qty > 0}) >= limits.max_positions: reasons.append("POSITION_SLOT_CAP")
    if type(cluster_signals_today) is not int or cluster_signals_today < 0:
        reasons.append("CLUSTER_COUNT_UNPROVEN")
    elif cluster_signals_today >= limits.max_cluster_signals: reasons.append("CLUSTER_SIGNAL_CAP")
    regime_unit = "PROBE" if regime in {"BEAR_RISK", "RISK_OFF_RETREAT"} else "STANDARD"
    unit = effective_risk_unit(plan, regime_unit)
    pct = limits.max_risk_pct if high_conviction and not a6_tightened and unit == "STANDARD" else limits.standard_risk_pct
    risk_budget = account.nav * pct * (limits.probe_multiplier if unit == "PROBE" else 1)
    if account.nav <= account.peak_nav * (1 - limits.drawdown_half_pct): risk_budget *= Decimal("0.5")
    risk_qty = _lots(risk_budget / (plan.price - plan.stop_price)) if plan.price > plan.stop_price else 0
    room = {
        "POSITION_CAP": account.nav * limits.max_position_pct - sum((p.market_value for p in existing), _ZERO),
        "CLUSTER_CAP": account.nav * limits.max_cluster_pct - sum((p.market_value for p in positions if p.cluster == plan.cluster), _ZERO),
        "CASH_RESERVE": account.cash - account.frozen_cash - account.nav * limits.min_cash_reserve_pct,
    }
    caps = {key: _lots(value / plan.price) if plan.price > 0 else 0 for key, value in room.items()}
    qty,quantity_audit=clamp_entry_quantity(unit=unit,nav=account.nav,price=plan.price,
        stop_price=plan.stop_price,risk_budget=risk_budget,
        symbol_value=sum((p.market_value for p in existing),_ZERO),position_pct=limits.max_position_pct,
        other_caps={key:value for key,value in caps.items() if key!='POSITION_CAP'})
    binding=quantity_audit['binding_limits']
    quantity_audit['daily_atr14']=str(plan.daily_atr14) if plan.daily_atr14 is not None else None
    tight=bool(plan.daily_atr14 is not None and 0<plan.price-plan.stop_price<plan.daily_atr14)
    quantity_audit['stop_distance_atr']=str((plan.price-plan.stop_price)/plan.daily_atr14) if plan.daily_atr14 else None
    quantity_audit['review_flags']=['STOP_TOO_TIGHT'] if tight else []
    if qty <= 0: reasons.extend(k for k, v in caps.items() if v <= 0); reasons.append("ROUND_LOT_UNAVAILABLE")
    return RiskEnvelope(plan.plan_id, plan.symbol, now.date().isoformat(), not reasons,
                        unit if not reasons else "NONE", risk_budget, plan.max_batches,
                        qty if not reasons else 0, tuple(dict.fromkeys(reasons)), binding,
                        a6_tightened, high_conviction, purpose, plan.risk_unit, plan.degradation_tags,
                        ('CLAMPED_TO_PROBE', *tuple(['plan.risk_unit'] if plan.risk_unit=='PROBE' else []),
                         *tuple('plan.'+tag for tag in plan.degradation_tags))
                        if unit=='PROBE' and regime_unit=='STANDARD' else (),quantity_audit)


def conservative_envelope(envelope: RiskEnvelope, *, consecutive_failures: int) -> RiskEnvelope:
    if type(consecutive_failures) is not int or consecutive_failures < 1:
        raise ValueError("A6_FAILURE_COUNT_INVALID")
    if consecutive_failures >= 2:
        return replace(envelope, eligible=False, risk_unit="NONE", max_shares_cap=0,
                       max_batches=1, a6_tightened=True,
                       blocked_reasons=tuple(dict.fromkeys((*envelope.blocked_reasons, "A6_DEGRADED_HALT"))))
    factor = Decimal("0.5") if envelope.risk_unit == "STANDARD" else Decimal("1")
    cap = _lots(Decimal(envelope.max_shares_cap) * factor)
    eligible = envelope.eligible and cap > 0
    return replace(envelope, eligible=eligible, risk_unit="PROBE" if eligible else "NONE", max_batches=1,
                   risk_budget=envelope.risk_budget * factor,
                   max_shares_cap=cap, a6_tightened=True,
                   blocked_reasons=envelope.blocked_reasons if cap else tuple(dict.fromkeys((*envelope.blocked_reasons,"ROUND_LOT_UNAVAILABLE"))))


def adjudicate_signal(signal_id: str, action: str, plan: PlanRisk, envelope: RiskEnvelope,
                      account: AccountRisk, positions: tuple[PositionRisk, ...], limits: RiskLimits,
                      *, now: datetime, regime: str, cluster_signals_today: int = 0) -> RiskDecision:
    def reject(*codes): return RiskDecision(signal_id, plan.plan_id, "REJECTED", 0, tuple(codes), "NONE")
    if not signal_id: return reject("SIGNAL_ID_MISSING")
    if envelope.purpose != "EXECUTION": return reject("PLANNING_ENVELOPE_NOT_EXECUTABLE")
    if envelope.plan_id != plan.plan_id or envelope.symbol != plan.symbol or envelope.trade_date != _aware(now).date().isoformat():
        return reject("ENVELOPE_BINDING_MISMATCH")
    if action in {"SELL_SIGNAL", "REDUCE_SIGNAL"}:
        if not plan.tradable: return reject("NOT_TRADABLE")
        if not plan.data_ready: return reject("DATA_NOT_READY")
        held = [p for p in positions if p.symbol == plan.symbol and p.plan_id == plan.plan_id]
        qty = sum(p.sellable_qty for p in held)
        if not qty: return reject("T1_LOCKED")
        # Never expand an exact position into a different plan or broker account.
        shares = qty if action == "SELL_SIGNAL" else max(1, qty // 2)
        return RiskDecision(signal_id, plan.plan_id, "APPROVED", shares, (), "NONE")
    fresh = build_risk_envelope(plan, account, positions, limits, now=now, regime=regime,
                               cluster_signals_today=cluster_signals_today, action=action,
                               high_conviction=envelope.high_conviction, a6_tightened=envelope.a6_tightened)
    upstream_cap=envelope.max_shares_cap
    envelope = cap_risk_envelope(envelope, plan=plan, limits=limits)
    if not envelope.eligible or not fresh.eligible:
        return reject(*(envelope.blocked_reasons or fresh.blocked_reasons or ("ENVELOPE_INELIGIBLE",)))
    risk_distance = plan.price - plan.stop_price
    risk_cap = _lots(min(_number(envelope.risk_budget), fresh.risk_budget) / risk_distance) if risk_distance > 0 else 0
    shares = min(envelope.max_shares_cap, fresh.max_shares_cap, risk_cap)
    if shares <= 0: return reject("ROUND_LOT_UNAVAILABLE")
    codes = tuple(dict.fromkeys((*fresh.binding_limits, *envelope.risk_cap_audit, *fresh.risk_cap_audit,
        *(("A6_RISK_BUDGET_CAP",) if risk_cap < min(upstream_cap, fresh.max_shares_cap) else ()))))
    return RiskDecision(signal_id, plan.plan_id, "REDUCED" if codes else "APPROVED",
                        shares, codes, effective_risk_unit(plan, envelope, fresh),
                        {**fresh.quantity_audit,'final_qty':shares,'binding_limits':list(codes)})
