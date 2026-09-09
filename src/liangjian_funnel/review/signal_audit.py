"""Read-only, code-owned signal performance and entry evidence summaries.

Returns are price observations, never simulated realized P&L. No provider
request, execution, lifecycle mutation or retrospective strategy decision.
"""
from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime
from typing import Any, Mapping
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Shanghai")
ACTIONS = {"BUY_SIGNAL", "ADD_SIGNAL", "SELL_SIGNAL", "REDUCE_SIGNAL", "FORCED_RISK_EXIT"}


def mapping(value):
    if isinstance(value, Mapping):
        return dict(value)
    try:
        parsed = json.loads(value or "{}")
        return parsed if isinstance(parsed, dict) else {}
    except (ValueError, TypeError):
        return {}


def stamp(value):
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return result.astimezone(TZ) if result.tzinfo else result.replace(tzinfo=TZ)
    except (ValueError, TypeError):
        return None


def number(value):
    try:
        result = float(value)
        return result if not isinstance(value, bool) and math.isfinite(result) else None
    except (ValueError, TypeError):
        return None


def pct(value, base):
    value, base = number(value), number(base)
    return round((value / base - 1) * 100, 4) if value is not None and base and base > 0 else None


def percentage(value):
    return f"{value:+.2f}%" if value is not None else "资料不足"


def build_signal_stock_reviews(events, plans, fills, market, cutoff, *, lifecycles=()):
    """One row per effective event, exact signal-key fill join, cutoff-safe."""
    by_plan = {str(p.get("plan_id")): mapping(p.get("payload_json")) for p in plans}
    result = []
    for event in events:
        at = stamp(event.get("minute_end"))
        if not event.get("effective") or event.get("action") not in ACTIONS or not at or at.date() != cutoff.date() or at > cutoff:
            continue
        payload = mapping(event.get("payload_json"))
        strategy = mapping(payload.get("strategy"))
        plan = by_plan.get(str(payload.get("plan_id")), {})
        symbol = str(payload.get("symbol") or "")
        observed = mapping(market.get(symbol))
        bars = sorted([b for b in observed.get("bars", [])
                       if stamp(b.get("bar_end")) and stamp(b["bar_end"]).date() == cutoff.date()
                       and stamp(b["bar_end"]) <= cutoff and (number(b.get("close")) or 0) > 0
                       and ("09:31" <= stamp(b["bar_end"]).strftime("%H:%M") <= "11:30"
                            or "13:01" <= stamp(b["bar_end"]).strftime("%H:%M") <= "15:00")],
                      key=lambda b: stamp(b["bar_end"]))
        bars = list({stamp(b["bar_end"]): b for b in bars}.values())
        last = bars[-1] if bars else {}
        entry_signal = event.get("action") in {"BUY_SIGNAL", "ADD_SIGNAL"}
        matched = [f for f in fills if str(f.get("signal_id")) == str(event.get("event_key"))
                   and str(f.get("symbol")) == symbol and f.get("action") in {"BUY", "ADD"}
                   and stamp(f.get("bar_end")) and at < stamp(f["bar_end"]) <= cutoff]
        qty = sum(int(f.get("qty") or 0) for f in matched)
        price = sum(float(f["price"]) * int(f["qty"]) for f in matched) / qty if qty else None
        fee = sum(float(f.get("fee") or 0) for f in matched)
        entry_at = min((stamp(f["bar_end"]) for f in matched), default=None)
        reference = number(strategy.get("live_entry_price")) or number(strategy.get("reference_price"))
        after_signal = [b for b in bars if stamp(b["bar_end"]) > at]
        # Exclude the fill minute's high/low: intrabar ordering is unknown.
        after_entry = [b for b in bars if entry_at and stamp(b["bar_end"]) > entry_at]
        signal_return = pct(after_signal[-1]["close"], reference) if after_signal else None
        entry_return = pct(last.get("close"), price) if entry_at and stamp(last.get("bar_end")) and stamp(last["bar_end"]) >= entry_at else None
        highs = [number(b.get("high")) for b in after_entry if number(b.get("high")) is not None]
        lows = [number(b.get("low")) for b in after_entry if number(b.get("low")) is not None]
        confirmations = mapping(strategy.get("confirmation_results"))
        llm = payload.get("llm_reason_code") or strategy.get("llm_reason_code")
        failed = strategy.get("all_failed_confirmations") or strategy.get("unmet_conditions") or strategy.get("veto_conditions") or []
        rr, minimum = number(strategy.get("live_reward_risk")), number(strategy.get("minimum_reward_risk"))
        times = [stamp(strategy.get(key)) for key in ("closed_5m_end", "closed_15m_end")]
        checks = {
            "quant_confirmation_present": bool(confirmations),
            "quant_confirmation_values_pass": bool(confirmations) and all(
                isinstance(c, Mapping) and c.get("available") is True
                and (c.get("met") is False if c.get("kind") == "VETO" else c.get("met") is True)
                for c in confirmations.values()),
            "quant_no_failed_conditions": not bool(failed),
            "model_pass": llm == "LLM_PASS" and payload.get("llm_veto", strategy.get("llm_veto")) is False,
            "frozen_minute_snapshot_present": bool(payload.get("minute_snapshot_id")),
            "closed_windows_not_future": all(t is not None and t <= at for t in times),
            "reward_risk_pass": rr >= minimum if rr is not None and minimum is not None else None,
        }
        audit_state = "证据齐全" if all(v is True for v in checks.values()) else "需核对入场证据"
        fill_label = f"模拟成交{qty}股，均价{price:.4f}，买入费{fee:.2f}元" if qty else "截至复盘未匹配到模拟买入成交"
        terminal = next((l for l in lifecycles if l.get("entry_event_key") == event.get("event_key")
                         and l.get("status") == "UNFILLED" and stamp(l.get("updated_at"))
                         and stamp(l["updated_at"]) <= cutoff), None)
        unfilled_reason = terminal.get("exit_reason") if terminal and not qty else None
        reason_labels = {"PRICE_OUTSIDE_BAR": "模拟委托价格不在成交分钟价格范围内",
                         "INSUFFICIENT_CASH": "模拟账户可用资金不足"}
        if unfilled_reason:
            fill_label += "；" + reason_labels.get(unfilled_reason, "执行阻断，具体原因保留在审计证据中")
        if not entry_signal:
            audit_state, fill_label = "退出事件（非新入场）", "本条不作为买入成交或已实现收益"
        day_return = pct(last.get("close"), observed.get("previous_close"))
        performance = {"cutoff_at": cutoff.isoformat(), "price_as_of": last.get("bar_end"),
            "source": observed.get("source"), "previous_close": observed.get("previous_close"),
            "last_price": last.get("close"), "day_return_pct": day_return,
            "observed_day_high": max((number(b.get("high")) for b in bars if number(b.get("high")) is not None), default=None),
            "observed_day_low": min((number(b.get("low")) for b in bars if number(b.get("low")) is not None), default=None),
            "signal_return_pct": signal_return, "entry_mark_return_pct": entry_return,
            "post_entry_high_return_pct": pct(max(highs), price) if highs else None,
            "post_entry_low_return_pct": pct(min(lows), price) if lows else None,
            "observed_minute_count": len(bars), "expected_minute_count": observed.get("expected_minutes"),
            "coverage_complete": len(bars) == observed.get("expected_minutes") and bool(bars),
            "basis": "价格变化，未扣费；不是已实现收益；成交后极值不含成交分钟"}
        audit = {"state": audit_state, "checks": checks, "signal_at": at.isoformat(),
            "signal_reference_price": reference, "fill_at": entry_at.isoformat() if entry_at else None,
            "fill_qty": qty, "fill_price": price, "entry_fee": fee,
            "fill_ids": [f.get("fill_id") for f in matched], "fill_summary": fill_label,
            "unfilled_reason_code": unfilled_reason,
            "simulation_reference_from_plan": plan.get("trigger_low"),
            "account_ids": sorted({str(f.get("account_id")) for f in matched}),
            "live_reward_risk": rr, "minimum_reward_risk": minimum,
            "fill_vs_signal_price_pct": pct(price, reference),
            "closed_5m_end": strategy.get("closed_5m_end"), "closed_15m_end": strategy.get("closed_15m_end"),
            "market_gate": mapping(strategy.get("market_gate")),
            "stop_level": strategy.get("live_stop_level"), "target_price": strategy.get("live_target_price"),
            "confirmation_results": confirmations, "failed_conditions": failed,
            "llm_reason_code": llm, "minute_snapshot_id": payload.get("minute_snapshot_id"),
            "strategy_hash": hashlib.sha256(json.dumps(strategy, sort_keys=True, ensure_ascii=False).encode()).hexdigest(),
            "t1_note": "当日新买入股份受T+1限制，当日不可卖；退出信号不等于卖出成交" if qty else "未以信号推定持仓或收益",
            "scope": "核对已冻结的决策和成交记录，不代表全策略重新计算通过"}
        coverage = "完整分钟覆盖" if performance["coverage_complete"] else "分钟覆盖不完整或不可用"
        result.append({"event_id": event.get("event_id"), "plan_id": payload.get("plan_id"), "symbol": symbol,
            "name": plan.get("name") or plan.get("company_name") or "名称未提供",
            "strategy_profile": strategy.get("strategy_profile") or plan.get("strategy_profile"),
            "action": event.get("action"), "performance": performance, "entry_audit": audit,
            "performance_summary": f"较昨收{percentage(day_return)}；信号后{percentage(signal_return)}；成交价至观察价{percentage(entry_return)}；{coverage}",
            "entry_audit_summary": f"{at:%H:%M}，{audit_state}；{fill_label}；{audit['t1_note']}",
            "evidence_id": f"A5S:{event.get('event_id')}"})
    return result
