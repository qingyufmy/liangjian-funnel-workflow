"""Direct Lark notifications for approved A3 plans and effective A4 events."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .lark import LarkConfigurationError, LarkNotifier
from .state import RuntimeStore


_ACTION_LABELS = {
    "BUY_SIGNAL": "买入触发",
    "ADD_SIGNAL": "加仓触发",
    "SELL_SIGNAL": "离场触发",
    "REDUCE_SIGNAL": "减仓触发",
    "LLM_VETO": "模型否决",
    "PLAN_INVALIDATED": "计划失效",
    "DATA_BLOCK": "数据阻断",
    "FORCED_RISK_EXIT": "强制风控离场",
}
_STRATEGY_LABELS = {
    "LEADER_INTRADAY": "龙头战法",
    "MA520_SWING": "520 均线波段",
    "TREND_MA5": "趋势 5 日线",
}


def _payload(row: Mapping[str, Any]) -> dict[str, Any]:
    try:
        value = json.loads(str(row.get("payload_json") or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _text(value: Any, *, limit: int = 300, fallback: str = "—") -> str:
    rendered = str(value or "").strip().replace("\r", " ").replace("\n", " ")
    return rendered[:limit] if rendered else fallback


def _items(value: Any, *, limit: int = 4) -> list[str]:
    if not isinstance(value, (list, tuple)):
        return []
    return [_text(item, limit=160) for item in value[:limit] if str(item or "").strip()]


def _number(value: Any) -> str:
    try:
        return f"{float(value):.3f}".rstrip("0").rstrip(".")
    except (TypeError, ValueError):
        return "—"


def _plan_sort_key(row: Mapping[str, Any]) -> tuple[int, str, str]:
    payload = _payload(row)
    rank = {"P1": 0, "P2": 1, "P3": 2}.get(
        str(payload.get("plan_priority") or "").strip().upper(),
        3,
    )
    return rank, str(row.get("symbol") or ""), str(row.get("plan_id") or "")


def _monthly_line(value: Mapping[str, Any]) -> str:
    name = _text(value.get("name") or value.get("code"), limit=30)
    strength = _number(value.get("relative_strength_percentile_20d"))
    try:
        return_5d = f"{float(value.get('return_5d')) * 100:.1f}%"
    except (TypeError, ValueError):
        return_5d = "—"
    return f"{name}(5日 {return_5d} / 强度分位 {strength})"


class WorkflowLarkPublisher:
    """Send synchronously, then persist only a safe delivery summary."""

    def __init__(
        self,
        store: RuntimeStore,
        webhook_url: Any,
        *,
        webhook_path: Path | None = None,
        timeout_seconds: float = 8.0,
    ):
        self.store = store
        self.webhook_path = webhook_path
        self.timeout_seconds = timeout_seconds
        self.configuration_reason: str | None = None
        try:
            self.notifier = LarkNotifier(webhook_url, timeout_seconds=timeout_seconds)
        except (LarkConfigurationError, ValueError) as exc:
            self.notifier = LarkNotifier(None, timeout_seconds=timeout_seconds)
            self.configuration_reason = str(getattr(exc, "reason_code", "LARK_CONFIGURATION_INVALID"))

    @property
    def enabled(self) -> bool:
        notifier, _ = self._resolve_notifier()
        return notifier.enabled

    def _resolve_notifier(self) -> tuple[LarkNotifier, str | None]:
        # Tests and explicit callers may inject a notifier. Production uses the
        # file path and reloads it for every send so a UI edit needs no restart.
        if self.webhook_path is None or self.notifier.enabled:
            return self.notifier, self.configuration_reason
        try:
            if not self.webhook_path.is_file() or self.webhook_path.stat().st_size > 4_096:
                return self.notifier, "LARK_WEBHOOK_NOT_CONFIGURED"
            payload = json.loads(self.webhook_path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict) or payload.get("schemaVersion") != 1:
                return self.notifier, "LARK_WEBHOOK_CONFIGURATION_INVALID"
            notifier = LarkNotifier(
                payload.get("webhookUrl"),
                timeout_seconds=self.timeout_seconds,
            )
            return notifier, None
        except (OSError, ValueError, TypeError, json.JSONDecodeError, LarkConfigurationError):
            return self.notifier, "LARK_WEBHOOK_CONFIGURATION_INVALID"

    def _send(
        self,
        *,
        delivery_key: str,
        kind: str,
        source_id: str,
        title: str,
        lines: Sequence[str],
        summary: Mapping[str, Any],
        now: datetime,
    ) -> dict[str, Any]:
        notifier, configuration_reason = self._resolve_notifier()
        if not notifier.enabled:
            return {"status": "DISABLED", "reason_code": configuration_reason or "LARK_WEBHOOK_NOT_CONFIGURED"}
        existing = self.store.get_delivery_by_key(delivery_key)
        if existing is not None:
            return {"status": str(existing["status"]), "duplicate": True, "delivery_id": existing["delivery_id"]}
        color = self.store.next_notification_color()
        result = notifier.send(title, lines, color)
        row, _ = self.store.record_delivery(
            delivery_key=delivery_key,
            kind=kind,
            source_id=source_id,
            title=title,
            status="SENT" if result.ok else "FAILED",
            color=color,
            attempt_count=result.attempts,
            last_reason_code=None if result.ok else result.reason_code,
            payload=summary,
            created_at=now,
            sent_at=now if result.ok else None,
        )
        return {
            "status": str(row["status"]),
            "delivery_id": str(row["delivery_id"]),
            "reason_code": row.get("last_reason_code"),
        }

    def publish_premarket(
        self,
        plans: Sequence[Mapping[str, Any]],
        *,
        reviewed_at: datetime,
        evidence: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        if not plans:
            return []
        ordered = sorted(plans, key=lambda row: (str(row.get("lane_id") or ""), str(row.get("symbol") or "")))
        plan_ids = [str(row.get("plan_id") or "") for row in ordered]
        batch_hash = hashlib.sha256("|".join(plan_ids).encode("utf-8")).hexdigest()[:16]
        plan_payloads = [_payload(row) for row in ordered]
        themes = []
        strategies = []
        for payload in plan_payloads:
            theme = _text(
                payload.get("theme") or payload.get("theme_name") or payload.get("industry"),
                limit=50,
                fallback="",
            )
            if theme and theme != "—" and theme not in themes:
                themes.append(theme)
            strategy = _STRATEGY_LABELS.get(str(payload.get("strategy_profile") or ""), "")
            if strategy and strategy not in strategies:
                strategies.append(strategy)
        outputs: list[dict[str, Any]] = []
        chunks = [ordered[index : index + 4] for index in range(0, len(ordered), 4)]
        for page, chunk in enumerate(chunks, start=1):
            lines = [
                f"**一、盘前执行结论** {reviewed_at.date().isoformat()} {reviewed_at.strftime('%H:%M')} 完成复核；{len(ordered)} 只进入 A4，本卡 {page}/{len(chunks)}。",
                f"**二、主线与适用策略** {'、'.join(themes[:5]) if themes else '以 A2/A3 已验证方向为准'}｜{'、'.join(strategies) if strategies else '按个股计划'}",
            ]
            symbols: list[str] = []
            source_ids: list[str] = []
            for row in chunk:
                payload = _payload(row)
                symbol = _text(row.get("symbol"), limit=20)
                name = _text(payload.get("name"), limit=30, fallback="名称未提供")
                symbols.append(symbol)
                source_ids.append(_text(payload.get("source_run_id"), limit=160, fallback=""))
                strategy = _STRATEGY_LABELS.get(str(payload.get("strategy_profile") or ""), _text(payload.get("strategy_profile"), limit=40))
                reasons = _items(payload.get("selection_reasons") or payload.get("reason_codes"), limit=2)
                conditions = _items(payload.get("required_conditions"), limit=3)
                auction_price = (evidence.get(symbol) or {}).get("price") if isinstance(evidence.get(symbol), Mapping) else None
                lines.extend(
                    [
                        f"---\n**三、A3 个股计划｜{name}｜{symbol}**　{strategy}",
                        f"**入选逻辑** {'；'.join(reasons) if reasons else '已通过 A3 确定性技术计划'}",
                        f"**日内计划** 触发 {_number(payload.get('trigger_low'))}–{_number(payload.get('trigger_high'))}｜止损 {_number(payload.get('stop_level'))}｜禁止追价 {_number(payload.get('no_chase_price') or payload.get('max_chase_price'))}",
                        f"**盘前价 / 条件** {_number(auction_price)}｜{'；'.join(conditions) if conditions else '按计划触发条件执行'}",
                    ]
                )
            lines.append("---\n**四、风险与纪律** 只在计划条件成立后执行；未触发不交易，超过禁止追价位不追，失效价触发即按风控处理。")
            source_id = next((item for item in source_ids if item and item != "—"), f"premarket-{reviewed_at.date().isoformat()}")
            outputs.append(
                self._send(
                    delivery_key=f"premarket:{reviewed_at.date().isoformat()}:{batch_hash}:{page}",
                    kind="PREMARKET_A3",
                    source_id=source_id,
                    title=f"A股盘前计划｜{reviewed_at.date().isoformat()}｜{page}/{len(chunks)}",
                    lines=lines,
                    summary={"trade_date": reviewed_at.date().isoformat(), "symbols": symbols, "plan_count": len(ordered), "page": page},
                    now=reviewed_at,
                )
            )
        return outputs

    def publish_a3_premarket_analysis(
        self,
        plans: Sequence[Mapping[str, Any]],
        *,
        analyzed_at: datetime,
        source_run_id: str | None = None,
        research_context: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Publish a professional read-only A1-A3 premarket brief.

        This is intentionally separate from :meth:`publish_premarket`, which
        is called by the 09:26 auction review after plans are activated.  No
        auction quote or current-session fact is accepted here.  The compact
        context was persisted by the source research run, so this task never
        reopens a large model audit or performs an implicit data sync.
        """

        if not plans:
            return []
        ordered = sorted(plans, key=_plan_sort_key)
        plan_ids = [str(row.get("plan_id") or "") for row in ordered]
        batch_hash = hashlib.sha256(
            f"{source_run_id or ''}|{'|'.join(plan_ids)}".encode("utf-8")
        ).hexdigest()[:16]
        context = dict(research_context) if isinstance(research_context, Mapping) else {}
        a1 = context.get("a1") if isinstance(context.get("a1"), Mapping) else {}
        a2 = context.get("a2") if isinstance(context.get("a2"), Mapping) else {}
        a3 = context.get("a3") if isinstance(context.get("a3"), Mapping) else {}
        macro = a1.get("macro") if isinstance(a1.get("macro"), Mapping) else {}
        constraints = a3.get("market_open_constraints") if isinstance(a3.get("market_open_constraints"), Mapping) else {}
        directions = _items(macro.get("policy_direction"), limit=4)
        uncertainties = _items(macro.get("key_uncertainties"), limit=4)
        monthly = [item for item in a1.get("monthly_industries", ()) if isinstance(item, Mapping)] if isinstance(a1.get("monthly_industries"), (list, tuple)) else []
        themes = [item for item in a2.get("active_themes", ()) if isinstance(item, Mapping)] if isinstance(a2.get("active_themes"), (list, tuple)) else []
        reason_codes = _items(context.get("reason_codes"), limit=6)
        context_lines = [
            f"**一、盘前总览** {analyzed_at.date().isoformat()} {analyzed_at.strftime('%H:%M')}｜A3 计划 {len(ordered)} 只｜上下文 {_text(context.get('status'), limit=30, fallback='UNAVAILABLE')}",
            f"**数据时点** 收盘事实 {_text(context.get('market_trade_date'), limit=20)}｜目标交易日 {_text(context.get('target_trade_date'), limit=20)}｜主模型 {_text(context.get('model'), limit=50)}",
            f"**A1 宏观** 流动性 {_text(macro.get('liquidity_condition'), limit=30)}｜盈利周期 {_text(macro.get('profit_cycle_position'), limit=30)}｜研究池 {_number(a1.get('active_count'))}",
            f"**政策方向** {'；'.join(directions) if directions else '未提供可核验方向'}",
            f"**月度行业前列** {'；'.join(_monthly_line(item) for item in monthly[:5]) if monthly else '未持久化'}",
            "---\n**二、A2 板块轮动 TOP**",
        ]
        if themes:
            for index, item in enumerate(themes[:5], start=1):
                context_lines.append(
                    f"**{index}. {_text(item.get('name') or item.get('theme_id'), limit=40)}**｜强度 {_number(item.get('score'))}｜周 {_text(item.get('weekly_state'), limit=24)}｜新开仓 {_text(item.get('new_entry_policy'), limit=24)}\n"
                    f"广度 {_number(item.get('breadth'))}｜资金 {_number(item.get('capital_flow'))}｜龙头 {_number(item.get('leader_structure'))}｜梯队 {_number(item.get('tier_structure'))}｜产业链共振 {_number(item.get('index_chain_resonance'))}｜追高风险 {_text(item.get('chase_risk_level'), limit=20)}"
                )
                contradiction = _items(item.get("contradicting_evidence"), limit=1)
                if contradiction:
                    context_lines.append(f"反证：{contradiction[0]}")
        else:
            context_lines.append("A2 主题上下文未持久化；此处空白不解释为市场无机会。")
        context_lines.extend(
            [
                "---\n**三、A1→A2→A3 决策链** A1 定月度研究方向；A2 以板块强度、资金、龙头、梯队与产业链共振确认轮动；A3 以日/周/月技术生成次日计划。",
                f"**昨日环境与仓位** {_text(constraints.get('prior_market_environment'), limit=40)}｜A3 建议仓位 {_number(constraints.get('recommended_position_min_pct'))}–{_number(constraints.get('recommended_position_max_pct'))}",
                "**权限边界** A3 不阻断次日计划；当日新开仓权限只由 A4 当前交易日实时市场状态判定。",
                f"**核心不确定性** {'；'.join(uncertainties) if uncertainties else '未提供'}",
                f"**上下文风险码** {'；'.join(reason_codes) if reason_codes else '无'}",
                "**事实边界** 未落库的24h新闻、竞价和实时龙虎榜不推演、不补造；09:26 独立竞价复核前不激活 A4。",
            ]
        )
        chunks = [ordered[index : index + 4] for index in range(0, len(ordered), 4)]
        outputs: list[dict[str, Any]] = [
            self._send(
                delivery_key=f"a3-premarket-professional-v2:{analyzed_at.date().isoformat()}:{batch_hash}:overview",
                kind="PREMARKET_A3_ANALYSIS",
                source_id=source_run_id or f"a3-premarket-{analyzed_at.date().isoformat()}",
                title=f"A股专业盘前研究｜{analyzed_at.date().isoformat()}｜总览",
                lines=context_lines,
                summary={
                    "trade_date": analyzed_at.date().isoformat(),
                    "source_run_id": source_run_id,
                    "plan_count": len(ordered),
                    "context_status": context.get("status"),
                    "market_trade_date": context.get("market_trade_date"),
                    "target_trade_date": context.get("target_trade_date"),
                    "card": "overview",
                },
                now=analyzed_at,
            )
        ]
        for page, chunk in enumerate(chunks, start=1):
            lines = [
                f"**四、A3 次日计划** {analyzed_at.date().isoformat()}｜本卡 {page}/{len(chunks)}｜共 {len(ordered)} 只。",
                f"**来源与边界** 批次 {_text(source_run_id, limit=160, fallback='未提供')}；09:26 前不读取竞价、不改变计划状态、不激活 A4。",
            ]
            symbols: list[str] = []
            for row in chunk:
                payload = _payload(row)
                symbol = _text(row.get("symbol"), limit=20)
                name = _text(payload.get("name"), limit=30, fallback="名称未提供")
                symbols.append(symbol)
                strategy = _STRATEGY_LABELS.get(
                    str(payload.get("strategy_profile") or ""),
                    _text(payload.get("strategy_profile"), limit=40, fallback="按 A3 技术计划"),
                )
                reasons = _items(payload.get("selection_reasons") or payload.get("reason_codes"), limit=4)
                conditions = _items(payload.get("required_conditions"), limit=4)
                invalidators = _items(
                    payload.get("overnight_invalidators")
                    or payload.get("invalidation_conditions")
                    or payload.get("invalidation_reasons")
                    or payload.get("veto_conditions"),
                    limit=4,
                )
                if not invalidators:
                    invalidators = [
                        "跌破日线失效价",
                        "开盘或盘中超过禁止追价位",
                        "必要条件未成立",
                    ]
                lines.extend(
                    [
                        f"---\n**{name}｜{symbol}**　{_text(payload.get('plan_priority'), limit=4)}｜{strategy}",
                        f"**类型 / 角色 / 主题** {_text(payload.get('stock_behavior_type'), limit=30)}｜{_text(payload.get('market_role'), limit=30)}｜{_text(payload.get('theme_name') or payload.get('theme') or payload.get('theme_id') or payload.get('industry'), limit=50)}",
                        f"**优先级依据** {'；'.join(_items(payload.get('priority_reasons'), limit=4)) or '按确定性计划成熟度'}",
                        f"**A1-A3 入选链** {'；'.join(reasons) if reasons else '已通过确定性日线技术计划'}",
                        f"**参考收盘** {_number(payload.get('reference_price'))}（{_text(payload.get('reference_price_as_of'), limit=32)}）｜**次日触发区** {_number(payload.get('trigger_low'))}–{_number(payload.get('trigger_high'))}",
                        f"**止损/失效** {_number(payload.get('stop_level') or payload.get('daily_invalidation'))}｜**禁止追价** {_number(payload.get('no_chase') or payload.get('no_chase_price') or payload.get('max_chase_price'))}｜**压力/减仓参考** {_number(payload.get('pressure_reduce_price'))}（{_text(payload.get('pressure_basis'), limit=32)}）",
                        f"**必要条件** {'；'.join(conditions) if conditions else '按 A3 计划条件，待 09:26 复核'}",
                        f"**隔夜失效项** {'；'.join(invalidators)}",
                        f"**三情景** 强：不超过禁追价仍等 A4 确认｜中：进入触发区且条件成立｜弱：跌破失效价则计划作废",
                    ]
                )
            lines.append("---\n**五、执行纪律** 未触发不交易；超过禁止追价位不追；跌破失效价不等待模型解释。仅用于本地模拟研究。")
            outputs.append(
                self._send(
                    delivery_key=f"a3-premarket-professional-v2:{analyzed_at.date().isoformat()}:{batch_hash}:plans:{page}",
                    kind="PREMARKET_A3_ANALYSIS",
                    source_id=source_run_id or f"a3-premarket-{analyzed_at.date().isoformat()}",
                    title=f"A股专业盘前研究｜A3计划｜{page}/{len(chunks)}",
                    lines=lines,
                    summary={
                        "trade_date": analyzed_at.date().isoformat(),
                        "source_run_id": source_run_id,
                        "symbols": symbols,
                        "plan_count": len(ordered),
                        "page": page,
                        "card": "plans",
                        "activation_deferred_to": "09:26",
                    },
                    now=analyzed_at,
                )
            )
        return outputs

    def publish_a4_events(
        self,
        events: Sequence[Mapping[str, Any]],
        *,
        plans: Mapping[str, Mapping[str, Any]],
        now: datetime,
    ) -> list[dict[str, Any]]:
        outputs: list[dict[str, Any]] = []
        for event in events:
            if not bool(event.get("effective")):
                continue
            event_payload = _payload(event)
            strategy_result = event_payload.get("strategy") if isinstance(event_payload.get("strategy"), Mapping) else {}
            action = str(event.get("action") or "")
            plan_id = str(event.get("plan_id") or event_payload.get("plan_id") or "")
            plan = plans.get(plan_id, {})
            payload = _payload(plan)
            symbol = _text(event.get("symbol") or event_payload.get("symbol") or plan.get("symbol"), limit=20)
            name = _text(payload.get("name"), limit=30, fallback="名称未提供")
            strategy = _STRATEGY_LABELS.get(str(payload.get("strategy_profile") or ""), _text(payload.get("strategy_profile"), limit=40))
            met = _items(event.get("met_conditions") or strategy_result.get("met_conditions") or payload.get("met_conditions"), limit=4)
            unmet = _items(event.get("unmet_conditions") or strategy_result.get("unmet_conditions") or payload.get("unmet_conditions"), limit=4)
            veto = _items(event.get("veto_conditions") or strategy_result.get("veto_conditions") or payload.get("veto_conditions"), limit=3)
            source_id = f"{event.get('lane_id') or ''}:{plan_id}:{action}"
            title = f"A4有效事件｜{name} {symbol}｜{_ACTION_LABELS.get(action, action)}"
            lines = [
                f"**时间** {_text(event.get('minute_end') or now.isoformat(), limit=40)}　**策略** {strategy}",
                f"**动作 / 原因** {_ACTION_LABELS.get(action, action)}｜{_text(event.get('reason_code'), limit=100)}",
                f"**已满足** {'；'.join(met) if met else '触发条件已由确定性策略确认'}",
                f"**未满足** {'；'.join(unmet) if unmet else '无'}",
                f"**否决边界** {'；'.join(veto) if veto else '无新增否决条件'}",
                f"**计划约束** 触发 {_number(payload.get('trigger_low'))}–{_number(payload.get('trigger_high'))}｜止损 {_number(payload.get('stop_level'))}｜禁止追价 {_number(payload.get('no_chase_price') or payload.get('max_chase_price'))}",
            ]
            outputs.append(
                self._send(
                    delivery_key=f"a4:{source_id}",
                    kind="A4_EFFECTIVE",
                    source_id=source_id,
                    title=title,
                    lines=lines,
                    summary={"minute_end": event.get("minute_end"), "symbol": symbol, "action": action, "reason_code": event.get("reason_code")},
                    now=now,
                )
            )
        return outputs


__all__ = ["WorkflowLarkPublisher"]
