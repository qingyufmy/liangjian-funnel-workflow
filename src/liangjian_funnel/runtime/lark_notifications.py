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
