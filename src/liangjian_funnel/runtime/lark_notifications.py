"""Direct Lark notifications for A3 plans, A4 events and A5 reviews."""

from __future__ import annotations

import hashlib
import json
import re
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

_A5_ATTRIBUTION_LABELS = {
    "GOOD_EXECUTION": "执行符合计划",
    "SELECTION_ERROR": "选股环节存在问题",
    "PLAN_ERROR": "日线计划存在问题",
    "CONFIRM_ERROR": "盘中确认存在问题",
    "DATA_ERROR": "行情或事实数据存在问题",
    "MARKET_REVERSAL": "盘中市场发生反转",
    "DATA_LIMITED": "数据不足，暂缓归因",
    "NOT_AN_ERROR": "不是策略错误",
    "UNCLASSIFIED": "暂未完成归因",
}

_A5_DROP_STAGE_LABELS = {
    "A1": "未进入月度研究池",
    "A2": "未进入板块聚焦池",
    "A3": "未形成日线计划",
    "A4": "盘中条件未触发",
    "UNRESOLVED": "尚未定位漏斗位置",
}

_A5_PROPOSAL_LABELS = {
    "ENGINEERING_FIX": "工程修复",
    "DATA_FIX": "数据修复",
    "SHADOW_TEST": "影子验证",
}

_DISPLAY_LABELS = {
    "READY": "资料完整",
    "DEGRADED": "部分资料待完善",
    "READY_DEGRADED": "可用但需留意",
    "VALIDATED": "已验证",
    "ACTIVE_CURRENT_SESSION": "当日盘中监测中",
    "PENDING_0926": "等待早盘复核",
    "ALREADY_ACTIVE": "已进入盘中监测",
    "BULL": "多头",
    "BEAR": "空头",
    "BEAR_RISK": "偏弱防守",
    "WEAK_ROTATION": "弱势轮动",
    "NEUTRAL": "中性",
    "TREND": "趋势票",
    "EMOTION": "情绪票",
    "TREND_CORE": "趋势核心",
    "EMOTION_LEADER": "情绪龙头",
    "LEADER": "龙头",
    "FOLLOWER": "跟随",
    "PERSISTENT": "趋势延续",
    "ACCELERATING": "加速增强",
    "EARLY_REVERSAL": "初步转强",
    "MIXED": "多空交织",
    "COOLING": "热度降温",
    "REPAIR": "修复阶段",
    "CONFIRMATION": "确认阶段",
    "ICE_POINT": "情绪冰点",
    "LIQUIDITY_CONTRACTION": "流动性收缩",
    "ALLOW": "允许关注",
    "NO_NEW_ENTRY": "暂不追高开仓",
    "CAUTION": "谨慎参与",
    "HIGH": "较高",
    "MEDIUM": "中等",
    "LOW": "较低",
    "NONE": "暂无",
    "SINGLE_STOCK": "单股带动",
    "QUALIFIED": "符合计划条件",
    "WATCH_ONLY": "继续观察",
    "QUALIFIED_STANDARD": "常规计划条件合格",
    "QUALIFIED_PROBE": "试探计划条件合格",
    "HIGHER_TIMEFRAME_CONDITIONAL_PROBE": "大周期仍需盘中确认",
    "A3_WATCH_ONLY_TECHNICALLY_QUALIFIED_PROBE": "观察池中技术条件合格，可小仓试探",
    "A3_STAGE_LINEAGE_MISSING": "上游阶段追溯信息不完整",
    "A1_ACTIVE_REUSED": "沿用本月有效研究池",
    "A2_FOCUS_POOL_UNDERFILLED_MARKET": "当日强势板块数量较少",
    "POOL_UNDERFILLED_MARKET": "市场机会数量较少",
    "FIRST_RESISTANCE": "第一压力位",
    "R2_OBSERVATION": "第二压力位观察",
    "MONTH_CLOSED": "月线数据完整",
    "WEEK_CLOSED": "周线数据完整",
    "DAILY_CLOSED": "日线数据完整",
    "TRADABLE": "当前可交易",
    "DAILY_CLOSE_AVAILABLE": "参考收盘价可用",
    "HIGHER_TIMEFRAME_RISK_CLASSIFIED": "大周期风险已分类",
    "PRICE_GEOMETRY_VALID": "价格结构有效",
    "TREND_DAILY_PATH_CONFIRMED": "日线趋势路径确认",
    "DAILY_MA5_AVAILABLE_FOR_A4": "五日线可供盘中择时",
    "NOT_OVEREXTENDED_OR_RETEST_CONFIRMED": "未明显超涨或回踩已确认",
    "NOT_DISTRIBUTION": "未发现明显派发",
    "A4_WILL_CONFIRM_DAILY_MA5_PULLBACK": "盘中继续确认五日线回踩",
    "DAILY_NOT_BEARISH": "个股日线未转空",
    "DETERMINISTIC_TRIGGER_PASS": "确定性触发条件通过",
    "DETERMINISTIC_EXIT_TRIGGER": "确定性离场条件触发",
    "HARD_STOP_BEFORE_ENTRY": "入场前触及风险线",
    "CURRENT_1M_HARD_STOP": "当前一分钟价格触及持仓硬止损",
    "PRE_ENTRY_RISK_LEVEL_TOUCHED": "入场前触及风险线，等待闭合周期确认",
    "ENTRY_BLOCKED_CURRENT_MINUTE": "当前分钟暂停入场",
    "TREND_PRE_ENTRY_STRUCTURE_INVALIDATED": "趋势结构经闭合周期确认失效",
    "MA520_PRE_ENTRY_STRUCTURE_INVALIDATED": "五二零结构经闭合周期确认失效",
    "A4_FORCED_EXIT_WITHOUT_POSITION": "策略产生了无持仓离场指令，已作为异常阻断",
    "A4_BEHAVIOR_TYPE_MISSING": "股票类型尚未确定，不能选择盘中策略",
    "BLOCKED_T1": "受 A 股当日买入次日可卖规则限制，等待下一交易日离场",
    "ENTRY_NEXT_BAR_MISSED": "入场信号后的下一根完整分钟线未能成交",
    "TREND_5M_FAILED_MA5_RECLAIM": "趋势股连续跌破五日线参考且回抽失败",
    "TREND_HIGH_VOLUME_MA5_BREAK": "趋势股放量跌破五日线参考",
    "TREND_HIGH_VOLUME_UPPER_SHADOW": "趋势股放量长上影，触发减仓",
    "MA520_5M_FAILED_MA20_RECLAIM": "五二零策略跌破二十日线参考且回抽失败",
    "MA520_HIGH_VOLUME_MA20_BREAK": "五二零策略放量跌破二十日线参考",
    "TREND_5M_REVERSAL_NOT_CONFIRMED": "五分钟转强尚未确认",
    "TREND_15M_PRESSURE_NOT_EASING": "十五分钟压力尚未缓解",
    "TREND_PULLBACK_ZONE_NOT_MET": "尚未进入趋势回踩区",
    "PLAN_INVALIDATED_AT_OPEN": "开盘价格触发计划失效",
    "LLM_VETO": "盘中复核模型否决",
    "HARD_STOP": "价格触及硬止损",
    "DATA_BLOCK": "数据条件未满足",
    "INSUFFICIENT": "不足",
    "SUFFICIENT": "充足",
    "PARTIAL": "部分可用",
    "NOT_APPLICABLE": "不适用",
    "TERMINAL": "已结束",
    "MISSING": "缺失",
    "RECOVERY": "修复阶段",
    "ROTATION": "轮动行情",
    "MAIN_RISE": "主升趋势",
    "STRONG_SETUP": "强势形态",
    "STRONG_TREND": "强势趋势",
    "PLATFORM_BREAKOUT": "平台突破",
    "NEW_HIGH": "创新高",
    "HEALTHY": "运行健康",
    "NEEDS_ATTENTION": "需要关注",
    "DATA_LIMITED": "数据受限",
    "INCIDENT": "存在执行事故",
    "UNAVAILABLE": "证据不可用",
    "NOT_AN_ERROR": "不是策略错误",
    "GOOD_EXECUTION": "执行符合计划",
    "SELECTION_ERROR": "选股环节存在问题",
    "PLAN_ERROR": "日线计划存在问题",
    "CONFIRM_ERROR": "盘中确认存在问题",
    "DATA_ERROR": "行情或事实数据存在问题",
    "MARKET_REVERSAL": "盘中市场发生反转",
    "UNCLASSIFIED": "暂未完成归因",
    "ENGINEERING_FIX": "工程修复",
    "DATA_FIX": "数据修复",
    "SHADOW_TEST": "影子验证",
    "INSUFFICIENT_SAMPLE": "样本不足",
    "MISSING_DATA": "数据缺失",
    "CONFOUNDED": "影响因素相互干扰",
    "REGIME_NOT_OBSERVED": "尚未观察到对应市场环境",
    "MATCH": "交叉核验一致",
    "MISMATCH": "交叉核验不一致",
    "A1_NOT_ACTIVE": "未进入 A1 有效研究池",
    "A2_NOT_EVALUATED": "A2 尚未完成评估",
    "A2_NOT_FOCUSED": "未进入 A2 聚焦池",
    "A3_NOT_PLANNED": "A3 未形成日线计划",
    "A4_NO_EFFECTIVE_SIGNAL": "A4 未触发有效信号",
    "PREVIOUS_CLOSE": "以前一日收盘价为基准",
    "FIRST_INTRADAY_OPEN": "以当日第一笔盘中开盘价为基准",
}

_THEME_LABELS = {
    "TH_ELEC_COMPONENTS": "电子元件",
    "TH_NONMETAL_MATERIALS": "非金属材料与电子化学品",
    "TH_AGRI_FOREST": "种植业与林业",
    "TH_PRECIOUS_METAL": "贵金属",
    "TH_COMM_EQUIP": "通信设备",
    "TH_SEMI_LOCALIZATION": "半导体国产替代",
    "TH_MEDICAL_SERVICE": "医疗服务",
    "INDUSTRY:881278.TI": "线缆部件与电网设备",
    "INDUSTRY:884073.TI": "制冷空调设备",
    "INDUSTRY:881118.TI": "专用设备",
    "INDUSTRY:881177.TI": "互联网服务",
    "INDUSTRY:884202.TI": "房地产服务",
    "CONCEPT:885999.TI": "智能座舱",
}

_TEXT_REPLACEMENTS = {
    "SOCIAL_FINANCING": "社会融资",
    "NEW_CREDIT": "新增信贷",
    "M2_YOY": "广义货币同比",
    "M1_YOY": "狭义货币同比",
    "business_purity": "主营纯度",
    "MARKET_CORE": "市场核心",
    "eligible=true": "符合入选条件",
    "eligible=false": "不符合入选条件",
    "PMI": "制造业采购经理指数",
    "PPI": "工业生产者出厂价格指数",
    "tier": "梯队",
    "24h": "24小时",
    "Tencent": "腾讯行情",
    "TENCENT": "腾讯行情",
    "HITHINK": "同花顺",
    "TDX": "通达信",
    "mootdx": "通达信",
    "MA5": "五日均线",
    "MA20": "二十日均线",
    "MA60": "六十日均线",
    "MACD": "指数平滑异同移动平均线",
    "KDJ": "随机指标",
}


def _payload(row: Mapping[str, Any]) -> dict[str, Any]:
    try:
        value = json.loads(str(row.get("payload_json") or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _json_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not value:
        return {}
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(decoded) if isinstance(decoded, Mapping) else {}


def _text(value: Any, *, limit: int = 300, fallback: str = "—") -> str:
    rendered = str(value or "").strip().replace("\r", " ").replace("\n", " ")
    return rendered[:limit] if rendered else fallback


def _items(value: Any, *, limit: int = 4) -> list[str]:
    if not isinstance(value, (list, tuple)):
        return []
    return [_text(item, limit=160) for item in value[:limit] if str(item or "").strip()]


def _display_text(value: Any, *, limit: int = 300, fallback: str = "—") -> str:
    raw = _text(value, limit=max(limit * 2, 320), fallback=fallback)
    if raw == fallback:
        return raw
    direct = _DISPLAY_LABELS.get(raw.strip().upper()) or _THEME_LABELS.get(raw.strip().upper())
    if direct:
        return direct[:limit]
    rendered = raw
    for source, target in _TEXT_REPLACEMENTS.items():
        if not re.fullmatch(r"[A-Z][A-Z0-9_:-]*", source):
            rendered = rendered.replace(source, target)
    rendered = rendered.replace("AI算力", "人工智能算力").replace("AI应用", "人工智能应用")
    rendered = re.sub(
        r"\b[A-Z][A-Z0-9_:-]{2,}(?:\.[A-Z]{2})?\b",
        lambda match: (
            _DISPLAY_LABELS.get(match.group(0).replace("-", "_"))
            or _THEME_LABELS.get(match.group(0).replace("-", "_"))
            or _TEXT_REPLACEMENTS.get(match.group(0).replace("-", "_"))
            or "系统内部状态"
        ),
        rendered,
    )
    return rendered[:limit]


def _display_items(value: Any, *, limit: int = 4) -> list[str]:
    return [_display_text(item, limit=180) for item in _items(value, limit=limit)]


def _percentage(value: Any) -> str:
    try:
        return f"{float(value) * 100:.2f}%"
    except (TypeError, ValueError):
        return "—"


def _theme_label(value: Mapping[str, Any]) -> str:
    name = value.get("theme_name") or value.get("theme") or value.get("industry")
    if not name and (value.get("theme_id") or value.get("code")):
        name = value.get("name")
    if name:
        return _display_text(name, limit=40)
    code = str(value.get("theme_id") or "").strip().upper()
    if code in _THEME_LABELS:
        return _THEME_LABELS[code]
    if code.startswith("INDUSTRY:"):
        return f"行业方向（{code.split(':', 1)[1].split('.', 1)[0]}）"
    if code.startswith("CONCEPT:"):
        return f"题材方向（{code.split(':', 1)[1].split('.', 1)[0]}）"
    return "未标注方向"


def _model_label(value: Any) -> str:
    raw = str(value or "").lower()
    if "deepseek" in raw:
        return "深度求索研究模型"
    if "kimi" in raw or "moonshot" in raw:
        return "月之暗面研究模型"
    if "glm" in raw or "z-ai" in raw:
        return "智谱研究模型"
    return "主研究模型"


def _stock_code(value: Any) -> str:
    raw = str(value or "").strip().upper()
    return raw.split(".", 1)[0] if raw else "代码未提供"


def _priority_label(value: Any) -> str:
    return {
        "P1": "最高优先",
        "P2": "常规优先",
        "P3": "试探观察",
    }.get(str(value or "").strip().upper(), "优先级待确认")


def _number(value: Any) -> str:
    try:
        return f"{float(value):.3f}".rstrip("0").rstrip(".")
    except (TypeError, ValueError):
        return "—"


def _time_label(value: Any, *, fallback: str = "—") -> str:
    raw = str(value or "").strip()
    if not raw:
        return fallback
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return _display_text(raw, limit=40, fallback=fallback)
    return parsed.strftime("%Y年%m月%d日 %H:%M")


def _reference_price_as_of(
    payload: Mapping[str, Any],
    context: Mapping[str, Any],
) -> str:
    """Project a daily reference price onto its frozen market date.

    Historical recovery plans created before the source fix may carry the
    recovery wall clock even though ``reference_price`` is the previous close.
    Correct only that cross-date mismatch at the presentation boundary; keep a
    same-date stock-specific bar timestamp unchanged.
    """

    raw = str(payload.get("reference_price_as_of") or "").strip()
    market_date = str(context.get("market_trade_date") or "").strip()
    if len(market_date) == 10 and market_date[4:5] == "-" and market_date[7:8] == "-":
        if not raw or raw[:10] != market_date:
            return f"{market_date}T15:00:00+08:00"
    return raw or "—"


def _plan_sort_key(row: Mapping[str, Any]) -> tuple[int, str, str]:
    payload = _payload(row)
    rank = {"P1": 0, "P2": 1, "P3": 2}.get(
        str(payload.get("plan_priority") or "").strip().upper(),
        3,
    )
    return rank, str(row.get("symbol") or ""), str(row.get("plan_id") or "")


def _monthly_line(value: Mapping[str, Any]) -> str:
    name = _display_text(value.get("name") or value.get("code"), limit=30)
    strength = _number(value.get("relative_strength_percentile_20d"))
    try:
        return_5d = f"{float(value.get('return_5d')) * 100:.1f}%"
    except (TypeError, ValueError):
        return_5d = "—"
    return f"{name}：近5日 {return_5d}，20日强度分位 {strength}"


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
            theme = _display_text(
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
        chunks = [ordered[index : index + 3] for index in range(0, len(ordered), 3)]
        for page, chunk in enumerate(chunks, start=1):
            lines = [
                "**早盘复核结果**",
                f"• {reviewed_at.strftime('%m月%d日 %H:%M')} 完成复核，{len(ordered)} 只计划进入盘中监测。",
                f"• 重点方向：{'、'.join(themes[:5]) if themes else '以已验证板块为准'}。",
                f"• 适用策略：{'、'.join(strategies) if strategies else '按个股计划执行'}。",
            ]
            symbols: list[str] = []
            source_ids: list[str] = []
            for row in chunk:
                payload = _payload(row)
                symbol_raw = _text(row.get("symbol"), limit=20)
                symbol = _stock_code(symbol_raw)
                name = _text(payload.get("name"), limit=30, fallback="名称未提供")
                symbols.append(symbol)
                source_ids.append(_text(payload.get("source_run_id"), limit=160, fallback=""))
                strategy = _STRATEGY_LABELS.get(str(payload.get("strategy_profile") or ""), _display_text(payload.get("strategy_profile"), limit=40, fallback="按个股计划执行"))
                reasons = _display_items(payload.get("selection_reasons") or payload.get("reason_codes"), limit=2)
                conditions = _display_items(payload.get("required_conditions"), limit=3)
                auction = evidence.get(symbol_raw) or evidence.get(symbol) or {}
                auction_price = auction.get("price") if isinstance(auction, Mapping) else None
                lines.extend(
                    [
                        f"\n**{name}（{symbol}）｜{strategy}**",
                        f"• 入选依据：{'；'.join(reasons) if reasons else '已通过 A3 确定性技术计划'}",
                        f"• 价格计划：触发区 {_number(payload.get('trigger_low'))}–{_number(payload.get('trigger_high'))}；止损 {_number(payload.get('stop_level'))}；禁止追价 {_number(payload.get('no_chase_price') or payload.get('max_chase_price'))}",
                        f"• 早盘价格：{_number(auction_price)}；确认条件：{'；'.join(conditions) if conditions else '按计划触发条件执行'}",
                    ]
                )
            lines.extend(["", "**执行纪律**", "• 未触发不交易，超过禁止追价位不追，跌破失效价立即按风控处理。"])
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
        activation_state: str = "PENDING_0926",
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
        active_session = activation_state == "ACTIVE_CURRENT_SESSION"
        batch_hash = hashlib.sha256(
            f"{source_run_id or ''}|{activation_state}|{'|'.join(plan_ids)}".encode("utf-8")
        ).hexdigest()[:16]
        context = dict(research_context) if isinstance(research_context, Mapping) else {}
        a1 = context.get("a1") if isinstance(context.get("a1"), Mapping) else {}
        a2 = context.get("a2") if isinstance(context.get("a2"), Mapping) else {}
        a3 = context.get("a3") if isinstance(context.get("a3"), Mapping) else {}
        macro = a1.get("macro") if isinstance(a1.get("macro"), Mapping) else {}
        constraints = a3.get("market_open_constraints") if isinstance(a3.get("market_open_constraints"), Mapping) else {}
        directions = _display_items(macro.get("policy_direction"), limit=4)
        uncertainties = _display_items(macro.get("key_uncertainties"), limit=4)
        monthly = [item for item in a1.get("monthly_industries", ()) if isinstance(item, Mapping)] if isinstance(a1.get("monthly_industries"), (list, tuple)) else []
        themes = [item for item in a2.get("active_themes", ()) if isinstance(item, Mapping)] if isinstance(a2.get("active_themes"), (list, tuple)) else []
        reason_codes = _display_items(context.get("reason_codes"), limit=6)
        report_label = "盘中补发" if active_session else "盘前总览"
        execution_boundary = (
            "本卡为盘中补发，只呈现已完成当日行情复核并处于 A4 监测中的计划；不回填竞价或早盘信号。"
            if active_session
            else "未落库的24h新闻、竞价和实时龙虎榜不推演、不补造；09:26 独立竞价复核前不激活 A4。"
        )
        context_lines = [
            f"**{report_label}｜{analyzed_at.strftime('%m月%d日 %H:%M')}**",
            (
                f"今日发布 {len(ordered)} 只 A3 计划，"
                + ("均已进入盘中监测；只等待实时条件，不回填早盘信号。" if active_session else "等待早盘复核后进入盘中监测。")
            ),
            "",
            "**研究时点**",
            f"• 收盘数据：{_text(context.get('market_trade_date'), limit=20)}",
            f"• 目标交易日：{_text(context.get('target_trade_date'), limit=20)}",
            f"• 研究模型：{_model_label(context.get('model'))}",
            f"• 资料状态：{_display_text(context.get('status'), limit=30, fallback='尚未确认')}",
            "",
            "**宏观与政策**",
            f"• 流动性：{_display_text(macro.get('liquidity_condition'), limit=180, fallback='暂无可核验结论')}",
            f"• 盈利周期：{_display_text(macro.get('profit_cycle_position'), limit=180, fallback='暂无可核验结论')}",
            f"• 政策方向：{'；'.join(directions) if directions else '暂无可核验方向'}",
            f"• A1 研究池：{_number(a1.get('active_count'))} 只",
            "",
            "**本月重点行业**",
        ]
        if monthly:
            context_lines.extend(
                f"{index}. {_monthly_line(item)}"
                for index, item in enumerate(monthly[:5], start=1)
            )
        else:
            context_lines.append("• 暂无已持久化的月度行业结论。")
        context_lines.extend(["", "**A2 板块轮动方向**"])
        if themes:
            for index, item in enumerate(themes[:5], start=1):
                theme_name = _theme_label(item)
                context_lines.append(
                    f"**{index}. {theme_name}｜强度 {_number(item.get('score'))}**\n"
                    f"• 周期：{_display_text(item.get('weekly_state'), limit=24)}；阶段：{_display_text(item.get('stage'), limit=24)}；开仓建议：{_display_text(item.get('new_entry_policy'), limit=24)}\n"
                    f"• 广度 {_number(item.get('breadth'))}；资金 {_number(item.get('capital_flow'))}；龙头 {_number(item.get('leader_structure'))}；梯队 {_number(item.get('tier_structure'))}；产业链共振 {_number(item.get('index_chain_resonance'))}\n"
                    f"• 追高风险：{_display_text(item.get('chase_risk_level'), limit=20)}"
                )
                contradiction = _display_items(item.get("contradicting_evidence"), limit=1)
                if contradiction:
                    context_lines.append(f"• 主要风险：{contradiction[0]}")
        else:
            context_lines.append("• A2 板块资料尚未持久化；这不代表市场没有机会。")
        context_lines.extend(
            [
                "",
                "**仓位与执行边界**",
                f"• 昨日环境：{_display_text(constraints.get('prior_market_environment'), limit=40)}",
                f"• 建议仓位：{_number(constraints.get('recommended_position_min_pct'))}–{_number(constraints.get('recommended_position_max_pct'))}",
                "• A3 负责次日计划；是否开仓由 A4 根据当日实时市场决定。",
                f"• 需要留意：{'；'.join(reason_codes) if reason_codes else '暂无额外事项'}",
                "",
                "**主要不确定性**",
                *(f"• {item}" for item in uncertainties),
                f"• {execution_boundary}",
            ]
        )
        chunks = [ordered[index : index + 3] for index in range(0, len(ordered), 3)]
        outputs: list[dict[str, Any]] = [
            self._send(
                delivery_key=f"a3-premarket-professional-v4:{analyzed_at.date().isoformat()}:{batch_hash}:overview",
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
                f"**A3 个股计划｜第 {page}/{len(chunks)} 页**",
                (
                    f"共 {len(ordered)} 只；"
                    + ("当前已进入 A4 盘中监测，不补造已错过的信号。" if active_session else "早盘复核通过后才进入 A4 盘中监测。")
                ),
            ]
            symbols: list[str] = []
            for row in chunk:
                payload = _payload(row)
                symbol = _stock_code(row.get("symbol"))
                name = _text(payload.get("name"), limit=30, fallback="名称未提供")
                symbols.append(symbol)
                strategy = _STRATEGY_LABELS.get(
                    str(payload.get("strategy_profile") or ""),
                    _display_text(payload.get("strategy_profile"), limit=40, fallback="按 A3 技术计划"),
                )
                reasons = _display_items(payload.get("selection_reasons") or payload.get("reason_codes"), limit=3)
                conditions = _display_items(payload.get("required_conditions"), limit=3)
                invalidators = _display_items(
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
                        f"\n**{name}（{symbol}）｜{_priority_label(payload.get('plan_priority'))}**",
                        f"• 类型：{_display_text(payload.get('stock_behavior_type'), limit=30)}；角色：{_display_text(payload.get('market_role'), limit=30)}；方向：{_theme_label(payload)}",
                        f"• 策略：{strategy}；优先依据：{'；'.join(_display_items(payload.get('priority_reasons'), limit=2)) or '按计划成熟度'}",
                        f"• 入选依据：{'；'.join(reasons) if reasons else '已通过确定性日线技术计划'}",
                         f"• 参考收盘：{_number(payload.get('reference_price'))}（{_time_label(_reference_price_as_of(payload, context))}）",
                        f"• 价格计划：触发区 {_number(payload.get('trigger_low'))}–{_number(payload.get('trigger_high'))}；止损 {_number(payload.get('stop_level') or payload.get('daily_invalidation'))}；禁止追价 {_number(payload.get('no_chase') or payload.get('no_chase_price') or payload.get('max_chase_price'))}",
                        f"• 压力参考：{_number(payload.get('pressure_reduce_price'))}（{_display_text(payload.get('pressure_basis'), limit=32, fallback='暂无明确压力位')}）",
                        f"• 盘中确认：{'；'.join(conditions) if conditions else ('由 A4 继续监测' if active_session else '等待早盘复核')}",
                        f"• 失效条件：{'；'.join(invalidators)}",
                        "• 三种情景：强势不追高，等待确认；中性进入触发区再判断；弱势跌破止损则计划作废。",
                    ]
                )
            lines.extend(["", "**执行纪律**", "• 未触发不交易；超过禁止追价位不追；跌破止损不等待模型解释。", "• 仅用于本地模拟研究，不连接真实账户。"])
            outputs.append(
                self._send(
                    delivery_key=f"a3-premarket-professional-v4:{analyzed_at.date().isoformat()}:{batch_hash}:plans:{page}",
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
                        "activation_deferred_to": (
                            "ALREADY_ACTIVE" if active_session else "09:26"
                        ),
                    },
                    now=analyzed_at,
                )
            )
        return outputs

    def publish_a5_review(
        self,
        review: Mapping[str, Any],
        *,
        now: datetime,
    ) -> list[dict[str, Any]]:
        """Publish one persisted A5 review as a compact Chinese card.

        The database row is the delivery source of truth.  Raw prompts,
        evidence identifiers, model transport, and internal reason codes are
        deliberately excluded from the card and its safe delivery summary.
        """

        report = _json_mapping(review.get("report") or review.get("report_json"))
        facts = _json_mapping(
            review.get("fact_snapshot") or review.get("fact_snapshot_json")
        )
        review_id = str(review.get("review_id") or "").strip()
        trade_date = str(review.get("trade_date") or report.get("trade_date") or "").strip()
        review_kind = str(
            review.get("review_kind") or report.get("review_kind") or ""
        ).strip().upper()
        if not report or not facts or not review_id or not trade_date:
            return []

        kind_label = "盘中复盘" if review_kind == "MIDDAY" else "盘后复盘"
        notification_kind = (
            "A5_MIDDAY_REVIEW" if review_kind == "MIDDAY" else "A5_POST_CLOSE_REVIEW"
        )
        metrics = _json_mapping(facts.get("metrics"))
        data_quality = _json_mapping(facts.get("data_quality"))
        verification = _json_mapping(facts.get("independent_verification"))
        verification_status = (
            verification.get("status")
            or metrics.get("a5_independent_verification_status")
            or "UNAVAILABLE"
        )
        lines = [
            "**复盘结论**",
            f"• {_display_text(report.get('executive_summary'), limit=420, fallback='本次复盘摘要未提供。')}",
            f"• 总体评价：{_display_text(report.get('overall_verdict'), limit=40, fallback='尚无结论')}",
            f"• 事实截止：{_time_label(review.get('cutoff_at') or facts.get('cutoff_at'))}",
            f"• 证据质量：{_display_text(data_quality.get('status'), limit=40, fallback='尚未确认')}；异源核验：{_display_text(verification_status, limit=40, fallback='尚未确认')}",
            "",
            "**漏斗概况**",
            f"• A2：聚焦 {_number(metrics.get('a2_focus_count'))} 只，观察 {_number(metrics.get('a2_watch_count'))} 只，覆盖 {_number(metrics.get('a2_theme_count'))} 个主题。",
            f"• A3：形成 {_number(metrics.get('a3_plan_count'))} 只日线计划。",
            f"• A4：记录 {_number(metrics.get('a4_effective_event_count'))} 条有效事件，跟踪 {_number(metrics.get('a4_lifecycle_count'))} 个信号生命周期。",
            "",
            "**分层验收**",
        ]
        for label, key in (
            ("A2 板块与选股", "a2_review"),
            ("A3 日线计划", "a3_review"),
            ("A4 盘中择时", "a4_review"),
        ):
            layer = _json_mapping(report.get(key))
            lines.append(
                f"• **{label}｜{_display_text(layer.get('verdict'), limit=32, fallback='尚无结论')}**："
                f"{_display_text(layer.get('summary'), limit=260, fallback='本次没有足够事实形成评价。')}"
            )

        signal_reviews = [
            item for item in report.get("signal_reviews", ()) if isinstance(item, Mapping)
        ] if isinstance(report.get("signal_reviews"), (list, tuple)) else []
        if signal_reviews:
            lines.extend(["", f"**信号复盘｜共 {len(signal_reviews)} 条，展示前 5 条**"])
            for item in signal_reviews[:5]:
                name = _text(item.get("name"), limit=24, fallback="名称未提供")
                symbol = _stock_code(item.get("symbol"))
                strategy = _STRATEGY_LABELS.get(
                    str(item.get("strategy_profile") or "").upper(),
                    _display_text(item.get("strategy_profile"), limit=32, fallback="策略待确认"),
                )
                attribution = _A5_ATTRIBUTION_LABELS.get(
                    str(item.get("attribution") or "").upper(),
                    _display_text(item.get("attribution"), limit=32, fallback="暂未完成归因"),
                )
                lines.append(
                    f"• **{name}（{symbol}）｜{strategy}**：{attribution}；"
                    f"{_display_text(item.get('assessment'), limit=220, fallback='评价待补充。')}"
                )

        counterexamples = [
            item for item in report.get("missed_opportunity_reviews", ()) if isinstance(item, Mapping)
        ] if isinstance(report.get("missed_opportunity_reviews"), (list, tuple)) else []
        if counterexamples:
            lines.extend(["", f"**反向拷问｜共 {len(counterexamples)} 个样本，展示前 5 个**"])
            for item in counterexamples[:5]:
                name = _text(item.get("name"), limit=24, fallback="名称未提供")
                symbol = _stock_code(item.get("symbol"))
                drop = _A5_DROP_STAGE_LABELS.get(
                    str(item.get("funnel_drop_stage") or "").upper(),
                    "尚未定位漏斗位置",
                )
                conclusion = "已确认缺陷" if bool(item.get("is_confirmed_defect")) else "需要继续验证"
                lines.append(
                    f"• **{name}（{symbol}）｜{drop}**："
                    f"{_display_text(item.get('observed_performance'), limit=140, fallback='表现资料未提供')}；"
                    f"{_display_text(item.get('assessment'), limit=180, fallback='判断待补充')}（{conclusion}）"
                )

        defects = [
            item for item in report.get("core_defects", ()) if isinstance(item, Mapping)
        ] if isinstance(report.get("core_defects"), (list, tuple)) else []
        lines.extend(["", "**核心缺陷**"])
        if defects:
            for item in defects[:4]:
                severity = _display_text(item.get("severity"), limit=20, fallback="待分级")
                layer = _display_text(item.get("layer"), limit=20, fallback="相关环节")
                data_note = "，受数据缺口影响" if bool(item.get("blocked_by_data")) else ""
                lines.append(
                    f"• **{layer}｜{severity}**："
                    f"{_display_text(item.get('problem'), limit=240, fallback='问题描述待补充')}{data_note}"
                )
        else:
            lines.append("• 本次未确认可归因的核心缺陷。")

        proposals = [
            item for item in report.get("improvement_proposals", ()) if isinstance(item, Mapping)
        ] if isinstance(report.get("improvement_proposals"), (list, tuple)) else []
        lines.extend(["", "**改进与验证**"])
        if proposals:
            for item in proposals[:3]:
                proposal_type = _A5_PROPOSAL_LABELS.get(
                    str(item.get("type") or "").upper(), "待验证建议"
                )
                target = _display_text(item.get("target"), limit=20, fallback="相关环节")
                lines.append(
                    f"• **{target}｜{proposal_type}**："
                    f"{_display_text(item.get('proposed_change'), limit=220, fallback='建议待补充')}；"
                    f"至少观察 {_number(item.get('min_shadow_days'))} 个交易日。"
                )
        else:
            lines.append("• 当前证据尚不足以提出新的改进实验。")
        lines.extend(
            [
                "",
                "**执行边界**",
                "• 复盘用于发现问题和积累样本；改进建议只进入影子验证，不自动修改生产策略。",
                "• 本系统仅做本地模拟研究，不连接真实账户，不发送真实订单。",
            ]
        )
        return [
            self._send(
                delivery_key=f"a5-review:{review_id}",
                kind=notification_kind,
                source_id=review_id,
                title=f"A股 A5 {kind_label}｜{trade_date}",
                lines=lines,
                summary={
                    "trade_date": trade_date,
                    "review_kind": review_kind,
                    "overall_verdict": report.get("overall_verdict"),
                    "signal_count": len(signal_reviews),
                    "counterexample_count": len(counterexamples),
                    "defect_count": len(defects),
                    "proposal_count": len(proposals),
                },
                now=now,
            )
        ]

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
            symbol = _stock_code(event.get("symbol") or event_payload.get("symbol") or plan.get("symbol"))
            name = _text(payload.get("name"), limit=30, fallback="名称未提供")
            strategy = _STRATEGY_LABELS.get(str(payload.get("strategy_profile") or ""), _display_text(payload.get("strategy_profile"), limit=40, fallback="按个股计划执行"))
            met = _display_items(event.get("met_conditions") or strategy_result.get("met_conditions") or payload.get("met_conditions"), limit=4)
            unmet = _display_items(event.get("unmet_conditions") or strategy_result.get("unmet_conditions") or payload.get("unmet_conditions"), limit=4)
            veto = _display_items(event.get("veto_conditions") or strategy_result.get("veto_conditions") or payload.get("veto_conditions"), limit=3)
            source_id = f"{event.get('lane_id') or ''}:{plan_id}:{action}"
            title = f"A4 盘中信号｜{name}（{symbol}）｜{_ACTION_LABELS.get(action, _display_text(action))}"
            lines = [
                "**盘中有效信号**",
                 f"• 时间：{_time_label(event.get('minute_end') or now.isoformat())}",
                f"• 股票：{name}（{symbol}）",
                f"• 策略：{strategy}",
                f"• 动作：{_ACTION_LABELS.get(action, _display_text(action))}",
                f"• 触发原因：{_display_text(event.get('reason_code'), limit=100, fallback='确定性条件成立')}",
                "",
                "**条件核对**",
                f"• 已满足：{'；'.join(met) if met else '确定性策略已确认'}",
                f"• 未满足：{'；'.join(unmet) if unmet else '无'}",
                f"• 否决边界：{'；'.join(veto) if veto else '无新增否决条件'}",
                "",
                "**价格纪律**",
                f"• 触发区：{_number(payload.get('trigger_low'))}–{_number(payload.get('trigger_high'))}",
                f"• 止损：{_number(payload.get('stop_level'))}；禁止追价：{_number(payload.get('no_chase_price') or payload.get('max_chase_price'))}",
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
