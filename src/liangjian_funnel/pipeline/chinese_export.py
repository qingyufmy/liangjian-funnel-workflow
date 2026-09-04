"""Chinese-only A1/A2/A3 result projection with strict lineage checks.

The workbook layer consumes this small, deterministic projection instead of
rendering model/server fields directly.  Internal enum names and reason codes
therefore never become user-facing copy by accident.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import re
from typing import Any


_INTERNAL_TOKEN = re.compile(r"(?:^|\s)[A-Z][A-Z0-9_]{2,}(?:$|\s)")

_CYCLE_CN = {
    "LATENT": "潜伏期",
    "STARTUP": "启动期",
    "IGNITION": "启动期",
    "CONFIRMATION": "加速期",
    "ACCELERATION": "加速期",
    "EARLY_ACCELERATION": "加速期",
    "CLIMAX": "情绪高潮期",
    "DIVERGENCE": "分化退潮期",
    "RETREAT": "分化退潮期",
    "ICE_POINT": "冰点期",
}

_STRATEGY_CN = {
    "LEADER_INTRADAY": "情绪龙头",
    "MA520_SWING": "五日与二十日均线波段",
    "MA520": "五日与二十日均线波段",
    "TREND_MA5": "五日线趋势",
    "NO_NEXT_DAY_PLAN": "暂无次日计划",
}


def validate_stage_lineage(
    a1_output: Mapping[str, Any],
    a2_output: Mapping[str, Any],
    a3_output: Mapping[str, Any],
) -> dict[str, Any]:
    """Fail unless the complete exported sets satisfy A1 >= A2 >= A3."""

    a1_rows = _pool_rows(a1_output, ("active_research_pool",))
    a2_rows = _pool_rows(a2_output, ("focus_pool", "watch_only_pool"))
    a3_rows = _pool_rows(a3_output, ("core_watch_pool", "secondary_watch_pool"))
    stage_rows = {"A1": a1_rows, "A2": a2_rows, "A3": a3_rows}
    sets: dict[str, set[str]] = {}
    duplicates: dict[str, list[str]] = {}
    missing_symbols: dict[str, int] = {}
    for stage, rows in stage_rows.items():
        symbols = [_symbol(row) for row in rows]
        missing_symbols[stage] = sum(not symbol for symbol in symbols)
        clean = [symbol for symbol in symbols if symbol]
        sets[stage] = set(clean)
        duplicates[stage] = sorted({symbol for symbol in clean if clean.count(symbol) > 1})

    a2_outside = sorted(sets["A2"] - sets["A1"])
    a3_outside = sorted(sets["A3"] - sets["A2"])
    errors: list[str] = []
    if any(missing_symbols.values()):
        errors.append(f"存在缺少股票代码的记录：{missing_symbols}")
    if any(duplicates.values()):
        errors.append(f"存在重复股票：{duplicates}")
    if a2_outside:
        errors.append(f"A2存在未进入A1的股票：{a2_outside}")
    if a3_outside:
        errors.append(f"A3存在未进入A2的股票：{a3_outside}")
    if errors:
        raise ValueError("；".join(errors))
    return {
        "通过": True,
        "A1数量": len(sets["A1"]),
        "A2数量": len(sets["A2"]),
        "A3数量": len(sets["A3"]),
        "A2未包含于A1": [],
        "A3未包含于A2": [],
    }


def build_chinese_export_rows(
    a1_output: Mapping[str, Any],
    a2_output: Mapping[str, Any],
    a3_output: Mapping[str, Any],
) -> dict[str, Any]:
    """Return workbook-ready rows containing only user-facing Chinese copy."""

    checks = validate_stage_lineage(a1_output, a2_output, a3_output)
    a1_rows = [
        {
            "代码": _display_code(row),
            "名称": _name(row),
            "板块": _sector(row),
            "类型": _a1_type(row),
            "入选理由": _a1_reason(row),
        }
        for row in _pool_rows(a1_output, ("active_research_pool",))
    ]
    a2_rows: list[dict[str, Any]] = []
    for pool, status in (("focus_pool", "核心"), ("watch_only_pool", "观察")):
        for row in _pool_rows(a2_output, (pool,)):
            a2_rows.append(
                {
                    "代码": _display_code(row),
                    "名称": _name(row),
                    "板块": _sector(row),
                    "类别": _behavior_cn(row),
                    "状态": status,
                    "入选理由": _a2_reason(row, status=status),
                }
            )
    a3_rows: list[dict[str, Any]] = []
    for pool, status in (("core_watch_pool", "计划池"), ("secondary_watch_pool", "条件未满足")):
        for row in _pool_rows(a3_output, (pool,)):
            a3_rows.append(
                {
                    "代码": _display_code(row),
                    "名称": _name(row),
                    "板块": _sector(row),
                    "策略": _STRATEGY_CN.get(_upper(row.get("strategy_profile")), "暂无次日计划"),
                    "状态": status,
                    "入选理由": _a3_reason(row, status=status),
                }
            )
    result = {"自查": checks, "A1": a1_rows, "A2": a2_rows, "A3": a3_rows}
    _assert_no_internal_labels(result)
    return result


def _a1_type(row: Mapping[str, Any]) -> str:
    return "日度情绪补充" if _upper(row.get("selection_basis")) == "DAILY_EMOTION_OVERLAY" else "月度研究"


def _a1_reason(row: Mapping[str, Any]) -> str:
    if _upper(row.get("selection_basis")) == "DAILY_EMOTION_OVERLAY":
        rank = _integer(row.get("eastmoney_hot_rank"))
        rank_text = f"第{rank}名" if rank else "前100"
        return f"进入东方财富股吧人气{rank_text}，作为当日情绪候选纳入；仍需结合涨停梯队、换手承接和情绪周期复核"
    theme = _clean_cn(row.get("primary_theme")) or _sector(row)
    return f"本月研究方向为{theme}；已披露主营业务与该方向完成映射，基本面和数据质量达到研究门槛"


def _a2_reason(row: Mapping[str, Any], *, status: str) -> str:
    prefix = "保留观察：" if status == "观察" else ""
    behavior = _behavior_cn(row)
    if behavior == "情绪票":
        rank = _integer(row.get("eastmoney_hot_rank"))
        rank_text = f"第{rank}名" if rank else "前100"
        cycle = _CYCLE_CN.get(
            _upper(row.get("emotion_cycle_stage") or row.get("theme_stage")),
            "情绪阶段已核对",
        )
        height = _integer(row.get("ladder_height"))
        ladder_text = f"，当前{height}板梯队" if height else ""
        return f"{prefix}进入东方财富股吧人气{rank_text}，识别为情绪票；处于{cycle}{ladder_text}，按首段主升与接力纪律筛选"
    selected = _selected_board(row)
    board = _clean_cn(selected.get("board_name")) or _sector(row)
    rank = _integer(selected.get("primary_rank") or row.get("theme_rotation_rank"))
    rank_text = f"，量见一级板块强度第{rank}名" if rank else ""
    inflow = _number(selected.get("main_net_inflow_cny"))
    flow_text = f"，主力净流入{_format_cny(inflow)}" if inflow is not None else ""
    return f"{prefix}属于正净流入强势板块{board}{rank_text}{flow_text}；识别为趋势票，基本面有支撑并等待回踩后的右侧确认"


def _a3_reason(row: Mapping[str, Any], *, status: str) -> str:
    strategy = _STRATEGY_CN.get(_upper(row.get("strategy_profile")), "暂无次日计划")
    if status != "计划池":
        return f"进入{strategy}候选核对，但必要条件或风险条件尚未通过；仅观察，不新增仓位"
    if strategy == "情绪龙头":
        mode = _upper(row.get("plan_mode"))
        sizing = "仅限小仓试探" if mode == "PROBE" else "按冻结计划执行"
        return f"情绪龙头第一段主升条件成立，{sizing}；不得接回调反弹，炸板、放量派发或失效时立即退出"
    if strategy == "五日与二十日均线波段":
        return "五日与二十日均线右侧条件成立；只在回踩支撑并重新转强时执行，跌破失效位退出"
    if strategy == "五日线趋势":
        return "日线主升趋势成立；等待五日线附近回踩止跌和盘中确认，不追涨，跌破失效位退出"
    return "当前没有满足条件的次日执行策略"


def _behavior_cn(row: Mapping[str, Any]) -> str:
    channel = _upper(row.get("a2_pool_channel"))
    behavior = _upper(row.get("stock_behavior_type"))
    if channel == "EMOTION" or behavior == "EMOTION":
        return "情绪票"
    if channel == "TREND" or behavior == "TREND":
        return "趋势票"
    return "待分类"


def _sector(row: Mapping[str, Any]) -> str:
    selected = _selected_board(row)
    board_name = _clean_cn(selected.get("board_name"))
    if board_name:
        return board_name
    # Prefer the user-facing monthly direction on A1 and the concrete sector
    # index on A2.  Internal theme/node identifiers are deliberately last and
    # will be discarded by ``_clean_cn``.
    for key in (
        "selected_board_name",
        "monthly_direction_name",
        "sector_index_name",
        "primary_theme",
        "theme_name",
        "industry_chain_node",
    ):
        value = _clean_cn(row.get(key))
        if value:
            return value
    names: list[str] = []
    raw = row.get("ths_industries")
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
        for item in raw:
            if isinstance(item, Mapping):
                value = _clean_cn(item.get("industry_name") or item.get("name"))
                if value and value not in names:
                    names.append(value)
    return " / ".join(names[:2]) or "板块待核对"


def _selected_board(row: Mapping[str, Any]) -> Mapping[str, Any]:
    value = row.get("selected_board")
    return value if isinstance(value, Mapping) else {}


def _pool_rows(output: Mapping[str, Any], pools: Sequence[str]) -> list[Mapping[str, Any]]:
    rows: list[Mapping[str, Any]] = []
    for pool in pools:
        value = output.get(pool)
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            rows.extend(item for item in value if isinstance(item, Mapping))
    return rows


def _symbol(row: Mapping[str, Any]) -> str:
    return str(row.get("symbol") or row.get("thscode") or row.get("ticker") or row.get("code") or "").strip().upper()


def _display_code(row: Mapping[str, Any]) -> str:
    return _symbol(row).split(".", 1)[0]


def _name(row: Mapping[str, Any]) -> str:
    return str(row.get("company_name") or row.get("name") or "名称待补充").strip()


def _clean_cn(value: Any) -> str:
    text = str(value or "").strip()
    if not text or re.fullmatch(r"[A-Z][A-Z0-9_:-]*", text):
        return ""
    return re.sub(r"\s+", " ", text)


def _upper(value: Any) -> str:
    return str(value or "").strip().upper()


def _integer(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _format_cny(value: float) -> str:
    if abs(value) >= 100_000_000:
        return f"{value / 100_000_000:.2f}亿元"
    if abs(value) >= 10_000:
        return f"{value / 10_000:.0f}万元"
    return f"{value:.0f}元"


def _assert_no_internal_labels(value: Any) -> None:
    if isinstance(value, Mapping):
        for nested in value.values():
            _assert_no_internal_labels(nested)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for nested in value:
            _assert_no_internal_labels(nested)
    elif isinstance(value, str) and _INTERNAL_TOKEN.search(value):
        raise ValueError(f"导出内容包含内部英文标识：{value}")


__all__ = ["build_chinese_export_rows", "validate_stage_lineage"]
