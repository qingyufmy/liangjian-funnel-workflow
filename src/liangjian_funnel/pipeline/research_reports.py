"""Human-readable stage reports derived only from persisted stage outputs."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from ..reporting import atomic_write_text
from .a3_display import a3_nonqualified_explanation
from .presentation import SCHEMA_VERSION as PRESENTATION_SCHEMA_VERSION, project_research_presentation


_POOLS: dict[str, tuple[tuple[str, str], ...]] = {
    "A1": (
        ("active_research_pool", "ACTIVE"),
        ("monitor_pool", "MONITOR"),
        ("rejected_candidates", "REJECTED"),
    ),
    "A2": (
        ("focus_pool", "FOCUS"),
        ("watch_only_pool", "WATCH"),
        ("rejected_candidates", "REJECTED"),
    ),
    "A3": (
        ("core_watch_pool", "CORE"),
        ("secondary_watch_pool", "SECONDARY"),
        ("rejected_candidates", "未晋级（含观察／缺口）"),
    ),
}


def write_stage_markdown_reports(result: Any, output_dir: Path) -> dict[str, str]:
    """Write one bounded Markdown report per stage and return its paths."""

    output_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, str] = {}
    for stage in ("A1", "A2", "A3"):
        path = output_dir / f"research_{result.run_id}_{stage}.md"
        atomic_write_text(path, _render_stage(result, stage))
        paths[stage] = str(path)
    return paths


def _render_stage(result: Any, stage_name: str) -> str:
    lines = [
        f"# {stage_name} 分阶段研究结果",
        "",
        "> 内部模拟、非投资建议；内容来自持久化阶段结果，不连接外部交易。",
        "",
        f"- run_id：`{result.run_id}`",
        f"- 总状态：`{result.status}`",
    ]
    for lane in result.lanes:
        stage = next((item for item in lane.stages if str(item.stage).upper() == stage_name), None)
        lines.extend(["", f"## {lane.lane} · {lane.model}", ""])
        if stage is None:
            lines.append("- 状态：`NOT_RECORDED`")
            continue
        lines.extend(
            [
                f"- 状态：`{stage.status}`",
                f"- 输入快照：`{stage.snapshot_id}`",
                f"- 阶段原因：`{', '.join(stage.reason_codes) if stage.reason_codes else '-'}`",
            ]
        )
        output = stage.output if isinstance(stage.output, Mapping) else {}
        summary = output.get("analysis_summary")
        if isinstance(summary, Mapping):
            outcome = summary.get("outcome")
            if outcome:
                lines.append(f"- 结论：`{_cell(outcome)}`")
        counts = []
        for pool, label in _POOLS[stage_name]:
            values = _rows(output.get(pool))
            counts.append(f"{label}={len(values)}")
        lines.append(f"- 池计数：`{' / '.join(counts)}`")
        for pool, label in _POOLS[stage_name]:
            values = _rows(output.get(pool))
            lines.extend(["", f"### {label}（{len(values)}）", ""])
            if not values:
                lines.append("无。")
                continue
            # A1 MONITOR may contain the whole market. Preserve exact counts
            # and reason distribution without producing a multi-megabyte MD;
            # complete rows remain in the lane JSON and stage-detail API.
            if stage_name == "A1" and pool == "monitor_pool":
                lines.extend(_reason_summary(values))
                continue
            if stage_name == "A3" and pool == "rejected_candidates":
                lines.extend([
                    "| 代码 | 名称 | 主题/节点 | 路线/角色 | 状态 | 主要阻断 | 其他条件 | 背景风险 | A4待确认 |",
                    "|---|---|---|---|---|---|---|---|---|",
                ])
            elif stage_name == "A2":
                lines.extend(
                    [
                        "| 代码 | 名称 | 主题/节点 | 主题强度 | 个股相对强度 | 市场角色/研究路径 | 个股总分 | 原因 |",
                        "|---|---|---|---:|---:|---|---:|---|",
                    ]
                )
            elif stage_name == "A3":
                lines.extend(
                    [
                        "| 代码 | 名称 | 主题/节点 | 日线设置 | A4确认 | 当前入场资格 | 计划有效期 | 目标依据 | 原因 |",
                        "|---|---|---|---|---|---|---|---|---|",
                    ]
                )
            else:
                lines.extend(
                    [
                        "| 代码 | 名称 | 主题/节点 | 路线/角色 | 分数 | 原因 |",
                        "|---|---|---|---|---:|---|",
                    ]
                )
            for row in values:
                if stage_name == "A3" and pool == "rejected_candidates":
                    explanation = a3_nonqualified_explanation(row)
                    lines.append(_a3_rejected_row_line(row, explanation))
                else:
                    lines.append(_row_line(row, stage_name=stage_name, pool=pool))
    return "\n".join(lines) + "\n"


def _rows(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _reason_summary(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    counts = Counter(reason for row in rows for reason in _reasons(row))
    lines = ["完整观察池保存在lane JSON与前端阶段明细中；Markdown仅汇总原因，避免重复写入全市场数据。", ""]
    lines.extend(["| 原因码 | 股票数 |", "|---|---:|"])
    if not counts:
        lines.append("| 未提供原因 | 0 |")
    else:
        for reason, count in counts.most_common():
            lines.append(f"| {_cell(reason)} | {count} |")
    return lines


def _presentation(row: Mapping[str, Any], *, stage_name: str, pool: str) -> Mapping[str, Any]:
    persisted = row.get("presentation")
    if isinstance(persisted, Mapping) and persisted.get("schema_version") == PRESENTATION_SCHEMA_VERSION:
        return persisted
    pool_name = "approved" if pool in {"active_research_pool", "focus_pool", "core_watch_pool"} else (
        "watch" if pool in {"monitor_pool", "watch_only_pool", "secondary_watch_pool"} else "rejected"
    )
    return project_research_presentation(row, stage=stage_name, pool=pool_name)


def _row_line(row: Mapping[str, Any], *, stage_name: str, pool: str) -> str:
    symbol = row.get("symbol") or row.get("stock_code") or "-"
    name = row.get("company_name") or row.get("name") or "-"
    theme = row.get("primary_theme") or row.get("theme_id") or "-"
    node = row.get("industry_chain_node") or row.get("node_id") or "-"
    route = (
        row.get("a2_route")
        or row.get("selection_route")
        or row.get("market_role")
        or row.get("role")
        or "-"
    )
    presentation = _presentation(row, stage_name=stage_name, pool=pool)
    reasons = ", ".join(_reasons(row)) or "-"
    if stage_name == "A2":
        a2 = presentation.get("a2") if isinstance(presentation.get("a2"), Mapping) else {}
        role_path = "/".join(str(value) for value in (a2.get("market_role"), a2.get("research_path")) if value not in (None, "")) or "-"
        return (
            f"| {_cell(symbol)} | {_cell(name)} | {_cell(theme)}/{_cell(node)} | "
            f"{_cell(a2.get('theme_strength'))} | {_cell(a2.get('stock_relative_strength'))} | "
            f"{_cell(role_path)} | {_cell(a2.get('individual_total_score'))} | {_cell(reasons)} |"
        )
    if stage_name == "A3":
        a3 = presentation.get("a3") if isinstance(presentation.get("a3"), Mapping) else {}
        target = a3.get("target") if isinstance(a3.get("target"), Mapping) else {}
        target_label = f"{target.get('kind') or 'UNAVAILABLE'} / {target.get('claim') or 'NO_TARGET_EVIDENCE'}"
        return (
            f"| {_cell(symbol)} | {_cell(name)} | {_cell(theme)}/{_cell(node)} | "
            f"{_cell(a3.get('daily_setup_state'))} | {_cell(a3.get('a4_confirmation_state'))} | "
            f"{_cell(a3.get('current_entry_eligibility'))} | {_cell(a3.get('plan_validity_state'))} | "
            f"{_cell(target_label)} | {_cell(reasons)} |"
        )
    score = presentation.get("display_score", "-")
    return (
        f"| {_cell(symbol)} | {_cell(name)} | {_cell(theme)}/{_cell(node)} | "
        f"{_cell(route)} | {_cell(score)} | {_cell(reasons)} |"
    )


def _a3_rejected_row_line(row: Mapping[str, Any], explanation: Mapping[str, Any]) -> str:
    symbol = row.get("symbol") or row.get("stock_code") or "-"
    name = row.get("company_name") or row.get("name") or "-"
    theme = row.get("primary_theme") or row.get("theme_id") or "-"
    node = row.get("industry_chain_node") or row.get("node_id") or "-"
    route = (
        row.get("deterministic_strategy_profile")
        or row.get("strategy_profile")
        or row.get("a2_route")
        or row.get("market_role")
        or "-"
    )
    return (
        f"| {_cell(symbol)} | {_cell(name)} | {_cell(theme)}/{_cell(node)} | {_cell(route)} | "
        f"{_cell(explanation.get('状态'))} | {_cell(explanation.get('主要原因'))} | "
        f"{_cell(explanation.get('其他条件'))} | {_cell(explanation.get('背景风险'))} | "
        f"{_cell(explanation.get('盘中待确认'))} |"
    )


def _reasons(row: Mapping[str, Any]) -> list[str]:
    value = row.get("reason_codes")
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [str(item) for item in value if str(item)]
    single = row.get("reason_code")
    return [str(single)] if single else []


def _cell(value: Any) -> str:
    return str(value if value not in (None, "") else "-").replace("|", "\\|").replace("\n", " ")


__all__ = ["write_stage_markdown_reports"]
