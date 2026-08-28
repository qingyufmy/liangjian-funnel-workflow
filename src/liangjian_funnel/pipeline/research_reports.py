"""Human-readable stage reports derived only from persisted stage outputs."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from ..reporting import atomic_write_text


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
        ("rejected_candidates", "REJECTED"),
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
            lines.extend(
                [
                    "| 代码 | 名称 | 主题/节点 | 路线/角色 | 分数 | 原因 |",
                    "|---|---|---|---|---:|---|",
                ]
            )
            for row in values:
                lines.append(_row_line(row))
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


def _row_line(row: Mapping[str, Any]) -> str:
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
    score = next(
        (row.get(key) for key in ("technical_score", "theme_score", "identifiability_score", "structural_score", "score") if row.get(key) is not None),
        "-",
    )
    reasons = ", ".join(_reasons(row)) or "-"
    return (
        f"| {_cell(symbol)} | {_cell(name)} | {_cell(theme)}/{_cell(node)} | "
        f"{_cell(route)} | {_cell(score)} | {_cell(reasons)} |"
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
