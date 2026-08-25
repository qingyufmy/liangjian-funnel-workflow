from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from .contracts import CapabilityReport
from .redaction import sanitize


def atomic_write_text(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="\n", delete=False, dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
    )
    temporary = Path(handle.name)
    try:
        with handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return path


def atomic_write_json(path: Path, value: BaseModel | dict[str, Any]) -> Path:
    raw = value.model_dump(mode="json") if isinstance(value, BaseModel) else value
    clean = sanitize(raw)
    return atomic_write_text(path, json.dumps(clean, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def capability_markdown(report: CapabilityReport) -> str:
    lines = [
        f"# {report.provider} capability report",
        "",
        f"- Generated: `{report.generated_at.isoformat()}`",
        f"- Overall: `{report.overall_status.value}`",
        f"- Secrets redacted: `{str(report.secrets_redacted).lower()}`",
        "",
        "| Check | Status | HTTP | Latency ms | Reason |",
        "|---|---|---:|---:|---|",
    ]
    for check in report.checks:
        lines.append(
            f"| {check.name} | {check.status.value} | {check.http_status or ''} | "
            f"{check.latency_ms if check.latency_ms is not None else ''} | {check.reason_code or ''} |"
        )
    lines.extend(["", "> Capability evidence is structural and redacted. Response bodies, authentication headers and reasoning text are not retained.", ""])
    return "\n".join(lines)


def write_capability_report(report: CapabilityReport, output_dir: Path) -> tuple[Path, Path]:
    stamp = report.generated_at.strftime("%Y%m%dT%H%M%S%z")
    stem = f"{report.provider.lower()}_{stamp}"
    json_path = atomic_write_json(output_dir / f"{stem}.json", report)
    md_path = atomic_write_text(output_dir / f"{stem}.md", capability_markdown(report))
    return json_path, md_path

