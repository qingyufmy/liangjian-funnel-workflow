"""Independent A1 → A2 → A3 research lanes.

This module owns orchestration and audit boundaries only.  It does not attempt
to validate the complete, prompt-defined business schemas; it validates the
generic envelope, permissions, strict JSON and symbol lineage that must hold
before a stage can be consumed by its downstream stage.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable
from zoneinfo import ZoneInfo

from ..redaction import digest_text, safe_error
from ..reporting import atomic_write_json, atomic_write_text
from ..settings import RESEARCH_MODELS, Settings
from .model_client import ModelCallResult, ModelClientError, OpenAICompatibleModelClient
from .prompts import PromptBundle, PromptRepository, PromptRepositoryError


STAGES: tuple[str, ...] = ("A1", "A2", "A3")
AGENT_BY_STAGE: Mapping[str, str] = {"A1": "AGENT_1", "A2": "AGENT_2", "A3": "AGENT_3"}
_SYMBOL = re.compile(
    r"(?<![A-Za-z0-9_])(?:(?P<code>\d{6})\.(?P<suffix>SH|SZ|BJ)|"
    r"(?P<prefix>SHSE|SZSE|BJSE|XSHG|XSHE)\.(?P<prefix_code>\d{6}))(?![A-Za-z0-9_])",
    re.IGNORECASE,
)
_EXCHANGE_CANONICAL = {
    "SH": "SH",
    "SZ": "SZ",
    "BJ": "BJ",
    "SHSE": "SH",
    "XSHG": "SH",
    "SZSE": "SZ",
    "XSHE": "SZ",
    "BJSE": "BJ",
}
_CANDIDATE_KEYS = {
    "active_research_pool",
    "monitor_pool",
    "focus_pool",
    "watch_only_pool",
    "crowded_pool",
    "low_identity_pool",
    "core_watch_pool",
    "secondary_watch_pool",
    "rejected_candidates",
    "candidates",
    "candidate_pool",
}
_REASONING_KEYS = {"reasoning", "reasoning_content", "thinking", "chain_of_thought", "cot"}
_PERMISSION_KEYS = {
    "live_trading",
    "external_orders",
    "real_trading",
    "send_order",
    "broker_order",
    "order_permission",
}
_ALLOWED_DISABLED = {False, None, "", "DISABLED", "DISABLE", "OFF", "SHADOW", "SIMULATION"}


class ResearchPipelineError(RuntimeError):
    """Safe pipeline error for invalid construction or snapshot shape."""

    def __init__(self, reason_code: str):
        self.reason_code = reason_code
        super().__init__(f"research pipeline {reason_code}")


@dataclass(frozen=True, slots=True)
class FrozenInputSnapshot(Mapping[str, Any]):
    """Small immutable wrapper accepted by :class:`ResearchPipeline`.

    The workflow also accepts ordinary mappings, which is useful for tests and
    for adapters that already provide a frozen snapshot contract.
    """

    snapshot_id: str
    data: Mapping[str, Any]
    snapshot_hash: str = ""
    as_of: Any = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "data", MappingProxyType(dict(self.data)))
        if not self.snapshot_hash:
            object.__setattr__(self, "snapshot_hash", _sha256_json(self.data))

    def __getitem__(self, key: str) -> Any:
        return self.data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.data)

    def __len__(self) -> int:
        return len(self.data)


@dataclass(frozen=True, slots=True)
class StageAudit:
    lane: str
    model: str
    stage: str
    status: str
    snapshot_id: str
    prompt_hash: str | None
    input_hash: str | None
    output_hash: str | None
    latency_ms: int | None
    attempts: int
    thinking_variant: str | None
    symbols: tuple[str, ...]
    reason_codes: tuple[str, ...]
    output: Mapping[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "lane": self.lane,
            "model": self.model,
            "stage": self.stage,
            "status": self.status,
            "snapshot_id": self.snapshot_id,
            "prompt_hash": self.prompt_hash,
            "input_hash": self.input_hash,
            "output_hash": self.output_hash,
            "latency_ms": self.latency_ms,
            "attempts": self.attempts,
            "thinking_variant": self.thinking_variant,
            "symbols": list(self.symbols),
            "reason_codes": list(self.reason_codes),
            "output": dict(self.output) if isinstance(self.output, Mapping) else None,
        }


@dataclass(frozen=True, slots=True)
class LaneResult:
    lane: str
    model: str
    status: str
    stages: tuple[StageAudit, ...]
    final_output: Mapping[str, Any] | None = None
    audit_path: Path | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "lane": self.lane,
            "model": self.model,
            "status": self.status,
            "stages": [stage.as_dict() for stage in self.stages],
            "final_output": dict(self.final_output) if isinstance(self.final_output, Mapping) else None,
        }


@dataclass(frozen=True, slots=True)
class ResearchRunResult:
    run_id: str
    generated_at: datetime
    snapshot_id: str
    snapshot_hash: str | None
    status: str
    lanes: tuple[LaneResult, ...]
    audit_paths: tuple[Path, ...]
    markdown_path: Path | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "generated_at": self.generated_at.isoformat(),
            "snapshot_id": self.snapshot_id,
            "snapshot_hash": self.snapshot_hash,
            "status": self.status,
            "lanes": [lane.as_dict() for lane in self.lanes],
            "audit_paths": [str(path) for path in self.audit_paths],
            "markdown_path": str(self.markdown_path) if self.markdown_path else None,
        }


class ResearchPipeline:
    """Run three independent research-model lanes with fail-closed gates."""

    def __init__(
        self,
        settings: Settings,
        *,
        prompt_repository: PromptRepository | str | Path | None = None,
        model_client: Any | None = None,
        output_dir: str | Path | None = None,
        now: Callable[[], datetime] | None = None,
        parallel_lanes: bool = False,
    ):
        self.settings = settings
        if prompt_repository is None:
            configured = os.environ.get("LIANGJIAN_PROMPT_DIR")
            prompt_repository = Path(configured) if configured else settings.root / "prompts"
        self.prompts = (
            prompt_repository
            if isinstance(prompt_repository, PromptRepository)
            else PromptRepository(prompt_repository)
        )
        self.model_client = model_client or OpenAICompatibleModelClient(settings)
        self.output_dir = Path(output_dir or settings.root / "outputs" / "research").resolve()
        self.now = now or (lambda: datetime.now(ZoneInfo(settings.timezone)))
        self.parallel_lanes = bool(parallel_lanes)

    def run(
        self,
        snapshot: FrozenInputSnapshot | Mapping[str, Any] | Any,
        *,
        run_id: str | None = None,
        generated_at: datetime | None = None,
    ) -> ResearchRunResult:
        current = generated_at or self.now()
        if current.tzinfo is None or current.utcoffset() is None:
            current = current.replace(tzinfo=ZoneInfo(self.settings.timezone))
        try:
            frozen = _coerce_snapshot(snapshot)
            g0 = _extract_g0(frozen.data)
        except ResearchPipelineError as exc:
            frozen = FrozenInputSnapshot(snapshot_id="UNKNOWN", data={})
            g0 = set()
            global_reason = exc.reason_code
        else:
            global_reason = None if frozen.snapshot_id else "SNAPSHOT_ID_MISSING"
            if not g0:
                global_reason = global_reason or "G0_UNPROVABLE"

        effective_run_id = _safe_run_id(run_id or _default_run_id(current, frozen.snapshot_id))
        try:
            bundle = self.prompts.load()
        except PromptRepositoryError:
            bundle = None
            global_reason = global_reason or "PROMPT_REPOSITORY_BLOCKED"
        except (OSError, UnicodeError, ValueError):
            bundle = None
            global_reason = global_reason or "PROMPT_REPOSITORY_BLOCKED"

        models = tuple(self.settings.research_models)
        if models != RESEARCH_MODELS:
            global_reason = global_reason or "RESEARCH_MODEL_CONFIG_INVALID"
        def execute_lane(index: int, model: str) -> LaneResult:
            lane_id = f"lane_{index}"
            lane = self._run_lane(
                lane_id=lane_id,
                model=model,
                snapshot=frozen,
                g0=g0,
                bundle=bundle,
                run_id=effective_run_id,
                global_reason=global_reason,
            )
            audit_path = self._write_lane_audit(effective_run_id, lane)
            return LaneResult(
                lane=lane.lane,
                model=lane.model,
                status=lane.status,
                stages=lane.stages,
                final_output=lane.final_output,
                audit_path=audit_path,
            )

        indexed_models = tuple(enumerate(models, start=1))
        if self.parallel_lanes:
            with ThreadPoolExecutor(max_workers=3, thread_name_prefix="liangjian-lane") as executor:
                lanes = list(executor.map(lambda item: execute_lane(*item), indexed_models))
        else:
            lanes = [execute_lane(index, model) for index, model in indexed_models]

        ready_count = sum(lane.status == "READY" for lane in lanes)
        overall = "READY" if lanes and ready_count == len(lanes) else "PARTIAL" if ready_count else "BLOCKED"
        result = ResearchRunResult(
            run_id=effective_run_id,
            generated_at=current,
            snapshot_id=frozen.snapshot_id,
            snapshot_hash=frozen.snapshot_hash if frozen.snapshot_id != "UNKNOWN" else None,
            status=overall,
            lanes=tuple(lanes),
            audit_paths=tuple(lane.audit_path for lane in lanes if lane.audit_path is not None),
            markdown_path=None,
        )
        markdown_path = self._write_markdown(result)
        return ResearchRunResult(
            run_id=result.run_id,
            generated_at=result.generated_at,
            snapshot_id=result.snapshot_id,
            snapshot_hash=result.snapshot_hash,
            status=result.status,
            lanes=result.lanes,
            audit_paths=result.audit_paths,
            markdown_path=markdown_path,
        )

    def _run_lane(
        self,
        *,
        lane_id: str,
        model: str,
        snapshot: FrozenInputSnapshot,
        g0: set[str],
        bundle: PromptBundle | None,
        run_id: str,
        global_reason: str | None,
    ) -> LaneResult:
        audits: list[StageAudit] = []
        upstream_output: Mapping[str, Any] | None = None
        upstream_symbols = set(g0)
        for stage in STAGES:
            if global_reason:
                audits.append(self._blocked_stage(lane_id, model, stage, snapshot.snapshot_id, global_reason))
                continue
            previous = audits[-1] if audits else None
            if previous is not None and previous.status != "VALIDATED":
                audits.append(
                    self._blocked_stage(lane_id, model, stage, snapshot.snapshot_id, "UPSTREAM_STAGE_BLOCKED")
                )
                continue
            if previous is not None and not previous.symbols:
                audits.append(
                    self._blocked_stage(lane_id, model, stage, snapshot.snapshot_id, "UPSTREAM_POOL_EMPTY")
                )
                continue
            audit = self._run_stage(
                lane_id=lane_id,
                model=model,
                stage=stage,
                snapshot=snapshot,
                upstream_output=upstream_output,
                upstream_symbols=upstream_symbols,
                bundle=bundle,
                run_id=run_id,
            )
            audits.append(audit)
            if audit.status == "VALIDATED":
                upstream_output = audit.output
                upstream_symbols = set(audit.symbols)

        status = "READY" if len(audits) == 3 and all(item.status == "VALIDATED" for item in audits) else "BLOCKED"
        final_output = audits[-1].output if status == "READY" else None
        return LaneResult(lane=lane_id, model=model, status=status, stages=tuple(audits), final_output=final_output)

    def _run_stage(
        self,
        *,
        lane_id: str,
        model: str,
        stage: str,
        snapshot: FrozenInputSnapshot,
        upstream_output: Mapping[str, Any] | None,
        upstream_symbols: set[str],
        bundle: PromptBundle | None,
        run_id: str,
    ) -> StageAudit:
        if bundle is None:
            return self._blocked_stage(lane_id, model, stage, snapshot.snapshot_id, "PROMPT_REPOSITORY_BLOCKED")
        try:
            replacements = _prompt_replacements(bundle, stage, snapshot, upstream_output)
            shared = bundle.render("00_shared_system_v2.txt", replacements)
            stage_prompt = bundle.render_stage(stage, replacements)
            prompt_hash = digest_text(shared + "\n" + stage_prompt)
            runtime = _runtime_input(snapshot, lane_id, model, stage, upstream_output, upstream_symbols)
            input_hash = _sha256_json(runtime)
            # Snapshot fields are already rendered into the immutable stage
            # prompt placeholders.  Sending the complete snapshot again in
            # the user message duplicated hundreds of kilobytes and could
            # exceed gateway context/body limits.  Keep it in ``runtime`` for
            # the lineage hash, but send only the compact execution envelope.
            model_runtime = {key: value for key, value in runtime.items() if key != "snapshot_data"}
            messages = [
                {"role": "system", "content": shared + "\n\n" + stage_prompt},
                {"role": "user", "content": "RUNTIME_INPUT\n" + _canonical_json(model_runtime)},
            ]
        except (PromptRepositoryError, TypeError, ValueError):
            return StageAudit(
                lane=lane_id,
                model=model,
                stage=stage,
                status="BLOCKED",
                snapshot_id=snapshot.snapshot_id,
                prompt_hash=None,
                input_hash=None,
                output_hash=None,
                latency_ms=None,
                attempts=0,
                thinking_variant=None,
                symbols=(),
                reason_codes=("PROMPT_RENDER_BLOCKED",),
            )

        try:
            result = self._call_model(
                model,
                messages,
                prompt_hash=prompt_hash,
                input_hash=input_hash,
                snapshot_id=snapshot.snapshot_id,
                stage=stage,
            )
        except ModelClientError as exc:
            return StageAudit(
                lane=lane_id,
                model=model,
                stage=stage,
                status="BLOCKED",
                snapshot_id=snapshot.snapshot_id,
                prompt_hash=prompt_hash,
                input_hash=input_hash,
                output_hash=None,
                latency_ms=None,
                attempts=max(0, _safe_int(getattr(exc, "attempts", 0))),
                thinking_variant=None,
                symbols=(),
                reason_codes=(exc.reason_code,),
            )
        except (OSError, TypeError, ValueError):
            return StageAudit(
                lane=lane_id,
                model=model,
                stage=stage,
                status="BLOCKED",
                snapshot_id=snapshot.snapshot_id,
                prompt_hash=prompt_hash,
                input_hash=input_hash,
                output_hash=None,
                latency_ms=None,
                attempts=0,
                thinking_variant=None,
                symbols=(),
                reason_codes=("MODEL_CALL_FAILED",),
            )

        output = _strip_reasoning(result.output)
        output_hash = _sha256_json(output)
        symbols = tuple(sorted(_approved_symbols(output, stage)))
        reasons = _validate_output(
            output,
            stage=stage,
            model=model,
            snapshot_id=snapshot.snapshot_id,
            upstream_symbols=upstream_symbols,
        )
        envelope = output.get("envelope") if isinstance(output, Mapping) else None
        model_status = envelope.get("status") if isinstance(envelope, Mapping) else None
        status = "VALIDATED" if not reasons and model_status != "BLOCKED" else "BLOCKED"
        if model_status == "BLOCKED" and "MODEL_DECLARED_BLOCKED" not in reasons:
            reasons.append("MODEL_DECLARED_BLOCKED")
        return StageAudit(
            lane=lane_id,
            model=model,
            stage=stage,
            status=status,
            snapshot_id=snapshot.snapshot_id,
            prompt_hash=prompt_hash,
            input_hash=input_hash,
            output_hash=output_hash,
            latency_ms=result.latency_ms,
            attempts=result.attempts,
            thinking_variant=result.thinking_variant,
            symbols=symbols,
            reason_codes=tuple(dict.fromkeys(reasons)),
            output=output,
        )

    def _call_model(self, model: str, messages: Sequence[Mapping[str, Any]], **metadata: Any) -> ModelCallResult:
        operation = getattr(self.model_client, "complete", None) or getattr(self.model_client, "call", None)
        if operation is None:
            raise ModelClientError("MODEL_CLIENT_INVALID")
        try:
            raw = operation(model, messages, **metadata)
        except TypeError as exc:
            # Permit tiny offline test doubles that expose only (model,
            # messages), while never hiding non-TypeError model failures.
            message = str(exc).lower()
            if "unexpected keyword" not in message and "positional argument" not in message:
                raise ModelClientError("MODEL_CLIENT_CALL_TYPE_ERROR") from exc
            try:
                raw = operation(model, messages)
            except TypeError:
                raise ModelClientError("MODEL_CLIENT_SIGNATURE_INVALID") from exc
        if isinstance(raw, ModelCallResult):
            return raw
        if isinstance(raw, Mapping):
            output = raw.get("output", raw.get("json", raw.get("data", raw)))
            if not isinstance(output, Mapping):
                raise ModelClientError("MODEL_OUTPUT_NOT_OBJECT")
            return ModelCallResult(
                model=str(raw.get("model", model)),
                output=dict(output),
                prompt_hash=raw.get("prompt_hash", metadata.get("prompt_hash")),
                input_hash=raw.get("input_hash", metadata.get("input_hash")),
                latency_ms=_safe_int(raw.get("latency_ms", 0)),
                attempts=max(1, _safe_int(raw.get("attempts", 1))),
                thinking_variant=str(raw.get("thinking_variant", "unknown")),
                reasoning_tokens=_safe_optional_int(raw.get("reasoning_tokens")),
            )
        output = getattr(raw, "output", None)
        if isinstance(output, Mapping):
            return ModelCallResult(
                model=str(getattr(raw, "model", model)),
                output=dict(output),
                prompt_hash=getattr(raw, "prompt_hash", metadata.get("prompt_hash")),
                input_hash=getattr(raw, "input_hash", metadata.get("input_hash")),
                latency_ms=_safe_int(getattr(raw, "latency_ms", 0)),
                attempts=max(1, _safe_int(getattr(raw, "attempts", 1))),
                thinking_variant=str(getattr(raw, "thinking_variant", "unknown")),
                reasoning_tokens=_safe_optional_int(getattr(raw, "reasoning_tokens", None)),
            )
        raise ModelClientError("MODEL_OUTPUT_NOT_OBJECT")

    def _blocked_stage(self, lane: str, model: str, stage: str, snapshot_id: str, reason: str) -> StageAudit:
        return StageAudit(
            lane=lane,
            model=model,
            stage=stage,
            status="BLOCKED",
            snapshot_id=snapshot_id,
            prompt_hash=None,
            input_hash=None,
            output_hash=None,
            latency_ms=None,
            attempts=0,
            thinking_variant=None,
            symbols=(),
            reason_codes=(reason,),
        )

    def _write_lane_audit(self, run_id: str, lane: LaneResult) -> Path:
        path = self.output_dir / f"research_{run_id}_{_safe_run_id(lane.lane)}.json"
        return atomic_write_json(path, lane.as_dict())

    def _write_markdown(self, result: ResearchRunResult) -> Path:
        lines = [
            "# 量见 A 股三模型研究漏斗报告",
            "",
            "> 内部模拟、非投资建议。该报告不连接真实证券账户，不发送真实委托。",
            "",
            f"- 运行时间：`{result.generated_at.isoformat()}`",
            f"- run_id：`{result.run_id}`",
            f"- snapshot_id：`{result.snapshot_id}`",
            f"- snapshot_hash：`{result.snapshot_hash or '-'}`",
            f"- 总状态：`{result.status}`",
            "",
            "| Lane | Model | A1 | A2 | A3 | Lane 状态 |",
            "|---|---|---|---|---|---|",
        ]
        for lane in result.lanes:
            by_stage = {stage.stage: stage.status for stage in lane.stages}
            lines.append(
                f"| {lane.lane} | {lane.model} | {by_stage.get('A1', 'BLOCKED')} | "
                f"{by_stage.get('A2', 'BLOCKED')} | {by_stage.get('A3', 'BLOCKED')} | {lane.status} |"
            )
        lines.extend(["", "## 阶段审计", ""])
        for lane in result.lanes:
            lines.append(f"### {lane.lane} · {lane.model}")
            lines.append("")
            for stage in lane.stages:
                reasons = ", ".join(stage.reason_codes) if stage.reason_codes else "-"
                latency = f"{stage.latency_ms} ms" if stage.latency_ms is not None else "-"
                lines.append(
                    f"- {stage.stage}：`{stage.status}`；原因：`{reasons}`；"
                    f"请求次数：`{stage.attempts}`；耗时：`{latency}`"
                )
            lines.append("")
        lines.extend(["", "## 最终候选/计划 JSON", ""])
        for lane in result.lanes:
            lines.extend(
                [
                    f"### {lane.lane} · {lane.model}",
                    "",
                    "```json",
                    json.dumps(lane.final_output or {}, ensure_ascii=False, indent=2, sort_keys=True, default=str),
                    "```",
                    "",
                ]
            )
        path = self.output_dir / f"research_{result.run_id}.md"
        return atomic_write_text(path, "\n".join(lines))


def _coerce_snapshot(snapshot: FrozenInputSnapshot | Mapping[str, Any] | Any) -> FrozenInputSnapshot:
    if isinstance(snapshot, FrozenInputSnapshot):
        if not snapshot.snapshot_id:
            raise ResearchPipelineError("SNAPSHOT_ID_MISSING")
        return snapshot
    if isinstance(snapshot, Mapping):
        raw = dict(snapshot)
    elif hasattr(snapshot, "model_dump"):
        try:
            raw = dict(snapshot.model_dump(mode="python"))
        except (TypeError, ValueError):
            raw = {}
    else:
        raw = dict(vars(snapshot)) if hasattr(snapshot, "__dict__") else {}
    snapshot_id = raw.get("snapshot_id") or raw.get("id")
    nested = raw.get("data")
    data = dict(nested) if isinstance(nested, Mapping) else raw
    if not snapshot_id:
        raise ResearchPipelineError("SNAPSHOT_ID_MISSING")
    provided_hash = raw.get("snapshot_hash") or data.get("snapshot_hash") or ""
    return FrozenInputSnapshot(
        snapshot_id=str(snapshot_id),
        data=data,
        snapshot_hash=str(provided_hash) if provided_hash else _sha256_json(data),
        as_of=raw.get("as_of") or data.get("as_of"),
    )


def _extract_g0(data: Mapping[str, Any]) -> set[str]:
    candidates: list[Any] = []
    # The canonical frozen snapshot exposes these typed candidate collections.
    # ``trade_candidates`` is preferred because it is the deterministic,
    # tradable G0 passed to the research chain; the generic mapping form still
    # accepts the broader universe aliases below.
    preferred = data.get("trade_candidates")
    if preferred is not None:
        candidates.append(preferred)
    else:
        for key, value in data.items():
            normalized = str(key).lower().replace("-", "_")
            if normalized in {
                "g0",
                "g0_symbols",
                "g0_universe",
                "universe",
                "universe_symbols",
                "research_universe",
                "research_universe_symbols",
                "tradable_universe",
                "research_candidates",
                "universe_candidates",
            }:
                candidates.append(value)
    symbols: set[str] = set()
    for value in candidates:
        symbols.update(_scan_symbols(value))
    return symbols


def _prompt_replacements(
    bundle: PromptBundle,
    stage: str,
    snapshot: FrozenInputSnapshot,
    upstream_output: Mapping[str, Any] | None,
) -> dict[str, Any]:
    names = set(bundle.shared.placeholders)
    names.update(bundle.document({"A1": "agent_1_macro_chain_v2.txt", "A2": "agent_2_theme_sentiment_v2.txt", "A3": "agent_3_technical_planner_v2.txt"}[stage]).placeholders)
    replacements: dict[str, Any] = {}
    for name in names:
        if name == "UPSTREAM_ACTIVE_POOL" or name == "UPSTREAM_FOCUS_POOL":
            replacements[name] = upstream_output if upstream_output is not None else None
            continue
        if name == "SNAPSHOT_MANIFEST":
            replacements[name] = snapshot.data.get("snapshot_manifest", snapshot.data)
            continue
        found, value = _lookup_field(snapshot.data, name)
        replacements[name] = value if found else None
    return replacements


def _runtime_input(
    snapshot: FrozenInputSnapshot,
    lane: str,
    model: str,
    stage: str,
    upstream_output: Mapping[str, Any] | None,
    upstream_symbols: set[str],
) -> dict[str, Any]:
    return {
        "snapshot_id": snapshot.snapshot_id,
        "snapshot_hash": snapshot.snapshot_hash,
        "as_of": snapshot.as_of,
        "lane": lane,
        "model_name": model,
        "stage": stage,
        "required_envelope": {
            "stage_id": AGENT_BY_STAGE[stage],
            "model_name": model,
            "input_snapshot_ids": [snapshot.snapshot_id],
            "config_version": snapshot.data.get("config_version", "funnel-config-v2"),
            "market_regime": (
                snapshot.data.get("MARKET_REGIME_SNAPSHOT", {}).get("regime", "ROTATION_NO_MAINLINE")
                if isinstance(snapshot.data.get("MARKET_REGIME_SNAPSHOT"), Mapping)
                else "ROTATION_NO_MAINLINE"
            ),
            "status": "OK|DEGRADED|BLOCKED",
        },
        "g0_symbols": sorted(_extract_g0(snapshot.data)),
        "upstream_symbols": sorted(upstream_symbols),
        "upstream_output": upstream_output,
        "snapshot_data": snapshot.data,
    }


def _lookup_field(data: Mapping[str, Any], name: str) -> tuple[bool, Any]:
    target = name.lower().replace("-", "_")
    for key, value in data.items():
        if str(key).lower().replace("-", "_") == target:
            return True, value
    return False, None


def _validate_output(
    output: Mapping[str, Any],
    *,
    stage: str,
    model: str,
    snapshot_id: str,
    upstream_symbols: set[str],
) -> list[str]:
    reasons: list[str] = []
    envelope = output.get("envelope")
    if not isinstance(envelope, Mapping):
        reasons.append("ENVELOPE_MISSING")
        return reasons
    required = {"stage_id", "status", "input_snapshot_ids", "model_name", "config_version", "prompt_version", "market_regime"}
    reasons.extend(f"ENVELOPE_FIELD_MISSING:{key}" for key in sorted(required.difference(envelope)))
    if envelope.get("stage_id") != AGENT_BY_STAGE[stage]:
        reasons.append("STAGE_ID_MISMATCH")
    if envelope.get("model_name") != model:
        reasons.append("MODEL_NAME_MISMATCH")
    if envelope.get("status") not in {"OK", "DEGRADED", "BLOCKED"}:
        reasons.append("ENVELOPE_STATUS_INVALID")
    input_snapshot_ids = envelope.get("input_snapshot_ids")
    if not isinstance(input_snapshot_ids, list) or snapshot_id not in input_snapshot_ids:
        reasons.append("SNAPSHOT_LINEAGE_MISSING")
    reasons.extend(_permission_violations(output))
    symbols = _scan_symbols(output)
    outside = sorted(symbols.difference(upstream_symbols))
    if outside:
        reasons.append("POOL_OUTSIDE_G0" if stage == "A1" else "POOL_OUTSIDE_UPSTREAM")
    if _unprovable_candidate_pool(output):
        reasons.append("CANDIDATE_LINEAGE_UNPROVABLE")
    return list(dict.fromkeys(reasons))


def _approved_symbols(output: Mapping[str, Any], stage: str) -> set[str]:
    key = {"A1": "active_research_pool", "A2": "focus_pool", "A3": "core_watch_pool"}[stage]
    return _scan_symbols(output.get(key, ()))


def _permission_violations(value: Any) -> list[str]:
    violations: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized = str(key).lower().replace("-", "_")
            disabled = item is None or (
                isinstance(item, (bool, str, int, float)) and item in _ALLOWED_DISABLED
            )
            if normalized in _PERMISSION_KEYS and not disabled:
                if isinstance(item, str) and item.upper() in {"FALSE", "NO", "NONE", "DISABLED", "OFF"}:
                    pass
                else:
                    violations.append("PERMISSION_ESCALATION")
            violations.extend(_permission_violations(item))
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for item in value:
            violations.extend(_permission_violations(item))
    return list(dict.fromkeys(violations))


def _unprovable_candidate_pool(value: Any, *, parent_key: str = "") -> bool:
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized = str(key).lower().replace("-", "_")
            if normalized in _CANDIDATE_KEYS and isinstance(item, Sequence) and not isinstance(item, (str, bytes, bytearray)):
                if item and not _scan_symbols(item):
                    return True
            if _unprovable_candidate_pool(item, parent_key=normalized):
                return True
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return any(_unprovable_candidate_pool(item, parent_key=parent_key) for item in value)
    return False


def _scan_symbols(value: Any) -> set[str]:
    symbols: set[str] = set()
    if isinstance(value, Mapping):
        for item in value.values():
            symbols.update(_scan_symbols(item))
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for item in value:
            symbols.update(_scan_symbols(item))
    elif isinstance(value, str):
        for match in _SYMBOL.finditer(value):
            raw_exchange = (match.group("suffix") or match.group("prefix") or "").upper()
            code = match.group("code") or match.group("prefix_code")
            exchange = _EXCHANGE_CANONICAL[raw_exchange]
            symbols.add(f"{code}.{exchange}")
    return symbols


def _strip_reasoning(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _strip_reasoning(item)
            for key, item in value.items()
            if str(key).lower() not in _REASONING_KEYS
        }
    if isinstance(value, list):
        return [_strip_reasoning(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_strip_reasoning(item) for item in value)
    return value


def _canonical_json(value: Any) -> str:
    return json.dumps(_jsonable(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _jsonable(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        try:
            return _jsonable(value.model_dump(mode="json"))
        except (TypeError, ValueError):
            return str(value)
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _default_run_id(current: datetime, snapshot_id: str) -> str:
    stamp = current.strftime("%Y%m%dT%H%M%S%z")
    return f"{stamp}_{_sha256_json(snapshot_id)[:12]}"


def _safe_run_id(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value)).strip("._")
    return cleaned or "run"


def _safe_int(value: Any) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def _safe_optional_int(value: Any) -> int | None:
    try:
        parsed = int(value)
        return parsed if parsed >= 0 else None
    except (TypeError, ValueError):
        return None


__all__ = [
    "FrozenInputSnapshot",
    "LaneResult",
    "ResearchPipeline",
    "ResearchPipelineError",
    "ResearchRunResult",
    "StageAudit",
]
