from __future__ import annotations

import json
import os
import tempfile
import weakref
from pathlib import Path
from collections.abc import Iterator
from typing import Any

from pydantic import BaseModel
from pydantic_core import PydanticSerializationError, to_jsonable_python

from .contracts import CapabilityReport
from .redaction import SENSITIVE_KEYS, redact_text, sanitize


class _StreamingRedactionContext:
    """Create lazy, JSON-encoder-compatible redacted views.

    ``redaction.sanitize`` deliberately returns a complete object tree.  That
    is convenient for small reports, but it temporarily duplicates large
    snapshots before they can be written.  The views below retain only the
    source object and transform one mapping value/sequence item when the
    standard-library encoder asks for it.  A per-write cache is important: it
    preserves the encoder's circular-reference checks for containers that
    refer to themselves or to one another.
    """

    def __init__(self) -> None:
        # Weak entries preserve source-identity while a container is actively
        # being encoded (needed for circular-reference detection), but release
        # completed row views immediately.  A normal dict retained one wrapper
        # per market row until the entire file finished and therefore was not
        # truly bounded for full-market snapshots.
        self._views: weakref.WeakValueDictionary[tuple[int, bool], Any] = (
            weakref.WeakValueDictionary()
        )

    def wrap(self, value: Any, *, json_mode: bool = False) -> Any:
        if isinstance(value, BaseModel):
            # Pydantic's JSON mode is the mode used by the legacy writer for
            # top-level models.  A model view reads fields lazily; only model
            # serializers that cannot be represented field-by-field fall back
            # to Pydantic's complete dump.
            key = (id(value), True)
            cached = self._views.get(key)
            if cached is not None:
                return cached
            if _has_custom_serializers(value):
                # Custom model/field serializers can change the shape of the
                # model, so preserve their exact Pydantic JSON representation.
                view = self.wrap(value.model_dump(mode="json"), json_mode=False)
            else:
                view = _StreamingRedactedModel(value, self)
            self._views[key] = view
            return view
        if isinstance(value, dict):
            key = (id(value), json_mode)
            cached = self._views.get(key)
            if cached is not None:
                return cached
            view = _StreamingRedactedDict(value, self, json_mode=json_mode)
            self._views[key] = view
            return view
        if isinstance(value, (list, tuple)):
            key = (id(value), json_mode)
            cached = self._views.get(key)
            if cached is not None:
                return cached
            view = _StreamingRedactedList(value, self, json_mode=json_mode)
            self._views[key] = view
            return view
        if isinstance(value, str):
            return redact_text(value)
        if value is None or isinstance(value, (bool, int, float)):
            return value
        if json_mode:
            try:
                # This is a scalar conversion only.  Unlike model_dump on the
                # complete model, it does not walk or duplicate a container.
                return self.wrap(to_jsonable_python(value), json_mode=True)
            except (PydanticSerializationError, TypeError, ValueError):
                pass
        return value


class _StreamingRedactedDict(dict[str, Any]):
    """A dict-shaped lazy view consumed by ``JSONEncoder.iterencode``."""

    def __init__(
        self,
        source: dict[Any, Any],
        context: _StreamingRedactionContext,
        *,
        json_mode: bool = False,
    ) -> None:
        # The backing dict stays empty.  JSONEncoder only requires the dict
        # protocol (truthiness and items()) and will therefore not copy the
        # source mapping into another full tree.
        super().__init__()
        self._source = source
        self._context = context
        self._json_mode = json_mode

    def __len__(self) -> int:
        return len(self._source)

    def __iter__(self) -> Iterator[str]:
        for key in self._source:
            yield str(key)

    def items(self):  # type: ignore[override]
        # Key normalization can make two source keys collide (e.g. 1 and
        # "1").  ``sanitize`` uses a dict comprehension, so the last source
        # value wins; reproduce that behavior before JSONEncoder sorts keys.
        normalized: dict[str, Any] = {}
        for key, value in self._source.items():
            key_text = str(key)
            normalized[key_text] = (
                "[REDACTED]"
                if key_text.lower() in SENSITIVE_KEYS
                else self._context.wrap(value, json_mode=self._json_mode)
            )
        return normalized.items()


class _StreamingRedactedModel(dict[str, Any]):
    """A lazy dict-shaped view over a regular Pydantic model."""

    def __init__(self, source: BaseModel, context: _StreamingRedactionContext) -> None:
        super().__init__()
        self._source = source
        self._context = context

    def _field_names(self) -> list[str]:
        model_fields = getattr(type(self._source), "model_fields", {})
        names = list(model_fields)
        extras = getattr(self._source, "__pydantic_extra__", None) or {}
        names.extend(str(key) for key in extras)
        computed = getattr(type(self._source), "model_computed_fields", {}) or {}
        names.extend(str(key) for key in computed if str(key) not in names)
        return names

    def __len__(self) -> int:
        return len(self._field_names())

    def __iter__(self) -> Iterator[str]:
        return iter(self._field_names())

    def items(self):  # type: ignore[override]
        normalized: dict[str, Any] = {}
        model_fields = getattr(type(self._source), "model_fields", {})
        for name in model_fields:
            key_text = str(name)
            normalized[key_text] = (
                "[REDACTED]"
                if key_text.lower() in SENSITIVE_KEYS
                else self._context.wrap(getattr(self._source, name), json_mode=True)
            )
        extras = getattr(self._source, "__pydantic_extra__", None) or {}
        for key, value in extras.items():
            key_text = str(key)
            normalized[key_text] = (
                "[REDACTED]"
                if key_text.lower() in SENSITIVE_KEYS
                else self._context.wrap(value, json_mode=True)
            )
        computed = getattr(type(self._source), "model_computed_fields", {}) or {}
        for name in computed:
            if str(name) not in normalized:
                key_text = str(name)
                normalized[key_text] = (
                    "[REDACTED]"
                    if key_text.lower() in SENSITIVE_KEYS
                    else self._context.wrap(getattr(self._source, name), json_mode=True)
                )
        return normalized.items()


class _StreamingRedactedList(list[Any]):
    """A list-shaped lazy view for source lists and tuples."""

    def __init__(
        self,
        source: list[Any] | tuple[Any, ...],
        context: _StreamingRedactionContext,
        *,
        json_mode: bool = False,
    ) -> None:
        super().__init__()
        self._source = source
        self._context = context
        self._json_mode = json_mode

    def __len__(self) -> int:
        return len(self._source)

    def __iter__(self):
        for item in self._source:
            yield self._context.wrap(item, json_mode=self._json_mode)


def _has_custom_serializers(value: BaseModel) -> bool:
    decorators = getattr(type(value), "__pydantic_decorators__", None)
    if decorators is None:
        return False
    return bool(
        getattr(decorators, "model_serializers", {})
        or getattr(decorators, "field_serializers", {})
    )


def _streaming_default(value: Any) -> str:
    """Match ``default=str`` while applying the string redaction policy."""

    return redact_text(str(value))


def atomic_write_text(
    path: Path,
    text: str,
    *,
    mode: int | None = None,
    group_id: int | None = None,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="\n", delete=False, dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
    )
    temporary = Path(handle.name)
    try:
        with handle:
            handle.write(text)
            handle.flush()
            if group_id is not None and hasattr(os, "fchown"):
                try:
                    os.fchown(handle.fileno(), -1, group_id)
                except OSError:
                    # A non-root writer may not be allowed to change group.
                    # Keeping its own group is still safe and preserves the
                    # atomic publication contract.
                    pass
            if mode is not None:
                if hasattr(os, "fchmod"):
                    os.fchmod(handle.fileno(), mode)
                else:
                    os.chmod(temporary, mode)
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return path


def atomic_write_json(
    path: Path,
    value: BaseModel | dict[str, Any],
    *,
    mode: int | None = None,
    group_id: int | None = None,
) -> Path:
    raw = value.model_dump(mode="json") if isinstance(value, BaseModel) else value
    clean = sanitize(raw)
    return atomic_write_text(
        path,
        json.dumps(clean, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        mode=mode,
        group_id=group_id,
    )


def atomic_write_json_streaming(
    path: Path,
    value: BaseModel | dict[str, Any] | list[Any] | tuple[Any, ...],
    *,
    mode: int | None = None,
    group_id: int | None = None,
) -> Path:
    """Atomically write redacted JSON without materializing its JSON text.

    The existing :func:`atomic_write_json` contract is intentionally left
    unchanged.  This variant writes each chunk yielded by
    :class:`json.JSONEncoder` directly to a sibling temporary file, then
    flushes, fsyncs, and atomically replaces ``path``.  Containers are exposed
    through lazy redacted views, so the redaction pass does not create a
    second full object graph in memory.
    """

    path.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="\n",
        delete=False,
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    temporary = Path(handle.name)
    try:
        encoder = json.JSONEncoder(
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            default=_streaming_default,
        )
        view = _StreamingRedactionContext().wrap(value)
        for chunk in encoder.iterencode(view):
            handle.write(chunk)
        handle.write("\n")
        handle.flush()
        if group_id is not None and hasattr(os, "fchown"):
            try:
                os.fchown(handle.fileno(), -1, group_id)
            except OSError:
                # A non-root writer may not be allowed to change group.
                # Keeping its own group is still safe and preserves the
                # atomic publication contract.
                pass
        if mode is not None:
            if hasattr(os, "fchmod"):
                os.fchmod(handle.fileno(), mode)
            else:
                os.chmod(temporary, mode)
        os.fsync(handle.fileno())
    except Exception:
        try:
            handle.close()
        finally:
            temporary.unlink(missing_ok=True)
        raise
    else:
        try:
            handle.close()
            os.replace(temporary, path)
        except Exception:
            temporary.unlink(missing_ok=True)
            raise
    return path


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
