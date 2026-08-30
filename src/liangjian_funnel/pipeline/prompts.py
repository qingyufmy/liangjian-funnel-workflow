"""Read-only prompt template loading for the research funnel.

The prompt files are part of the research contract.  This module deliberately
does not edit, normalise, or "repair" them.  It reads the complete UTF-8 file,
keeps a content digest, and only permits a model call after every template
placeholder has been replaced.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


PROMPT_FILENAMES: tuple[str, ...] = (
    "00_shared_system_v2.txt",
    "agent_1_macro_chain_v2.txt",
    "agent_2_theme_sentiment_v2.txt",
    "agent_3_technical_planner_v2.txt",
    "agent_4_intraday_signal_v2.txt",
    "agent_4_intraday_veto_v3.txt",
    "agent_5_review_calibrator_v2.txt",
)

STAGE_PROMPT_FILES: Mapping[str, str] = {
    "A1": "agent_1_macro_chain_v2.txt",
    "A2": "agent_2_theme_sentiment_v2.txt",
    "A3": "agent_3_technical_planner_v2.txt",
}

_PLACEHOLDER = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")
_ANY_PLACEHOLDER = re.compile(r"\{\{[^{}]*\}\}")


class PromptRepositoryError(RuntimeError):
    """A safe, stable prompt repository error.

    The exception text never includes file contents or replacement values.
    """

    def __init__(self, reason_code: str, *, filename: str | None = None):
        self.reason_code = reason_code
        self.filename = filename
        suffix = f":{filename}" if filename else ""
        super().__init__(f"prompt repository {reason_code}{suffix}")


@dataclass(frozen=True, slots=True)
class PromptDocument:
    name: str
    path: Path
    text: str
    sha256: str

    @property
    def placeholders(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(match.group(1) for match in _PLACEHOLDER.finditer(self.text)))


@dataclass(frozen=True, slots=True)
class PromptBundle:
    """The immutable prompt documents and their aggregate digest."""

    directory: Path
    documents: Mapping[str, PromptDocument]
    sha256: str

    def document(self, name: str) -> PromptDocument:
        try:
            return self.documents[name]
        except KeyError as exc:
            raise PromptRepositoryError("PROMPT_NOT_LOADED", filename=name) from exc

    @property
    def shared(self) -> PromptDocument:
        return self.document("00_shared_system_v2.txt")

    def render(self, name: str, replacements: Mapping[str, Any]) -> str:
        """Render one document and fail closed on every unresolved token.

        Missing replacements are an error rather than an implicit empty string.
        A caller that intentionally has no value should pass ``None``; it is
        rendered as ``-`` to preserve the prompt's explicit missing-data rule.
        """

        document = self.document(name)
        values = dict(replacements)
        expected = set(document.placeholders)
        missing = sorted(expected.difference(values))
        if missing:
            raise PromptRepositoryError("UNRESOLVED_PLACEHOLDER", filename=name)

        def replace(match: re.Match[str]) -> str:
            key = match.group(1)
            return _replacement_text(values[key])

        rendered = _PLACEHOLDER.sub(replace, document.text)
        if _ANY_PLACEHOLDER.search(rendered):
            raise PromptRepositoryError("UNRESOLVED_PLACEHOLDER", filename=name)
        if "\x00" in rendered:
            raise PromptRepositoryError("NUL_IN_RENDERED_PROMPT", filename=name)
        return rendered

    def render_stage(self, stage: str, replacements: Mapping[str, Any]) -> str:
        try:
            filename = STAGE_PROMPT_FILES[stage.upper()]
        except KeyError as exc:
            raise PromptRepositoryError("UNKNOWN_STAGE", filename=str(stage)) from exc
        return self.render(filename, replacements)


class PromptRepository:
    """Load the configured prompt directory without mutating its files."""

    def __init__(self, directory: str | Path):
        self.directory = Path(directory).expanduser().resolve()
        self._bundle: PromptBundle | None = None

    def load(self) -> PromptBundle:
        documents: dict[str, PromptDocument] = {}
        for filename in PROMPT_FILENAMES:
            path = self.directory / filename
            if not path.is_file():
                raise PromptRepositoryError("PROMPT_FILE_MISSING", filename=filename)
            try:
                raw = path.read_bytes()
                text = raw.decode("utf-8-sig")
            except (OSError, UnicodeError) as exc:
                raise PromptRepositoryError("PROMPT_FILE_UNREADABLE", filename=filename) from exc
            if not text or "\x00" in text:
                raise PromptRepositoryError("PROMPT_FILE_INVALID", filename=filename)
            documents[filename] = PromptDocument(
                name=filename,
                path=path,
                text=text,
                sha256=hashlib.sha256(raw).hexdigest(),
            )

        joined = "\n".join(f"{name}:{documents[name].sha256}" for name in PROMPT_FILENAMES)
        self._bundle = PromptBundle(
            directory=self.directory,
            documents=documents,
            sha256=hashlib.sha256(joined.encode("utf-8")).hexdigest(),
        )
        return self._bundle

    def bundle(self) -> PromptBundle:
        return self._bundle or self.load()

    def document(self, name: str) -> PromptDocument:
        return self.bundle().document(name)

    def render(self, name: str, replacements: Mapping[str, Any]) -> str:
        return self.bundle().render(name, replacements)

    def render_stage(self, stage: str, replacements: Mapping[str, Any]) -> str:
        return self.bundle().render_stage(stage, replacements)

    @property
    def sha256(self) -> str:
        return self.bundle().sha256


def _replacement_text(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, str):
        return value
    if isinstance(value, (bool, int, float)):
        return str(value).lower() if isinstance(value, bool) else str(value)
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    except (TypeError, ValueError) as exc:
        raise PromptRepositoryError("REPLACEMENT_NOT_SERIALIZABLE") from exc


__all__ = [
    "PROMPT_FILENAMES",
    "STAGE_PROMPT_FILES",
    "PromptBundle",
    "PromptDocument",
    "PromptRepository",
    "PromptRepositoryError",
]
