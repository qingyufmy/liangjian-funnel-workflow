"""Explicit Team V2 prompt bundle, isolated from the production legacy bundle.

Loads Claude-authored bytes without rewriting roles. It is not an activation
flag: live callers are integrated only after the corresponding role schema.
"""
from dataclasses import dataclass
import hashlib
import re
from pathlib import Path
from collections.abc import Mapping
from typing import Any
from types import MappingProxyType

from .prompts import PromptDocument, PromptRepositoryError, _replacement_text
from .team_v2_contract import blind_plan_facts

_FILES = {"A1": "agent_1_macro_chain_v3.txt", "A2": "agent_2_theme_sentiment_v3.txt",
          "A3": "agent_3_portfolio_planner_v4.txt", "A4": "agent_4_trader_v1.txt",
          "A5": "agent_5_daily_reviewer_v2.txt", "A6": "agent_6_risk_governor_v1.txt"}
_VERSIONS = {"A1": "agent-1/3.0.0", "A2": "agent-2/3.0.0", "A3": "agent-3/4.0.0",
             "A4": "agent-4-trader/1.0.0", "A5": "agent-5/2.0.0", "A6": "agent-6-risk/1.0.0"}
_TOKEN = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")


@dataclass(frozen=True)
class TeamPromptRequest:
    role: str
    shared_version: str
    role_version: str
    system: str
    sha256: str
    source_hashes: Mapping[str, str]


class TeamPromptRepository:
    def __init__(self, directory: str | Path):
        self.directory = Path(directory).resolve()

    def _read(self, filename: str) -> PromptDocument:
        path = self.directory / filename
        try:
            raw = path.read_bytes(); text = raw.decode("utf-8-sig")
        except (OSError, UnicodeError) as exc:
            raise PromptRepositoryError("PROMPT_FILE_UNREADABLE", filename=filename) from exc
        if not text.strip() or "\x00" in text:
            raise PromptRepositoryError("PROMPT_FILE_INVALID", filename=filename)
        return PromptDocument(filename, path, text, hashlib.sha256(raw).hexdigest())

    def document(self, role: str) -> PromptDocument:
        if role not in _FILES:
            raise ValueError("TEAM_ROLE_INVALID")
        return self._read(_FILES[role])

    def prepare(self, role: str, replacements: Mapping[str, Any], *, session: str | None = None) -> TeamPromptRequest:
        shared = self._read("00_shared_system_v3.txt"); document = self.document(role)
        if role == "A1" and "【v3 修订清单】" in document.text:
            raise ValueError("A1_COMPOSITION_REQUIRED")
        if f"版本: {_VERSIONS[role]}" not in document.text or "版本: shared-system/3.0.0" not in shared.text:
            raise ValueError("TEAM_PROMPT_VERSION_MISMATCH")
        values = dict(replacements)
        role_text = document.text
        if role == "A3":
            if session not in {"PLAN_REVIEW", "PM_PREMARKET", "PM_POST_CLOSE"}:
                raise ValueError("TEAM_SESSION_REQUIRED")
            # Verbatim section routing is wiring, not a rewrite of Claude's rules.
            preface, sep, remainder = role_text.partition("═══════ 会话 ")
            sections = (sep+remainder).split("═══════ 会话 ")[1:]
            selected = [section for section in sections if section.startswith(session+"（")]
            if len(selected) != 1: raise ValueError("TEAM_SESSION_TEMPLATE_MISSING")
            role_text = preface+"═══════ 会话 "+selected[0]
            if session == "PLAN_REVIEW":
                for key in ("A3_FACT_SHEET", "A3_CANDIDATE_POOL"):
                    if key in values: values[key] = blind_plan_facts(values[key])
        if role == "A6" and session is not None:
            if session not in {"PREMARKET", "INTRADAY_EXCEPTION", "POST_CLOSE"}:
                raise ValueError("TEAM_SESSION_INVALID")
            if values.get("RISK_SESSION") != session:
                raise ValueError("TEAM_SESSION_BINDING_MISMATCH")
        needed = set(shared.placeholders) | {m.group(1) for m in _TOKEN.finditer(role_text)}
        if not needed.issubset(values):
            raise ValueError("TEAM_UNRESOLVED_PLACEHOLDER")
        text = shared.text + "\n\n" + role_text
        rendered = _TOKEN.sub(lambda m: _replacement_text(values[m.group(1)]), text)
        if "{{" in rendered or "\x00" in rendered:
            raise ValueError("TEAM_UNRESOLVED_PLACEHOLDER")
        return TeamPromptRequest(role, "shared-system/3.0.0", _VERSIONS[role], rendered,
                                 hashlib.sha256(rendered.encode()).hexdigest(),
                                 MappingProxyType({shared.name: shared.sha256, document.name: document.sha256}))
