"""Explicit Team V2 prompt bundle, isolated from the production legacy bundle.

Loads Claude-authored bytes without rewriting roles. It is not an activation
flag: live callers are integrated only after the corresponding role schema.
"""
from dataclasses import dataclass
import hashlib
import json
import re
from copy import deepcopy
from pathlib import Path
from collections.abc import Mapping
from typing import Any
from types import MappingProxyType

from .prompts import PromptDocument, PromptRepositoryError, _replacement_text
from .team_v2_contract import blind_plan_facts

_FILES = {"A1": "agent_1_macro_chain_v3.txt", "A2": "agent_2_theme_sentiment_v3.txt",
          "A3": "agent_3_portfolio_planner_v4.txt", "A4": "agent_4_trader_v1.txt",
          "A5": "agent_5_daily_reviewer_v2.txt", "A6": "agent_6_risk_governor_v1.txt"}
_VERSIONS = {"A1": "agent-1/3.0.1", "A2": "agent-2/3.0.1", "A3": "agent-3/4.0.1",
             "A4": "agent-4-trader/1.0.2", "A5": "agent-5/2.0.3", "A6": "agent-6-risk/1.0.3"}
_TOKEN = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")


@dataclass(frozen=True)
class TeamPromptRequest:
    role: str
    shared_version: str
    role_version: str
    system: str
    sha256: str
    source_hashes: Mapping[str, str]


@dataclass(frozen=True)
class TeamMessageRequest:
    prompt: TeamPromptRequest
    messages: tuple[Mapping[str, str], ...]
    input_hash: str
    evidence_catalog_hash: str


_ENVELOPE_KEYS = frozenset('schema_version run_id stage_id source_trade_date execution_trade_date as_of '
    'knowledge_cutoff input_snapshot_ids config_version config_hash prompt_version model_name market_regime '
    'status reason_codes'.split())
_SHORTHAND_INPUTS = {('A3','PM_POST_CLOSE'): frozenset(('ACCOUNT_SNAPSHOT','POSITIONS',
    'MARKET_STATE','THEME_REVIEWS','EVIDENCE_CATALOG')),
    ('A4','INTRADAY'): frozenset(('DECISION_ID','MINUTE_SNAPSHOT_ID'))}


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

    def prepare_messages(self, role: str, replacements: Mapping[str, Any], *,
                         required_envelope: Mapping[str, Any], session: str | None = None) -> TeamMessageRequest:
        """Bind immutable transport metadata plus shorthand input exactly once.

        The role body remains byte-identical. This creates messages only; no
        provider, state store, execution permission or fallback is involved.
        """
        if (not isinstance(required_envelope,Mapping) or set(required_envelope)!=_ENVELOPE_KEYS
                or required_envelope.get('stage_id')!=role):
            raise ValueError('TEAM_REQUIRED_ENVELOPE_INVALID')
        extra_keys=_SHORTHAND_INPUTS.get((role,session),frozenset())
        if not extra_keys.issubset(replacements):raise ValueError('TEAM_UNRESOLVED_PLACEHOLDER')
        values=deepcopy(dict(replacements))
        if role=='A3' and session=='PLAN_REVIEW':values=blind_plan_facts(values)
        if 'EVIDENCE_CATALOG' not in values:raise ValueError('TEAM_EVIDENCE_CATALOG_REQUIRED')
        prompt=self.prepare(role,values,session=session)
        catalog_text=json.dumps(values['EVIDENCE_CATALOG'],ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)
        catalog_hash=hashlib.sha256(catalog_text.encode()).hexdigest()
        extra_inputs={key:values[key] for key in sorted(extra_keys)}
        if 'EVIDENCE_CATALOG' not in self.document(role).placeholders:
            extra_inputs['EVIDENCE_CATALOG']=values['EVIDENCE_CATALOG']
        runtime={'required_envelope':deepcopy(dict(required_envelope)), 'session':session,
                 'catalog_sha256':catalog_hash, 'inputs':extra_inputs}
        user='RUNTIME_INPUT\n'+json.dumps(runtime,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)
        messages=(MappingProxyType({'role':'system','content':prompt.system}),
                  MappingProxyType({'role':'user','content':user}))
        binding=json.dumps([dict(message) for message in messages],ensure_ascii=False,
                           sort_keys=True,separators=(',',':'),allow_nan=False)
        return TeamMessageRequest(prompt,messages,hashlib.sha256(binding.encode()).hexdigest(),catalog_hash)

    def prepare(self, role: str, replacements: Mapping[str, Any], *, session: str | None = None) -> TeamPromptRequest:
        shared = self._read("00_shared_system_v3.txt"); document = self.document(role)
        if role == "A1" and "【v3 修订清单】" in document.text:
            raise ValueError("A1_COMPOSITION_REQUIRED")
        if f"版本: {_VERSIONS[role]}" not in document.text or "版本: shared-system/3.0.2" not in shared.text:
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
        return TeamPromptRequest(role, "shared-system/3.0.2", _VERSIONS[role], rendered,
                                 hashlib.sha256(rendered.encode()).hexdigest(),
                                 MappingProxyType({shared.name: shared.sha256, document.name: document.sha256}))
