"""Offline, lossless legacy projections for MARKET_STATE_FACTS/1.

No runtime caller, source fetch, model invocation or permission reducer is
registered here. Mapped tokens retain source namespaces, not risk equivalence.
"""
from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .config import read_yaml
from ..pipeline.emotion_vocabulary import STAGES, read_emotion_stage


SOURCES = frozenset({'live_market', 'team_regime', 'a6_prior', 'a2_emotion', 'macro_quadrant'})
SCHEMA_VERSION = 'MARKET_STATE_FACTS/1'


def _date(value: str) -> date:
    if not isinstance(value, str) or re.fullmatch(r'\d{4}-\d{2}-\d{2}', value) is None:
        raise ValueError('MARKET_STATE_DATE_INVALID')
    return date.fromisoformat(value)


def _stamp(value: str) -> datetime:
    if not isinstance(value, str) or 'T' not in value:
        raise ValueError('MARKET_STATE_TIMESTAMP_INVALID')
    parsed = datetime.fromisoformat(value)
    if parsed.utcoffset() is None:
        raise ValueError('MARKET_STATE_TIMEZONE_REQUIRED')
    return parsed


class PitProof(BaseModel):
    """Original timestamps remain text; unavailable provider time stays null."""
    model_config = ConfigDict(extra='forbid', frozen=True, strict=True)
    source_trade_date: str
    target_trade_date: str
    mode: Literal['CLOSE', 'INTRADAY']
    as_of: str | None
    market_close_at: str | None
    provider_timestamp: str | None
    capture_started_at: str | None
    capture_completed_at: str | None

    def missing_fields(self):
        return [name for name in ('as_of', 'market_close_at', 'provider_timestamp',
                'capture_started_at', 'capture_completed_at') if getattr(self, name) is None]

    @model_validator(mode='after')
    def clocks(self):
        from zoneinfo import ZoneInfo
        tz = ZoneInfo('Asia/Shanghai')
        source, target = _date(self.source_trade_date), _date(self.target_trade_date)
        as_of, close, start, end = (None if getattr(self, key) is None else _stamp(getattr(self, key)) for key in
                                  ('as_of', 'market_close_at', 'capture_started_at', 'capture_completed_at'))
        provider = None if self.provider_timestamp is None else _stamp(self.provider_timestamp)
        if (target < source or (close is not None and close.astimezone(tz).date() != source)
                or (as_of is not None and end is not None and as_of > end)
                or (start is not None and end is not None and start > end)
                or (as_of is not None and as_of.astimezone(tz).date() != source)):
            raise ValueError('MARKET_STATE_PIT_INCONSISTENT')
        if self.mode == 'CLOSE' and (
            (as_of is not None and close is not None and as_of != close)
            or (start is not None and close is not None and start < close)
        ):
            raise ValueError('MARKET_STATE_CLOSE_NOT_CLOSED')
        if provider is not None and end is not None and provider > end:
            raise ValueError('MARKET_STATE_PROVIDER_IN_FUTURE')
        return self


@dataclass(frozen=True)
class SourceInput:
    source_id: str
    original: bytes
    pit: PitProof


class TokenFact(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    status: Literal['MAPPED', 'UNMAPPED', 'MISSING']
    raw_value: str | None
    canonical: str | None
    reason: str | None


class ComponentFacts(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    status: Literal['PRESERVED', 'UNMAPPED', 'DATA_BLOCK']
    source_id: str | None
    input_sha256: str | None = Field(pattern=r'^[0-9a-f]{64}$')
    payload_sha256: str | None = Field(pattern=r'^[0-9a-f]{64}$')
    pit: PitProof | None
    legacy: dict[str, Any] | None
    tokens: dict[str, TokenFact]
    reasons: list[str]

    @model_validator(mode='after')
    def binding(self):
        if self.legacy is None:
            if (self.status != 'DATA_BLOCK' or self.source_id is not None or self.pit is not None
                    or self.input_sha256 is not None or self.payload_sha256 is not None or self.tokens):
                raise ValueError('MARKET_STATE_MISSING_SOURCE_BINDING_INVALID')
        else:
            if not self.source_id or not self.input_sha256 or not self.payload_sha256 or self.pit is None:
                raise ValueError('MARKET_STATE_SOURCE_BINDING_REQUIRED')
            if self.pit.missing_fields() and self.status != 'DATA_BLOCK':
                raise ValueError('MARKET_STATE_INCOMPLETE_PIT_MUST_BLOCK')
            data = json.dumps(self.legacy, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)
            if hashlib.sha256(data.encode('utf-8')).hexdigest() != self.payload_sha256:
                raise ValueError('MARKET_STATE_PAYLOAD_HASH_MISMATCH')
        return self


def _closed_component_schema(schema):
    """Expose the same closed namespace set to non-Python schema consumers."""
    item = schema.pop('additionalProperties')
    schema.update(properties={name: deepcopy(item) for name in sorted(SOURCES)},
                  required=sorted(SOURCES), additionalProperties=False)


class MarketStateFacts(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    schema_version: Literal['MARKET_STATE_FACTS/1'] = SCHEMA_VERSION
    authority: Literal['SHADOW_ONLY'] = 'SHADOW_ONLY'
    source_trade_date: str
    target_trade_date: str
    vocabulary_version: str
    vocabulary_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    components: dict[str, ComponentFacts] = Field(json_schema_extra=_closed_component_schema)

    @model_validator(mode='after')
    def complete_namespaces(self):
        if set(self.components) != SOURCES or _date(self.target_trade_date) < _date(self.source_trade_date):
            raise ValueError('MARKET_STATE_FRAME_INVALID')
        return self


def load_vocabulary(path: str | Path) -> dict:
    table = read_yaml(path)
    try:
        if (table['schema_version'] != 'market-state-vocabulary/1'
                or table['authority'] != 'SHADOW_ONLY'
                or table['cross_source_equivalence'] != 'UNAPPROVED'
                or not isinstance(table['version'], str) or not table['version']
                or set(table['sources']) != SOURCES):
            raise ValueError
        namespaces = set()
        for source in table['sources'].values():
            if set(source) != {'fields'} or not source['fields']:
                raise ValueError
            for spec in source['fields'].values():
                if (not {'path', 'namespace', 'values'} <= set(spec)
                        or set(spec) - {'path', 'namespace', 'values', 'unmapped', 'historical_alias', 'blocking_values'}
                        or not isinstance(spec['path'], list) or not spec['path']
                        or any(not isinstance(k, str) or not k for k in spec['path'])
                        or not isinstance(spec['namespace'], str) or not spec['namespace']
                        or spec['namespace'] in namespaces
                        or not isinstance(spec['values'], list) or not spec['values']
                        or any(not isinstance(v, str) or not v for v in spec['values'])
                        or len(set(spec['values'])) != len(spec['values'])
                        or not set(spec.get('unmapped', {})) <= set(spec['values'])
                        or not set(spec.get('blocking_values', [])) <= set(spec['values'])):
                    raise ValueError
                namespaces.add(spec['namespace'])
                aliases = spec.get('historical_alias', {})
                if aliases and (spec['namespace'] != 'a2.emotion_cycle' or aliases != {
                    'STARTUP': {'canonical': 'IGNITION', 'source_trade_date_lte': '2026-10-27'}
                }):
                    raise ValueError
        stage = table['sources']['a2_emotion']['fields']['emotion_cycle']
        if set(stage['values']) != STAGES:
            raise ValueError
        if table['sources']['a6_prior']['fields']['market_regime'].get('unmapped', {}):
            raise ValueError
    except (KeyError, TypeError, ValueError, AttributeError):
        raise ValueError('MARKET_STATE_VOCABULARY_INVALID') from None
    return table


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('MARKET_STATE_DUPLICATE_JSON_KEY')
        result[key] = value
    return result


def _bad_constant(value):
    raise ValueError('MARKET_STATE_NONFINITE_JSON')


def _token(raw: Mapping, spec: Mapping, source_day: str) -> TokenFact:
    value: Any = raw
    for key in spec['path']:
        if not isinstance(value, Mapping) or key not in value or value[key] is None:
            return TokenFact(status='MISSING', raw_value=None, canonical=None, reason='SOURCE_FIELD_MISSING')
        value = value[key]
    if not isinstance(value, str):
        raise ValueError('MARKET_STATE_ENUM_TYPE_INVALID')
    normalized = value
    if value in spec.get('historical_alias', {}):
        normalized = read_emotion_stage(value, source_trade_date=source_day)
    if normalized not in spec['values']:
        raise ValueError('MARKET_STATE_UNKNOWN_ENUM')
    if normalized in spec.get('unmapped', {}):
        return TokenFact(status='UNMAPPED', raw_value=value, canonical=None,
                         reason=spec['unmapped'][normalized])
    return TokenFact(status='MAPPED', raw_value=value,
                     canonical=f"{spec['namespace']}.{normalized}", reason=None)


def build_market_state(sources: Mapping[str, SourceInput], *, source_trade_date: str,
                       target_trade_date: str, vocabulary: dict) -> MarketStateFacts:
    """Preserve each supplied original; missing source never borrows another."""
    if set(sources) - SOURCES:
        raise ValueError('MARKET_STATE_UNKNOWN_SOURCE')
    components = {}
    for name in sorted(SOURCES):
        original = sources.get(name)
        if original is None:
            components[name] = ComponentFacts(status='DATA_BLOCK', source_id=None,
                input_sha256=None, payload_sha256=None, pit=None, legacy=None, tokens={},
                reasons=['SOURCE_NOT_SUPPLIED'])
            continue
        if (not isinstance(original, SourceInput) or not isinstance(original.pit, PitProof)
                or not isinstance(original.source_id, str) or not original.source_id.strip()
                or not isinstance(original.original, bytes)):
            raise ValueError('MARKET_STATE_SOURCE_INPUT_INVALID')
        if original.pit.target_trade_date != target_trade_date:
            raise ValueError('MARKET_STATE_TARGET_MISMATCH')
        if _date(original.pit.source_trade_date) > _date(source_trade_date):
            raise ValueError('MARKET_STATE_SOURCE_DAY_AFTER_FRAME')
        raw = json.loads(original.original, object_pairs_hook=_pairs, parse_constant=_bad_constant)
        if not isinstance(raw, dict):
            raise ValueError('MARKET_STATE_SOURCE_OBJECT_REQUIRED')
        canonical = json.dumps(raw, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)
        specs = vocabulary['sources'][name]['fields']
        tokens = {key: _token(raw, spec, original.pit.source_trade_date) for key, spec in specs.items()}
        reasons = [f'{key}:{token.reason}' for key, token in tokens.items() if token.reason]
        blocking = [key for key, token in tokens.items()
                    if token.raw_value in specs[key].get('blocking_values', [])]
        reasons.extend(f'{key}:SOURCE_DATA_BLOCK' for key in blocking)
        reasons.extend(name.upper()+'_MISSING' for name in original.pit.missing_fields())
        status = ('DATA_BLOCK' if blocking or any(t.status == 'MISSING' for t in tokens.values())
                  or original.pit.missing_fields() else
                  'UNMAPPED' if any(t.status == 'UNMAPPED' for t in tokens.values()) else 'PRESERVED')
        components[name] = ComponentFacts(status=status, source_id=original.source_id,
            input_sha256=hashlib.sha256(original.original).hexdigest(),
            payload_sha256=hashlib.sha256(canonical.encode('utf-8')).hexdigest(),
            pit=original.pit, legacy=deepcopy(raw), tokens=tokens, reasons=reasons)
    return MarketStateFacts(source_trade_date=source_trade_date, target_trade_date=target_trade_date,
        vocabulary_version=vocabulary['version'], vocabulary_sha256=vocabulary['config_sha256'],
        components=components)


def legacy_projection(facts: MarketStateFacts, source: str) -> dict | None:
    """Return independent exact JSON values, including pending/current fields."""
    if source not in SOURCES:
        raise ValueError('MARKET_STATE_UNKNOWN_SOURCE')
    component = facts.components[source]
    component.binding()
    return deepcopy(component.legacy)
