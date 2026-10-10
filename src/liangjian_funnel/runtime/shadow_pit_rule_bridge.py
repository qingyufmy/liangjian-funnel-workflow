"""Local, immutable daily-rule/PIT material; no provider, ledger or outcome wiring.

Original receipt bytes and original payload_json UTF-8 bytes have distinct pins.
Checksums and declared clocks do not authenticate a transport or historical clock.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, time
import hashlib
import json
from pathlib import Path
import re

from . import shadow_rule_preflight as rules
from .shadow_pit_capture import FIELDS, _json, _stamp
from .shadow_price_limits import build_shadow_plan_pit_capture, validate_shadow_rule_receipt

VERSION = 'shadow-pit-rule-bridge/1'
MAX_BYTES = 16 * 1024 * 1024


@dataclass(frozen=True)
class FrozenDailyRulePackage:
    receipt_raw: bytes
    receipt_file_sha256: str
    receipt_path: str
    approved: rules.ApprovedRuleArchive
    target_trade_date: str
    known_at: str


@dataclass(frozen=True)
class FrozenRuleBoundPITMaterial:
    canonical_bytes: bytes
    sha256: str


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False).encode('utf-8')


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _pin(raw, pin):
    if (not isinstance(raw, bytes) or not raw or len(raw) > MAX_BYTES
            or not isinstance(pin, str) or not re.fullmatch('[0-9a-f]{64}', pin)
            or _sha(raw) != pin):
        raise ValueError('ORIGINAL_BYTES_HASH_UNPROVEN')


def _limited(code, **fields):
    return dict(schema_version=VERSION, status='DATA_LIMITED', reason_codes=[code],
                source_authenticated=False, http_requests=0, ledger_seal_status='NOT_WRITTEN',
                provider_integration='UNWIRED', derived_outcome_consumption='UNWIRED', **fields)


def _code(exc):
    text = str(exc)
    return text if isinstance(exc, ValueError) and re.fullmatch('[A-Z_]+', text) else 'INPUT_EVIDENCE_UNPROVEN'


def _checked_package(package, now):
    if not isinstance(package, FrozenDailyRulePackage):
        raise ValueError('DAILY_RULE_PACKAGE_UNWIRED')
    _pin(package.receipt_raw, package.receipt_file_sha256)
    receipt = _json(package.receipt_raw)
    known = _stamp(package.known_at)
    if (package.target_trade_date != now.date().isoformat() or known.date() != now.date()
            or known > now or known.time().replace(tzinfo=None) >= time(9, 26)):
        raise ValueError('DAILY_RULE_PACKAGE_DAY_OR_CLOCK_UNPROVEN')
    # The receipt's actual declared completion cannot be later than load/knowledge.
    if _stamp(receipt.get('known_at')) > known:
        raise ValueError('DAILY_RULE_PACKAGE_KNOWLEDGE_UNPROVEN')
    checked = validate_shadow_rule_receipt(package.approved, receipt, at=known)
    if checked['status'] != 'RULE_VERSION_CONFIRMED':
        raise ValueError('DAILY_RULE_RECEIPT_REVALIDATION_FAILED')
    return receipt, checked


def load_daily_rule_package(*, receipt_path, receipt_file_sha256,
                            approved_archive_root, target_trade_date, known_at):
    """Read an explicitly pinned file plus the fixed reviewed four-file archive.

    No default path, directory discovery, network, new approval or implicit clock.
    known_at is the caller's actual same-day knowledge clock, strictly before 09:26.
    """
    try:
        now = _stamp(known_at)
        if type(target_trade_date) is not str or date.fromisoformat(target_trade_date).isoformat() != target_trade_date:
            raise ValueError('TARGET_DAY_UNPROVEN')
        path = Path(receipt_path)
        if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_BYTES:
            raise ValueError('DAILY_RULE_FILE_UNAVAILABLE')
        resolved = path.resolve(strict=True)
        raw = resolved.read_bytes()
        _pin(raw, receipt_file_sha256)
        approved = rules.load_approved_rule_archive(approved_archive_root)
        package = FrozenDailyRulePackage(raw, receipt_file_sha256, str(resolved),
                                        approved, target_trade_date, now.isoformat())
        _, checked = _checked_package(package, now)
        return dict(schema_version=VERSION, status='RULE_VERSION_CONFIRMED', package=package,
                    rule_confirmation=checked, receipt_file_sha256=receipt_file_sha256,
                    source_authenticated=False, http_requests=0, reason_codes=[])
    except Exception as exc:
        return _limited(_code(exc), package=None)


def _plan_binding(plan, now):
    if not isinstance(plan, Mapping):
        raise ValueError('ORIGINAL_PLAN_ROW_UNPROVEN')
    raw_text = plan.get('payload_json')
    if not isinstance(raw_text, str) or not raw_text or len(raw_text.encode('utf-8')) > MAX_BYTES:
        raise ValueError('ORIGINAL_PAYLOAD_JSON_STRING_REQUIRED')
    payload = _json(raw_text)
    # JSON numeric overflow (e.g. 1e400) is not parse_constant; reject it too.
    _canonical(payload)
    for key in ('plan_id', 'symbol', 'lane_id', 'plan_version'):
        if not isinstance(plan.get(key), str) or not plan[key].strip():
            raise ValueError('PLAN_IDENTITY_UNPROVEN')
    if (any(payload.get(key, plan[key]) != plan[key] for key in ('plan_id', 'lane_id', 'plan_version'))
            or payload.get('symbol') != plan['symbol']
            or payload.get('target_trade_date') != now.date().isoformat()):
        raise ValueError('PLAN_PAYLOAD_IDENTITY_MISMATCH')
    return dict(plan_id=plan['plan_id'], symbol=plan['symbol'], lane_id=plan['lane_id'],
                plan_version=plan['plan_version'], target_trade_date=payload['target_trade_date'],
                plan_status_at_capture=plan.get('status'), valid_from=plan.get('valid_from'),
                expires_at=plan.get('expires_at'), invalidated_at=plan.get('invalidated_at'),
                original_row_canonical_sha256=_sha(_canonical(dict(plan))),
                payload_json_bytes_sha256=_sha(raw_text.encode('utf-8')),
                payload_hash_basis='ORIGINAL_PAYLOAD_JSON_STRING_UTF8_BYTES_NOT_REENCODED')


def prepare_rule_bound_pit(plan, source, *, observed_at, rule_package):
    """Prepare immutable material from exact original inputs; never seal or admit.

    Reuses the frozen opt-in wrapper, all original plan/identity/PIT gates, and
    fixed daily rule validator. Missing catalogue/source bytes remain missing.
    Returned entry is only preparation, not a provider READY or ledger receipt.
    """
    try:
        now = _stamp(observed_at)
        receipt, checked = _checked_package(rule_package, now)
        binding = _plan_binding(plan, now)
        entry = build_shadow_plan_pit_capture(plan, source, observed_at=now,
                                             rule_receipt=receipt, approved=rule_package.approved)
        if entry['status'] != 'COMPLETE' or entry['limits'].get('status') != 'KNOWN':
            return _limited('PIT_CAPTURE_INCOMPLETE', entry=entry, material=None)
        limits = entry['limits']
        basis = limits.get('basis')
        if basis not in {'DERIVED_FROM_PRECLOSE_AND_BOARD_RULE', 'EXPLICIT_FROZEN_SAME_DAY'}:
            raise ValueError('PRICE_LIMIT_AUTHORITY_UNPROVEN')
        body = dict(schema_version=VERSION, captured_at=now.isoformat(),
                    plan_binding=binding,
                    authority_kind=('SHADOW_DAILY_RULE_DERIVED' if basis == 'DERIVED_FROM_PRECLOSE_AND_BOARD_RULE'
                                    else 'EXPLICIT_SOURCE_LIMITS'),
                    rule_binding=dict(receipt_file_sha256=rule_package.receipt_file_sha256,
                                      receipt_canonical_sha256=checked['receipt_sha256'],
                                      approved_archive_receipt_sha256=rules.APPROVED_RECEIPT_SHA,
                                      target_trade_date=rule_package.target_trade_date,
                                      package_known_at=rule_package.known_at,
                                      receipt_known_at=checked['known_at'],
                                      metadata_conflicts=checked['metadata_conflicts']),
                    source_lineage=entry['source_lineage'],
                    evidence={key: entry['evidence'].get(key) for key in (*FIELDS, 'source_ref', 'source_input_sha256')},
                    limits={key: limits.get(key) for key in (
                        'status', 'upper', 'lower', 'basis', 'symbol', 'trade_date', 'board', 'preclose',
                        'rate', 'tick', 'rounding', 'rule_effective_from', 'rule_confirmed_trade_date',
                        'source_ref', 'input_sha256', 'source_input_sha256')},
                    source_authenticated=False, ledger_seal_status='NOT_WRITTEN',
                    provider_integration='UNWIRED', derived_outcome_consumption='UNWIRED')
        raw = _canonical(body)
        material = FrozenRuleBoundPITMaterial(raw, _sha(raw))
        return dict(schema_version=VERSION, status='PREPARED', entry=entry, material=material,
                    source_authenticated=False, ledger_seal_status='NOT_WRITTEN',
                    provider_integration='UNWIRED', derived_outcome_consumption='UNWIRED', reason_codes=[])
    except Exception as exc:
        return _limited(_code(exc), entry=None, material=None)


def validate_rule_bound_material(material, *, plan, source, rule_package, at):
    """Revalidate original capture, not a new capture using intraday/current clock.

    No stored status is trusted. Rebuild from caller's exact original objects and
    compare canonical material bytes. This is not an ACTIVE/PENDING transition
    adapter, persistent seal, or price-limit outcome consumer.
    """
    try:
        if not isinstance(material, FrozenRuleBoundPITMaterial):
            raise ValueError('PREPARED_MATERIAL_MISSING')
        _pin(material.canonical_bytes, material.sha256)
        body = _json(material.canonical_bytes)
        now, captured = _stamp(at), _stamp(body.get('captured_at'))
        if body.get('schema_version') != VERSION or now.date() != captured.date() or captured > now:
            raise ValueError('MATERIAL_DAY_OR_CAPTURE_CLOCK_UNPROVEN')
        expiry = _stamp(body['plan_binding']['expires_at'])
        invalidated = body['plan_binding'].get('invalidated_at')
        if now > expiry or (invalidated is not None and _stamp(invalidated) <= now):
            raise ValueError('MATERIAL_ORIGINAL_PLAN_WINDOW_CLOSED')
        rebuilt = prepare_rule_bound_pit(plan, source, observed_at=captured, rule_package=rule_package)
        other = rebuilt.get('material')
        if other is None or other.canonical_bytes != material.canonical_bytes or other.sha256 != material.sha256:
            raise ValueError('MATERIAL_ORIGINAL_INPUT_BINDING_MISMATCH')
        return dict(schema_version=VERSION, status='VALIDATED_PREPARED_MATERIAL',
                    captured_at=captured.isoformat(), material_sha256=material.sha256,
                    source_authenticated=False, ledger_seal_status='NOT_WRITTEN',
                    provider_integration='UNWIRED', derived_outcome_consumption='UNWIRED', reason_codes=[])
    except Exception as exc:
        return _limited(_code(exc))


__all__ = ['FrozenDailyRulePackage', 'FrozenRuleBoundPITMaterial', 'load_daily_rule_package',
           'prepare_rule_bound_pit', 'validate_rule_bound_material']
