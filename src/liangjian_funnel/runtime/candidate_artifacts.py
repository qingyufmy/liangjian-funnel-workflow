"""One discovery contract for close candidates from every publishing entry.

This validates original publication objects before an inert candidate becomes
visible. It does not rebuild facts from a plan or a runtime seed fallback.
"""
from collections.abc import Mapping
from datetime import date, datetime
import hashlib
import json
from pathlib import Path
import re
from types import SimpleNamespace

from .publication_receipt import object_hash


def rows_hash(batch):
    rows=[]
    for row in batch:
        payload=row.get('payload')
        if payload is None:payload=json.loads(row['payload_json'])
        expiry=row['expires_at']
        rows.append(dict(plan_id=row['plan_id'],lane_id=row['lane_id'],symbol=row['symbol'],
            status=row['status'],plan_version=row.get('plan_version',1),
            valid_from=row.get('valid_from'),expires_at=expiry.isoformat() if isinstance(expiry,datetime) else expiry,
            payload=payload))
    return object_hash(sorted(rows,key=lambda row:row['plan_id']))


def _json_file(path, *, limit=4_000_000):
    path = Path(path)
    if path.is_symlink() or not path.is_file() or path.stat().st_size > limit:
        raise ValueError('CANDIDATE_ARTIFACT_FILE_UNAVAILABLE')
    return json.loads(path.read_bytes())


def _verify_file_pin(pin):
    path=Path(pin['path'])
    if path.is_symlink() or not path.is_file():raise ValueError('CANDIDATE_ORIGINAL_FILE_MISSING')
    before=path.stat(); digest=hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda:stream.read(1024*1024),b''):digest.update(block)
    after=path.stat()
    if ((before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns)!=
            (after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns) or digest.hexdigest()!=pin['sha256']):
        raise ValueError('CANDIDATE_ORIGINAL_FILE_CHANGED')


def summary_binding(summary):
    publication = summary.get('plan_publication') or {}
    return dict(run_id=summary.get('run_id'), slot=summary.get('slot'),
                market_trade_date=summary.get('market_trade_date'),
                projection_manifests=[{k: row.get(k) for k in ('lane_id', 'path', 'sha256')}
                    for row in publication.get('projection_manifests', [])],
                pattern_preparation=publication.get('pattern_preparation'))


def verify_candidate_artifacts(batch, *, run_id, proof, calendar=None, verify_original_files=True):
    """Recheck files and exact rows; a caller-provided PASS flag has no effect."""
    from .calendar import ExchangeTradingCalendar
    from .entry_confirmation_sources import read_runtime_daily_facts, _read_manifest
    calendar = calendar or ExchangeTradingCalendar()
    if (not isinstance(proof, Mapping) or proof.get('schema') != 'close-candidate-artifacts/1'
            or proof.get('run_id') != run_id or not re.fullmatch(r'[A-Za-z0-9_.-]{1,180}', run_id)):
        raise ValueError('CANDIDATE_ARTIFACT_CONTRACT_REQUIRED')
    root = Path(proof['workflow_output_dir']).resolve()
    summary = _json_file(root/'runs'/f'{run_id}.json', limit=2_000_000)
    binding = summary_binding(summary)
    if (object_hash(binding) != proof.get('summary_binding_sha256')
            or binding['run_id'] != run_id or str(binding['slot']).lower() != 'close'
            or summary.get('status') not in {'READY', 'READY_DEGRADED', 'PUBLISHED'}):
        raise ValueError('CANDIDATE_SUMMARY_BINDING_CONFLICT')
    try:
        source = date.fromisoformat(binding['market_trade_date'])
        target = date.fromisoformat(proof['target_trade_date'])
    except (TypeError, ValueError, KeyError):
        raise ValueError('CANDIDATE_SOURCE_DATE_UNPROVEN') from None
    if calendar.next_trading_day(source) != target:
        raise ValueError('CANDIDATE_SOURCE_TARGET_CONFLICT')
    if rows_hash(batch) != proof.get('candidate_rows_sha256'):
        raise ValueError('CANDIDATE_ROWS_CHANGED')
    expected = {row['plan_id']: row for row in batch}
    if len(expected) != len(batch):
        raise ValueError('CANDIDATE_DUPLICATE_PLAN')
    discovered = {}; checked_files=set()
    for descriptor in binding['projection_manifests']:
        if not isinstance(descriptor.get('path'),str) or not re.fullmatch('[0-9a-f]{64}',str(descriptor.get('sha256') or '')):
            raise ValueError('CANDIDATE_MANIFEST_UNAVAILABLE')
        manifest_path = Path(descriptor['path'])
        if not manifest_path.resolve().is_relative_to(root/'publication_proofs'):
            raise ValueError('CANDIDATE_MANIFEST_SCOPE_CONFLICT')
        parsed = _read_manifest(manifest_path)
        if parsed is None or parsed[1] != descriptor['sha256']:
            raise ValueError('CANDIDATE_MANIFEST_HASH_CONFLICT')
        manifest = parsed[0]
        if manifest.get('run_id') != run_id or manifest.get('lane_id') != descriptor['lane_id']:
            raise ValueError('CANDIDATE_MANIFEST_IDENTITY_CONFLICT')
        for pin in [manifest['base_snapshot'],manifest['audit']] if verify_original_files else []:
            identity=(pin['path'],pin['sha256'])
            if identity not in checked_files:_verify_file_pin(pin);checked_files.add(identity)
        for entry in manifest['plans']:
            plan_id = entry['plan_id']
            if plan_id in discovered:
                raise ValueError('CANDIDATE_MANIFEST_DUPLICATE_PLAN')
            row = expected.get(plan_id)
            if row is None or row['lane_id'] != descriptor['lane_id']:
                raise ValueError('CANDIDATE_MANIFEST_PLAN_SCOPE_CONFLICT')
            payload = row.get('payload')
            if payload is None:
                payload = json.loads(row['payload_json'])
            original = entry.get('published_payload') or {}
            if (original.get('sha256') != object_hash(payload) or original.get('value') != payload
                    or payload.get('source_run_id') != run_id or payload.get('symbol') != row['symbol']
                    or entry.get('target_trade_date') != target.isoformat()
                    or str(row['expires_at'])[:10] != target.isoformat()):
                raise ValueError('CANDIDATE_MANIFEST_PAYLOAD_CONFLICT')
            discovered[plan_id] = entry
    if set(discovered) != set(expected):
        raise ValueError('CANDIDATE_PUBLICATION_MANIFEST_INCOMPLETE')
    preparation = binding.get('pattern_preparation') or {}
    history = {row['plan_id']: row for row in preparation.get('plans', [])}
    if len(history) != len(batch) or set(history) != set(expected):
        raise ValueError('CANDIDATE_PATTERN_PREPARATION_MISSING')
    facts = read_runtime_daily_facts(SimpleNamespace(workflow_output_dir=root), batch, current_trade_date=target)
    for plan_id, row in expected.items():
        payload = row.get('payload') or json.loads(row['payload_json'])
        fact = facts.get(plan_id) or {}
        if fact.get('status') != 'READY':
            # A missing/changed discovery object is never a data-limited stock.
            # Keep the existing explicit A6 treatment only for genuine data gaps.
            reasons = set(fact.get('reason_codes') or [])
            allowed = {'DAILY20_INSUFFICIENT', 'PREVIOUS_DAY_HIGH_MISSING', 'PREVIOUS_TRADING_DAY_MISSING'}
            limited = payload.get('source_data_limits') or {}
            if not reasons or not reasons <= allowed or limited.get('a6_explicit_review_required') is not True:
                raise ValueError('CANDIDATE_DAILY_FACTS_UNPROVEN:' + plan_id)
        prior = history[plan_id]
        if (prior.get('symbol') != row['symbol'] or prior.get('source_trade_date') != source.isoformat()
                or prior.get('target_trade_date') != target.isoformat()):
            raise ValueError('CANDIDATE_HISTORY_SOURCE_CONFLICT')
        if prior.get('status') == 'READY':
            if (prior.get('available_5m_count') != 48 or prior.get('required_5m_count') != 48
                    or not re.fullmatch('[0-9a-f]{64}', prior.get('native_rowset_sha256', ''))):
                raise ValueError('CANDIDATE_HISTORY_PROOF_INVALID')
            from .published_session_seed import read_published_session_seed
            bars, readback = read_published_session_seed(proof['minute_cache_path'], row['symbol'],
                target_trade_date=target, calendar=calendar,
                as_of=datetime.fromisoformat(proof['observed_at']),
                expected_rowset_sha256=prior['native_rowset_sha256'])
            rowset=hashlib.sha256(json.dumps([bar.model_dump(mode='json') for bar in sorted(bars,key=lambda b:b.bar_end)],
                sort_keys=True,separators=(',',':')).encode()).hexdigest()
            if readback['status']!='READY' or rowset!=prior['native_rowset_sha256']:
                raise ValueError('CANDIDATE_HISTORY_READBACK_CONFLICT')
        elif (payload.get('source_data_limits') or {}).get('a6_explicit_review_required') is not True:
            raise ValueError('CANDIDATE_HISTORY_GAP_NOT_REVIEWED')
    return dict(source_trade_date=source.isoformat(), target_trade_date=target.isoformat(),
                daily_facts={key: value['status'] for key, value in facts.items()})


def discover_existing_candidate(settings, batch, *, run_id, observed_at):
    """Read-only compatibility for a pre-upgrade natural batch, never fake a proof.

    All compact artifacts must already exist and pass the identical checks.
    No summary, manifest, source date or candidate row is created or repaired.
    """
    root=Path(settings.workflow_output_dir).resolve()
    summary=_json_file(root/'runs'/f'{run_id}.json',limit=2_000_000)
    return dict(schema='close-candidate-artifacts/1',run_id=run_id,workflow_output_dir=str(root),
        target_trade_date=str(batch[0]['expires_at'])[:10],candidate_rows_sha256=rows_hash(batch),
        summary_binding_sha256=object_hash(summary_binding(summary)),
        minute_cache_path=str(Path(settings.minute_cache_dir)/'minute_bars.sqlite3'),
        observed_at=observed_at.isoformat())


def prepare_candidate_artifacts(batch, *, workflow_output_dir, run_id, status,
                                market_trade_date, projection_manifests, pattern_preparation,
                                minute_cache_path, observed_at, calendar=None, research_summary=None):
    """Persist the same discovery summary used by A4, then verify it before stage.

    The normal full run summary can add research/report fields afterwards;
    its discovery binding must remain exactly equal to this captured object.
    """
    from ..reporting import atomic_write_json
    if not batch:
        return None
    root = Path(workflow_output_dir).resolve()
    target = str(batch[0]['expires_at'])[:10]
    # PipelineResult.as_dict() contains complete per-lane stage inputs/outputs.
    # They already live in the immutable audit_paths and can dwarf the bounded
    # discovery summary. Match the final run-summary shape; preserve its audit
    # references, outcome/source facts and all publication pins unchanged.
    discovery = {key: value for key, value in (research_summary or {}).items() if key != 'lanes'}
    summary = dict(discovery,run_id=run_id, status=status, slot='close', market_trade_date=str(market_trade_date),
                   plan_publication=dict(projection_manifests=projection_manifests,
                                         pattern_preparation=pattern_preparation))
    path = root/'runs'/f'{run_id}.json'
    if path.exists():
        if summary_binding(_json_file(path, limit=2_000_000)) != summary_binding(summary):
            raise ValueError('CANDIDATE_EXISTING_SUMMARY_CONFLICT')
    else:
        atomic_write_json(path, summary)
    proof = dict(schema='close-candidate-artifacts/1', run_id=run_id,
                 workflow_output_dir=str(root), target_trade_date=target,
                 minute_cache_path=str(Path(minute_cache_path).resolve()),observed_at=observed_at.isoformat(),
                 candidate_rows_sha256=rows_hash(batch),
                 summary_binding_sha256=object_hash(summary_binding(summary)))
    verify_candidate_artifacts(batch, run_id=run_id, proof=proof, calendar=calendar)
    return proof
