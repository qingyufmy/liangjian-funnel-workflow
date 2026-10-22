"""Explicit closed-session research updates, never execution publication."""
from collections.abc import Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import re

from .calendar import latest_closed_research_session
from .closed_session_capture import KEY, aware, validate_research_capture_proof

_CLOCK_SOURCE = ContextVar('closed_session_research_update_clock', default=None)


@dataclass(frozen=True)
class ResearchUpdateBinding:
    purpose: str
    run_id: str
    source_trade_date: str
    target_trade_date: str
    started_at: datetime
    snapshot_id: str
    snapshot_hash: str
    snapshot_file_sha256: str
    capture_as_of: datetime
    generation_id: str
    generation_manifest_hash: str
    generation_payload_hash: str


def _require(value, reason):
    if not value:
        raise ValueError('RESEARCH_UPDATE_' + reason)


def _file_sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def load_bound_seed(app, binding, *, now, prepared=None):
    _require(type(binding) is ResearchUpdateBinding, 'BINDING_REQUIRED')
    current = aware(now)
    session = latest_closed_research_session(current, app.trading_calendar)
    _require(binding.source_trade_date == session.source_trade_date.isoformat()
        and binding.target_trade_date == session.target_trade_date.isoformat()
        and binding.started_at <= current, 'SOURCE_SESSION_CHANGED')
    path = app.settings.snapshot_dir / (binding.snapshot_id + '.json')
    _require(not path.is_symlink() and path.is_file()
        and path.resolve().is_relative_to(app.settings.snapshot_dir.resolve()), 'SEED_PATH_INVALID')
    _require(_file_sha(path) == binding.snapshot_file_sha256, 'SEED_FILE_CHANGED')
    if prepared is None:
        prepared = app._load_research_snapshot_by_id(binding.snapshot_id,
            expected_date=binding.capture_as_of.date().isoformat())
    else:
        from .publication_receipt import object_hash
        _require(prepared.path.resolve() == path.resolve()
            and object_hash(prepared.snapshot.data) == binding.snapshot_hash, 'SEED_DATA_CHANGED')
    snapshot = prepared.snapshot
    _require(snapshot.snapshot_hash == binding.snapshot_hash and snapshot.as_of == binding.capture_as_of,
        'SEED_IDENTITY_CHANGED')
    proof = validate_research_capture_proof(snapshot.data.get(KEY), captured_at=snapshot.as_of,
        calendar=app.trading_calendar, now=current, source_trade_date=binding.source_trade_date)
    manifest = snapshot.data.get('snapshot_manifest')
    _require(isinstance(manifest, Mapping)
        and manifest.get('as_of') == snapshot.as_of.isoformat()
        and snapshot.data.get('MARKET_DATA_AS_OF') == proof['source_market_close_at'], 'SEED_MARKET_MISMATCH')
    g0 = snapshot.data.get('g0_symbols')
    _require(isinstance(g0, list) and len(g0) == len(set(g0))
        and prepared.selected_count == prepared.research_universe_count == len(g0) > 0,
        'FULL_SEED_REQUIRED')
    return prepared


def require_active_generation(app, binding):
    _require(type(binding) is ResearchUpdateBinding, 'BINDING_REQUIRED')
    generation = app.a1_registry.get_active_generation()
    _require(generation is not None and generation.generation_id == binding.generation_id
        and generation.manifest_hash == binding.generation_manifest_hash
        and generation.payload_hash == binding.generation_payload_hash, 'A1_POINTER_CHANGED')
    return generation


def bind_research_update(app, *, purpose, snapshot_id, snapshot_hash, snapshot_file_sha256,
        source_trade_date, expected_a1_generation_id, run_id, now):
    _require(purpose in {'A1', 'A2_A3'}, 'PURPOSE_INVALID')
    _require(isinstance(run_id, str) and re.fullmatch(r'[A-Za-z0-9_.-]{1,150}', run_id), 'RUN_ID_INVALID')
    _require(re.fullmatch(r'snapshot-[A-Za-z0-9+._-]{8,180}', str(snapshot_id or '')), 'SEED_ID_INVALID')
    _require(all(isinstance(v, str) and re.fullmatch('[0-9a-f]{64}', v)
        for v in (snapshot_hash, snapshot_file_sha256)), 'HASH_REQUIRED')
    current = aware(now)
    session = latest_closed_research_session(current, app.trading_calendar)
    _require(source_trade_date == session.source_trade_date.isoformat(), 'SOURCE_DATE_INVALID')
    path = app.settings.snapshot_dir / (snapshot_id + '.json')
    _require(not path.is_symlink() and path.is_file()
        and path.resolve().is_relative_to(app.settings.snapshot_dir.resolve()), 'SEED_PATH_INVALID')
    _require(_file_sha(path) == snapshot_file_sha256, 'SEED_FILE_CHANGED')
    with path.open('r', encoding='utf-8') as stream:
        body = json.load(stream)
    capture_as_of = aware(body['as_of'])
    del body  # Do not retain a second full-market tree while verifying the seed.
    generation = app.a1_registry.get_active_generation()
    _require(generation is not None and generation.generation_id == expected_a1_generation_id,
        'A1_POINTER_CHANGED')
    binding = ResearchUpdateBinding(purpose, run_id, source_trade_date,
        session.target_trade_date.isoformat(), current, snapshot_id, snapshot_hash,
        snapshot_file_sha256, capture_as_of, generation.generation_id,
        generation.manifest_hash, generation.payload_hash)
    load_bound_seed(app, binding, now=current)
    if purpose == 'A2_A3':
        manifest = generation.manifest
        daily = manifest.get('delta', {}).get('daily_refresh_proof', {})
        _require(isinstance(daily, Mapping) and daily.get('snapshot_id') == snapshot_id
            and daily.get('snapshot_hash') == snapshot_hash
            and daily.get('snapshot_file_sha256') == snapshot_file_sha256,
            'A1_SEED_BINDING_MISMATCH')
        validate_research_capture_proof(daily.get('closed_session'), captured_at=generation.as_of,
            calendar=app.trading_calendar, now=current, source_trade_date=source_trade_date)
    return binding


def claim_update(app, binding):
    """A failed attempt remains recorded; callers must choose a new identity."""
    directory = app.settings.workflow_output_dir / 'research_updates'
    directory.mkdir(parents=True, exist_ok=True)
    claim = directory / (binding.run_id + '.claim.json')
    with claim.open('x', encoding='utf-8') as stream:
        json.dump(dict(run_id=binding.run_id, purpose=binding.purpose,
            started_at=binding.started_at.isoformat(), source_trade_date=binding.source_trade_date,
            target_trade_date=binding.target_trade_date, snapshot_id=binding.snapshot_id,
            snapshot_hash=binding.snapshot_hash, snapshot_file_sha256=binding.snapshot_file_sha256,
            generation_id=binding.generation_id, publication='RESEARCH_ONLY_NOT_EXECUTION'), stream)


def execute_update(app, *, purpose, now, **kwargs):
    binding = bind_research_update(app, purpose=purpose, now=now, **kwargs)
    _require(not any((app.settings.workflow_output_dir / 'runs' / (binding.run_id + suffix)).exists()
        for suffix in ('.json', '-a1-maintenance.json')),
        'RUN_ID_ALREADY_USED')
    claim_update(app, binding)
    if purpose == 'A1':
        return app.run_a1_maintenance(now=now, mode='INCREMENTAL', daily_refresh=True,
            snapshot_id=binding.snapshot_id, source_trade_date=binding.source_trade_date,
            run_id=binding.run_id, _research_update=binding)
    return app.run_research('close', primary_only=True, from_active_a1=True,
        active_a1_generation_id=binding.generation_id, publish_plans=False,
        schedule_comparison=False, record_runtime=False, reuse_resume_snapshot=False,
        run_id_override=binding.run_id, _research_update=binding)


@contextmanager
def research_time_context(app, binding, prepared, *, clock):
    """A request-local source original, reset even when the model path fails."""
    original = load_bound_seed(app, binding, now=clock(), prepared=prepared)
    require_active_generation(app, binding)
    _require(original.snapshot.snapshot_id == prepared.snapshot.snapshot_id
        and original.snapshot.snapshot_hash == prepared.snapshot.snapshot_hash, 'CLOCK_BASE_MISMATCH')
    token = _CLOCK_SOURCE.set((binding, original.snapshot, clock))
    try:
        yield
    finally:
        _CLOCK_SOURCE.reset(token)


def research_update_times(snapshot, calendar):
    """Replay the captured builder recipes against the actual bound seed.

    The transform and data-recipe rules match PublicationReceiptBuilder and
    its exporter. A child timestamp is accepted only for captured stock news;
    every other original transform must retain its parent timestamp.
    """
    context = _CLOCK_SOURCE.get()
    if context is None:
        return None
    from .publication_receipt import PublicationReceiptBuilder, object_hash
    binding, base, clock = context
    now = aware(clock())
    proof = validate_research_capture_proof(base.data.get(KEY), captured_at=base.as_of,
        calendar=calendar, now=now, source_trade_date=binding.source_trade_date)
    _require(object_hash(base.data) == binding.snapshot_hash, 'CLOCK_BASE_HASH_CHANGED')
    prior = dict(snapshot_id=base.snapshot_id, snapshot_hash=base.snapshot_hash,
        as_of=aware(base.as_of).isoformat(), data=base.data)
    nodes = snapshot.proof_chain
    _require(isinstance(nodes, (list, tuple)) and len(nodes) <= 32, 'CLOCK_CHAIN_INVALID')
    seen = {base.snapshot_id}
    for node in nodes:
        _require(isinstance(node, Mapping) and node.get('schema_version') == 'research-snapshot-recipe/1'
            and node.get('status') == 'CAPTURED' and node.get('parent_snapshot_id') == prior['snapshot_id']
            and node.get('parent_snapshot_hash') == prior['snapshot_hash']
            and node.get('parent_as_of') == prior['as_of'], 'CLOCK_PARENT_CONFLICT')
        recipe, delta = node.get('hash_recipe'), node.get('data_recipe')
        _require(isinstance(recipe, Mapping) and isinstance(delta, Mapping)
            and object_hash(recipe) == node.get('snapshot_hash'), 'CLOCK_RECIPE_HASH_CONFLICT')
        kind = node.get('recipe_kind')
        parent_key = 'source_hash' if kind == 'A2_ROTATION_POLICY' else 'base_snapshot_hash'
        _require(recipe.get(parent_key) == prior['snapshot_hash'], 'CLOCK_RECIPE_PARENT_CONFLICT')
        reasons = set()
        expected, tag = PublicationReceiptBuilder._snapshot_transform(kind, prior, recipe, reasons)
        _require(not reasons and expected is not None and tag is not None, 'CLOCK_TRANSFORM_INVALID')
        if delta.get('operation') == 'SHALLOW_OVERLAY' and set(delta) == {'operation', 'overlay'} and isinstance(delta['overlay'], Mapping):
            derived = {**prior['data'], **delta['overlay']}
        elif kind == 'A2_STOCK_NEWS' and set(delta) == {'NEWS_HEAT_SNAPSHOT', 'A2_STOCK_NEWS_SCOPE'}:
            derived = {**prior['data'], **delta}
        elif (kind == 'A3_DETERMINISTIC_CONTEXT' and delta.get('operation') == kind
                and set(delta) == {'operation', 'context', 'factor_projection'}
                and delta.get('factor_projection') == 'MONTH_WEEK_DAY_ONLY'
                and object_hash(delta['context']) == object_hash(recipe.get('context'))):
            derived = expected
        else:
            raise ValueError('RESEARCH_UPDATE_CLOCK_DATA_RECIPE_INVALID')
        _require(object_hash(derived) == object_hash(expected), 'CLOCK_DATA_RECIPE_CONFLICT')
        expected_id = (prior['snapshot_id'] + ':rotation-policy' if kind == 'A2_ROTATION_POLICY'
            else f"{prior['snapshot_id']}:{tag}:{node['snapshot_hash'][:12]}")
        stamp = aware(node.get('as_of'))
        _require(node.get('snapshot_id') == expected_id and expected_id not in seen,
            'CLOCK_CHILD_ID_CONFLICT')
        _require(stamp <= now and (stamp >= aware(prior['as_of']) if kind == 'A2_STOCK_NEWS'
            else stamp == aware(prior['as_of'])), 'CLOCK_CHRONOLOGY_INVALID')
        seen.add(expected_id)
        prior = dict(snapshot_id=expected_id, snapshot_hash=node['snapshot_hash'],
            as_of=stamp.isoformat(), data=expected)
    _require(snapshot.snapshot_id == prior['snapshot_id'] and snapshot.snapshot_hash == prior['snapshot_hash']
        and aware(snapshot.as_of).isoformat() == prior['as_of']
        and object_hash(snapshot.data) == object_hash(prior['data']), 'CLOCK_TERMINAL_CONFLICT')
    _require(snapshot.data.get(KEY) == proof
        and snapshot.data.get('MARKET_DATA_AS_OF') == proof['source_market_close_at'], 'CLOCK_MARKET_CONFLICT')
    return aware(snapshot.as_of), aware(proof['source_market_close_at'])
