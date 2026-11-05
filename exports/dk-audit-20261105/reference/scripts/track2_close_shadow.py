"""Local, offline, exclusive-create MKT2/DK facts from a frozen manifest.

All input references use the projection audit's hash/pointer rules. Nothing
is fetched or scheduled. Run through the local maintenance wrapper.
"""
from datetime import date, datetime
import argparse
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
from liangjian_funnel.market_state.config import load_market_definitions
from liangjian_funnel.market_state.dk_shadow import build_dk_shadow
from liangjian_funnel.market_state.index_trend import market_close_facts
from liangjian_funnel.market_state.calendar import market_calendar
from track2_replay_market_state import _json, _original


def build(manifest_path, config_dir, *, calendar):
    manifest_path = Path(manifest_path)
    raw = manifest_path.read_bytes()
    if len(raw) > 2*1024*1024:
        raise ValueError('SHADOW_MANIFEST_OVERSIZE')
    manifest = _json(raw)
    if manifest.get('schema_version') != 'track2-close-shadow-inputs/1':
        raise ValueError('SHADOW_MANIFEST_VERSION')
    day = date.fromisoformat(manifest['source_trade_date'])
    now = datetime.fromisoformat(manifest['observed_at'])
    definitions = load_market_definitions(config_dir)
    bindings = {}
    def selected(ref):
        path = Path(ref['path'])
        if not path.is_absolute():
            path = manifest_path.parent/path
        key = str(path.resolve())
        if key in bindings and bindings[key] != ref['file_sha256']:
            raise ValueError('SHADOW_CONFLICTING_INPUT_HASH')
        bindings[key] = ref['file_sha256']
        return _json(_original(ref, manifest_path.parent))
    plan_ref = manifest['plan_scope']
    scope = selected(plan_ref)
    if (not isinstance(scope, dict) or scope.get('trade_date') != day.isoformat()
            or not isinstance(scope.get('symbols'), list)
            or scope.get('evidence_class') != 'FROZEN_PLAN_SYMBOL_SCOPE'):
        raise ValueError('SHADOW_PLAN_SCOPE_INVALID')
    daily = {}
    for key, ref in manifest.get('index_close', {}).items():
        value = selected(ref)
        # Input SHA binds the normalized source object the pure calculator
        # actually receives; backing file SHA remains separately retained.
        value['input_sha256'] = hashlib.sha256(_original(ref, manifest_path.parent)).hexdigest()
        if value.get('pit', {}).get('source_trade_date') != day.isoformat():
            raise ValueError('SHADOW_INDEX_DAY_MISMATCH')
        daily[key] = value
    minutes = {}
    for key, ref in manifest.get('minutes', {}).items():
        minutes[key] = dict(payload=selected(ref), source_id=ref['source_id'],
            input_sha256=hashlib.sha256(_original(ref, manifest_path.parent)).hexdigest(),
            collection_status=ref['collection_status'],pit=ref['pit'])
    result = dict(schema_version='track2-close-shadow-output/1', authority='SHADOW_ONLY', enabled=False,
        manifest_sha256=hashlib.sha256(raw).hexdigest(),
        plan_scope=dict(trade_date=scope['trade_date'], symbols=scope['symbols'],
            file_sha256=plan_ref['file_sha256'], evidence_class=scope['evidence_class']),
        mkt2=market_close_facts(daily, definitions=definitions, calendar=calendar),
        dk=build_dk_shadow(minutes, plan_symbols=scope['symbols'], definitions=definitions,
            trade_date=day, observed_at=now, calendar=calendar),
        publication_gates=dict(h_full='NOT_RUN', r_full='NOT_RUN', runtime_40_10_10='NOT_RUN',
            three_batches='NOT_RUN', d5='NOT_RUN', d8='NOT_RUN', final_accept='NOT_RECEIVED'),
        provider_calls=0, model_calls=0, production_writes=0)
    changed = []
    for path, sha in sorted(bindings.items()):
        item = Path(path)
        if (not item.is_file() or item.stat().st_size > 8*1024*1024
                or any(p.is_symlink() for p in (item, *item.parents))
                or hashlib.sha256(item.read_bytes()).hexdigest() != sha):
            changed.append(path)
    if changed:
        raise ValueError('SHADOW_SOURCE_CHANGED_DURING_RUN')
    result['original_files_rechecked'] = len(bindings)
    result['original_bindings'] = bindings
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--config', type=Path, default=Path(__file__).resolve().parents[1]/'config')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    output_root = Path(__file__).resolve().parents[1]/'artifacts/track2'
    if (not args.output.resolve().is_relative_to(output_root.resolve())
            or any(p.is_symlink() for p in (args.output, *args.output.parents))):
        raise ValueError('SHADOW_OUTPUT_OUTSIDE_OWNED_ARTIFACTS')
    result = build(args.manifest, args.config, calendar=market_calendar())
    with args.output.open('x', encoding='utf-8') as stream:
        json.dump(result, stream, ensure_ascii=False, sort_keys=True, indent=2)
    print(json.dumps(dict(output=str(args.output), series=len(result['dk']['series']),
        source_calls=0, model_calls=0, production_writes=0)))
