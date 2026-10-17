"""Measure the existing A4 request without claiming/calling a model.

Stop immediately after the real repository renders messages, before trader or
model claims. Original inputs are preserved; no proposed compression is applied.
"""
import argparse
from collections import Counter
from datetime import datetime
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from liangjian_funnel.pipeline.prompts import _replacement_text
from liangjian_funnel.runtime.team_v2_control_minute import run_control_minute
from run_team_a4_frozen_probe import prepare
from run_team_a5_frozen_dryrun import save


class ProfileComplete(Exception):
    pass


def size(value):
    rendered = _replacement_text(value)
    return dict(chars=len(rendered), utf8_bytes=len(rendered.encode('utf-8')))


def describe(values, request):
    """Component sizes are separate from the exact final-message denominator."""
    catalog = values['EVIDENCE_CATALOG']
    categories = Counter()
    largest = []
    context_fields = Counter()
    plan_fields = Counter()
    for item in catalog['items']:
        categories[item['category']] += size(item)['chars']
        value = item.get('value', {})
        if isinstance(value, dict):
            target = context_fields if item['category'] == 'STOCK_TECH' else plan_fields if item['category'] == 'PLAN' else None
            if target is not None:
                for key, field in value.items():
                    target[key] += size(field)['chars']
        largest.append(dict(id=item['id'], category=item['category'], **size(item)))
    messages = [dict(message) for message in request.messages]
    content_chars = sum(len(message['content']) for message in messages)
    return dict(
        exact_message_content_chars=content_chars,
        exact_message_content_utf8_bytes=sum(len(m['content'].encode('utf-8')) for m in messages),
        messages=[dict(role=m['role'], chars=len(m['content']), utf8_bytes=len(m['content'].encode('utf-8'))) for m in messages],
        input_hash=request.input_hash, catalog_hash=request.evidence_catalog_hash,
        source_hashes=dict(request.prompt.source_hashes), role_version=request.prompt.role_version,
        replacements={key:size(value) for key, value in sorted(values.items())},
        unavailable_placeholders=sorted(key for key, value in values.items()
            if isinstance(value, dict) and value.get('status') == 'UNAVAILABLE'),
        catalog_item_chars_by_category=dict(categories),
        catalog_item_count_by_category=dict(Counter(item['category'] for item in catalog['items'])),
        largest_catalog_items=sorted(largest, key=lambda row:row['chars'], reverse=True)[:10],
        stock_tech_field_value_chars=dict(context_fields.most_common()),
        plan_field_value_chars=dict(plan_fields.most_common()),
        active_plan_fields=sorted(set().union(*(set(p) for p in values.get('ACTIVE_PLANS', [])))),
        counting_note='Exact denominator is final message content, not tokens. Component/nested sizes overlap; do not add them. No token estimate or latency root-cause claim.')


class CaptureRepository:
    def __init__(self, repository):
        self.repository = repository
        self.captured = None

    def __getattr__(self, name):
        return getattr(self.repository, name)

    def prepare_messages(self, *args, **kwargs):
        request = self.repository.prepare_messages(*args, **kwargs)
        self.captured = (args[1], request)
        raise ProfileComplete()


class NoModelClient:
    def complete(self, *args, **kwargs):
        raise AssertionError('OFFLINE_PROFILE_NETWORK_FORBIDDEN')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-pack', type=Path, required=True)
    parser.add_argument('--decision-at', required=True)
    parser.add_argument('--output', type=Path, required=True)
    options = parser.parse_args()
    output = options.output.resolve()
    if output.exists() or not output.is_relative_to((ROOT / 'artifacts/team-v2-20261016').resolve()):
        raise ValueError('PROFILE_NEW_OWNED_DIRECTORY_REQUIRED')
    output.mkdir(parents=True)
    # These fields suffice for the source-bound preparation. No env, provider
    # key, SSH, WorkflowApplication or production RuntimeStore is constructed.
    settings = SimpleNamespace(root=ROOT, review_model='deepseek-v4-pro-0813')
    args, bars, source = prepare(options.source_pack.resolve(), output, options.decision_at, settings)
    repository = CaptureRepository(args['repository'])
    args.update(repository=repository, client=NoModelClient())
    try:
        run_control_minute(session_kwargs=args, closed_bars=bars, deadline_monotonic=time.monotonic()+47)
    except ProfileComplete:
        pass
    if repository.captured is None:
        raise ValueError('PROFILE_REQUEST_NOT_REACHED')
    values, request = repository.captured
    original = json.dumps(dict(values=values, messages=[dict(m) for m in request.messages]),
        ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf-8')
    with gzip.open(output / 'ORIGINAL_RENDERED_INPUT.json.gz', 'wb') as stream:
        stream.write(original)
    result = dict(status='OFFLINE_ORIGINAL_INPUT_PROFILE_NOT_MODEL_GATE',
        actual_collected_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
        source_head=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        original_rendered_input_sha256=hashlib.sha256(original).hexdigest(),
        source=source, profile=describe(values, request),
        model_calls=0, model_claims=0, orders=len(args['broker'].get_orders(args['trade_date'])),
        production_writes=0, provider_config_reads=0,
        unchanged_renderer=True, release_gate='NOT_RUN')
    save(output / 'PROFILE.json', result)
    print(json.dumps({key:result[key] for key in ('status','actual_collected_at','model_calls','model_claims','orders')}))
    print(json.dumps(result['profile'], ensure_ascii=True))
    return 0


if __name__ == '__main__':
    sys.exit(main())
