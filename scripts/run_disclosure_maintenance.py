"""Preview or explicitly execute a frozen night disclosure queue.

Default is read-only: no env/cache/client initialization. --execute warms only
official disclosure caches; never constructs RuntimeStore, models or Lark.
Scheduling/promotion remains separately authorized.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager, ExitStack
from datetime import datetime
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from liangjian_funnel.pipeline.disclosure_maintenance import run_maintenance, validate_queue


@contextmanager
def open_collector(queue):
    # Import and construct adapters only after the explicit execute flag.
    from liangjian_funnel.data.bse import BseClient
    from liangjian_funnel.data.cninfo import CninfoClient
    from liangjian_funnel.data.cninfo_pdf import CninfoPdfClient
    from liangjian_funnel.data.exchange_disclosure import SseDisclosureClient, SzseDisclosureClient
    from liangjian_funnel.data.disclosure_router import OfficialDisclosureRouter
    from liangjian_funnel.pipeline.disclosure_cache_worker import DisclosureCacheWorker
    from liangjian_funnel.settings import Settings
    settings = Settings.from_env(root=Path.cwd())
    # One maintenance process per cache, even if different queues are used.
    lock = settings.fact_cache_db_path.with_suffix('.disclosure-maintenance.lock')
    lock.parent.mkdir(parents=True, exist_ok=True)
    try:
        handle = lock.open('x', encoding='utf-8')
    except FileExistsError as exc:
        raise ValueError('DISCLOSURE_CACHE_MAINTENANCE_ALREADY_RUNNING') from exc
    try:
        with handle:
            handle.write(queue['queue_hash'])
        with ExitStack() as stack:
            args = dict(timeout_seconds=settings.timeout_seconds,
                        min_request_interval_seconds=settings.cninfo_min_request_interval_seconds)
            cninfo = stack.enter_context(CninfoClient(**args, base_url=settings.cninfo_base_url))
            bse = stack.enter_context(BseClient(**args))
            sse = stack.enter_context(SseDisclosureClient(**args))
            szse = stack.enter_context(SzseDisclosureClient(**args))
            pdf = stack.enter_context(CninfoPdfClient(settings.cninfo_pdf_cache_dir,
                timeout_seconds=settings.timeout_seconds))
            worker = DisclosureCacheWorker(settings,
                OfficialDisclosureRouter(cninfo, sse=sse, szse=szse, bse=bse), pdf)
            if queue['deferred_symbols']:
                # Catalogue network work runs lazily inside the same bounded
                # collector gate, never before the maintenance deadline starts.
                worker.configure_catalog(cninfo, queue['deferred_symbols'])
            yield worker
    finally:
        lock.unlink(missing_ok=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--queue', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--budget-seconds', type=float, default=3600)
    args = parser.parse_args(argv)
    queue = json.loads(args.queue.read_text(encoding='utf-8'))
    validate_queue(queue)
    now = datetime.now(ZoneInfo('Asia/Shanghai'))
    if args.execute:
        with open_collector(queue) as worker:
            report = run_maintenance(queue, output_dir=args.output_dir, now=now,
                execute=True, collect=worker.collect, can_reuse=worker.can_reuse,
                budget_seconds=args.budget_seconds)
    else:
        report = run_maintenance(queue, output_dir=args.output_dir, now=now,
            budget_seconds=args.budget_seconds)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 1 if report['status'] == 'PARTIAL_FAILURE' else 0


if __name__ == '__main__':
    raise SystemExit(main())
