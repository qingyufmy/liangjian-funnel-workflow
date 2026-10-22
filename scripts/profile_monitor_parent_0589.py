"""Isolated *legacy* monitor parent CPU/DB profile, not full production acceptance.

CLI: --directory EXISTING_ISOLATED_PARENT --packet JSON --packet-sha256 SHA
Default: one generated >=1,490,000,000-byte SQLite DB, ten fresh interpreters.
Each child constructs the real WorkflowApplication and calls its real
monitor_once(now=frozen_as_of). Actual wall time is never changed. The same
three already-active CONTROLLED plans and immutable acquisition packet are
repeated; this is not ten natural market minutes or cold-storage performance.

Packet schema monitor-parent-packet/0589-v1 contains: as_of, plans (three
{plan_id,symbol,payload}), bars (normalized MinuteBar JSON array), quotes
(MarketQuote JSON array), market_state, provenance. Each of the four data
categories has provenance either {kind:CONTROLLED,reason:...} or
{kind:ORIGINAL,path:absolute,sha256:...,pointer:RFC6901_JSON_pointer}. ORIGINAL
must exactly equal the referenced original JSON subtree. Plans MUST be
CONTROLLED: this harness does not reproduce production activation. Missing
historical 5m/daily-facts are explicitly reported as absent, never invented.
Optional daily_facts (plan_id -> original fact mapping) and historical_5m
(normalized prior-day MinuteBar array) need the same provenance contract.

Acquisition seams only: frozen bars, quotes, market state, and already-active
plan activation. Model/HTTP/network calls are denied and any attempted call
fails the profile even if business code catches the exception. Notifications
are explicitly disabled; their original publisher methods are also guarded.
Actual monitor cache writes, publication checks, strategy engine, state and
paper broker remain real and confined to the owned root. A triggered model
review cannot be replaced by fake NO_ACTION. No production DB is copied.

PASS_OFFLINE_PARENT_SCOPE means only the measured, stated scope has p95<20s;
Claude decides whether this satisfies the release gate. --bytes/--samples
allow tiny local tests, which are labelled NOT_GATE_SCALE. No VM/SSH helper.
"""
from __future__ import annotations

import argparse
from contextlib import closing
import gc
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import uuid
from urllib.parse import parse_qs, unquote, urlsplit

PRODUCTION = Path('/media/qy/120aebe1-8c98-412b-a490-3797cae5656b/Agu/liangjian-funnel-workflow')
ROOT = Path(__file__).resolve().parents[1]
SCHEMA = 'monitor-parent-packet/0589-v1'


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''): h.update(chunk)
    return h.hexdigest()


def child_failure_diagnostic(stderr):
    """Keep bounded stack locations and stable harness codes, never raw messages."""
    frames=[]
    for line in stderr.splitlines():
        match=re.fullmatch(r'\s*File "([^"]+)", line (\d+), in ([A-Za-z0-9_<>.]+)',line)
        if match:
            path=Path(match[1])
            try:shown=path.resolve().relative_to(ROOT.resolve()).as_posix()
            except (ValueError,OSError):shown=path.name
            frames.append(dict(file=shown[:240],line=int(match[2]),function=match[3][:80]))
    last=next((line for line in reversed(stderr.splitlines()) if line.strip()),'')
    exception=re.match(r'^([A-Za-z_][A-Za-z0-9_.]*)(?::|$)',last)
    code=re.fullmatch(r'[A-Za-z_][A-Za-z0-9_.]*:\s*(PROFILE_[A-Z0-9_]+)',last)
    return dict(stderr_characters=len(stderr),frames=frames[-16:],
        exception_type=exception[1][:80] if exception else 'UNKNOWN',
        harness_reason_code=code[1] if code else None,raw_message_retained=False)


def pointer(body, path):
    if path == '': return body
    if not isinstance(path, str) or not path.startswith('/'):
        raise ValueError('PROFILE_ORIGINAL_POINTER_INVALID')
    for key in path[1:].split('/'):
        key = key.replace('~1', '/').replace('~0', '~')
        body = body[int(key)] if isinstance(body, list) else body[key]
    return body


def load_packet(path, sha):
    from datetime import datetime
    from liangjian_funnel.data.mootdx import MinuteBar
    from liangjian_funnel.data.tencent_minute import MarketQuote
    if digest(path) != sha: raise ValueError('PROFILE_PACKET_SHA_MISMATCH')
    packet = json.loads(Path(path).read_text(encoding='utf-8'))
    if packet.get('schema_version') != SCHEMA: raise ValueError('PROFILE_PACKET_SCHEMA')
    at = datetime.fromisoformat(packet['as_of'])
    if at.utcoffset() is None or at.second or at.microsecond:
        raise ValueError('PROFILE_FROZEN_CLOCK_INVALID')
    plans = packet['plans']
    symbols = {row['symbol'] for row in plans}
    if len(plans) != 3 or len(symbols) != 3 or len({row['plan_id'] for row in plans}) != 3:
        raise ValueError('PROFILE_EXPECTS_THREE_DISTINCT_PLANS')
    categories = ('plans', 'bars', 'quotes', 'market_state') + tuple(
        key for key in ('daily_facts','historical_5m') if key in packet)
    if set(packet['provenance']) != set(categories): raise ValueError('PROFILE_PROVENANCE_SCOPE_MISMATCH')
    for category in categories:
        proof = packet['provenance'][category]
        if proof.get('kind') == 'CONTROLLED':
            if not str(proof.get('reason', '')).strip(): raise ValueError('PROFILE_CONTROLLED_REASON_REQUIRED')
        elif proof.get('kind') == 'ORIGINAL':
            source = Path(proof['path'])
            if not source.is_absolute() or digest(source) != proof['sha256']:
                raise ValueError('PROFILE_ORIGINAL_SHA_MISMATCH')
            body = json.loads(source.read_text(encoding='utf-8'))
            if pointer(body, proof['pointer']) != packet[category]:
                raise ValueError('PROFILE_ORIGINAL_SUBTREE_MISMATCH')
        else: raise ValueError('PROFILE_PROVENANCE_REQUIRED')
    if packet['provenance']['plans']['kind'] != 'CONTROLLED':
        raise ValueError('PROFILE_ACTIVATION_IS_CONTROLLED')
    bars = [MinuteBar.model_validate(row) for row in packet['bars']]
    quotes = [MarketQuote.model_validate(row) for row in packet['quotes']]
    if {b.symbol for b in bars} != symbols or {q.symbol for q in quotes} != symbols or len(quotes) != 3:
        raise ValueError('PROFILE_ACQUISITION_SCOPE_MISMATCH')
    if any(b.interval != '1m' or b.bar_end.date() != at.date() or b.bar_end >= at for b in bars):
        raise ValueError('PROFILE_BARS_NOT_FROZEN_CLOSED_1M')
    if len({(b.symbol, b.bar_end) for b in bars}) != len(bars):
        raise ValueError('PROFILE_DUPLICATE_BAR')
    if any(q.quote_time.date() != at.date() or q.quote_time > at for q in quotes):
        raise ValueError('PROFILE_QUOTE_CLOCK_INVALID')
    if 'daily_facts' in packet and (not isinstance(packet['daily_facts'],dict) or
            set(packet['daily_facts']) != {row['plan_id'] for row in plans}):
        raise ValueError('PROFILE_DAILY_FACT_SCOPE_MISMATCH')
    if 'historical_5m' in packet:
        historical = [MinuteBar.model_validate(row) for row in packet['historical_5m']]
        if any(bar.symbol not in symbols or bar.interval != '5m' or bar.bar_end.date() >= at.date() for bar in historical):
            raise ValueError('PROFILE_HISTORICAL_5M_INVALID')
    return packet


def settings_for(directory):
    from liangjian_funnel.settings import Settings
    settings = Settings.from_env({}, root=directory)
    return settings.model_copy(update=dict(
        prompt_dir=ROOT/'prompts', source_config_path=ROOT/'config/funnel_config_v2.yaml',
        exchange_rules_path=ROOT/'config/exchange_rules.yaml'))


def install_guards(directory):
    """Audit guard cannot be disabled by a swallowed provider exception."""
    attempted = []
    owner = Path(directory).resolve()

    def deny(kind):
        attempted.append(kind)
        raise RuntimeError('PROFILE_FORBIDDEN_' + kind)

    def writable(path):
        if isinstance(path, int): return
        p = Path(os.fsdecode(path)).resolve()
        if not p.is_relative_to(owner): deny('WRITE_OUTSIDE_OWNED_ROOT')

    def sqlite_path(value):
        if isinstance(value,str) and value.startswith('file:'):
            parsed=urlsplit(value)
            try:query=parse_qs(parsed.query,strict_parsing=True)
            except ValueError:return deny('SQLITE_URI_NOT_READONLY')
            if (parsed.scheme!='file' or parsed.netloc not in ('','localhost') or parsed.fragment
                    or query!={'mode':['ro']}):
                return deny('SQLITE_URI_NOT_READONLY')
            decoded=unquote(parsed.path)
            if os.name=='nt' and re.match(r'^/[A-Za-z]:/',decoded):decoded=decoded[1:]
            return Path(decoded)
        return value

    def audit(event, args):
        if event in ('socket.connect', 'socket.getaddrinfo', 'socket.bind'):
            deny('NETWORK')
        if event == 'open':
            path, mode, flags = args
            if (isinstance(mode, str) and any(c in mode for c in 'wax+')) or (
                    isinstance(flags, int) and flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND)):
                writable(path)
        elif event == 'sqlite3.connect': writable(sqlite_path(args[0]))
        elif event in ('os.remove', 'os.rmdir', 'os.mkdir'): writable(args[0])
        elif event in ('os.rename', 'os.link', 'os.symlink'):
            writable(args[0]); writable(args[1])
        elif event == 'subprocess.Popen': deny('CHILD_PROCESS')
    sys.addaudithook(audit)
    from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient
    def model_denied(*args, **kwargs): return deny('MODEL')
    OpenAICompatibleModelClient.complete = model_denied
    from liangjian_funnel.runtime.lark_notifications import WorkflowLarkPublisher
    for name in dir(WorkflowLarkPublisher):
        if name.startswith('publish_'):
            setattr(WorkflowLarkPublisher, name, lambda *a, **kw: deny('NOTIFICATION'))
    return attempted


def child(directory, packet_path, packet_sha):
    started = time.perf_counter()
    from datetime import datetime
    from liangjian_funnel.workflow import WorkflowApplication
    import liangjian_funnel.workflow as workflow
    from liangjian_funnel.data.mootdx import FetchResult, MinuteBar
    from liangjian_funnel.data.tencent_minute import MarketQuote, QuoteResult
    # Python's Windows platform probe may run `ver` once. Do it before the
    # permanent no-subprocess guard; included in fresh-process import timing.
    import platform
    platform.uname()
    imported = time.perf_counter()
    packet = load_packet(packet_path, packet_sha)
    checked = time.perf_counter()
    settings = settings_for(Path(directory))
    attempted = install_guards(directory)
    app = WorkflowApplication(settings)
    constructed = time.perf_counter()
    app.lark_publisher = None  # Declared disabled boundary, not fake SENT.
    app.activate_latest_a3_for_monitor = lambda **_: {
        'status': 'CONTROLLED_ALREADY_ACTIVE', 'production_activation_verified': False}
    bars = [MinuteBar.model_validate(row) for row in packet['bars']]
    quotes = {row['symbol']: MarketQuote.model_validate(row) for row in packet['quotes']}
    acquisition_calls = {'bars': 0, 'quote': 0, 'market': 0}

    def frozen_bars(symbol, interval, required, cutoff, **_):
        acquisition_calls['bars'] += 1
        if interval != '1m': raise RuntimeError('PROFILE_UNEXPECTED_ACQUISITION_INTERVAL')
        rows = tuple(sorted((bar for bar in bars if bar.symbol == symbol and bar.bar_end <= cutoff), key=lambda bar: bar.bar_end))
        return FetchResult(symbol=symbol, interval=interval, requested_bars=required,
            returned_bars=len(rows), bars=rows, complete=len(rows) >= required,
            reason_code='OK' if len(rows) >= required else 'INSUFFICIENT_BARS')

    def frozen_quote(symbol, *_, **__):
        acquisition_calls['quote'] += 1
        return QuoteResult(symbol=symbol, quote=quotes[symbol], complete=True, reason_code='OK')

    def frozen_market(*_, **__):
        acquisition_calls['market'] += 1
        return dict(packet['market_state'])

    app._fetch_live_bars = frozen_bars
    app._fetch_live_quote = frozen_quote
    workflow.load_or_refresh_live_market_state = frozen_market
    if 'daily_facts' in packet:
        import liangjian_funnel.runtime.entry_confirmation as confirmation
        def frozen_daily(settings, rows, **_):
            # Isolated acquisition seam only. Original strategy stays intact.
            return {row['plan_id']:dict(packet['daily_facts'][row['plan_id']]) for row in rows}
        confirmation.runtime_confirmation_facts = frozen_daily
    seams_ready = time.perf_counter()
    result = app.monitor_once(now=datetime.fromisoformat(packet['as_of']))
    finished = time.perf_counter()
    required = sorted(row['symbol'] for row in packet['plans'])
    obs = result['observability']
    errors = result['minute_cache']['errors']
    if attempted: raise RuntimeError('PROFILE_FORBIDDEN_ATTEMPT_RECORDED')
    if sorted(obs['required_scope']) != required or errors:
        raise RuntimeError('PROFILE_REAL_MONITOR_SCOPE_OR_CACHE_ERROR')
    if acquisition_calls['bars'] < 3 or acquisition_calls['quote'] < 3 or acquisition_calls['market'] < 1:
        raise RuntimeError('PROFILE_ACQUISITION_SEAMS_NOT_REACHED')
    return dict(pid=os.getpid(), import_ms=(imported-started)*1000,
        packet_verification_ms=(checked-imported)*1000, constructor_ms=(constructed-checked)*1000,
        seam_install_ms=(seams_ready-constructed)*1000, monitor_ms=(finished-seams_ready)*1000,
        inner_total_ms=(finished-started)*1000, model_calls=0, forbidden_attempts=attempted,
        acquisition_calls=acquisition_calls, required_scope=obs['required_scope'],
        ready_scope=obs['ready_scope'],blocked_scope=obs['blocked_scope'],terminal_reason=obs['terminal_reason'],
        business_reasons=[dict(symbol=event.get('symbol'),action=event['action'],reason_code=event['reason_code'])
            for lane in result['lanes'] for event in lane['events']],
        timing_spans=obs['timing_spans'], cache_errors=errors,
        phase_timings=result['phase_timings'], notifications_disabled=True)


def generate_database(directory, packet, minimum_bytes):
    from datetime import datetime
    from liangjian_funnel.runtime.state import RuntimeStore, PlanStatus
    database = settings_for(directory).state_db_path
    store = RuntimeStore(database)
    at = datetime.fromisoformat(packet['as_of'])
    for row in packet['plans']:
        store.create_execution_plan(row['plan_id'], 'lane_1', row['symbol'],
            status=PlanStatus.ACTIVE_TODAY, valid_from=at.replace(hour=9, minute=26),
            expires_at=at.replace(hour=15, minute=0), payload=row['payload'])
    del store
    gc.collect()
    with closing(sqlite3.connect(database)) as db:
        db.execute('PRAGMA journal_mode=DELETE')
        db.execute('CREATE TABLE profile_generated_ballast(payload BLOB NOT NULL)')
        while database.stat().st_size < minimum_bytes:
            for _ in range(min(8, max(1, math.ceil((minimum_bytes-database.stat().st_size)/1048576)))):
                db.execute('INSERT INTO profile_generated_ballast VALUES(zeroblob(1048576))')
            db.commit()
        db.execute('PRAGMA journal_mode=WAL')
    checked = RuntimeStore(database, integrity_maintenance=True)
    if not checked.healthy: raise RuntimeError('PROFILE_GENERATED_DB_NOT_HEALTHY')
    del checked
    gc.collect()
    if packet.get('historical_5m'):
        from liangjian_funnel.data.cache import MinuteBarStore
        from liangjian_funnel.data.mootdx import MinuteBar
        cache=MinuteBarStore(settings_for(directory).minute_cache_dir)
        cache.write([MinuteBar.model_validate(row) for row in packet['historical_5m']])
        del cache
        gc.collect()
    return database


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values)*fraction)-1)]


def summarize(samples):
    phase_names = sorted({span['name'] for sample in samples for span in sample['timing_spans']})
    phases = {}
    for name in phase_names:
        values = [sum(span['duration_ms'] for span in sample['timing_spans'] if span['name'] == name) for sample in samples]
        phases[name] = dict(p50_ms=percentile(values,.5), p95_ms=percentile(values,.95), max_ms=max(values))
    for name in ('import_ms','packet_verification_ms','constructor_ms','seam_install_ms','monitor_ms','inner_total_ms'):
        values=[sample[name] for sample in samples if name in sample]
        if values:
            phases['parent:'+name]=dict(p50_ms=percentile(values,.5),p95_ms=percentile(values,.95),max_ms=max(values))
    walls = [sample['parent_wall_ms'] for sample in samples]
    return dict(p50_ms=percentile(walls,.5),p95_ms=percentile(walls,.95),max_ms=max(walls),phases=phases)


def source_fingerprints():
    files=('src/liangjian_funnel/workflow.py','src/liangjian_funnel/runtime/state.py',
        'src/liangjian_funnel/runtime/strategies.py','src/liangjian_funnel/runtime/entry_confirmation.py',
        'config/entry_confirmation.yaml','config/exchange_rules.yaml','config/funnel_config_v2.yaml',
        'prompts/agent_4_intraday_veto_v3.txt','scripts/profile_monitor_parent_0589.py')
    return {name:digest(ROOT/name) for name in files}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path)
    parser.add_argument('--packet', type=Path, required=True)
    parser.add_argument('--packet-sha256', required=True)
    parser.add_argument('--bytes',type=int,default=1_490_000_000)
    parser.add_argument('--samples',type=int,default=10)
    parser.add_argument('--cleanup',action='store_true',help='remove only this generated owned child after emitting result')
    parser.add_argument('--child',type=Path,help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    sys.path.insert(0,str(ROOT/'src'))
    if args.child:
        print(json.dumps(child(args.child,args.packet,args.packet_sha256),ensure_ascii=False))
        return 0
    if args.directory is None or not args.directory.is_dir() or args.directory.is_symlink():
        parser.error('existing real --directory required')
    parent = args.directory.resolve()
    if parent == PRODUCTION.resolve() or parent.is_relative_to(PRODUCTION.resolve()):
        parser.error('production directory forbidden')
    if args.bytes < 0 or not 1 <= args.samples <= 100: parser.error('invalid scale')
    if shutil.disk_usage(parent).free < args.bytes + 256*1024*1024: parser.error('insufficient free space')
    packet = load_packet(args.packet,args.packet_sha256)
    source_pins=source_fingerprints()
    directory = Path(tempfile.mkdtemp(prefix='monitor-parent-0589-',dir=parent))
    owner={'run_id':uuid.uuid4().hex,'kind':'generated-not-production'}
    (directory/'OWNER.json').write_text(json.dumps(owner),encoding='utf-8')
    (directory/'config').mkdir()
    shutil.copyfile(ROOT/'config/entry_confirmation.yaml',directory/'config/entry_confirmation.yaml')
    database = generate_database(directory,packet,args.bytes)
    samples = []
    for _ in range(args.samples):
        begin = time.perf_counter()
        env = {key:value for key,value in os.environ.items() if key in ('PATH','SYSTEMROOT','WINDIR','TEMP','TMP','LANG','LC_ALL')}
        env['PYTHONDONTWRITEBYTECODE']='1'
        proc = subprocess.run([sys.executable,'-B',str(Path(__file__).resolve()),'--child',str(directory),
            '--packet',str(args.packet.resolve()),'--packet-sha256',args.packet_sha256],
            capture_output=True,text=True,env=env,timeout=90)
        if proc.returncode:
            # Preserve only bounded, redacted stack locations; source lines and
            # arbitrary exception messages may contain credentials/provider text.
            diagnostic=child_failure_diagnostic(proc.stderr)
            diagnostic_path=directory/'CHILD_STDERR_DIAGNOSTIC.json'
            with diagnostic_path.open('x',encoding='utf-8') as stream:
                json.dump(diagnostic,stream,ensure_ascii=False)
            failure = dict(status='FAIL_CHILD',returncode=proc.returncode,
                stdout_sha256=hashlib.sha256(proc.stdout.encode()).hexdigest(),
                stderr_sha256=hashlib.sha256(proc.stderr.encode()).hexdigest(),
                stderr_diagnostic_path=str(diagnostic_path),stderr_diagnostic_sha256=digest(diagnostic_path))
            (directory/'FAILURE.json').write_text(json.dumps(failure),encoding='utf-8')
            raise RuntimeError('PROFILE_CHILD_FAILED_SEE_OWNED_FAILURE')
        sample = json.loads(proc.stdout)
        sample['parent_wall_ms']=(time.perf_counter()-begin)*1000
        samples.append(sample)
    if len({sample['pid'] for sample in samples}) != args.samples:
        raise RuntimeError('PROFILE_FRESH_PROCESS_COUNT_MISMATCH')
    if source_fingerprints()!=source_pins: raise RuntimeError('PROFILE_SOURCE_CHANGED_DURING_MEASUREMENT')
    summary = summarize(samples)
    gate_scale = args.samples >= 10 and database.stat().st_size >= 1_490_000_000
    ready = all(sample['required_scope']==sample['ready_scope'] and not sample['blocked_scope'] for sample in samples)
    originals = {key:value['kind'] for key,value in packet['provenance'].items()}
    result = dict(schema_version='monitor-parent-profile/0589-v1',scope='LEGACY_MONITOR_ONCE_OFFLINE_CPU_DB',
        status=('PASS_OFFLINE_PARENT_SCOPE' if summary['p95_ms'] < 20000 else 'FAIL_P95') if gate_scale and ready
            else 'EVIDENCE_LIMITED_SCOPE_BLOCKED' if not ready else 'NOT_GATE_SCALE',
        actual_wall_completed_at=__import__('datetime').datetime.now().astimezone().isoformat(),
        frozen_as_of=packet['as_of'], packet_path=str(args.packet.resolve()),packet_sha256=args.packet_sha256,
        output_root=str(directory),actual_db_bytes=database.stat().st_size,samples=samples,summary=summary,
        source_sha256=source_pins,
        provenance=originals,entry_confirmation_config_sha256=digest(directory/'config/entry_confirmation.yaml'),
        optional_acquisition={'daily_facts':'PROVIDED' if 'daily_facts' in packet else 'ABSENT_NOT_SYNTHESIZED',
            'historical_5m':'PROVIDED' if 'historical_5m' in packet else 'ABSENT_NOT_SYNTHESIZED'},
        model_http_calls=0,production_db_copy=False,
        limitations=['Controlled already-active plans, not production activation.',
            'Acquisition is frozen; provider latency and Team monitor_dispatch are not measured.',
            'Notifications disabled; no live model judgment or actual execution-chain acceptance.',
            'Absent historical 5m/daily-facts are not synthesized.',
            'Generated ballast page volume is not real production index/event shape.',
            'Same packet/shared isolated state repeated in fresh interpreters; OS cache may be warm.',
            'Not a complete day replay; Claude must decide release-gate adequacy.'])
    (directory/'RESULT.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    result['generated_files_removed']=False
    if args.cleanup:
        if (directory.resolve().parent != parent or directory.is_symlink() or
                not directory.name.startswith('monitor-parent-0589-') or
                json.loads((directory/'OWNER.json').read_text()) != owner or
                any(path.is_symlink() or not path.resolve().is_relative_to(directory.resolve()) for path in directory.rglob('*'))):
            raise ValueError('PROFILE_CLEANUP_OWNERSHIP_CONFLICT')
        shutil.rmtree(directory)
        result['generated_files_removed']=True
    print(json.dumps(result,ensure_ascii=False))
    return 0 if summary['p95_ms'] < 20000 and ready else 1


if __name__ == '__main__': raise SystemExit(main())
