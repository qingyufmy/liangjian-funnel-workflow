"""0589 external VM release supervisor; import/default preflight never deploy.

Example (all files are reviewed, transferred VM originals):
  .venv/bin/python -B /var/tmp/OWN/supervise_release_0589.py \
    --binding /var/tmp/OWN/BINDING.json --approval /var/tmp/OWN/FINAL.json \
    --approval-sha256 SHA [--phase preflight|deploy|rollback]

The final approval binds the *file SHA* of BINDING.json and exact new Claude
reply/quotes. No model probe, notification, env/latch edit, service kill or
historical marker is part of this program. Lifecycle verification runs in this
outer process, never inside the transient deploy unit.
"""
from contextlib import closing
from datetime import datetime
from pathlib import Path
from urllib.request import urlopen
from zoneinfo import ZoneInfo
import argparse
import hashlib
import importlib.util
import json
import os
import re
import sqlite3
import subprocess
import sys
import sysconfig
import time

ROOT = Path('/media/qy/120aebe1-8c98-412b-a490-3797cae5656b/Agu/liangjian-funnel-workflow')
SAFE_PATH = '/www/server/nodejs/v24.18.0/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'
HEALTH = 'http://127.0.0.1:3210/api/health'
PROJECT_ROOT_DECLARATION = b'PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"'
REQUIRED_GATES = {'production_startup', 'timeout_consistency', 'whole_chain'}
PUBLIC_RUNTIME = {'research_models', 'review_model', 'monitor_model', 'team_intraday_model',
                  'a4_trader_mode', 'broker_adapter', 'model_base_url'}
PROTECTED = {
    'workflow.sqlite3': ('execution_plans',),
    'team_v2/model.db': ('model_call_claims',),
    'team_v2/team.db': ('control_record_latch', 'risk_day_state'),
    'team_v2/paper.db': ('paper_meta', 'paper_lots', 'paper_orders', 'paper_fills', 'paper_risk_days', 'exec_ledger'),
}


class ReleaseError(RuntimeError):
    pass


def need(value, reason):
    if not value:
        raise ReleaseError(reason)


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def aware(value):
    d = datetime.fromisoformat(value) if isinstance(value, str) else value
    need(isinstance(d, datetime) and d.utcoffset() is not None, 'RELEASE_AWARE_WINDOW_REQUIRED')
    return d


def real_now():
    return datetime.now(ZoneInfo('Asia/Shanghai'))


def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def exact_json(ref):
    need(re.fullmatch('[0-9a-f]{64}', ref.get('sha256', '')) is not None, 'RELEASE_RECEIPT_SHA_REQUIRED')
    need(sha(ref['path']) == ref['sha256'], 'RELEASE_RECEIPT_BYTES_CHANGED')
    return json.loads(Path(ref['path']).read_bytes())


def validate_final(binding, approval, *, binding_sha):
    need(approval.get('schema') == 'release-final/0589' and approval.get('decision') == 'ACCEPT', 'RELEASE_FINAL_ACCEPT_REQUIRED')
    need(approval.get('binding_sha256') == binding_sha, 'RELEASE_FINAL_BINDING_SHA_CHANGED')
    for key in ('candidate_head', 'baseline_head', 'rollback_head'):
        need(re.fullmatch('[0-9a-f]{40}', binding.get(key, '')) is not None and approval.get(key) == binding[key], 'RELEASE_FINAL_HEAD_BINDING')
    need(approval.get('deploy_accepted') is True and approval.get('automatic_forward_rollback_accepted') is True,
         'RELEASE_EXPLICIT_DEPLOY_AND_FORWARD_ROLLBACK_REQUIRED')
    reply = approval['reply']
    need(sha(reply['path']) == reply['sha256'], 'RELEASE_FINAL_REPLY_BYTES_CHANGED')
    text = Path(reply['path']).read_text(encoding='utf-8')
    for name in ('deploy_quote', 'rollback_quote'):
        quote = approval.get(name)
        need(isinstance(quote, str) and bool(quote.strip()) and quote in text, 'RELEASE_FINAL_ACTUAL_QUOTE_REQUIRED')
    need(set(approval.get('authorities', {})) == {'0589', '0591'}, 'RELEASE_CURRENT_AUTHORITIES_REQUIRED')
    for ref in approval['authorities'].values():
        need(sha(ref['path']) == ref['sha256'], 'RELEASE_AUTHORITY_BYTES_CHANGED')
    need(approval.get('model_gate_sha256') == binding['model_gate']['sha256'], 'RELEASE_FINAL_MODEL_GATE_BINDING')
    need(set(binding.get('required_gates', {})) == REQUIRED_GATES, 'RELEASE_CORE_GATES_REQUIRED')
    need(binding.get('preserve_env_and_record_latch') is True, 'RELEASE_ENV_LATCH_MUST_BE_PRESERVED')
    need(set(binding.get('expected_runtime', {})) == PUBLIC_RUNTIME, 'RELEASE_COMPLETE_PUBLIC_RUNTIME_REQUIRED')
    need(binding.get('rollback_tree') == binding.get('baseline_tree') and re.fullmatch('[0-9a-f]{40}', binding.get('baseline_tree', '')),
         'RELEASE_ROLLBACK_BASELINE_TREE_REQUIRED')
    need(isinstance(binding.get('baseline_business_hashes'), dict) and bool(binding['baseline_business_hashes']), 'RELEASE_BASELINE_BUSINESS_REQUIRED')
    need(re.fullmatch('[a-z0-9-]{1,48}', binding.get('run_id', '')) is not None, 'RELEASE_NEW_RUN_ID_REQUIRED')
    start, end = aware(binding['not_before']), aware(binding['deadline'])
    need(0 < (end - start).total_seconds() <= 18 * 3600, 'RELEASE_APPROVED_WINDOW_INVALID')
    return approval


def verify_receipts(binding):
    source = Path(binding['candidate_source'])
    verifier = load_module(source / 'scripts/full_test_baseline.py', 'release0589_full_verifier')
    summaries = {}
    for name, head in [('candidate_full', binding['candidate_head']), ('rollback_full', binding['rollback_head'])]:
        ref = binding[name]
        proof = verifier.verify_evidence(Path(ref['path']), ref['sha256'], head)
        need(proof.get('release_qualified') is True and proof.get('failures') == [], 'RELEASE_FULL_NOT_QUALIFIED')
        summaries[name] = dict(head=head, sha256=ref['sha256'], totals=proof['totals'])
    forty = exact_json(binding['same_head40'])
    need(forty.get('head') == binding['candidate_head'] and forty.get('status') == 'PASSED'
         and isinstance(forty.get('cases'), list) and len(forty['cases']) == 40
         and not forty.get('failures'), 'RELEASE_SAME_HEAD40_NOT_PASSED')
    for name, ref in binding['required_gates'].items():
        proof = exact_json(ref)
        need(proof.get('head') == binding['candidate_head'] and proof.get('status') == 'PASSED', 'RELEASE_CORE_GATE_NOT_PASSED_' + name)
        refs = proof.get('originals')
        need(isinstance(refs, list) and bool(refs), 'RELEASE_CORE_GATE_ORIGINALS_REQUIRED_' + name)
        for original in refs:
            need(sha(original['path']) == original['sha256'], 'RELEASE_CORE_GATE_ORIGINAL_CHANGED_' + name)
    model_gate = load_module(source / 'src/liangjian_funnel/runtime/model_change_gate.py', 'release0589_model_gate')
    model_gate.validate_model_change_gate(Path(binding['model_gate']['path']), expected_sha256=binding['model_gate']['sha256'],
        head=binding['candidate_head'], source_trade_date=binding['source_trade_date'], models=binding['models'],
        execution_trade_date=binding['execution_trade_date'], quality_run_id=binding['quality_run_id'],
        role_run_ids=binding['role_run_ids'], provider_base_url=binding['provider_base_url'])
    return summaries


def exclusive(path, body):
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(fd, 'w', encoding='utf-8') as f:
        json.dump(body, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())


def freeze_target_deploy(original, destination, *, target, phase, source_path, expected_git_blob):
    """Freeze the target HEAD's script, never ROOT's mutable running script.

    The only byte change redirects its one PROJECT_ROOT declaration to the
    fixed production root. Prefix/suffix hashes prove all other bytes remain
    exactly those of the already-tested target, including rollback semantics.
    """
    need(original.count(PROJECT_ROOT_DECLARATION) == 1, 'RELEASE_TARGET_PROJECT_ROOT_DECLARATION_NOT_UNIQUE')
    blob = hashlib.sha1(b'blob ' + str(len(original)).encode() + b'\0' + original).hexdigest()
    need(blob == expected_git_blob, 'RELEASE_TARGET_DEPLOY_NOT_APPROVED_GIT_BYTES')
    root = str(ROOT).encode()
    need(b"'" not in root and b'\n' not in root and b'\r' not in root, 'RELEASE_FIXED_ROOT_UNQUOTABLE')
    replacement = b"PROJECT_ROOT='" + root + b"'"
    prefix, declaration, suffix = original.partition(PROJECT_ROOT_DECLARATION)
    frozen = prefix + replacement + suffix
    need(declaration == PROJECT_ROOT_DECLARATION and frozen.replace(replacement, declaration, 1) == original,
         'RELEASE_TARGET_DEPLOY_ONLY_ROOT_REPLACEMENT_FAILED')
    fd = os.open(destination, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(fd, 'wb') as f:
        f.write(frozen)
        f.flush()
        os.fsync(f.fileno())
    frozen_sha = hashlib.sha256(frozen).hexdigest()
    need(sha(destination) == frozen_sha, 'RELEASE_FROZEN_SCRIPT_BYTES_CHANGED')
    return dict(target_head=target, phase=phase, original_path=str(source_path), original_git_blob=blob,
        original_sha256=hashlib.sha256(original).hexdigest(), original_bytes=len(original),
        frozen_path=str(destination), frozen_sha256=frozen_sha, frozen_bytes=len(frozen),
        project_root_declaration_count=1, only_transformation='FIXED_PRODUCTION_PROJECT_ROOT',
        fixed_project_root=str(ROOT), all_other_bytes_unchanged=True,
        prefix_sha256=hashlib.sha256(prefix).hexdigest(), suffix_sha256=hashlib.sha256(suffix).hexdigest())


class VM:
    """Only actual execute methods mutate git/systemd; all probes are bounded RO."""
    def __init__(self, binding):
        self.b = binding
        self.own = Path(binding['output']).resolve()
        self.finished_units = {}

    def command(self, args, *, timeout=30, allowed=(0,)):
        p = subprocess.run(args, capture_output=True, text=True, timeout=timeout, cwd=ROOT,
                           env={**os.environ, 'PATH': SAFE_PATH})
        need(p.returncode in allowed, 'RELEASE_COMMAND_FAILED_' + str(p.returncode) + '_' + hashlib.sha256(p.stderr.encode()).hexdigest())
        return p.stdout.strip()

    def git(self, *args, source=None):
        prefix = ['/usr/sbin/runuser', '-u', 'www', '--'] if source is None else []
        return self.command(prefix + ['git', '-C', str(source or ROOT), *args])

    def verify_host_sources(self):
        need(self.command(['hostname']) == 'debian' and Path.cwd().resolve() == ROOT, 'RELEASE_FIXED_VM_ROOT_REQUIRED')
        # A venv executable commonly symlinks to /usr/bin/python. Check its
        # invoked path and actual prefix, not the resolved interpreter binary.
        need(Path(sys.executable).absolute().is_relative_to(ROOT / '.venv')
             and Path(sys.prefix).resolve() == (ROOT / '.venv').resolve(), 'RELEASE_PRODUCTION_VENV_REQUIRED')
        need(not os.environ.get('PYTHONPATH'), 'RELEASE_NO_SOURCE_IMPORT_OVERRIDE')
        need(self.own.is_relative_to(Path('/var/tmp')) and not self.own.is_relative_to(ROOT), 'RELEASE_EXCLUSIVE_OUTSIDE_PRODUCTION_REQUIRED')
        need(self.own.is_dir() and not self.own.is_symlink(), 'RELEASE_OUTPUT_ROOT_INVALID')
        for field, head in [('candidate_source', self.b['candidate_head']), ('rollback_source', self.b['rollback_head'])]:
            source = Path(self.b[field]).resolve()
            need(not source.is_relative_to(ROOT) and source.is_dir(), 'RELEASE_OWNED_SOURCE_REQUIRED')
            need(self.git('rev-parse', 'HEAD', source=source) == head, 'RELEASE_SOURCE_HEAD_CHANGED')
            clean = load_module(source / 'scripts/full_test_baseline.py', 'release0589_clean_' + field)
            need(clean.clean(source), 'RELEASE_SOURCE_NOT_CLEAN')
        back = Path(self.b['rollback_source'])
        need(self.git('rev-parse', 'HEAD^', source=back) == self.b['candidate_head'], 'RELEASE_ROLLBACK_MUST_BE_FORWARD_CHILD')
        need(self.git('rev-parse', 'HEAD^{tree}', source=back) == self.b['rollback_tree'], 'RELEASE_ROLLBACK_TREE_MISMATCH')
        need(self.git('rev-parse', self.b['baseline_head'] + '^{tree}', source=back) == self.b['baseline_tree'], 'RELEASE_BASELINE_TREE_MISMATCH')
        # The candidate must already contain both lifecycle protection layers.
        deploy = (Path(self.b['candidate_source']) / 'deploy.sh').read_text(encoding='utf-8')
        need('deployment_kill_mode' in deploy and '"process"' in deploy, 'RELEASE_DEPLOY_KILLMODE_GUARD_MISSING')

    def check_window(self):
        current = real_now()
        need(aware(self.b['not_before']) <= current < aware(self.b['deadline']), 'RELEASE_APPROVED_WINDOW_CLOSED')
        # Mirror, do not bypass, the existing deploy.sh protections.
        clock = current.hour * 100 + current.minute
        need(not (current.weekday() < 5 and (900 <= clock < 1630 or 1750 <= clock < 1810)), 'RELEASE_EXISTING_DEPLOY_WINDOW_CLOSED')

    def unit_state(self, unit):
        raw = self.command(['systemctl', 'show', unit, '-p', 'LoadState', '-p', 'ActiveState', '-p', 'SubState',
                            '-p', 'ExecMainStatus', '-p', 'Result', '-p', 'MainPID', '-p', 'KillMode'], allowed=(0, 1))
        state = dict(line.split('=', 1) for line in raw.splitlines() if '=' in line)
        if state.get('LoadState') == 'not-found':
            # GC is not exit0 evidence. Preserve a terminal observation already
            # made by this independent process, or classify unknown as failed.
            return {**self.finished_units.get(unit, dict(ActiveState='inactive', Result='unknown', ExecMainStatus='unknown')),
                    'LoadState': 'not-found', 'collected': True}
        need(state.get('ActiveState') in {'active', 'activating', 'deactivating', 'inactive', 'failed'}, 'RELEASE_SYSTEMD_STATE_UNKNOWN')
        if state['ActiveState'] in {'inactive', 'failed'}:
            self.finished_units[unit] = dict(state)
        return state

    def assert_idle(self):
        p = subprocess.run(['pgrep', '-u', 'www', '-f', r'python.*-m liangjian_funnel (run-|maintain-|monitor-once|backfill-)'],
                           capture_output=True, text=True, timeout=5)
        need(p.returncode in (0, 1), 'RELEASE_PROCESS_QUERY_FAILED')
        need(p.returncode == 1, 'RELEASE_ACTIVE_WORKFLOW_NO_KILL')
        units = self.command(['systemctl', 'list-units', '--type=service', '--state=active,activating,deactivating',
                              '--no-legend', '--no-pager', 'liangjian-*'])
        need(not units.strip(), 'RELEASE_ACTIVE_SYSTEMD_WORK_NO_KILL')
        with closing(self.ro(ROOT / 'state/team_v2/paper.db')) as db:
            count = db.execute("SELECT count(*) FROM paper_orders WHERE status IN ('OPEN','PARTIAL')").fetchone()[0]
        need(count == 0, 'RELEASE_OPEN_ORDERS_NO_DEPLOY')

    @staticmethod
    def ro(path):
        db = sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True, timeout=1)
        db.execute('PRAGMA query_only=ON')
        return db

    def business_hashes(self):
        hashes = {'env_file_sha256': sha(ROOT / '.env')}
        for relative, tables in PROTECTED.items():
            with closing(self.ro(ROOT / 'state' / relative)) as db:
                db.execute('BEGIN')
                for table in tables:
                    count, h = 0, hashlib.sha256()
                    for row in db.execute('SELECT * FROM ' + table + ' ORDER BY rowid'):
                        # Unambiguous row-wise digest, bounded RAM, no DB copies.
                        raw = json.dumps(row, ensure_ascii=False, separators=(',', ':')).encode()
                        h.update(str(len(raw)).encode() + b':' + raw)
                        count += 1
                    hashes[relative + ':' + table] = dict(rows=count, sha256=h.hexdigest())
        return hashes

    def node(self):
        raw = self.command(['pgrep', '-u', 'www', '-f', r'node dist/server/index\.js'])
        ids = [int(pid) for pid in raw.split() if (Path('/proc') / pid / 'cwd').resolve() == ROOT]
        need(len(ids) == 1, 'RELEASE_NODE_IDENTITY_AMBIGUOUS')
        return ids[0]

    def observe(self, target):
        need(self.git('rev-parse', 'HEAD') == target, 'RELEASE_POST_HEAD_CHANGED')
        pid = self.node()
        # Linux stat start ticks make same-numeric-PID reuse fail the check.
        ticks = (Path('/proc') / str(pid) / 'stat').read_text().rsplit(')', 1)[1].split()[19]
        with urlopen(HEALTH, timeout=3) as response:
            health = response.status
        return dict(node_pid=str(pid) + ':' + ticks, health_status=health, business_hashes=self.business_hashes())

    def installed(self, target, source):
        purelib = Path(sysconfig.get_paths()['purelib'])
        manifest = {}
        for file in (Path(source) / 'src').rglob('*.py'):
            relative = file.relative_to(Path(source) / 'src')
            need(sha(file) == sha(purelib / relative), 'RELEASE_INSTALLED_SOURCE_MISMATCH')
            manifest[str(relative)] = sha(file)
        need(len(manifest) >= 340, 'RELEASE_INSTALLED_MANIFEST_INCOMPLETE')
        pid = self.node()
        env = dict(item.split('=', 1) for item in (Path('/proc') / str(pid) / 'environ').read_bytes().decode().split('\0') if '=' in item)
        need(not env.get('PYTHONPATH'), 'RELEASE_NODE_SOURCE_IMPORT_OVERRIDE')
        # No env values/credentials are emitted. Use the actual new Node
        # inheritance and installed Settings rather than the invoking shell.
        code = "from pathlib import Path;import sysconfig,json,liangjian_funnel;from liangjian_funnel.settings import Settings;assert Path(liangjian_funnel.__file__).resolve().is_relative_to(Path(sysconfig.get_paths()['purelib']).resolve());s=Settings.from_env(root=Path(" + repr(str(ROOT)) + "));print(json.dumps({k:getattr(s,k) for k in " + repr(tuple(self.b['expected_runtime'])) + "}))"
        p = subprocess.run([str(ROOT / '.venv/bin/python'), '-B', '-c', code], cwd=ROOT,
                           env={**env, 'PATH': SAFE_PATH}, capture_output=True, text=True, timeout=15)
        need(p.returncode == 0, 'RELEASE_ACTUAL_NODE_SETTINGS_FAILED')
        actual = json.loads(p.stdout)
        need(actual == self.b['expected_runtime'], 'RELEASE_ACTUAL_RUNTIME_MISMATCH')
        return dict(head=target, modules=len(manifest), installed_manifest_sha256=digest(manifest), runtime_verified=True)

    def publish_ff(self, target, source):
        current = self.git('rev-parse', 'HEAD')
        need(current in {self.b['baseline_head'], self.b['candidate_head']}, 'RELEASE_UNEXPECTED_PRODUCTION_HEAD')
        self.git('fetch', str(source), target)
        need(self.git('rev-parse', 'FETCH_HEAD') == target, 'RELEASE_LOCAL_TRANSFER_TARGET_CHANGED')
        self.command(['/usr/sbin/runuser', '-u', 'www', '--', 'git', '-C', str(ROOT), 'merge-base', '--is-ancestor', current, target])
        remote = self.git('ls-remote', 'origin', 'refs/heads/main').split()[0]
        need(remote in {self.b['baseline_head'], self.b['candidate_head'], target}, 'RELEASE_REMOTE_CHANGED_NO_FORCE')
        if remote != target:
            self.git('push', 'origin', target + ':refs/heads/main')
        need(self.git('ls-remote', 'origin', 'refs/heads/main').split()[0] == target, 'RELEASE_REMOTE_NOT_TARGET')

    def start_unit(self, target, phase):
        unit = 'liangjian-release-' + self.b['run_id'] + '-' + phase
        need(phase in {'deploy', 'rollback'} and target == self.b['candidate_head' if phase == 'deploy' else 'rollback_head'],
             'RELEASE_TARGET_PHASE_MISMATCH')
        source = Path(self.b['candidate_source' if phase == 'deploy' else 'rollback_source'])
        original_path = source / 'deploy.sh'
        original = original_path.read_bytes()
        expected_blob = self.git('rev-parse', target + ':deploy.sh', source=source)
        frozen = self.own / (phase + '-' + target[:12] + '.frozen.sh')
        script_proof = freeze_target_deploy(original, frozen, target=target, phase=phase,
            source_path=original_path, expected_git_blob=expected_blob)
        exclusive(self.own / (phase + '.script.json'), script_proof)
        log = self.own / (phase + '.private.log')
        fd = os.open(log, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(fd)
        ref = self.b['candidate_full' if phase == 'deploy' else 'rollback_full']
        args = ['systemd-run', '--unit=' + unit, '--property=Type=exec', '--property=KillMode=process',
                '--property=WorkingDirectory=' + str(ROOT), '--property=StandardOutput=append:' + str(log),
                '--property=StandardError=append:' + str(log), '--setenv=PATH=' + SAFE_PATH,
                '--setenv=LIANGJIAN_TEST_EVIDENCE=' + str(ref['path']), '--setenv=LIANGJIAN_TEST_EVIDENCE_SHA256=' + ref['sha256'],
                '/bin/bash', str(frozen)]
        need(sha(frozen) == script_proof['frozen_sha256'], 'RELEASE_FROZEN_SCRIPT_CHANGED_BEFORE_DISPATCH')
        self.command(args)
        state = self.unit_state(unit)
        need(state.get('KillMode') == 'process' and state.get('MainPID') != str(os.getpid()), 'RELEASE_EXTERNAL_SUPERVISOR_OR_KILLMODE_INVALID')
        return unit

    def script_proof(self, unit):
        prefix = 'liangjian-release-' + self.b['run_id'] + '-'
        need(unit.startswith(prefix) and unit[len(prefix):] in {'deploy', 'rollback'}, 'RELEASE_FROZEN_PROOF_UNIT_MISMATCH')
        phase = unit[len(prefix):]
        path = self.own / (phase + '.script.json')
        proof = json.loads(path.read_bytes())
        need(proof['target_head'] == self.b['candidate_head' if phase == 'deploy' else 'rollback_head']
             and proof['all_other_bytes_unchanged'] is True and proof['project_root_declaration_count'] == 1
             and sha(proof['frozen_path']) == proof['frozen_sha256'], 'RELEASE_FROZEN_PROOF_CHANGED')
        return dict(receipt_path=str(path), receipt_sha256=sha(path), original_sha256=proof['original_sha256'],
                    frozen_sha256=proof['frozen_sha256'], target_head=proof['target_head'])


class Supervisor:
    def __init__(self, binding, backend, lifecycle, *, binding_sha, sleep=time.sleep, monotonic=time.monotonic):
        self.b, self.vm, self.lifecycle = binding, backend, lifecycle
        self.binding_sha, self.sleep, self.monotonic = binding_sha, sleep, monotonic

    def preflight(self, *, rollback=False):
        self.vm.verify_host_sources()
        self.vm.check_window()
        self.vm.assert_idle()
        current = self.vm.git('rev-parse', 'HEAD')
        allowed = {self.b['baseline_head'], self.b['candidate_head']} if rollback else {self.b['baseline_head']}
        need(current in allowed, 'RELEASE_EXPECTED_BASELINE_HEAD')
        need(not self.vm.git('diff', '--name-only', 'HEAD'), 'RELEASE_PRODUCTION_DIRTY_NO_RESET')
        actual = self.vm.business_hashes()
        if not rollback:
            need(actual == self.b['baseline_business_hashes'], 'RELEASE_BASELINE_BUSINESS_CHANGED')
        # A changed business hash is never repaired/rewritten by this OPS. It
        # does not prevent restoring approved code once idle, but the rollback
        # acceptance below still cannot claim hashes equal to the baseline.
        return dict(status='PREFLIGHT_PASS_NOT_DEPLOYED', head=current, business_hashes=actual, model_http=0)

    def accept_unit(self, unit, target, source):
        limit = self.monotonic() + self.b.get('unit_timeout_seconds', 1800)
        while True:
            state = self.vm.unit_state(unit)
            if state.get('ActiveState') in {'inactive', 'failed'}:
                break
            need(self.monotonic() < limit, 'RELEASE_UNIT_STILL_RUNNING_NO_ROLLBACK_NO_KILL')
            self.sleep(1)
        need(state.get('ActiveState') == 'inactive' and state.get('ExecMainStatus') == '0'
             and state.get('Result') == 'success', 'RELEASE_DEPLOY_UNIT_FAILED')
        def active():
            return self.vm.unit_state(unit).get('ActiveState') != 'inactive'
        survived = self.lifecycle.verify_after_unit_exit(unit_active=active, observe=lambda: self.vm.observe(target),
            before=self.b['baseline_business_hashes'], sleep=self.sleep, monotonic=self.monotonic, wait_seconds=60, timeout=180)
        installed = self.vm.installed(target, source)
        need(self.vm.business_hashes() == self.b['baseline_business_hashes'], 'RELEASE_POST_INSTALL_BUSINESS_CHANGED')
        final = self.vm.observe(target)
        need(not active() and final.get('node_pid') == survived['node_pid'] and final.get('health_status') == 200
             and final.get('business_hashes') == self.b['baseline_business_hashes'], 'RELEASE_FINAL_SAME_PID_ACCEPTANCE_FAILED')
        return dict(status='PASS', target_head=target, unit=unit, survival=survived, installed=installed,
                    deploy_script=self.vm.script_proof(unit))

    def phase(self, name):
        self.preflight(rollback=name == 'rollback')
        target = self.b['candidate_head' if name == 'deploy' else 'rollback_head']
        source = self.b['candidate_source' if name == 'deploy' else 'rollback_source']
        exclusive(self.vm.own / (name + '.attempt.json'), dict(at=real_now().isoformat(), phase=name,
            target_head=target, binding_sha256=self.binding_sha, before=self.b['baseline_business_hashes']))
        self.vm.assert_idle()
        self.vm.check_window()
        self.vm.publish_ff(target, source)
        self.vm.assert_idle()
        unit = 'liangjian-release-' + self.b['run_id'] + '-' + name
        try:
            unit = self.vm.start_unit(target, name)
            result = self.accept_unit(unit, target, source)
        except BaseException as exc:
            exclusive(self.vm.own / (name + '.failure.json'), dict(at=real_now().isoformat(), unit=unit,
                exception_type=type(exc).__name__, error_sha256=hashlib.sha256(str(exc).encode()).hexdigest(), binding_sha256=self.binding_sha))
            if name == 'deploy':
                # Never update code underneath an unknown/in-flight release.
                state = self.vm.unit_state(unit)
                need(state.get('ActiveState') in {'inactive', 'failed'}, 'RELEASE_UNIT_STILL_RUNNING_NO_ROLLBACK_NO_KILL')
                restored = self.phase('rollback')
                result = dict(status='CANDIDATE_FAILED_FORWARD_ROLLBACK_PASS', candidate_head=target, rollback=restored)
            else:
                raise
        exclusive(self.vm.own / (name + '.result.json'), result)
        return result


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binding', required=True, type=Path)
    p.add_argument('--approval', required=True, type=Path)
    p.add_argument('--approval-sha256', required=True)
    p.add_argument('--phase', choices=('preflight', 'deploy', 'rollback'), default='preflight')
    args = p.parse_args(argv)
    need(sha(args.approval) == args.approval_sha256, 'RELEASE_FINAL_APPROVAL_SHA_CHANGED')
    b = json.loads(args.binding.read_bytes())
    approval = validate_final(b, json.loads(args.approval.read_bytes()), binding_sha=sha(args.binding))
    vm = VM(b)
    vm.verify_host_sources()
    proofs = verify_receipts(b)
    lifecycle = load_module(Path(b['candidate_source']) / 'src/liangjian_funnel/runtime/release_lifecycle.py', 'release0589_lifecycle')
    s = Supervisor(b, vm, lifecycle, binding_sha=sha(args.binding))
    result = s.preflight() if args.phase == 'preflight' else s.phase(args.phase)
    print(json.dumps(dict(result=result, full_proofs=proofs, approval_sha256=args.approval_sha256,
                         actual_model_http=0, env_or_latch_changes_by_supervisor=0), ensure_ascii=False))
    return 0 if result['status'] in {'PASS', 'PREFLIGHT_PASS_NOT_DEPLOYED'} else 2


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (ReleaseError, ValueError, OSError, subprocess.SubprocessError) as exc:
        print(json.dumps(dict(status='BLOCKED', exception_type=type(exc).__name__, error_sha256=hashlib.sha256(str(exc).encode()).hexdigest())))
        raise SystemExit(2)
