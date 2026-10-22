"""External supervisor simulation only: no VM/systemd/network/model calls."""
from pathlib import Path
from types import SimpleNamespace
import ast
import copy
import hashlib
import importlib.util
import json

import pytest

SOURCE = Path(__file__).resolve().parents[1]


def module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


m = module(SOURCE / 'scripts/supervise_release_0589.py', 'supervisor0589_tested')
lifecycle = module(SOURCE / 'src/liangjian_funnel/runtime/release_lifecycle.py', 'lifecycle0589_tested')


def write(path, body):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(body), encoding='utf-8')
    return dict(path=str(path), sha256=m.sha(path))


def binding(tmp_path):
    return dict(candidate_head='c' * 40, baseline_head='b' * 40, rollback_head='d' * 40,
        baseline_tree='a' * 40, rollback_tree='a' * 40, output=str(tmp_path),
        candidate_source=str(SOURCE), rollback_source=str(tmp_path / 'rollback-source'),
        baseline_business_hashes={'plans': {'rows': 3, 'sha256': 'e' * 64}, 'env_file_sha256': 'f' * 64},
        run_id='postclose-0589-declared', not_before='2026-10-22T18:10:00+08:00', deadline='2026-10-23T06:30:00+08:00',
        preserve_env_and_record_latch=True, source_trade_date='2026-10-22', execution_trade_date='2026-10-23',
        quality_run_id='DECLARED_OFFLINE_QUALITY', role_run_ids={r:'DECLARED_' + r + '_REAL_ID' for r in ('A2','A3','A6','PM')},
        provider_base_url='https://declared.invalid/v1', models={r:'DECLARED_PRO' for r in ('A2','A3','A6','PM')}, expected_runtime=dict(
            research_models=['DECLARED_PRO'], review_model='DECLARED_PRO', monitor_model='DECLARED_FLASH',
            team_intraday_model='DECLARED_FLASH', a4_trader_mode='control', broker_adapter='local_paper',
            model_base_url='https://declared.invalid/v1'),
        model_gate=dict(path='declared', sha256='1' * 64), required_gates={key: {} for key in m.REQUIRED_GATES})


def final(tmp_path, b):
    reply = tmp_path / 'final.md'
    reply.write_text('FINAL ACCEPT\nDeploy this exact candidate.\nOn failure forward rollback this exact target.', encoding='utf-8')
    authority = tmp_path / 'authority.md'
    authority.write_text('DECLARED OFFLINE AUTHORITY', encoding='utf-8')
    return dict(schema='release-final/0589', decision='ACCEPT', deploy_accepted=True,
        automatic_forward_rollback_accepted=True, candidate_head=b['candidate_head'], baseline_head=b['baseline_head'],
        rollback_head=b['rollback_head'], binding_sha256='2' * 64, model_gate_sha256=b['model_gate']['sha256'],
        reply=dict(path=str(reply), sha256=m.sha(reply)), deploy_quote='Deploy this exact candidate.',
        rollback_quote='On failure forward rollback this exact target.',
        authorities={key: dict(path=str(authority), sha256=m.sha(authority)) for key in ('0589', '0591')})


class Clock:
    def __init__(self):
        self.value = 0

    def sleep(self, seconds):
        self.value += seconds

    def monotonic(self):
        return self.value


class FakeVM:
    def __init__(self, b, clock, *, killed=False, hash_changed=False, never_exits=False):
        self.b, self.own, self.clock = b, Path(b['output']), clock
        self.head = b['baseline_head']
        self.units, self.events = {}, []
        self.inflight = False
        self.killed, self.hash_changed, self.never_exits = killed, hash_changed, never_exits

    def verify_host_sources(self):
        self.events.append('host-sources')

    def check_window(self):
        self.events.append('window')

    def assert_idle(self):
        self.events.append('idle')
        m.need(not self.inflight, 'RELEASE_ACTIVE_WORKFLOW_NO_KILL')

    def git(self, *args):
        if args == ('rev-parse', 'HEAD'):
            return self.head
        assert args == ('diff', '--name-only', 'HEAD')
        return ''

    def business_hashes(self):
        original = copy.deepcopy(self.b['baseline_business_hashes'])
        if self.hash_changed and self.head == self.b['candidate_head']:
            original['plans']['sha256'] = 'BAD'
        return original

    def publish_ff(self, target, source):
        self.events.append(('publish_ff', target))

    def start_unit(self, target, phase):
        self.head = target
        unit = 'declared-unit-' + phase
        self.units[unit] = dict(target=target, done_at=self.clock.value + 2, phase=phase)
        self.events.append(('start', phase))
        return unit

    def unit_state(self, unit):
        row = self.units[unit]
        return dict(ActiveState='active' if self.never_exits or self.clock.value < row['done_at'] else 'inactive',
                    ExecMainStatus='0', Result='success')

    def observe(self, target):
        assert self.head == target
        unit = self.units['declared-unit-' + ('deploy' if target == self.b['candidate_head'] else 'rollback')]
        assert self.clock.value >= unit['done_at'], 'acceptance must happen outside finished unit'
        candidate = target == self.b['candidate_head']
        killed = candidate and self.killed and self.clock.value >= unit['done_at'] + 20
        self.events.append(('observe', target, self.clock.value))
        return dict(node_pid=None if killed else ('22:111' if candidate else '33:222'), health_status=503 if killed else 200,
                    business_hashes=self.business_hashes())

    def installed(self, target, source):
        return dict(head=target, modules=400, runtime_verified=True)

    def script_proof(self, unit):
        return dict(scope='DECLARED_OFFLINE_FROZEN_SCRIPT', unit=unit)


def supervisor(tmp_path, **kwargs):
    b, clock = binding(tmp_path), Clock()
    vm = FakeVM(b, clock, **kwargs)
    return m.Supervisor(b, vm, lifecycle, binding_sha='2' * 64, sleep=clock.sleep, monotonic=clock.monotonic), vm, clock


def test_default_preflight_does_not_publish_or_start(tmp_path):
    s, vm, _ = supervisor(tmp_path)
    result = s.preflight()
    assert result['status'] == 'PREFLIGHT_PASS_NOT_DEPLOYED'
    assert not any(isinstance(e, tuple) for e in vm.events)
    assert not list(tmp_path.glob('*.attempt.json'))


def test_survives60_after_inactive_not_only_unit_exit0(tmp_path):
    s, vm, clock = supervisor(tmp_path)
    result = s.phase('deploy')
    assert result['status'] == 'PASS'
    assert result['survival']['post_unit_elapsed_seconds'] >= 60
    assert clock.value >= 62
    assert len([e for e in vm.events if isinstance(e, tuple) and e[0] == 'observe']) >= 13


def test_node_killed_after_unit_exit_triggers_forward_rollback(tmp_path):
    s, vm, _ = supervisor(tmp_path, killed=True)
    result = s.phase('deploy')
    assert result['status'] == 'CANDIDATE_FAILED_FORWARD_ROLLBACK_PASS'
    assert vm.head == s.b['rollback_head']
    assert result['rollback']['survival']['post_unit_elapsed_seconds'] >= 60
    assert (tmp_path / 'deploy.failure.json').is_file()
    assert ('publish_ff', s.b['rollback_head']) in vm.events


def test_candidate_business_hash_change_cannot_pass(tmp_path):
    s, vm, _ = supervisor(tmp_path, hash_changed=True)
    result = s.phase('deploy')
    assert result['status'] == 'CANDIDATE_FAILED_FORWARD_ROLLBACK_PASS'
    assert vm.head == s.b['rollback_head']


def test_persistent_business_change_restores_code_but_never_fakes_repair(tmp_path):
    s, vm, _ = supervisor(tmp_path)
    original = vm.business_hashes
    def persistent():
        value = original()
        if vm.head != s.b['baseline_head']:
            value['plans']['sha256'] = 'PERSISTENT_CHANGED'
        return value
    vm.business_hashes = persistent
    with pytest.raises(ValueError, match='POST_UNIT_ACCEPTANCE_FAILED'):
        s.phase('deploy')
    assert vm.head == s.b['rollback_head']
    assert (tmp_path / 'rollback.failure.json').exists()
    assert not (tmp_path / 'rollback.result.json').exists()


def test_inflight_refuses_no_kill_no_publish(tmp_path):
    s, vm, _ = supervisor(tmp_path)
    vm.inflight = True
    with pytest.raises(m.ReleaseError, match='ACTIVE_WORKFLOW_NO_KILL'):
        s.phase('deploy')
    assert not any(isinstance(e, tuple) for e in vm.events)


def test_inflight_after_failure_does_not_bypass_rollback_guard(tmp_path):
    s, vm, _ = supervisor(tmp_path, killed=True)
    original = vm.unit_state
    def state(unit):
        result = original(unit)
        if unit.endswith('deploy') and vm.clock.value > 5:
            vm.inflight = True
        return result
    vm.unit_state = state
    with pytest.raises(m.ReleaseError, match='ACTIVE_WORKFLOW_NO_KILL'):
        s.phase('deploy')
    assert ('publish_ff', s.b['rollback_head']) not in vm.events


def test_unknown_running_unit_never_killed_or_rolled_over(tmp_path):
    s, vm, _ = supervisor(tmp_path, never_exits=True)
    s.b['unit_timeout_seconds'] = 3
    with pytest.raises(m.ReleaseError, match='STILL_RUNNING_NO_ROLLBACK_NO_KILL'):
        s.phase('deploy')
    assert ('publish_ff', s.b['rollback_head']) not in vm.events


def test_attempt_is_exclusive_not_retried(tmp_path):
    s, vm, _ = supervisor(tmp_path)
    s.phase('deploy')
    vm.head = s.b['baseline_head']
    with pytest.raises(FileExistsError):
        s.phase('deploy')


def test_rollback_failure_no_recursive_deploy(tmp_path):
    s, vm, _ = supervisor(tmp_path, killed=True)
    original = vm.installed
    def installed(target, source):
        if target == s.b['rollback_head']:
            raise m.ReleaseError('DECLARED_ROLLBACK_INSTALLED_FAILED')
        return original(target, source)
    vm.installed = installed
    with pytest.raises(m.ReleaseError, match='DECLARED_ROLLBACK'):
        s.phase('deploy')
    assert len([e for e in vm.events if isinstance(e, tuple) and e[0] == 'start']) == 2


def test_valid_final_requires_real_reply_and_exact_binding(tmp_path):
    b = binding(tmp_path)
    a = final(tmp_path, b)
    assert m.validate_final(b, a, binding_sha='2' * 64) is a


@pytest.mark.parametrize('mutation', ['noaccept', 'head', 'binding', 'rollback', 'quote', 'model', 'window', 'env'])
def test_final_binding_rejections(tmp_path, mutation):
    b = binding(tmp_path)
    a = final(tmp_path, b)
    if mutation == 'noaccept': a['decision'] = 'PENDING'
    if mutation == 'head': a['candidate_head'] = 'a' * 40
    if mutation == 'binding': a['binding_sha256'] = '3' * 64
    if mutation == 'rollback': a['automatic_forward_rollback_accepted'] = False
    if mutation == 'quote': a['rollback_quote'] = 'INVENTED APPROVAL'
    if mutation == 'model': a['model_gate_sha256'] = '4' * 64
    if mutation == 'window': b['deadline'] = '2026-10-23T19:00:00+08:00'
    if mutation == 'env': b['preserve_env_and_record_latch'] = False
    with pytest.raises(m.ReleaseError):
        m.validate_final(b, a, binding_sha='2' * 64)


def test_final_reply_bytes_tamper_not_accepted(tmp_path):
    b = binding(tmp_path)
    a = final(tmp_path, b)
    Path(a['reply']['path']).write_text('ACCEPT something else', encoding='utf-8')
    with pytest.raises(m.ReleaseError, match='REPLY_BYTES_CHANGED'):
        m.validate_final(b, a, binding_sha='2' * 64)


def test_lifecycle_pid_change_after_unit_inactive_rejected():
    clock = Clock()
    def observe():
        return dict(node_pid='1:1' if clock.value < 20 else '2:2', health_status=200, business_hashes={'a': 1})
    with pytest.raises(ValueError, match='POST_UNIT_ACCEPTANCE_FAILED'):
        lifecycle.verify_after_unit_exit(unit_active=lambda: False, observe=observe, before={'a': 1},
            sleep=clock.sleep, monotonic=clock.monotonic)


def test_systemd_command_killmode_process_existing_deploy_no_intraday(tmp_path, monkeypatch):
    b = binding(tmp_path)
    b['candidate_full'] = dict(path='/owned/full/evidence.json', sha256='5' * 64)
    vm = m.VM(b)
    commands = []
    monkeypatch.setattr(vm, 'command', lambda args, **_: commands.append(args) or '')
    original = (SOURCE / 'deploy.sh').read_bytes()
    blob = hashlib.sha1(b'blob ' + str(len(original)).encode() + b'\0' + original).hexdigest()
    monkeypatch.setattr(vm, 'git', lambda *_, **__: blob)
    monkeypatch.setattr(vm, 'unit_state', lambda _: dict(KillMode='process', MainPID='123456'))
    vm.start_unit(b['candidate_head'], 'deploy')
    command = commands[0]
    assert '--property=KillMode=process' in command
    frozen = tmp_path / ('deploy-' + b['candidate_head'][:12] + '.frozen.sh')
    assert command[-2:] == ['/bin/bash', str(frozen)]
    assert str(m.ROOT / 'deploy.sh') not in command
    assert '--allow-intraday' not in command
    assert '--wait' not in command  # parent supervisor waits independently
    assert '--setenv=PATH=' + m.SAFE_PATH in command


def test_freeze_changes_only_single_root_declaration(tmp_path):
    original = (SOURCE / 'deploy.sh').read_bytes()
    blob = hashlib.sha1(b'blob ' + str(len(original)).encode() + b'\0' + original).hexdigest()
    target = tmp_path / 'frozen.sh'
    proof = m.freeze_target_deploy(original, target, target='c' * 40, phase='deploy', source_path=SOURCE / 'deploy.sh', expected_git_blob=blob)
    fixed = b"PROJECT_ROOT='" + str(m.ROOT).encode() + b"'"
    assert target.read_bytes().replace(fixed, m.PROJECT_ROOT_DECLARATION, 1) == original
    assert proof['original_sha256'] == hashlib.sha256(original).hexdigest()
    assert proof['frozen_sha256'] == m.sha(target)
    assert proof['all_other_bytes_unchanged'] and proof['project_root_declaration_count'] == 1
    with pytest.raises(FileExistsError):
        m.freeze_target_deploy(original, target, target='c' * 40, phase='deploy', source_path='declared', expected_git_blob=blob)


@pytest.mark.parametrize('original', [b'#!/bin/bash\nPROJECT_ROOT=/wrong\n', m.PROJECT_ROOT_DECLARATION * 2])
def test_missing_or_duplicate_root_declaration_cannot_dispatch(tmp_path, original):
    with pytest.raises(m.ReleaseError, match='DECLARATION_NOT_UNIQUE'):
        m.freeze_target_deploy(original, tmp_path / 'never.sh', target='c' * 40, phase='deploy', source_path='declared', expected_git_blob='0' * 40)
    assert not (tmp_path / 'never.sh').exists()


def test_changed_target_git_bytes_cannot_dispatch(tmp_path):
    original = (SOURCE / 'deploy.sh').read_bytes()
    with pytest.raises(m.ReleaseError, match='NOT_APPROVED_GIT_BYTES'):
        m.freeze_target_deploy(original, tmp_path / 'never.sh', target='c' * 40, phase='deploy', source_path='declared', expected_git_blob='0' * 40)
    assert not (tmp_path / 'never.sh').exists()


def test_rollback_uses_old_target_script_not_current_root_after_pull(tmp_path, monkeypatch):
    import subprocess
    # Actual ae baseline and current candidate differ at the integrity CLI.
    old = subprocess.check_output(['git', '-C', str(SOURCE), 'show', 'ae9529191656bf6db1b3d6a1c0379b7b67ac03b2:deploy.sh'])
    candidate = (SOURCE / 'deploy.sh').read_bytes()
    assert b'verify-runtime-integrity' not in old
    assert b'verify-runtime-integrity' in candidate
    b = binding(tmp_path)
    rollback_source = tmp_path / 'old-target'
    rollback_source.mkdir()
    (rollback_source / 'deploy.sh').write_bytes(old)
    b['rollback_source'] = str(rollback_source)
    b['rollback_full'] = dict(path='/owned/old-full/evidence.json', sha256='6' * 64)
    live_root = tmp_path / 'mutable-production'
    live_root.mkdir()
    (live_root / 'deploy.sh').write_bytes(candidate)
    monkeypatch.setattr(m, 'ROOT', live_root)
    vm = m.VM(b)
    blob = hashlib.sha1(b'blob ' + str(len(old)).encode() + b'\0' + old).hexdigest()
    monkeypatch.setattr(vm, 'git', lambda *_, **__: blob)
    commands = []
    monkeypatch.setattr(vm, 'command', lambda args, **_: commands.append(args) or '')
    monkeypatch.setattr(vm, 'unit_state', lambda _: dict(KillMode='process', MainPID='123456'))
    vm.start_unit(b['rollback_head'], 'rollback')
    executed = Path(commands[0][-1])
    before = executed.read_bytes()
    assert b'verify-runtime-integrity' not in before
    # Simulate both production git pull and accidental later checkout changes.
    (live_root / 'deploy.sh').write_bytes(candidate + b'\nexit 99\n')
    (rollback_source / 'deploy.sh').write_bytes(candidate)
    assert executed.read_bytes() == before
    proof = json.loads((tmp_path / 'rollback.script.json').read_bytes())
    assert proof['target_head'] == b['rollback_head']
    assert proof['original_sha256'] == hashlib.sha256(old).hexdigest()
    assert proof['frozen_sha256'] == m.sha(executed)


def test_exact_json_tamper_refused(tmp_path):
    ref = write(tmp_path / 'receipt.json', {'status': 'PASSED'})
    Path(ref['path']).write_text('{"status":"FAILED"}', encoding='utf-8')
    with pytest.raises(m.ReleaseError, match='RECEIPT_BYTES_CHANGED'):
        m.exact_json(ref)


def receipt_fixture(tmp_path):
    b = binding(tmp_path)
    for key, head in [('candidate_full', b['candidate_head']), ('rollback_full', b['rollback_head'])]:
        folder = tmp_path / key
        folder.mkdir()
        steps = {}
        for name in ('pytest', 'npm_test', 'typecheck'):
            log = folder / (name + '.log')
            log.write_text('DECLARED_OFFLINE_ZERO_HTTP', encoding='utf-8')
            steps[name] = dict(exit_code=0, log=log.name, log_sha256=m.sha(log))
        b[key] = write(folder / 'evidence.json', dict(schema_version='full-test-baseline/1', head=head,
            head_end=head, tree='a' * 40, tree_end='a' * 40, clean_start=True, clean_end=True,
            status='PASSED', release_qualified=True, totals={'failed': 0, 'tests': 10}, failures=[], steps=steps))
    b['same_head40'] = write(tmp_path / 'forty.json', dict(head=b['candidate_head'], status='PASSED', cases=list(range(40)), failures=[]))
    original = tmp_path / 'original.json'
    original.write_text('{"fixture":"DECLARED_ONLY"}', encoding='utf-8')
    for name in m.REQUIRED_GATES:
        b['required_gates'][name] = write(tmp_path / (name + '.json'), dict(head=b['candidate_head'], status='PASSED',
            originals=[dict(path=str(original), sha256=m.sha(original))]))
    from test_model_change_gate_0589 import quality_fixture
    quality = quality_fixture(tmp_path / 'quality-originals', head=b['candidate_head'], source_trade_date=b['source_trade_date'],
        execution_trade_date=b['execution_trade_date'], quality_run_id=b['quality_run_id'], role_run_ids=b['role_run_ids'],
        provider_base_url=b['provider_base_url'], models=b['models'])
    b['model_gate'] = quality['ref']
    return b


def test_actual_full40_quality_verifiers_read_all_pinned_originals(tmp_path):
    b = receipt_fixture(tmp_path)
    result = m.verify_receipts(b)
    assert result['candidate_full']['head'] == b['candidate_head']
    assert result['rollback_full']['head'] == b['rollback_head']


def test_candidate_full_log_tamper_blocks_release(tmp_path):
    b = receipt_fixture(tmp_path)
    (tmp_path / 'candidate_full/pytest.log').write_text('CHANGED', encoding='utf-8')
    with pytest.raises(ValueError, match='TEST_EVIDENCE_LOG_CHANGED'):
        m.verify_receipts(b)


@pytest.mark.parametrize('invalid', ['missing_pm', 'raw_invalid', 'over_budget'])
def test_actual_model_quality_negative_cases(tmp_path, invalid):
    b = receipt_fixture(tmp_path)
    path = Path(b['model_gate']['path'])
    proof = json.loads(path.read_bytes())
    if invalid == 'missing_pm': proof['calls'] = proof['calls'][:-1]
    if invalid == 'raw_invalid':
        ref = proof['calls'][0]
        original_path = Path(ref['receipt_path'])
        original = json.loads(original_path.read_bytes())
        original['journal_row']['validated'] = 0
        updated = write(original_path, original)
        ref['receipt_sha256'] = updated['sha256']
    if invalid == 'over_budget': proof['actual_http'] = 25
    b['model_gate'] = write(path, proof)
    with pytest.raises(ValueError, match='MODEL_GATE_'):
        m.verify_receipts(b)


def test_collected_unit_not_exit0_without_observed_terminal(tmp_path, monkeypatch):
    vm = m.VM(binding(tmp_path))
    monkeypatch.setattr(vm, 'command', lambda *_, **__: 'LoadState=not-found\nActiveState=inactive')
    state = vm.unit_state('declared')
    assert state['ExecMainStatus'] == 'unknown' and state['Result'] == 'unknown'


def test_collected_unit_retains_only_actual_previously_observed_terminal(tmp_path, monkeypatch):
    vm = m.VM(binding(tmp_path))
    values = iter(['LoadState=loaded\nActiveState=inactive\nExecMainStatus=0\nResult=success', 'LoadState=not-found\nActiveState=inactive'])
    monkeypatch.setattr(vm, 'command', lambda *_, **__: next(values))
    first, second = vm.unit_state('declared'), vm.unit_state('declared')
    assert first['ExecMainStatus'] == second['ExecMainStatus'] == '0'
    assert second['collected'] is True


def test_no_network_model_or_force_reset_kill_business_entry():
    text = (SOURCE / 'scripts/supervise_release_0589.py').read_text(encoding='utf-8')
    tree = ast.parse(text)
    attrs = {n.func.attr for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
    assert not attrs & {'complete', 'run_research', 'kill', 'terminate', 'send', 'send_message'}
    constants = {n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)}
    assert '--force' not in constants and '--hard' not in constants and 'reset' not in constants


def test_protected_sql_is_ro(tmp_path):
    path = tmp_path / 'small.db'
    import sqlite3
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE x(n INTEGER)')
    with m.closing(m.VM.ro(path)) as db:
        with pytest.raises(sqlite3.OperationalError):
            db.execute('INSERT INTO x VALUES(1)')
