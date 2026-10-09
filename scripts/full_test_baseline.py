"""One full-suite receipt, with immutable logs and HEAD-bound release proof."""
from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import xml.etree.ElementTree as ET


def file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_evidence(path: Path, expected_sha256: str, head: str):
    if not re.fullmatch(r'[0-9a-f]{64}', expected_sha256) or file_hash(path) != expected_sha256:
        raise ValueError('TEST_EVIDENCE_HASH_MISMATCH')
    proof = json.loads(path.read_text(encoding='utf-8'))
    if proof.get('schema_version') != 'full-test-baseline/1' or proof.get('head') != head:
        raise ValueError('TEST_EVIDENCE_HEAD_MISMATCH')
    if proof.get('status') != 'PASSED' or not proof.get('clean_start') or not proof.get('clean_end'):
        raise ValueError('TEST_EVIDENCE_NOT_RELEASE_QUALIFIED')
    if proof.get('head_end') != head or proof.get('tree_end') != proof.get('tree'):
        raise ValueError('TEST_EVIDENCE_SOURCE_CHANGED_DURING_TESTS')
    steps = proof.get('steps', {})
    if set(steps) != {'pytest', 'npm_test', 'typecheck'}:
        raise ValueError('TEST_EVIDENCE_INCOMPLETE')
    for name, step in steps.items():
        log = (path.parent / step['log']).resolve()
        if log.parent != path.parent.resolve() or step.get('exit_code') != 0:
            raise ValueError(f'TEST_EVIDENCE_FAILED_STEP:{name}')
        if not log.is_file() or file_hash(log) != step.get('log_sha256'):
            raise ValueError(f'TEST_EVIDENCE_LOG_CHANGED:{name}')
    if proof.get('totals', {}).get('failed') != 0 or proof.get('totals', {}).get('tests', 0) <= 0:
        raise ValueError('TEST_EVIDENCE_FAILURES_PRESENT')
    return proof


def git(root: Path, *args):
    return subprocess.run(['git', '-C', str(root), *args], capture_output=True,
                          text=True, encoding='utf-8')


def clean(root: Path) -> bool:
    # Runtime evidence is untracked. Every tracked source change, including
    # staged changes, invalidates a HEAD-bound release receipt.
    if git(root, 'diff', '--quiet', 'HEAD', '--').returncode != 0:
        return False
    files = git(root, 'ls-files', '--others', '--exclude-standard').stdout.splitlines()
    source_dirs = {'src', 'tests', 'server', 'test', 'web', 'frontend',
                   'scripts', 'config', 'prompts'}
    source_roots = {'pyproject.toml', 'package.json', 'package-lock.json',
                    'deploy.sh', 'tsconfig.json', 'vitest.config.ts'}
    return not any(Path(name).parts[0] in source_dirs or name in source_roots for name in files)


def pytest_counts(path):
    result = {'tests': 0, 'passed': 0, 'failed': 0, 'skipped': 0, 'xfail': 0}
    failures = []
    if not path.is_file():
        return result, ['PYTEST_REPORT_MISSING']
    for case in ET.parse(path).iter('testcase'):
        result['tests'] += 1
        key = f"{case.get('classname', '')}::{case.get('name', '')}"
        error, failure, skipped = case.find('error'), case.find('failure'), case.find('skipped')
        if error is not None or failure is not None:
            result['failed'] += 1
            failures.append(key)
        elif skipped is not None:
            category = 'xfail' if skipped.get('type') == 'pytest.xfail' else 'skipped'
            result[category] += 1
        else:
            result['passed'] += 1
    return result, failures


def vitest_counts(path):
    result = {'tests': 0, 'passed': 0, 'failed': 0, 'skipped': 0, 'xfail': 0}
    if not path.is_file():
        return result, ['VITEST_REPORT_MISSING']
    data = json.loads(path.read_text(encoding='utf-8'))
    result.update(tests=data.get('numTotalTests', 0), passed=data.get('numPassedTests', 0),
                  failed=data.get('numFailedTests', 0), skipped=data.get('numPendingTests', 0))
    failures = [f"{suite.get('name')}::{case.get('fullName')}"
                for suite in data.get('testResults', []) for case in suite.get('assertionResults', [])
                if case.get('status') == 'failed']
    return result, failures


def run_all(root: Path, output: Path, python: str):
    if output.exists():
        raise ValueError('REFUSE_OVERWRITE_TEST_EVIDENCE')
    output.mkdir(parents=True)
    head = git(root, 'rev-parse', 'HEAD').stdout.strip()
    tree = git(root, 'rev-parse', 'HEAD^{tree}').stdout.strip()
    if not re.fullmatch(r'[0-9a-f]{40,64}', head):
        raise ValueError('TEST_HEAD_UNAVAILABLE')
    env = dict(os.environ)
    env['PYTHONPATH'] = str(root/'src')
    env.pop('PYTEST_ADDOPTS', None)
    env['CI'] = 'true'
    npm = shutil.which('npm.cmd' if os.name == 'nt' else 'npm')
    commands = {
        'pytest': [python, '-m', 'pytest', 'tests', '-o', 'addopts=', '-q',
                   '--junitxml='+str(output/'pytest.xml')],
        'npm_test': [npm or 'npm', 'test', '--', '--reporter=default', '--reporter=json',
                     '--outputFile='+str(output/'vitest.json')],
        'typecheck': [npm or 'npm', 'run', 'typecheck'],
    }
    proof = {'schema_version': 'full-test-baseline/1', 'head': head, 'tree': tree,
             'root': str(root), 'started_at': datetime.now().astimezone().isoformat(),
             'python': python, 'clean_start': clean(root), 'steps': {}}
    for name, command in commands.items():
        stamp = time.monotonic()
        log = output/(name+'.log')
        print(f'[test_all] {name}: running', flush=True)
        with log.open('xb') as f:
            try:
                result = subprocess.run(command, cwd=root, env=env, stdout=f, stderr=subprocess.STDOUT)
                code = result.returncode
            except OSError as exc:
                f.write(f'COMMAND_UNAVAILABLE:{exc.__class__.__name__}'.encode())
                code = 127
        proof['steps'][name] = {'command': command, 'exit_code': code, 'log': log.name,
            'log_sha256': file_hash(log), 'seconds': round(time.monotonic()-stamp, 3)}
        print(f'[test_all] {name}: exit {code}', flush=True)
    py, py_fail = pytest_counts(output/'pytest.xml')
    node, node_fail = vitest_counts(output/'vitest.json')
    proof.update(python_counts=py, node_counts=node,
        totals={key: py[key]+node[key] for key in py}, failures=py_fail+node_fail,
        clean_end=clean(root), head_end=git(root, 'rev-parse', 'HEAD').stdout.strip(),
        tree_end=git(root, 'rev-parse', 'HEAD^{tree}').stdout.strip(),
        finished_at=datetime.now().astimezone().isoformat())
    success = all(step['exit_code'] == 0 for step in proof['steps'].values())
    success = success and not proof['failures'] and proof['totals']['tests'] > 0
    proof['status'] = 'PASSED' if success else 'FAILED'
    proof['release_qualified'] = (success and proof['clean_start'] and proof['clean_end']
        and proof['head_end'] == head and proof['tree_end'] == tree)
    receipt = output/'evidence.json'
    with receipt.open('x', encoding='utf-8') as f:
        json.dump(proof, f, ensure_ascii=False, indent=2)
    checksum = file_hash(receipt)
    (output/'evidence.sha256').write_text(checksum+'\n', encoding='ascii')
    print(json.dumps({'evidence': str(receipt), 'sha256': checksum, 'head': head,
        'status': proof['status'], 'release_qualified': proof['release_qualified'],
        'totals': proof['totals'], 'failures': proof['failures']}, ensure_ascii=False), flush=True)
    return 0 if success else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--python', default=sys.executable)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--verify', type=Path)
    parser.add_argument('--sha256')
    parser.add_argument('--head')
    args = parser.parse_args()
    if args.verify:
        if not args.sha256 or not args.head:
            raise ValueError('TEST_VERIFICATION_ARGUMENTS_REQUIRED')
        verify_evidence(args.verify.resolve(), args.sha256, args.head)
        print('FULL_TEST_EVIDENCE_VERIFIED')
        return 0
    root = Path(__file__).resolve().parents[1]
    output = (args.output or root/'artifacts/wp0-20261009'/datetime.now().strftime('full-%Y%m%dT%H%M%S')).resolve()
    return run_all(root, output, args.python)


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (ValueError, OSError) as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(2)
