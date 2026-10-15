"""Explicit isolated B3 VM counterexamples, never a production application."""
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import argparse
import hashlib
import json
import subprocess
import tarfile
import shlex


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--test-file', action='append', default=[])
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    tests = args.test_file or [
        'tests/test_b3_publication_archive.py', 'tests/test_b3_publication_reader.py',
        'tests/test_b3_durable_event_scope.py', 'tests/test_b3_publication_end_to_end.py',
        'tests/test_b3_publication_projection.py', 'tests/test_b3_snapshot_recipe_capture.py',
        'tests/test_b3_a5_observability.py', 'tests/test_b3_research_variants.py',
        'tests/test_b3_variant_replay.py',
    ]
    for name in tests:
        path = Path(name)
        if (path.parts[:1] != ('tests',) or len(path.parts) != 2
                or not path.name.startswith('test_') or path.suffix != '.py'
                or not (root / path).is_file()):
            raise ValueError('INVALID_ISOLATED_TEST_FILE')
    out = args.output.resolve(); out.mkdir(parents=True, exist_ok=False)
    tag = datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y%m%dT%H%M%S%f')
    remote = '/tmp/liangjian-b3-' + tag
    archive = out / 'slice.tar'
    paths = sorted((root / 'src').rglob('*.py')) + sorted((root / 'tests').glob('test_*.py'))
    paths += sorted((root / 'prompts').glob('*.txt'))
    manifest = []
    with tarfile.open(archive, 'x') as tar:
        for path in paths:
            assert path.is_file() and not path.is_symlink() and path.resolve().is_relative_to(root)
            tar.add(path, arcname=path.relative_to(root).as_posix(), recursive=False)
            manifest.append({'path': path.relative_to(root).as_posix(),
                'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
    ssh = ['ssh', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
           '-o', 'ConnectTimeout=8', 'aurum-vm']
    commands = []
    def call(command, *, data=None, timeout=90):
        result = subprocess.run(command, input=data, capture_output=True, timeout=timeout)
        index = len(commands)
        (out / f'{index}-stdout.log').write_bytes(result.stdout)
        (out / f'{index}-stderr.log').write_bytes(result.stderr)
        commands.append({'command': command, 'exit_code': result.returncode})
        (out / 'commands.json').write_text(json.dumps(commands, indent=2), encoding='utf-8')
        if result.returncode:
            raise RuntimeError(f'VM_SLICE_COMMAND_FAILED:{index}')
        return result.stdout
    identity = call(ssh + ['hostname; cd /www/wwwroot/Agu/liangjian-funnel-workflow && runuser -u www -- git rev-parse HEAD']).decode().splitlines()
    assert identity == ['debian', '42969252fb4369c5fc1fdd95650bf6fcbf71150e'], identity
    call(ssh + ['mkdir -m 755 ' + remote])
    call(['scp', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
          str(archive), 'aurum-vm:' + remote + '/slice.tar'])
    source = r'''
from pathlib import Path
import tarfile,subprocess,os,json,sys,hashlib
root=Path(sys.argv[1]).resolve()
assert root.parent==Path('/tmp') and root.name.startswith('liangjian-b3-20261015T')
with tarfile.open(root/'slice.tar') as tar:
 for item in tar:
  assert item.isfile() and (root/item.name).resolve().is_relative_to(root)
  tar.extract(item,root)
(root/'tmp').mkdir()
subprocess.run(['chown','-R','www:www',str(root)],check=True)
env={'PATH':'/usr/bin:/bin','PYTHONPATH':str(root/'src'),'PYTHONDONTWRITEBYTECODE':'1','TMPDIR':str(root/'tmp'),'TZ':'Asia/Shanghai','HOME':str(root),'LIANGJIAN_ROOT':str(root)}
python='/www/wwwroot/Agu/liangjian-funnel-workflow/.venv/bin/python'
tests=json.loads(sys.argv[2])
p=subprocess.run(['/usr/sbin/runuser','-u','www','--',python,'-B','-m','pytest',*tests,'-q',
 '--junitxml='+str(root/'slice.xml'),'--basetemp='+str(root/'tmp/pytest')],cwd=root,env=env,capture_output=True,timeout=70)
(root/'pytest.stdout.log').write_bytes(p.stdout);(root/'pytest.stderr.log').write_bytes(p.stderr)
import xml.etree.ElementTree as ET
counts={'tests':0,'passed':0,'failed':0,'skipped':0}
if (root/'slice.xml').exists():
 for case in ET.parse(root/'slice.xml').iter('testcase'):
  counts['tests']+=1
  label='failed' if case.find('failure') is not None or case.find('error') is not None else ('skipped' if case.find('skipped') is not None else 'passed')
  counts[label]+=1
cli_probe=None
if 'tests/test_b3_opening_evidence.py' in tests and p.returncode==0:
 from datetime import datetime,time
 from zoneinfo import ZoneInfo
 current=datetime.now(ZoneInfo('Asia/Shanghai'))
 assert not time(9,25)<=current.time().replace(tzinfo=None)<=time(9,25,30), 'REFUSE_LIVE_WINDOW_CLI_PROBE'
 probe=subprocess.run(['/usr/sbin/runuser','-u','www','--',python,'-B','-m','liangjian_funnel','run-auction-evidence'],
  cwd=root,env=env,capture_output=True,timeout=25)
 body=json.loads(probe.stdout)
 assert probe.returncode==0 and body.get('reason_code')=='OPENING_EVIDENCE_WINDOW_MISSED'
 assert not (root/'state').exists() and not (root/'outputs').exists()
 cli_probe={'exit_code':probe.returncode,'result':body,'actual_clock':current.isoformat(),
  'production_environment_loaded':False,'runtime_store_created':False,'network_called':False}
result={'actual_at':__import__('datetime').datetime.now().astimezone().isoformat(),
 'sandbox':str(root),'pytest_exit':p.returncode,'counts':counts,
 'production_writes':0,'models':0,'notifications':0,'selected_test_files':tests,'opening_cli_probe':cli_probe,
 'source_hashes':{str(path.relative_to(root)):hashlib.sha256(path.read_bytes()).hexdigest() for path in (root/'src').rglob('*.py')}}
(root/'ACCEPTANCE.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
print(json.dumps(result))
'''
    raw = call(ssh + ['cd /tmp && /www/wwwroot/Agu/liangjian-funnel-workflow/.venv/bin/python -B - '
                     + remote + ' ' + shlex.quote(json.dumps(tests))], data=source.encode(), timeout=90)
    result = json.loads(raw)
    for row in manifest:
        if row['path'].startswith('src/'):
            assert result['source_hashes'][row['path']] == row['sha256']
    for name in ('slice.xml', 'pytest.stdout.log', 'pytest.stderr.log'):
        call(['scp', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
              'aurum-vm:' + remote + '/' + name, str(out / name)])
    result.update(source_manifest=manifest, identity=identity,
        archive_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),
        local_head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip(),
        command_exit_codes=[entry['exit_code'] for entry in commands])
    (out / 'ACCEPTANCE.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
    # Evidence has been copied locally before removing this uniquely created sandbox.
    cleanup = r'''
from pathlib import Path
import shutil,sys
root=Path(sys.argv[1]).resolve(strict=True)
assert root.parent==Path('/tmp') and root.name.startswith('liangjian-b3-20261015T')
assert (root/'ACCEPTANCE.json').is_file() and (root/'slice.xml').is_file()
shutil.rmtree(root)
assert not root.exists()
print('ISOLATED_SANDBOX_REMOVED_AFTER_EVIDENCE_COPY')
'''
    call(ssh + ['cd /tmp && /www/wwwroot/Agu/liangjian-funnel-workflow/.venv/bin/python -B - ' + remote],
         data=cleanup.encode())
    result['sandbox_cleaned_after_local_evidence_copy'] = True
    result['command_exit_codes'] = [entry['exit_code'] for entry in commands]
    (out / 'ACCEPTANCE.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
    print(json.dumps({'output':str(out),'counts':result['counts'],'pytest_exit':result['pytest_exit']}))
    return result['pytest_exit']


if __name__ == '__main__':
    raise SystemExit(main())
