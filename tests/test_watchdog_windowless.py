"""Prevent imported pin subprocesses regressing to visible Windows consoles."""
from pathlib import Path
import hashlib,importlib.util,json,os,subprocess
import pytest

ROOT=Path(__file__).resolve().parents[1]

def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module

@pytest.mark.skipif(os.name!='nt',reason='Windows creationflags contract')
def test_two_actual_pin_checks_are_windowless_and_strict():
    check=load('windowless_guard',ROOT/'scripts/check_watchdog_windowless.py')
    result=check.verify(ROOT/'scripts/watch_a4_runtime.py')
    assert result['status']=='PASSED' and result['remote_ssh_connections']==0
    assert len(result['local_pin_checks'])==2 and result['strict_host_check']

@pytest.mark.skipif(os.name!='nt',reason='Windows creationflags contract')
def test_proxy_preserves_existing_flags_and_actual_check_output_arguments():
    watch=load('watchdog_flags',ROOT/'scripts/watch_a4_runtime.py');original=subprocess.check_output;calls=[]
    class Spy:
        CREATE_NO_WINDOW=subprocess.CREATE_NO_WINDOW
        def __getattr__(self,name):return getattr(subprocess,name)
        def check_output(self,*args,**kwargs):calls.append((args,dict(kwargs)));return original(*args,**kwargs)
    watch.subprocess=Spy();proxy=watch._WindowlessPinSubprocess()
    command=['ssh','-G','aurum-vm'];flag=subprocess.CREATE_NEW_PROCESS_GROUP
    result=proxy.check_output(command,text=True,creationflags=flag)
    assert 'hostname 192.168.0.254\n'in result
    assert calls[0][0]==(command,) and calls[0][1]['text']is True
    assert calls[0][1]['creationflags']==flag|subprocess.CREATE_NO_WINDOW
    assert proxy.CalledProcessError is subprocess.CalledProcessError

def test_hash_conflict_still_fails_before_any_pin_subprocess(tmp_path):
    watch=load('watchdog_bad_pin',ROOT/'scripts/watch_a4_runtime.py')
    config=json.loads((ROOT/'config/deployment_host.json').read_bytes());p=Path(config['ssh_pin_module'])
    before=hashlib.sha256(p.read_bytes()).hexdigest();config['ssh_pin_module_sha256']='0'*64
    (tmp_path/'scripts').mkdir();(tmp_path/'config').mkdir()
    (tmp_path/'config/deployment_host.json').write_text(json.dumps(config),encoding='utf8')
    watch.__file__=str(tmp_path/'scripts/watch_a4_runtime.py')
    class Forbidden:
        def check_output(self,*args,**kwargs):raise AssertionError('HASH_CONFLICT_MUST_NOT_EXECUTE')
    watch.subprocess=Forbidden()
    with pytest.raises(ValueError,match='WATCHDOG_PIN_MODULE_HASH_CONFLICT'):watch.pinned_ssh_options()
    assert hashlib.sha256(p.read_bytes()).hexdigest()==before

def test_install_guard_precedes_task_mutation():
    source=(ROOT/'scripts/install_a4_watchdog.ps1').read_text('utf8')
    assert source.index('check_watchdog_windowless.py')<source.index('Register-ScheduledTask')
    assert 'if ($LASTEXITCODE -ne 0)'in source
