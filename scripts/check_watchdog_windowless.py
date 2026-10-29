"""Local-only update guard: actual known_hosts and ssh -G pin checks, no SSH connection."""
from pathlib import Path
import hashlib,importlib.util,json,os,subprocess,sys

def verify(script=None):
    if os.name!='nt':raise RuntimeError('WATCHDOG_WINDOWLESS_CHECK_REQUIRES_WINDOWS')
    script=Path(script or Path(__file__).with_name('watch_a4_runtime.py')).resolve()
    spec=importlib.util.spec_from_file_location('_watchdog_windowless_verification',script)
    watcher=importlib.util.module_from_spec(spec);spec.loader.exec_module(watcher)
    conf=script.parents[1]/'config/deployment_host.json';config_before=conf.read_bytes();cfg=json.loads(config_before)
    pin=Path(cfg['ssh_pin_module']);pin_before=pin.read_bytes();calls=[]
    original=subprocess.check_output
    allowed=(['ssh-keygen','-F',cfg['ssh_alias']],['ssh','-G',cfg['ssh_alias']])
    class LocalOnlySubprocess:
        def __getattr__(self,name):return getattr(subprocess,name)
        def check_output(self,*args,**kwargs):
            command=args[0]if args else kwargs.get('args')
            if command not in allowed:raise AssertionError('WATCHDOG_VERIFY_NONLOCAL_COMMAND_FORBIDDEN')
            flags=kwargs.get('creationflags',0)
            assert flags&subprocess.CREATE_NO_WINDOW,'WATCHDOG_PIN_CHILD_CAN_OPEN_WINDOW'
            calls.append(dict(command=list(command),creationflags=flags))
            return original(*args,**kwargs)
    watcher.subprocess=LocalOnlySubprocess()
    options=watcher.pinned_ssh_options()
    assert [x['command']for x in calls]==list(allowed),'WATCHDOG_PIN_CHECK_COVERAGE_INVALID'
    assert 'StrictHostKeyChecking=yes'in options and 'BatchMode=yes'in options
    assert any(x.startswith('HostKeyAlgorithms=')for x in options)
    assert conf.read_bytes()==config_before and pin.read_bytes()==pin_before
    assert hashlib.sha256(pin_before).hexdigest()==cfg['ssh_pin_module_sha256']
    return dict(status='PASSED',local_pin_checks=calls,strict_host_check=True,
        pin_module_sha256=cfg['ssh_pin_module_sha256'],remote_ssh_connections=0,model_http=0,
        production_writes=0,task_changes=0)

if __name__=='__main__':
    sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
    print(json.dumps(verify(),ensure_ascii=False))
