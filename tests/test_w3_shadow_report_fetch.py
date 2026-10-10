"""Pure fixture transport; never SSH, scheduler or production."""
from datetime import datetime
import hashlib
import importlib.util
import json
from pathlib import Path

import pytest

SPEC=importlib.util.spec_from_file_location('shadow_fetch',Path(__file__).parents[1]/'scripts/fetch_shadow_report_readonly.py')
mod=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(mod)
DAY='2026-10-12'
NOW=datetime.fromisoformat(DAY+'T17:00:00+08:00')
ROOT='/isolated/shadow'
ARCH=ROOT+'/archive';DOC=ROOT+'/docs';BRIDGE=ROOT+'/bridge'


def raw(body):return json.dumps(body,sort_keys=True,separators=(',',':')).encode()
def sha(body):return hashlib.sha256(body).hexdigest()


def package():
    proof={'schema':'shadow-production-equivalence/1','status':'DATA_LIMITED'}
    report={'schema':'shadow-reporting-gap/1','trade_date':DAY,'status':'DATA_LIMITED','production_equivalence':proof}
    files={ARCH+'/'+DAY+'/report.json':raw(report),DOC+'/SHADOW_DAILY_'+DAY+'.md':b'# DATA_LIMITED\n',
           DOC+'/PRODUCTION_EQUIVALENCE_'+DAY+'.json':raw(proof)}
    delivery={'schema_version':'shadow-reporting/1','trade_date':DAY,'report_sha256':sha(files[ARCH+'/'+DAY+'/report.json']),
              'outputs':{p:sha(b) for p,b in files.items()},'notifications':0,'production_mutation':False}
    files[BRIDGE+'/'+DAY+'/W3_SHADOW_DAILY.json']=raw(delivery)
    manifest={'schema_version':'shadow-reporting/1','trade_date':DAY,'as_of':DAY+'T16:45:00+08:00',
              'outputs':{p:sha(b) for p,b in files.items()},'a5_status':'A5_NOT_COMPLETE_OR_AMBIGUOUS',
              'draft_status':'ON_TIME_DRAFT','draft_sha256':'a'*64}
    path=ARCH+'/'+DAY+'/final-manifest.json';files[path]=raw(manifest)
    return files,path


def fetch(tmp_path, files=None, **kw):
    files,path=package() if files is None else files
    args=dict(remote_root=ROOT,archive_root=ARCH,report_root=DOC,bridge_root=BRIDGE,trade_date=DAY,
              manifest_sha256=sha(files[path]),shadow_outbox=tmp_path/'shadow_outbox',observed_at=NOW,reader=files.__getitem__)
    args.update(kw)
    return mod.fetch_report(**args)


def test_bytes_verified_idempotent_and_sources_unchanged(tmp_path):
    files,path=package();before=dict(files)
    first=fetch(tmp_path,(files,path));assert first['status']=='BYTES_VERIFIED'
    saved={p:p.read_bytes() for p in (tmp_path/'shadow_outbox').rglob('*') if p.is_file()}
    assert fetch(tmp_path,(files,path))['status']=='ALREADY_PRESENT'
    assert all(p.read_bytes()==b for p,b in saved.items()) and files==before
    assert first['production_mutation'] is False and first['claude_review_received'] is False


@pytest.mark.parametrize('case',['hash','date','future','naive','extra','missing','traversal','duplicate'])
def test_bad_manifest_no_local_output(tmp_path,case):
    files,path=package();body=json.loads(files[path])
    if case=='hash':body['outputs'][next(iter(body['outputs']))]='b'*64
    elif case=='date':body['trade_date']='2026-10-09'
    elif case=='future':body['as_of']=DAY+'T18:00:00+08:00'
    elif case=='naive':body['as_of']=DAY+'T16:45:00'
    elif case=='extra':body['outputs'][ROOT+'/state.sqlite3']='b'*64
    elif case=='missing':body['outputs'].pop(next(iter(body['outputs'])))
    elif case=='traversal':body['outputs'][ROOT+'/docs/../state.sqlite3']='b'*64
    files[path]=raw(body)
    if case=='duplicate':files[path]=files[path][:-1]+b',"trade_date":"2026-10-09"}'
    with pytest.raises((ValueError,KeyError)):fetch(tmp_path,(files,path))
    assert not (tmp_path/'shadow_outbox').exists()


@pytest.mark.parametrize('case',['receipt','report_date','proof','body_size','manifest_pin','transport','changed_manifest'])
def test_package_pin_and_semantic_bindings(tmp_path,case):
    files,path=package();body=json.loads(files[path]);opts={}
    target=BRIDGE+'/'+DAY+'/W3_SHADOW_DAILY.json'
    if case=='receipt':
        receipt=json.loads(files[target]);receipt['report_sha256']='b'*64;files[target]=raw(receipt)
    elif case=='report_date':
        target=ARCH+'/'+DAY+'/report.json';r=json.loads(files[target]);r['trade_date']='2026-10-09';files[target]=raw(r)
    elif case=='proof':target=DOC+'/PRODUCTION_EQUIVALENCE_'+DAY+'.json';files[target]=raw({'schema':'shadow-production-equivalence/1','status':'MATCHED'})
    elif case=='body_size':files[target]=b'x'*(mod.MAX_FILE_BYTES+1)
    elif case=='manifest_pin':opts['manifest_sha256']='b'*64
    elif case=='transport':
        def fail(p):raise OSError('fixture source down')
        opts['reader']=fail
    elif case=='changed_manifest':
        calls=[]
        def changing(p):
            calls.append(p)
            return files[p]+b' ' if p==path and calls.count(p)>1 else files[p]
        opts['reader']=changing
    if case in ('receipt','report_date','proof','body_size'):
        body['outputs'][target]=sha(files[target]);files[path]=raw(body)
    with pytest.raises((ValueError,OSError)):fetch(tmp_path,(files,path),**opts)
    assert not (tmp_path/'shadow_outbox').exists()


@pytest.mark.parametrize('root',['relative','/','/isolated//shadow','/isolated/shadow/..','/isolated/shadow;touch-x'])
def test_unsafe_remote_roots_rejected(tmp_path,root):
    with pytest.raises(ValueError):fetch(tmp_path,remote_root=root)
    assert not (tmp_path/'shadow_outbox').exists()


def test_conflict_no_overwrite_or_new_files(tmp_path):
    out=tmp_path/'shadow_outbox'/DAY;out.mkdir(parents=True)
    p=out/'report.json';p.write_bytes(b'original')
    with pytest.raises(ValueError,match='CONFLICT'):fetch(tmp_path)
    assert p.read_bytes()==b'original' and list(out.iterdir())==[p]


@pytest.mark.parametrize('name',['inbox','outbox','state'])
def test_no_canonical_bridge_write(tmp_path,name):
    with pytest.raises(ValueError):fetch(tmp_path,shadow_outbox=tmp_path/'.claude_bridge'/name/'shadow_outbox')
    assert not (tmp_path/'.claude_bridge').exists()


def test_ssh_command_fixed_readonly_strict_and_decodes_pin():
    seen=[];body=b'fixture'
    def invoke(command,**kw):
        from types import SimpleNamespace
        seen.append((command,kw))
        import base64
        return SimpleNamespace(returncode=0,stdout=raw({'path':ARCH+'/file','raw_sha256':sha(body),'body_base64':base64.b64encode(body).decode()}),stderr=b'')
    reader=mod.ssh_reader(run=invoke)
    assert reader(ARCH+'/file')==body
    command=seen[0][0]
    assert command[-2]=='aurum-vm' and 'StrictHostKeyChecking=yes' in command and 'BatchMode=yes' in command
    assert command[-1].startswith('python3 -c ')
    assert 'scp' not in command and seen[0][1]['timeout']<=8


def test_local_real_w3_producer_manifest(tmp_path):
    from test_w3_shadow_reporting import fixture,at
    from liangjian_funnel.runtime.shadow_reporting import run_reporting_tick
    (tmp_path/'producer').mkdir()
    cfg=fixture(tmp_path/'producer');assert run_reporting_tick(cfg,observed_at=at('16:45:00'))['status']=='FORMAL_WRITTEN'
    saved=json.loads((cfg.archive_root/DAY/'final-manifest.json').read_bytes())
    files,path=package();mapping={str(cfg.archive_root/DAY/'report.json'):ARCH+'/'+DAY+'/report.json',
        str(cfg.report_root/f'SHADOW_DAILY_{DAY}.md'):DOC+'/SHADOW_DAILY_'+DAY+'.md',
        str(cfg.report_root/f'PRODUCTION_EQUIVALENCE_{DAY}.json'):DOC+'/PRODUCTION_EQUIVALENCE_'+DAY+'.json',
        str(cfg.bridge_outbox/DAY/'W3_SHADOW_DAILY.json'):BRIDGE+'/'+DAY+'/W3_SHADOW_DAILY.json'}
    # Explicit fixture transport projection Windows paths -> deployed POSIX;
    # hashes use actual producer bytes except receipt's path projection.
    files={mapping[p]:Path(p).read_bytes() for p in saved['outputs']}
    bridge=BRIDGE+'/'+DAY+'/W3_SHADOW_DAILY.json';r=json.loads(files[bridge]);r['outputs']={mapping[p]:h for p,h in r['outputs'].items()};files[bridge]=raw(r)
    saved['outputs']={p:sha(b) for p,b in files.items()};files[path]=raw(saved)
    assert fetch(tmp_path,(files,path))['status']=='BYTES_VERIFIED'


def test_package_total_limit_precedes_local_writes(tmp_path,monkeypatch):
    monkeypatch.setattr(mod,'MAX_PACKAGE_BYTES',100)
    with pytest.raises(ValueError,match='PACKAGE_SIZE'):fetch(tmp_path)
    assert not (tmp_path/'shadow_outbox').exists()


def test_existing_hardlink_alias_rejected(tmp_path):
    import os
    out=tmp_path/'shadow_outbox'/DAY;out.mkdir(parents=True)
    secret=tmp_path/'original';secret.write_bytes(b'original')
    os.link(secret,out/'report.json')
    with pytest.raises(ValueError,match='ALIAS'):fetch(tmp_path)
    assert secret.read_bytes()==b'original'


def test_existing_reparse_point_rejected_before_transport(tmp_path,monkeypatch):
    from types import SimpleNamespace
    out=tmp_path/'shadow_outbox';out.mkdir()
    original=Path.lstat
    def fake(path,*a,**kw):
        value=original(path,*a,**kw)
        return SimpleNamespace(st_file_attributes=1024,st_mode=value.st_mode) if path==out else value
    monkeypatch.setattr(Path,'lstat',fake)
    with pytest.raises(ValueError,match='ALIAS'):fetch(tmp_path)
    assert list(out.iterdir())==[]


@pytest.mark.parametrize('case',['path','sha','size','exit'])
def test_ssh_invalid_envelope_rejected(case):
    import base64
    from types import SimpleNamespace
    body=b'x'*(mod.MAX_FILE_BYTES+1) if case=='size' else b'fixture'
    response={'path':ARCH+'/other' if case=='path' else ARCH+'/file',
              'raw_sha256':'b'*64 if case=='sha' else sha(body),'body_base64':base64.b64encode(body).decode()}
    with pytest.raises(ValueError):
        mod.ssh_reader(run=lambda *a,**k:SimpleNamespace(returncode=1 if case=='exit' else 0,stdout=raw(response),stderr=b'private'))(ARCH+'/file')


def test_default_cli_unwired_no_ssh_or_output(tmp_path,monkeypatch,capsys):
    def forbidden(**kw):raise AssertionError('must not construct SSH transport')
    monkeypatch.setattr(mod,'ssh_reader',forbidden)
    args=['--remote-root',ROOT,'--archive-root',ARCH,'--report-root',DOC,'--bridge-root',BRIDGE,
          '--trade-date',DAY,'--manifest-sha256','a'*64,'--shadow-outbox',str(tmp_path/'shadow_outbox')]
    assert mod.main(args)==2
    assert 'private' not in capsys.readouterr().out and not (tmp_path/'shadow_outbox').exists()
