"""Locked minimal merges for small shared JSON state, never in-place writes."""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import tempfile
import time


@contextmanager
def exclusive_file(path, *, timeout=5):
    path=Path(path); deadline=time.monotonic()+timeout
    while True:
        try:
            fd=os.open(path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
            break
        except FileExistsError:
            if time.monotonic()>=deadline:raise TimeoutError('SHARED_JSON_WRITER_BUSY')
            time.sleep(.01)
    try:
        with os.fdopen(fd,'w') as stream:
            stream.write(str(os.getpid()));stream.flush();os.fsync(stream.fileno())
        yield
    finally:path.unlink()


def atomic_json(path, value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    payload=json.dumps(value,ensure_ascii=False,sort_keys=True,indent=2,allow_nan=False)+'\n'
    json.loads(payload)
    fd,name=tempfile.mkstemp(prefix='.'+path.name+'.',suffix='.tmp',dir=path.parent)
    temp=Path(name)
    try:
        with os.fdopen(fd,'w',encoding='utf-8',newline='\n') as stream:
            stream.write(payload);stream.flush();os.fsync(stream.fileno())
        if path.exists():os.chmod(temp,path.stat().st_mode & 0o777)
        os.replace(temp,path)
        if json.loads(path.read_bytes())!=value:raise ValueError('SHARED_JSON_POSTWRITE_MISMATCH')
        if os.name!='nt':
            directory=os.open(path.parent,os.O_RDONLY)
            try:os.fsync(directory)
            finally:os.close(directory)
    finally:
        if temp.exists():temp.unlink()


def merge_shared_json(path, updates, *, allowed_prefix=None):
    """Top-level owned keys only. Refuse corrupt originals instead of losing keys."""
    path=Path(path)
    if not isinstance(updates,dict) or not updates:raise ValueError('SHARED_JSON_PATCH_INVALID')
    if allowed_prefix and any(not k.startswith(allowed_prefix) for k in updates):
        raise ValueError('SHARED_JSON_UNOWNED_KEY')
    with exclusive_file(path.with_name(path.name+'.lock')):
        before=path.read_bytes() if path.exists() else b'{}'
        state=json.loads(before)
        if not isinstance(state,dict):raise ValueError('SHARED_JSON_ROOT_NOT_OBJECT')
        state.update(updates)
        # Also detect a non-cooperating writer that changed the file while read.
        if path.exists() and path.read_bytes()!=before:raise ValueError('SHARED_JSON_CONCURRENT_CHANGE')
        atomic_json(path,state)
        return state
