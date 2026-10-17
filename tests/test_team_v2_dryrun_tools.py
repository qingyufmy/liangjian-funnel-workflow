import base64
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path

import pytest


def module(name):
    path=Path(__file__).resolve().parents[1]/'scripts'/f'{name}.py'
    spec=importlib.util.spec_from_file_location(name,path);loaded=importlib.util.module_from_spec(spec);spec.loader.exec_module(loaded)
    return loaded


def test_diff_covers_missing_added_nested_and_length_without_schema_equivalence_claim():
    diff=module('run_team_a5_frozen_dryrun').field_diff({'a':[1,2],'old':True},{'a':[3],'new':None})
    assert {(r['path'],r['kind']) for r in diff}=={('$.a[0]','CHANGED'),('$.a[1]','REMOVED'),('$.old','REMOVED'),('$.new','ADDED')}


@pytest.mark.parametrize('bad',['digest','scope','incomplete',None])
def test_source_unpack_refuses_bad_hash_path_or_incomplete_stream(tmp_path,bad):
    mod=module('export_team_dryrun_sources_readonly');raw=b'original-input';relative='../escape' if bad=='scope' else 'safe/source.json'
    rows=[dict(kind='manifest',actual_at='2026-10-17T08:00:00+08:00'),
        dict(kind='file_start',relative=relative),dict(kind='chunk',data=base64.b64encode(raw).decode()),
        dict(kind='file_end',relative=relative,bytes=len(raw),sha256='0'*64 if bad=='digest' else hashlib.sha256(raw).hexdigest())]
    if bad!='incomplete':rows.append(dict(kind='complete'))
    archive=tmp_path/'source.gz'
    with gzip.open(archive,'wt') as f:
        for row in rows:f.write(json.dumps(row)+'\n')
    if bad:
        with pytest.raises(ValueError,match='ARCHIVE_'):mod.unpack(archive,tmp_path/'out')
    else:
        proof=mod.unpack(archive,tmp_path/'out')
        assert len(proof['files'])==1 and (tmp_path/'out/safe/source.json').read_bytes()==raw
