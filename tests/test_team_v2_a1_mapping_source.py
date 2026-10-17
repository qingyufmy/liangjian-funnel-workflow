from dataclasses import asdict
from datetime import datetime
import hashlib,importlib.util,json,sqlite3,sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from liangjian_funnel.pipeline.research.common import StageAudit
from liangjian_funnel.runtime.team_v2_store import canonical


def fixture(tmp_path):
    scripts=Path(__file__).resolve().parents[1]/'scripts';sys.path.insert(0,str(scripts))
    spec=importlib.util.spec_from_file_location('a1_mapping_probe',scripts/'run_team_a1_mapping_frozen_dryrun.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    output={'structural_themes':[]};digest=hashlib.sha256(canonical(output).encode()).hexdigest()
    audit=StageAudit('lane_1','deepseek-v4-pro','A1','VALIDATED','original','p','i',digest,12,1,None,(),(),
        output=output,diagnostics={'team_call_id':'original-call'})
    (tmp_path/'STAGE_AUDIT.json').write_text(json.dumps(audit.as_dict()),encoding='utf-8')
    (tmp_path/'PREPARED.json').write_text(json.dumps({'parent_snapshot_sha256':'s'}),encoding='utf-8')
    path=tmp_path/'state/team_v2/model.db';path.parent.mkdir(parents=True)
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE model_calls(call_id TEXT,role TEXT,session TEXT,validated INTEGER,output_json TEXT,output_hash TEXT)')
        db.execute('INSERT INTO model_calls VALUES (?,?,?,?,?,?)',('original-call','A1','POLICY_MACRO_DISCOVERY',1,json.dumps(output),digest))
    snapshot=SimpleNamespace(snapshot_id='original',snapshot_hash='s')
    return module,snapshot,path


def test_company_mapping_requires_actual_validated_and_hash_bound_discovery(tmp_path):
    module,snapshot,path=fixture(tmp_path);original=path.read_bytes()
    audit,_=module.verified_discovery(tmp_path,snapshot)
    assert audit.status=='VALIDATED' and path.read_bytes()==original


@pytest.mark.parametrize('fault',['snapshot','unvalidated','corrupt_raw','wrong_session','audit_payload'])
def test_old_blocked_foreign_or_changed_capture_cannot_be_used_to_start_company_mapping(tmp_path,fault):
    module,snapshot,path=fixture(tmp_path)
    if fault=='snapshot':snapshot.snapshot_hash='changed'
    elif fault=='audit_payload':
        target=tmp_path/'STAGE_AUDIT.json';body=json.loads(target.read_bytes());body['output']['invented']=True
        target.write_text(json.dumps(body),encoding='utf-8')
    else:
        with sqlite3.connect(path) as db:
            if fault=='unvalidated':db.execute('UPDATE model_calls SET validated=0')
            elif fault=='corrupt_raw':db.execute('UPDATE model_calls SET output_hash=?',('wrong',))
            else:db.execute('UPDATE model_calls SET session=?',('COMPANY_MAPPING',))
    with pytest.raises(ValueError,match='UNPROVEN|INVALID'):module.verified_discovery(tmp_path,snapshot)
