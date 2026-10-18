import hashlib
import importlib
import json
from pathlib import Path
import sqlite3

import pytest


def capture(tmp_path,monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1]/'scripts'))
    helper=importlib.import_module('run_team_pm_frozen_dryrun').verified_a6_capture
    from liangjian_funnel.pipeline.team_v2_judgments import ModelJudgmentJournal
    journal=ModelJudgmentJournal(tmp_path/'model.db')
    source=dict(run_id='frozen-a6',head='source-head',request_input_sha256='input',prompt_hash='prompt',
        source_trade_date='2026-10-14',execution_trade_date='2026-10-15')
    output=json.dumps({'session':'PREMARKET'});output_hash=hashlib.sha256(output.encode()).hexdigest()
    result=dict(status='PASS',source_head=source['head'],input_sha256='input',response_output_sha256=output_hash)
    journal.claim('A6:frozen-a6','binding')
    with sqlite3.connect(journal.path) as db:
        db.execute("UPDATE model_call_claims SET state='CAPTURED'")
        db.execute('INSERT INTO model_calls VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            ('A6:frozen-a6','frozen-a6','A6','PREMARKET','2026-10-14','2026-10-15',
             '2026-10-17T10:00:00+08:00','model','prompt','input','catalog',output_hash,20,1,1,'[]',0,output,'binding'))
    return helper,source,result


def test_pm_uses_original_validated_a6_capture_without_request_or_source_write(tmp_path,monkeypatch):
    helper,source,result=capture(tmp_path,monkeypatch);path=tmp_path/'model.db';before=path.read_bytes()
    raw,valid,basis=helper(tmp_path,source,result)
    assert raw=={'session':'PREMARKET'} and valid and basis=='ACTUAL_VALIDATED_A6_CAPTURE'
    assert path.read_bytes()==before


@pytest.mark.parametrize('column,value',[('validated',0),('input_hash','foreign'),('prompt_hash','old'),
    ('source_trade_date','2026-10-13'),('output_json','{}'),('issues_json','["INVALID"]')])
def test_pm_cannot_turn_modified_or_foreign_a6_capture_into_permission(tmp_path,monkeypatch,column,value):
    helper,source,result=capture(tmp_path,monkeypatch)
    with sqlite3.connect(tmp_path/'model.db') as db:db.execute('UPDATE model_calls SET '+column+'=?',(value,))
    with pytest.raises(ValueError,match='CAPTURE_HASH_OR_BINDING_INVALID'):helper(tmp_path,source,result)


def test_actual_failed_a6_retains_conservative_fallback_not_a_fabricated_vote(tmp_path,monkeypatch):
    helper,source,result=capture(tmp_path,monkeypatch);result['status']='MODEL_CALL_FAILED'
    with sqlite3.connect(tmp_path/'model.db') as db:
        db.execute('DELETE FROM model_calls')
        db.execute("UPDATE model_call_claims SET state='FAILED',failure_code='TEAM_MODEL_CALL_FAILED'")
    assert helper(tmp_path,source,result)==(None,False,'ACTUAL_FAILURE_CONSERVATIVE_FALLBACK')
