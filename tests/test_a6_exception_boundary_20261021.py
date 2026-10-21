import json,hashlib,sqlite3
from pathlib import Path
from datetime import datetime

from liangjian_funnel.pipeline.team_v2_risk_schema import validate_risk_exception
from liangjian_funnel.runtime.team_v2_risk_exception import RiskExceptionRuntime
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore


def test_actual_1441_eight_errors_remain_rejected_not_silently_rewritten(tmp_path):
    b=json.loads((Path(__file__).parent/'fixtures/a6_exception_20261021_1441_original.json').read_text(encoding='utf-8'))
    call=b['call'];raw=call['output_json']
    assert hashlib.sha256(raw.encode()).hexdigest()=='2afabfb0b61711579452dbb07af57c275dc81a248635becd5b961dba63ce5e71'
    output=json.loads(raw);source=json.loads(b['risk_source'][0]['body_json']);ctx=json.loads(b['requests'][0]['context_json'])
    result=validate_risk_exception(output,json.loads(call['catalog']['catalog_json']),
        required_envelope=output['envelope'],targets=ctx['targets'],resume_allowed=set(),hard_halt=False,
        engine_utilization_cap=source['engine_utilization_cap'],capacity_slots=source['source']['capacity_slots'],
        cluster_caps=source['source']['cluster_caps'])
    assert len(result.issues)==8 and not result.valid
    assert 'A6_EXCEPTION_ACTION_INVALID' in result.issues
    store=TeamMeetingStore(tmp_path/'isolated-team.db');runtime=RiskExceptionRuntime(store)
    with sqlite3.connect(store.path) as db:
        db.execute('INSERT INTO risk_day_state VALUES (?,?,?,?,?)',('local-paper','2026-10-21',0,0,'2026-10-21T08:35:01.361626+08:00'))
    before=runtime.restrictions('local-paper','2026-10-21')
    runtime.claim('1441',account_id='local-paper',trade_date='2026-10-21',
        observed_at=datetime.fromisoformat(ctx['observed_at']),input_sha256=call['input_hash'],targets=ctx['targets'],hard_halt=False)
    saved=runtime.record('1441',source_call_id=call['call_id'],input_sha256=call['input_hash'],output=output,validated=False)
    assert saved['failure_scope']=='EXCEPTION_SESSION_ONLY' and not saved['account_record_required']
    assert not saved['position_directives']
    after=runtime.restrictions('local-paper','2026-10-21')
    assert after['consecutive_failures']==before['consecutive_failures']==0
    with sqlite3.connect(store.path) as db:
        assert db.execute('SELECT * FROM risk_day_state').fetchall()==[('local-paper','2026-10-21',0,0,'2026-10-21T08:35:01.361626+08:00')]


def test_exception_failure_cannot_clear_real_morning_halt(tmp_path):
    from test_team_v2_risk_exception import response,targets,AT
    store=TeamMeetingStore(tmp_path/'isolated-team.db');runtime=RiskExceptionRuntime(store)
    with sqlite3.connect(store.path) as db:
        db.execute('INSERT INTO risk_day_state VALUES (?,?,?,?,?)',('paper','2026-10-19',2,1,AT.isoformat()))
    runtime.claim('halted',account_id='paper',trade_date='2026-10-19',observed_at=AT,
        input_sha256='frozen',targets=targets(),hard_halt=False)
    result=runtime.record('halted',source_call_id='A6:halted',input_sha256='frozen',output=response('NO_ACTION'),validated=True)
    assert result['status']=='HALT' and result['account_record_required']
    with sqlite3.connect(store.path) as db:
        assert db.execute('SELECT consecutive_failures,halted FROM risk_day_state').fetchall()==[(2,1)]
