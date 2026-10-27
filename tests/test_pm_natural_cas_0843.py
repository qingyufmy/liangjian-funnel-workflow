"""Natural A6 -> concurrent PM -> original-evidence revalidation -> CAS."""
from copy import deepcopy
from contextlib import closing
import json,sqlite3,threading,re
import httpx
import pytest
from fixture_model_stream import sse_model_response
from test_natural_reviewed_batch_0595 import setup,CAL
from test_team_v2_morning import advance
from test_team_v2_risk_schema import output as risk_output
from test_team_v2_pm_session import response as pm_output
from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient
from liangjian_funnel.runtime.team_v2_morning import run_morning_stage
from liangjian_funnel.runtime.reviewed_batch_natural import read_natural_candidate
from liangjian_funnel.runtime.reviewed_batch_evidence import build_reviewed_batch_evidence


@pytest.mark.parametrize('count,late',[(14,False),(30,False),(14,True)])
def test_all_natural_originals_revalidate_before_cas_and_timeout_is_explicit(tmp_path,monkeypatch,count,late):
    settings,clock,store,batch,staged=setup(tmp_path,monkeypatch,count=count)
    from liangjian_funnel.data.cache import MinuteBarStore
    from liangjian_funnel.data.mootdx import MinuteBar
    from liangjian_funnel.runtime.history_freshness import session_grid
    from datetime import date
    MinuteBarStore(settings.minute_cache_dir).write([MinuteBar(symbol=row['symbol'],interval='5m',bar_end=end,
        open=10,high=11,low=9,close=10,volume=1000,amount=10000,source_id='CONTROLLED_NATIVE_PM_FIXTURE')
        for row in batch if row['plan_id']!='p1' for end in session_grid(date(2026,10,16))])
    barrier=threading.Barrier((count+11)//12);seen=[]
    def transport(request):
        wire=json.loads(request.read());runtime=json.loads(wire['messages'][-1]['content'].removeprefix('RUNTIME_INPUT\n'))
        envelope=runtime['required_envelope'];seen.append(envelope['stage_id'])
        if envelope['stage_id']=='A6':
            raw=risk_output();raw['envelope_adjustments']=[dict(raw['envelope_adjustments'][0],plan_id=row['plan_id'])
                for row in runtime['values']['PLANS']] if 'values' in runtime else []
            # The actual request has named replacements under the runtime input.
            if not raw['envelope_adjustments']:
                raw['envelope_adjustments']=[dict(risk_output()['envelope_adjustments'][0],plan_id=f'p{i}') for i in range(1,count+1)]
            raw['utilization_cap']['cap_pct']=65
            raw['capacity_for_tomorrow'].update(plans_acceptable=2,cluster_quota={'bank':2})
        else:
            refs=sorted(set(re.findall(r'MORNING:PLAN:p\d+',str(wire))),key=lambda value:int(value.rsplit('p',1)[1]))
            assert refs and len(refs)<=12
            barrier.wait(timeout=5)
            if late and 'MORNING:PLAN:p13' in refs:raise httpx.ReadTimeout('CONTROLLED_PM_FIRST_PACKET_TIMEOUT',request=request)
            raw=pm_output();raw['plan_decisions']=[dict(raw['plan_decisions'][0],plan_ref=ref,size_tier='PROBE') for ref in refs]
            raw['deployment_target'].update(target_pct=60,baseline_range=[40,65])
        raw['envelope']=envelope
        text=json.dumps(raw).replace('"C1"','"MORNING:ACCOUNT"').replace('"M1"','"MORNING:MARKET"')
        text=text.replace('"A"','"MORNING:ACCOUNT"').replace('"M"','"MORNING:MARKET"')
        return sse_model_response(200,json={'choices':[{'message':{'content':text}}]},request=request)
    client=OpenAICompatibleModelClient(settings,transport=httpx.MockTransport(transport),max_attempts=1)
    run_morning_stage(settings,'engine',clock=lambda:clock.now,calendar=CAL)
    advance(clock,8,35)
    risk=run_morning_stage(settings,'risk',clock=lambda:clock.now,calendar=CAL,client=client)
    with closing(sqlite3.connect(settings.root/'state/team_v2/model.db')) as db:
        issues=db.execute("select issues_json from model_calls where role='A6'").fetchall()
    assert risk['status']=='READY',(risk,issues)
    result=run_morning_stage(settings,'pm',clock=lambda:clock.now,calendar=CAL,client=client)
    assert result.get('model_validated') is True,json.dumps(result)
    assert result['natural_batch_switch']['scope_id']==staged['scope_id']
    assert read_natural_candidate(settings,day=clock.now.date().isoformat())['state']=='SWITCHED'
    assert len(result['batch_audit']['reviewed_plan_ids'])==count-(2 if late else 0)
    assert len(result['batch_audit']['unreviewed_plan_ids'])==(2 if late else 0)
    assert seen.count('A6')==1 and seen.count('A3')==(count+11)//12
    assert all(row['status']=='PENDING_MORNING_REVIEW' for row in store.list_execution_plans() if row['plan_id'].startswith('p'))


@pytest.mark.parametrize('deadline,expected',[
    ('2026-10-27T08:45:00+08:00',True),('2026-10-27T00:45:00+00:00',True),
    ('2026-10-27T08:45:00.000001+08:00',False),('2026-10-27T08:45:59+08:00',False),
    ('2026-10-27T08:45:00+00:00',False),('2026-10-26T08:45:00+08:00',False),
    ('2026-10-27T08:45:00',False)])
def test_pm_evidence_deadline_is_exact_bjt_instant(deadline,expected):
    from datetime import datetime
    from liangjian_funnel.runtime.reviewed_batch_evidence import _pm_deadline_in_session
    assert _pm_deadline_in_session(datetime.fromisoformat(deadline),'2026-10-27') is expected
