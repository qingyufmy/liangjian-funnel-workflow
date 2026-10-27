"""Actual production stream client with controlled concurrent transports."""
from contextlib import closing
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
import hashlib,json,re,threading,time
import httpx
import pytest
from fixture_model_stream import sse_model_response
from test_team_v2_pm_session import fixture,response,NOW
from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient
from liangjian_funnel.runtime.team_v2_pm_session import run_pm_premarket
from liangjian_funnel.settings import Settings


def prepared(tmp_path,count,*,timeout_batch=None):
    args,_=fixture(tmp_path);base=args['envelopes'][0]
    ids=[f'p{i}' for i in range(1,count+1)]
    args.update(envelopes=tuple(replace(base,plan_id=pid,symbol=f'{i:06d}.SZ') for i,pid in enumerate(ids,1)),
        plan_clusters={pid:f'cluster{i}' for i,pid in enumerate(ids)},free_slots=6,max_positions=6,
        geometry_plan_ids=set(ids),deadline_at=NOW.replace(minute=45),fallback_enabled=False)
    cat=args['replacements']['EVIDENCE_CATALOG']
    cat['items']=[i for i in cat['items'] if i['category']!='PLAN']+[
        dict(id='PLAN:'+pid,category='PLAN',value=dict(plan_id=pid,symbol=f'{i:06d}.SZ',cluster=f'cluster{pid}',
             publication_facts=dict(plan_priority_rank=i))) for i,pid in enumerate(ids,1)]
    # Catalog cluster binding uses the same immutable server cluster domain.
    for item in cat['items']:
        if item['category']=='PLAN':item['value']['cluster']=args['plan_clusters'][item['value']['plan_id']]
    batch_count=(count+11)//12;barrier=threading.Barrier(batch_count);seen=[];lock=threading.Lock()
    def handler(request):
        body=json.loads(request.read());refs=sorted(set(re.findall(r'PLAN:p\d+',str(body))),key=lambda s:int(s[6:]))
        assert 0<len(refs)<=12
        with lock:seen.append(dict(refs=refs,at=time.monotonic()))
        # A sequential implementation fails here before any synthetic reply.
        barrier.wait(timeout=3)
        if timeout_batch is not None and refs[0]=='PLAN:p'+str(timeout_batch*12+1):
            raise httpx.ReadTimeout('CONTROLLED_NO_FIRST_PACKET',request=request)
        out=response();out['envelope']=args['required_envelope'];out['plan_decisions']=[
            dict(plan_ref=ref,decision='ACCEPT',size_tier='STANDARD',reason_code='ACCEPT_STRUCTURE',note='冻结形态') for ref in refs]
        return sse_model_response(200,json={'choices':[{'message':{'content':json.dumps(out)}}]},request=request)
    settings=Settings.from_env({'LIANGJIAN_MODEL_API_KEY':'CONTROLLED_NOT_SECRET'},root=tmp_path)
    args['client']=OpenAICompatibleModelClient(settings,transport=httpx.MockTransport(handler),max_attempts=1)
    return args,seen


@pytest.mark.parametrize('count',[14,30])
def test_all_candidates_reviewed_in_parallel_and_original_calls_independent(tmp_path,count,monkeypatch):
    import socket
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:(_ for _ in ()).throw(AssertionError('REAL_NETWORK_FORBIDDEN')))
    args,seen=prepared(tmp_path,count)
    result=run_pm_premarket(**args)
    assert result['model_validated'],result
    audit=result['batch_audit'];assert len(audit['reviewed_plan_ids'])==count and not audit['unreviewed_plan_ids']
    assert len(seen)==(count+11)//12 and audit['parallel']
    assert max(row['at'] for row in seen)-min(row['at'] for row in seen)<2
    assert len({pid for row in seen for pid in row['refs']})==count
    assert len(result['slot_plan'])<=6
    with closing(args['journal']._connect()) as db:
        calls=db.execute('select call_id,validated from model_calls').fetchall()
    assert len(calls)==len(seen) and all(row[1] for row in calls)
    assert args['call_id'] not in {row[0] for row in calls}
    assert run_pm_premarket(**args)==result and len(seen)==len(calls)


def test_one_batch_timeout_disposes_only_its_ids_and_preserves_other_original(tmp_path):
    args,seen=prepared(tmp_path,14,timeout_batch=1)
    result=run_pm_premarket(**args)
    assert result['model_validated'],result
    assert result['review_completeness']=='PARTIAL_TIME_BUDGET'
    audit=result['batch_audit'];assert audit['unreviewed_plan_ids']==['p13','p14']
    assert audit['reviewed_plan_ids']==[f'p{i}' for i in range(1,13)]
    assert not {'p13','p14'}&{row['plan_id'] for row in result['plan_allocations']}
    with closing(args['store']._connect()) as db:
        notices=db.execute('select kind,body_json from team_notifications').fetchall()
    assert len(notices)==1 and notices[0][0]=='PM_NOT_REVIEWED_TIME_BUDGET'
    assert len(seen)==2
