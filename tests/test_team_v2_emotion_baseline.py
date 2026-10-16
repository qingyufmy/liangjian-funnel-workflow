from copy import deepcopy
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
import gzip
import hashlib
import json
import time

import httpx
import pytest

from liangjian_funnel.settings import Settings
from liangjian_funnel.pipeline.data_source import HithinkClient,HithinkFetchResult,HithinkRow
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
from liangjian_funnel.runtime.team_v2_emotion_baseline import prepare_emotion_baseline,load_emotion_baseline,observe_full_market
from test_team_v2_emotion_source import row,quote,universe,NOW,CAT

CAL=ExchangeTradingCalendar()


def result(endpoint,rows,metadata):
    return HithinkFetchResult(endpoint=endpoint,items=tuple(HithinkRow.model_validate(r) for r in rows),
        ok=True,complete=True,reason_code='OK',pages=1,total=len(rows),fetch_time=CAT,metadata=metadata)


def sources():
    cats,prices,_=universe()
    cat=result('/api/meta/tickers/list',cats,{'timestamp':CAT.timestamp()*1000})
    ladder=result('/api/a-share/limit-up/ladder',[{'date':'2026-10-16','boards':{
        'two':[{'thscode':cats[0]['thscode'],'board_num':2,'seal_nextday':True}]}}],
        {'timestamp':CAT.timestamp()*1000,'window':{'date_list':['2026-10-16']}})
    return cat,ladder


def settings(tmp_path):
    return Settings.from_env({'HITHINK_FINANCE_API_KEY':'UNIT_TEST_ONLY_NOT_A_CREDENTIAL'},root=tmp_path)


def factory(cat,ladder,seen):
    class Client:
        def __init__(self,*args):pass
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def ticker_catalog(self,**kw):seen.append(('catalogue',kw));return cat
        def limit_up_ladder(self):seen.append(('ladder',{}));return ladder
    return Client


def test_morning_owned_baseline_proves_absence_zero_and_reuses_without_http(tmp_path):
    conf=settings(tmp_path);cat,ladder=sources();seen=[]
    body=prepare_emotion_baseline(conf,clock=lambda:CAT,calendar=CAL,client_factory=factory(cat,ladder,seen))
    assert body['status']=='READY' and len(seen)==2
    counts=body['previous_streaks']['counts']
    assert counts['000001.SZ']==2 and counts['000002.SZ']==0
    assert body['previous_streaks']['zero_basis']=='ABSENT_FROM_COMPLETE_DATED_LADDER'
    original=(tmp_path/'state/team_v2/emotion_baseline/2026-10-19.json').read_bytes()
    again=prepare_emotion_baseline(conf,clock=lambda:CAT+timedelta(minutes=1),calendar=CAL,
        client_factory=lambda *_:pytest.fail('must not refetch'))
    assert again==body and len(seen)==2
    assert (tmp_path/'state/team_v2/emotion_baseline/2026-10-19.json').read_bytes()==original


@pytest.mark.parametrize('defect',['old_catalogue','partial_catalogue','wrong_ladder_day','partial_ladder','missing_height','duplicate'])
def test_bad_sources_do_not_create_ready_baseline_or_make_fake_zero(tmp_path,defect):
    conf=settings(tmp_path);cat,ladder=sources();seen=[]
    if defect=='old_catalogue':cat=cat.model_copy(update={'metadata':{'timestamp':'2026-10-16T08:20:00+08:00'}})
    elif defect=='partial_catalogue':cat=cat.model_copy(update={'complete':False})
    elif defect=='wrong_ladder_day':ladder=ladder.model_copy(update={'metadata':{'window':{'date_list':['2026-10-15']}}})
    elif defect=='partial_ladder':ladder=ladder.model_copy(update={'complete':False})
    else:
        raw=ladder.items[0].model_dump(mode='json');stock=raw['boards']['two'][0]
        if defect=='missing_height':stock.pop('board_num')
        else:raw['boards']['two'].append(deepcopy(stock))
        ladder=ladder.model_copy(update={'items':(HithinkRow.model_validate(raw),)})
    body=prepare_emotion_baseline(conf,clock=lambda:CAT,calendar=CAL,client_factory=factory(cat,ladder,seen))
    assert body['status']=='DATA_LIMITED' and 'previous_streaks' not in body
    assert prepare_emotion_baseline(conf,clock=lambda:CAT,calendar=CAL,
        client_factory=lambda *_:pytest.fail('failure original retained'))==body


def test_late_catalogue_worker_does_not_start_ladder_or_publish_late_ready(tmp_path):
    conf=settings(tmp_path);cat,ladder=sources();seen=[]
    Base=factory(cat,ladder,seen)
    class Slow(Base):
        def ticker_catalog(self,**kw):time.sleep(.06);return super().ticker_catalog(**kw)
    body=prepare_emotion_baseline(conf,clock=lambda:CAT,calendar=CAL,client_factory=Slow,budget_seconds=.01)
    assert body['status']=='DATA_LIMITED'
    original=(tmp_path/'state/team_v2/emotion_baseline/2026-10-19.json').read_bytes()
    time.sleep(.09)
    assert [name for name,_ in seen]==['catalogue']
    assert (tmp_path/'state/team_v2/emotion_baseline/2026-10-19.json').read_bytes()==original


def test_same_request_full_rows_produce_four_counts_and_immutable_compressed_frame(tmp_path):
    conf=settings(tmp_path);cat,ladder=sources();seen=[]
    prepare_emotion_baseline(conf,clock=lambda:CAT,calendar=CAL,client_factory=factory(cat,ladder,seen))
    prices=[quote(r.model_dump()['thscode']) for r in cat.items]
    prices[0].update(last_price=11,high_price=11)
    full=result('/api/a-share/prices/snapshot',prices,{'timestamp':NOW.replace(second=0).timestamp()*1000})
    capture=observe_full_market(conf,full,observed_at=NOW,calendar=CAL,deadline=time.monotonic()+10)
    assert capture['status']=='READY' and capture['emotion']['highest_streak']==3
    raw=gzip.decompress((tmp_path/capture['frame_path']).read_bytes())
    assert hashlib.sha256(raw).hexdigest()==capture['frame_sha256']
    frame=json.loads(raw)
    assert frame['source']['items']==prices and len(frame['emotion']['identities'])==500
    assert frame['emotion']['authority']=='EMOTION_STATISTICS_ONLY'
    assert len(seen)==2 # observer itself performs no network calls


def test_provider_old_snapshot_not_relabelled_and_deadline_no_source_write(tmp_path):
    conf=settings(tmp_path);cat,ladder=sources();seen=[]
    prepare_emotion_baseline(conf,clock=lambda:CAT,calendar=CAL,client_factory=factory(cat,ladder,seen))
    full=result('/api/a-share/prices/snapshot',[quote(r.model_dump()['thscode']) for r in cat.items],
        {'timestamp':'2026-10-16T14:18:15+08:00'})
    limited=observe_full_market(conf,full,observed_at=NOW,calendar=CAL,deadline=time.monotonic()+10)
    assert limited['status']=='DATA_BLOCK' and limited['source_as_of'].startswith('2026-10-16')
    before=set((tmp_path/'state/team_v2/emotion_live').rglob('*.gz'))
    assert observe_full_market(conf,full,observed_at=NOW,calendar=CAL,deadline=time.monotonic()-1)['status']=='DATA_BLOCK'
    assert set((tmp_path/'state/team_v2/emotion_live').rglob('*.gz'))==before


def test_baseline_tamper_is_not_trusted_and_missing_credential_has_no_request(tmp_path):
    conf=settings(tmp_path);cat,ladder=sources();seen=[]
    prepare_emotion_baseline(conf,clock=lambda:CAT,calendar=CAL,client_factory=factory(cat,ladder,seen))
    path=tmp_path/'state/team_v2/emotion_baseline/2026-10-19.json'
    raw=json.loads(path.read_bytes());raw['previous_streaks']['counts']['000001.SZ']=99;path.write_text(json.dumps(raw))
    assert load_emotion_baseline(conf,now=NOW)['status']=='DATA_LIMITED'
    clean=Settings.from_env({},root=tmp_path/'missing')
    assert prepare_emotion_baseline(clean,clock=lambda:CAT,calendar=CAL,
        client_factory=lambda *_:pytest.fail('no credential no request'))['reason_code']=='HITHINK_API_KEY_MISSING'


def test_actual_existing_client_HTTP_contract_keeps_original_date_metadata_and_no_pool_query(tmp_path):
    conf=settings(tmp_path);cat,ladder=sources();seen=[]
    def handler(request):
        seen.append((request.url.path,dict(request.url.params)))
        original=cat if request.url.path.endswith('tickers/list') else ladder
        data={'timestamp':original.metadata['timestamp'],'item':[r.model_dump(mode='json') for r in original.items]}
        if original is cat:data['total']=len(cat.items)
        else:data['window']=original.metadata['window']
        return httpx.Response(200,json={'code':0,'data':data},request=request)
    def actual(settings):
        client=HithinkClient(settings,transport=httpx.MockTransport(handler),sleep=lambda _:None)
        client._now=lambda:CAT
        return client
    body=prepare_emotion_baseline(conf,clock=lambda:CAT,calendar=CAL,client_factory=actual)
    assert body['status']=='READY' and len(seen)==2
    assert seen[0]==('/api/meta/tickers/list',{'exchange':'SH,SZ,BJ','asset_type':'a-share','limit':'1000','offset':'0'})
    assert seen[1][1]=={} and 'ladder' in seen[1][0]
    assert 'UNIT_TEST_ONLY_NOT_A_CREDENTIAL' not in json.dumps(body)


def test_5000_row_projection_local_compute_budget_not_network_or_VM_proof(tmp_path):
    conf=settings(tmp_path);_,ladder=sources();seen=[]
    cats=[row(f'{i:06}.SZ') for i in range(1,4000)]+[row(f'{i:06}.SZ') for i in range(300001,301002)]
    cat=result('/api/meta/tickers/list',cats,{'timestamp':CAT.timestamp()*1000})
    prepare_emotion_baseline(conf,clock=lambda:CAT,calendar=CAL,client_factory=factory(cat,ladder,seen))
    prices=[quote(r['thscode']) for r in cats];prices[0].update(last_price=11,high_price=11)
    full=result('/api/a-share/prices/snapshot',prices,{'timestamp':NOW.replace(second=0).timestamp()*1000})
    started=time.monotonic()
    capture=observe_full_market(conf,full,observed_at=NOW,calendar=CAL,deadline=started+8)
    elapsed=time.monotonic()-started
    print(json.dumps({'measurement':'LOCAL_5000_ROWS_COMPUTE_AND_OWNED_COMPRESSED_SEAL_ONLY',
        'elapsed_seconds':elapsed,'status':capture['status'],'network_requests':0,
        'not_claimed':['VM_PHASE_BUDGET','PRODUCTION_LATENCY']}))
    assert capture['status']=='READY' and capture['emotion']['observed_count']==5000
