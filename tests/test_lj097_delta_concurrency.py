"""Real native HiThink request path, bounded workers and durable cache resume."""
from datetime import datetime
from zoneinfo import ZoneInfo
from threading import Lock
import time
import httpx
import pytest
from liangjian_funnel.settings import Settings
from liangjian_funnel.pipeline.data_source import HithinkClient
from liangjian_funnel.pipeline.data_sync import HithinkIncrementalSynchronizer
from liangjian_funnel.pipeline.local_fact_cache import LocalFactCache

NOW=datetime(2026,10,29,17,tzinfo=ZoneInfo('Asia/Shanghai'))

def test_real_hithink_concurrent_delta_persists_and_resume_has_zero_dispatch(tmp_path):
    settings=Settings.from_env({'HITHINK_FINANCE_API_KEY':'CONTROLLED_NOT_REAL',
        'ASTOCK_HITHINK_MIN_REQUEST_INTERVAL_SECONDS':'0'},root=tmp_path)
    lock=Lock();state={'active':0,'peak':0};seen=[]
    def handler(request):
        assert request.url.path.endswith('/prices/historical')
        with lock:
            state['active']+=1;state['peak']=max(state['peak'],state['active']);seen.append(str(request.url))
        try:
            time.sleep(.04)
            return httpx.Response(200,json={'code':0,'data':{'item':[{
                'date_ms':int(NOW.replace(hour=15).timestamp()*1000),'open_price':10,
                'high_price':11,'low_price':9,'close_price':10.5,'volume':1000,'turnover':10000}]}},request=request)
        finally:
            with lock:state['active']-=1
    sync=HithinkIncrementalSynchronizer(LocalFactCache(tmp_path/'facts.db'))
    symbols=[f'{n:06d}.SZ'for n in range(1,13)]
    with HithinkClient(settings,transport=httpx.MockTransport(handler))as client:
        result=sync.prime_latest_closed_days(client,symbols,as_of=NOW,deadline=time.monotonic()+30,workers=4)
        assert len(seen)==12 and 1<state['peak']<=4
        assert [r['symbol']for r in result['records']]==symbols
        assert all(r['status']=='AVAILABLE_LATEST_ONLY' for r in result['records'])
        resumed=sync.prime_latest_closed_days(client,symbols,as_of=NOW,deadline=time.monotonic()+30,workers=4)
        assert len(seen)==12
        assert all(r['status']=='CACHE_LATEST_PRESENT'for r in resumed['records'])
        expired=sync.prime_latest_closed_days(client,symbols,as_of=NOW,deadline=time.monotonic()-1,workers=4)
        assert len(seen)==12 and all(r['reason_code']=='DATA_STAGE_BUDGET_EXHAUSTED'for r in expired['records'])

@pytest.mark.parametrize('workers',[0,9,True,1.5])
def test_worker_bound_is_strict_before_any_client_call(tmp_path,workers):
    sync=HithinkIncrementalSynchronizer(LocalFactCache(tmp_path/'facts.db'))
    with pytest.raises(ValueError,match='LATEST_DAILY_WORKERS_INVALID'):
        sync.prime_latest_closed_days(object(),[],as_of=NOW,deadline=time.monotonic()+1,workers=workers)
