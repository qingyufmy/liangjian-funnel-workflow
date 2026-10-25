from datetime import datetime,timedelta
import json
import httpx
import pytest
from liangjian_funnel.runtime.company_research_budget import CompanyResearchBudget,CompanyBudgetStop

NOW=datetime.fromisoformat('2026-10-25T09:00:00+08:00')


def guard(tmp_path,handler=None):
    binding=dict(approved=True,deadline='2026-10-25T18:00:00+08:00',model='original-pro',
        chat_url='https://example.invalid/chat/completions',caps=dict(A1=4,A2_A3=6,A5=2,TOTAL=12))
    sleeps=[]
    budget=CompanyResearchBudget(tmp_path,binding=binding,now=lambda:NOW,sleep=sleeps.append,
        transport=httpx.MockTransport(handler or (lambda request:httpx.Response(200,content=b'data',request=request))))
    return budget,sleeps


def dispatch(budget,role):
    with budget.phase(role),budget.logical_call():
        with httpx.Client(transport=budget) as client:
            return client.post(budget.binding['chat_url'],json={'model':'original-pro','messages':[]})


def test_role_group_caps_count_actual_http_and_survive_restart(tmp_path):
    budget,_=guard(tmp_path)
    for _ in range(4):assert dispatch(budget,'A1').status_code==200
    restored,_=guard(tmp_path)
    with pytest.raises(CompanyBudgetStop,match='CAP_REACHED'):dispatch(restored,'A1')
    assert len(restored.state['dispatches'])==4
    assert (tmp_path/'response-001.bin').read_bytes()==b'data'
    assert json.loads((tmp_path/'request-001.json').read_bytes())['model']=='original-pro'


def test_a2_a3_share_cap_not_separate_resets(tmp_path):
    budget,_=guard(tmp_path)
    for role in ['A2']*3+['A3']*3:dispatch(budget,role)
    with pytest.raises(CompanyBudgetStop,match='CAP_REACHED'):dispatch(budget,'A3')


@pytest.mark.parametrize('status',[403,500,503])
def test_error_response_retained_then_no_further_dispatch_or_backoff(tmp_path,status):
    budget,sleeps=guard(tmp_path,lambda r:httpx.Response(status,content=b'original error',request=r))
    assert dispatch(budget,'A1').content==b'original error'
    with pytest.raises(CompanyBudgetStop):budget.backoff_sleep(1)
    with pytest.raises(CompanyBudgetStop):dispatch(budget,'A2')
    assert len(budget.state['dispatches'])==1 and sleeps==[]


def test_429_exact_backoff_and_per_logical_call_guard(tmp_path):
    budget,sleeps=guard(tmp_path,lambda r:httpx.Response(429,request=r))
    # A2 cap permits five attempts for one logical call; sixth is forbidden.
    with budget.phase('A2'),budget.logical_call():
        with httpx.Client(transport=budget) as client:
            for attempt in range(5):
                client.post(budget.binding['chat_url'],json={'model':'original-pro'})
                if attempt<4:budget.backoff_sleep(.1)
            with pytest.raises(CompanyBudgetStop,match='PER_CALL'):client.post(budget.binding['chat_url'],json={'model':'original-pro'})
    assert sleeps==[60,120,240,480]


def test_twelfth_429_stops_without_extra_wait_or_dispatch(tmp_path):
    budget,sleeps=guard(tmp_path,lambda r:httpx.Response(429,request=r))
    for role in ['A1']*4+['A2']*3+['A3']*3+['A5']*2:dispatch(budget,role)
    assert budget.state['terminal_stop']=='COMPANY_TWELFTH_429_STOP'
    with pytest.raises(CompanyBudgetStop,match='TWELFTH'):budget.backoff_sleep(480)
    assert sleeps==[] and len(budget.state['dispatches'])==12


def test_missing_phase_changed_model_and_deadline_are_pre_dispatch(tmp_path):
    budget,_=guard(tmp_path)
    with pytest.raises(CompanyBudgetStop,match='UNSCOPED'):
        httpx.Client(transport=budget).post(budget.binding['chat_url'],json={'model':'original-pro'})
    assert not budget.state['dispatches']
    budget,_=guard(tmp_path/'second')
    with budget.phase('A1'),budget.logical_call():
        with pytest.raises(CompanyBudgetStop,match='PROVIDER_CHANGED'):
            httpx.Client(transport=budget).post(budget.binding['chat_url'],json={'model':'replacement'})
    budget,_=guard(tmp_path/'third');budget.now=lambda:datetime.fromisoformat(budget.binding['deadline'])
    with pytest.raises(CompanyBudgetStop,match='DEADLINE'):dispatch(budget,'A1')
    assert not budget.state['dispatches']


def test_unknown_prior_inflight_is_not_retried(tmp_path):
    budget,_=guard(tmp_path);budget.state['dispatches']=[dict(status=None)];budget._save()
    with pytest.raises(CompanyBudgetStop,match='UNKNOWN_PRIOR'):guard(tmp_path)


def test_interrupted_process_after_headers_cannot_repeat_the_request(tmp_path):
    budget,_=guard(tmp_path);budget.state['dispatches']=[dict(status=200)];budget._save()
    with pytest.raises(CompanyBudgetStop,match='UNKNOWN_PRIOR'):guard(tmp_path)


def test_borrowed_http_client_close_does_not_close_transport_between_roles(tmp_path):
    class Pooled(httpx.BaseTransport):
        closed=False
        def handle_request(self,request):
            assert not self.closed
            return httpx.Response(200,content=b'original',request=request)
        def close(self):self.closed=True
    budget,_=guard(tmp_path);pool=Pooled();budget.transport=pool
    dispatch(budget,'A1');dispatch(budget,'A2')
    assert not pool.closed
    budget.finish();assert pool.closed


def test_midstream_error_is_terminal_and_response_hash_records_read_bytes(tmp_path):
    class Broken(httpx.SyncByteStream):
        def __iter__(self):
            yield b'first';raise httpx.ReadError('stream broke')
    budget,_=guard(tmp_path,lambda r:httpx.Response(200,stream=Broken(),request=r))
    with pytest.raises(CompanyBudgetStop):dispatch(budget,'A1')
    assert budget.state['terminal_stop']=='COMPANY_STREAM_INTERRUPTED'
    assert budget.state['dispatches'][0]['response_bytes']==5


def test_actual_authority_amendment_keeps_original_binding_counts_and_total_cap(tmp_path):
    budget,_=guard(tmp_path);budget.binding['approval_sha256']='a'*64;budget.state['binding']=dict(budget.binding);budget._save()
    for _ in range(4):dispatch(budget,'A1')
    original=dict(budget.binding)
    path=tmp_path/'approval_amendment.json'
    path.write_text(json.dumps(dict(schema='company-research-budget-amendment/1',approved=True,
        source='ACTUAL_HUMAN_REPLY_IN_THIS_THREAD',human_answer='同意调整A1子额度，总量仍12次（推荐）',
        request='ROBOT_COMPANY_A1_BUDGET_REALLOCATION_REQUEST_20261025.json',
        original_approval_sha256='a'*64,deadline=original['deadline'],symbols=['003021.SZ','300296.SZ'],
        caps=dict(A1=8,A2_A3=6,A5=2,TOTAL=12))),encoding='utf-8')
    amended=CompanyResearchBudget(tmp_path,binding=original,now=lambda:NOW,amendment_path=path,
        transport=httpx.MockTransport(lambda r:httpx.Response(200,content=b'original',request=r)))
    assert amended.state['binding']==original and len(amended.state['dispatches'])==4
    for _ in range(4):dispatch(amended,'A1')
    for role in ['A2','A2','A5','A5']:dispatch(amended,role)
    with pytest.raises(CompanyBudgetStop,match='CAP_REACHED'):dispatch(amended,'A2')
    assert len(amended.state['dispatches'])==12 and amended.effective_caps['TOTAL']==12


def test_amendment_without_actual_reply_cannot_increase_a1_subcap(tmp_path):
    budget,_=guard(tmp_path);p=tmp_path/'pending.json';p.write_text(json.dumps({'approved':False}),encoding='utf-8')
    with pytest.raises(ValueError,match='EXPLICIT_SCOPE'):
        CompanyResearchBudget(tmp_path,binding=budget.binding,now=lambda:NOW,amendment_path=p)


@pytest.mark.parametrize('reason,terminal',[
    ('MODEL_STREAM_TELEMETRY_PERSIST_FAILED',False),
    ('STREAM_DONE_MISSING',True),
    ('FINPOINTS_STREAM_REQUIRED',True),
    ('MODEL_WALL_CLOCK_TIMEOUT',True),
])
def test_client_failure_preserves_reason_and_does_not_mislabel_local_telemetry(tmp_path,reason,terminal):
    from liangjian_funnel.pipeline.model_client import ModelClientError
    from liangjian_funnel.runtime.company_research_budget import BudgetedCompanyModelClient
    budget,_=guard(tmp_path)
    class FailedClient:
        def complete(self,*args,**kwargs):raise ModelClientError(reason)
    client=BudgetedCompanyModelClient(FailedClient(),budget)
    with budget.phase('A2'),pytest.raises(CompanyBudgetStop if terminal else ModelClientError):
        client.complete('original-pro',[])
    assert bool(budget.state['terminal_stop']) is terminal
    assert budget.state['client_failures'][0]['reason_code']==reason
    assert not budget.state['dispatches']
