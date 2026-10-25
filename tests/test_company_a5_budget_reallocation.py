from datetime import datetime
import json
import httpx,pytest
from liangjian_funnel.runtime.company_research_budget import CompanyResearchBudget,CompanyBudgetStop
from test_company_research_budget import dispatch,NOW

def original(tmp_path):
    binding=dict(approved=True,deadline='2026-10-25T18:00:00+08:00',model='original-pro',
        chat_url='https://example.invalid/chat/completions',caps=dict(A1=4,A2_A3=6,A5=2,TOTAL=12),approval_sha256='a'*64,inputs_sha256='b'*64)
    new=lambda **kw:CompanyResearchBudget(tmp_path,binding=binding,now=lambda:NOW,transport=httpx.MockTransport(lambda r:httpx.Response(200,content=b'original',request=r)),**kw)
    budget=new()
    for role in ['A1']*4+['A2']*4+['A5']*2:dispatch(budget,role)
    value=dict(schema='company-research-budget-amendment/1',approved=True,source='ACTUAL_HUMAN_REPLY_IN_THIS_THREAD',
        human_answer='批准按剩余两次分配继续（推荐）',request='COMPANY_CURRENT_AUDIT_REALLOCATION_REQUEST_20261025.json',
        original_approval_sha256='a'*64,original_inputs_sha256='b'*64,deadline=binding['deadline'],
        caps=dict(A1=4,A2_A3=6,A5=3,TOTAL=12),symbols=['003021.SZ','300296.SZ'],dispatches_already_used=10)
    return new,budget,value

def test_a5_subcap_amendment_preserves_ten_calls_and_global_twelve_limit(tmp_path):
    new,old,value=original(tmp_path);before=old.state['dispatches'].copy()
    path=tmp_path/'actual-human-amendment.json';path.write_text(json.dumps(value),encoding='utf-8')
    budget=new(amendment_path=path)
    assert budget.state['dispatches']==before and budget.effective_caps['A5']==3
    dispatch(budget,'A2');dispatch(budget,'A5')
    resumed=new();assert len(resumed.state['dispatches'])==12 and resumed.effective_caps['A5']==3
    with pytest.raises(CompanyBudgetStop,match='CAP_REACHED'):dispatch(resumed,'A5')
    assert len(resumed.state['dispatches'])==12

@pytest.mark.parametrize('fault',['answer','input','total','a1','used'])
def test_new_amendment_cannot_add_total_or_rebind_approval(tmp_path,fault):
    new,old,value=original(tmp_path)
    if fault=='answer':value['human_answer']=''
    if fault=='input':value['original_inputs_sha256']='c'*64
    if fault=='total':value['caps']['TOTAL']=13
    if fault=='a1':value['caps']['A1']=5
    if fault=='used':value['dispatches_already_used']=0
    path=tmp_path/'bad.json';path.write_text(json.dumps(value),encoding='utf-8')
    with pytest.raises(ValueError,match='EXPLICIT_SCOPE'):new(amendment_path=path)
