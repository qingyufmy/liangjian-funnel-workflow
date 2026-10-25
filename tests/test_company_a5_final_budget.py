"""The thirteenth request is A5-only and needs a real renewed time window."""
from copy import deepcopy
from datetime import datetime
import hashlib,json
import pytest
from test_company_a5_budget_reallocation import original
from test_company_research_budget import dispatch
from liangjian_funnel.runtime.company_research_budget import CompanyResearchBudget,CompanyBudgetStop

def final_scope(tmp_path):
    new,old,first=original(tmp_path);p=tmp_path/'first.json';p.write_text(json.dumps(first),encoding='utf-8')
    budget=new(amendment_path=p);dispatch(budget,'A2');dispatch(budget,'A5')
    value=dict(schema='company-research-budget-amendment/1',approved=True,source='ACTUAL_HUMAN_REPLY_IN_THIS_THREAD',
        human_answer='仅新增1次A5，总13，A5最多4，不重跑A1/A2',request='COMPANY_A5_FINAL_SEMANTIC_REQUEST_20261025.json',
        original_approval_sha256='a'*64,original_inputs_sha256='b'*64,original_deadline=first['deadline'],deadline=first['deadline'],
        caps=dict(A1=4,A2_A3=6,A5=4,TOTAL=13),symbols=['003021.SZ','300296.SZ'],dispatches_already_used=12,
        exact_role='A5',additional_http=1,no_retry=True,request_sha256='c'*64,
        parent_amendment_sha256=hashlib.sha256(p.read_bytes()).hexdigest(),parent_ledger_sha256=hashlib.sha256(budget.path.read_bytes()).hexdigest())
    return new,budget,value

def test_thirteenth_a5_inherits_all_twelve_and_survives_restart(tmp_path):
    new,old,v=final_scope(tmp_path);original=deepcopy(old.state);p=tmp_path/'final.json';p.write_text(json.dumps(v),encoding='utf-8')
    budget=new(amendment_path=p)
    assert budget.state['dispatches']==original['dispatches'] and budget.state['binding']==original['binding']
    assert len(budget.state['budget_amendments'])==2 and budget.effective_caps['TOTAL']==13
    with pytest.raises(ValueError,match='A5_ONLY'):dispatch(budget,'A2')
    dispatch(budget,'A5');resumed=new()
    assert len(resumed.state['dispatches'])==13 and resumed.only_role=='A5'
    with pytest.raises(CompanyBudgetStop,match='CAP_REACHED'):dispatch(resumed,'A5')
    assert len(resumed.state['dispatches'])==13

def test_after_18_approval_for_13_does_not_implicitly_extend_deadline(tmp_path):
    _,old,v=final_scope(tmp_path);p=tmp_path/'final.json';p.write_text(json.dumps(v),encoding='utf-8')
    budget=CompanyResearchBudget(tmp_path,binding=old.binding,amendment_path=p,now=lambda:datetime.fromisoformat('2026-10-25T19:00:00+08:00'))
    with pytest.raises(CompanyBudgetStop,match='DEADLINE'):dispatch(budget,'A5')
    assert len(budget.state['dispatches'])==12

@pytest.mark.parametrize('approved,answer,accepted',[(False,'',False),(True,'',False),(True,'仅延长这1次A5至21:00',True)])
def test_new_window_needs_explicit_actual_extension_reply(tmp_path,approved,answer,accepted):
    _,old,v=final_scope(tmp_path);v.update(deadline='2026-10-25T21:00:00+08:00',deadline_extension_approved=approved,deadline_human_answer=answer)
    p=tmp_path/'final.json';p.write_text(json.dumps(v),encoding='utf-8')
    if not accepted:
        with pytest.raises(ValueError,match='EXPLICIT_SCOPE'):CompanyResearchBudget(tmp_path,binding=old.binding,amendment_path=p)
    else:
        budget=CompanyResearchBudget(tmp_path,binding=old.binding,amendment_path=p,now=lambda:datetime.fromisoformat('2026-10-25T19:00:00+08:00'))
        assert budget.deadline.isoformat()=='2026-10-25T21:00:00+08:00' and budget.only_role=='A5'

@pytest.mark.parametrize('fault',['parent','amendment','additional','role','total','prior_status'])
def test_final_extra_call_cannot_rebind_history_or_expand_scope(tmp_path,fault):
    new,old,v=final_scope(tmp_path)
    if fault=='parent':v['parent_ledger_sha256']='d'*64
    elif fault=='amendment':v['parent_amendment_sha256']='d'*64
    elif fault=='additional':v['additional_http']=2
    elif fault=='role':v['exact_role']='A2'
    elif fault=='total':v['caps']['TOTAL']=14
    else:old.state['dispatches'][-1]['status']=429;old._save();v['parent_ledger_sha256']=hashlib.sha256(old.path.read_bytes()).hexdigest()
    p=tmp_path/'bad.json';p.write_text(json.dumps(v),encoding='utf-8')
    with pytest.raises(ValueError):new(amendment_path=p)

def test_final_no_retry_does_not_wait_after_the_only_new_request(tmp_path):
    new,old,v=final_scope(tmp_path);p=tmp_path/'final.json';p.write_text(json.dumps(v),encoding='utf-8')
    budget=new(amendment_path=p);dispatch(budget,'A5')
    with pytest.raises(CompanyBudgetStop,match='NO_RETRY'):budget.backoff_sleep(60)
    assert len(budget.state['dispatches'])==13
