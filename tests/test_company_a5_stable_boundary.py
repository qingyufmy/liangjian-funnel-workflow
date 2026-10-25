from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import json,sqlite3
import pytest
from liangjian_funnel.pipeline.company_audit_policy import company_a5_source_policy,current_company_audit_view
from liangjian_funnel.pipeline.company_research_ledger import CompanyResearchLedger,read_company_tracking
from liangjian_funnel.runtime.research_feature_policy import RESEARCH_FLAGS,require_research_feature
from liangjian_funnel.pipeline.feature_store import content_hash


def prepared(tmp_path):
    value=json.loads((Path(__file__).parent/'fixtures/company_a5_original_20261025.json').read_bytes())
    raw=value['output'];context=value['legacy_context'];context['symbols']=set(context['symbols'])
    context.update(company_a5_source_policy(context['current_bindings']));context['request']=SimpleNamespace(input_hash='controlled-input')
    for r in raw['company_reviews']:
        r['realized_vs_unrealized']='机器人细分收入贡献尚未独立核实，不能认定为零或未实现'
        r['realization_fact']=deepcopy(context['fact_boundaries'][r['symbol']])
    raw['followup_actions']=[deepcopy(a) for a in context['followup_catalog'].values() if a['owner']=='A1']
    ledger=CompanyResearchLedger(tmp_path/'company.sqlite3')
    with sqlite3.connect(ledger.path) as db:
        for symbol,b in context['current_bindings'].items():
            db.execute('INSERT INTO company_reviews VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                (b['a1_review_id'],'controlled-a1',symbol,raw['observed_at'],raw['observed_at'],'i','o','{}','{}',content_hash({}),'CONTROLLED_TEST'))
    return ledger,raw,context


def test_invalid_original_is_advisory_only_and_no_downstream_view_reads_it(tmp_path):
    ledger,raw,context=prepared(tmp_path);raw['followup_actions'][0]['trigger']='两日净流入后才允许确认'
    with pytest.raises(ValueError,match='VALIDATED_CONTEXT_REQUIRED'):
        ledger.record_a5_audit(call_id='bad',input_hash='controlled-input',observed_at=raw['observed_at'],output=raw,context=context)
    ledger.record_a5_advisory(call_id='bad',input_hash='controlled-input',observed_at=raw['observed_at'],output=raw)
    with sqlite3.connect(ledger.path) as db:
        assert db.execute('SELECT count(*) FROM company_chain_audits').fetchone()[0]==0
        assert db.execute('SELECT count(*) FROM company_followup_actions').fetchone()[0]==0
    tracking=read_company_tracking(ledger.path,observed_at=raw['observed_at'])
    assert '两日净流入' not in json.dumps(tracking,ensure_ascii=False)
    assert current_company_audit_view(tracking,symbols=context['symbols'],observed_at=raw['observed_at'])['historical_registry']['audit_refs']==[]


def test_admitted_fact_row_contains_no_model_prose_or_new_condition(tmp_path):
    ledger,raw,context=prepared(tmp_path)
    ledger.record_a5_audit(call_id='valid',input_hash='controlled-input',observed_at=raw['observed_at'],output=raw,context=context)
    with sqlite3.connect(ledger.path) as db:
        stored=json.loads(db.execute('SELECT output_json FROM company_chain_audits').fetchone()[0])
    assert stored['schema']=='company-chain-facts/1'
    assert all(set(r)=={'symbol','current_state','realization_fact'} for r in stored['company_reviews'])
    assert all(set(a)=={'action_id','symbol','owner'} for a in stored['followup_actions'])
    legacy=deepcopy(context);legacy.pop('fact_boundaries')
    with pytest.raises(ValueError,match='VALIDATED_CONTEXT_REQUIRED'):
        ledger.record_a5_audit(call_id='legacy',input_hash='controlled-input',observed_at=raw['observed_at'],output=raw,context=legacy)


def test_research_semantics_are_off_without_runtime_enable_path():
    assert not any(RESEARCH_FLAGS.values())
    for flag in RESEARCH_FLAGS:
        with pytest.raises(ValueError,match='DISABLED_PENDING_QUALITY_GATE'):require_research_feature(flag)
