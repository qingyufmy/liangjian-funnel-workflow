"""0950 real research/A6/card originals, controlled local ledgers, no HTTP."""
from contextlib import closing
from copy import deepcopy
from datetime import datetime
import hashlib
import json
from pathlib import Path
import socket
import sqlite3

import pytest

from liangjian_funnel.review.research_execution import research_execution_summary
from liangjian_funnel.review.key_counts import freeze_key_counts, reconcile_counts
from liangjian_funnel.runtime.state import RuntimeStore

FIXTURE=Path(__file__).parent/'fixtures/postclose_20261027/a5_midday_1028_boundary_original.json'
NOW=datetime.fromisoformat('2026-10-28T11:30:00+08:00')


def local_originals(tmp_path):
    original=json.loads(FIXTURE.read_text(encoding='utf8'))
    row=original['candidate']['candidate'];head=original['candidate']['head'][0]
    path=tmp_path/'state/workflow.sqlite3';store=RuntimeStore(path)
    with closing(sqlite3.connect(path)) as db:
        cols=['scope_id','account_id','trade_date','lane_id','run_id','body_sha256','body_json','created_at','state']
        db.execute('INSERT INTO natural_review_candidates VALUES (?,?,?,?,?,?,?,?,?)', [row[c] for c in cols])
        db.execute('INSERT INTO natural_review_candidate_head VALUES (?,?,?,?,?,?)',
            [head[c] for c in ['account_id','trade_date','lane_id','scope_id','body_sha256','revision']])
        db.commit()
    model=path.parent/'team_v2/model.db';model.parent.mkdir()
    call=original['calls'][0]
    raw=json.dumps(call['output'],ensure_ascii=False,sort_keys=True,separators=(',',':'))
    assert hashlib.sha256(raw.encode()).hexdigest()==call['output_hash']
    with closing(sqlite3.connect(model)) as db:
        db.execute('CREATE TABLE model_calls(call_id TEXT,output_json TEXT,output_hash TEXT,validated INTEGER,issues_json TEXT,captured_at TEXT)')
        db.execute('INSERT INTO model_calls VALUES (?,?,?,?,?,?)',
            [call['call_id'],raw,call['output_hash'],call['validated'],call['issues_json'],call['captured_at']])
        db.commit()
    return store,model,original


def summary(store,**kwargs):
    return research_execution_summary(store.path,day='2026-10-28',lane='lane_1',cutoff=NOW,**kwargs)


def test_real_research7_invalid_a6_zero_execution_read_only(tmp_path,monkeypatch):
    store,model,original=local_originals(tmp_path)
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('NO_NETWORK'))
    before=[hashlib.sha256(p.read_bytes()).hexdigest() for p in [store.path,model]]
    value=summary(store)
    assert value['status']=='AVAILABLE'
    assert (value['research_candidate_count'],value['published_execution_count'])==(7,0)
    assert value['body_sha256']==original['candidate']['candidate']['body_sha256']
    assert value['a6']['status']=='VALIDATION_FAILED'
    assert value['a6']['output_sha256']==original['calls'][0]['output_hash']
    assert 'A6_CAPACITY_INVALID' in value['a6']['issues']
    assert before==[hashlib.sha256(p.read_bytes()).hexdigest() for p in [store.path,model]]


@pytest.mark.parametrize('mutation',['body','head','day','call_hash'])
def test_conflicting_original_never_becomes_zero(tmp_path,mutation):
    store,model,_=local_originals(tmp_path)
    with closing(sqlite3.connect(model if mutation=='call_hash' else store.path)) as db:
        sql={'body':"UPDATE natural_review_candidates SET body_json='{}'",
             'head':"UPDATE natural_review_candidate_head SET body_sha256='wrong'",
             'day':"UPDATE natural_review_candidates SET trade_date='2026-10-29'",
             'call_hash':"UPDATE model_calls SET output_hash='wrong'"}[mutation]
        db.execute(sql);db.commit()
    value=summary(store)
    assert value['status']=='NOT_AVAILABLE' and 'research_candidate_count' not in value


@pytest.mark.parametrize('condition',['absent','after_cutoff','different_scope'])
def test_execution_zero_cannot_establish_a6_failure(tmp_path,condition):
    store,model,_=local_originals(tmp_path)
    with closing(sqlite3.connect(model)) as db:
        db.execute({'absent':'DELETE FROM model_calls',
            'after_cutoff':"UPDATE model_calls SET captured_at='2026-10-28T11:31:00+08:00'",
            'different_scope':"UPDATE model_calls SET call_id='other:A6'"}[condition]);db.commit()
    value=summary(store)
    assert value['research_candidate_count']==7 and value['published_execution_count']==0
    assert value['a6']['status']=='UNKNOWN'


def test_candidate_after_cutoff_unavailable(tmp_path):
    store,_,_=local_originals(tmp_path)
    with closing(sqlite3.connect(store.path)) as db:
        db.execute("UPDATE natural_review_candidates SET created_at='2026-10-28T11:31:00+08:00'");db.commit()
    assert summary(store)['status']=='NOT_AVAILABLE'


def test_research_count_independently_reconciles_and_rejects_mismatch(tmp_path):
    store,_,_=local_originals(tmp_path)
    facts=dict(trade_date='2026-10-28',cutoff_at=NOW.isoformat(),lane_id='lane_1',a3={'plans':[]},
        metrics=dict(a3_plan_count=0,a4_monitor_observation_count=0,a4_trade_signal_count=0,
                     research_execution_summary=summary(store)))
    proof=freeze_key_counts(facts,store=store,root=tmp_path,observed_at=NOW)
    assert proof['actual']['research_candidates']==7 and proof['actual']['published_research_plans']==0
    changed=deepcopy(proof['actual']);changed['research_candidates']=0
    with pytest.raises(ValueError,match='A5_KEY_COUNT_LEDGER_MISMATCH'):
        reconcile_counts(facts,changed,source_sha256='controlled',observed_at=NOW.isoformat())


def test_native_fact_builder_and_guard_keep_research_separate(tmp_path):
    from liangjian_funnel.review.daily import build_a5_fact_snapshot,A5ReviewKind,A5ReviewReport
    from liangjian_funnel.review.fact_guard import reconcile_report
    from test_a5_daily_review import _report
    store,_,_=local_originals(tmp_path)
    facts=build_a5_fact_snapshot(store,tmp_path,trade_date=NOW.date(),cutoff_at=NOW,
        review_kind=A5ReviewKind.MIDDAY,lane_id='lane_1')
    assert facts['metrics']['research_execution_summary']['research_candidate_count']==7
    assert facts['metrics']['a3_plan_count']==0
    value=_report();value['trade_date']='2026-10-28'
    report=A5ReviewReport.model_validate(value)
    reconcile_report(report,facts)
    assert '研究候选7只' in report.executive_summary and '已发布执行计划0只' in report.a3_review.summary
    assert '晨会A6原回复校验失败' in report.a3_review.summary


def test_real_midday_card_before_and_source_bound_customer_after(tmp_path,monkeypatch):
    from test_d8_contract_0881 import make_publisher,render,OPS
    from test_d8_real_outputs_0837 import text,canonical
    store,_,original=local_originals(tmp_path/'originals')
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('NO_NETWORK'))
    before=original['customer_card']['card']
    assert hashlib.sha256(canonical(before)).hexdigest()==original['customer_card']['card_sha256']
    assert text(before).count('未识别的说明')==3 and 'A2候选数为0' in text(before)
    facts=dict(trade_date='2026-10-28',cutoff_at=NOW.isoformat(),lane_id='lane_1',a3={'plans':[]},
        metrics=dict(a3_plan_count=0,a4_monitor_observation_count=0,a4_trade_signal_count=0,
            a2_availability='NOT_AVAILABLE',research_execution_summary=summary(store)))
    facts['key_count_reconciliation']=freeze_key_counts(facts,store=store,root=tmp_path/'originals',observed_at=NOW)
    publisher,customer,_=make_publisher(tmp_path/'publisher')
    publisher.publish_a5_review(dict(review_id=original['customer_card']['source_id'],trade_date='2026-10-28',
        review_kind='MIDDAY',cutoff_at=NOW.isoformat(),fact_snapshot=facts,
        report={'executive_summary':'controlled preserved raw, not a new model reply','overall_verdict':'NEEDS_ATTENTION'}),now=NOW)
    assert len(customer.cards)==1
    after=customer.cards[0];body=text(after)
    assert '研究候选 7 / 可执行 0（晨会 A6 失败）' in body
    assert not any(x in body for x in ['未识别','无候选','None','A2候选数为0'])
    assert '不可用' in body  # Controlled local ledger has no historical fills source.
    out=OPS/'D8_A5_1028_BOUNDARY_0950';out.mkdir(exist_ok=True)
    (out/'BEFORE.json').write_bytes(canonical(before));(out/'AFTER.json').write_bytes(canonical(after))
    description='Real11:35 issued card BEFORE; AFTER native renderer with actual candidate/A6 originals and controlled local ledger. Not a new historical model reply or production send.'
    render(out/'AFTER.html',after,description)
    (out/'BINDING.json').write_text(json.dumps(dict(source_fixture_sha256=hashlib.sha256(FIXTURE.read_bytes()).hexdigest(),
        before_sha256=hashlib.sha256(canonical(before)).hexdigest(),after_sha256=hashlib.sha256(canonical(after)).hexdigest(),
        classification=description,model_http=0,source_http=0,production_writes=0),indent=2),encoding='utf8')
