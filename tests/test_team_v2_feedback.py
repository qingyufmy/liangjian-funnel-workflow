from fixture_model_stream import sse_model_response
from contextlib import closing
from copy import deepcopy
from datetime import date,datetime,time,timedelta
import hashlib
import json
import sqlite3

import pytest

from liangjian_funnel.pipeline.model_client import ModelCallResult
from liangjian_funnel.pipeline.team_v2_contract import TeamValidation
from liangjian_funnel.pipeline.team_v2_feedback import append_judgment_outcome,read_method_feedback
from liangjian_funnel.pipeline.team_v2_judgments import ModelJudgmentJournal
from liangjian_funnel.runtime.team_v2_store import canonical


class Calendar:
    # Explicit unit-test calendar only; production uses ExchangeTradingCalendar.
    def is_trading_day(self,d):return d.weekday()<5
    def next_trading_day(self,d):
        d+=timedelta(days=1)
        while not self.is_trading_day(d):d+=timedelta(days=1)
        return d
    def previous_trading_day(self,d):
        d-=timedelta(days=1)
        while not self.is_trading_day(d):d-=timedelta(days=1)
        return d


CAL=Calendar()
ASOF=datetime.fromisoformat('2026-09-25T16:00:00+08:00')
CATALOG=dict(categories=['NEWS','QUANT'],items=[dict(id='N',category='NEWS'),dict(id='Q',category='QUANT')])


def h(value):return hashlib.sha256(canonical(value).encode()).hexdigest()


def evidence(day,close='10',**changes):
    row=dict(symbol='000001.SZ',trade_date=day.isoformat(),close=close,adjust='none')
    row.update(changes)
    return dict(row=row,row_sha256=h(row),source_ref='original-daily:'+day.isoformat(),
        observed_at=day.isoformat()+'T15:30:00+08:00')


def capture(journal,day,*,with_catalog=True,valid=True,symbol='000001.SZ',ordinal=0,action=None):
    row=dict(symbol=symbol,supporting_refs=['N'],opposing_refs=['Q'])
    if action is not None:row['action']=action
    call_id=f'a2:{day.isoformat()}:{symbol}:{ordinal}'
    response=ModelCallResult('frozen-model',{'focus_decisions':[row]},'a'*64,'b'*64,5,1,'disabled')
    journal.capture(call_id=call_id,run_id='r:'+day.isoformat(),role='A2',session='CLOSE_REVIEW',
        source_trade_date=day.isoformat(),execution_trade_date=CAL.next_trading_day(day).isoformat(),
        captured_at=datetime.fromisoformat(day.isoformat()+'T17:00:00+08:00'),catalog_hash=h(CATALOG),response=response,
        validation=TeamValidation(valid,() if valid else ('INVALID',),(dict(row,path='focus_decisions[0]'),),0),
        catalog=CATALOG if with_catalog else None)
    with closing(journal._connect()) as db:
        return db.execute('SELECT decision_id FROM model_judgments WHERE call_id=?',(call_id,)).fetchone()[0]


def label(journal,decision,day,*,captured_at=ASOF):
    with closing(journal._connect()) as db:
        symbol=db.execute('SELECT symbol FROM model_judgments WHERE decision_id=?',(decision,)).fetchone()[0]
    target=day
    for n in range(1,4):
        target=CAL.next_trading_day(target)
        if n in (1,3):append_judgment_outcome(journal,decision_id=decision,horizon=n,reference=evidence(day,symbol=symbol),
            target=evidence(target,'11',symbol=symbol),captured_at=captured_at,calendar=CAL)


def mature_fixture(tmp_path,n=10,*,with_catalog=True,samples=15,symbols=5):
    journal=ModelJudgmentJournal(tmp_path/'model.db');day=date(2026,9,1);decisions=[];days=[]
    for _ in range(n):days.append(day);day=CAL.next_trading_day(day)
    for i in range(samples):
        day=days[i%n]
        decision=capture(journal,day,with_catalog=with_catalog,symbol=f'{1+i%symbols:06d}.SZ',ordinal=i)
        label(journal,decision,day);decisions.append((decision,day))
    return journal,decisions


def test_missing_journal_reader_is_read_only_and_cannot_inject_arbitrary_feedback(tmp_path):
    path=tmp_path/'absent.db'
    result=read_method_feedback(path,recipient='A1',as_of=ASOF,calendar=CAL)
    assert result['status']=='INSUFFICIENT' and not path.exists()


def test_ten_distinct_mature_days_only_method_aggregate_no_stock_or_prose(tmp_path):
    journal,_=mature_fixture(tmp_path);before=journal.path.read_bytes()
    result=read_method_feedback(journal.path,recipient='A2',as_of=ASOF,calendar=CAL)
    assert result['status']=='READY' and len(result['methods'])==1
    method=result['methods'][0]
    assert method['sample_count']==15 and method['labeled_trade_dates']==10
    assert method['evidence_types']==['NEWS','QUANT'] and method['positive_fraction_1d']=='1'
    assert method['positive_fraction_3d']=='1' and 'mean_return_1d' not in method and 'mean_return_3d' not in method
    text=canonical(result)
    assert '000001' not in text and 'source_ref' not in text and 'supporting_refs' not in text
    assert journal.path.read_bytes()==before
    # Another role cannot borrow A2's mature dates.
    assert read_method_feedback(journal.path,recipient='A1',as_of=ASOF,calendar=CAL)['status']=='INSUFFICIENT'


@pytest.mark.parametrize('days,samples,symbols',[(9,15,5),(10,14,5),(10,15,4),(10,20,1)])
def test_each_bucket_must_meet_all_three_independent_maturity_minima(tmp_path,days,samples,symbols):
    journal,_=mature_fixture(tmp_path,n=days,samples=samples,symbols=symbols)
    result=read_method_feedback(journal.path,recipient='A2',as_of=ASOF,calendar=CAL)
    assert result['status']=='INSUFFICIENT' and result['methods']==[]
    assert result['minimum_samples']==15 and result['minimum_distinct_symbols']==5


def test_mature_dates_and_stocks_cannot_be_borrowed_from_a_different_judgment_bucket(tmp_path):
    journal=ModelJudgmentJournal(tmp_path/'models.db');day=date(2026,9,1)
    for i in range(10):
        for action in ('BUY_SIGNAL','BLOCK'):
            decision=capture(journal,day,symbol=f'{1+i%5:06d}.SZ',ordinal=action,action=action)
            label(journal,decision,day)
        day=CAL.next_trading_day(day)
    result=read_method_feedback(journal.path,recipient='A2',as_of=ASOF,calendar=CAL)
    assert result['status']=='INSUFFICIENT' and result['methods']==[]


def test_nine_dates_or_missing_t3_cannot_be_called_mature(tmp_path):
    journal,decisions=mature_fixture(tmp_path,n=9)
    result=read_method_feedback(journal.path,recipient='A2',as_of=ASOF,calendar=CAL)
    assert result['status']=='INSUFFICIENT' and result['methods']==[]
    decision=capture(journal,date(2026,9,14))
    append_judgment_outcome(journal,decision_id=decision,horizon=1,reference=evidence(date(2026,9,14)),
        target=evidence(date(2026,9,15),'11'),captured_at=ASOF,calendar=CAL)
    assert read_method_feedback(journal.path,recipient='A2',as_of=ASOF,calendar=CAL)['status']=='INSUFFICIENT'


def test_late_available_labels_cannot_leak_to_earlier_asof(tmp_path):
    journal,_=mature_fixture(tmp_path)
    # The target prices are old, but these label receipts were captured 09-25.
    result=read_method_feedback(journal.path,recipient='A2',as_of='2026-09-24T18:00:00+08:00',calendar=CAL)
    assert result['status']=='INSUFFICIENT' and result['methods']==[]


def test_legacy_nullable_return_columns_and_missing_original_catalog_not_proof(tmp_path):
    journal,decisions=mature_fixture(tmp_path,with_catalog=False)
    with closing(journal._connect()) as db,db:
        db.execute("UPDATE model_judgments SET t1_return='999',t3_return='999'")
    assert read_method_feedback(journal.path,recipient='A2',as_of=ASOF,calendar=CAL)['status']=='INSUFFICIENT'


@pytest.mark.parametrize('kind',['holiday-due','wrong-symbol','future-quote','altered-hash','adjusted-no-factor'])
def test_invalid_price_evidence_cannot_be_written(tmp_path,kind):
    journal=ModelJudgmentJournal(tmp_path/'models.db');day=date(2026,9,4);decision=capture(journal,day)
    target=evidence(date(2026,9,7),'11')
    if kind=='holiday-due':target=evidence(date(2026,9,5),'11')
    if kind=='wrong-symbol':target=evidence(date(2026,9,7),'11',symbol='000002.SZ')
    if kind=='future-quote':target['observed_at']='2026-09-28T16:00:00+08:00'
    if kind=='altered-hash':target['row']['close']='99'
    if kind=='adjusted-no-factor':target=evidence(date(2026,9,7),'11',adjust='qfq')
    with pytest.raises(ValueError):append_judgment_outcome(journal,decision_id=decision,horizon=1,
        reference=evidence(day),target=target,captured_at=ASOF,calendar=CAL)
    with closing(journal._connect()) as db:
        assert db.execute('SELECT count(*) FROM model_judgment_outcomes').fetchone()[0]==0


def test_outcomes_immutable_idempotent_and_no_legacy_columns_rewritten(tmp_path):
    journal=ModelJudgmentJournal(tmp_path/'model.db');day=date(2026,9,1);decision=capture(journal,day)
    args=dict(decision_id=decision,horizon=1,reference=evidence(day),target=evidence(date(2026,9,2),'11'),
        captured_at=ASOF,calendar=CAL)
    first=append_judgment_outcome(journal,**args)
    args['captured_at']=ASOF+timedelta(minutes=1)
    assert append_judgment_outcome(journal,**args)==dict(status='REUSED',body_sha256=first['body_sha256'])
    args['target']=evidence(date(2026,9,2),'12')
    with pytest.raises(ValueError,match='BINDING_CONFLICT'):append_judgment_outcome(journal,**args)
    with closing(journal._connect()) as db:
        assert db.execute('SELECT t1_return,t3_return FROM model_judgments').fetchone()==(None,None)


def test_corrupt_outcome_or_judgment_is_excluded_not_an_extra_good_sample(tmp_path):
    journal,decisions=mature_fixture(tmp_path)
    with closing(journal._connect()) as db,db:
        db.execute("UPDATE model_judgment_outcomes SET body_json='{}' WHERE decision_id=?",(decisions[0][0],))
    result=read_method_feedback(journal.path,recipient='A2',as_of=ASOF,calendar=CAL)
    assert result['status']=='INSUFFICIENT' and result['excluded_records']==2


def test_invalid_judgment_and_future_capture_cannot_be_labeled(tmp_path):
    journal=ModelJudgmentJournal(tmp_path/'models.db');day=date(2026,9,1);decision=capture(journal,day,valid=False)
    with pytest.raises(ValueError,match='NOT_PROVEN'):label(journal,decision,day)


def test_original_model_catalog_and_new_tables_commit_atomically(tmp_path):
    journal=ModelJudgmentJournal(tmp_path/'model.db')
    with closing(journal._connect()) as db,db:
        db.execute("CREATE TRIGGER fault BEFORE INSERT ON model_call_evidence BEGIN SELECT RAISE(ABORT,'fault'); END")
    with pytest.raises(sqlite3.IntegrityError):capture(journal,date(2026,9,1))
    with closing(journal._connect()) as db:
        assert db.execute('SELECT count(*) FROM model_calls').fetchone()[0]==0
        assert db.execute('SELECT count(*) FROM model_judgments').fetchone()[0]==0


@pytest.mark.parametrize('stance,method_id,expected',[
    ('TIGHTEN',None,'READY'),('KEEP',None,'READY'),('INSUFFICIENT',None,'INSUFFICIENT'),
    ('KEEP','NOT_IN_INPUT','INSUFFICIENT'),('LOOSEN',None,'A5_DEGRADED_NO_APPROVED_REPORT')])
def test_actual_a5_http_mock_selects_mature_method_then_research_reader_excludes_free_prose(tmp_path,monkeypatch,stance,method_id,expected):
    import httpx
    import liangjian_funnel.pipeline.team_v2_feedback as feedback
    from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient
    from liangjian_funnel.pipeline.team_v2_prompts import TeamPromptRepository
    from liangjian_funnel.runtime.team_v2_review import run_team_review
    from liangjian_funnel.runtime.team_v2_review_facts import build_team_review_facts
    from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
    from liangjian_funnel.settings import Settings
    from test_team_v2_review import report
    from test_team_v2_pm_session import fixture as morning_fixture
    from pathlib import Path
    journal,_=mature_fixture(tmp_path)
    monkeypatch.setattr(feedback,'ExchangeTradingCalendar',lambda:CAL)
    store=TeamMeetingStore(tmp_path/'team.db')
    original=dict(trade_date='2026-09-25',review_kind='POST_CLOSE',source_run_ids=[],metrics={})
    legacy={**original,'input_hash':h(original)}
    facts=build_team_review_facts(legacy_facts=legacy,store=store,journal=journal,paper_snapshot={'account':{'nav':'1000000','positions_value':'0'}},
        settlement=None,account_id='paper',trade_date='2026-09-25',review_kind='POST_CLOSE',observed_at=ASOF)
    method=facts['method_calibration']['A2']['methods'][0]
    base,_=morning_fixture(tmp_path/'morning')
    envelope=deepcopy(base['required_envelope'])
    envelope.update(stage_id='A5',source_trade_date='2026-09-24',execution_trade_date='2026-09-25',
        as_of=ASOF.isoformat(),knowledge_cutoff=ASOF.isoformat())
    output=report(envelope);output['trade_date']='2026-09-25'
    output['method_feedback']['A2']=[dict(method_id=method_id or method['method_id'],stance=stance,evidence_ids=['A5:METHOD:A2'],
        opposing_refs=['A5:LEGACY'],note='Do not inject stock 000001.SZ return 999 into another model')]
    client=OpenAICompatibleModelClient(Settings.from_env({'LIANGJIAN_MODEL_API_KEY':'UNIT_TEST_ONLY_NOT_A_CREDENTIAL'},root=tmp_path),
        transport=httpx.MockTransport(lambda request:sse_model_response(200,json={'choices':[{'message':{'content':json.dumps(output)}}]},request=request)),max_attempts=1)
    result=run_team_review(repository=TeamPromptRepository(Path(__file__).resolve().parents[1]/'prompts/team_v2'),client=client,
        journal=journal,store=store,review_id='actual-a5',call_id='actual-a5-model',account_id='paper',run_id=envelope['run_id'],
        model=envelope['model_name'],trade_date='2026-09-25',source_trade_date='2026-09-24',review_kind='POST_CLOSE',clock=lambda:ASOF,
        required_envelope=envelope,facts=facts,signal_domain={},counterexample_domain=set(),allow_model_call=True)
    assert result['status']==('A5_DEGRADED_NO_APPROVED_REPORT' if expected=='A5_DEGRADED_NO_APPROVED_REPORT' else 'READY'),result['reason_codes']
    before=store.path.read_bytes()
    actual=feedback.read_a5_method_feedback(journal.path,store.path,recipient='A2',as_of=ASOF)
    assert actual['status']==('INSUFFICIENT' if expected=='A5_DEGRADED_NO_APPROVED_REPORT' else expected)
    if expected=='READY':assert actual['methods'][0]==dict(method,stance=stance)
    assert '000001' not in canonical(actual) and 'note' not in canonical(actual) and 'mean_return' not in canonical(actual)
    assert before==store.path.read_bytes()
    assert feedback.read_a5_method_feedback(journal.path,store.path,recipient='A2',as_of=ASOF-timedelta(seconds=1))['status']=='INSUFFICIENT'
    with closing(journal._connect()) as db,db:
        db.execute("UPDATE model_judgment_outcomes SET body_json='{}' WHERE decision_id=(SELECT min(decision_id) FROM model_judgment_outcomes)")
    assert feedback.read_a5_method_feedback(journal.path,store.path,recipient='A2',as_of=ASOF)['status']=='INSUFFICIENT'


def test_new_a3_source_cannot_pass_arbitrary_legacy_ready_feedback(tmp_path):
    from dataclasses import replace
    from test_team_v2_research_adapter import actual_fixture
    app,args,seen=actual_fixture(tmp_path)
    args['snapshot']=replace(args['snapshot'],data={**args['snapshot'].data,
        'PRIOR_OUTCOME_FEEDBACK':{'status':'READY','secret_stock_return_999':'000001.SZ'}})
    result=app._run_stage(**args)
    assert result.status=='VALIDATED',result.reason_codes
    assert 'secret_stock_return_999' not in seen[0]['messages'][0]['content']
    assert 'INSUFFICIENT' in seen[0]['messages'][0]['content']
