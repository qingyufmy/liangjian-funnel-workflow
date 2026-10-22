"""Authored A5 bytes, actual HTTP client mock and owned DB fault injections."""
from fixture_model_stream import sse_model_response
from copy import deepcopy
from datetime import datetime
from pathlib import Path
import hashlib
import json
import sqlite3

import httpx
import pytest

from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient
from liangjian_funnel.pipeline.team_v2_review_schema import validate_daily_review
from liangjian_funnel.runtime.team_v2_review import run_team_review,TeamReviewRuntime
from liangjian_funnel.runtime.team_v2_review_facts import build_team_review_facts,read_paper_review_evidence
from liangjian_funnel.runtime.team_v2_store import canonical
from liangjian_funnel.settings import Settings
from test_team_v2_pm_session import fixture as morning_fixture

NOW=datetime.fromisoformat('2026-10-19T16:00:00+08:00')
REFS=dict(evidence_ids=['METRICS:DAILY'],opposing_refs=['A5:LEGACY'])


def hashed(body):return {**body,'input_hash':hashlib.sha256(canonical(body).encode()).hexdigest()}


def report(envelope):
    layer=dict(verdict='DATA_LIMITED',summary='样本不足',strengths=[],defects=[],data_limitations=['未成熟收益'],**REFS)
    return dict(envelope=envelope,schema_version='a5-daily-review/2.0.0',review_kind='POST_CLOSE',trade_date='2026-10-19',
        overall_verdict='DATA_LIMITED',executive_summary='按冻结事实等待观察，不改策略。',
        team_scorecard=dict(nav_vs_benchmark='UNAVAILABLE',utilization_vs_target='UNAVAILABLE',slot_usage='UNAVAILABLE',
            entry_hit_rate='UNAVAILABLE',drawdown='UNAVAILABLE',role_kpis={r:'UNAVAILABLE' for r in ('A1','A2','A3','A4','A6')},
            attribution=[dict(deviation='未成熟',owner='A4',**REFS)]),
        a2_review=deepcopy(layer),a3_review=deepcopy(layer),a4_review=deepcopy(layer),a6_review=deepcopy(layer),
        trader_vs_deterministic=dict(summary='等待对照',**REFS),
        risk_governor_value=dict(veto_group_vs_pass_group='等待对照',over_tightening='INSUFFICIENT',**REFS),
        model_value_assessment=[dict(layer='A6',judgment_type='风控',model_group='否决',control_group='放行',verdict='INSUFFICIENT',**REFS)],
        signal_reviews=[],missed_opportunity_reviews=[],core_defects=[],
        method_feedback={r:[] for r in ('A1','A2','A3','A4','A6')},
        hypotheses=[dict(hypothesis_id='h1',layer='A4',condition='等待条件未满足',expected_effect='区分无形态',
            validation_method='REPLAY',success_criteria='冻结数据可对照',falsification_criteria='窗口缺失',
            min_observation_days=5,automatic_production_change=False,**REFS)],
        handoff_notes=dict(to_a1=[],to_a3=[],to_a4=['等待成熟对照'],to_a6=[]),
        what_would_change_my_mind=['新增成熟收益样本'],
        unresolved_questions=[dict(question='样本何时成熟',reason='INSUFFICIENT_SAMPLE',resolution='等待真实交易日')])


def fixture(tmp_path,*,invalid=False,offline=False):
    base,_=morning_fixture(tmp_path)
    envelope=deepcopy(base['required_envelope']);envelope['stage_id']='A5'
    legacy=hashed(dict(trade_date='2026-10-19',review_kind='POST_CLOSE',source_run_ids=[],metrics={}))
    facts=build_team_review_facts(legacy_facts=legacy,store=base['store'],journal=base['journal'],
        paper_snapshot={'account':{'nav':'1000000','positions_value':'0'}},settlement=None,
        account_id='paper',trade_date='2026-10-19',review_kind='POST_CLOSE',observed_at=NOW)
    raw=report(envelope)
    if invalid:raw['hypotheses'][0]['automatic_production_change']=True
    seen=[]
    def handler(request):
        seen.append(json.loads(request.read()))
        if offline:raise httpx.ConnectError('PRIVATE_WEBHOOK_OR_PROVIDER_TEXT',request=request)
        return sse_model_response(200,json={'choices':[{'message':{'content':json.dumps(raw)}}]},request=request)
    client=OpenAICompatibleModelClient(Settings.from_env({'LIANGJIAN_MODEL_API_KEY':'UNIT_TEST_ONLY_NOT_A_CREDENTIAL'},root=tmp_path),
        transport=httpx.MockTransport(handler),max_attempts=1)
    args={k:base[k] for k in ('repository','journal','store','account_id','run_id','model','trade_date','source_trade_date')}
    args.update(client=client,required_envelope=envelope,facts=facts,review_id='review:paper:20261019:POST_CLOSE',
        call_id='A5:paper:20261019:POST_CLOSE',review_kind='POST_CLOSE',clock=lambda:NOW,
        signal_domain={},counterexample_domain=set(),allow_model_call=True)
    return args,seen


def check(raw,**changes):
    args=dict(required_envelope={},trade_date='2026-10-19',review_kind='POST_CLOSE',
        signal_domain={},counterexample_domain=set(),metrics={})|changes
    return validate_daily_review(raw,dict(categories=['METRICS','LEGACY'],items=[
        dict(id='METRICS:DAILY',category='METRICS'),dict(id='A5:LEGACY',category='LEGACY')]),**args)


def test_strict_authored_schema_and_complete_judgment_capture():
    raw=report({});before=deepcopy(raw);result=check(raw)
    assert result.valid and before==raw and len(result.judgments)==9


@pytest.mark.parametrize('name',['沪深300','中证1000','中证500','上证指数'])
def test_only_exact_benchmark_name_digits_are_excluded_from_numeric_claims(name):
    raw=report({});raw['team_scorecard']['nav_vs_benchmark']=name+' 收益率尚不可得'
    assert check(raw).valid
    raw['team_scorecard']['nav_vs_benchmark']=name+'收益率 37.25%'
    assert 'A5_SCORECARD_NUMBER_NOT_IN_METRICS' in check(raw).issues
    assert check(raw,metrics={'benchmark_return_pct':37.25}).valid


def test_nearby_benchmark_like_words_do_not_hide_unprovided_numbers():
    raw=report({});raw['team_scorecard']['nav_vs_benchmark']='沪深301和中证1001以及沪深30037.25'
    assert 'A5_SCORECARD_NUMBER_NOT_IN_METRICS' in check(raw).issues


@pytest.mark.parametrize('kind',['extra','envelope','date','summary','handoff','falsifier','refs','opposing',
    'sentinel_scope','hyp_auto','hyp_duplicate','hyp_count','hyp_days','hyp_method','hyp_empty','questions','score_number',
    'signal_outside','counter_outside','defect_unconfirmed','feedback_refs','feedback_stock','malformed','capability'])
def test_schema_rejects_without_repair_or_default_permission(kind):
    raw=report({});args={}
    if kind=='extra':raw['order_qty']=100
    elif kind=='envelope':raw['envelope']={'other':True}
    elif kind=='date':raw['trade_date']='2026-10-20'
    elif kind=='summary':raw['executive_summary']=''
    elif kind=='handoff':raw['handoff_notes']={}
    elif kind=='falsifier':raw['what_would_change_my_mind']=[]
    elif kind=='refs':raw['a6_review']['evidence_ids']=['OUTSIDE']
    elif kind=='opposing':raw['a6_review']['opposing_refs']=[]
    elif kind=='sentinel_scope':raw['a6_review']['opposing_refs']=['NO_OPPOSING_EVIDENCE_FOUND']
    elif kind=='hyp_auto':raw['hypotheses'][0]['automatic_production_change']=True
    elif kind=='hyp_duplicate':raw['hypotheses']*=2
    elif kind=='hyp_count':raw['hypotheses']*=4
    elif kind=='hyp_days':raw['hypotheses'][0]['min_observation_days']=0
    elif kind=='hyp_method':raw['hypotheses'][0]['validation_method']='LIVE_ORDER'
    elif kind=='hyp_empty':raw['hypotheses'][0]['condition']=''
    elif kind=='questions':raw['unresolved_questions']=[]
    elif kind=='score_number':raw['team_scorecard']['entry_hit_rate']='99%'
    elif kind=='signal_outside':raw['signal_reviews']=[dict(symbol='OUTSIDE',strategy_profile='TREND_MA5',
        lifecycle_status='UNFILLED',assessment='test',attribution='DATA_LIMITED',**REFS)]
    elif kind=='counter_outside':raw['missed_opportunity_reviews']=[dict(symbol='OUTSIDE',theme='t',
        observed_performance='up',funnel_drop_stage='A4',assessment='test',is_confirmed_defect=False,**REFS)]
    elif kind=='defect_unconfirmed':
        args['counterexample_domain']={'000001.SZ'};raw['missed_opportunity_reviews']=[dict(symbol='000001.SZ',theme='t',
            observed_performance='up',funnel_drop_stage='A4',assessment='test',is_confirmed_defect=True,**REFS)]
    elif kind=='feedback_refs':raw['method_feedback']['A1']=[dict(note='change',evidence_ids=['OUTSIDE'],opposing_refs=['A5:LEGACY'])]
    elif kind=='feedback_stock':raw['method_feedback']['A1']=[dict(symbol='000001.SZ',note='change',**REFS)]
    elif kind=='malformed':raw['signal_reviews']=[None]
    elif kind=='capability':raw['team_scorecard']['apply_limits']=True
    assert not check(raw,**args).valid


def test_sentinel_scope_and_method_feedback_are_research_judgments():
    raw=report({});raw['method_feedback']['A1']=[dict(method_id='unmatched-auditable-id',stance='INSUFFICIENT',
        note='等待更多样本',evidence_ids=['METRICS:DAILY'],opposing_refs=['NO_OPPOSING_EVIDENCE_FOUND'],
        opposing_search_scope=['METRICS','LEGACY'])]
    result=check(raw)
    assert result.valid and result.sentinel_count==1
    assert result.judgments[-1]['path']=='method_feedback.A1[0]'


def test_actual_http_to_judgments_atomic_hypothesis_feedback_and_internal_card(tmp_path):
    args,seen=fixture(tmp_path);before=deepcopy(args['facts']);result=run_team_review(**args)
    assert result['status']=='READY' and result['strategy_change_authorized'] is False
    assert run_team_review(**args)==result and len(seen)==1 and args['facts']==before
    assert seen[0]['messages'][0]['content'].startswith('===')
    with sqlite3.connect(args['store'].path) as db:
        assert db.execute('SELECT count(*) FROM team_hypothesis_queue').fetchone()[0]==1
        assert db.execute('SELECT count(*) FROM team_method_feedback').fetchone()[0]==5
        assert db.execute('SELECT status FROM team_internal_cards').fetchone()==('PENDING',)
        assert not db.execute("SELECT name FROM sqlite_master WHERE name='execution_plans'").fetchall()
    assert args['journal'].summary()[0]['role']=='A5'


@pytest.mark.parametrize('failure',['offline','invalid'])
def test_model_failures_preserve_original_and_fact_card_but_no_approved_report(tmp_path,failure):
    args,seen=fixture(tmp_path,invalid=failure=='invalid',offline=failure=='offline');result=run_team_review(**args)
    assert result['status']=='A5_DEGRADED_NO_APPROVED_REPORT' and result['report'] is None
    assert run_team_review(**args)==result and len(seen)==1
    with sqlite3.connect(args['store'].path) as db:
        assert db.execute('SELECT count(*) FROM team_hypothesis_queue').fetchone()[0]==0
        assert db.execute('SELECT status FROM team_internal_cards').fetchone()==('PENDING',)
    assert b'PRIVATE_WEBHOOK_OR_PROVIDER_TEXT' not in args['store'].path.read_bytes()


def test_mid_commit_fault_rolls_back_then_resumes_captured_no_recall(tmp_path):
    args,seen=fixture(tmp_path);TeamReviewRuntime(args['store'])
    with sqlite3.connect(args['store'].path) as db:
        db.execute("CREATE TRIGGER fail BEFORE INSERT ON team_hypothesis_queue BEGIN SELECT RAISE(ABORT,'FAULT'); END")
    with pytest.raises(sqlite3.IntegrityError):run_team_review(**args)
    with sqlite3.connect(args['store'].path) as db:
        assert db.execute('SELECT count(*) FROM team_a5_reviews').fetchone()[0]==0
        assert db.execute('SELECT count(*) FROM team_internal_cards').fetchone()[0]==0
        db.execute('DROP TRIGGER fail')
    assert run_team_review(**args)['status']=='READY' and len(seen)==1


def test_internal_callback_ack_unknown_and_restart_never_duplicate_send(tmp_path):
    args,seen=fixture(tmp_path);run_team_review(**args);runtime=TeamReviewRuntime(args['store']);sent=[]
    key='team-close:paper:2026-10-19'
    def sender(body):sent.append(body);return True
    assert runtime.deliver_internal(key,send_internal=sender)['status']=='SENT'
    assert runtime.deliver_internal(key,send_internal=sender)['status']=='SENT' and len(sent)==1
    assert '量见团队模拟盘收盘' in sent[0]['content']['text']
    with sqlite3.connect(args['store'].path) as db:db.execute("UPDATE team_internal_cards SET status='PENDING'")
    def failure(_):raise RuntimeError('PRIVATE_WEBHOOK_OR_PROVIDER_TEXT')
    assert runtime.deliver_internal(key,send_internal=failure)['status']=='UNKNOWN'
    assert runtime.deliver_internal(key,send_internal=sender)['status']=='UNKNOWN' and len(sent)==1
    assert runtime.deliver_internal('absent',send_internal=sender)['status']=='NO_REPORT_TODAY'
    assert b'PRIVATE_WEBHOOK_OR_PROVIDER_TEXT' not in args['store'].path.read_bytes()


def test_changed_fact_or_account_and_early_clock_or_authority_block_before_http(tmp_path):
    args,seen=fixture(tmp_path);args['allow_model_call']=False
    with pytest.raises(ValueError,match='NOT_AUTHORIZED'):run_team_review(**args)
    args['allow_model_call']=True;args['clock']=lambda:datetime.fromisoformat('2026-10-19T15:00:00+08:00')
    with pytest.raises(ValueError,match='WINDOW_INVALID'):run_team_review(**args)
    args['clock']=lambda:NOW;args['account_id']='other'
    with pytest.raises(ValueError,match='FACT_BINDING_INVALID'):run_team_review(**args)
    args['account_id']='paper';args['facts']['metrics']['fake']=99
    with pytest.raises(ValueError,match='FACT_BINDING_INVALID'):run_team_review(**args)
    assert seen==[]


def test_account_date_scoped_readonly_adapter_and_old_facts_tamper(tmp_path):
    args,_=fixture(tmp_path);store=args['store'];journal=args['journal']
    before=store.path.read_bytes();model_before=journal.path.read_bytes()
    legacy=args['facts']['legacy_facts']
    rebuilt=build_team_review_facts(legacy_facts=legacy,store=store,journal=journal,paper_snapshot=None,settlement=None,
        account_id='paper',trade_date='2026-10-19',review_kind='POST_CLOSE',observed_at=NOW)
    assert rebuilt['metrics']['team_v2']['nav'] is None
    assert rebuilt['metrics']['team_v2']['strict_t1_return'] is None
    assert store.path.read_bytes()==before and journal.path.read_bytes()==model_before
    legacy['metrics']['invented']=1
    with pytest.raises(ValueError,match='LEGACY_FACT_HASH_MISMATCH'):
        build_team_review_facts(legacy_facts=legacy,store=store,journal=journal,paper_snapshot=None,settlement=None,
            account_id='paper',trade_date='2026-10-19',review_kind='POST_CLOSE',observed_at=NOW)


def test_paper_read_does_not_create_or_settle_and_rejects_foreign_db(tmp_path):
    missing=tmp_path/'missing.db'
    assert read_paper_review_evidence(missing,trade_date='2026-10-19',observed_at=NOW)['status']=='UNAVAILABLE'
    assert not missing.exists()
    from liangjian_paper_broker.ledger import LocalPaperBroker
    from decimal import Decimal
    path=tmp_path/'paper.db';broker=LocalPaperBroker(path,initial_capital=Decimal(1000000),clock=lambda:NOW)
    before=path.read_bytes();read=read_paper_review_evidence(path,trade_date='2026-10-19',observed_at=NOW)
    assert read['settlement'] is None and read['rows']['fills']==[] and path.read_bytes()==before
    broker.settle('2026-10-19');before=path.read_bytes()
    assert read_paper_review_evidence(path,trade_date='2026-10-19',observed_at=NOW)['settlement']['account']['nav']=='1000000'
    assert path.read_bytes()==before
    wrong=tmp_path/'legacy.db'
    with sqlite3.connect(wrong) as db:db.execute('CREATE TABLE execution_plans(plan_id TEXT)')
    before=wrong.read_bytes()
    with pytest.raises(ValueError,match='PAPER_DATABASE_SCOPE_INVALID'):
        read_paper_review_evidence(wrong,trade_date='2026-10-19',observed_at=NOW)
    assert wrong.read_bytes()==before


def test_current_account_rows_and_model_judgments_not_other_account(tmp_path):
    args,seen=fixture(tmp_path)
    from test_team_v2_eod import fixture as eod_fixture
    from liangjian_funnel.runtime.team_v2_eod import run_team_eod
    eod,_=eod_fixture(tmp_path)
    run_team_eod(**eod)
    # Existing EOD fixture's account is paper; original two model calls are
    # selected through actual server-owned EOD account/date, never all calls.
    rebuilt=build_team_review_facts(legacy_facts=args['facts']['legacy_facts'],store=args['store'],journal=args['journal'],
        paper_snapshot=None,settlement=None,account_id='paper',trade_date='2026-10-19',review_kind='POST_CLOSE',
        observed_at=datetime.fromisoformat('2026-10-19T16:40:00+08:00'))
    assert len(rebuilt['a6']['rows']['eod_reviews'])==1
    assert len(rebuilt['model_judgments']['rows']['calls'])==2
    other=build_team_review_facts(legacy_facts=args['facts']['legacy_facts'],store=args['store'],journal=args['journal'],
        paper_snapshot=None,settlement=None,account_id='different',trade_date='2026-10-19',review_kind='POST_CLOSE',
        observed_at=datetime.fromisoformat('2026-10-19T16:40:00+08:00'))
    assert other['model_judgments']['rows']['calls']==[] and other['a6']['rows']['eod_reviews']==[]
