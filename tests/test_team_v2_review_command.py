"""Customer v1 unchanged; independent actual client at mocked HTTP only."""
from fixture_model_stream import sse_model_response
from datetime import datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
from types import SimpleNamespace

import httpx
import pytest

from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient
from liangjian_funnel.runtime.team_v2_review_command import run_review_stage,review_events,coach_projection
from liangjian_funnel.runtime.team_v2_review_facts import build_team_review_facts,read_paper_review_evidence
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore,canonical
from liangjian_funnel.runtime.team_v2_trader_session import run_trader_session
from liangjian_funnel.settings import Settings
from liangjian_paper_broker.ledger import LocalPaperBroker
from test_team_v2_review import report,hashed


NOW=datetime.fromisoformat('2026-10-19T16:46:00+08:00')
CAL=SimpleNamespace(is_trading_day=lambda day:day.isoformat()=='2026-10-19')
ROOT=Path(__file__).resolve().parents[1]


def test_0283_counterexample_stage_projection_keeps_original_detail_without_editing_source():
    from copy import deepcopy
    from liangjian_funnel.runtime.team_v2_review_command import counterexample_stage_projection
    rows=[dict(symbol='000001.SZ',drop_stage='A2_QUANT_FILTERED'),
          dict(symbol='000002.SZ',drop_stage='UNRESOLVED_A3_LINEAGE_MISSING')]
    before=deepcopy(rows);projected=counterexample_stage_projection(rows)
    assert rows==before
    assert [row['funnel_drop_stage'] for row in projected]==['A2','UNRESOLVED']
    assert [row['original_drop_stage'] for row in projected]==[r['drop_stage'] for r in rows]


def fixture(tmp_path,*,invalid=False):
    shutil.copytree(ROOT/'config',tmp_path/'config');shutil.copytree(ROOT/'prompts/team_v2',tmp_path/'prompts/team_v2')
    settings=Settings.from_env({'LIANGJIAN_MODEL_API_KEY':'UNIT_TEST_ONLY_NOT_A_CREDENTIAL'},root=tmp_path)
    owned=tmp_path/'state/team_v2';TeamMeetingStore(owned/'team.db')
    LocalPaperBroker(owned/'paper.db',initial_capital=Decimal(1000000),clock=lambda:NOW)
    facts=hashed(dict(trade_date='2026-10-19',review_kind='POST_CLOSE',cutoff_at='2026-10-19T15:00:00+08:00',
        operational_cutoff_at='2026-10-19T16:01:00+08:00',metrics={},source_run_ids=[],
        a2={},a3={'plans':[]},a4={'events':[]},data_quality={},review_history=[{'executive_summary':'CUSTOMER_PROSE_MUST_NOT_ENTER'}]))
    with sqlite3.connect(settings.state_db_path) as db:
        db.execute('CREATE TABLE a5_daily_reviews(trade_date,review_kind,input_hash,fact_snapshot_json,created_at,report_json)')
        db.execute('INSERT INTO a5_daily_reviews VALUES (?,?,?,?,?,?)',('2026-10-19','POST_CLOSE',facts['input_hash'],
            canonical(facts),'2026-10-19T16:02:00+08:00','CUSTOMER_CURRENT_REPORT_NOT_READ'))
    seen=[]
    def handler(request):
        packet=json.loads(request.read());seen.append(packet)
        runtime=json.loads(packet['messages'][1]['content'].removeprefix('RUNTIME_INPUT\n'))
        raw=report(runtime['required_envelope'])
        raw['review_kind']=runtime['session']
        if invalid:raw['unknown_order_permission']=True
        return sse_model_response(200,json={'choices':[{'message':{'content':json.dumps(raw)}}]},request=request)
    client=OpenAICompatibleModelClient(settings,transport=httpx.MockTransport(handler),max_attempts=1)
    return settings,client,seen,facts


def test_real_command_freezes_facts_calls_new_coach_once_no_customer_prose_or_write(tmp_path):
    settings,client,seen,facts=fixture(tmp_path)
    before=settings.state_db_path.read_bytes();owned=settings.root/'state/team_v2'
    paper_before=(owned/'paper.db').read_bytes()
    result=run_review_stage(settings,'POST_CLOSE',clock=lambda:NOW,calendar=CAL,client=client)
    assert result['status']=='READY' and len(seen)==1
    assert run_review_stage(settings,'POST_CLOSE',clock=lambda:NOW.replace(minute=49),calendar=CAL,client=client)==result
    assert len(seen)==1 and settings.state_db_path.read_bytes()==before and (owned/'paper.db').read_bytes()==paper_before
    assert 'CUSTOMER_PROSE' not in canonical(seen) and 'CUSTOMER_CURRENT_REPORT' not in canonical(seen)
    assert b'CUSTOMER_PROSE' not in (owned/'team.db').read_bytes()
    assert 'agent-5/2.0.7' in seen[0]['messages'][0]['content']
    with sqlite3.connect(owned/'team.db') as db:
        assert db.execute('SELECT count(*) FROM team_a5_reviews').fetchone()[0]==1
        assert db.execute('SELECT count(*) FROM team_internal_cards').fetchone()[0]==1
        assert not db.execute("SELECT name FROM sqlite_master WHERE name='execution_plans'").fetchall()


def test_invalid_team_response_does_not_call_v1_or_retry_model(tmp_path):
    settings,client,seen,_=fixture(tmp_path,invalid=True)
    for _ in range(2):
        assert run_review_stage(settings,'POST_CLOSE',clock=lambda:NOW,calendar=CAL,client=client)['status']=='A5_DEGRADED_NO_APPROVED_REPORT'
    assert len(seen)==1


def test_midday_keeps_journal_and_internal_card_without_free_text_research_export(tmp_path):
    settings,client,seen,facts=fixture(tmp_path)
    facts.update(review_kind='MIDDAY',cutoff_at='2026-10-19T11:30:00+08:00',operational_cutoff_at='2026-10-19T11:30:00+08:00')
    facts.pop('input_hash');facts=hashed(facts)
    with sqlite3.connect(settings.state_db_path) as db:
        db.execute('UPDATE a5_daily_reviews SET review_kind=?,input_hash=?,fact_snapshot_json=?,created_at=?',
            ('MIDDAY',facts['input_hash'],canonical(facts),'2026-10-19T11:35:10+08:00'))
    now=NOW.replace(hour=11,minute=36)
    result=run_review_stage(settings,'MIDDAY',clock=lambda:now,calendar=CAL,client=client)
    assert result['status']=='READY' and result['hypothesis_export']==dict(status='JOURNAL_ADVISORY_ONLY_NOT_EXPORTED',count=0)
    with sqlite3.connect(settings.root/'state/team_v2/team.db') as db:
        card=db.execute('SELECT card_id,body_json FROM team_internal_cards').fetchone()
    assert card[0].startswith('team-midday:') and '午间' in card[1]
    files=list((settings.root/'artifacts/replay-queue/2026-10-19').glob('*.json'))
    assert files==[]


def test_disabled_export_never_consumes_or_overwrites_old_free_text_queue(tmp_path):
    settings,client,seen,_=fixture(tmp_path)
    directory=settings.root/'artifacts/replay-queue/2026-10-19';directory.mkdir(parents=True)
    path=directory/'old-user-owned.json';path.write_text('USER_CHANGED',encoding='utf-8')
    assert run_review_stage(settings,'POST_CLOSE',clock=lambda:NOW,calendar=CAL,client=client)['status']=='READY'
    result=run_review_stage(settings,'POST_CLOSE',clock=lambda:NOW,calendar=CAL,client=client)
    assert result['status']=='READY' and result['hypothesis_export']['count']==0
    assert path.read_text(encoding='utf-8')=='USER_CHANGED' and len(seen)==1


def test_waiting_eod_is_before_any_model_and_legacy_fact_hash_tamper_blocks(tmp_path):
    settings,client,seen,_=fixture(tmp_path)
    assert run_review_stage(settings,'POST_CLOSE',clock=lambda:NOW.replace(hour=16,minute=30),calendar=CAL,client=client)==dict(
        status='WAITING',reason_code='TEAM_A5_EOD_PREREQUISITE_PENDING')
    with sqlite3.connect(settings.state_db_path) as db:db.execute("UPDATE a5_daily_reviews SET input_hash='tampered'")
    with pytest.raises(ValueError,match='BINDING_INVALID'):
        run_review_stage(settings,'POST_CLOSE',clock=lambda:NOW,calendar=CAL,client=client)
    assert seen==[]


def test_missing_frozen_facts_waits_without_reconstructing_or_model(tmp_path):
    settings,client,seen,_=fixture(tmp_path)
    with sqlite3.connect(settings.state_db_path) as db:db.execute('DELETE FROM a5_daily_reviews')
    assert run_review_stage(settings,'POST_CLOSE',clock=lambda:NOW,calendar=CAL,client=client)['status']=='WAITING'
    assert seen==[]


def test_actual_trader_journal_decision_id_binds_paper_admission_not_batch_id(tmp_path):
    from test_team_v2_trader_session import fixture as trader
    args,seen,raw=trader(tmp_path);result=run_trader_session(**args)
    assert result['admissions'][0]['dispatched']
    legacy=hashed(dict(trade_date='2026-10-19',review_kind='POST_CLOSE',source_run_ids=[],metrics={},
        a2={},a3={'plans':[dict(plan_id='p1',strategy_profile='TREND_MA5')]},a4={'events':[]}))
    paper=read_paper_review_evidence(args['broker'].path,trade_date='2026-10-19',observed_at=NOW)
    facts=build_team_review_facts(legacy_facts=legacy,store=args['store'],journal=args['journal'],paper_snapshot=paper,
        settlement=None,account_id='paper',trade_date='2026-10-19',review_kind='POST_CLOSE',observed_at=NOW)
    domain,events=review_events(facts)
    with sqlite3.connect(args['journal'].path) as db:
        original=db.execute("SELECT decision_id FROM model_judgments WHERE judgment_path='signals[0]'").fetchone()[0]
    assert list(domain)==[original] and original!=result['decision_id']
    assert original!=result['admissions'][0]['signal_id']
    assert domain[original]['strategy_profile']=='TREND_MA5'
    assert events[0]['identity_basis']=='ORIGINAL_MODEL_JUDGMENTS_DECISION_ID'
    original=canonical(facts);projection,bound=coach_projection(facts)
    assert canonical(facts)==original
    assert bound==domain and projection['signals_for_review']==events
    assert len(canonical(projection))<250000
    assert 'legacy_facts' not in projection and projection['full_team_fact_sha256']==facts['input_hash']


def test_original_local_1015_facts_project_without_models_or_source_changes(tmp_path):
    from liangjian_funnel.pipeline.team_v2_judgments import ModelJudgmentJournal
    path=Path('D:/dev_A股/liangjian_wp5_hotfix_20261009/artifacts/b3-a5-20261015/frozen-post-close-1612/post-close-322c9cec2e0c-facts.json')
    if not path.is_file():pytest.skip('optional pinned local original not in source distribution')
    raw=path.read_bytes();source=json.loads(raw);assert source['input_hash']==hashlib.sha256(canonical(
        {k:v for k,v in source.items() if k!='input_hash'}).encode()).hexdigest()
    archived_hash=source['input_hash'];source={k:v for k,v in source.items() if k not in {'review_history','input_hash'}}
    source['source_archive_input_sha256']=archived_hash;source=hashed(source)
    store=TeamMeetingStore(tmp_path/'team.db');journal=ModelJudgmentJournal(tmp_path/'model.db')
    facts=build_team_review_facts(legacy_facts=source,store=store,journal=journal,paper_snapshot=None,settlement=None,
        account_id='local-paper',trade_date='2026-10-15',review_kind='POST_CLOSE',
        observed_at=datetime.fromisoformat('2026-10-15T16:46:00+08:00'))
    before=canonical(facts);projected,domain=coach_projection(facts)
    # 0482 requires complete plan causal facts, not an old speed-oriented
    # projection-size target. The actual transport retains its independent
    # context guard and must still have an exact inverse.
    from liangjian_funnel.pipeline.team_v2_a5_projection import project_review_facts,expand_review_facts
    transport,_=project_review_facts(projected)
    assert expand_review_facts(transport)==projected
    assert 'plan_causal_chains' in projected
    assert canonical(facts)==before and path.read_bytes()==raw
    assert 'counterexamples' not in projected['independent_verification']
    reference=projected['independent_verification']['counterexamples_reference']
    assert reference['item_count']==len(projected['counterexamples'])
    assert reference['sha256']==hashlib.sha256(canonical(projected['counterexamples']).encode()).hexdigest()
    print('ORIGINAL_FROZEN_COACH_PROJECTION',json.dumps(dict(source_bytes=len(raw),source_file_sha256=hashlib.sha256(raw).hexdigest(),
        model_characters=len(canonical(projected)),signal_events=len(domain),model_calls=0)))


def test_readonly_current_account_matches_real_broker_without_settlement(tmp_path):
    from test_team_v2_paper_ledger import broker,stamp,place,drive,bar
    from liangjian_paper_broker.matching import PaperRules
    b=broker(tmp_path/'paper.db');place(b);drive(b,bar(),PaperRules('2026-10-19'));before=b.snapshot_bytes()
    actual=read_paper_review_evidence(b.path,trade_date='2026-10-19',observed_at=stamp(tm='11:35:00'))
    assert {k:actual['account'][k] for k in ('nav','cash','positions_value','pnl_realized','pnl_unrealized')}=={
        k:b.get_account()[k] for k in ('nav','cash','positions_value','pnl_realized','pnl_unrealized')}
    assert actual['settlement'] is None and b.snapshot_bytes()==before


def test_actual_cli_team_review_no_legacy_constructor_and_only_waiting_exit75(tmp_path,monkeypatch,capsys):
    import liangjian_funnel.cli as cli
    import liangjian_funnel.runtime.team_v2_review_command as module
    settings,_,_,_=fixture(tmp_path)
    def forbidden(*a,**kw):raise AssertionError('LEGACY_CONSTRUCTOR_FORBIDDEN')
    monkeypatch.setattr(cli,'WorkflowApplication',forbidden);monkeypatch.setattr(cli,'RuntimeStore',forbidden)
    monkeypatch.setattr(module,'run_review_stage',lambda *_:dict(status='WAITING',reason_code='TEAM_A5_FROZEN_FACTS_PENDING'))
    assert cli.main(['run-team-a5-close'],settings=settings)==75
    assert json.loads(capsys.readouterr().out)['status']=='WAITING'
    monkeypatch.setattr(module,'run_review_stage',lambda *_:dict(status='A5_DEGRADED_NO_APPROVED_REPORT'))
    assert cli.main(['run-team-a5-midday'],settings=settings)==2
    assert json.loads(capsys.readouterr().out)['status']=='A5_DEGRADED_NO_APPROVED_REPORT'
