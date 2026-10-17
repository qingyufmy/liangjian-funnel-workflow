from dataclasses import replace
from decimal import Decimal as D
import sqlite3

import pytest

from liangjian_funnel.evaluation.team_v2_replay_passes import (
    CapturesOnlyJournal, NoModelRecallClient, run_two_passes,
)
from liangjian_funnel.runtime.team_v2_trader_session import run_trader_session
from liangjian_funnel.runtime.team_v2_execution import admit_trader_signal
from liangjian_paper_broker.matching import PaperRules
from test_team_v2_trader_session import fixture
from test_team_v2_paper_ledger import stamp,bar,drive


def callback(*,change_second=False,no_order=False,inject_curve_difference=False,two_days=False):
    seen=[];runs=[]
    def execute(work,journal,client_override):
        args,http,raw=fixture(work)
        if no_order:
            raw['signals'][0].update(action='NO_ACTION',waiting_for='next closed reversal')
        args['journal']=journal
        if client_override is not None:
            args['client']=client_override
            if change_second:args['replacements']['TIME_RULES']={'CHANGED_INPUT':True}
        result=run_trader_session(**args);seen.extend(http);runs.append(result)
        broker=args['broker']
        drive(broker,bar(volume_shares=300000),PaperRules('2026-10-19'))
        drive(broker,bar(tm='15:00:00',close=D('10.05')
            if inject_curve_difference and client_override is not None else D('10')),
            PaperRules('2026-10-19'))
        broker.clock=lambda:stamp(tm='15:30:00')
        assert broker.settle('2026-10-19')['status']=='COMPLETE'
        if two_days:
            broker.clock=lambda:stamp(day='20',tm='08:33:00')
            broker.open_risk_day('2026-10-20')
            broker.clock=lambda:stamp(day='20',tm='10:00:00')
            plan=args['all_plans']['p1']
            outcome=admit_trader_signal(broker=broker,signal_id='held-next-day-exit',action='SELL_SIGNAL',plan=plan,
                envelope=replace(args['envelopes']['p1'],trade_date='2026-10-20'),limits=args['limits'],
                all_plans=args['all_plans'],now=broker.clock(),closed_5m_end=broker.clock(),
                mark_as_of=stamp(day='20',tm='09:59:00'),regime='DATA_BLOCK',live_market_ready=False,
                snapshot_ready=True,mode='control',calendar=type('Calendar',(),{'is_trading_day':lambda self,day:True})(),
                input_sha256='c'*64)
            assert outcome['dispatched']  # owned T+1 exit is independent of expired plan/new-entry PM
            drive(broker,bar(day='20',volume_shares=300000),PaperRules('2026-10-20'))
            assert broker.get_positions()==()
            broker.clock=lambda:stamp(day='20',tm='15:30:00')
            assert broker.settle('2026-10-20')['status']=='COMPLETE'
        assert broker.reconcile()['status']=='MATCH'
    return execute,seen,runs


def test_real_client_mock_guard_engine_fill_and_two_fresh_accounts_are_byte_identical(tmp_path):
    execute,seen,runs=callback();output=tmp_path/'original'
    result=run_two_passes(output,allowed_root=tmp_path,run_pass=execute)
    assert result['status']=='IDENTICAL_EXECUTION_BYTES' and result['has_funded_fill_path']
    assert result['ledger_bytes_equal'] and result['curve_bytes_equal'] and len(seen)==1 and runs[0]==runs[1]
    assert result['passes'][0]['buy_fills']==1 and result['passes'][1]['buy_fills']==1
    assert result['passes'][0]['paper_identity']==result['passes'][1]['paper_identity']
    assert (output/'pass-1-state/paper.db').is_file() and (output/'pass-2-state/paper.db').is_file()
    assert not (output/'working').exists() and result['control_release_gate']=='NOT_EVALUATED'
    with sqlite3.connect(output/'model.db') as db:
        assert db.execute('SELECT count(*) FROM model_calls').fetchone()[0]==1


def test_changed_second_input_is_rejected_before_network_and_originals_survive(tmp_path):
    execute,seen,runs=callback(change_second=True);output=tmp_path/'changed'
    with pytest.raises(ValueError,match='MODEL_CALL_BINDING_CONFLICT'):
        run_two_passes(output,allowed_root=tmp_path,run_pass=execute)
    assert len(seen)==1 and (output/'pass-1-state/paper.db').is_file()
    assert not (output/'REPEATABILITY.json').exists()


def test_captured_trader_buy_then_t1_owned_engine_exit_two_day_curves_match(tmp_path):
    execute,seen,_=callback(two_days=True)
    result=run_two_passes(tmp_path/'two-days',allowed_root=tmp_path,run_pass=execute)
    assert result['status']=='IDENTICAL_EXECUTION_BYTES' and len(seen)==1
    assert all(row['settlements']==2 and row['buy_fills']==row['sell_fills']==1 for row in result['passes'])


def test_two_equal_empty_curves_are_not_a_control_or_funded_fill_pass(tmp_path):
    execute,seen,_=callback(no_order=True)
    result=run_two_passes(tmp_path/'empty',allowed_root=tmp_path,run_pass=execute)
    assert result['status']=='IDENTICAL_EXECUTION_BYTES' and not result['has_funded_fill_path']
    assert result['control_release_gate']=='NOT_EVALUATED' and len(seen)==1


def test_curve_or_cash_difference_cannot_pass_on_same_model_output(tmp_path):
    execute,seen,_=callback(inject_curve_difference=True)
    result=run_two_passes(tmp_path/'different',allowed_root=tmp_path,run_pass=execute)
    assert result['status']=='EXECUTION_BYTES_DIFFER'
    assert not result['curve_bytes_equal'] and len(seen)==1


def test_second_pass_unknown_call_is_blocked_at_journal_claim_before_client(tmp_path):
    from liangjian_funnel.pipeline.team_v2_judgments import ModelJudgmentJournal
    path=tmp_path/'model.db';ModelJudgmentJournal(path);ro=CapturesOnlyJournal(path)
    with pytest.raises(ValueError,match='SECOND_PASS_NEW_MODEL_CALL_FORBIDDEN'):ro.claim('new','a'*64)
    with pytest.raises(ValueError,match='SECOND_PASS_CAPTURE_FORBIDDEN'):ro.mark_failed('new','TEAM_MODEL_CALL_FAILED')
    client=NoModelRecallClient()
    with pytest.raises(ValueError,match='SECOND_PASS_NETWORK_FORBIDDEN'):client.complete()
    assert client.attempts==1
    with ro._connect() as db:
        with pytest.raises(sqlite3.OperationalError):db.execute("DELETE FROM model_call_claims")


def test_transport_budget_is_original_metadata_readonly_on_second_pass_not_a_new_model_call(tmp_path):
    from liangjian_funnel.pipeline.team_v2_judgments import ModelJudgmentJournal
    path=tmp_path/'model.db';journal=ModelJudgmentJournal(path)
    assert journal.freeze_transport_budget('tail',source_sha256='a'*64,timeout_seconds=12.5)==12.5
    before=path.read_bytes();ro=CapturesOnlyJournal(path)
    assert ro.freeze_transport_budget('tail',source_sha256='a'*64,timeout_seconds=30)==12.5
    assert path.read_bytes()==before
    with pytest.raises(ValueError,match='BINDING_CONFLICT'):
        ro.freeze_transport_budget('tail',source_sha256='b'*64,timeout_seconds=30)
    with pytest.raises(ValueError,match='ORIGINAL_TRANSPORT_BUDGET_REQUIRED'):
        ro.freeze_transport_budget('missing',source_sha256='a'*64,timeout_seconds=30)
    with ro._connect() as db:
        assert db.execute('SELECT count(*) FROM model_call_claims').fetchone()[0]==0
        assert db.execute('SELECT count(*) FROM model_calls').fetchone()[0]==0


@pytest.mark.parametrize('value',[True,-1,31,float('nan'),float('inf')])
def test_transport_budget_never_grants_invalid_or_enlarged_initial_deadline(tmp_path,value):
    from liangjian_funnel.pipeline.team_v2_judgments import ModelJudgmentJournal
    journal=ModelJudgmentJournal(tmp_path/'model.db')
    with pytest.raises(ValueError,match='BUDGET_INVALID'):
        journal.freeze_transport_budget('tail',source_sha256='a'*64,timeout_seconds=value)


def test_existing_or_outside_workspace_is_never_moved_or_overwritten(tmp_path):
    output=tmp_path/'existing';output.mkdir();(output/'keep.txt').write_text('keep')
    with pytest.raises(ValueError,match='NEW_OWNED_OUTPUT_REQUIRED'):
        run_two_passes(output,allowed_root=tmp_path,run_pass=lambda *a:None)
    with pytest.raises(ValueError,match='NEW_OWNED_OUTPUT_REQUIRED'):
        run_two_passes(tmp_path.parent/'outside',allowed_root=tmp_path,run_pass=lambda *a:None)
    assert (output/'keep.txt').read_text()=='keep'
