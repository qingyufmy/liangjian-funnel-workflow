import sqlite3
import pytest
from h1_common_broker_contract import assert_atomic_failure,assert_replay_idempotent,assert_terminal_protected
from test_h1_entry_settlement_invariants import seeded,at
from liangjian_funnel.workflow import WorkflowApplication
from liangjian_funnel.runtime.state import A4SignalStatus


@pytest.mark.parametrize('scenario',['ATOMIC_FAULT','IDEMPOTENT_RETRY','TERMINAL_PROTECTION'])
def test_legacy_broker_shared_invariants(tmp_path,scenario):
    store,app,bar=seeded(tmp_path)
    def attempt():return WorkflowApplication._settle_prior_signals(app,'lane_1','600621.SH',bar)
    def snapshot():return {'cash':store.get_account('paper:lane_1')['cash'],
        'fills':store.list_fills('paper:lane_1'),'positions':store.list_positions('paper:lane_1'),
        'terminal':store.get_a4_signal_lifecycle('h1-entry')}
    if scenario=='ATOMIC_FAULT':
        with sqlite3.connect(tmp_path/'h1-local.sqlite3') as c:
            c.execute("CREATE TRIGGER shared_fault AFTER INSERT ON virtual_fills BEGIN SELECT RAISE(ABORT,'H1_SHARED_FAULT'); END")
        assert_atomic_failure(attempt,snapshot,allowed_errors={'STATE_CONFLICT'})
    elif scenario=='IDEMPOTENT_RETRY':
        assert_replay_idempotent(attempt,snapshot)
    else:
        store.mark_a4_signal_terminal('h1-entry',A4SignalStatus.UNFILLED,reason_code='LOCAL_CONTRACT_FIXTURE_ONLY',at=at('11:18:00'))
        assert_terminal_protected(attempt,snapshot,allowed_errors={'A4_TERMINAL_STATE_CONFLICT'})
