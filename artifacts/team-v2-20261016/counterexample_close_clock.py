"""Unresolved natural-clock counterexample; explicit only, not a green gate.

Synthetic original source fixtures, not a historical recovery or production
observation. No real HTTP/model/customer notification or production DB access.
"""
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'tests'))
from test_team_v2_control_minute import prepared,run
from test_team_v2_paper_ledger import stamp,bar


def test_natural_last_monitor_must_leave_a_proven_close_mark_for_settlement(tmp_path):
    args,seen,raw,clock=prepared(tmp_path)
    run(args,bar(tm='10:00:00'))
    clock[0]=stamp(tm='10:02:00');run(args,bar(tm='10:01:00',volume_shares=300000))
    clock[0]=stamp(tm='10:03:00');filled=run(args,bar(tm='10:02:00',volume_shares=300000))
    assert filled['fills'] and len(seen)==1
    # The last scheduled monitor is 15:00, and its proven cutoff is 14:59.
    clock[0]=stamp(tm='15:00:00')
    last=run(args,bar(tm='14:59:00'),model_enabled=False)
    assert last['execution_cutoff'].startswith('2026-10-19T14:59:00')
    clock[0]=stamp(tm='15:30:00');settlement=args['broker'].settle('2026-10-19')
    print('NATURAL_CLOSE_COUNTEREXAMPLE',settlement['status'],settlement['missing_current_marks'])
    # Desired release invariant. Current code must fail until a separately
    # proven 15:00 close-bar edge is integrated; never fabricate the mark.
    assert settlement['status']=='COMPLETE'
