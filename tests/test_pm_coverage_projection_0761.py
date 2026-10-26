import importlib.util
import json
from pathlib import Path
import sqlite3


def test_server_shortlist_is_not_a_model_rejection_and_keeps_actual_basis(tmp_path):
    path=Path(__file__).resolve().parents[1]/'scripts/team_ui_pm.py'
    spec=importlib.util.spec_from_file_location('pm_projection_0761',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    dbpath=tmp_path/'pm.db'
    with sqlite3.connect(dbpath) as db:
        db.execute('CREATE TABLE pm_decisions(account_id,trade_date,meeting_id,result_json)')
        audit=dict(selected_plan_ids=['selected'],excluded=[dict(plan_id='capacity',reason_code='PM_SHORTLIST_CUTOFF',exclusion_basis='SHORTLIST_CAP'),
            dict(plan_id='risk',reason_code='PM_SHORTLIST_CUTOFF',exclusion_basis='GEOMETRY_OR_A6_INELIGIBLE')])
        db.execute('INSERT INTO pm_decisions VALUES (?,?,?,?)',('local-paper','2026-10-26','actual-pm',json.dumps(dict(session='PM_PREMARKET',shortlist_audit=audit))))
    def connect(*args):
        db=sqlite3.connect(f'file:{dbpath}?mode=ro',uri=True);db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=1');return db
    rows=[dict(id=p) for p in ['selected','capacity','risk','unknown']]
    module.annotate_pm(tmp_path,'2026-10-26',rows,connect)
    assert rows[0]['pm_coverage']['status']=='PM_SHORTLIST'
    for row,basis in zip(rows[1:3],['SHORTLIST_CAP','GEOMETRY_OR_A6_INELIGIBLE']):
        assert row['pm_coverage']['status']=='NOT_SENT_TO_PM' and row['pm_coverage']['model_rejection'] is False
        assert row['pm_coverage']['exclusion_basis']==basis
    assert 'pm_coverage' not in rows[3]
