"""Standalone local SQLite fixtures; no real source, strategy, model or timer."""
from datetime import datetime
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from liangjian_funnel.runtime import shadow_reporting as mod
from test_w3_shadow_a5_observer import make_files

DAY='2026-10-12'
def at(clock):return datetime.fromisoformat(DAY+'T'+clock+'+08:00')


def fixture(tmp_path):
    database,approved,node,log,_=make_files(tmp_path)
    # This is a fresh local fixture day transformation, never production backfill.
    from liangjian_funnel.runtime.shadow_evidence import ShadowEvidenceLedger
    from test_w3_shadow_day_adapter import source
    from test_w2_shadow_evidence import evidence
    view,census,production=source()
    eventrow=dict(event_id='e1',event_key='k1',lane_id='lane_1',minute_end=at('10:00:00').isoformat(),
        action='WAIT',reason_code='NO_PULLBACK',effective=0,payload_json=json.dumps({'plan_id':'p1','symbol':'600000.SH'}),
        created_at=at('10:00:20').isoformat())
    view['minutes'][0]['baseline_records'][0]['event_sha256']=hashlib.sha256(mod._raw(eventrow)).hexdigest()
    ledger=ShadowEvidenceLedger(tmp_path/'shadow.sqlite',tmp_path/'shadow.jsonl')
    for item in view['signals']:
        ledger.record_signal({**item['signal'],'schema':'a4-shadow-signal/1','cohort':'REALTIME_SHADOW'},
            {**census['plans'][0],'valid_from':census['plans'][0]['activated_at'],'stop_level':9.5},observed_at=at('10:00:40'))
    ledger.record_minute({**view['minutes'][0],'elapsed_ms':1},observed_at=at('10:00:40'))
    con=sqlite3.connect(database)
    con.execute('CREATE TABLE execution_plans(plan_id TEXT,lane_id TEXT,symbol TEXT,status TEXT,plan_version INTEGER,valid_from TEXT,expires_at TEXT,payload_json TEXT,created_at TEXT,updated_at TEXT)')
    con.execute('CREATE TABLE monitor_events(event_id TEXT,event_key TEXT,lane_id TEXT,minute_end TEXT,action TEXT,reason_code TEXT,effective INTEGER,payload_json TEXT,created_at TEXT)')
    plan={'plan_id':'p1','symbol':'600000.SH','target_trade_date':DAY,'strategy_profile':'TREND_MA5',
          'strategy_version':'trend-ma5/2','entry_zone_lower':10,'entry_zone_upper':11}
    con.execute('INSERT INTO execution_plans VALUES(?,?,?,?,?,?,?,?,?,?)',('p1','lane_1','600000.SH','EXPIRED',1,at('09:32:00').isoformat(),at('15:00:00').isoformat(),json.dumps(plan),at('09:31:00').isoformat(),at('15:00:00').isoformat()))
    event={'plan_id':'p1','symbol':'600000.SH'}
    con.execute('INSERT INTO monitor_events VALUES(?,?,?,?,?,?,?,?,?)',('e1','k1','lane_1',at('10:00:00').isoformat(),'WAIT','NO_PULLBACK',0,json.dumps(event),at('10:00:20').isoformat()))
    con.commit();con.close()
    return mod.ReportingPaths(state_db=database,shadow_db=tmp_path/'shadow.sqlite',shadow_jsonl=tmp_path/'shadow.jsonl',
        lanes=('lane_1',),approved_output_root=approved,node_receipt_path=node,node_log_path=log,
        archive_root=tmp_path/'archive',report_root=tmp_path/'docs/shadow',bridge_outbox=tmp_path/'bridge/shadow-outbox')


def test_before_draft_and_a5_wait_do_not_emit_formal(tmp_path):
    cfg=fixture(tmp_path)
    assert mod.run_reporting_tick(cfg,observed_at=at('15:29:59'))['status']=='WAIT_DRAFT'
    result=mod.run_reporting_tick(cfg,observed_at=at('15:30:00'))
    assert result['status']=='WAIT_A5'
    assert (cfg.archive_root/DAY/'draft.json').is_file()
    assert not (cfg.report_root/f'SHADOW_DAILY_{DAY}.md').exists()


def test_deadline_fallback_idempotent_no_production_write(tmp_path):
    cfg=fixture(tmp_path);before=cfg.state_db.read_bytes()
    mod.run_reporting_tick(cfg,observed_at=at('15:30:00'))
    cfg.node_receipt_path.unlink()
    final=mod.run_reporting_tick(cfg,observed_at=at('16:45:00'))
    assert final['status']=='FORMAL_WRITTEN' and final['a5_status']=='A5_NOT_COMPLETE_OR_AMBIGUOUS'
    report=json.loads((cfg.archive_root/DAY/'report.json').read_bytes())
    assert report['execution_evidence_status']=='DATA_LIMITED'
    assert report['trigger_research_only'] is True
    assert report['groups']['V1']['TREND_MA5']['fill_count'] is None
    assert report['groups']['V1']['TREND_MA5']['returns']['T1']['mean'] is None
    assert cfg.state_db.read_bytes()==before
    files={str(p):p.read_bytes() for r in (cfg.archive_root,cfg.report_root,cfg.bridge_outbox) for p in r.rglob('*') if p.is_file()}
    again=mod.run_reporting_tick(cfg,observed_at=at('16:45:01'))
    assert again['status']=='ALREADY_WRITTEN'
    assert all(Path(p).read_bytes()==raw for p,raw in files.items())


def test_a5_degraded_success_can_finalize_with_exact_original_clock(tmp_path):
    cfg=fixture(tmp_path);mod.run_reporting_tick(cfg,observed_at=at('15:30:00'))
    done=mod.run_reporting_tick(cfg,observed_at=at('16:08:00'))
    assert done['status']=='FORMAL_WRITTEN' and done['a5_status']=='A5_COMPLETED_OBSERVED'
    package=json.loads((cfg.archive_root/DAY/'report.json').read_bytes())
    assert package['a5_observation']['completed_at']==at('16:06:01').isoformat()
    assert package['a5_observation']['report_quality']=='DEGRADED'
    assert package['notifications']==0 and package['production_mutation'] is False
    proof=json.loads((cfg.report_root/f'PRODUCTION_EQUIVALENCE_{DAY}.json').read_bytes())
    assert proof['production_count']==1
    assert proof['status']=='MATCHED' and proof['difference_count']==0
    assert proof['production_input_sha256']!=hashlib.sha256(cfg.state_db.read_bytes()).hexdigest()


def test_late_first_start_cannot_claim_1530_or_a5_on_time(tmp_path):
    cfg=fixture(tmp_path)
    done=mod.run_reporting_tick(cfg,observed_at=at('16:46:00'))
    assert done['draft_status']=='LATE_DRAFT'
    assert done['a5_status']=='A5_NOT_COMPLETE_OR_AMBIGUOUS' and done['deadline_missed'] is True


def test_failure_is_shadow_only_and_missing_db_not_created(tmp_path):
    cfg=fixture(tmp_path);cfg.state_db.unlink()
    result=mod.run_reporting_tick(cfg,observed_at=at('16:45:00'))
    assert result['status']=='SHADOW_DRAFT_FAILED'
    assert result['production_mutation'] is False and not cfg.state_db.exists()


def test_output_cannot_alias_inputs_and_report_tamper_not_idempotent(tmp_path):
    from dataclasses import replace
    cfg=fixture(tmp_path)
    bad=replace(cfg,report_root=cfg.state_db.parent)
    assert mod.run_reporting_tick(bad,observed_at=at('15:30:00'))['status']=='SHADOW_SETUP_FAILED'
    mod.run_reporting_tick(cfg,observed_at=at('16:45:00'))
    (cfg.report_root/f'SHADOW_DAILY_{DAY}.md').write_text('tampered',encoding='utf8')
    assert mod.run_reporting_tick(cfg,observed_at=at('16:45:01'))['status']=='SHADOW_OUTPUT_BINDING_FAILED'


def test_derived_or_no_pit_never_marks_fill_ready(tmp_path):
    cfg=fixture(tmp_path)
    result=mod.run_reporting_tick(cfg,observed_at=at('16:45:00'))
    report=json.loads((cfg.archive_root/DAY/'report.json').read_bytes())
    assert result['status']=='FORMAL_WRITTEN'
    assert 'PIT_IDENTITY_UNPROVEN:p1' in report['gap_codes']
    assert report['trigger_research_only'] and report['fill_evidence_ready'] is False


def test_cli_help_has_no_runtime_or_source_calls(capsys):
    path=Path(__file__).parents[1]/'scripts/run_shadow_reporting.py'
    spec=importlib.util.spec_from_file_location('report_cli',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    with pytest.raises(SystemExit) as exc:module.main(['--help'])
    assert exc.value.code==0
    assert '--state-db' in capsys.readouterr().out


def test_saved_draft_has_independent_pin_and_tampering_is_not_finalized(tmp_path):
    cfg=fixture(tmp_path);mod.run_reporting_tick(cfg,observed_at=at('15:30:00'))
    p=cfg.archive_root/DAY/'draft.json';draft=json.loads(p.read_bytes())
    draft['report']['production_equivalence']['status']='MATCHED';draft['report']['production_equivalence']['difference_count']=999
    p.write_text(json.dumps(draft),encoding='utf8')
    assert mod.run_reporting_tick(cfg,observed_at=at('16:45:00'))['status']=='SHADOW_DRAFT_BINDING_FAILED'


def test_finished_manifest_may_not_redirect_reads_or_replay_future(tmp_path):
    cfg=fixture(tmp_path);mod.run_reporting_tick(cfg,observed_at=at('16:08:00'))
    assert mod.run_reporting_tick(cfg,observed_at=at('16:07:00'))['status']=='SHADOW_OUTPUT_BINDING_FAILED'
    p=cfg.archive_root/DAY/'final-manifest.json';body=json.loads(p.read_bytes())
    extra=tmp_path/'unrelated.json';extra.write_bytes(b'{}')
    body['outputs'][str(extra)]=hashlib.sha256(b'{}').hexdigest();p.write_text(json.dumps(body),encoding='utf8')
    assert mod.run_reporting_tick(cfg,observed_at=at('16:09:00'))['status']=='SHADOW_OUTPUT_BINDING_FAILED'


def test_production_source_has_read_only_uri_and_no_checkpoint(tmp_path,monkeypatch):
    cfg=fixture(tmp_path);connect=sqlite3.connect;calls=[]
    def spy(database_uri,**kw):
        calls.append((database_uri,kw));return connect(database_uri,**kw)
    monkeypatch.setattr(mod.sqlite3,'connect',spy)
    mod.read_reporting_day(state_db=cfg.state_db,lanes=cfg.lanes,trade_date=DAY,observed_at=at('15:30:00'))
    assert calls[0][0].endswith('?mode=ro') and calls[0][1]['uri'] is True
    assert 'immutable' not in calls[0][0]


def test_weekly_missing_day_is_written_null_not_fabricated_complete(tmp_path):
    cfg=fixture(tmp_path);now=datetime.fromisoformat('2026-10-16T16:00:00+08:00')
    result=mod.run_weekly_reporting(cfg,trading_days=['2026-10-12','2026-10-13','2026-10-14','2026-10-15','2026-10-16'],observed_at=now)
    assert result['status']=='WEEK_WRITTEN' and result['coverage_status']=='DATA_LIMITED'
    report=json.loads((cfg.archive_root/'week-2026-10-16/report.json').read_bytes())
    assert len(report['missing_days'])==5 and report['groups']['V1']['TREND_MA5']['fill_plan_days'] is None
    assert mod.run_weekly_reporting(cfg,trading_days=report['trading_days'],observed_at=now)['status']=='ALREADY_WRITTEN'


def test_a5_file_read_finishes_after_fence_cannot_finalize_ready(tmp_path):
    from liangjian_funnel.runtime.shadow_a5_observer import read_a5_observation
    cfg=fixture(tmp_path)
    result=read_a5_observation(trade_date=DAY,observed_at=at('16:44:59'),state_db=cfg.state_db,
        approved_output_root=cfg.approved_output_root,node_receipt_path=cfg.node_receipt_path,
        node_log_path=cfg.node_log_path,observation_clock=lambda:at('16:45:01'))
    assert result['status']=='DATA_LIMITED' and result['candidate'] is None


def test_actual_a5_observation_clock_retains_completion_clock(tmp_path):
    from liangjian_funnel.runtime.shadow_a5_observer import read_a5_observation
    cfg=fixture(tmp_path)
    result=read_a5_observation(trade_date=DAY,observed_at=at('16:08:00'),state_db=cfg.state_db,
        approved_output_root=cfg.approved_output_root,node_receipt_path=cfg.node_receipt_path,
        node_log_path=cfg.node_log_path,observation_clock=lambda:at('16:08:02'))
    assert result['observed_at']==at('16:08:02').isoformat()
    assert result['completed_at']==at('16:06:01').isoformat()


def test_missing_shadow_ledger_still_reports_gap_not_empty_zero(tmp_path):
    cfg=fixture(tmp_path);cfg.shadow_db.unlink();cfg.shadow_jsonl.unlink()
    result=mod.run_reporting_tick(cfg,observed_at=at('16:45:00'))
    assert result['status']=='FORMAL_WRITTEN'
    report=json.loads((cfg.archive_root/DAY/'report.json').read_bytes())
    assert report['status']=='DATA_LIMITED'
    assert report['observed_minutes'] is None and report['groups']['V1']['TREND_MA5']['fill_count'] is None
    assert report['production_equivalence']['status']=='DATA_LIMITED'
    assert not cfg.shadow_db.exists()


def test_failed_production_and_shadow_writer_leave_original_actions_clocks(tmp_path,monkeypatch):
    cfg=fixture(tmp_path);body=json.loads(cfg.node_receipt_path.read_bytes())
    body['recentJobRuns'][0].update(status='failed',exitCode=2,reason='NON_ZERO_EXIT')
    cfg.node_receipt_path.write_text(json.dumps(body),encoding='utf8')
    originals={p:p.read_bytes() for p in (cfg.state_db,cfg.node_receipt_path,cfg.node_log_path)}
    mod.run_reporting_tick(cfg,observed_at=at('15:30:00'))
    assert mod.run_reporting_tick(cfg,observed_at=at('16:08:00'))['status']=='WAIT_A5'
    def fail(*args):raise OSError('fixture-only')
    monkeypatch.setattr(mod,'_write_new',fail)
    assert mod.run_reporting_tick(cfg,observed_at=at('16:45:00'))['status']=='SHADOW_REPORT_FAILED'
    assert all(p.read_bytes()==raw for p,raw in originals.items())


def test_technical_trigger_cohorts_do_not_become_execution_triggers():
    rows=[dict(signal=dict(plan_id='p1',variant_id='V1',minute=at('10:00:00').isoformat(),
              inputs_origin='SHADOW_PREOPEN_SIDECAR',technical_trigger=True,technical_action='BUY_SIGNAL',
              profile='TREND_MA5',technical_trigger_basis='ISOLATED_RESEARCH_ENGINE_NOT_PIT_OR_FILL')),
          dict(signal=dict(plan_id='p1',variant_id='V1',minute=at('10:01:00').isoformat(),
              inputs_origin='SHADOW_PREOPEN_SIDECAR',technical_trigger=True,technical_action='BUY_SIGNAL',
              profile='TREND_MA5',technical_trigger_basis='ISOLATED_RESEARCH_ENGINE_NOT_PIT_OR_FILL')),
          dict(signal=dict(plan_id='p2',variant_id='V1',minute=at('10:01:00').isoformat(),
              inputs_origin='PLAN_FROZEN',technical_trigger=True,technical_action='BUY_SIGNAL',
              profile='TREND_MA5',technical_trigger_basis='ISOLATED_RESEARCH_ENGINE_NOT_PIT_OR_FILL'))]
    groups=mod._technical_cohorts({'signals':rows},DAY)
    assert groups['SHADOW_PREOPEN_SIDECAR']['technical_first_triggers']==1
    assert groups['PLAN_FROZEN']['technical_first_triggers']==1
    assert groups['PLAN_FROZEN']['groups']['V1']['TREND_MA5']['technical_first_triggers']==1
    assert all(g['execution_ready'] is False for g in groups.values())


def test_self_declared_technical_boolean_without_engine_basis_is_not_trigger():
    row=dict(signal=dict(plan_id='p1',variant_id='V1',minute=at('10:00:00').isoformat(),
        inputs_origin='PLAN_FROZEN',profile='TREND_MA5',technical_trigger=True,technical_action='WAIT'))
    group=mod._technical_cohorts({'signals':[row]},DAY)['PLAN_FROZEN']
    assert group['technical_first_triggers']==0 and group['unproven_technical_rows']==1


def test_cli_actual_subprocess_fallback(tmp_path):
    import subprocess,sys
    cfg=fixture(tmp_path);before=cfg.state_db.read_bytes()
    path=Path(__file__).parents[1]/'scripts/run_shadow_reporting.py'
    args=[sys.executable,'-B',str(path),'--state-db',str(cfg.state_db),'--shadow-db',str(cfg.shadow_db),
        '--shadow-jsonl',str(cfg.shadow_jsonl),'--lane','lane_1','--approved-output-root',str(cfg.approved_output_root),
        '--node-log-root',str(cfg.node_log_path.parent),'--archive-root',str(cfg.archive_root),
        '--report-root',str(cfg.report_root),'--bridge-outbox',str(cfg.bridge_outbox),
        '--as-of',at('16:45:00').isoformat()]
    child=subprocess.run(args,capture_output=True,encoding='utf8')
    assert child.returncode==0 and json.loads(child.stdout)['a5_status']=='A5_NOT_COMPLETE_OR_AMBIGUOUS'
    assert cfg.state_db.read_bytes()==before


def test_monday_preopen_template_is_once_and_session_reads_sidecar():
    root=Path(__file__).parents[1]/'config/deploy/shadow-reporting'
    timer=(root/'liangjian-shadow-preopen-20261012.timer.example').read_text(encoding='utf8')
    service=(root/'liangjian-shadow-preopen-20261012.service.example').read_text(encoding='utf8')
    session=(root/'liangjian-shadow-session-20261012.service.example').read_text(encoding='utf8')
    assert 'OnCalendar=2026-10-12 08:40:00 Asia/Shanghai' in timer and 'Persistent=false' in timer
    assert 'run_shadow_preopen_sidecar.py' in service and '--target-trade-date 2026-10-12' in service
    assert '--fact-db ${SHADOW_FACT_DB}' in service and '--preopen-inputs-db ${PREOPEN_INPUTS_DB}' in service
    assert '--receipt-json ${PREOPEN_RECEIPT}' in service
    assert '--preopen-inputs-db ${PREOPEN_INPUTS_DB}' in session and '--monitor-latest ${SHADOW_MONITOR_LATEST}' in session
    assert 'After=liangjian-shadow-preopen-20261012.service' in session and 'Requires=' not in session
    assert 'run_research' not in service and 'RuntimeStore' not in service


def test_first_draft_source_read_crossing_fence_cannot_approve_old_a5(tmp_path):
    from dataclasses import replace
    cfg=replace(fixture(tmp_path),node_receipt_path=None)
    result=mod.run_reporting_tick(cfg,observed_at=at('16:44:59'),observation_clock=lambda:at('16:45:01'))
    assert result['status']=='FORMAL_WRITTEN' and result['deadline_missed'] is True
    assert result['a5_status']=='A5_NOT_COMPLETE_OR_AMBIGUOUS'
    report=json.loads((cfg.archive_root/DAY/'report.json').read_bytes())
    assert report['formal_written_at']>=report['draft_captured_at']
