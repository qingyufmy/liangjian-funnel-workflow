"""Recovery is a new, explicit run; old terminal/claim/binding are not reset."""
from datetime import datetime
from hashlib import sha256
import json
from types import SimpleNamespace as NS

import pytest

from test_close_regeneration_20261021 import app_fixture, NOW, HEAD
from liangjian_funnel.runtime import close_regeneration as regen

OLD_HEAD='b'*40


def prior(app,tmp_path,*,attempt=1):
    run='regen-20261021-'+OLD_HEAD
    if attempt>1:run+='-r'+str(attempt)
    body=dict(run_id=run,head=OLD_HEAD,source_run_id='old',source_trade_date='2026-10-21',
        target_trade_date='2026-10-22',wall_clock='2026-10-21T22:00:00+08:00',
        caller='cli:run-close',clock_override=False)
    if attempt>1:body['recovery_attempt']=attempt
    binding=app.settings.workflow_output_dir/'runs'/f'{run}.binding.json'
    binding.write_text(json.dumps(body))
    receipt=tmp_path/'PROCESS_FINISHED.json'
    terminal=dict(head=OLD_HEAD,exit=1,completed_at='2026-10-21T22:10:00+08:00')
    receipt.write_text(json.dumps(terminal))
    return run,binding,body,receipt,terminal


def bind(app,run,receipt,**kwargs):
    return regen.bind_close_regeneration(app,'old',now=NOW,head=HEAD,
        recovery_attempt=kwargs.pop('recovery_attempt',2),recovery_from=run,
        recovery_terminal=receipt,**kwargs)


def test_new_attempt_pins_failed_terminal_and_preserves_every_old_byte(tmp_path):
    app,_,_=app_fixture(tmp_path)
    run,binding,_,receipt,_=prior(app,tmp_path)
    original=(binding.read_bytes(),receipt.read_bytes())
    before=[app.store.get_execution_plan(str(i)) for i in range(3)]
    proof=bind(app,run,receipt)
    assert proof.run_id=='regen-20261021-'+HEAD+'-r2'
    assert proof.recovery_attempt==2 and proof.recovers==run
    assert proof.recovery_authority=='0538'
    assert proof.recovery_terminal_sha256==sha256(original[1]).hexdigest()
    assert (binding.read_bytes(),receipt.read_bytes())==original
    assert [app.store.get_execution_plan(str(i)) for i in range(3)]==before


@pytest.mark.parametrize('change',[
    {'exit':0},{'exit':True},{'head':'c'*40},{'run_id':'another-run'},
    {'completed_at':'2026-10-21T21:59:59+08:00'},
    {'completed_at':'2026-10-22T02:00:00+08:00'},
    {'completed_at':'2026-10-21T22:10:00'}, {'completed_at':'not-time'},
])
def test_cannot_recover_running_success_other_run_or_unproven_clock(tmp_path,change):
    app,_,_=app_fixture(tmp_path);run,_,_,receipt,terminal=prior(app,tmp_path)
    receipt.write_text(json.dumps({**terminal,**change}))
    with pytest.raises(ValueError,match='REGEN_RECOVERY_PRIOR_TERMINAL_UNPROVEN'):
        bind(app,run,receipt)


@pytest.mark.parametrize('change',[
    {'source_run_id':'different'},{'source_trade_date':'2026-10-20'},
    {'target_trade_date':'2026-10-23'},{'clock_override':True},
    {'caller':'manual'},{'run_id':'other'},{'head':'not-a-head'},
    {'wall_clock':'bad'},{'recovery_attempt':True},{'recovery_attempt':3},
])
def test_recovery_requires_exact_predecessor_source_session_and_attempt(tmp_path,change):
    app,_,_=app_fixture(tmp_path);run,binding,body,receipt,_=prior(app,tmp_path)
    binding.write_text(json.dumps({**body,**change}))
    with pytest.raises(ValueError,match='REGEN_RECOVERY_PRIOR_TERMINAL_UNPROVEN'):
        bind(app,run,receipt)


@pytest.mark.parametrize('attempt',[0,1,-1,True,2.0,'2'])
def test_recovery_attempt_is_explicit_integer_at_least_two(tmp_path,attempt):
    app,_,_=app_fixture(tmp_path);run,_,_,receipt,_=prior(app,tmp_path)
    with pytest.raises(ValueError,match='REGEN_RECOVERY_ARGUMENTS_INVALID'):
        bind(app,run,receipt,recovery_attempt=attempt)


def test_third_attempt_only_recovers_second_attempt_not_first(tmp_path):
    app,_,_=app_fixture(tmp_path);run,_,_,receipt,_=prior(app,tmp_path,attempt=2)
    assert bind(app,run,receipt,recovery_attempt=3).run_id.endswith('-r3')
    with pytest.raises(ValueError,match='REGEN_RECOVERY_PRIOR_TERMINAL_UNPROVEN'):
        bind(app,run,receipt,recovery_attempt=4)


@pytest.mark.parametrize('kind',['missing','not-json','too-large','not-object','relative'])
def test_terminal_evidence_missing_or_invalid_cannot_start(tmp_path,kind):
    app,_,_=app_fixture(tmp_path);run,_,_,receipt,_=prior(app,tmp_path)
    if kind=='missing':receipt.unlink()
    elif kind=='not-json':receipt.write_text('broken')
    elif kind=='too-large':receipt.write_text(' '*65537)
    elif kind=='not-object':receipt.write_text('[]')
    else:receipt='PROCESS_FINISHED.json'
    with pytest.raises(ValueError,match='REGEN_RECOVERY_EVIDENCE_INVALID'):
        bind(app,run,receipt)


def test_ready_prior_summary_blocks_recovery_even_with_nonzero_process_exit(tmp_path):
    app,_,_=app_fixture(tmp_path);run,_,_,receipt,_=prior(app,tmp_path)
    (app.settings.workflow_output_dir/'runs'/f'{run}.json').write_text(json.dumps(dict(run_id=run,status='READY')))
    with pytest.raises(ValueError,match='REGEN_RECOVERY_PRIOR_TERMINAL_UNPROVEN'):
        bind(app,run,receipt)


def test_recovery_does_not_bypass_source_a1_scope_or_0630_deadline(tmp_path):
    app,generation,_=app_fixture(tmp_path);run,_,_,receipt,_=prior(app,tmp_path)
    with pytest.raises(ValueError,match='CLOSE_RESEARCH_PUBLICATION_DEADLINE_REACHED'):
        regen.bind_close_regeneration(app,'old',now=NOW.replace(day=22,hour=6,minute=30),head=HEAD,
            recovery_attempt=2,recovery_from=run,recovery_terminal=receipt)
    generation.as_of=NOW.replace(day=16)
    with pytest.raises(ValueError,match='REGEN_FRESH_A1_REQUIRED'):bind(app,run,receipt)


def test_actual_entry_writes_new_binding_once_and_calls_existing_research(tmp_path,monkeypatch):
    app,_,_=app_fixture(tmp_path);app.settings.root=tmp_path
    run,old_binding,_,receipt,_=prior(app,tmp_path)
    old_bytes=(old_binding.read_bytes(),receipt.read_bytes())
    calls=[]
    app.run_research=lambda *args,**kwargs:calls.append((args,kwargs)) or {'status':'READY'}
    class Clock:
        @staticmethod
        def now(_):return NOW
    monkeypatch.setattr(regen,'datetime',Clock)
    # fromisoformat stays the real parser, while only actual wall time is stubbed.
    Clock.fromisoformat=staticmethod(datetime.fromisoformat)
    monkeypatch.setattr(regen.subprocess,'check_output',lambda *args,**kwargs:HEAD+'\n')
    args=dict(recovery_attempt=2,recovery_from=run,recovery_terminal=receipt)
    assert regen.run_close_regeneration(app,'old',**args)=={'status':'READY'}
    assert len(calls)==1
    _,kwargs=calls[0];proof=kwargs['regeneration']
    assert kwargs['run_id_override']==proof.run_id
    assert kwargs['from_active_a1'] is True and kwargs['schedule_comparison'] is False
    binding=app.settings.workflow_output_dir/'runs'/f'{proof.run_id}.binding.json'
    saved=json.loads(binding.read_bytes())
    assert saved['recovery_authority']=='0538' and saved['recovers']==run
    assert saved['recovery_terminal_sha256']==sha256(old_bytes[1]).hexdigest()
    before=binding.read_bytes()
    with pytest.raises(ValueError,match='REGEN_RUN_ALREADY_STARTED'):
        regen.run_close_regeneration(app,'old',**args)
    assert len(calls)==1 and binding.read_bytes()==before
    assert (old_binding.read_bytes(),receipt.read_bytes())==old_bytes
    manifest=regen.regeneration_manifest(proof,NS(data={},as_of=NOW),raw_pass=False,created=[])
    assert manifest['recovers']==run and manifest['status']=='OLD_BATCH_PRESERVED'


def test_parser_requires_explicit_recovery_flags_without_changing_defaults():
    from liangjian_funnel.cli import build_parser
    plain=build_parser().parse_args(['run-close'])
    assert plain.recovery_attempt is None and plain.recovery_from is None and plain.recovery_terminal is None
    args=build_parser().parse_args(['run-close','--regenerate-from','old','--recovery-attempt','2',
        '--recovery-from','prior','--recovery-terminal','/tmp/result.json'])
    assert args.recovery_attempt==2 and args.recovery_from=='prior'


def test_actual_cli_forwards_recovery_and_refuses_it_on_natural_close(tmp_path,monkeypatch,capsys):
    import liangjian_funnel.cli as cli
    from test_cli_workflow_coverage import _FakeApplication,_settings,_ready_response
    calls=[]
    monkeypatch.setattr(cli,'WorkflowApplication',_FakeApplication)
    monkeypatch.setattr(regen,'run_close_regeneration',
        lambda app,source,**kwargs:calls.append((source,kwargs)) or _ready_response())
    assert cli.main(['run-close','--regenerate-from','old','--recovery-attempt','2',
        '--recovery-from','previous','--recovery-terminal',str(tmp_path/'terminal.json')],
        settings=_settings(tmp_path))==0
    assert calls==[('old',dict(source_trade_date=None,recovery_attempt=2,
        recovery_from='previous',recovery_terminal=tmp_path/'terminal.json'))]
    assert cli.main(['run-close','--recovery-attempt','2'],settings=_settings(tmp_path))==2
    assert len(calls)==1 and not _FakeApplication.calls


def test_attempt_id_remains_distinct_after_existing_progress_filename_limit(tmp_path):
    app,_,_=app_fixture(tmp_path);run,_,_,receipt,_=prior(app,tmp_path)
    second=bind(app,run,receipt).run_id
    next_run,_,_,next_receipt,_=prior(app,tmp_path,attempt=2)
    third=bind(app,next_run,next_receipt,recovery_attempt=3).run_id
    assert len(second)<=64 and len(third)<=64 and second[:64]!=third[:64]
