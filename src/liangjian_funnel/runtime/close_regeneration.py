"""Explicit post-fix close entry. Research first, atomic whole-batch swap last.

No clock override, manual model runner, direct production SQL, or latch unlock.
An old batch is retained when input, raw model, or geometry proof is missing.
"""
from dataclasses import dataclass
from datetime import datetime,date
from pathlib import Path
from collections.abc import Mapping
from zoneinfo import ZoneInfo
import hashlib,json,re,subprocess


@dataclass(frozen=True)
class CloseRegenerationBinding:
    source_run_id:str
    run_id:str
    trade_date:str
    old_plan_ids:tuple[str,...]
    snapshot_id:str
    snapshot_hash:str
    source_summary_sha256:str
    source_data_as_of:datetime
    target_trade_date:str
    wall_clock:datetime
    publication_deadline:datetime
    caller:str
    recovery_attempt:int|None=None
    recovers:str|None=None
    recovery_terminal_sha256:str|None=None
    recovery_authority:str|None=None


def _recovery_proof(output,source_run_id,session,current,*,attempt,recovers,terminal):
    """An explicit new run references, but never changes, the prior attempt.

    0538 authorizes the recovery contract, not automatic recovery. The caller
    must provide the prior terminal receipt; active/unknown runs cannot be
    silently reclaimed. No journal claim or global OPS marker is reset here.
    """
    if attempt is None and recovers is None and terminal is None:return None
    if (type(attempt) is not int or attempt<2 or not isinstance(recovers,str)
            or re.fullmatch(r'[A-Za-z0-9_.-]+',recovers) is None
            or not isinstance(terminal,(str,Path))):
        raise ValueError('REGEN_RECOVERY_ARGUMENTS_INVALID')
    def original(path):
        try:
            if path.is_symlink() or not path.is_file() or path.stat().st_size>65536:
                raise ValueError('REGEN_RECOVERY_EVIDENCE_INVALID')
            raw=path.read_bytes();body=json.loads(raw)
            if not isinstance(body,dict):raise ValueError('REGEN_RECOVERY_EVIDENCE_INVALID')
            return raw,body
        except (OSError,ValueError,TypeError):
            raise ValueError('REGEN_RECOVERY_EVIDENCE_INVALID') from None
    _,prior=original(output/'runs'/f'{recovers}.binding.json')
    terminal_path=Path(terminal)
    if not terminal_path.is_absolute():raise ValueError('REGEN_RECOVERY_EVIDENCE_INVALID')
    raw,finished=original(terminal_path)
    old_head=prior.get('head');old_attempt=prior.get('recovery_attempt')
    if old_attempt is None:old_attempt=1
    expected='regen-'+session.source_trade_date.strftime('%Y%m%d')+'-'+str(old_head)
    if old_attempt!=1:expected+='-r'+str(old_attempt)
    try:
        started=datetime.fromisoformat(prior['wall_clock'])
        ended=datetime.fromisoformat(finished['completed_at'])
        valid_times=(started.utcoffset() is not None and ended.utcoffset() is not None
            and started<=ended<=current)
    except (KeyError,TypeError,ValueError):valid_times=False
    if (not isinstance(old_head,str) or re.fullmatch(r'[0-9a-f]{40}',old_head) is None
            or type(old_attempt) is not int or attempt!=old_attempt+1
            or recovers!=expected or prior.get('run_id')!=recovers
            or prior.get('source_run_id')!=source_run_id
            or prior.get('source_trade_date')!=session.source_trade_date.isoformat()
            or prior.get('target_trade_date')!=session.target_trade_date.isoformat()
            or prior.get('caller')!='cli:run-close' or prior.get('clock_override') is not False
            or finished.get('head')!=old_head
            or finished.get('run_id',recovers)!=recovers
            or type(finished.get('exit')) is not int or finished['exit']==0 or not valid_times):
        raise ValueError('REGEN_RECOVERY_PRIOR_TERMINAL_UNPROVEN')
    summary=output/'runs'/f'{recovers}.json'
    if summary.exists():
        _,body=original(summary)
        if body.get('run_id')!=recovers or body.get('status')=='READY':
            raise ValueError('REGEN_RECOVERY_PRIOR_TERMINAL_UNPROVEN')
    return dict(recovery_attempt=attempt,recovers=recovers,
        recovery_terminal_sha256=hashlib.sha256(raw).hexdigest(),recovery_authority='0538')


def bind_close_regeneration(app,source_run_id,*,now,head,source_trade_date=None,caller='cli:run-close',
        recovery_attempt=None,recovery_from=None,recovery_terminal=None):
    if (not isinstance(source_run_id,str) or re.fullmatch(r'[A-Za-z0-9_.-]+',source_run_id) is None
            or re.fullmatch(r'[0-9a-f]{40}',head) is None or now.utcoffset() is None):
        raise ValueError('REGEN_ARGUMENTS_INVALID')
    from .calendar import closed_research_session
    current=now.astimezone(ZoneInfo('Asia/Shanghai'));session=closed_research_session(current,app.trading_calendar)
    day=session.source_trade_date.isoformat()
    if source_trade_date is not None and source_trade_date!=day:
        raise ValueError('REGEN_SOURCE_TRADE_DATE_MISMATCH')
    if caller!='cli:run-close':raise ValueError('REGEN_CALLER_INVALID')
    path=app.settings.workflow_output_dir/'runs'/f'{source_run_id}.json'
    if path.is_symlink() or path.stat().st_size>1024*1024:raise ValueError('REGEN_SOURCE_SUMMARY_UNPROVEN')
    raw=path.read_bytes();summary=json.loads(raw)
    if (summary.get('run_id')!=source_run_id or summary.get('market_trade_date')!=day
            or summary.get('status')!='READY' or summary.get('slot')!='close'):
        raise ValueError('REGEN_SOURCE_DATE_OR_STATE_INVALID')
    primary=app.settings.research_primary_lane_id
    rows=app.store.list_execution_plans(lane_id=primary,status='PENDING_MORNING_REVIEW')
    ids=tuple(sorted(str(row['plan_id']) for row in rows))
    expected=tuple(sorted(summary.get('plan_publication',{}).get('created',())))
    if not ids or ids!=expected or any(json.loads(row['payload_json']).get('source_run_id')!=source_run_id for row in rows):
        raise ValueError('REGEN_SOURCE_PENDING_SCOPE_CHANGED')
    generation=app.a1_registry.require_active(as_of=current)
    a1_at=generation.as_of.astimezone(ZoneInfo('Asia/Shanghai'))
    if a1_at.date()!=session.source_trade_date or a1_at.hour<15 or a1_at>current:
        raise ValueError('REGEN_FRESH_A1_REQUIRED')
    recovery=_recovery_proof(app.settings.workflow_output_dir,source_run_id,session,current,
        attempt=recovery_attempt,recovers=recovery_from,terminal=recovery_terminal)
    run_id='regen-'+session.source_trade_date.strftime('%Y%m%d')+'-'+head
    if recovery:
        # Progress files retain at most 64 ID characters. Keep the attempt
        # inside that existing contract, rather than colliding after truncation.
        run_id+='-r'+str(recovery_attempt)
        if len(run_id)>64:raise ValueError('REGEN_RECOVERY_RUN_ID_TOO_LONG')
    if (app.settings.workflow_output_dir/'runs'/f'{run_id}.json').exists():
        raise ValueError('REGEN_RUN_ALREADY_RECORDED')
    return CloseRegenerationBinding(source_run_id,run_id,day,ids,generation.snapshot_id,
        generation.snapshot_hash,hashlib.sha256(raw).hexdigest(),a1_at,
        session.target_trade_date.isoformat(),current,session.publication_deadline,caller,**(recovery or {}))


def require_regeneration_publication_window(proof,calendar,*,now):
    from .calendar import closed_research_session
    session=closed_research_session(now,calendar)
    if (session.source_trade_date.isoformat()!=proof.trade_date
            or session.target_trade_date.isoformat()!=proof.target_trade_date
            or session.publication_deadline!=proof.publication_deadline):
        raise ValueError('REGEN_SOURCE_SESSION_CHANGED')


def require_raw_research_pass(result,primary_lane):
    lane=next((lane for lane in result.lanes if lane.lane==primary_lane),None)
    if result.status!='READY' or lane is None or lane.status!='READY':return False
    stage=next((stage for stage in lane.stages if stage.stage=='A2'),None)
    diagnostics=getattr(stage,'diagnostics',None)
    # Only 0498/0501's exact identity/contradictory-explanation exceptions are
    # eligible. Other derived/fallback stage success remains insufficient.
    history=diagnostics.get('provider_validation_history') if isinstance(diagnostics,Mapping) else None
    strict=bool(history and history[-1]==[])
    canonical=diagnostics.get('canonical_validation_history') if isinstance(diagnostics,Mapping) else None
    audits=diagnostics.get('canonicalization_audits',[]) if isinstance(diagnostics,Mapping) else []
    calls=diagnostics.get('team_call_ids',[]) if isinstance(diagnostics,Mapping) else []
    allowed_suffix=bool(canonical and canonical[-1]==[] and audits and audits[-1].get('authority')=='0501'
        and calls and audits[-1].get('source_call_id')==calls[-1]
        and audits[-1].get('rule_id')=='A2_IDENTITY_AND_CONTRADICTION_CANONICAL' and audits[-1].get('canonical_issues')==[]
        and audits[-1].get('actions') and all(a.get('rule_id') in {'SYMBOL_SUFFIX_CANONICAL','CONTRADICTORY_NO_FOCUS_DROPPED'} for a in audits[-1]['actions']))
    return bool(stage is not None and stage.status=='VALIDATED' and (strict or allowed_suffix))


def regeneration_manifest(proof,snapshot,*,raw_pass,created):
    datasets=[]
    for key,value in sorted(snapshot.data.items()):
        if any(token in key for token in ('CNINFO','FINANCIAL','COMPANY','RECENT_DAILY','NEWS_HEAT')):
            canonical=json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),default=str).encode()
            datasets.append(dict(field=key,canonical_projection_sha256=hashlib.sha256(canonical).hexdigest()))
    return dict(source_run_id=proof.source_run_id,source_summary_sha256=proof.source_summary_sha256,
        target_run_id=proof.run_id,trade_date=proof.trade_date,old_plan_ids=list(proof.old_plan_ids),
        source_snapshot_id=proof.snapshot_id,source_snapshot_hash=proof.snapshot_hash,
        reused_snapshot_as_of=snapshot.as_of.isoformat(),reused_datasets=datasets,
        raw_a2_pass=raw_pass,status='REPLACED' if raw_pass and created else 'OLD_BATCH_PRESERVED',
        replaced_plan_ids=list(proof.old_plan_ids) if raw_pass and created else [],new_plan_ids=list(created),
        source_trade_date=proof.trade_date,target_trade_date=proof.target_trade_date,
        source_data_as_of=proof.source_data_as_of.isoformat(),wall_clock=proof.wall_clock.isoformat(),
        completed_wall_clock=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
        **({k:getattr(proof,k) for k in ('recovery_attempt','recovers',
            'recovery_terminal_sha256','recovery_authority')} if proof.recovery_attempt else {}),
        publication_deadline=proof.publication_deadline.isoformat(),caller=proof.caller,
        clock_override=False,snapshot_reuse_basis='SEALED_A1_LATEST_CLOSED_SESSION_HASH_VERIFIED',
        no_full_data_resync_in_this_run=True)


def run_close_regeneration(app,source_run_id,*,source_trade_date=None,
        recovery_attempt=None,recovery_from=None,recovery_terminal=None):
    current=datetime.now(ZoneInfo('Asia/Shanghai'))
    head=subprocess.check_output(['git','-C',str(app.settings.root),'rev-parse','HEAD'],text=True).strip()
    proof=bind_close_regeneration(app,source_run_id,now=current,head=head,source_trade_date=source_trade_date,
        recovery_attempt=recovery_attempt,recovery_from=recovery_from,recovery_terminal=recovery_terminal)
    audit=app.settings.workflow_output_dir/'runs'/f'{proof.run_id}.binding.json'
    try:
        with audit.open('x',encoding='utf-8') as stream:
            json.dump(dict(run_id=proof.run_id,source_run_id=proof.source_run_id,head=head,
                source_trade_date=proof.trade_date,target_trade_date=proof.target_trade_date,
                wall_clock=proof.wall_clock.isoformat(),source_data_as_of=proof.source_data_as_of.isoformat(),
                publication_deadline=proof.publication_deadline.isoformat(),caller=proof.caller,
                source_summary_sha256=proof.source_summary_sha256,snapshot_id=proof.snapshot_id,
                snapshot_hash=proof.snapshot_hash,clock_override=False,
                **({k:getattr(proof,k) for k in ('recovery_attempt','recovers',
                    'recovery_terminal_sha256','recovery_authority')} if proof.recovery_attempt else {})),
                stream,ensure_ascii=False,indent=2)
    except FileExistsError:
        raise ValueError('REGEN_RUN_ALREADY_STARTED') from None
    # This is the sealed data timestamp, not a replacement for the wall clock.
    # The real clock remains recorded and is checked again at publication.
    return app.run_research('close',as_of=proof.source_data_as_of,primary_only=True,from_active_a1=True,
        schedule_comparison=False,run_id_override=proof.run_id,regeneration=proof)
