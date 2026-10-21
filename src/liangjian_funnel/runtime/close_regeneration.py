"""Explicit post-fix close entry. Research first, atomic whole-batch swap last.

No clock override, manual model runner, direct production SQL, or latch unlock.
An old batch is retained when input, raw model, or geometry proof is missing.
"""
from dataclasses import dataclass
from datetime import datetime,time
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


def bind_close_regeneration(app,source_run_id,*,now,head):
    if (not isinstance(source_run_id,str) or re.fullmatch(r'[A-Za-z0-9_.-]+',source_run_id) is None
            or re.fullmatch(r'[0-9a-f]{40}',head) is None or now.utcoffset() is None):
        raise ValueError('REGEN_ARGUMENTS_INVALID')
    current=now.astimezone(ZoneInfo('Asia/Shanghai'));day=current.date().isoformat()
    if current.time().replace(tzinfo=None)<time(15,10) or not app.trading_calendar.is_trading_day(current.date()):
        raise ValueError('REGEN_CLOSE_WINDOW_REQUIRED')
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
    if a1_at.date()!=current.date() or a1_at.hour<15:
        raise ValueError('REGEN_FRESH_A1_REQUIRED')
    run_id='regen-'+current.strftime('%Y%m%d')+'-'+head
    if (app.settings.workflow_output_dir/'runs'/f'{run_id}.json').exists():
        raise ValueError('REGEN_RUN_ALREADY_RECORDED')
    return CloseRegenerationBinding(source_run_id,run_id,day,ids,generation.snapshot_id,
        generation.snapshot_hash,hashlib.sha256(raw).hexdigest())


def require_raw_research_pass(result,primary_lane):
    lane=next((lane for lane in result.lanes if lane.lane==primary_lane),None)
    if result.status!='READY' or lane is None or lane.status!='READY':return False
    stage=next((stage for stage in lane.stages if stage.stage=='A2'),None)
    diagnostics=getattr(stage,'diagnostics',None)
    # A successful derived/normalized stage is explicitly not a raw pass.
    history=diagnostics.get('provider_validation_history') if isinstance(diagnostics,Mapping) else None
    return bool(stage is not None and stage.status=='VALIDATED' and history and history[-1]==[])


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
        clock_override=False,snapshot_reuse_basis='SEALED_FRESH_A1_SAME_DAY_HASH_VERIFIED',
        no_full_data_resync_in_this_run=True)


def run_close_regeneration(app,source_run_id):
    current=datetime.now(ZoneInfo('Asia/Shanghai'))
    head=subprocess.check_output(['git','-C',str(app.settings.root),'rev-parse','HEAD'],text=True).strip()
    proof=bind_close_regeneration(app,source_run_id,now=current,head=head)
    return app.run_research('close',as_of=current,primary_only=True,from_active_a1=True,
        schedule_comparison=False,run_id_override=proof.run_id,regeneration=proof)
