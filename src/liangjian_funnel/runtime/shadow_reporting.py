"""Independent W3 local reporting process; never run A4/A5 or business stores.

Live sources are explicit mode=ro transactions and original files. Draft and
final outputs belong solely to shadow; missing execution evidence stays null.
"""
from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, time, timedelta
import hashlib
import json
import os
from pathlib import Path
import sqlite3
from time import monotonic
from zoneinfo import ZoneInfo

from .calendar import ExchangeTradingCalendar
from .shadow_a5_observer import _decode, _read_stable, read_a5_observation
from .shadow_close_schedule import ShadowCloseCoordinator
from .shadow_evidence import read_shadow_evidence
from ..evaluation.ablation.shadow_day_adapter import build_shadow_day, render_shadow_day, production_equivalence, PROFILES, VARIANTS
from ..evaluation.ablation.shadow_week import build_shadow_week, render_shadow_week
from ..evaluation.ablation.strategy_accumulation import canonical_sha256

TZ=ZoneInfo('Asia/Shanghai')
VERSION='shadow-reporting/1'
MAX_ROWS=100000
MAX_BYTES=32*1024*1024


@dataclass(frozen=True)
class ReportingPaths:
    state_db: Path
    shadow_db: Path
    shadow_jsonl: Path | None
    lanes: tuple[str,...]
    approved_output_root: Path
    node_receipt_path: Path | None
    node_log_path: Path
    archive_root: Path
    report_root: Path
    bridge_outbox: Path


def _raw(value):
    return json.dumps(value,sort_keys=True,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode('utf8')


def _sha(raw):return hashlib.sha256(raw).hexdigest()


def _stamp(value):
    stamp=value if isinstance(value,datetime) else datetime.fromisoformat(value)
    if stamp.tzinfo is None or stamp.utcoffset() is None:raise ValueError('AWARE_REPORT_CLOCK_REQUIRED')
    return stamp.astimezone(TZ)


def _safe_result(status,**fields):
    return dict(schema_version=VERSION,status=status,production_mutation=False,
                models=0,notifications=0,execution_authorized=False,**fields)


def _validate_paths(cfg):
    if (not cfg.lanes or len(set(cfg.lanes))!=len(cfg.lanes)
            or any(not isinstance(s,str) or not s for s in cfg.lanes)):
        raise ValueError('EXPLICIT_UNIQUE_LANES_REQUIRED')
    sources=[Path(p).resolve() for p in (cfg.state_db,cfg.shadow_db,cfg.node_log_path,
        *([cfg.shadow_jsonl] if cfg.shadow_jsonl else []),*([cfg.node_receipt_path] if cfg.node_receipt_path else []))]
    roots=[Path(p).resolve() for p in (cfg.archive_root,cfg.report_root,cfg.bridge_outbox)]
    for index,root in enumerate(roots):
        rawroot=Path((cfg.archive_root,cfg.report_root,cfg.bridge_outbox)[index])
        if any(p.is_symlink() for p in (rawroot,*rawroot.parents)) or any(p==root or p.is_relative_to(root) for p in sources):
            raise ValueError('INDEPENDENT_SHADOW_OUTPUT_REQUIRED')
        if any(root==other or root.is_relative_to(other) or other.is_relative_to(root) for other in roots[index+1:]):
            raise ValueError('SEPARATE_SHADOW_OUTPUT_ROOTS_REQUIRED')
    if Path(cfg.state_db).resolve()==Path(cfg.shadow_db).resolve():
        raise ValueError('PRODUCTION_AND_SHADOW_DB_ALIAS')


def read_reporting_day(*,state_db,lanes,trade_date,observed_at):
    """Narrow real schema read, active WAL visible; no immutable/checkpoint/init.

    main-file byte SHA is NOT used as a live transaction identity. Original row
    and payload bytes hashes are distinct. Market-window census uses the actual
    scheduler's 09:31..11:30/13:01..15:00 minute contract, not event-only counts.
    """
    now=_stamp(observed_at);day=now.date()
    if day.isoformat()!=trade_date:raise ValueError('REPORT_SOURCE_DAY_MISMATCH')
    dbpath=Path(state_db).resolve(strict=True)
    if not dbpath.is_file():raise ValueError('REPORT_SOURCE_DB_MISSING')
    deadline=monotonic()+2
    marks=','.join('?' for _ in lanes)
    with closing(sqlite3.connect(dbpath.as_uri()+'?mode=ro',uri=True,timeout=.5)) as db:
        db.row_factory=sqlite3.Row
        db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
        db.set_progress_handler(lambda:int(monotonic()>=deadline),1000)
        # SQL date bounds retain expired/invalidated activated rows, never turn
        # final status back into historical PENDING or invent activation.
        plan_sizes=db.execute(f'''SELECT count(*),sum(length(CAST(payload_json AS BLOB))) FROM execution_plans
            WHERE lane_id IN ({marks}) AND valid_from>=? AND valid_from<?''',
            (*lanes,trade_date,(day+timedelta(days=1)).isoformat())).fetchone()
        if plan_sizes[0]>2000 or (plan_sizes[1] or 0)>MAX_BYTES:raise ValueError('REPORT_PLAN_CONTENT_BUDGET_EXCEEDED')
        rows=[dict(r) for r in db.execute(f'''SELECT * FROM execution_plans WHERE lane_id IN ({marks})
            AND valid_from>=? AND valid_from<? ORDER BY plan_id LIMIT 2001''',
            (*lanes,trade_date,(day+timedelta(days=1)).isoformat()))]
        sizes=db.execute(f'''SELECT count(*),sum(length(CAST(payload_json AS BLOB))) FROM monitor_events
            WHERE lane_id IN ({marks}) AND minute_end>=? AND minute_end<=?''',
            (*lanes,trade_date,datetime.combine(day,time(15),TZ).isoformat())).fetchone()
        if len(rows)>2000 or sizes[0]>MAX_ROWS or (sizes[1] or 0)>MAX_BYTES:
            raise ValueError('REPORT_READ_SCOPE_BUDGET_EXCEEDED')
        events=[dict(r) for r in db.execute(f'''SELECT * FROM monitor_events WHERE lane_id IN ({marks})
            AND minute_end>=? AND minute_end<=? ORDER BY minute_end,event_id''',
            (*lanes,trade_date,datetime.combine(day,time(15),TZ).isoformat()))]
        db.rollback()
    if monotonic()>=deadline:raise ValueError('REPORT_READ_ACCEPTANCE_BUDGET_EXCEEDED')
    plans=[];production=[];expected=[];gaps=[];identities={}
    for row in rows:
        payload=_decode(row['payload_json'])
        if (not isinstance(payload,dict) or payload.get('symbol')!=row['symbol']
                or payload.get('plan_id',row['plan_id'])!=row['plan_id']
                or payload.get('target_trade_date')!=trade_date):
            raise ValueError('REPORT_PLAN_PAYLOAD_IDENTITY_CONFLICT')
        begin,end=_stamp(row['valid_from']),_stamp(row['expires_at'])
        if begin.date()!=day or end.date()!=day or begin>=end or begin>now:
            raise ValueError('REPORT_PLAN_ACTIVATION_WINDOW_INVALID')
        terminal=[_stamp(e['minute_end']) for e in events if e['action']=='PLAN_INVALIDATED'
            and e['effective']==1 and _decode(e['payload_json']).get('plan_id')==row['plan_id']]
        profile=payload.get('strategy_profile')
        if profile not in ('TREND_MA5','MA520_SWING','LEADER_INTRADAY'):
            raise ValueError('REPORT_PLAN_PROFILE_UNPROVEN')
        plan=dict(plan_id=row['plan_id'],symbol=row['symbol'],strategy_profile=profile,
            strategy_version=payload.get('strategy_version'),activated_at=begin.isoformat(),expires_at=end.isoformat(),
            entry_zone_lower=payload.get('entry_zone_lower'),entry_zone_upper=payload.get('entry_zone_upper'))
        if terminal:plan['invalidated_at']=min(terminal).isoformat();end=min(end,min(terminal))
        elif row['status']=='INVALIDATED':gaps.append('INVALIDATION_TIME_UNPROVEN:'+row['plan_id'])
        plans.append(plan);identities[row['plan_id']]=row
        clock=datetime.combine(day,time(9,31),TZ)
        while clock<=datetime.combine(day,time(15),TZ):
            if (begin<=clock<=end and (time(9,31)<=clock.time()<time(11,31) or time(13,1)<=clock.time()<=time(15))):
                expected.append(dict(plan_id=row['plan_id'],minute=clock.isoformat()))
            clock+=timedelta(minutes=1)
    seen=set()
    for event in events:
        payload=_decode(event['payload_json']);pid=payload.get('plan_id') if isinstance(payload,dict) else None
        if pid is None:continue
        if pid not in identities:
            gaps.append('OUTER_EVENT_PLAN_NOT_IN_ACTIVATED_CENSUS:'+str(pid))
            production.append(dict(plan_id=pid,symbol=payload.get('symbol'),minute=_stamp(event['minute_end']).isoformat(),
                action=event['action'],first_cause=event['reason_code'],event_id=event['event_id'],
                event_sha256=_sha(_raw(event)),field_status='UNPROVEN_PLAN_IDENTITY'))
            continue
        row=identities[pid];minute=_stamp(event['minute_end']);created=_stamp(event['created_at'])
        key=(pid,minute.isoformat())
        if (key in seen or not minute<=created<=now or payload.get('symbol')!=row['symbol']
                or event['lane_id']!=row['lane_id']):
            raise ValueError('REPORT_OUTER_EVENT_IDENTITY_OR_CLOCK_CONFLICT')
        seen.add(key)
        production.append(dict(plan_id=pid,symbol=row['symbol'],minute=minute.isoformat(),
            action=event['action'],first_cause=event['reason_code'],event_id=event['event_id'],
            event_sha256=_sha(_raw(event)),field_status='OK'))
    census=dict(schema='shadow-day-census/1',trade_date=trade_date,source_ref='readonly:execution_plans+monitor_events',
        variant_set_version='a4-shadow-variants/1',plans=plans,expected_windows=expected)
    lineage=dict(source_db_mode='ro',transaction_sha256=_sha(_raw(dict(plans=rows,events=events))),
        hash_basis='CANONICAL_READ_TRANSACTION_NOT_ACTIVE_DB_FILE',scope_lanes=list(lanes),
        raw_payload_sha256={r['plan_id']:_sha(r['payload_json'].encode('utf8')) for r in rows},
        activated_row_count=len(rows),event_count=len(events),gap_codes=gaps,
        historical_status_reconstructed=False,as_of=now.isoformat())
    return dict(census=census,production=production,lineage=lineage)


def _write_new(path,raw):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    if any(p.is_symlink() for p in (path,*path.parents)):raise ValueError('SHADOW_OUTPUT_SYMLINK_REFUSED')
    # New independent evidence only, private on Unix; never replace originals.
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'wb') as handle:handle.write(raw)


def _technical_cohorts(view,day):
    groups={};seen=set()
    for item in sorted(view.get('signals',[]),key=lambda r:r['signal']['minute']):
        signal=item['signal']
        if _stamp(signal['minute']).date().isoformat()!=day:continue
        origin=signal.get('inputs_origin','UNKNOWN')
        if origin not in ('PLAN_FROZEN','SHADOW_PREOPEN_SIDECAR'):origin='UNKNOWN'
        group=groups.setdefault(origin,dict(observed_rows=0,technical_first_triggers=0,
            unproven_technical_rows=0,execution_ready=False,groups={}))
        group['observed_rows']+=1
        variant=signal.get('variant_id');profile=signal.get('profile')
        valid_scope=variant in VARIANTS and profile in PROFILES and (profile=='TREND_MA5' or variant in VARIANTS[:3])
        subgroup=(group['groups'].setdefault(variant,{}).setdefault(profile,
            dict(observed_rows=0,technical_first_triggers=0,execution_ready=False)) if valid_scope else None)
        if subgroup is not None:subgroup['observed_rows']+=1
        proven=(valid_scope and signal.get('technical_trigger_basis')=='ISOLATED_RESEARCH_ENGINE_NOT_PIT_OR_FILL'
            and signal.get('technical_action') in ('BUY','ADD','BUY_SIGNAL','ADD_SIGNAL'))
        key=(signal.get('plan_id'),variant,origin)
        if signal.get('technical_trigger') is True:
            if not proven:group['unproven_technical_rows']+=1
            elif key not in seen:
                seen.add(key);group['technical_first_triggers']+=1;subgroup['technical_first_triggers']+=1
    return groups


def _missing_ledger_report(source,day,now):
    groups={}
    for variant in VARIANTS:
        groups[variant]={}
        for profile in PROFILES:
            applicable=profile=='TREND_MA5' or variant in VARIANTS[:3]
            groups[variant][profile]=dict(status='DATA_LIMITED' if applicable else 'NOT_APPLICABLE',
                activated_count=sum(p['strategy_profile']==profile for p in source['census']['plans']) if applicable else 0,
                first_trigger_count=None,fill_count=None,fill_rate=None,
                returns={f'T{n}':dict(sample_count=None,mean=None,median=None,win_rate=None) for n in (1,3,5)})
    return dict(schema='shadow-reporting-gap/1',trade_date=day,as_of=now.isoformat(),status='DATA_LIMITED',
        source_kind='REALTIME_SHADOW',account_pnl=None,source_input_sha256=None,
        gap_codes=['SHADOW_LEDGER_SOURCE_UNAVAILABLE'],groups=groups,
        production_equivalence=production_equivalence(source['production'],[]),observed_minutes=None,
        technical_research_cohorts={},trigger_research_only=True,execution_evidence_status='DATA_LIMITED',
        fill_evidence_ready=False,production_read_lineage=source['lineage'],production_mutation=False,notifications=0,
        day_adapter_run=False)


def _freeze(cfg,day,now,observation_clock=None):
    source=read_reporting_day(state_db=cfg.state_db,lanes=cfg.lanes,trade_date=day,observed_at=now)
    view=read_shadow_evidence(cfg.shadow_db,cfg.shadow_jsonl)
    if observation_clock is not None:
        after=_stamp(observation_clock())
        if after<now or after.date()!=now.date():raise ValueError('DRAFT_OBSERVATION_CLOCK_CONFLICT')
        now=after
    # build_shadow_day verifies the real ledger chain and all explicit coverage;
    # missing PIT remains DATA_LIMITED instead of authorizing a fill.
    report=(build_shadow_day(view,source['census'],source['production'],as_of=now.isoformat())
        if view.get('ok') is True else _missing_ledger_report(source,day,now))
    report.update(technical_research_cohorts=_technical_cohorts(view,day),trigger_research_only=True,
        execution_evidence_status='DATA_LIMITED',fill_evidence_ready=False,
        production_read_lineage=source['lineage'],production_mutation=False,notifications=0)
    report['gap_codes']=sorted(set(report['gap_codes']+source['lineage']['gap_codes']))
    report['production_equivalence']['scope_lanes']=list(cfg.lanes)
    report['production_equivalence']['all_plan_event_identities_proven']=not any(
        g.startswith('OUTER_EVENT_PLAN_') for g in source['lineage']['gap_codes'])
    if not report['production_equivalence']['all_plan_event_identities_proven'] and report['production_equivalence']['status']=='MATCHED':
        report['production_equivalence']['status']='DATA_LIMITED'
    for pid,pin in source['lineage']['raw_payload_sha256'].items():
        bindings=[m.get('source_bindings',{}).get(pid,{}) for m in view.get('minutes',[])
            if _stamp(m['minute']).date().isoformat()==day]
        if not bindings or any(b.get('plan_payload_bytes_sha256')!=pin for b in bindings):
            report['gap_codes'].append('PLAN_PAYLOAD_BYTE_BINDING_UNPROVEN:'+pid)
    if report['gap_codes']:report['status']='DATA_LIMITED'
    # The current W2 outcome/provider route is explicitly unwired. Observed
    # historical ledger values remain auditable, not execution-ready metrics.
    for profiles in report['groups'].values():
        for group in profiles.values():
            if group['status']=='NOT_APPLICABLE':continue
            group['fill_count']=None;group['fill_rate']=None
            for stats in group['returns'].values():
                stats.update(mean=None,median=None,win_rate=None)
    package=dict(schema_version=VERSION,trade_date=day,captured_at=now.isoformat(),
        market_cutoff=datetime.combine(now.date(),time(15),TZ).isoformat(),
        draft_status='ON_TIME_DRAFT' if now.time()==time(15,30) else 'LATE_DRAFT',report=report,
        original_ledger_view_sha256=view.get('snapshot_canonical_sha256'),source_input=source)
    raw=_raw(package);path=cfg.archive_root/day/'draft.json';_write_new(path,raw)
    _write_new(cfg.archive_root/day/'draft-receipt.json',_raw(dict(schema_version=VERSION,
        trade_date=day,draft_sha256=_sha(raw),captured_at=package['captured_at'])))
    return dict(trade_date=day,market_cutoff=package['market_cutoff'],source_ref=str(path),sha256=_sha(raw))


def _final(cfg,draft,*,draft_status,a5_status,a5,as_of):
    raw=_read_stable(draft['source_ref'])
    if _sha(raw)!=draft['sha256']:raise ValueError('SHADOW_DRAFT_HASH_CONFLICT')
    package=_decode(raw);day=package['trade_date'];report=package['report']
    report.update(a5_status=a5_status,a5_observation=a5,draft_status=draft_status,
                  formal_written_at=as_of.isoformat(),formal_clock_basis='ACCEPTANCE_BEFORE_SYNCHRONOUS_OUTPUT_WRITES',
                  draft_captured_at=package['captured_at'])
    text=(render_shadow_day(report) if report['schema']=='shadow-day-adapter/1' else
        '# A4 影子日报 '+day+'\n\nDATA_LIMITED：独立影子源未取得，不把无账本写成0触发或0成交。\n'
        '生产外层等价：'+report['production_equivalence']['status']+'；缺腿 null。\n')
    text+='\nA5：'+a5_status+'；草稿：'+draft_status+'；成交腿 DATA_LIMITED，当前仅触发研究。\n'
    text+='\n技术触发 cohort（非成交首触发）：'+json.dumps(report['technical_research_cohorts'],ensure_ascii=False)+'\n'
    archive=cfg.archive_root/day
    files={archive/'report.json':_raw(report),
        cfg.report_root/f'SHADOW_DAILY_{day}.md':text.encode('utf8'),
        cfg.report_root/f'PRODUCTION_EQUIVALENCE_{day}.json':_raw(report['production_equivalence'])}
    for path,body in files.items():_write_new(path,body)
    delivery=dict(schema_version=VERSION,trade_date=day,a5_status=a5_status,
        report_status=report['status'],report_sha256=_sha(files[archive/'report.json']),
        production_equivalence=report['production_equivalence']['status'],
        draft_sha256=draft['sha256'],notifications=0,production_mutation=False,
        outputs={str(p.resolve()):_sha(b) for p,b in files.items()})
    bridge=cfg.bridge_outbox/day/'W3_SHADOW_DAILY.json';_write_new(bridge,_raw(delivery))
    files[bridge]=_raw(delivery)
    manifest=dict(schema_version=VERSION,trade_date=day,outputs={str(p.resolve()):_sha(b) for p,b in files.items()},
                  a5_status=a5_status,draft_status=draft_status,as_of=as_of.isoformat(),draft_sha256=draft['sha256'])
    path=archive/'final-manifest.json';body=_raw(manifest);_write_new(path,body)
    return dict(source_ref=str(path),sha256=_sha(body))


def _daily_paths(cfg,day):
    return {str(p.resolve()) for p in (cfg.archive_root/day/'report.json',
        cfg.report_root/f'SHADOW_DAILY_{day}.md',cfg.report_root/f'PRODUCTION_EQUIVALENCE_{day}.json',
        cfg.bridge_outbox/day/'W3_SHADOW_DAILY.json')}


def _saved_outputs(path,*,expected,now,day):
    saved=_decode(_read_stable(path))
    if (saved['schema_version']!=VERSION or saved['trade_date']!=day
            or _stamp(saved['as_of'])>now or set(saved['outputs'])!=expected):raise ValueError
    for output,pin in saved['outputs'].items():
        if Path(output).is_symlink() or _sha(_read_stable(output))!=pin:raise ValueError
    return saved


def run_reporting_tick(cfg,*,observed_at,observation_clock=None):
    """One independent poll; persistent immutable draft makes restart idempotent.

    Filesystem writes cannot be hard-preempted; failures are shadow-only. No
    retroactive 15:30/16:45 timestamps or assumed successful empty-day evidence.
    """
    try:
        now=_stamp(observed_at);day=now.date().isoformat();_validate_paths(cfg)
        if not ExchangeTradingCalendar().is_trading_day(now.date()):return _safe_result('NON_TRADING_DAY')
    except Exception:return _safe_result('SHADOW_SETUP_FAILED')
    final=cfg.archive_root/day/'final-manifest.json'
    if final.exists():
        try:
            saved=_saved_outputs(final,expected=_daily_paths(cfg,day),now=now,day=day)
            return _safe_result('ALREADY_WRITTEN',trade_date=day,a5_status=saved['a5_status'])
        except Exception:return _safe_result('SHADOW_OUTPUT_BINDING_FAILED')
    draftpath=cfg.archive_root/day/'draft.json'
    if not draftpath.exists() and now.time()>=time(15,30):
        try:
            _freeze(cfg,day,now,observation_clock)
            if observation_clock is not None:
                finished=_stamp(observation_clock())
                if finished<now or finished.date()!=now.date():raise ValueError('DRAFT_OBSERVATION_CLOCK_CONFLICT')
                now=finished
        except Exception:return _safe_result('SHADOW_DRAFT_FAILED')
    if draftpath.exists():
        try:
            raw=_read_stable(draftpath);saved=_decode(raw)
            pin=_decode(_read_stable(cfg.archive_root/day/'draft-receipt.json'))
            if pin.get('draft_sha256')!=_sha(raw) or pin.get('trade_date')!=day:raise ValueError
            if (saved['schema_version']!=VERSION or saved['trade_date']!=day or _stamp(saved['captured_at'])>now):raise ValueError
        except Exception:return _safe_result('SHADOW_DRAFT_BINDING_FAILED')
    a5=None
    if time(16)<=now.time()<=time(16,45) and cfg.node_receipt_path is not None:
        a5=read_a5_observation(trade_date=day,observed_at=now,state_db=cfg.state_db,
            approved_output_root=cfg.approved_output_root,node_receipt_path=cfg.node_receipt_path,node_log_path=cfg.node_log_path,
            observation_clock=observation_clock)
        if observation_clock is not None:
            finished=_stamp(observation_clock())
            if finished<now or finished.date()!=now.date():return _safe_result('SHADOW_OBSERVATION_CLOCK_FAILED')
            now=finished
    engine=ShadowCloseCoordinator(day,freeze=lambda **kw:_freeze(cfg,day,now,observation_clock),
        build_final=lambda **kw:_final(cfg,**kw))
    if draftpath.exists():
        engine.draft=dict(trade_date=day,market_cutoff=saved['market_cutoff'],source_ref=str(draftpath),sha256=_sha(raw))
        engine.draft_status=saved['draft_status']
    result=engine.poll(now,a5=a5)
    return {**_safe_result(result['status']),**result}


def run_weekly_reporting(cfg,*,trading_days,observed_at):
    """16:00 Friday independent snapshot; current draft is labelled provisional.

    Formal reports can supersede a draft only in a separately named future
    snapshot, never overwrite this immutable weekly report. Missing days null.
    """
    try:
        now=_stamp(observed_at);_validate_paths(cfg)
        day=trading_days[-1]
        if now.date().isoformat()!=day or now.time()<time(16):raise ValueError
        root=cfg.archive_root/('week-'+day);manifest=root/'final-manifest.json'
        paths={root/'report.json',cfg.report_root/f'SHADOW_WEEK_{day}.md',cfg.bridge_outbox/day/'W3_SHADOW_WEEK.json'}
        if manifest.exists():
            _saved_outputs(manifest,expected={str(p.resolve()) for p in paths},now=now,day=day)
            return _safe_result('ALREADY_WRITTEN',trade_date=day)
        inputs=[];lineage={};draft_days=[];input_gap_days=[]
        for session in trading_days:
            p=cfg.archive_root/session/'report.json'
            if p.is_file():
                daily_manifest=cfg.archive_root/session/'final-manifest.json'
                _saved_outputs(daily_manifest,expected=_daily_paths(cfg,session),now=now,day=session)
                raw=_read_stable(p);report=_decode(raw)
            elif session==day and (cfg.archive_root/session/'draft.json').is_file():
                p=cfg.archive_root/session/'draft.json';raw=_read_stable(p)
                pin=_decode(_read_stable(cfg.archive_root/session/'draft-receipt.json'))
                if pin['draft_sha256']!=_sha(raw):raise ValueError
                report=_decode(raw)['report'];draft_days.append(session)
            else:continue
            lineage[session]=dict(path=str(p.resolve()),original_file_sha256=_sha(raw))
            if report.get('schema')!='shadow-day-adapter/1':input_gap_days.append(session);continue
            inputs.append(dict(report=report,report_sha256=canonical_sha256(report)))
        report=build_shadow_week(inputs,trading_days=trading_days,as_of=now.isoformat())
        report.update(draft_days=draft_days,input_gap_days=input_gap_days,original_report_files=lineage,
            trigger_research_only=True,fill_evidence_ready=False,execution_evidence_status='DATA_LIMITED',
            technical_research_cohorts_by_day={x['report']['trade_date']:x['report'].get('technical_research_cohorts',{}) for x in inputs})
        bodies={root/'report.json':_raw(report),cfg.report_root/f'SHADOW_WEEK_{day}.md':(render_shadow_week(report)
            +'\n当前仅触发研究；派生成交/provider未接线。草稿日：'+','.join(draft_days)+'\n').encode('utf8')}
        bridge=cfg.bridge_outbox/day/'W3_SHADOW_WEEK.json'
        bodies[bridge]=_raw(dict(schema_version=VERSION,trade_date=day,report_status=report['status'],
            report_sha256=_sha(bodies[root/'report.json']),production_mutation=False,notifications=0))
        for path,body in bodies.items():_write_new(path,body)
        _write_new(manifest,_raw(dict(schema_version=VERSION,trade_date=day,as_of=now.isoformat(),
            outputs={str(p.resolve()):_sha(body) for p,body in bodies.items()})))
        return _safe_result('WEEK_WRITTEN',trade_date=day,coverage_status=report['status'])
    except Exception:return _safe_result('SHADOW_WEEK_FAILED')


__all__=['ReportingPaths','read_reporting_day','run_reporting_tick','run_weekly_reporting']
