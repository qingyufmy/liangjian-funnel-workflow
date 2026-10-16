"""Three independent real morning edges; writes owned paper/team state only.

No legacy application/store constructor, source fetch, activation or customer
I/O. Later edges consume the frozen first edge, never silently rebuild it.
"""
from dataclasses import asdict
from datetime import datetime, time
from decimal import Decimal as D
import hashlib
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml

from ..pipeline.model_client import OpenAICompatibleModelClient
from ..pipeline.team_v2_judgments import ModelJudgmentJournal
from ..pipeline.team_v2_prompts import TeamPromptRepository
from .broker.local_paper import create_broker
from .calendar import ExchangeTradingCalendar
from .risk_engine import AccountRisk, PositionRisk, RiskEnvelope, RiskLimits, build_planning_envelope, conservative_envelope
from .team_v2_risk_meeting import RiskMeetingResult
from .team_v2_risk_session import run_risk_premarket
from .team_v2_pm_session import run_pm_premarket
from .team_v2_sources import read_planning_scope, read_held_bindings, decode_plan, stamp
from .team_v2_store import TeamMeetingStore, canonical
from .team_v2_external import read_external

TZ=ZoneInfo('Asia/Shanghai')
ACCOUNT='local-paper'
WINDOWS={'engine':(time(8,33),time(8,35)), 'risk':(time(8,35),time(8,37)), 'pm':(time(8,38),time(8,40))}


def _directory(settings):
    directory=Path(settings.root)/'state/team_v2'
    if directory.is_symlink() or directory.resolve()!=Path(settings.root).resolve()/'state/team_v2':
        raise ValueError('TEAM_DATABASE_SCOPE_INVALID')
    for name in ('paper.db','team.db','model.db'):
        if (directory/name).is_symlink():raise ValueError('TEAM_DATABASE_SCOPE_INVALID')
    return directory


def _envelopes(rows):
    result=[]
    for raw in rows:
        row=dict(raw);row['risk_budget']=D(row['risk_budget'])
        for key in ('blocked_reasons','binding_limits'):row[key]=tuple(row[key])
        result.append(RiskEnvelope(**row))
    return tuple(result)


def _freeze_engine(settings,store,source,now,calendar,directory):
    previous=store.load_morning_engine(ACCOUNT,source['trade_date'])
    if previous is not None:
        if previous['source']['plan_rowset_sha256']!=source['plan_rowset_sha256']:
            raise ValueError('MORNING_PLAN_ROWS_CHANGED')
        return previous
    if source['status']!='COMPLETE' or source['market_regime'] is None:
        return dict(status='BLOCKED',reason_code='MORNING_SOURCE_NOT_COMPLETE',source=source)
    limits=RiskLimits.from_yaml(settings.root/'config/risk_limits.yaml')
    broker=create_broker('local_paper',database=directory/'paper.db',paper_config=settings.root/'config/paper_broker.yaml',
        risk_config=settings.root/'config/risk_limits.yaml',clock=lambda:now,calendar=calendar,run_id='morning-engine:'+source['trade_date'])
    broker.open_risk_day(source['trade_date']);snapshot=broker.planning_snapshot()
    if broker.reconcile()['status']!='MATCH':raise ValueError('BROKER_RECONCILE_MISMATCH')
    account=snapshot['account'];basis=snapshot['risk_day']
    plans=tuple(decode_plan(p) for p in source['plans']);by_id={p.plan_id:p for p in plans}
    held=read_held_bindings(settings,snapshot['positions'])
    owners={p['plan_id']:p for p in held}
    positions=[]
    for row in snapshot['positions']:
        owner=owners.get(row['plan_id'])
        if owner is None or owner['symbol']!=row['symbol'] or row['market_value'] is None:
            raise ValueError('MORNING_HELD_PLAN_SCOPE_UNPROVEN')
        positions.append(PositionRisk(owner['plan_id'],owner['symbol'],owner['cluster'],row['qty'],row['sellable_qty'],
            D(row['market_value']),D(row['avg_cost'])))
    view=AccountRisk(D(account['nav']),D(account['cash']),D(basis['day_open_nav']),
        max(D(snapshot['peak_nav']),D(account['nav'])),D(account['frozen']),
        hard_halt=bool(snapshot['global_halt'] or basis['halt_reason_codes']))
    envelopes=tuple(build_planning_envelope(p,view,tuple(positions),limits,now=now,regime=source['market_regime']) for p in plans)
    raw=(settings.root/'config/team_v2_deployment.yaml').read_bytes()
    policy=yaml.safe_load(raw)
    if policy.get('schema_version')!='team-deployment/1':raise ValueError('DEPLOYMENT_POLICY_INVALID')
    bounds=policy['ranges'][source['market_regime']]
    if (not isinstance(bounds,list) or len(bounds)!=2 or any(type(x) is not int for x in bounds)
            or not 0<=bounds[0]<=bounds[1]<=95):raise ValueError('DEPLOYMENT_POLICY_INVALID')
    external=read_external(settings,now=now,policy=policy['external'])
    bounds=[max(0,min(95,v+external['value_pp'])) for v in bounds]
    body=dict(status='COMPLETE',session='ENGINE_PREMARKET',trade_date=source['trade_date'],
        observed_at=now.isoformat(),account_id=ACCOUNT,source=source,snapshot=snapshot,held_plan_bindings=held,
        envelopes=[asdict(e) for e in envelopes],capacity_slots=max(0,limits.max_positions-len({p.symbol for p in positions})),
        cluster_caps={key:limits.max_positions for key in sorted({p.cluster for p in plans})},
        baseline_range=bounds,engine_utilization_cap=bounds[1],deployment_policy_sha256=hashlib.sha256(raw).hexdigest(),
        risk_limits_sha256=hashlib.sha256((settings.root/'config/risk_limits.yaml').read_bytes()).hexdigest(),
        external_adjustment=external,
        authority='PLANNING_ONLY_NOT_EXECUTION_PERMISSION')
    return store.record_morning_engine(ACCOUNT,source['trade_date'],body)


def run_morning_stage(settings,stage,*,clock=None,calendar=None,client=None):
    if stage not in WINDOWS:raise ValueError('MORNING_STAGE_INVALID')
    clock=clock or (lambda:datetime.now(TZ));now=stamp(clock());day=now.date().isoformat()
    calendar=calendar or ExchangeTradingCalendar()
    if calendar.is_trading_day(now.date()) is not True:return dict(status='NOOP',reason_code='NON_TRADING_DAY')
    start,end=WINDOWS[stage]
    if not start<=now.time().replace(tzinfo=None)<end:return dict(status='NOOP',reason_code='MORNING_STAGE_WINDOW_MISSED')
    # Source access is always RO, including the dependent stages' binding check.
    source=read_planning_scope(settings,now=now,calendar=calendar)
    directory=_directory(settings)
    if stage!='engine' and not (directory/'team.db').is_file():
        return dict(status='BLOCKED',reason_code='MORNING_ENGINE_MISSING')
    store=TeamMeetingStore(directory/'team.db')
    if stage=='engine':return _freeze_engine(settings,store,source,now,calendar,directory)
    frozen=store.load_morning_engine(ACCOUNT,day)
    if frozen is None:return dict(status='BLOCKED',reason_code='MORNING_ENGINE_MISSING')
    if source['plan_rowset_sha256']!=frozen['source']['plan_rowset_sha256']:
        return dict(status='BLOCKED',reason_code='MORNING_PLAN_ROWS_CHANGED')
    if frozen['risk_limits_sha256']!=hashlib.sha256((settings.root/'config/risk_limits.yaml').read_bytes()).hexdigest():
        return dict(status='BLOCKED',reason_code='MORNING_RISK_POLICY_CHANGED')
    envelopes=_envelopes(frozen['envelopes'])
    catalog={'categories':['ACCOUNT','MARKET','PLAN'], 'items':[
        {'id':'MORNING:ACCOUNT','category':'ACCOUNT','value':frozen['snapshot']},
        {'id':'MORNING:MARKET','category':'MARKET','value':{'market_regime':source['market_regime'],
            'external_adjustment':frozen['external_adjustment']}},
        *({'id':'MORNING:PLAN:'+r['plan_id'],'category':'PLAN','value':r} for r in frozen['source']['plan_rows'])]}
    repository=TeamPromptRepository(settings.root/'prompts/team_v2')
    journal=ModelJudgmentJournal(directory/'model.db')
    client=client or OpenAICompatibleModelClient(settings,max_attempts=1)
    role='A6' if stage=='risk' else 'A3';run_id='team-morning:'+day
    required=dict(schema_version='team-envelope/1',run_id=run_id,stage_id=role,
        source_trade_date=frozen['source']['source_trade_date'],execution_trade_date=day,as_of=frozen['observed_at'],
        knowledge_cutoff=frozen['observed_at'],input_snapshot_ids=[source['plan_rowset_sha256']],
        config_version='team-v2/1',config_hash=frozen['risk_limits_sha256'],prompt_version=repository.prepare(
            role,{key:[] for key in repository.document(role).placeholders}|{'RISK_SESSION':'PREMARKET'},
            session='PREMARKET' if stage=='risk' else 'PM_PREMARKET').role_version,
        model_name=settings.review_model,market_regime=source['market_regime'],status='FROZEN',reason_codes=[])
    values={key:dict(status='NOT_AVAILABLE') for key in repository.document(role).placeholders}
    values.update(EVIDENCE_CATALOG=catalog,ACCOUNT_SNAPSHOT=frozen['snapshot']['account'],POSITIONS=frozen['snapshot']['positions'],
        MARKET_STATE={'market_regime':source['market_regime'],'basis':'FROZEN_PRIOR_NOT_INTRADAY'},
        PLANS=frozen['source']['plan_rows'],PRIOR_OUTCOME_FEEDBACK={'status':'INSUFFICIENT_TEN_DAYS'},
        OVERNIGHT_EXTERNAL_SNAPSHOT=frozen['external_adjustment'])
    common=dict(repository=repository,client=client,journal=journal,store=store,account_id=ACCOUNT,run_id=run_id,
        model=settings.review_model,trade_date=day,source_trade_date=frozen['source']['source_trade_date'],clock=clock,
        required_envelope=required,replacements=values,envelopes=envelopes,timeout_seconds=100,
        max_output_tokens=settings.model_max_output_tokens,allow_model_call=True)
    if stage=='risk':
        result=run_risk_premarket(**common,meeting_id='morning-risk:'+day,call_id='A6:morning:'+day,
            engine_utilization_cap=D(frozen['engine_utilization_cap']),capacity_slots=frozen['capacity_slots'],
            cluster_caps=frozen['cluster_caps'],unready_plan_ids={e.plan_id for e in envelopes if not e.eligible},
            limits=RiskLimits.from_yaml(settings.root/'config/risk_limits.yaml'))
        return json.loads(canonical(dict(status=result.status,session='PREMARKET',result=asdict(result))))
    result=store.load_morning_risk(ACCOUNT,day,source_call_id='A6:morning:'+day)
    if result is None:
        # No fabricated A6 response or success label. PM may allocate only the
        # deterministic conservative envelopes, recording the missing meeting.
        result=RiskMeetingResult('A6_MISSING_CONSERVATIVE',tuple(conservative_envelope(e,consecutive_failures=1)
            for e in envelopes),D(frozen['engine_utilization_cap']),1,('A6_MORNING_MISSING',))
    common.update(envelopes=result.envelopes)
    values['MARKET_STATE']['a6_morning']={'status':result.status,'reason_codes':list(result.reason_codes)}
    bounds=tuple(min(D(v),result.utilization_cap) for v in frozen['baseline_range'])
    return run_pm_premarket(**common,meeting_id='morning-pm:'+day,call_id='A3:morning:'+day,
        plan_clusters={p['plan_id']:p['cluster'] for p in frozen['source']['plans']},
        free_slots=frozen['capacity_slots'],baseline_range=bounds,
        utilization_cap=result.utilization_cap,high_conviction_ids=set())
