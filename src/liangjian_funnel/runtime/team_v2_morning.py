"""Three independent real morning edges; writes owned paper/team state only.

No legacy application/store constructor, source fetch, activation or customer
I/O. Later edges consume the frozen first edge, never silently rebuild it.
"""
from dataclasses import asdict, replace
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
from ..pipeline.team_v2_pm_schema import PM_PRIMARY_CLUSTER_CAP
from .team_v2_sources import read_planning_scope, read_held_bindings, decode_plan, stamp, meeting_plan_facts
from .team_v2_store import TeamMeetingStore, canonical
from .team_v2_external import read_external

TZ=ZoneInfo('Asia/Shanghai')
ACCOUNT='local-paper'
WINDOWS={'engine':(time(8,33),time(8,35)), 'risk':(time(8,35),time(8,37)), 'pm':(time(8,35),time(8,45))}


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


def _freeze_engine(settings,store,source,now,calendar,directory,clock,authorization=None):
    previous=store.load_morning_engine(ACCOUNT,source['trade_date'])
    if previous is not None:
        if (previous['source']['plan_rowset_sha256']!=source['plan_rowset_sha256']
                or any(previous['source'].get(key)!=source.get(key)
                    for key in ('candidate_scope_id','candidate_sha256','candidate_revision'))):
            raise ValueError('MORNING_PLAN_ROWS_CHANGED')
        return previous
    # Market evidence is independent of whether A3 happened to publish plans.
    # An empty plan day must not prevent the intraday radar's G5 source seed.
    from .team_v2_emotion_baseline import prepare_emotion_baseline
    emotion_baseline=prepare_emotion_baseline(settings,clock=clock,calendar=calendar)
    if source['status']!='COMPLETE' or source['market_regime'] is None:
        return dict(status='BLOCKED',reason_code='MORNING_SOURCE_NOT_COMPLETE',source=source)
    limits=RiskLimits.from_yaml(settings.root/'config/risk_limits.yaml')
    from .team_v2_first_day import load_first_day_policy,first_day_limits,probe_envelope
    first_policy=load_first_day_policy(settings.root/'config/risk_limits.yaml')
    limits=first_day_limits(limits,first_policy,now=now)
    broker=create_broker('local_paper',database=directory/'paper.db',paper_config=settings.root/'config/paper_broker.yaml',
        risk_config=settings.root/'config/risk_limits.yaml',clock=lambda:now,calendar=calendar,run_id='morning-engine:'+source['trade_date'])
    if authorization is None:
        broker.open_risk_day(source['trade_date']);snapshot=broker.planning_snapshot()
    else:
        snapshot=broker.planning_snapshot()
        basis=snapshot.get('risk_day')
        if basis is None:
            if time(9,25)<=now.time().replace(tzinfo=None)<time(9,30):
                raise ValueError('MORNING_RISK_DAY_CREATION_AFTER0925_1201')
            # At/after09:30 retain the native PAPER_RISK_DAY_OPEN_TIME_INVALID.
            broker.open_risk_day(source['trade_date']);snapshot=broker.planning_snapshot()
        else:
            opened=datetime.fromisoformat(basis['opened_at']).astimezone(TZ)
            if opened.date()!=now.date()or opened.time().replace(tzinfo=None)>=time(9,30)or opened>now:
                raise ValueError('MORNING_RISK_DAY_BASELINE_UNPROVEN_1201')
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
    planning_args={}if authorization is None else dict(window_authorization=authorization,window_source=source)
    envelopes=tuple(build_planning_envelope(p,view,tuple(positions),limits,now=now,regime=source['market_regime'],**planning_args) for p in plans)
    envelopes=tuple(probe_envelope(e,first_policy,now=now,probe_multiplier=limits.probe_multiplier) for e in envelopes)
    from .news_data_limits import morning_news_facts,cap_news_envelopes
    news_facts=morning_news_facts(meeting_plan_facts(source['plan_rows']))
    news_ids={p['plan_id'] for p in news_facts if p['publication_facts'].get('NEWS_DATA_LIMITED') is True}
    envelopes=cap_news_envelopes(envelopes,news_ids)
    from .history_freshness import inspect_plan_seeds
    seed_freshness=inspect_plan_seeds(source['plans'],path=settings.minute_cache_dir/'minute_bars.sqlite3',
        target_trade_date=now.date(),calendar=calendar,as_of=now)
    missing={r['plan_id']:r for r in seed_freshness['plans'] if r['status']!='READY'}
    envelopes=tuple(replace(e,eligible=False,risk_unit='NONE',risk_budget=D(0),max_shares_cap=0,
        blocked_reasons=tuple(dict.fromkeys((*e.blocked_reasons,missing[e.plan_id]['reason_code']))),
        degradation_tags=tuple(dict.fromkeys((*e.degradation_tags,'DATA_LIMITED'))))
        if e.plan_id in missing else e for e in envelopes)
    if missing:
        from .provider_health import queue_internal
        queue_internal(settings,kind='PLAN_HISTORY_INCOMPLETE',source_id='morning-seed:'+source['trade_date'],
            text='盘前历史5分钟种子未齐：'+','.join(sorted({r['symbol'] for r in missing.values()}))+
            '；必须为上一交易日闭合48根，晨会已标数据受限。',now=now)
    raw=(settings.root/'config/team_v2_deployment.yaml').read_bytes()
    policy=yaml.safe_load(raw)
    if policy.get('schema_version')!='team-deployment/1':raise ValueError('DEPLOYMENT_POLICY_INVALID')
    bounds=policy['ranges'][source['market_regime']]
    if (not isinstance(bounds,list) or len(bounds)!=2 or any(type(x) is not int for x in bounds)
            or not 0<=bounds[0]<=bounds[1]<=95):raise ValueError('DEPLOYMENT_POLICY_INVALID')
    external_policy=yaml.safe_load((settings.root/'config/risk_limits.yaml').read_text(encoding='utf-8'))['external_adjustment']
    external=read_external(settings,now=now,policy=external_policy)
    bounds=[max(0,min(external_policy['maximum_utilization_pct'],v+external['value_pp'])) for v in bounds]
    from .team_v2_rule_source import prepare_daily_rule_source
    rule_source=prepare_daily_rule_source(settings,clock=clock,calendar=calendar)
    body=dict(status='COMPLETE',session='ENGINE_PREMARKET',trade_date=source['trade_date'],
        observed_at=stamp(clock()).isoformat(),account_id=ACCOUNT,source=source,snapshot=snapshot,held_plan_bindings=held,
        envelopes=[asdict(e) for e in envelopes],capacity_slots=max(0,limits.max_positions-len({p.symbol for p in positions})),
        cluster_caps={key:min(limits.max_positions,PM_PRIMARY_CLUSTER_CAP) for key in sorted({p.cluster for p in plans})},
        baseline_range=bounds,engine_utilization_cap=bounds[1],deployment_policy_sha256=hashlib.sha256(raw).hexdigest(),
        risk_limits_sha256=hashlib.sha256((settings.root/'config/risk_limits.yaml').read_bytes()).hexdigest(),
        external_adjustment=external,rule_preflight=rule_source,
        emotion_baseline={k:v for k,v in emotion_baseline.items() if k not in ('catalogue','previous_streaks','source_results')},
        seed_freshness=seed_freshness,
        news_plan_facts=news_facts,
        authority='PLANNING_ONLY_NOT_EXECUTION_PERMISSION')
    if authorization is not None:body['morning_window_authorization']=dict(authorization)
    return store.record_morning_engine(ACCOUNT,source['trade_date'],body)


def run_morning_stage(settings,stage,*,clock=None,calendar=None,client=None,authorization=None):
    if stage not in WINDOWS:raise ValueError('MORNING_STAGE_INVALID')
    clock=clock or (lambda:datetime.now(TZ));now=stamp(clock());day=now.date().isoformat()
    calendar=calendar or ExchangeTradingCalendar()
    if calendar.is_trading_day(now.date()) is not True:return dict(status='NOOP',reason_code='NON_TRADING_DAY')
    if authorization is not None:
        from .morning_window_authority import morning_deadline
        morning_deadline(day,'2026-10-29',authorization=authorization,now=now)
    start,end=WINDOWS[stage]
    if authorization is None and not start<=now.time().replace(tzinfo=None)<end:return dict(status='NOOP',reason_code='MORNING_STAGE_WINDOW_MISSED')
    # Source access is always RO, including the dependent stages' binding check.
    from .reviewed_batch_natural import read_natural_candidate,candidate_planning_source
    candidate=read_natural_candidate(settings,day=day)
    if candidate is not None and candidate.get('state')=='EMPTY':
        # An audited zero source is a successful no-op, not a failed meeting.
        # Do not open the broker, run risk meetings or synthesize A6/PM replies.
        emotion=None
        if stage=='engine':
            from .team_v2_emotion_baseline import prepare_emotion_baseline
            try:emotion=prepare_emotion_baseline(settings,clock=clock,calendar=calendar)
            except Exception:
                emotion=dict(status='DATA_LIMITED',reason_code='FORMAL_EMPTY_EMOTION_BASELINE_UNAVAILABLE')
        return dict(status='NOOP',reason_code='NATURAL_ZERO_PLAN_SOURCE',stage=stage,
            trade_date=day,scope_id=candidate['scope_id'],body_sha256=candidate['body_sha256'],
            model_http=0,execution_plans_written=0,account_opened=False,
            **({'emotion_baseline_status':emotion.get('status'),
                'emotion_baseline_reason_code':emotion.get('reason_code')} if emotion is not None else {}))
    source=(candidate_planning_source(settings,candidate,now=now,calendar=calendar) if candidate is not None
        else read_planning_scope(settings,now=now,calendar=calendar))
    from .morning_window_authority import morning_deadline
    deadline=morning_deadline(day,source.get('source_trade_date'),authorization=authorization,source=source,now=now)
    directory=_directory(settings)
    if stage!='engine' and not (directory/'team.db').is_file():
        return dict(status='BLOCKED',reason_code='MORNING_ENGINE_MISSING')
    store=TeamMeetingStore(directory/'team.db')
    if stage=='engine':return _freeze_engine(settings,store,source,now,calendar,directory,clock,authorization=authorization)
    frozen=store.load_morning_engine(ACCOUNT,day)
    if frozen is None:return dict(status='BLOCKED',reason_code='MORNING_ENGINE_MISSING')
    if frozen.get('morning_window_authorization')!=authorization:return dict(status='BLOCKED',reason_code='MORNING_WINDOW_AUTHORITY_CHANGED')
    if source['plan_rowset_sha256']!=frozen['source']['plan_rowset_sha256']:
        return dict(status='BLOCKED',reason_code='MORNING_PLAN_ROWS_CHANGED')
    if candidate is not None and any(source.get(key)!=frozen['source'].get(key)
            for key in ('candidate_scope_id','candidate_sha256','candidate_revision')):
        return dict(status='BLOCKED',reason_code='MORNING_CANDIDATE_CHANGED')
    if frozen['risk_limits_sha256']!=hashlib.sha256((settings.root/'config/risk_limits.yaml').read_bytes()).hexdigest():
        return dict(status='BLOCKED',reason_code='MORNING_RISK_POLICY_CHANGED')
    envelopes=_envelopes(frozen['envelopes'])
    from .news_data_limits import morning_news_facts
    meeting_plans=frozen.get('news_plan_facts') or morning_news_facts(meeting_plan_facts(frozen['source']['plan_rows']))
    missing={r['plan_id']:r for r in frozen.get('seed_freshness',{}).get('plans',[]) if r['status']!='READY'}
    for fact in meeting_plans:
        if fact['plan_id'] not in missing:continue
        old=fact['publication_facts'].get('source_data_limits') or {}
        detail=dict(old,status='DATA_LIMITED',authority='0589_PLAN_HISTORY_GAPS',
            reason_codes=list(dict.fromkeys([*old.get('reason_codes',()),missing[fact['plan_id']]['reason_code']])),
            history_readiness=missing[fact['plan_id']],
            a6_explicit_review_required=True,maximum_risk_unit='PROBE')
        fact['publication_facts']={**fact['publication_facts'],'DATA_LIMITED':True,'source_data_limits':detail}
    catalog={'categories':['ACCOUNT','MARKET','PLAN'], 'items':[
        {'id':'MORNING:ACCOUNT','category':'ACCOUNT','value':frozen['snapshot']},
        {'id':'MORNING:MARKET','category':'MARKET','value':{'market_regime':source['market_regime'],
            'external_adjustment':frozen['external_adjustment']}},
        *({'id':'MORNING:PLAN:'+r['plan_id'],'category':'PLAN','value':r} for r in meeting_plans)]}
    repository=TeamPromptRepository(settings.root/'prompts/team_v2')
    journal=ModelJudgmentJournal(directory/'model.db')
    client=client or OpenAICompatibleModelClient(settings,max_attempts=1)
    role='A6' if stage=='risk' else 'A3';run_id=(candidate['scope_id']+':review:'+day if candidate else 'team-morning:'+day)
    risk_meeting=candidate['scope_id']+':risk' if candidate else 'morning-risk:'+day
    risk_call=candidate['scope_id']+':A6' if candidate else 'A6:morning:'+day
    pm_meeting=candidate['scope_id']+':pm' if candidate else 'morning-pm:'+day
    pm_call=candidate['scope_id']+':PM' if candidate else 'A3:morning:'+day
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
        PLANS=meeting_plans,PLANS_TODAY=meeting_plans,PRIOR_OUTCOME_FEEDBACK={'status':'INSUFFICIENT_TEN_DAYS'},
        OVERNIGHT_EXTERNAL_SNAPSHOT=frozen['external_adjustment'])
    common=dict(repository=repository,client=client,journal=journal,store=store,account_id=ACCOUNT,run_id=run_id,
        model=settings.review_model,trade_date=day,source_trade_date=frozen['source']['source_trade_date'],clock=clock,
        required_envelope=required,replacements=values,envelopes=envelopes,timeout_seconds=100,
        max_output_tokens=settings.model_max_output_tokens,allow_model_call=True)
    if stage=='risk':
        from ..pipeline.team_v2_cluster_quota import build_cluster_quota_context
        from .team_v2_pm_policy import priority_order
        quota_context=build_cluster_quota_context(engine_slots=frozen['capacity_slots'],
            cluster_caps=frozen['cluster_caps'],
            plan_clusters={p['plan_id']:p['cluster'] for p in frozen['source']['plans']},
            plan_priority_order=priority_order(meeting_plans))
        from .team_v2_risk_policy import load_risk_policy, risk_output_budget
        risk_policy=load_risk_policy(settings.root)
        common.update(timeout_seconds=risk_policy['timeout_seconds'],max_output_tokens=risk_output_budget(
            risk_policy,len(meeting_plans),len(frozen['cluster_caps']),settings.model_max_output_tokens))
        if authorization is not None:
            common['timeout_seconds']=min(common['timeout_seconds'],max(0,(deadline-clock()).total_seconds()-2))
            common.update(window_authorization=authorization,window_source=frozen['source'])
        result=run_risk_premarket(**common,meeting_id=risk_meeting,call_id=risk_call,
            engine_utilization_cap=D(frozen['engine_utilization_cap']),capacity_slots=frozen['capacity_slots'],
            cluster_caps=frozen['cluster_caps'],unready_plan_ids={e.plan_id for e in envelopes if not e.eligible},
            limits=RiskLimits.from_yaml(settings.root/'config/risk_limits.yaml'),quota_context=quota_context)
        return json.loads(canonical(dict(status=result.status,session='PREMARKET',result=asdict(result))))
    result=store.load_morning_risk(ACCOUNT,day,source_call_id=risk_call)
    if candidate is not None:
        if result is None or result.status!='READY':
            return dict(status='BLOCKED',reason_code='NATURAL_CANDIDATE_A6_NOT_ORIGINAL_READY',old_batch_preserved=True)
        from .a6_effective_review import verify_premarket_capture
        if not verify_premarket_capture(directory,engine=frozen,meeting_id=risk_meeting,
                scope_id=candidate['scope_id'],account_id=ACCOUNT,day=day):
            return dict(status='BLOCKED',reason_code='NATURAL_CANDIDATE_A6_NOT_ORIGINAL_READY',old_batch_preserved=True)
    if result is None:
        # No fabricated A6 response or success label. PM may allocate only the
        # deterministic conservative envelopes, recording the missing meeting.
        from .regeneration_data_limits import restrict_unreviewed
        result=RiskMeetingResult('A6_MISSING_CONSERVATIVE',restrict_unreviewed(tuple(conservative_envelope(e,consecutive_failures=1)
            for e in envelopes)),D(frozen['engine_utilization_cap']),1,('A6_MORNING_MISSING',))
    common.update(envelopes=result.envelopes)
    values['MARKET_STATE']['a6_morning']={'status':result.status,'reason_codes':list(result.reason_codes)}
    bounds=tuple(min(D(v),result.utilization_cap) for v in frozen['baseline_range'])
    from .team_v2_pm_policy import load_pm_policy, pm_output_budget, priority_order
    pm_policy=load_pm_policy(settings.root)
    limits=RiskLimits.from_yaml(settings.root/'config/risk_limits.yaml')
    from .team_v2_first_day import load_first_day_policy,first_day_limits
    limits=first_day_limits(limits,load_first_day_policy(settings.root/'config/risk_limits.yaml'),now=now)
    from .team_v2_pm_geometry import pm_geometry_domain
    geometry_ids,geometry_audit=pm_geometry_domain(frozen['source']['plan_rows'])
    common.update(timeout_seconds=pm_policy['timeout_seconds'],
        max_output_tokens=pm_output_budget(pm_policy,len(meeting_plans),settings.model_max_output_tokens,
            max_positions=limits.max_positions))
    pm=run_pm_premarket(**common,meeting_id=pm_meeting,call_id=pm_call,
        plan_clusters={p['plan_id']:p['cluster'] for p in frozen['source']['plans']},
        free_slots=frozen['capacity_slots'],baseline_range=bounds,
        utilization_cap=result.utilization_cap,high_conviction_ids=set(),
        fallback_enabled=False if candidate is not None else pm_policy['fallback_enabled'],fallback_priority_order=tuple(priority_order(meeting_plans)),
        max_new_entries=limits.max_new_entries,max_positions=limits.max_positions,
        geometry_plan_ids=geometry_ids,geometry_audit=geometry_audit,a6_validated=result.status=='READY',
        deadline_at=deadline,window_authorization=authorization,
        a6_degraded_conservative=result.status=='DEGRADED' and result.consecutive_failures==1)
    if candidate is not None:
        if (pm.get('status')!='READY' or pm.get('model_validated') is not True
                or pm.get('decision_authority')!='VALIDATED_MODEL'):
            return dict(status='BLOCKED',reason_code='NATURAL_CANDIDATE_PM_NOT_READY',old_batch_preserved=True,pm=pm)
        from .reviewed_batch_natural import finish_natural_candidate
        switched=finish_natural_candidate(settings,candidate,calendar=calendar,observed_at=clock())
        if switched.get('reason_code')=='NATURAL_CANDIDATE_REVIEW_ZERO_ALLOCATION':
            return dict(status='NOOP',reason_code=switched['reason_code'],
                old_batch_preserved=True,natural_batch_switch=switched,pm=pm)
        return dict(pm,natural_batch_switch=switched)
    return pm
