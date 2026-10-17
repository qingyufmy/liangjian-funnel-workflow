"""Single CONTROL task path. Never runs the legacy entry engine first.

Missing morning/source is an explicit no-entry result, not an implicit
legacy fallback. RECORD is selected by the caller and runs the protected legacy
path. This module only writes owned Team/paper state, not execution_plans.
"""
from dataclasses import asdict,replace
from datetime import datetime,time as wall_time
from decimal import Decimal as D
import hashlib
import json
import time
from zoneinfo import ZoneInfo

from ..data.mootdx import MinuteBar
from ..data.tencent_minute import QuoteResult
from ..pipeline.model_client import OpenAICompatibleModelClient
from ..pipeline.team_v2_judgments import ModelJudgmentJournal
from ..pipeline.team_v2_prompts import TeamPromptRepository
from .bounded_work import BoundedWorkGate
from .broker.local_paper import create_broker
from .risk_engine import AccountRisk,PositionRisk,RiskLimits,conservative_envelope
from .team_v2_control_minute import run_control_minute
from .team_v2_envelope_refresh import refresh_execution_envelopes
from .team_v2_minute_projection import project_closed_minute
from .team_v2_monitor_source import collect_monitor_packet
from .team_v2_morning import _directory,_envelopes,ACCOUNT
from .team_v2_sources import read_execution_scope,decode_plan,stamp
from .team_v2_store import TeamMeetingStore,canonical
from .team_v2_risk_exception import RiskExceptionRuntime,restrict_exception_envelopes
from .team_v2_tradability import production_monitor_tradability
from .trader_guardrails import is_closed_trader_tick

TZ=ZoneInfo('Asia/Shanghai');_MARKET=BoundedWorkGate(1)


def run_monitor_control(application,*,now=None,clock=None,client=None,tradability_loader=production_monitor_tradability,
        record_fallback=None):
    from ..workflow import A4_DECISION_BUDGET_SECONDS,A4_MARKET_STATE_BUDGET_SECONDS,_intraday_market_context
    from .live_market import load_or_refresh_live_market_state
    from .team_v2_emotion_baseline import observe_full_market
    from .team_v2_live_regime_source import record_current_regime,run_source_only
    from .strategies import evaluate_strategy
    clock=clock or (lambda:datetime.now(TZ));current=stamp(now or clock());started=time.monotonic()
    deadline=started+A4_DECISION_BUDGET_SECONDS;calendar=application.trading_calendar;settings=application.settings
    if calendar.is_trading_day(current.date()) is not True:return dict(status='NOOP',reason_code='NON_TRADING_DAY')
    if not (wall_time(9,30)<=current.time().replace(tzinfo=None)<=wall_time(11,30)
            or wall_time(13)<=current.time().replace(tzinfo=None)<=wall_time(15)):
        return dict(status='NOOP',reason_code='NO_TRADING_TIME')
    directory=_directory(settings);day=current.date().isoformat()
    if not (directory/'paper.db').is_file():
        return run_source_only(settings,application.market_data,clock=clock,calendar=calendar,gate=_MARKET,deadline=deadline)
    store=TeamMeetingStore(directory/'team.db');morning=store.load_morning_engine(ACCOUNT,day)
    broker=create_broker('local_paper',database=directory/'paper.db',paper_config=settings.root/'config/paper_broker.yaml',
        risk_config=settings.root/'config/risk_limits.yaml',clock=clock,calendar=calendar,run_id='team-monitor:'+day)
    scope=read_execution_scope(settings,now=current,calendar=calendar,morning_source=morning['source'] if morning else None,
        positions=broker.get_positions())
    if morning is None and not scope['plans']:
        return run_source_only(settings,application.market_data,clock=clock,calendar=calendar,gate=_MARKET,deadline=deadline)
    packet=collect_monitor_packet(settings,scope,clock=clock,deadline=deadline,
        fetch_bars=lambda symbol,cutoff,count,end:application._fetch_live_bars(symbol,'1m',count,cutoff,deadline=end),
        fetch_quote=lambda symbol,at,end:application._fetch_live_quote(symbol,at,deadline=end))
    if packet['status']=='NOOP':return packet
    if record_fallback is not None:
        from .team_v2_record_protection import protect_packet
        return protect_packet(settings,scope=scope,packet=packet,broker=broker,store=store,clock=clock,
            account_id=ACCOUNT,deadline_monotonic=deadline,reason_code=record_fallback['reason_code'],
            tradability_loader=tradability_loader)
    from .team_v2_quote_evidence import archive_monitor_quotes
    try:quote_archive=archive_monitor_quotes(settings,packet,used_at=stamp(clock()))
    except Exception:
        quote_archive=dict(status='FAILED',reason_code='CONTROL_ORIGINAL_QUOTE_ARCHIVE_FAILED')
    from .team_v2_first_day import load_first_day_policy,observe_first_day_packet,latch_record,first_day_limits
    first_policy=load_first_day_policy(settings.root/'config/risk_limits.yaml')
    first_observation=observe_first_day_packet(store,first_policy,account_id=ACCOUNT,packet=packet,now=stamp(clock()),
        guard_anomaly=quote_archive['status']!='ARCHIVED' or any(
            p.get('quote',{}).get('reason_code')=='CONTROL_REPLAY_QUOTE_FORBIDDEN' for p in packet['sources'].values()))
    if first_observation['record_required']:
        from .team_v2_record_protection import protect_packet
        protection=protect_packet(settings,scope=scope,packet=packet,broker=broker,store=store,clock=clock,
            account_id=ACCOUNT,deadline_monotonic=deadline,
            reason_code=first_observation['latch']['reason_code'],tradability_loader=tradability_loader)
        return dict(status='CONTROL_RECORD_REQUIRED',mode='record',record_effective_next_tick=True,
            first_day_observation=first_observation,original_quote_archive=quote_archive,
            current_tick='PROTECT_OWNED_ONLY_NO_SECOND_ENGINE_OR_BUDGET_RESET',record_protection=protection)
    cutoff=stamp(packet['execution_cutoff']);active=set(scope['active_plan_ids']);plans={};paper={};contexts={};problems=[]
    if quote_archive['status']!='ARCHIVED':problems.append({'reason_code':'CONTROL_ORIGINAL_QUOTE_ARCHIVE_FAILED'})
    raw_rows={r['plan_id']:json.loads(r['payload_json']) for r in scope['plan_rows']
        if r['plan_id'] in {p['plan_id'] for p in scope['plans']}}
    # Full-market collector is the existing durable five-minute bucket. Missing
    # current data is never replaced with yesterday's regime or a fake ALLOW.
    market_deadline=min(deadline,time.monotonic()+A4_MARKET_STATE_BUDGET_SECONDS)
    market_work=_MARKET.call(lambda:load_or_refresh_live_market_state(settings,application.market_data,as_of=stamp(clock()),
        minimum_source_as_of=cutoff.replace(minute=cutoff.minute-cutoff.minute%5,second=0,microsecond=0),
        snapshot_observer=lambda result:observe_full_market(settings,result,observed_at=stamp(clock()),
            calendar=calendar,deadline=market_deadline)),deadline=market_deadline,name='team-live-market') if morning else None
    live=market_work.value if market_work and market_work.status=='READY' else {'status':'DATA_BLOCKED','reason_code':'CONTROL_LIVE_MARKET_UNAVAILABLE'}
    decision='team:'+current.strftime('%Y%m%dT%H%M')
    regime=morning['source']['market_regime'] if morning else 'DATA_BLOCK'
    try:
        current_state=record_current_regime(settings,store,live=live,cutoff=cutoff,
            observed_at=stamp(clock()),prior=regime) if morning else None
        if current.time().replace(tzinfo=None)>=wall_time(9,45):
            tick=cutoff.replace(minute=cutoff.minute-cutoff.minute%5,second=0,microsecond=0)
            current_state=current_state or store.load_live_regime(day,tick)
            regime=current_state['regime'] if current_state and current_state.get('live_authority') else 'DATA_BLOCK'
    except Exception:
        # Classification/persistence failure cannot skip this minute's existing
        # holdings and hard exits. New entry is blocked, no fake prior fallback.
        regime='DATA_BLOCK';current_state={'status':'DATA_BLOCK','leader_open':False,'radar_open':False}
        problems.append({'reason_code':'CONTROL_LIVE_REGIME_UPDATE_FAILED'})
    live={**live,'team_v2_regime':current_state or {'status':'DATA_BLOCK','reason_code':'LIVE_REGIME_TICK_NOT_YET_OBSERVED'}}
    # A retry reuses the first captured market view, not a second same-minute
    # query relabelled as the original model input. A changed packet is rejected.
    market=store.freeze_control_context('market:'+decision,source_sha256=packet['packet_sha256'],
        body=dict(live=live,regime=regime))
    live=market['live'];regime=market['regime']
    references={};evaluations={};entry_proven={};model_contexts={}
    for row in scope['plans']:
        plan=decode_plan(row);packed=packet['sources'].get(plan.symbol,{});bars=tuple(MinuteBar.model_validate(b) for b in packed.get('bars',()))
        projected=project_closed_minute(bars,(),observed_at=stamp(packet['captured_at']))
        bar=projected.get('paper_bar');quote=QuoteResult.model_validate(packed.get('quote',{'symbol':plan.symbol,'reason_code':'MISSING'}))
        if quote_archive['status']!='ARCHIVED':
            quote=quote.model_copy(update={'complete':False,'reason_code':'CONTROL_ORIGINAL_QUOTE_ARCHIVE_FAILED'})
        proof=tradability_loader(settings,symbol=plan.symbol,now=stamp(packet['captured_at']),bar=bar,
            quote=quote.quote if quote.complete else None,plan=raw_rows[plan.plan_id],
            minute_bars=bars) if bar is not None else {'status':'DATA_LIMITED'}
        ready=bool(bar is not None and packed.get('minute_complete') and proof.get('status')=='KNOWN')
        if ready:
            paper[plan.symbol]=replace(bar,tradability_known=True,halted=proof['halted'],limit_up=proof['limit_up'],limit_down=proof['limit_down'])
        else:problems.append(dict(plan_id=plan.plan_id,reason_code=proof.get('reason_code') or
            ('CONTROL_PRODUCTION_CONTRACT_UNPROVEN' if bar else 'CONTROL_MINUTE_PROJECTION_UNPROVEN')))
        # 0247: unavailable preflight is an annotation, not a new entry gate.
        # Only a detected changed formal rule body pauses new entries all day.
        rule_blocked=bool(morning and morning.get('rule_preflight',{}).get('status')=='BLOCK')
        entry_proven[plan.plan_id]=(ready and quote.complete and proof.get('entry_tradable',False)
            and not rule_blocked and quote_archive['status']=='ARCHIVED')
        if raw_rows[plan.plan_id].get('strategy_profile')=='LEADER_INTRADAY':
            entry_proven[plan.plan_id] &= bool(live.get('team_v2_regime',{}).get('leader_open'))
        plan=replace(plan,price=D(str(quote.quote.price)) if quote.complete else bar.close if bar is not None else plan.price,
            data_ready=ready,tradable=ready and not proof.get('halted',True))
        plans[plan.plan_id]=plan
        context=_intraday_market_context(plan.symbol,bars,(),current=stamp(packet['captured_at']),execution_cutoff=cutoff,
            realtime_quote=quote.quote if quote.complete else None,live_market_state=live)
        context={**context,'original_tradability_contract':proof}
        contexts[plan.plan_id]=context
        from .team_v2_trader_inputs import model_closed_context
        model_contexts[plan.plan_id]=model_closed_context(context,bars,cutoff=cutoff)
        if plan.plan_id in active:
            value=evaluate_strategy(raw_rows[plan.plan_id],bars,now=cutoff,decision_time=current,market_context=context)
            evaluations[plan.plan_id]=value.model_dump(mode='json')
            references[plan.plan_id]={**{x:'MET' for x in value.met_conditions},**{x:'UNMET' for x in value.unmet_conditions}}
    if morning is None:
        result=run_control_minute(session_kwargs=dict(broker=broker,store=store,clock=clock,account_id=ACCOUNT,all_plans=plans),
            closed_bars=paper,deadline_monotonic=deadline,model_enabled=False)
        return dict(status='DEGRADED',reason_code='CONTROL_MORNING_ENGINE_MISSING',result=result,source_reasons=problems)
    reviewed=store.load_morning_risk(ACCOUNT,day,source_call_id='A6:morning:'+day)
    upper=reviewed.envelopes if reviewed is not None else tuple(conservative_envelope(e,consecutive_failures=1)
        for e in _envelopes(morning['envelopes']))
    limits=RiskLimits.from_yaml(settings.root/'config/risk_limits.yaml')
    limits=first_day_limits(limits,first_policy,now=current)
    exception_runtime=RiskExceptionRuntime(store)
    exception_runtime.expire_pending(ACCOUNT,day,now=stamp(clock()))
    restrictions=exception_runtime.restrictions(ACCOUNT,day)
    utilization_cap=reviewed.utilization_cap if reviewed is not None else D(morning['engine_utilization_cap'])
    pm_upper=store.load_pm_target_upper('morning-pm:'+day,account_id=ACCOUNT,trade_date=day)
    utilization_cap=min(utilization_cap,pm_upper if pm_upper is not None else D(0))
    if restrictions['utilization_cap'] is not None:utilization_cap=min(utilization_cap,D(restrictions['utilization_cap']))
    def refresh():
        if hashlib.sha256((settings.root/'config/risk_limits.yaml').read_bytes()).hexdigest()!=morning['risk_limits_sha256']:
            raise ValueError('CONTROL_MORNING_RISK_POLICY_CHANGED')
        snapshot=broker.planning_snapshot();view=snapshot['account'];basis=snapshot['risk_day']
        account=AccountRisk(D(view['nav']),D(view['cash']),D(basis['day_open_nav']),max(D(snapshot['peak_nav']),D(view['nav'])),
            D(view['frozen']),hard_halt=bool(snapshot['global_halt'] or basis['halt_reason_codes']))
        positions=tuple(PositionRisk(p['plan_id'],p['symbol'],plans[p['plan_id']].cluster,p['qty'],p['sellable_qty'],
            D(p['market_value']),D(p['avg_cost'])) for p in snapshot['positions'])
        fresh=refresh_execution_envelopes(tuple(plans.values()),morning_envelopes=upper,
            account=account,positions=positions,limits=limits,now=stamp(clock()),regime=regime,
            consecutive_failures=reviewed.consecutive_failures if reviewed is not None else 1,first_day_policy=first_policy)
        fresh=restrict_exception_envelopes(fresh,plans=plans,account_id=ACCOUNT,
            restrictions=exception_runtime.restrictions(ACCOUNT,day))
        return {e.plan_id:e if entry_proven[e.plan_id] else replace(e,eligible=False,risk_unit='NONE',max_shares_cap=0,
            blocked_reasons=tuple(dict.fromkeys((*e.blocked_reasons,'CONTROL_CURRENT_ENTRY_TRADABILITY_UNPROVEN')))) for e in fresh}
    # These provisional blocked envelopes are replaced after existing intents
    # match and marks update; never used for new entry admission before refresh.
    envelopes={pid:replace(_envelopes(morning['envelopes'])[0],plan_id=pid,symbol=p.symbol,
        purpose='EXECUTION',eligible=False,max_shares_cap=0,risk_unit='NONE') for pid,p in plans.items()}
    repository=TeamPromptRepository(settings.root/'prompts/team_v2')
    catalog={'categories':['MARKET','STOCK_TECH','PLAN'],'items':[{'id':'LIVE:MARKET','category':'MARKET','value':live},
        *({'id':'PLAN:'+pid,'category':'PLAN','value':raw_rows[pid]} for pid in sorted(plans)),
        *({'id':'TECH:'+pid,'category':'STOCK_TECH','value':contexts[pid]} for pid in sorted(plans))]}
    values={key:{'status':'UNAVAILABLE'} for key in repository.document('A4').placeholders}
    values.update(EVIDENCE_CATALOG=catalog,TIME_RULES={'NO_NEW_ENTRY_AFTER':limits.no_new_entry_after},
        LIVE_MARKET_STATE=live,SECTOR_LIVE_STATE={'status':'UNAVAILABLE'},MINUTE_SOURCE=packet['packet_sha256'])
    prompt=repository.prepare('A4',values,session='INTRADAY')
    required=dict(schema_version='team-envelope/1',run_id='team-monitor:'+day,stage_id='A4',
        source_trade_date=morning['source']['source_trade_date'],execution_trade_date=day,as_of=packet['binding']['minute'],
        knowledge_cutoff=packet['captured_at'],input_snapshot_ids=[packet['packet_sha256']],config_version='team-v2/1',
        config_hash=morning['risk_limits_sha256'],prompt_version=prompt.role_version,model_name=settings.review_model,
        market_regime=regime,status='FROZEN',reason_codes=[])
    args=dict(repository=repository,client=client or OpenAICompatibleModelClient(settings,max_attempts=1),
        journal=ModelJudgmentJournal(directory/'model.db'),store=store,broker=broker,decision_id=decision,
        minute_snapshot_id=packet['packet_sha256'],call_id='A4:'+decision,account_id=ACCOUNT,run_id='team-monitor:'+day,
        model=settings.review_model,trade_date=day,source_trade_date=morning['source']['source_trade_date'],clock=clock,
        decision_at=current,closed_5m_end=cutoff,mark_as_of=cutoff,required_envelope=required,replacements=values,
        active_plan_ids=active,all_plans=plans,envelopes=envelopes,held_plan_ids={},position_snapshot=[],tech_reference=references,
        limits=limits,regime=regime,live_market_ready=regime!='DATA_BLOCK' and live.get('status') in {'READY','READY_DEGRADED'},
        snapshot_ready=True,mode='control',calendar=calendar,timeout_seconds=30,
        utilization_cap_pct=utilization_cap,
        max_output_tokens=settings.model_max_output_tokens,allow_model_call=True,pm_meeting_id='morning-pm:'+day,
        input_projection=dict(plan_rows=[r for r in scope['plan_rows'] if r['plan_id'] in plans],
            contexts=contexts,model_contexts=model_contexts,evaluations=evaluations))
    def exception_handler(**kwargs):
        from .team_v2_exception_coordinator import run_exception_after_trader
        from .team_v2_exception_policy import load_exception_policy
        return run_exception_after_trader(session_kwargs=args,live_state=live['team_v2_regime'],
            engine_utilization_cap=utilization_cap,capacity_slots=morning['capacity_slots'],
            cluster_caps=morning['cluster_caps'],trigger_policy=load_exception_policy(settings.root/'config/risk_limits.yaml'),**kwargs)
    try:
        result=run_control_minute(session_kwargs=args,closed_bars=paper,deadline_monotonic=deadline,envelope_refresh=refresh,
            exception_handler=exception_handler)
    except Exception:
        latch_record(store,first_policy,account_id=ACCOUNT,now=stamp(clock()),
            reason_code='CONTROL_FIRST_DAY_EXECUTION_EXCEPTION',packet_sha256=packet['packet_sha256'])
        raise
    if any(e.get('reason_code') in {'CONTROL_PAPER_BAR_FAILED','CONTROL_HARD_EXIT_FAILED'} for e in result.get('errors',[])):
        first_observation=latch_record(store,first_policy,account_id=ACCOUNT,now=stamp(clock()),
            reason_code='CONTROL_FIRST_DAY_GUARD_ANOMALY',packet_sha256=packet['packet_sha256'])
    return dict(status='DEGRADED' if problems or packet['status']=='DATA_LIMITED' or quote_archive['status']!='ARCHIVED' else result['status'],mode='control',
        result=result,source_reasons=problems,scope_reasons=scope['reason_codes'],packet_sha256=packet['packet_sha256'],
        original_quote_archive=quote_archive,
        first_day_observation=first_observation,
        deterministic_reference=evaluations,total_ms=(time.monotonic()-started)*1000)
