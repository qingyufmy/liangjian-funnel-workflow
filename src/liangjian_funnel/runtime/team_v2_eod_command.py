"""Natural close-completion edge; PM -> A6, owned advisory state only."""
from contextlib import closing
from datetime import datetime,time
from decimal import Decimal as D
import hashlib
import json

from ..pipeline.model_client import OpenAICompatibleModelClient
from ..pipeline.team_v2_judgments import ModelJudgmentJournal
from ..pipeline.team_v2_prompts import TeamPromptRepository
from .broker.local_paper import create_broker
from .calendar import ExchangeTradingCalendar
from .risk_engine import RiskLimits
from .team_v2_eod import run_team_eod
from .team_v2_morning import ACCOUNT,TZ,_directory
from .team_v2_risk_exception import RiskExceptionRuntime
from .team_v2_sources import read_held_bindings,stamp
from .team_v2_store import TeamMeetingStore,canonical


def run_eod_stage(settings,*,clock=None,calendar=None,client=None):
    clock=clock or (lambda:datetime.now(TZ));now=stamp(clock());day=now.date().isoformat()
    calendar=calendar or ExchangeTradingCalendar()
    if calendar.is_trading_day(now.date()) is not True:return dict(status='NOOP',reason_code='NON_TRADING_DAY')
    if now.time().replace(tzinfo=None)<time(15,10):return dict(status='NOOP',reason_code='TEAM_EOD_WINDOW_NOT_OPEN')
    directory=_directory(settings)
    if not all((directory/name).is_file() for name in ('paper.db','team.db')):
        return dict(status='BLOCKED',reason_code='TEAM_ACCOUNT_OR_MORNING_MISSING')
    store=TeamMeetingStore(directory/'team.db');mid='team-eod:'+day
    with closing(store._connect()) as db:
        prior=db.execute('SELECT body_json FROM control_minute_context WHERE decision_id=?',('eod-source:'+mid,)).fetchone()
    if prior:
        frozen=json.loads(prior[0])
    else:
        morning=store.load_morning_engine(ACCOUNT,day)
        if morning is None:return dict(status='BLOCKED',reason_code='MORNING_ENGINE_MISSING')
        policy_sha=hashlib.sha256((settings.root/'config/risk_limits.yaml').read_bytes()).hexdigest()
        if policy_sha!=morning['risk_limits_sha256']:return dict(status='BLOCKED',reason_code='EOD_RISK_POLICY_CHANGED')
        limits=RiskLimits.from_yaml(settings.root/'config/risk_limits.yaml')
        broker=create_broker('local_paper',database=directory/'paper.db',paper_config=settings.root/'config/paper_broker.yaml',
            risk_config=settings.root/'config/risk_limits.yaml',clock=clock,calendar=calendar,run_id=mid)
        snapshot=broker.planning_snapshot();reconcile=broker.reconcile()
        if reconcile['status']!='MATCH':return dict(status='BLOCKED',reason_code='BROKER_RECONCILE_MISMATCH')
        held=read_held_bindings(settings,snapshot['positions']);owners={r['plan_id']:r for r in held}
        positions=[dict(row,cluster=owners[row['plan_id']]['cluster']) for row in snapshot['positions']]
        events=[]
        for row in broker.get_admissions(day):
            if row['judgment'].get('status')=='REJECTED':
                events.append(dict(event_id='PAPER:'+row['signal_id'],kind='GUARDRAIL_REJECTION',evidence=row))
        for code in (snapshot.get('risk_day') or {}).get('halt_reason_codes',[]):
            events.append(dict(event_id='ENGINE:'+day+':'+code,kind='ENGINE_HALT',reason_code=code))
        risk_runtime=RiskExceptionRuntime(store);restrictions=risk_runtime.restrictions(ACCOUNT,day)
        with closing(store._connect()) as db:
            raw=db.execute('SELECT result_json FROM regime_observations WHERE trade_date=? ORDER BY tick DESC LIMIT 1',(day,)).fetchone()
            exceptions=[dict(event_id='A6:'+r[0],kind='A6_EXCEPTION',evidence=json.loads(r[1])) for r in db.execute(
                'SELECT meeting_id,result_json FROM risk_exception_reviews WHERE account_id=? AND trade_date=? ORDER BY meeting_id',(ACCOUNT,day))]
        events.extend(exceptions)
        market=json.loads(raw[0]) if raw else dict(status='UNAVAILABLE',prior=morning['source']['market_regime'])
        cap=D(morning['engine_utilization_cap'])
        risk=store.load_morning_risk(ACCOUNT,day,source_call_id='A6:morning:'+day)
        if risk is None:cap=D(0)
        else:cap=min(cap,risk.utilization_cap)
        if restrictions['utilization_cap'] is not None:cap=min(cap,D(restrictions['utilization_cap']))
        if restrictions['consecutive_failures']>=2:cap=D(0)
        frozen=dict(account_id=ACCOUNT,trade_date=day,source_trade_date=morning['source']['source_trade_date'],
            snapshot=snapshot,positions=positions,held=held,risk_events=events,market=market,
            engine_utilization_cap=cap,capacity_slots=min(limits.max_new_entries,max(0,limits.max_positions-len(positions))),
            cluster_caps=morning['cluster_caps'],observed_at=now.isoformat(),risk_limits_sha256=policy_sha)
        frozen=store.freeze_control_context('eod-source:'+mid,source_sha256=hashlib.sha256(canonical([ACCOUNT,day,mid]).encode()).hexdigest(),body=frozen)
    if frozen['account_id']!=ACCOUNT or frozen['trade_date']!=day:raise ValueError('EOD_ACCOUNT_DATE_BINDING_INVALID')
    repository=TeamPromptRepository(settings.root/'prompts/team_v2');journal=ModelJudgmentJournal(directory/'model.db')
    catalog=dict(categories=['ACCOUNT','POSITION','RISK_EVENT','MARKET'],items=[
        dict(id='EOD:ACCOUNT',category='ACCOUNT',value=frozen['snapshot']),
        dict(id='EOD:MARKET',category='MARKET',value=frozen['market']),
        *[dict(id='EOD:POSITION:'+r['symbol'],category='POSITION',value=r) for r in frozen['positions']],
        *[dict(id=e['event_id'],category='RISK_EVENT',value=e) for e in frozen['risk_events']]])
    envelopes={};values={}
    for role,session in (('A3','PM_POST_CLOSE'),('A6','POST_CLOSE')):
        value={key:{'status':'UNAVAILABLE'} for key in repository.document(role).placeholders}
        value.update(ACCOUNT_SNAPSHOT=frozen['snapshot']['account'],POSITIONS=frozen['positions'],MARKET_STATE=frozen['market'],
            RISK_EVENTS_TODAY=frozen['risk_events'],THEME_REVIEWS={'status':'UNAVAILABLE'},EVIDENCE_CATALOG=catalog,
            RISK_LIMITS={'config_sha256':frozen['risk_limits_sha256']},RISK_SESSION='POST_CLOSE')
        prompt=repository.prepare(role,value,session=session)
        envelopes[role]=dict(schema_version='team-envelope/1',run_id=mid,stage_id=role,
            source_trade_date=frozen['source_trade_date'],execution_trade_date=day,as_of=frozen['observed_at'],
            knowledge_cutoff=frozen['observed_at'],input_snapshot_ids=[hashlib.sha256(canonical(frozen).encode()).hexdigest()],
            config_version='team-v2/1',config_hash=frozen['risk_limits_sha256'],prompt_version=prompt.role_version,
            model_name=settings.review_model,market_regime=frozen['market'].get('regime','DATA_BLOCK'),status='FROZEN',reason_codes=[])
        values[role]=value
    return run_team_eod(repository=repository,client=client or OpenAICompatibleModelClient(settings,max_attempts=1),
        journal=journal,store=store,meeting_id=mid,pm_call_id='A3:'+mid,risk_call_id='A6:'+mid,
        account_id=ACCOUNT,run_id=mid,model=settings.review_model,trade_date=day,
        source_trade_date=frozen['source_trade_date'],clock=clock,pm_required_envelope=envelopes['A3'],
        risk_required_envelope=envelopes['A6'],pm_replacements=values['A3'],risk_replacements=values['A6'],
        positions=frozen['positions'],risk_events=frozen['risk_events'],catalog=catalog,
        engine_utilization_cap=D(frozen['engine_utilization_cap']),capacity_slots=frozen['capacity_slots'],
        cluster_caps=frozen['cluster_caps'],timeout_seconds=50,max_output_tokens=settings.model_max_output_tokens,
        allow_model_call=True)
