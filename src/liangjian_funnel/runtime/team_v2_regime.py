"""Frozen current-session classification and deterministic five-minute debounce.

No source fetch, old regime fabrication, execution plan, model or notification.
The owned runtime persists observations/transitions; source adapters must prove
the declared universe instead of deriving emotion counts from quote breadth.
"""
from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime,time,timedelta
from decimal import Decimal,InvalidOperation
import hashlib
from pathlib import Path
from zoneinfo import ZoneInfo

from .trader_guardrails import is_closed_trader_tick

TZ=ZoneInfo('Asia/Shanghai')
REGIMES={'TREND_MAINLINE','ROTATION_NO_MAINLINE','RISK_OFF_RETREAT'}


def number(value):
    if isinstance(value,bool):return None
    try:amount=Decimal(str(value))
    except (InvalidOperation,ValueError):return None
    return amount if amount.is_finite() else None


def load_rules(path):
    import yaml
    raw=Path(path).read_bytes();rules=yaml.safe_load(raw)
    try:
        primary=rules['primary'];emotion=rules['emotion'];on=emotion['on'];off=emotion['off']
        if (rules['schema_version']!='live-regime/1' or not isinstance(rules['version'],str)
                or rules['confirmation_bars']!=2 or rules['authority_from']!='09:45'
                or any(number(primary[k]) is None or not 0<=number(primary[k])<=1
                    for k in ('breadth_risk_off_lte','rotation_overlap_gte'))
                or type(emotion['enabled']) is not bool or emotion['status'] not in ('APPROVED','PROVISIONAL')
                or emotion['approved_by']!='tony' or emotion['exclude_st'] is not True
                or emotion['minimum_listing_days']!=5
                or (emotion['enabled'] and emotion['status']!='APPROVED')
                or any(type(obj[k]) is not int or obj[k]<0 for obj,keys in
                    ((on,('limit_up_gte','highest_streak_gte','limit_down_lte')),
                     (off,('limit_up_lt','limit_down_gte'))) for k in keys)
                or any(number(obj[k]) is None or not 0<=number(obj[k])<=1
                    for obj,k in ((on,'broken_rate_lte'),(off,'broken_rate_gt')))
                or off['risk_off'] is not True):raise ValueError
        approved=datetime.fromisoformat(emotion['approved_at'])
        if approved.utcoffset() is None:raise ValueError
    except (KeyError,TypeError,ValueError):raise ValueError('LIVE_REGIME_CONFIG_INVALID') from None
    return {**rules,'config_sha256':hashlib.sha256(raw).hexdigest()}


def classify_primary(breadth,sector_cycle,rules):
    value=number(breadth)
    if value is None or not 0<=value<=1:return 'DATA_BLOCK'
    if value<=number(rules['primary']['breadth_risk_off_lte']):return 'RISK_OFF_RETREAT'
    if not isinstance(sector_cycle,Mapping) or sector_cycle.get('available') is not True:return 'DATA_BLOCK'
    metrics=sector_cycle.get('history_metrics')
    if not isinstance(metrics,Mapping):return 'DATA_BLOCK'
    overlap=number(metrics.get('top3_daily_overlap'));persistent=metrics.get('persistent_mainline_candidates')
    if overlap is None or not 0<=overlap<=1 or not isinstance(persistent,list):return 'DATA_BLOCK'
    return ('TREND_MAINLINE' if overlap>=number(rules['primary']['rotation_overlap_gte']) and persistent
        else 'ROTATION_NO_MAINLINE')


def classify_emotion(facts,primary,rules):
    if (primary not in REGIMES or not isinstance(facts,Mapping) or facts.get('status')!='READY'
            or facts.get('universe_complete') is not True or facts.get('universe_excludes_st') is not True
            or type(facts.get('minimum_listing_days')) is not int
            or facts['minimum_listing_days']!=rules['emotion']['minimum_listing_days']):return 'DATA_BLOCK'
    up,down,streak=(facts.get(k) for k in ('limit_up_count','limit_down_count','highest_streak'))
    rate=number(facts.get('broken_board_rate'))
    if any(type(v) is not int or v<0 for v in (up,down,streak)) or rate is None or not 0<=rate<=1:return 'DATA_BLOCK'
    on=rules['emotion']['on'];off=rules['emotion']['off']
    if (primary=='RISK_OFF_RETREAT' or up<off['limit_up_lt'] or rate>number(off['broken_rate_gt'])
            or down>=off['limit_down_gte']):return 'EMOTION_OFF'
    if (up>=on['limit_up_gte'] and rate<=number(on['broken_rate_lte'])
            and streak>=on['highest_streak_gte'] and down<=on['limit_down_lte']):return 'EMOTION_ON'
    return 'KEEP'


def advance(previous,*,tick,primary,emotion,prior,rules):
    if not is_closed_trader_tick(tick):raise ValueError('REGIME_TICK_INVALID')
    tick=tick.astimezone(TZ)
    if primary not in REGIMES|{'DATA_BLOCK'} or emotion not in {'EMOTION_ON','EMOTION_OFF','KEEP','DATA_BLOCK'}:
        raise ValueError('REGIME_CANDIDATE_INVALID')
    state=deepcopy(previous);last=state.get('last_tick');consecutive=False
    if last:
        last=datetime.fromisoformat(last)
        if last.utcoffset() is None or last>=tick:raise ValueError('REGIME_TICK_NOT_INCREASING')
        if last.date()!=tick.date():state={}
        else:
            if state.get('config_sha256')!=rules['config_sha256']:raise ValueError('REGIME_POLICY_CHANGED')
            consecutive=tick-last==timedelta(minutes=5)
    transitions=[]
    # No opposite-session/gapped confirmation. Confirmed history remains in the
    # audit state; missing current data cannot use it as current permission.
    if primary=='DATA_BLOCK':emotion='DATA_BLOCK'
    for kind,candidate in (('primary',primary),('emotion',emotion)):
        confirmed=state.get(kind+'_confirmed')
        if not consecutive:state.update({kind+'_pending':None,kind+'_pending_count':0})
        if candidate in ('DATA_BLOCK','KEEP') or candidate==confirmed:
            state.update({kind+'_pending':None,kind+'_pending_count':0})
        else:
            count=state.get(kind+'_pending_count',0)+1 if state.get(kind+'_pending')==candidate else 1
            state.update({kind+'_pending':candidate,kind+'_pending_count':count})
            if count>=rules['confirmation_bars']:
                state.update({kind+'_confirmed':candidate,kind+'_pending':None,kind+'_pending_count':0})
                transitions.append(dict(kind=kind,previous=confirmed,current=candidate,tick=tick.isoformat()))
    state.update(last_tick=tick.isoformat(),config_sha256=rules['config_sha256'],config_version=rules['version'])
    live=tick.time().replace(tzinfo=None)>=time.fromisoformat(rules['authority_from'])
    primary_ready=primary!='DATA_BLOCK' and state.get('primary_confirmed') in REGIMES
    emotion_ready=emotion!='DATA_BLOCK' and state.get('emotion_confirmed') in {'EMOTION_ON','EMOTION_OFF'}
    effective=state.get('primary_confirmed') if primary_ready else 'DATA_BLOCK'
    emotion_effective=state.get('emotion_confirmed') if emotion_ready else 'DATA_BLOCK'
    return dict(state=state,regime=effective if live else prior if prior in REGIMES else 'DATA_BLOCK',
        primary_status='READY' if primary_ready else 'DATA_BLOCK',emotion_state=emotion_effective,
        live_authority=live and primary_ready,
        leader_open=bool(live and primary_ready and emotion_ready and effective!='RISK_OFF_RETREAT'
            and emotion_effective=='EMOTION_ON' and rules['emotion']['enabled']),
        exit_authority_changed=False,transitions=transitions)


def frozen_candidates(facts,*,tick,observed_at,rules):
    """A previous cache bucket cannot serve as a second confirming observation."""
    if (not is_closed_trader_tick(tick) or not isinstance(observed_at,datetime)
            or observed_at.utcoffset() is None or observed_at<tick
            or observed_at-tick>timedelta(minutes=5)):
        raise ValueError('REGIME_SOURCE_CLOCK_INVALID')
    def current(raw):
        if not isinstance(raw,Mapping) or raw.get('status')!='READY':return False
        try:stamp=datetime.fromisoformat(raw['as_of'])
        except (KeyError,TypeError,ValueError):return False
        return stamp.utcoffset() is not None and tick<=stamp<=observed_at
    if not isinstance(facts,Mapping):return 'DATA_BLOCK','DATA_BLOCK'
    market=facts.get('market')
    if not current(market) or market.get('fallback') is not False:
        return 'DATA_BLOCK','DATA_BLOCK'
    sector=facts.get('sector_cycle',{})
    try:known=datetime.fromisoformat(facts['sector_cycle_known_at'])
    except (KeyError,TypeError,ValueError):known=None
    if known is None or known.utcoffset() is None or known>observed_at:sector={}
    primary=classify_primary(market.get('breadth'),sector,rules)
    emotion=facts.get('emotion')
    return primary,classify_emotion(emotion if current(emotion) else {},primary,rules)
