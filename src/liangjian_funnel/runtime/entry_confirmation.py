"""Pure closed-bar pattern evidence. No model, source, store or order calls.

Legacy is a delegation token and never inspects new inputs. A confirmed pattern
is NOT an admission: the caller must still apply all unchanged execution gates
and bind B's raised stop / PROBE to the actual intent, not merely display them.
"""
from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime,time as wall_time,timedelta
import math
import time
from pathlib import Path
from statistics import mean
from zoneinfo import ZoneInfo
from .bounded_work import BoundedWorkGate

TZ=ZoneInfo('Asia/Shanghai')
_DAILY_FACT_WORK=BoundedWorkGate(1)

DEFAULT_CONFIG = dict(mode='pattern_v1',patterns={'A':True,'B':True,'C':True},atr_period=20,volume_lookback=6,segment_min_bars=2,
    tolerance_atr=0.5,tolerance_reference_pct=0.003,a_pullback_volume_ratio=0.7,
    a_wick_body_ratio=1.0,a_wick_volume_ratio=1.2,b_breakout_volume_ratio=1.5,
    b_retest_volume_ratio=0.6,b_retest_upper_tolerance=2.0,b_max_breakouts=2,
    platform_days=20,platform_quantile=0.8,c_decline_atr=1.5,c_volume_ratio=1.5,
    c_recovery_fraction=0.382,c_upper_wick_body_ratio=1.5,c_sync_bars=1)


def load_entry_confirmation(root):
    import yaml
    path=Path(root)/'config/entry_confirmation.yaml'
    try:
        body=yaml.safe_load(path.read_text(encoding='utf-8')) if path.is_file() else {
            # Historical roots without the new versioned file retain the
            # old contract. A release must install and verify the real file.
            'schema_version':'entry-confirmation/1','entry_confirmation':{**DEFAULT_CONFIG,'mode':'legacy'}}
    except yaml.YAMLError as error:
        raise ValueError('ENTRY_CONFIRMATION_CONFIG_INVALID') from error
    if not isinstance(body,Mapping) or body.get('schema_version')!='entry-confirmation/1':
        raise ValueError('ENTRY_CONFIRMATION_CONFIG_INVALID')
    return checked_config(body.get('entry_confirmation'))


def runtime_confirmation_overlay(root):
    """Server-only overlay; invalid config blocks new entries, never exits.

    Legacy adds no field to the historical monitor/model/card inputs. Daily
    facts are deliberately not taken from an unverified plan extension.
    """
    try:
        config=load_entry_confirmation(root)
    except (OSError,ValueError,TypeError):
        return {'entry_confirmation_config':{'mode':'INVALID_CONFIG'}}
    return {} if config['mode']=='legacy' else {'entry_confirmation_config':config,
        'entry_pattern_execution_supported':True}


def runtime_confirmation_facts(settings,plan_rows,*,current_trade_date,deadline_monotonic):
    """One existing-source read under the caller's unchanged absolute budget.

    Only small, summary-SHA-pinned publication manifests are read here. A
    stalled filesystem cannot consume the whole original minute budget or
    multiply workers; absent facts block B only, not protective exits.
    """
    from .entry_confirmation_sources import read_runtime_daily_facts
    rows=tuple(dict(row) for row in plan_rows)
    facts_deadline=min(deadline_monotonic,time.monotonic()+1.0)
    work=_DAILY_FACT_WORK.call(lambda:read_runtime_daily_facts(settings,rows,
        current_trade_date=current_trade_date,deadline_monotonic=facts_deadline),
        deadline=facts_deadline,name='entry-confirmation-daily-facts')
    if work.status=='READY':return work.value
    return {row['plan_id']:dict(status='UNPROVEN',reason_codes=['DAILY_FACTS_'+work.status],
        daily_closes=[],previous_day_high=None,daily_ma5=None) for row in rows if row.get('plan_id')}


def checked_config(config):
    if not isinstance(config,Mapping) or set(config)!=set(DEFAULT_CONFIG):
        raise ValueError('ENTRY_CONFIRMATION_CONFIG_INVALID')
    if config['mode'] not in {'legacy','pattern_v1'}:raise ValueError('ENTRY_CONFIRMATION_CONFIG_INVALID')
    if (not isinstance(config['patterns'],Mapping) or set(config['patterns'])!={'A','B','C'}
            or any(type(value) is not bool for value in config['patterns'].values())):
        raise ValueError('ENTRY_CONFIRMATION_CONFIG_INVALID')
    integers={'atr_period','volume_lookback','segment_min_bars','b_max_breakouts','platform_days','c_sync_bars'}
    for key in integers:
        if type(config[key]) is not int or not 1<=config[key]<=500:raise ValueError('ENTRY_CONFIRMATION_CONFIG_INVALID')
    if config['b_max_breakouts']>2 or config['c_sync_bars']>1:raise ValueError('ENTRY_CONFIRMATION_CONFIG_INVALID')
    for key in set(config)-integers-{'mode','patterns'}:
        if type(config[key]) not in (int,float) or not math.isfinite(config[key]) or config[key]<=0:
            raise ValueError('ENTRY_CONFIRMATION_CONFIG_INVALID')
    if config['platform_quantile']>1 or config['c_recovery_fraction']>1:raise ValueError('ENTRY_CONFIRMATION_CONFIG_INVALID')
    return {**config,'patterns':dict(config['patterns'])}


def _get(row,*keys):
    for key in keys:
        value=row.get(key) if isinstance(row,Mapping) else getattr(row,key,None)
        if value is not None:return value
    return None


def _number(value):
    if isinstance(value,bool):return None
    try:value=float(value)
    except (TypeError,ValueError,OverflowError):return None
    return value if math.isfinite(value) else None


def _time(value):
    try:value=value if isinstance(value,datetime) else datetime.fromisoformat(str(value).replace('Z','+00:00'))
    except (ValueError,TypeError):return None
    return value.astimezone(TZ) if value.utcoffset() is not None else None


def _bars(values,*,minutes,as_of):
    result=[]
    for row in values:
        end=_time(_get(row,'end','bar_end'))
        if (end is None or end>as_of or end.minute%minutes or end.second or end.microsecond
                or _get(row,'closed') is False or _get(row,'interval') not in (None,str(minutes)+'m')):
            return None,'CONFIRM_UNCLOSED_OR_INVALID_BAR'
        clock=end.time().replace(tzinfo=None)
        if not (wall_time(9,30+minutes)<=clock<=wall_time(11,30) or wall_time(13,minutes)<=clock<=wall_time(15)):
            return None,'CONFIRM_BAR_OUTSIDE_SESSION'
        numbers={key:_number(_get(row,key)) for key in ('open','high','low','close','volume')}
        if (any(v is None for v in numbers.values()) or numbers['low']<=0 or numbers['volume']<0
                or numbers['high']<max(numbers['open'],numbers['close'],numbers['low'])
                or numbers['low']>min(numbers['open'],numbers['close'])):
            return None,'CONFIRM_BAR_INVALID'
        if result and end<=result[-1]['end']:return None,'CONFIRM_BAR_ORDER_INVALID'
        if result and minutes==5 and end.date()==result[-1]['end'].date():
            prior=result[-1]['end'];gap=(end-prior).total_seconds()
            lunch=prior.time().replace(tzinfo=None)==wall_time(11,30) and clock==wall_time(13,5)
            if gap!=300 and not lunch:return None,'CONFIRM_CLOSED_BAR_GAP'
        elif result and minutes==5:
            if result[-1]['end'].time().replace(tzinfo=None)!=wall_time(15) or clock!=wall_time(9,35):
                return None,'CONFIRM_CLOSED_BAR_GAP'
        result.append(dict(end=end,amount=_number(_get(row,'amount')),**numbers))
    return result,None


def _stamp(rows):return [r['end'].isoformat() for r in rows]


def _fact(value,*,as_of):
    if isinstance(value,Mapping):
        at=_time(_get(value,'as_of','bar_end','end'));number=_number(_get(value,'value','close','high'))
        if at is None or at>as_of:return None,None
        return number,at.isoformat()
    number=_number(value)
    return number,as_of.isoformat() if number is not None else None


def _condition(rows,name,passed,value,threshold,source):
    rows[name]=dict(status='UNPROVEN' if passed is None else 'MET' if passed else 'UNMET',
        value=value,threshold=threshold,source_as_of=source)


def _finish(pattern,conditions,required,*,close,metrics=None,stop=None,force_probe=False):
    states=[conditions[k]['status'] for k in required]
    confirmed=all(s=='MET' for s in states)
    return dict(mode='pattern_v1',status='CONFIRMED' if confirmed else 'DATA_LIMITED' if 'UNPROVEN' in states else 'NOT_CONFIRMED',
        confirmed=confirmed,pattern=pattern,reason_codes=[] if confirmed else [k for k in required if conditions[k]['status']!='MET'],
        conditions=conditions,required_conditions=list(required),current_entry_close=close,tightened_stop=stop,
        force_probe=force_probe,metrics=metrics or {})


def _atr(rows,period):
    if len(rows)<period:return None
    start=len(rows)-period;values=[]
    for i in range(start,len(rows)):
        row=rows[i];previous=rows[i-1]['close'] if i else row['close']
        values.append(max(row['high']-row['low'],abs(row['high']-previous),abs(row['low']-previous)))
    return mean(values)


def _v6(rows,index,count):
    return mean(r['volume'] for r in rows[index-count:index]) if index>=count else None


def _prefix_vwap(rows,index):
    window=[r for r in rows[:index+1] if r['end'].date()==rows[index]['end'].date()]
    if (not window or any(r.get('amount') is None or r['amount']<0 for r in window)
            or sum(r['volume'] for r in window)<=0 or sum(r['amount'] for r in window)<=0):return None,None
    return sum(r['amount'] for r in window)/sum(r['volume'] for r in window),rows[index]['end'].isoformat()


def _segments(rows,minimum):
    result=[];start=1;direction=0
    for i in range(1,len(rows)):
        sign=(rows[i]['close']>rows[i-1]['close'])-(rows[i]['close']<rows[i-1]['close'])
        if sign!=direction:
            if direction and i-start>=minimum:result.append(dict(direction=direction,start=start,end=i-1))
            start=i;direction=sign
    if direction and len(rows)-start>=minimum:result.append(dict(direction=direction,start=start,end=len(rows)-1))
    return result


def _zone(plan):
    for key in ('trigger_zone','entry_reference_zone','entry_zone','pullback_zone'):
        zone=plan.get(key)
        if isinstance(zone,Mapping):return _number(_get(zone,'low','min')),_number(_get(zone,'high','max'))
    return _number(plan.get('trigger_low')),_number(plan.get('trigger_high'))


def _levels(plan):
    stop=next((_number(plan[k]) for k in ('stop_level','invalidation_level','daily_invalidation') if plan.get(k) is not None),None)
    target=next((_number(plan[k]) for k in ('first_resistance','pressure_reduce_price','observation_target') if plan.get(k) is not None),None)
    strategy=plan.get('strategy_facts',{})
    if target is None and isinstance(strategy,Mapping):
        targets=strategy.get('observation_targets',{})
        if isinstance(targets,Mapping):target=_number(targets.get('r2'))
    risk=plan.get('risk',{})
    if stop is None and isinstance(risk,Mapping):stop=_number(_get(risk,'stop_level','invalidation_level'))
    facts=plan.get('deterministic_price_evidence',{})
    rr=_number(facts.get('minimum_reward_risk')) if isinstance(facts,Mapping) else None
    if rr is None:rr=next((_number(plan[k]) for k in ('minimum_reward_risk','min_reward_risk') if plan.get(k) is not None),None)
    if rr is None and isinstance(risk,Mapping):rr=_number(risk.get('minimum_reward_risk'))
    chase=next((_number(plan[k]) for k in ('no_chase','no_chase_price','max_chase_price') if plan.get(k) is not None),None)
    return stop,target,rr,chase


def _geometry(conditions,plan,price,at,*,stop=None,inside_zone=True):
    old,target,rr,chase=_levels(plan);stop=old if stop is None else stop
    low,high=_zone(plan)
    actual=(target-price)/(price-stop) if None not in (target,stop) and target>price>stop else None
    _condition(conditions,'ENTRY_RR',None if rr is None or rr<=0 or actual is None else actual>=rr,actual,dict(minimum=rr,stop=stop,target=target),at)
    if inside_zone:_condition(conditions,'ENTRY_ZONE',None if low is None or high is None else low<=price<=high,price,dict(low=low,high=high),at)
    return actual


def _a(plan,rows,fifteen,cfg,daily,vwap,tol,atr):
    day=[r for r in rows if r['end'].date()==rows[-1]['end'].date()];latest=day[-1];close=latest['close'];at=latest['end'].isoformat();conditions={}
    segments=_segments(day,cfg['segment_min_bars']);pair=None
    for i in range(len(segments)-1,0,-1):
        if segments[i]['direction']==-1 and segments[i-1]['direction']==1:
            if segments[i-1]['end']+1==segments[i]['start']:pair=(segments[i-1],segments[i]);break
    up,down=pair if pair else (None,None)
    pull=day[down['start']:down['end']+1] if down else []
    rise=day[up['start']:up['end']+1] if up else []
    low,_=_zone(plan)
    ma,ma_at=_fact(daily.get('daily_ma5') if isinstance(daily.get('daily_ma5'),Mapping) else None,as_of=latest['end'])
    vw,vw_at=_fact(vwap,as_of=latest['end'])
    previous=day[down['start']-1] if down else None
    candidates=[(name,value,source) for name,value,source in (
        ('PLAN_ZONE_LOW',low,'FROZEN_PLAN'),('DAILY_MA5',ma,ma_at),('TODAY_VWAP',vw,vw_at),
        ('PRE_PULLBACK_5M_LOW',previous['low'] if previous else None,previous['end'].isoformat() if previous else None))
        if value is not None and value>0 and value<=close]
    support=max(candidates,key=lambda item:item[1]) if candidates else None
    s=support[1] if support else None;pull_low=min(r['low'] for r in pull) if pull else None
    rise_high=max(r['high'] for r in rise) if rise else None
    _condition(conditions,'A_SUPPORT',None if s is None else True,dict(level=s,source=support[0] if support else None),dict(at_or_below=close),support[2] if support else None)
    _condition(conditions,'A_PULLBACK',None if not pair or s is None else pull_low>=s-tol and close<rise_high,
        dict(low=pull_low,close=close,rise_high=rise_high),dict(support_floor=s-tol if s is not None else None),_stamp(pull))
    pv=mean(r['volume'] for r in pull) if pull else None;uv=mean(r['volume'] for r in rise) if rise else None
    _condition(conditions,'A_CONTRACTION',None if pv is None or uv is None or uv<=0 else pv<uv*cfg['a_pullback_volume_ratio'],
        dict(pullback_mean=pv,rise_mean=uv),dict(ratio=cfg['a_pullback_volume_ratio']),_stamp(rise+pull))
    raised=day[-1]['low']>day[-2]['low'] if len(day)>=2 else None
    _condition(conditions,'A_HIGHER_LOWS',None if raised is None or s is None else raised and close>=s,
        dict(prior_low=day[-2]['low'] if len(day)>=2 else None,low=latest['low'],close=close),dict(close_min=s,strictly_higher_low=True),_stamp(day[-2:]))
    body=abs(close-latest['open']);wick=min(close,latest['open'])-latest['low'];v6=_v6(rows,len(rows)-1,cfg['volume_lookback'])
    hammer=None if s is None or v6 is None or v6<=0 else wick>=body*cfg['a_wick_body_ratio'] and close>=s and latest['volume']<=v6*cfg['a_wick_volume_ratio']
    _condition(conditions,'A_LOWER_WICK',hammer,dict(wick=wick,body=body,volume=latest['volume'],v6=v6),
        dict(wick_body_ratio=cfg['a_wick_body_ratio'],volume_ratio=cfg['a_wick_volume_ratio']),at)
    stable=True if conditions['A_HIGHER_LOWS']['status']=='MET' or hammer is True else None if raised is None or hammer is None else False
    _condition(conditions,'A_STABILIZED',stable,None,dict(any_of=['A_HIGHER_LOWS','A_LOWER_WICK']),at)
    env=[r for r in fifteen if down and r['end']>=day[down['start']]['end']] or fifteen[-1:]
    _condition(conditions,'A_15M_SUPPORT',None if not env or s is None else all(r['close']>=s-tol for r in env),
        [r['close'] for r in env],dict(minimum=s-tol if s is not None else None),_stamp(env))
    _geometry(conditions,plan,close,at)
    return _finish('A_TREND_PULLBACK',conditions,('A_SUPPORT','A_PULLBACK','A_CONTRACTION','A_STABILIZED','A_15M_SUPPORT','ENTRY_ZONE','ENTRY_RR'),close=close,
        metrics=dict(atr5=atr,tol=tol,support=s,support_source=support[0] if support else None,v6=v6))


def _key_level(plan,daily,cfg,at):
    _,upper=_zone(plan);raw=daily.get('daily_closes');values=[];times=[]
    if not isinstance(raw,(list,tuple)) or len(raw)<cfg['platform_days']:return None,dict(reason_code='B_DAILY20_MISSING')
    for row in raw[-cfg['platform_days']:]:
        value,time=_fact(row,as_of=at)
        if value is None or value<=0 or time is None or _time(time).date()>=at.date():return None,dict(reason_code='B_DAILY20_INVALID')
        values.append(value);times.append(time)
    if times!=sorted(times) or len({_time(t).date() for t in times})!=len(times):return None,dict(reason_code='B_DAILY20_ORDER_INVALID')
    previous,previous_at=_fact(daily.get('previous_day_high'),as_of=at)
    if previous is None or previous<=0 or previous_at is None or _time(previous_at).date()!=_time(times[-1]).date() or upper is None:
        return None,dict(reason_code='B_PREVIOUS_HIGH_MISSING')
    ordered=sorted(values);position=(len(ordered)-1)*cfg['platform_quantile'];index=int(position)
    platform=ordered[index]+(ordered[min(index+1,len(ordered)-1)]-ordered[index])*(position-index)
    from .entry_pattern_binding import digest
    return max(upper,previous,platform),dict(plan_upper=upper,previous_high=previous,platform=platform,
        platform_quantile=cfg['platform_quantile'],daily_sources=times,previous_high_as_of=previous_at,
        daily_closes=[dict(close=value,as_of=at) for value,at in zip(values,times)],
        daily_facts_sha256=digest(daily),source_pin=daily.get('source_pin'))


def _b(plan,rows,cfg,daily,vwap,tol,atr,reference):
    latest=rows[-1];at=latest['end'];close=latest['close'];conditions={};key,k_evidence=_key_level(plan,daily,cfg,at)
    old,target,rr,chase=_levels(plan)
    _condition(conditions,'B_KEY_LEVEL',None if key is None else True,key,k_evidence,k_evidence.get('daily_sources'))
    _condition(conditions,'B_KEY_BELOW_NO_CHASE',None if key is None or chase is None else key<=chase,key,dict(no_chase=chase),'FROZEN_PLAN')
    # The two-attempt limit belongs to the WHOLE session, not the warm suffix.
    # A missing opening prefix/ATR/V6 makes prior attempts unknown. Never skip
    # those rows and then relabel a possible third breakout as the first.
    today=[(i,row) for i,row in enumerate(rows) if row['end'].date()==at.date()]
    missing=[]
    first=today[0][1]['end']
    expected=at.replace(hour=9,minute=35,second=0,microsecond=0)
    while expected<first:
        clock=expected.time().replace(tzinfo=None)
        if wall_time(9,35)<=clock<=wall_time(11,30) or wall_time(13,5)<=clock<=wall_time(15):
            missing.append(dict(as_of=expected.isoformat(),reason_codes=['CLOSED5M_PREFIX_MISSING']))
        expected+=timedelta(minutes=5)
    for i,row in today:
        reasons=[]
        if _atr(rows[:i+1],cfg['atr_period']) is None:reasons.append('ATR20_UNPROVEN')
        v6=_v6(rows,i,cfg['volume_lookback'])
        if v6 is None or v6<=0:reasons.append('V6_UNPROVEN')
        if reasons:missing.append(dict(as_of=row['end'].isoformat(),reason_codes=reasons))
    prefix_known=not missing
    _condition(conditions,'B_ATTEMPT_PREFIX',True if prefix_known else None,
        dict(missing_timestamps=missing),dict(first_closed5m='09:35:00',
        atr_period=cfg['atr_period'],volume_lookback=cfg['volume_lookback']),at.isoformat())
    attempts=0 if prefix_known else None;active=None;invalidated=[]
    if key is not None and prefix_known:
        for i,row in enumerate(rows):
            if row['end'].date()!=at.date():continue
            local_atr=_atr(rows[:i+1],cfg['atr_period']);v6=_v6(rows,i,cfg['volume_lookback'])
            if local_atr is None or v6 is None or v6<=0:continue
            local_tol=max(cfg['tolerance_atr']*local_atr,cfg['tolerance_reference_pct']*reference)
            if active is not None and row['close']<key-local_tol:
                invalidated.append(row['end'].isoformat());active=None
            if active is None and attempts<cfg['b_max_breakouts'] and row['close']>key+local_tol and row['volume']>=v6*cfg['b_breakout_volume_ratio']:
                attempts+=1;active=dict(index=i,volume=row['volume'],tol=local_tol,v6=v6,as_of=row['end'].isoformat())
    _condition(conditions,'B_EFFECTIVE_BREAKOUT',None if key is None or not prefix_known else active is not None,active,
        dict(price_above=key+tol if key is not None else None,volume_ratio=cfg['b_breakout_volume_ratio'],max_attempts=cfg['b_max_breakouts']),at.isoformat())
    # Keep the breakout as the direction anchor, not part of the pullback:
    # the first two lower closes after it are already a two-bar down segment.
    tail=rows[active['index']:] if active else []
    segments=_segments(tail,cfg['segment_min_bars']);downs=[s for s in segments if s['direction']==-1]
    segment=downs[-1] if downs else None;pull=tail[segment['start']:segment['end']+1] if segment else []
    pull_low=min(r['low'] for r in pull) if pull else None;pull_vol=mean(r['volume'] for r in pull) if pull else None
    _condition(conditions,'B_RETEST',None if key is None or not pull else key-tol<=pull_low<=key+cfg['b_retest_upper_tolerance']*tol,
        dict(low=pull_low),dict(low=key-tol if key is not None else None,high=key+cfg['b_retest_upper_tolerance']*tol if key is not None else None),_stamp(pull))
    _condition(conditions,'B_RETEST_VOLUME',None if not pull or active is None else pull_vol<active['volume']*cfg['b_retest_volume_ratio'],pull_vol,
        dict(breakout_volume=active['volume'] if active else None,ratio=cfg['b_retest_volume_ratio']),_stamp(pull))
    after=bool(pull and latest['end']>pull[-1]['end']);v6=_v6(rows,len(rows)-1,cfg['volume_lookback']);vw,vw_at=_fact(vwap,as_of=at)
    mid=(latest['high']+latest['low'])/2
    candle=None if key is None or v6 is None else after and close>key and close>=mid and latest['volume']>=v6
    reclaim=None if key is None or vw is None else after and close>=vw and vw>=key-tol and rows[-2]['close']<vw
    _condition(conditions,'B_CANDLE_ACCEPTANCE',candle,dict(close=close,midpoint=mid,volume=latest['volume']),dict(key=key,v6=v6),at.isoformat())
    _condition(conditions,'B_VWAP_RECLAIM',reclaim,dict(close=close,vwap=vw,prior_close=rows[-2]['close']),dict(key_floor=key-tol if key is not None else None),vw_at)
    accept=True if candle is True or reclaim is True else None if candle is None or reclaim is None else False
    _condition(conditions,'B_ACCEPTED',accept,None,dict(any_of=['B_CANDLE_ACCEPTANCE','B_VWAP_RECLAIM']),at.isoformat())
    candidate=pull_low-tol if pull_low is not None else None
    _condition(conditions,'B_STOP_ONLY_RAISED',None if candidate is None or old is None else candidate>=old,candidate,dict(original_stop=old),_stamp(pull))
    _condition(conditions,'B_NO_CHASE',None if chase is None else close<=chase,close,dict(maximum=chase),at.isoformat())
    _geometry(conditions,plan,close,at.isoformat(),stop=candidate,inside_zone=False)
    return _finish('B_BREAKOUT_RETEST',conditions,('B_KEY_LEVEL','B_KEY_BELOW_NO_CHASE','B_ATTEMPT_PREFIX','B_EFFECTIVE_BREAKOUT','B_RETEST','B_RETEST_VOLUME','B_ACCEPTED','B_STOP_ONLY_RAISED','B_NO_CHASE','ENTRY_RR'),
        close=close,stop=candidate,force_probe=True,metrics=dict(atr5=atr,tol=tol,key_level=key,breakout_count=attempts,
            max_breakouts=cfg['b_max_breakouts'],invalidated_at=invalidated,active_breakout=active,key_evidence=k_evidence,
            attempt_prefix_proven=prefix_known,attempt_prefix_missing=missing))


def _c(plan,rows,cfg,vwap,tol,atr):
    day=[r for r in rows if r['end'].date()==rows[-1]['end'].date()];latest=day[-1];close=latest['close'];at=latest['end'].isoformat();conditions={}
    segments=_segments(day,cfg['segment_min_bars']);down=next((s for s in reversed(segments) if s['direction']==-1 and s['end']<len(day)-1),None)
    trough=min(r['low'] for r in day[down['start']:down['end']+1]) if down else None
    peak=max(r['high'] for r in day[:down['start']]) if down else None
    decline=peak-trough if None not in (peak,trough) else None
    _condition(conditions,'C_DECLINE',None if decline is None else decline>=cfg['c_decline_atr']*atr,
        dict(peak=peak,trough=trough,decline=decline),dict(minimum=cfg['c_decline_atr']*atr),_stamp(day[down['start']:down['end']+1]) if down else [])
    volume_events=[];repair_events=[]
    for index in range(max(0,len(rows)-2),len(rows)):
        row=rows[index];stamp=row['end'].isoformat();v6=_v6(rows,index,cfg['volume_lookback']);body=abs(row['close']-row['open']);upper=row['high']-max(row['close'],row['open'])
        volume=None if v6 is None or v6<=0 else row['volume']>v6*cfg['c_volume_ratio'] and row['close']>row['open']
        not_upper=upper<body*cfg['c_upper_wick_body_ratio']
        vw,vw_at=_fact(vwap,as_of=row['end']) if index==len(rows)-1 or isinstance(vwap,Mapping) else (None,None)
        if vw is None:vw,vw_at=_prefix_vwap(rows,index)
        previous=rows[index-1]['close'] if index else None
        reclaim=None if vw is None or previous is None else row['close']>=vw and previous<vw
        recovered=None if decline is None else row['close']>=trough+cfg['c_recovery_fraction']*decline
        repair=True if reclaim is True or recovered is True else None if reclaim is None or recovered is None else False
        _condition(conditions,'C_VOLUME_'+str(index),volume,dict(volume=row['volume'],v6=v6,open=row['open'],close=row['close']),dict(ratio=cfg['c_volume_ratio'],close_above_open=True),stamp)
        _condition(conditions,'C_PRICE_REPAIR_'+str(index),repair,dict(close=row['close'],vwap=vw,vwap_as_of=vw_at,reclaim=reclaim,recovered=recovered),
            dict(recovery=trough+cfg['c_recovery_fraction']*decline if decline is not None else None,fraction=cfg['c_recovery_fraction']),stamp)
        _condition(conditions,'C_NO_LONG_UPPER_'+str(index),not_upper,dict(upper=upper,body=body),dict(maximum_ratio=cfg['c_upper_wick_body_ratio']),stamp)
        if volume is True and not_upper:volume_events.append(index)
        if repair is True and not_upper:repair_events.append(index)
    synchronized=any(abs(v-r)<=cfg['c_sync_bars'] and max(v,r)==len(rows)-1 for v in volume_events for r in repair_events)
    _condition(conditions,'C_VOLUME_PRICE_SYNCHRONIZED',synchronized,dict(volume_events=volume_events,repair_events=repair_events),dict(maximum_gap=cfg['c_sync_bars'],current_bar_required=True),_stamp(rows[-2:]))
    _geometry(conditions,plan,close,at)
    return _finish('C_VOLUME_REBOUND',conditions,('C_DECLINE','C_VOLUME_PRICE_SYNCHRONIZED','ENTRY_ZONE','ENTRY_RR'),
        close=close,metrics=dict(atr5=atr,tol=tol,decline=decline))


def evaluate_pattern(plan,five,fifteen,*,as_of,config,daily_facts=None,vwap=None,historical_five=()):
    cfg=checked_config(config)
    if cfg['mode']=='legacy':return dict(mode='legacy',status='LEGACY_DELEGATED',confirmed=None)
    if plan.get('strategy_profile') not in {'TREND_MA5','MA520_SWING'}:
        return dict(mode='pattern_v1',status='UNCHANGED_STRATEGY',confirmed=None,pattern=None,reason_codes=[])
    def disabled(pattern):
        return dict(mode='pattern_v1',status='PATTERN_DISABLED',confirmed=False,pattern=pattern,
            reason_codes=['ENTRY_PATTERN_DISABLED'],conditions={})
    if not any(cfg['patterns'].values()):return disabled(None)
    at=_time(as_of)
    if at is None:raise ValueError('ENTRY_CONFIRMATION_CLOCK_INVALID')
    current=list(five)
    # Server-owned local cache seeds ATR20/V6 only. Pattern segments/VWAP stay
    # within the current session. Never manufacture missing 5m buckets, and
    # never accept a same-day or future bar through the historical side door.
    history=list(historical_five or ())[-cfg['atr_period']:]
    if history:
        symbol=str(plan.get('symbol') or '').replace('.','').upper()
        for row in history:
            end=_time(_get(row,'end','bar_end'))
            row_symbol=_get(row,'symbol')
            if end is None or end.date()>=at.date() or (row_symbol is not None and
                    str(row_symbol).replace('.','').upper()!=symbol):
                return dict(mode='pattern_v1',status='DATA_LIMITED',confirmed=False,pattern=None,
                    reason_codes=['CONFIRM_HISTORICAL_SEED_INVALID'],conditions={})
    rows,error=_bars([*history,*current],minutes=5,as_of=at);environment,error15=_bars(fifteen,minutes=15,as_of=at)
    if error or error15:return dict(mode='pattern_v1',status='DATA_LIMITED',confirmed=False,pattern=None,reason_codes=[error or error15],conditions={})
    if not rows or rows[-1]['end'].date()!=at.date():
        return dict(mode='pattern_v1',status='DATA_LIMITED',confirmed=False,pattern=None,
            reason_codes=['CONFIRM_CURRENT_SESSION_5M_MISSING'],conditions={})
    if len(rows)<cfg['atr_period']:return dict(mode='pattern_v1',status='CONFIRM_WARMUP',confirmed=False,pattern=None,
        reason_codes=['CONFIRM_WARMUP'],conditions={},metrics=dict(closed_5m_count=len(rows),required=cfg['atr_period']))
    if rows[-1]['volume']<=0:return dict(mode='pattern_v1',status='NO_TRADES',confirmed=False,pattern=None,
        reason_codes=['CONFIRM_CLOSED_BAR_NO_TRADES'],conditions={})
    daily=daily_facts if isinstance(daily_facts,Mapping) else {}
    reference=_number(plan.get('reference_price')) or rows[-1]['close'];atr=_atr(rows,cfg['atr_period'])
    tol=max(cfg['tolerance_atr']*atr,cfg['tolerance_reference_pct']*reference)
    low,high=_zone(plan);b=None
    today=[r for r in rows if r['end'].date()==rows[-1]['end'].date()]
    if high is not None and any(r['close']>high for r in today):
        b=_b(plan,rows,cfg,daily,vwap,tol,atr,reference)
        if (b['metrics']['breakout_count'] or 0)>0 or rows[-1]['close']>high:
            return {**b,'source_as_of':rows[-1]['end'].isoformat()} if cfg['patterns']['B'] else disabled('B_BREAKOUT_RETEST')
    environment=[r for r in environment if r['end'].date()==rows[-1]['end'].date()]
    if plan.get('strategy_profile')=='TREND_MA5':
        result=_a(plan,rows,environment,cfg,daily,vwap,tol,atr) if cfg['patterns']['A'] else disabled('A_TREND_PULLBACK')
        rebound=_c(plan,rows,cfg,vwap,tol,atr) if cfg['patterns']['C'] else disabled('C_VOLUME_REBOUND')
        if rebound['confirmed']:
            result=rebound
        elif not cfg['patterns']['A']:
            result=rebound
        else:
            result['alternative_confirmation']=rebound
    else:
        result=_c(plan,rows,cfg,vwap,tol,atr) if cfg['patterns']['C'] else disabled('C_VOLUME_REBOUND')
    return {**result,'source_as_of':rows[-1]['end'].isoformat()}
