"""DERIVED_IDENTITY_V1: emotion statistics, never individual entry permission.

0247 uses the existing catalogue and full-market snapshot, not strict shadow
PIT. Neither supplied hashes nor these derived fields authenticate a provider.
No I/O, clock sampling, model call, rule renewal or execution-plan mutation.
"""
from collections import Counter
from collections.abc import Mapping
from datetime import date,datetime,time
from decimal import Decimal,InvalidOperation
import hashlib
import json

from .calendar import TradingCalendarError
from .shadow_price_limits import RATES,round_price_limits
from .stock_trading_rules import stock_trading_rules
from .team_v2_sources import stamp

VERSION='DERIVED_IDENTITY_V1'


def _hash(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,ensure_ascii=False,
        separators=(',',':'),default=str,allow_nan=False).encode()).hexdigest()


def _number(value):
    if value is None or isinstance(value,bool):return None
    try:value=Decimal(str(value))
    except (ValueError,TypeError,InvalidOperation):return None
    return value if value.is_finite() else None


def _symbol(row):
    symbol=row.get('thscode')
    if not isinstance(symbol,str) or len(symbol)!=9 or not symbol[:6].isdigit():
        raise ValueError('EMOTION_SYMBOL_INVALID')
    return symbol.upper()


def _index(rows):
    result={}
    for row in rows:
        if not isinstance(row,Mapping):raise ValueError('EMOTION_ROW_INVALID')
        symbol=_symbol(row)
        if symbol in result:raise ValueError('EMOTION_DUPLICATE_SYMBOL')
        result[symbol]=dict(row)
    return result


def _source_stamp(value):
    # Existing endpoints return Unix milliseconds in data.timestamp.
    if isinstance(value,(int,float)) and not isinstance(value,bool):
        from datetime import timezone
        return stamp(datetime.fromtimestamp(value/1000,timezone.utc))
    return stamp(value)


def _session(at):
    clock=at.time().replace(tzinfo=None)
    return time(9,30)<=clock<=time(11,30) or time(13)<=clock<=time(15)


def derive_identity(row,quote,*,as_of,catalogue_as_of,calendar):
    """Every derived field is accompanied by its source and observation time."""
    symbol=_symbol(row);code=symbol[:6];exchange=row.get('exchange')
    fields={};reasons=[]
    def field(key,value,source,observed):
        fields[key]=dict(value=value,source=source,as_of=observed.isoformat())
    board=('STAR' if code.startswith('68') and symbol.endswith('.SH') and exchange=='SH' else
        'SSE_MAIN' if code.startswith('60') and symbol.endswith('.SH') and exchange=='SH' else
        'CHINEXT' if code.startswith('30') and symbol.endswith('.SZ') and exchange=='SZ' else
        'SZSE_MAIN' if code.startswith('00') and symbol.endswith('.SZ') and exchange=='SZ' else None)
    field('board',board,'CATALOGUE.thscode+exchange;0247_SUPPORTED_CODE_MAP',catalogue_as_of)
    field('security_type',row.get('asset_type'),'CATALOGUE.asset_type',catalogue_as_of)
    if board is None:reasons.append('UNSUPPORTED_BOARD')
    if row.get('asset_type')!='a-share':reasons.append('NOT_A_SHARE')
    name=row.get('name');is_st=None if not isinstance(name,str) or not name.strip() else name.strip().upper().startswith(('ST','*ST'))
    field('is_st',is_st,'CATALOGUE.name_PREFIX',catalogue_as_of)
    if is_st is None:reasons.append('NAME_MISSING')
    elif is_st:reasons.append('ST_EXCLUDED')
    if isinstance(name,str) and '退' in name:reasons.append('DELISTING_EXCLUDED')
    listing=row.get('list_date');eligible=False
    try:
        listed=date.fromisoformat(listing);threshold=as_of.date()
        for _ in range(4):threshold=calendar.previous_trading_day(threshold)
        eligible=calendar.is_trading_day(listed) is True and listed<=threshold
    except (ValueError,TypeError,TradingCalendarError):pass
    field('listing_date',listing,'CATALOGUE.list_date',catalogue_as_of)
    field('minimum_five_sessions',eligible,'CATALOGUE.list_date+EXCHANGE_CALENDAR',catalogue_as_of)
    if not eligible:reasons.append('LISTING_PERIOD_UNPROVEN_OR_LT5')
    quote=quote or {};volume=_number(quote.get('volume'));last=_number(quote.get('last_price'))
    traded=bool(volume is not None and volume>0 and last is not None and last>0)
    field('security_status','TRADING_OBSERVED' if traded else 'NO_TRADES_OBSERVED',
        'SNAPSHOT.volume+last_price',as_of)
    if not traded:reasons.append('NO_TRADES_OR_QUOTE_MISSING')
    # One ordinary rate authority. ST override is the explicit 0247 policy;
    # these fields are not supplied to strict PIT or any order/tradability gate.
    rate=RATES.get(board)
    if board in ('SSE_MAIN','SZSE_MAIN') and is_st:rate=Decimal('.05')
    field('limit_rate',str(rate) if rate is not None else None,
        'PRICE_LIMITS.RATES;0247_MAIN_ST_5_PERCENT',catalogue_as_of)
    return dict(schema_version=VERSION,symbol=symbol,fields=fields,
        excluded_reasons=reasons,included=not reasons,authority='EMOTION_STATISTICS_ONLY')


def emotion_pool_stats(catalogue,snapshot,*,catalogue_timestamp,snapshot_timestamp,
        observed_at,calendar,previous_streaks=None,catalogue_complete=False,snapshot_complete=False):
    """No old-day relabelling and no unknown prior streak silently made zero."""
    now=stamp(observed_at);result=dict(schema_version=VERSION,status='DATA_BLOCK',
        as_of=None,authority='EMOTION_STATISTICS_ONLY',source_authenticated=False,
        universe_excludes_st=True,minimum_listing_days=5,universe_complete=False,
        limit_up_count=None,limit_down_count=None,broken_board_rate=None,highest_streak=None,
        identities=[],excluded_counts={},reason_codes=[])
    try:
        at=_source_stamp(snapshot_timestamp);cat_at=_source_stamp(catalogue_timestamp)
        result['as_of']=at.isoformat()
        if (not catalogue_complete or not snapshot_complete or at.date()!=now.date()
                or cat_at.date()!=now.date() or cat_at>now or at>now or (now-at).total_seconds()>300 or not _session(at)
                or calendar.is_trading_day(at.date()) is not True):
            raise ValueError('EMOTION_SOURCE_DATE_SESSION_OR_COMPLETENESS_UNPROVEN')
        cats=_index(catalogue);quotes=_index(snapshot);excluded=Counter();identities=[];included=[]
        denominator=0
        for symbol,row in sorted(cats.items()):
            if symbol.endswith('.BJ') or row.get('exchange')=='BJ' or symbol.startswith(('8','4','92')):
                excluded['BJ_EXCLUDED']+=1;continue
            if row.get('exchange') in ('SH','SZ') and row.get('asset_type')=='a-share':denominator+=1
            identity=derive_identity(row,quotes.get(symbol),as_of=at,catalogue_as_of=cat_at,calendar=calendar)
            identities.append(identity)
            if not identity['included']:
                excluded.update(identity['excluded_reasons']);continue
            try:
                quote=quotes[symbol];preclose=_number(quote.get('prev_price'));high=_number(quote.get('high_price'))
                last=_number(quote.get('last_price'));tick=Decimal(stock_trading_rules(symbol).tick)
                if preclose is None or high is None or high<last:raise ValueError
                upper,lower=round_price_limits(preclose,identity['fields']['limit_rate']['value'],tick)
                if not lower<=last<=upper or high>upper:raise ValueError
            except (ValueError,InvalidOperation):
                identity['included']=False;identity['excluded_reasons'].append('LIMIT_PRICE_INPUT_UNPROVEN')
                excluded['LIMIT_PRICE_INPUT_UNPROVEN']+=1;continue
            identity['limits']=dict(upper=str(upper),lower=str(lower),preclose=str(preclose),
                source='SNAPSHOT.prev_price+SHARED_ROUNDING',as_of=at.isoformat())
            included.append((symbol,last==upper,last==lower,high>=upper and last<upper))
        coverage=len(included)/denominator if denominator else 0
        result.update(identities=identities,excluded_counts=dict(sorted(excluded.items())),
            observed_count=len(included),expected_count=denominator,coverage=coverage,
            source_input_sha256=_hash(dict(catalogue=catalogue,snapshot=snapshot,
                catalogue_timestamp=catalogue_timestamp,snapshot_timestamp=snapshot_timestamp)),
            previous_streaks_sha256=_hash(previous_streaks) if previous_streaks is not None else None)
        if len(included)<500 or coverage<.75:raise ValueError('EMOTION_COVERAGE_INSUFFICIENT')
        ups=[symbol for symbol,up,_,_ in included if up];highest=0
        if ups:
            previous=previous_streaks
            if (not isinstance(previous,Mapping) or previous.get('complete') is not True
                    or previous.get('trade_date')!=calendar.previous_trading_day(at.date()).isoformat()
                    or stamp(previous.get('known_at')).date()!=at.date()
                    or stamp(previous.get('known_at'))>at.replace(hour=9,minute=30,second=0,microsecond=0)):
                raise ValueError('EMOTION_PRIOR_STREAKS_UNPROVEN')
            counts=previous.get('counts',{})
            if not isinstance(counts,Mapping) or any(type(counts.get(s)) is not int or counts[s]<0 for s in ups):
                raise ValueError('EMOTION_PRIOR_STREAKS_UNPROVEN')
            highest=max(counts[s]+1 for s in ups)
        broken=sum(row[3] for row in included);touched=len(ups)+broken
        result.update(status='READY',universe_complete=True,limit_up_count=len(ups),
            limit_down_count=sum(row[2] for row in included),highest_streak=highest,
            broken_board_count=broken,broken_board_rate=broken/touched if touched else 0)
    except (ValueError,TypeError,TradingCalendarError,InvalidOperation,OverflowError) as exc:
        code=str(exc) if type(exc) is ValueError and str(exc).startswith('EMOTION_') else 'EMOTION_SOURCE_INVALID'
        result['reason_codes']=[code]
    return result
