"""File-oriented six-index/plan-stock SSL facts, with no execution consumer."""
from collections.abc import Mapping
from dataclasses import asdict
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
import re

from .bars15m import ENDS, TZ, compose_15m
from .ssl import CANONICAL_SOURCE, PERIODS, evaluate_ssl, ssl_series
from .tencent_indices import parse_index_5m
from .contract import PitProof
from .tencent_daily import parse_quote_fields


def _equity_prices(payload, symbol):
    """OHLC-only parsing. Index inputs must use the explicit registry parser."""
    provider = symbol[-2:].lower()+symbol[:6]
    if not isinstance(payload, Mapping) or type(payload.get('code')) is not int or payload['code'] != 0:
        raise ValueError('DK_EQUITY_RESPONSE_INVALID')
    data = payload.get('data')
    if not isinstance(data, Mapping) or set(data) != {provider}:
        raise ValueError('DK_EQUITY_IDENTITY_MISMATCH')
    value = data[provider]
    qt = value.get('qt', {})
    quote = qt.get(provider)
    if not isinstance(quote, (list, tuple)) or len(quote) < 3 or quote[2] != symbol[:6]:
        raise ValueError('DK_EQUITY_IDENTITY_MISMATCH')
    raw = value.get('m5')
    if not isinstance(raw, list) or len(raw) > 320:
        raise ValueError('DK_EQUITY_ROWS_INVALID')
    result, previous = [], None
    for row in raw:
        if not isinstance(row, (list, tuple)) or len(row) < 6:
            raise ValueError('DK_EQUITY_ROWS_INVALID')
        end = datetime.strptime(row[0], '%Y%m%d%H%M').replace(tzinfo=TZ)
        if previous is not None and end <= previous:
            raise ValueError('DK_EQUITY_ROWS_INVALID')
        o, c, h, low = (Decimal(str(value)) for value in row[1:5])
        if any(not value.is_finite() or value <= 0 for value in (o, c, h, low)):
            raise ValueError('DK_EQUITY_ROWS_INVALID')
        result.append(dict(symbol=symbol, interval='5m', bar_end=end,
            open=o, close=c, high=h, low=low, source_id=CANONICAL_SOURCE,
            adjust_mode='none', evidence_kind='MARKET_BAR', volume=None, amount=None))
        previous = end
    return result


def json_value(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [json_value(item) for item in value]
    return value


def canonical_minute_input(payload,*,key,definitions,trade_date,input_sha256,capture_started_at,capture_completed_at):
    """One source identity/PIT parser for the collector and offline assembler."""
    indices=definitions['registry']['indices']
    if key in indices:
        identity=indices[key]
        rows=parse_index_5m(payload,registry=definitions['registry'],key=key)
    else:
        if not isinstance(key,str) or re.fullmatch(r'\d{6}\.(SH|SZ)',key) is None:
            raise ValueError('DK_MINUTE_SCOPE_INVALID')
        if key in {item['thscode'] for item in indices.values()}:raise ValueError('DK_INDEX_REGISTRY_KEY_REQUIRED')
        rows=_equity_prices(payload,key)
        provider=key[-2:].lower()+key[:6];fields=payload['data'][provider]['qt'][provider]
        identity=dict(thscode=key,tencent_code=provider,name=fields[1])
    quote=parse_quote_fields(payload['data'][identity['tencent_code']]['qt'][identity['tencent_code']],item=identity)
    if not rows:raise ValueError('DK_MINUTE_EMPTY')
    dataset=dict(payload=payload,source_id=CANONICAL_SOURCE,input_sha256=input_sha256,collection_status='VERIFIED_COLLECTION',
        pit=dict(source_trade_date=trade_date.isoformat(),target_trade_date=trade_date.isoformat(),mode='INTRADAY',
            as_of=max(row['bar_end'] for row in rows).isoformat(),market_close_at=datetime.combine(trade_date,time(15),tzinfo=TZ).isoformat(),
            provider_timestamp=quote['provider_timestamp'],capture_started_at=capture_started_at,capture_completed_at=capture_completed_at))
    return dataset,rows


def build_dk_shadow(datasets, *, plan_symbols, definitions, trade_date, observed_at, calendar):
    """Every required index and plan stock appears, including missing inputs.

    dataset = {payload, input_sha256, source_id, collection_status, pit}. This is
    a research calculation, never permission to consume a failed collection.
    """
    if (not isinstance(datasets, Mapping) or not isinstance(plan_symbols, (list, tuple))
            or any(not isinstance(s, str) or re.fullmatch(r'\d{6}\.(SH|SZ)', s) is None for s in plan_symbols)
            or len(set(plan_symbols)) != len(plan_symbols)
            or not isinstance(observed_at, datetime) or observed_at.utcoffset() is None
            or isinstance(trade_date, datetime) or not isinstance(trade_date, date)
            or calendar.is_trading_day(trade_date) is not True):
        raise ValueError('DK_SHADOW_SCOPE_INVALID')
    indices = definitions['registry']['indices']
    if set(plan_symbols)&{item['thscode'] for item in indices.values()}:
        raise ValueError('DK_PLAN_SCOPE_CONTAINS_INDEX')
    required = {**{key: item['thscode'] for key, item in indices.items()}, **{s: s for s in plan_symbols}}
    if set(datasets) - set(required):
        raise ValueError('DK_SHADOW_UNREQUESTED_SERIES')
    output = {}
    for key, symbol in required.items():
        base = dict(symbol=symbol, authority='SHADOW_ONLY', label='自研、非站点同款', status='DATA_BLOCK', periods={})
        dataset = datasets.get(key)
        if dataset is None:
            output[key] = {**base, 'reasons': ['TENCENT_SERIES_NOT_SUPPLIED']}
            continue
        try:
            if (dataset.get('source_id') != CANONICAL_SOURCE
                    or re.fullmatch('[0-9a-f]{64}', dataset['input_sha256']) is None
                    or dataset.get('collection_status') not in {'VERIFIED_COLLECTION', 'HISTORICAL_COLLECTION_NONCOMPLIANT'}):
                raise ValueError('DK_SHADOW_SOURCE_PROOF_INVALID')
            base.update(input_sha256=dataset['input_sha256'], source_id=CANONICAL_SOURCE,
                collection_status=dataset['collection_status'])
            proof=PitProof(**dataset['pit'])
            if proof.source_trade_date!=trade_date.isoformat() or proof.missing_fields():
                raise ValueError('DK_SHADOW_PIT_INCOMPLETE_OR_STALE')
            if (proof.target_trade_date!=trade_date.isoformat() or proof.mode!='INTRADAY'
                    or datetime.fromisoformat(proof.capture_completed_at)>observed_at):
                raise ValueError('DK_SHADOW_PIT_AFTER_OBSERVATION_OR_WRONG_SCOPE')
            provider=datetime.fromisoformat(proof.provider_timestamp)
            if provider.astimezone(TZ).date()!=trade_date or provider<datetime.fromisoformat(proof.as_of):
                raise ValueError('DK_SHADOW_PROVIDER_CLOCK_BEFORE_INPUT')
            base['pit']=proof.model_dump()
            rows = (parse_index_5m(dataset['payload'], registry=definitions['registry'], key=key)
                    if key in indices else _equity_prices(dataset['payload'], symbol))
            if not rows or rows[-1]['bar_end']!=datetime.fromisoformat(proof.as_of):
                raise ValueError('DK_SHADOW_INPUT_AS_OF_MISMATCH')
            dates = sorted({row['bar_end'].date() for row in rows})
            if any(day > trade_date for day in dates):
                raise ValueError('DK_SHADOW_FUTURE_DAY')
            composed = [compose_15m([row for row in rows if row['bar_end'].date() == day], symbol=symbol,
                trade_date=day, observed_at=observed_at,
                market_close_at=datetime.combine(day, time(15), tzinfo=TZ), calendar=calendar) for day in dates]
            slots = [bar for day in composed for bar in day.bars]
            closed = [bar for bar in slots if bar.status == 'CLOSED']
            if not closed:
                raise ValueError('DK_SHADOW_NO_CLOSED_BARS')
            # A partial leading day may precede a 320-bar window. Internal
            # gaps are rejected by ssl_series, not silently concatenated.
            periods = {}
            for period in PERIODS:
                points = ssl_series(closed, period=period, observed_at=observed_at, calendar=calendar)
                periods[str(period)] = dict(current=json_value(asdict(points[-1])),
                    timeline=json_value([asdict(point) for point in points]),
                    evaluation=evaluate_ssl(closed, period=period, observed_at=observed_at, calendar=calendar))
            eligible = [datetime.combine(trade_date, time.fromisoformat(slot), tzinfo=TZ) for slot in ENDS
                if datetime.combine(trade_date, time.fromisoformat(slot), tzinfo=TZ)+timedelta(seconds=60) <= observed_at]
            fresh = bool(eligible) and closed[-1].bar_end == eligible[-1]
            reasons = [] if fresh else ['DK_SHADOW_LATEST_CLOSED_BAR_MISSING']
            if dataset['collection_status'] != 'VERIFIED_COLLECTION':
                reasons.append('COLLECTION_NONCOMPLIANT_DIAGNOSTIC_ONLY')
            output[key] = {**base, 'periods': periods, 'bars': len(closed),
                'status': 'SHADOW_RECORDED' if not reasons else 'DATA_BLOCK', 'reasons': reasons,
                'complete_15m_dates': [day.trade_date.isoformat() for day in composed if all(b.status == 'CLOSED' for b in day.bars)],
                'window_notes': 'Leading incomplete slots excluded; internal gaps fail. 20-day/5-day acceptance not inferred.'}
        except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
            reason = str(exc) if type(exc) is ValueError and re.fullmatch('[A-Z0-9_]{1,100}', str(exc)) else 'DK_SHADOW_INPUT_REJECTED'
            output[key] = {**base, 'reasons': [reason]}
    return dict(schema_version='dk-shadow-facts/1', authority='SHADOW_ONLY', enabled=False,
        source_trade_date=trade_date.isoformat(), observed_at=observed_at.isoformat(), series=output,
        five_day_shadow_acceptance='NOT_COMPLETED', model_calls=0, source_calls=0, production_writes=0)
