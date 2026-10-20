"""Project existing normalized execution bars, never synthetic ticks, to paper.

This boundary proves the closed source window and actual turnover basis. It
does not authenticate a supplied price-limit/tradability proof: that separate
rule adapter must finish before a projected bar may claim tradability_known.
"""
from datetime import timedelta
from decimal import Decimal
import hashlib

from liangjian_paper_broker.matching import MinuteBar as PaperMinuteBar
from ..data.execution_evidence import execution_evidence
from ..data.mootdx import MinuteBar
from ..data.session_windows import closed_window_ends
from .team_v2_sources import stamp


def project_closed_minute(one_minute, native_five, *, observed_at, execution_cutoff=None):
    now=stamp(observed_at);latest=now.replace(second=0,microsecond=0)-timedelta(minutes=1)
    cutoff=latest if execution_cutoff is None else stamp(execution_cutoff)
    # Dedicated close consumer may prove an older explicit session close;
    # the original arrival time is retained, never replaced with a fake clock.
    if cutoff>latest or cutoff.date()!=now.date():raise ValueError('CONTROL_PROJECTION_CUTOFF_INVALID')
    result=dict(status='DATA_LIMITED',reason_codes=[],paper_bar=None,
        execution_cutoff=cutoff.isoformat(),source_requests_started=0,
        authority='CLOSED_BAR_ONLY_TRADABILITY_NOT_AUTHENTICATED')
    if not isinstance(one_minute,(tuple,list)) or not one_minute:
        return {**result,'reason_codes':['CONTROL_SOURCE_MINUTE_MISSING']}
    if any(not isinstance(bar,MinuteBar) for bar in one_minute):
        return {**result,'reason_codes':['CONTROL_SOURCE_MINUTE_TYPE_INVALID']}
    last=one_minute[-1];symbol=last.symbol
    if (any(b.symbol!=symbol or b.interval!='1m' or b.adjust_mode not in {'none','raw'}
            or b.evidence_kind!='MARKET_BAR' or b.volume_unit!='shares' for b in one_minute)
            or last.bar_end!=cutoff):
        return {**result,'reason_codes':['CONTROL_SOURCE_CLOSED_WINDOW_UNPROVEN']}
    if tuple(b.bar_end for b in one_minute)!=closed_window_ends(cutoff,'1m'):
        return {**result,'reason_codes':['CONTROL_SOURCE_SESSION_INCOMPLETE']}
    if (not isinstance(native_five,(tuple,list)) or any(not isinstance(b,MinuteBar)
            or b.symbol!=symbol or b.interval!='5m' or b.bar_end>cutoff
            or b.evidence_kind!='MARKET_BAR' for b in native_five)):
        return {**result,'reason_codes':['CONTROL_NATIVE_COMPARISON_SCOPE_INVALID']}
    _,proof=execution_evidence(tuple(one_minute),tuple(native_five),as_of=cutoff)
    result.update(symbol=symbol,execution_data=proof,
        last_bar_sha256=hashlib.sha256(last.model_dump_json().encode()).hexdigest(),
        amount_kind=last.amount_kind)
    if proof['entry_window_complete'] is not True:
        return {**result,'reason_codes':['CONTROL_SOURCE_SESSION_INCOMPLETE']}
    if proof['native_5m_comparison']['status']=='CONFLICT':
        return {**result,'reason_codes':['CONTROL_SOURCE_INDEPENDENT_BAR_CONFLICT']}
    # Claude 0242: actual volume is still mandatory. Estimated amount never
    # becomes VWAP; matching uses explicitly tagged side-specific OHLC prices.
    if (last.volume == 0 and last.amount == 0
            and last.open == last.high == last.low == last.close and last.close > 0):
        # A complete, flat zero-turnover minute is a market fact, not a source
        # outage. Keep its actual last price for valuation; never fabricate a
        # fill/entry bar or reuse an older bar as this minute's evidence.
        return {**result, 'status':'NO_TRADES', 'last_price':str(last.close),
            'reason_codes':['CONTROL_CLOSED_MINUTE_NO_TRADES']}
    if last.volume<=0 or last.amount_kind=='reported' and last.amount<=0:
        return {**result,'reason_codes':['CONTROL_REPORTED_MINUTE_VWAP_UNPROVEN']}
    reported=last.amount_kind=='reported'
    vwap=Decimal(str(last.amount))/Decimal(str(last.volume)) if reported else None
    if reported and not Decimal(str(last.low))<=vwap<=Decimal(str(last.high)):
        return {**result,'reason_codes':['CONTROL_REPORTED_MINUTE_VWAP_OUTSIDE_BAR']}
    projected=PaperMinuteBar(symbol,last.bar_end,Decimal(str(last.low)),Decimal(str(last.high)),
        vwap,int(last.volume),closed=True,tradability_known=False,close=Decimal(str(last.close)),
        fill_basis='REPORTED_VWAP' if reported else 'OHLC_ESTIMATED_PAPER_FILL')
    if Decimal(str(last.volume))!=projected.volume_shares:
        return {**result,'reason_codes':['CONTROL_SOURCE_VOLUME_NOT_INTEGER_SHARES']}
    # Source completeness and order eligibility are different claims.
    return {**result,'status':'BAR_PROJECTED_RULES_PENDING','paper_bar':projected,
        'vwap_basis':'REPORTED_AMOUNT_DIVIDED_BY_SHARES' if reported else 'NOT_AVAILABLE_OHLC_SIDE_SPECIFIC_ESTIMATE',
        'reason_codes':['CONTROL_CURRENT_TRADABILITY_PROOF_PENDING']}
