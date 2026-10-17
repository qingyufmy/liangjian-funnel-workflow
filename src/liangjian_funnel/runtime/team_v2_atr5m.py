"""0304 Wilder ATR(14), closed 5m originals only, no daily substitution."""
from decimal import Decimal,InvalidOperation
from .team_v2_sources import stamp


def atr5m_evidence(rows,*,cutoff):
    at=stamp(cutoff);series=[];previous=None
    for raw in rows:
        try:
            end=stamp(raw['bar_end'])
            high,low,close=(Decimal(str(raw[k])) for k in ('high','low','close'))
            if (raw.get('interval')!='5m' or raw.get('closed') is False or end>at or end.minute%5
                    or any(not x.is_finite() or x<=0 for x in (high,low,close)) or not low<=close<=high):
                raise ValueError()
            if previous:
                before=previous[0];minutes=(end-before).total_seconds()/60
                lunch=before.hour==11 and before.minute==30 and end.hour==13 and end.minute==5
                overnight=before.hour==15 and before.minute==0 and end.hour==9 and end.minute==35 and end.date()>before.date()
                if minutes!=5 and not (end.date()==before.date() and lunch) and not overnight:raise ValueError()
            tr=high-low if previous is None else max(high-low,abs(high-previous[1]),abs(low-previous[1]))
            series.append(tr);previous=(end,close)
        except (KeyError,TypeError,ValueError,InvalidOperation):
            return dict(status='UNPROVEN',value=None,reason_code='ATR5M_CLOSED_SOURCE_GAP_OR_INVALID',period=14)
    if len(series)<14:return dict(status='UNPROVEN',value=None,reason_code='ATR5M_SEED_INSUFFICIENT',period=14,seed_bars=len(series))
    value=sum(series[:14])/Decimal(14)
    for tr in series[14:]:value=(value*Decimal(13)+tr)/Decimal(14)
    return dict(status='PROVEN_CLOSED_CACHE_ONLY',value=str(value),period=14,algorithm='WILDER',
        seed_bars=len(series),last_closed_bar=previous[0].isoformat(),listing_full_history='NOT_CLAIMED')
