from datetime import timedelta
from decimal import Decimal
from copy import deepcopy
from liangjian_funnel.runtime.team_v2_atr5m import atr5m_evidence
from liangjian_funnel.runtime.team_v2_sources import stamp


def rows():
    start=stamp('2026-10-19T09:35:00+08:00')
    return [dict(bar_end=(start+timedelta(minutes=5*i)).isoformat(),interval='5m',high=11,low=9,close=10) for i in range(15)]


def test_exact_wilder_seed_and_smoothing_no_daily_atr_or_missing_seed_substitute():
    source=rows();before=deepcopy(source);cutoff=stamp(source[-1]['bar_end'])
    assert Decimal(atr5m_evidence(source,cutoff=cutoff)['value'])==2 and source==before
    changed=deepcopy(source);changed[-1].update(high=12)
    assert Decimal(atr5m_evidence(changed,cutoff=cutoff)['value'])==Decimal(29)/Decimal(14)
    assert atr5m_evidence(source[:13],cutoff=cutoff)['status']=='UNPROVEN'
    source[0]['interval']='daily'
    assert atr5m_evidence(source,cutoff=cutoff)['status']=='UNPROVEN'


def test_missing_or_future_closed_bars_fail_closed_and_cross_day_seed_is_allowed():
    source=rows();cutoff=stamp(source[-1]['bar_end'])
    assert atr5m_evidence(source[:4]+source[5:],cutoff=cutoff)['status']=='UNPROVEN'
    assert atr5m_evidence(source,cutoff=cutoff-timedelta(minutes=5))['status']=='UNPROVEN'
    start=stamp('2026-10-16T13:55:00+08:00')
    prior=[dict(source[0],bar_end=(start+timedelta(minutes=5*i)).isoformat()) for i in range(14)]
    prior.append(dict(source[0],bar_end='2026-10-19T09:35:00+08:00'))
    assert atr5m_evidence(prior,cutoff=stamp(prior[-1]['bar_end']))['status']=='PROVEN_CLOSED_CACHE_ONLY'
