"""B3 proposed counterfactual contracts, isolated from production namespaces.

These research functions are never imported by workflow/monitor. Definitions
are explicit pending review, not silently selected by signal counts.
"""
from __future__ import annotations
from copy import deepcopy
from datetime import date,datetime
from functools import lru_cache
import math
from zoneinfo import ZoneInfo

from .ablation.engine import _isolated_namespace, _rewrite
from ..runtime import strategies as production

TZ=ZoneInfo('Asia/Shanghai')
VARIANTS=('V0','V1','V2','V3','V4','V5')
CONTRACT='B3_PROPOSED_20261015_V1_NOT_WEAK_V2_BOTH_PATHS_FULL_BAND_WIDTH'

class VariantEvidenceError(ValueError):
    pass

def _stamp(value):
    try:
        at=value if isinstance(value,datetime) else datetime.fromisoformat(str(value))
        if at.tzinfo is None or at.utcoffset() is None:raise ValueError()
        return at.astimezone(TZ)
    except (ValueError,TypeError):raise VariantEvidenceError('AWARE_EVIDENCE_TIME_REQUIRED')

def _positive(value):
    if isinstance(value,bool):raise VariantEvidenceError('POSITIVE_FINITE_PRICE_REQUIRED')
    try:result=float(value)
    except (ValueError,TypeError,OverflowError):raise VariantEvidenceError('POSITIVE_FINITE_PRICE_REQUIRED')
    if not math.isfinite(result) or result<=0:raise VariantEvidenceError('POSITIVE_FINITE_PRICE_REQUIRED')
    return result

@lru_cache(maxsize=6)
def build_evaluator(variant):
    if variant not in VARIANTS:raise VariantEvidenceError('UNKNOWN_B3_VARIANT')
    if variant=='V0':return production.evaluate_strategy
    namespace=_isolated_namespace()
    if variant in ('V1','V5'):
        original='prior15 is not None and latest15.low + _EPSILON >= prior15.low if versioned_entry else _fifteen_not_weak(latest15, prior15)'
        replacement=f'({original}) or (prior15 is not None and _fifteen_not_weak(latest15, prior15))'
        _rewrite(production._evaluate_trend,namespace,[(original,replacement)])
    if variant in ('V2','V5'):
        old='failed_5m_reclaim or confirmed_15m_break'
        new='(failed_5m_reclaim or confirmed_15m_break) and (profile is not StrategyProfile.TREND_MA5 or (len(fifteen) >= 2 and all(item.close < stop - _EPSILON for item in fifteen[-2:])))'
        _rewrite(production._pre_entry_risk_decision,namespace,[(old,new)])
    return namespace['evaluate_strategy']

def rebase_plan(plan,variant,evidence,decision_time):
    """Price units: max(auction*1%, ATR*.5) is the entire centered width.

    No evidence is synthesised. Missing quote exchange/receipt clocks or ATR
    knowledge time raises; calling research code must label DATA_LIMITED.
    Original targets and reward/risk gate remain unchanged.
    """
    if variant not in VARIANTS:raise VariantEvidenceError('UNKNOWN_B3_VARIANT')
    result=deepcopy(plan)
    if variant not in ('V3','V4','V5'):
        return result,{'status':'UNCHANGED','variant':variant,'contract':CONTRACT}
    now=_stamp(decision_time)
    if evidence.get('auction_status')!='ORIGINAL_TIMESTAMPED':
        raise VariantEvidenceError('ORIGINAL_AUCTION_TIMESTAMP_REQUIRED')
    q=_positive(evidence.get('auction_price'));atr=_positive(evidence.get('atr14'))
    quoted=_stamp(evidence.get('auction_at'));received=_stamp(evidence.get('auction_received_at'))
    known=_stamp(evidence.get('atr_as_of'))
    if quoted.date()!=now.date() or (quoted.hour,quoted.minute)!=(9,25) or not quoted<=received<=now:
        raise VariantEvidenceError('SAME_DAY_0925_AUCTION_REQUIRED')
    try:last_closed=date.fromisoformat(evidence.get('atr_last_closed_day'))
    except (ValueError,TypeError):raise VariantEvidenceError('PRIOR_CLOSED_ATR_KNOWLEDGE_REQUIRED')
    if known>now or known.date()>=now.date() or last_closed>=now.date() or last_closed>known.date():
        raise VariantEvidenceError('PRIOR_CLOSED_ATR_KNOWLEDGE_REQUIRED')
    original={k:deepcopy(plan.get(k)) for k in ('trigger_zone','trigger_low','trigger_high','stop_level','daily_invalidation','invalidation_level','no_chase','no_chase_price','max_chase_price') if k in plan}
    proof={'status':'RESEARCH_ONLY','variant':variant,'contract':CONTRACT,'original_levels':original,
        'auction_price':q,'atr14':atr,'auction_at':quoted.isoformat(),'auction_received_at':received.isoformat(),
        'atr_as_of':known.isoformat(),'atr_last_closed_day':evidence.get('atr_last_closed_day'),
        'evidence_basis':'ORIGINAL_TIMESTAMPED_ONLY_NOT_FIRST_MINUTE_OPEN'}
    if variant in ('V3','V5'):
        width=max(q*.01,.5*atr);low=q-width/2;high=q+width/2
        if low<=0:raise VariantEvidenceError('REBASED_ZONE_NON_POSITIVE')
        result.update(trigger_low=low,trigger_high=high,trigger_zone={'low':low,'high':high})
        for key in ('entry_reference_zone','entry_zone','pullback_zone'):
            if key in result:result[key]={'low':low,'high':high}
        proof.update(entry_band_total_width=width,entry_band_half_width=width/2)
    if variant in ('V4','V5'):
        stop=q-.8*atr;chase=q+.8*atr
        if stop<=0:raise VariantEvidenceError('REBASED_STOP_NON_POSITIVE')
        result.update(stop_level=stop,no_chase=chase)
        for key in ('daily_invalidation','invalidation_level'):
            if key in result:result[key]=stop
        for key in ('no_chase_price','max_chase_price'):
            if key in result:result[key]=chase
        proof.update(stop_level=stop,no_chase=chase,k=.8,
            old_daily_risk_level_preserved=False,
            caution='REDEFINES_REFERENCE_RISK_NOT_AN_EQUIVALENT_OLD_HARD_GATE')
    return result,proof
