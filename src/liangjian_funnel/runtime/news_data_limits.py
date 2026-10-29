"""LJ-103: explicit stock-news coverage, preserved facts and reduction-only caps."""
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import replace
import hashlib
import json

AUTHORITY='LJ103_STOCK_NEWS_SOURCE_GAPS'
NEWS_TAG='NEWS_DATA_LIMITED'
NEWS_PUBLICATION_FIELDS=frozenset(('DATA_LIMITED','NEWS_DATA_LIMITED','news_source_health_checked',
    'news_source_health_evidence','source_data_limits','risk_unit','degradation_tags'))

def news_projection_hash(payload):
    return digest({k:payload[k] for k in NEWS_PUBLICATION_FIELDS if k in payload})

def news_publication_required(stages):
    """Select the proof version from original A2 recipes, never mutable stamps."""
    return any(getattr(stage,'stage',None)=='A2' and isinstance(proof:=getattr(stage,'snapshot_proof',None),Mapping)
        and any(isinstance(node,Mapping)and node.get('recipe_kind')=='A2_STOCK_NEWS'
            for node in proof.get('chain',())) for stage in stages)

def receipt_news_detail(proofs,symbol):
    """Reproduce the new optional projection from original A2 hash recipes."""
    for proof in reversed(tuple(proofs)):
        if proof.recipe_kind!='A2_STOCK_NEWS' or proof.hash_recipe is None:continue
        recipe=proof.hash_recipe.value
        if (digest(recipe)!=proof.snapshot.value.get('snapshot_hash')
                or symbol not in recipe.get('A2_STOCK_NEWS_SCOPE',())):
            continue
        heat=recipe.get('NEWS_HEAT_SNAPSHOT')
        detail=stock_news_detail(heat,symbol)
        if detail is not None:return detail
        return dict(status='AVAILABLE',source_health_sha256=digest(heat['stock_source_health']),
                    snapshot_hash=proof.snapshot.value['snapshot_hash'])
    raise ValueError('NEWS_PUBLICATION_SOURCE_RECIPE_UNPROVEN')

def digest(value):
    return hashlib.sha256(json.dumps(value,ensure_ascii=False,sort_keys=True,
        separators=(',',':'),allow_nan=False).encode()).hexdigest()

def stock_news_detail(heat,symbol):
    """A healthy empty result is coverage; missing health is explicitly unproven."""
    source_id='open_news.eastmoney_stock.'+symbol.replace('.','_')
    rows=heat.get('stock_source_health') if isinstance(heat,Mapping) else None
    matches=[r for r in rows if isinstance(r,Mapping) and r.get('source_id')==source_id] if isinstance(rows,list) else []
    health=matches[0] if len(matches)==1 else None
    details=health.get('details') if health is not None else None
    if health is not None and health.get('available') is True and isinstance(details,Mapping) and details.get('complete') is True:
        return None
    reason=('STOCK_NEWS_SOURCE_UNAVAILABLE' if health is not None and health.get('available') is False
        else 'STOCK_NEWS_SOURCE_HEALTH_UNPROVEN')
    return dict(status='DATA_LIMITED',authority=AUTHORITY,symbol=symbol,reason_codes=[reason],
        source_reason_code=health.get('reason_code') if health else None,
        source_health=deepcopy(health),source_health_sha256=digest(health),
        source_as_of=heat.get('as_of') if isinstance(heat,Mapping) else None,
        a6_explicit_review_required=True,required_gap_treatments=['KEEP_WITH_GAP','DOWNGRADE','REJECT'],
        maximum_risk_unit='PROBE',main_rank_minimum=3)

def publication_news_detail(stages,symbol):
    """Use the actual A2 recipe, never a market-wide availability flag or title count."""
    if not isinstance(symbol,str) or not symbol:return None
    for audit in reversed(tuple(stages)):
        if getattr(audit,'stage',None)!='A2':continue
        proof=getattr(audit,'snapshot_proof',None)
        if not isinstance(proof,Mapping):continue  # Legacy isolated publication adapters.
        for node in reversed(proof.get('chain',())):
            if not isinstance(node,Mapping) or node.get('recipe_kind')!='A2_STOCK_NEWS':continue
            data=node.get('data_recipe') or {};scope=data.get('A2_STOCK_NEWS_SCOPE')
            if not isinstance(scope,list) or symbol not in scope:continue
            recipe=node.get('hash_recipe')
            if (node.get('status')!='CAPTURED' or not isinstance(recipe,Mapping)
                    or digest(recipe)!=node.get('snapshot_hash')
                    or recipe.get('NEWS_HEAT_SNAPSHOT')!=data.get('NEWS_HEAT_SNAPSHOT')
                    or recipe.get('A2_STOCK_NEWS_SCOPE')!=scope):
                return stock_news_detail({},symbol)
            heat=data.get('NEWS_HEAT_SNAPSHOT')
            detail=stock_news_detail(heat,symbol)
            return detail if detail is not None else dict(status='AVAILABLE',
                source_health_sha256=digest(heat['stock_source_health']),snapshot_hash=node['snapshot_hash'])
    return None

def merge_news_detail(payload,detail):
    if detail is None:return payload
    result=deepcopy(payload);old=result.get('source_data_limits') or {}
    if detail.get('status')=='AVAILABLE':
        result.update(news_source_health_checked=True,news_source_health_evidence=deepcopy(detail))
        return result
    if not isinstance(old,Mapping):raise ValueError('NEWS_EXISTING_SOURCE_LIMITS_INVALID')
    combined={**old,**deepcopy(detail),'reason_codes':list(dict.fromkeys(
        [*old.get('reason_codes',()),*detail['reason_codes']]))}
    # Preserve pre-existing history/A1 evidence under an explicit nested binding.
    if old:combined['prior_source_data_limits']=deepcopy(old)
    result.update(DATA_LIMITED=True,NEWS_DATA_LIMITED=True,news_source_health_checked=True,risk_unit='PROBE',source_data_limits=combined,
        degradation_tags=sorted(set(result.get('degradation_tags',()))|{'DATA_LIMITED',NEWS_TAG}))
    return result

def morning_news_facts(facts):
    result=deepcopy(facts)
    for fact in result:
        payload=fact['publication_facts'];detail=payload.get('source_data_limits') or {}
        evidence=payload.get('news_source_health_evidence')
        checked=(payload.get('news_source_health_checked') is True and isinstance(evidence,Mapping)
            and evidence.get('status')=='AVAILABLE' and all(isinstance(evidence.get(k),str)
                and len(evidence[k])==64 and all(c in '0123456789abcdef' for c in evidence[k])
                for k in ('source_health_sha256','snapshot_hash')))
        # Pre-cutover candidates without source health remain unknown, never silently healthy.
        if not checked and not payload.get('NEWS_DATA_LIMITED'):
            detail=stock_news_detail({},fact['symbol'])
            payload=merge_news_detail(payload,detail)
            payload['source_data_limits']['legacy_source_row_sha256']=fact['source_row_sha256']
            fact['publication_facts']=payload
    return result

def news_limited_ids(catalog):
    return {i['value']['plan_id'] for i in catalog.get('items',())
        if isinstance(i,Mapping) and i.get('category')=='PLAN' and isinstance(i.get('value'),Mapping)
        and ((i['value'].get('publication_facts') or {}).get('NEWS_DATA_LIMITED') is True)}

def cap_news_envelopes(envelopes,limited_ids):
    from .risk_engine import cap_risk_envelope
    return tuple(cap_risk_envelope(replace(e,plan_risk_unit='PROBE',degradation_tags=tuple(sorted(
        set(e.degradation_tags)|{'DATA_LIMITED',NEWS_TAG})))) if e.plan_id in limited_ids else e for e in envelopes)

def main_position_allowed(envelope,position):
    return position>2 or NEWS_TAG not in envelope.degradation_tags
