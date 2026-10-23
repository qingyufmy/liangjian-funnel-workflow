"""Research-only A2 exclusions, derived before any model selection.

No eligibility override: rows still eligible for the normal A2/A3 technical
watch path are excluded. Relative strength orders a bounded research view;
it is not a new trading threshold. Missing facts are never invented.
"""
from collections import Counter
from collections.abc import Mapping
from dataclasses import replace
from decimal import Decimal,InvalidOperation
import hashlib
import re

from ..runtime.team_v2_store import canonical


def _hash(value):return hashlib.sha256(canonical(value).encode()).hexdigest()


def _relative(row):
    value=row.get('role_breakdown',{}).get('relative_strength') if isinstance(row.get('role_breakdown'),Mapping) else None
    if isinstance(value,bool) or not isinstance(value,(str,int,float,Decimal)):return None
    try:number=Decimal(str(value))
    except InvalidOperation:return None
    return number if number.is_finite() and 0<=number<=100 else None


def nomination_source_candidates(snapshot,review_symbols):
    """Pure full queue for the approved shared budget; old ranking unchanged."""
    from .research.common import _jsonable
    contexts=snapshot.data.get('A2_BOTTLENECK_CONTEXT')
    if not isinstance(contexts,Mapping):raise ValueError('TEAM_A2_NOMINATION_SOURCE_INVALID')
    original=_jsonable(contexts);sample=[];excluded=Counter()
    for symbol,row in sorted(original.items()):
        if symbol in review_symbols:continue
        if not isinstance(row,dict) or re.fullmatch(r'\d{6}\.(SH|SZ|BJ)',symbol) is None:
            excluded['MALFORMED_SOURCE']+=1;continue
        if row.get('deterministic_status')!='LOCAL_MONITOR':
            excluded['NOT_QUANTITATIVE_MONITOR']+=1;continue
        # These rows already enter the old technical watch path independently
        # of a nomination; do not misleadingly claim them as a new model pool.
        if row.get('top_rotation_theme') is True or row.get('stock_behavior_type')=='EMOTION' and row.get('eastmoney_hot100'):
            excluded['EXISTING_TECHNICAL_WATCH_PATH']+=1;continue
        reasons=row.get('deterministic_reason_codes');theme=row.get('theme_id');relative=_relative(row)
        if (not isinstance(reasons,list) or not reasons or any(not isinstance(v,str) or not v.strip() for v in reasons)
                or not isinstance(theme,str) or not theme.strip()
                or relative is None and row.get('selected_board_theme_match') is not True):
            excluded['FILTER_REASON_OR_RESEARCH_FACT_MISSING']+=1;continue
        sample.append(dict(symbol=symbol,theme_id=theme,company_name=row.get('company_name'),
            quant_filter_reason=';'.join(reasons),filter_reason_codes=reasons,
            relative_strength=str(relative) if relative is not None else None,
            selected_board_theme_match=row.get('selected_board_theme_match') is True,
            a2_factor_scores=row.get('a2_factor_scores'),selected_board=row.get('selected_board'),
            source_refs=['A2:FILTERED:'+_hash(row)],original_context_sha256=_hash(row),
            original_decision_id=row.get('decision_id'),source_snapshot_sha256=snapshot.snapshot_hash,
            scope='RESEARCH_ONLY_NOT_A3_OR_EXECUTION'))
    sample.sort(key=lambda r:(not r['selected_board_theme_match'],r['relative_strength'] is None,
        -Decimal(r['relative_strength']) if r['relative_strength'] is not None else 0,r['symbol']))
    return dict(candidates=sample,excluded_counts=dict(sorted(excluded.items())),
        source_context_sha256=_hash(original))


def prepare_nomination_source(snapshot,review_symbols):
    # Preserve the public top30 and its historical freeze byte-for-byte.
    queue=nomination_source_candidates(snapshot,review_symbols)
    total=len(queue['candidates']);sample=queue['candidates'][:30]
    proof=dict(schema_version='a2-filtered-sample/1',source_snapshot_sha256=snapshot.snapshot_hash,
        source_context_sha256=queue['source_context_sha256'],review_symbols=sorted(review_symbols),
        selection='EXCLUDED_LOCAL_MONITOR_NOT_EXISTING_TECHNICAL_WATCH;THEME_MATCH_THEN_ORIGINAL_RELATIVE_DESC_SYMBOL',
        eligible_before_cap=total,maximum_sample_size=30,excluded_counts=queue['excluded_counts'],
        sample_sha256=_hash(sample))
    if ('A2_QUANT_FILTERED_SAMPLE' in snapshot.data and snapshot.data['A2_QUANT_FILTERED_SAMPLE']!=sample
            or '_TEAM_A2_NOMINATION_FREEZE' in snapshot.data and snapshot.data['_TEAM_A2_NOMINATION_FREEZE']!=proof):
        raise ValueError('TEAM_A2_NOMINATION_SOURCE_MISMATCH')
    return replace(snapshot,data={**snapshot.data,'A2_QUANT_FILTERED_SAMPLE':sample,'_TEAM_A2_NOMINATION_FREEZE':proof})
