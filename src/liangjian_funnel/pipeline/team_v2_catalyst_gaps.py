"""0298 C5: a validated catalyst-gap theme cannot fund an ACTIVE research row.

Keep original model judgments immutable. This is a conservative server-owned
projection, including deterministic local rows merged after model batches.
"""
from collections.abc import Mapping
from copy import deepcopy

from .team_v2_a1_schema import valid_catalyst_gap


def catalyst_gap_summary(themes):
    rows=[row for row in themes if isinstance(row,Mapping)]
    gaps=[row for row in rows if valid_catalyst_gap(row)]
    return dict(theme_count=len(rows),catalyst_gap_theme_count=len(gaps),
        catalyst_gap_rate=len(gaps)/len(rows) if rows else None,
        catalyst_gap_theme_ids=sorted({row['theme_id'] for row in gaps if isinstance(row.get('theme_id'),str)}))


def enforce_catalyst_gaps(output,context=None):
    result=deepcopy(dict(output));context=context or {}
    themes=context.get('structural_themes') or result.get('structural_themes') or []
    gaps=[row for row in themes if isinstance(row,Mapping) and valid_catalyst_gap(row)]
    aliases={value for row in gaps for key in ('theme_id','display_name')
        if isinstance((value:=row.get(key)),str) and value}
    moved=[];active=[]
    for row in result.get('active_research_pool',[]):
        if isinstance(row,Mapping) and row.get('primary_theme') in aliases:
            moved.append({**row,'status':'MONITOR','reason_codes':list(dict.fromkeys(
                [*(row.get('reason_codes') or []),'CATALYST_GAP']))})
        else:active.append(row)
    if 'active_research_pool' in result:result['active_research_pool']=active
    if moved:
        existing=list(result.get('monitor_pool') or [])
        by_symbol={row.get('symbol'):i for i,row in enumerate(existing) if isinstance(row,Mapping)}
        for row in moved:
            if row['symbol'] in by_symbol:existing[by_symbol[row['symbol']]]=row
            else:by_symbol[row['symbol']]=len(existing);existing.append(row)
        result['monitor_pool']=existing
    summary=dict(result.get('analysis_summary') or {})
    summary['catalyst_evidence']=catalyst_gap_summary(themes)
    result['analysis_summary']=summary
    return result
