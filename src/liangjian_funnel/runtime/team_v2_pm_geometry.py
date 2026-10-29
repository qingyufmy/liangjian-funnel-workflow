"""Read-only PM geometry domain from its already-frozen publication rows.

This does not rewrite plans, infer a missing target or borrow a current-market
RR threshold. It invokes the existing P0-6 projector with each plan's facts.
"""
from collections import Counter
from collections.abc import Mapping
import hashlib
import json

from ..pipeline.team_v2_plan_geometry import project_team_plan_geometry, project_stop_rr_geometry, GEOMETRY_FIELD
from .team_v2_store import canonical


def pm_geometry_domain(rows):
    eligible=[];audits=[];seen=set()
    for row in rows:
        if (not isinstance(row,Mapping) or not isinstance(row.get('plan_id'),str)
                or not row['plan_id'] or row['plan_id'] in seen):
            raise ValueError('PM_GEOMETRY_SOURCE_INVALID')
        seen.add(row['plan_id'])
        raw=row.get('payload_json')
        if not isinstance(raw,str):raise ValueError('PM_GEOMETRY_SOURCE_INVALID')
        payload=json.loads(raw)
        if (not isinstance(payload,dict) or payload.get('symbol')!=row.get('symbol')):
            raise ValueError('PM_GEOMETRY_SOURCE_INVALID')
        # A new P0-6 plan already contains an audit and a narrowed zone. Check
        # the current zone again; audit classification alone is not permission.
        existing=payload.get(GEOMETRY_FIELD)
        threshold=None
        if existing is not None:
            if (not isinstance(existing,Mapping)
                    or existing.get('schema_version') not in {'team-plan-geometry/1','team-plan-geometry/2'}):
                raise ValueError('PM_GEOMETRY_AUDIT_INVALID')
            threshold=existing.get('minimum_reward_risk')
        original=dict(payload);original.pop(GEOMETRY_FIELD,None)
        projector=(project_stop_rr_geometry if isinstance(existing,Mapping) and existing.get('schema_version')=='team-plan-geometry/2'
                   else project_team_plan_geometry)
        result=projector(original,minimum_reward_risk=threshold)
        if result['status']=='PUBLISHABLE':eligible.append(row['plan_id'])
        audits.append(dict(plan_id=row['plan_id'],symbol=row['symbol'],
            source_row_sha256=hashlib.sha256(canonical(row).encode()).hexdigest(),
            payload_sha256=hashlib.sha256(raw.encode()).hexdigest(),
            status=result['status'],audit=result['audit']))
    audit=dict(schema='pm-frozen-geometry-domain/1',authority='EXISTING_P06_PROJECTOR_FROZEN_PLAN_FACTS',
        plan_count=len(audits),eligible_plan_ids=eligible,
        counts=dict(Counter(r['audit']['classification'] for r in audits)),rows=audits,
        plan_writes=0,source_fetches=0)
    audit['sha256']=hashlib.sha256(canonical(audit).encode()).hexdigest()
    return frozenset(eligible),audit
