"""G-2 daily observed strategy mix; context warnings are not gate vetoes.

Uses already-frozen A5 facts only. Never scans history or acquires rankings.
Event groups are references to the same raw events, not additional decisions.
"""
from collections import Counter,defaultdict
from collections.abc import Mapping

PROFILES=frozenset({'TREND_MA5','MA520_SWING','LEADER_INTRADAY'})
REGIME_ENTRY_BLOCK_CODES=frozenset({'A4_LIVE_MARKET_BLOCK_WAIT'})


def strategy_portfolio_diagnostics(facts):
    plans=(facts.get('a3') or {}).get('plans')
    events=(facts.get('a4') or {}).get('events')
    if not isinstance(plans,list) or not isinstance(events,list):
        return dict(status='UNAVAILABLE',reason_code='FROZEN_PLAN_OR_EVENT_ROWS_MISSING',
            strategy_profile_counts=None,regime_entry_block_events_by_profile=None)
    profile_by_id={};counts=Counter();warnings=[]
    for row in plans:
        if not isinstance(row,Mapping) or not isinstance(row.get('plan_id'),str) or not row['plan_id']:
            warnings.append('PLAN_ID_MISSING');continue
        pid=row['plan_id'];profile=row.get('strategy_profile')
        if pid in profile_by_id:
            warnings.append('DUPLICATE_PLAN_ID');continue
        profile_by_id[pid]=profile if isinstance(profile,str) and profile in PROFILES else 'UNKNOWN'
        counts[profile_by_id[pid]]+=1
    blocked=Counter();minutes=defaultdict(set);unique={};conflicts=set();unbound=0
    for row in events:
        if not isinstance(row,Mapping):warnings.append('EVENT_ROW_INVALID');continue
        identity=row.get('evidence_id') or row.get('event_id')
        if not isinstance(identity,str) or not identity:
            warnings.append('EVENT_ID_MISSING');continue
        if identity in unique and unique[identity]!=row:conflicts.add(identity)
        else:unique[identity]=dict(row)
    for identity,row in unique.items():
        if identity in conflicts:continue
        raw_reasons=row.get('strategy_reason_codes') or []
        if not isinstance(raw_reasons,(list,tuple)) or any(not isinstance(r,str) for r in raw_reasons):
            warnings.append('EVENT_REASON_CODES_INVALID');continue
        reasons=set(raw_reasons)
        if isinstance(row.get('reason_code'),str):reasons.add(row['reason_code'])
        if not reasons&REGIME_ENTRY_BLOCK_CODES:continue
        pid=row.get('plan_id')
        profile=profile_by_id.get(pid) if isinstance(pid,str) else None
        declared=row.get('strategy_profile')
        if profile is None or declared is not None and declared!=profile:
            unbound+=1;continue
        blocked[profile]+=1
        if isinstance(row.get('minute_end'),str) and isinstance(row.get('symbol'),str):
            minutes[profile].add((row['symbol'],row['minute_end']))
    if conflicts:warnings.append('CONFLICTING_EVENT_ID_EXCLUDED')
    return dict(status='DATA_LIMITED' if warnings else 'OBSERVED_FROZEN_ROWS',trade_date=facts.get('trade_date'),
        window='CURRENT_FROZEN_DAY_ONLY_NOT_TWENTY_DAY_COVERAGE',
        plan_scope='ALL_FROZEN_PUBLISHED_ROWS_NOT_SURVIVING_ACTIVE_POOL',
        unique_plan_count=len(profile_by_id),strategy_profile_counts=dict(sorted(counts.items())),
        original_event_record_count=len(events),distinct_event_ids=len(unique),
        conflicting_event_ids=len(conflicts),unbound_regime_block_events=unbound,
        regime_entry_block_events_by_profile={p:blocked[p] for p in sorted(PROFILES)},
        regime_entry_block_symbol_minutes_by_profile={p:len(minutes[p]) for p in sorted(PROFILES)},
        regime_block_definition='ONLY_EXPLICIT_A4_LIVE_MARKET_BLOCK_WAIT_NOT_PRIOR_RISK_OFF_CONTEXT',
        waiting_groups_not_added='a4.observation_groups_reference_the_same_a4.events',
        coverage_claim='OBSERVED_ROWS_ONLY_NOT_WHOLE_SESSION_SUCCESS_RATE',
        selected_board_next_session_rank_lag=dict(status='UNAVAILABLE',value=None,
            reason_code='SAME_DOMAIN_NEXT_SESSION_RANKING_NOT_BOUND'),warnings=sorted(set(warnings)))
