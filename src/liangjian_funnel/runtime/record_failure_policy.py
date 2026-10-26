"""A missing observation is not a proved account-wide conflict.

Temporary failures still stop this tick's entry. Only explicit identity,
evidence-integrity or hard-risk conflicts persist the all-day record latch.
"""
PERSISTENT_REASONS = frozenset({
    'TEAM_BROKER_ADAPTER_NOT_ENABLED', 'ENTRY_PATTERN_PLAN_SOURCE_CONFLICT',
    'CONTROL_MORNING_RISK_POLICY_CHANGED', 'CONTROL_REPLAY_QUOTE_FORBIDDEN',
    'CONTROL_RECORD_LATCH_HASH_INVALID', 'CONTROL_RECORD_LATCH_BINDING_INVALID',
    'CONTROL_RECORD_LATCH_SCOPE_INVALID', 'RECORD_AUTHORIZATION_CHAIN_INVALID',
    'CONTROL_PLAN_IDENTITY_MISMATCH', 'CONTROL_PM_ALLOCATION_OUT_OF_SCOPE',
    'CONTROL_RISK_LIMIT_BREACH',
    # Codes emitted by the actual packet, allocation, intent and owned ledger
    # paths. A missing reply/source is deliberately absent from this set.
    'CONTROL_SCOPE_DATE_LANE_MISMATCH','CONTROL_SCOPE_ROWSET_HASH_MISMATCH',
    'CONTROL_PACKET_HASH_MISMATCH','CONTROL_PACKET_SCOPE_CHANGED',
    'EXECUTION_MORNING_SOURCE_HASH_MISMATCH','CONTROL_MINUTE_SOURCE_CONFLICT',
    'TRADER_SCOPE_SOURCE_CONFLICT','MORNING_ENGINE_BINDING_CONFLICT',
    'A6_SESSION_BINDING_CONFLICT','TRADER_SESSION_BINDING_CONFLICT',
    'PM_MEETING_BINDING_CONFLICT','PM_ALLOCATION_ACCOUNT_DATE_MISMATCH',
    'PM_ALLOCATION_BINDING_INVALID','TEAM_MEETING_BINDING_CONFLICT',
    'ENTRY_PATTERN_PLAN_BINDING_CONFLICT','ENTRY_PATTERN_INTENT_BINDING_CONFLICT',
    'ENTRY_PATTERN_INTENT_CLOCK_CONFLICT','ENTRY_PATTERN_HELD_STOP_CONFLICT',
    'PAPER_ADMISSION_BINDING_CONFLICT','PAPER_RULE_POLICY_CONFLICT',
    'PAPER_BAR_CONFLICT','PAPER_LOT_STATE_CONFLICT','PAPER_NEGATIVE_CASH',
    'DAILY_LOSS_HALT','DRAWDOWN_HALT',
})


def requires_persistent_record(reason):
    return reason in PERSISTENT_REASONS


def exception_reason(exc):
    reason=getattr(exc,'reason_code',None) or str(exc)
    # Provider messages, URLs and arbitrary exception text never enter alerts.
    return reason if reason in PERSISTENT_REASONS else 'TEAM_MONITOR_EXCEPTION'


def receipt_reason(result):
    # Results can carry a rejected admission several levels below the lane
    # receipt. Match only explicit known codes; never infer a conflict from
    # FAILED/DEGRADED, an absent allocation, or arbitrary provider text.
    def find(value):
        if isinstance(value,str):return value if value in PERSISTENT_REASONS else None
        if isinstance(value,dict):
            for key in ('reason_code','reason_codes'):
                found=find(value.get(key))
                if found:return found
            for nested in value.values():
                if isinstance(nested,(dict,list,tuple)):
                    found=find(nested)
                    if found:return found
        elif isinstance(value,(list,tuple)):
            for nested in value:
                found=find(nested)
                if found:return found
        return None
    return find(result) or 'TEAM_MONITOR_DEGRADED'


def queue_temporary_failure(settings, *, now, reason):
    from .provider_health import queue_internal
    event=queue_internal(settings,kind='TEAM_TICK_BLOCKED',source_id=now.date().isoformat()+':'+reason,
        text='Team本轮入场阻断：'+reason+'；保留持仓保护。缺少有效资料/模型结果不等于已证身份或风控冲突，未新增当日持久锁。',now=now)
    return dict(status='RECORD_THIS_TICK',record_required=True,persistent_record=False,
                reason_code=reason,internal_alert=event,next_step='NEXT_NATURAL_TICK_NO_MANUAL_RELEASE')
