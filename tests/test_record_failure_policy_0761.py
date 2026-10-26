from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.runtime.record_failure_policy import requires_persistent_record,exception_reason
from liangjian_funnel.runtime.team_v2_dual_monitor import _runtime_failure
from liangjian_funnel.runtime.team_v2_first_day import forced_record


@pytest.mark.parametrize('reason', ['TEAM_MONITOR_DEGRADED','TEAM_MONITOR_EXCEPTION','QUOTE_UNAVAILABLE',
    'MODEL_TIMEOUT','PUBLICATION_MANIFEST_DISCOVERY_FAILED','CONTROL_HARD_EXIT_FAILED','CONTROL_PAPER_BAR_FAILED'])
def test_missing_evidence_or_temporary_error_does_not_become_persistent_permission_change(tmp_path,reason):
    assert not requires_persistent_record(reason)
    settings=SimpleNamespace(root=tmp_path)
    now=datetime(2026,10,27,10,tzinfo=ZoneInfo('Asia/Shanghai'))
    result=_runtime_failure(settings,now=now,reason=reason)
    assert result['record_required'] and result['persistent_record'] is False
    assert forced_record(settings,account_id='local-paper',now=now) is None


@pytest.mark.parametrize('reason',['TEAM_BROKER_ADAPTER_NOT_ENABLED','ENTRY_PATTERN_PLAN_SOURCE_CONFLICT',
    'CONTROL_RISK_LIMIT_BREACH','CONTROL_PM_ALLOCATION_OUT_OF_SCOPE',
    'CONTROL_SCOPE_ROWSET_HASH_MISMATCH','PM_ALLOCATION_BINDING_INVALID','PAPER_BAR_CONFLICT'])
def test_real_identity_and_risk_conflict_still_latch(tmp_path,reason):
    now=datetime(2026,10,27,10,tzinfo=ZoneInfo('Asia/Shanghai'));settings=SimpleNamespace(root=tmp_path)
    result=_runtime_failure(settings,now=now,reason=reason)
    assert result['record_required']
    assert forced_record(settings,account_id='local-paper',now=now)['reason_code']==reason


def test_unknown_exception_text_is_not_forwarded_as_reason():
    assert exception_reason(ValueError('provider error with private text'))=='TEAM_MONITOR_EXCEPTION'
    assert exception_reason(ValueError('ENTRY_PATTERN_PLAN_SOURCE_CONFLICT'))=='ENTRY_PATTERN_PLAN_SOURCE_CONFLICT'


def test_actual_nested_admission_conflict_is_detected_without_label_inference():
    from liangjian_funnel.runtime.record_failure_policy import receipt_reason
    actual=dict(result=dict(admissions=[dict(judgment=dict(status='REJECTED',reason_codes=['PM_ALLOCATION_BINDING_INVALID']))]))
    assert receipt_reason(actual)=='PM_ALLOCATION_BINDING_INVALID'
    actual['result']['admissions'][0]['judgment']['reason_codes']=['PM_ALLOCATION_UNAVAILABLE']
    assert receipt_reason(actual)=='TEAM_MONITOR_DEGRADED'
