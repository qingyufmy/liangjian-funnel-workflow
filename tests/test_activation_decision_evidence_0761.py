from copy import deepcopy
from datetime import datetime
import json
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.runtime.activation_evidence import decision
from liangjian_funnel.runtime.state import RuntimeStore, PlanStatus

NOW=datetime(2026,10,26,9,31,10,tzinfo=ZoneInfo('Asia/Shanghai'))


def pending(tmp_path):
    store=RuntimeStore(tmp_path/'workflow.db')
    store.create_execution_plan('p','lane_1','300678.SZ',status=PlanStatus.PENDING_MORNING_REVIEW,
        expires_at=NOW.replace(hour=15,minute=0),payload=dict(source_run_id='close',symbol='300678.SZ',
        stop_level=20,trigger_high=21,no_chase=22))
    row=store.get_execution_plan('p')
    quote=dict(symbol='300678.SZ',quote_time=NOW.isoformat(),price=22.03,source_id='CONTROLLED_TENCENT')
    evidence=decision(row,quote,now=NOW,entrypoint='MONITOR_RECOVERY',reason_code='OPEN_PRICE_CHASE_BLOCK')
    return store,row,evidence


@pytest.mark.parametrize('entry',['morning','recovery'])
def test_original_trigger_and_reason_commit_atomically_with_invalidation(tmp_path,entry):
    store,row,evidence=pending(tmp_path)
    if entry=='morning':
        store.activate_pending_plan_batch([],invalidated_plan_ids=['p'],valid_from=NOW,decision_evidence={'p':evidence})
    else:
        store.activate_latest_a3_plan_batch([],invalidated_plan_ids=['p'],valid_from=NOW,source_run_id='close',
            as_of=NOW,decision_evidence={'p':evidence})
    assert store.get_execution_plan('p')['status']=='INVALIDATED'
    rows=store.list_plan_activation_evidence(plan_id='p');assert len(rows)==1
    actual=json.loads(rows[0]['body_json'])
    assert actual['action']=='INVALIDATED' and actual['quote']==evidence['quote']
    assert actual['effective_thresholds']['no_chase']==22
    assert actual['reason_code']=='OPEN_PRICE_CHASE_BLOCK'
    assert actual['before_status']=='PENDING_MORNING_REVIEW' and actual['after_status']=='INVALIDATED'
    assert RuntimeStore(store.path).healthy


def test_audit_failure_rolls_back_state_change(tmp_path):
    store,row,evidence=pending(tmp_path);bad=deepcopy(evidence);bad['quote']['price']=21
    with pytest.raises(ValueError,match='QUOTE_HASH_CONFLICT'):
        store.activate_pending_plan_batch([],invalidated_plan_ids=['p'],valid_from=NOW,decision_evidence={'p':bad})
    assert store.get_execution_plan('p')['status']=='PENDING_MORNING_REVIEW'
    assert not store.list_plan_activation_evidence(plan_id='p')


def test_source_block_is_retained_without_mutating_plan(tmp_path):
    store,row,_=pending(tmp_path)
    evidence=decision(row,None,now=NOW,entrypoint='MORNING_REVIEW',reason_code='MORNING_SOURCE_NOT_PREVIOUS_CLOSE')
    store.record_activation_blocks({'p':evidence});store.record_activation_blocks({'p':evidence})
    assert store.get_execution_plan('p')==row
    rows=store.list_plan_activation_evidence(plan_id='p');assert len(rows)==1
    actual=json.loads(rows[0]['body_json'])
    assert actual['action']=='BLOCKED' and actual['quote_evidence']=='UNAVAILABLE'
    assert actual['quote'] is None


def test_new_audit_schema_requires_maintenance_instead_of_mutating_old_schema_at_runtime(tmp_path):
    from liangjian_funnel.runtime.integrity import verify_integrity
    store,row,evidence=pending(tmp_path)
    with store._connect() as db,db:db.execute('DROP TABLE plan_activation_evidence')
    verify_integrity(store.path)  # Represents an honestly checked older schema.
    old=RuntimeStore(store.path)
    with pytest.raises(ValueError,match='SCHEMA_MAINTENANCE_REQUIRED'):
        old.activate_pending_plan_batch([],invalidated_plan_ids=['p'],valid_from=NOW,decision_evidence={'p':evidence})
    assert RuntimeStore(store.path).healthy
    migrated=RuntimeStore(store.path,integrity_maintenance=True)
    migrated.activate_pending_plan_batch([],invalidated_plan_ids=['p'],valid_from=NOW,decision_evidence={'p':evidence})
    assert RuntimeStore(store.path).healthy
