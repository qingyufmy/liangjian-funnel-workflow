"""Reviewed ratio changes; original, different absolute limits stay intact."""
from decimal import Decimal
from types import SimpleNamespace

import pytest

from liangjian_funnel.runtime.resource_guard import ResourceSnapshot, evaluate_resources
from liangjian_funnel.runtime.storage_governance import evaluate_disk_watermark


def _resources(*, ratio, free_mb):
    return ResourceSnapshot(100,100,2048,0,free_mb,ratio,10)


@pytest.mark.parametrize('ratio,allowed', [('0.0499',False),('0.0500',True),('0.0501',True)])
def test_separate_existing_guards_at_five_percent(tmp_path,ratio,allowed):
    total=200*1024**3
    free=int(Decimal(total)*Decimal(ratio))
    watermark=evaluate_disk_watermark(tmp_path,
        usage=SimpleNamespace(total=total,used=total-free,free=free))
    assert watermark.full_rebuild_allowed is allowed
    assert watermark.incremental_write_allowed is allowed
    assert watermark.research_write_allowed is allowed
    assert watermark.as_dict()['thresholds']['critical_free_percent']==5.0
    assert watermark.as_dict()['thresholds']['block_free_percent']==5.0
    decision=evaluate_resources(tmp_path,
        probe=lambda _: _resources(ratio=float(ratio),free_mb=free/1024**2))
    assert decision.allowed is allowed


@pytest.mark.parametrize('delta,allowed',[(-1,False),(0,True),(1,True)])
def test_full_keeps_five_gib_and_incremental_keeps_no_absolute_floor(tmp_path,delta,allowed):
    total=10*1024**3
    free=5*1024**3+delta
    watermark=evaluate_disk_watermark(tmp_path,
        usage=SimpleNamespace(total=total,used=total-free,free=free))
    assert watermark.full_rebuild_allowed is allowed
    assert watermark.incremental_write_allowed is True
    assert watermark.research_write_allowed is True
    assert watermark.as_dict()['thresholds']['full_rebuild_min_free_bytes']==5*1024**3


@pytest.mark.parametrize('delta,allowed',[(-1,False),(0,True),(1,True)])
def test_research_entry_keeps_original_four_gib(tmp_path,delta,allowed):
    decision=evaluate_resources(tmp_path,
        probe=lambda _: _resources(ratio=.40,free_mb=(4*1024**3+delta)/1024**2))
    assert decision.allowed is allowed
    assert ('BLOCKED_RESOURCE_DISK_LOW' in decision.reason_codes) is (not allowed)


def test_incremental_does_not_gain_a_new_absolute_floor(tmp_path):
    total=10*1024**3;free=3*1024**3
    watermark=evaluate_disk_watermark(tmp_path,
        usage=SimpleNamespace(total=total,used=total-free,free=free))
    assert watermark.full_rebuild_allowed is False
    assert watermark.incremental_write_allowed is True
    assert watermark.research_write_allowed is True
