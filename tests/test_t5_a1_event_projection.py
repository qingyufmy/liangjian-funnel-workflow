"""Incremental evidence must retain catalysts and hard-risk exclusions."""
from copy import deepcopy

import pytest

from liangjian_funnel.pipeline.deterministic import _catalyst_factor, _hard_risk_events
from liangjian_funnel.pipeline.research.common import (
    FrozenInputSnapshot, _restrict_snapshot_to_symbols,
)


@pytest.mark.parametrize("key", ["DISCLOSURE_EVENTS", "RISK_EVENTS"])
def test_incremental_preserves_scoped_official_events_and_original_times(key):
    rows = [{"symbol": "000021.SZ", "announcement_title": "重大合同订单",
             "event_type": "MATERIAL_REDUCTION", "source_ref": "cninfo:original",
             "fetch_time": "2026-11-03T02:12:00+08:00"}]
    data = {key: {"available": True, "by_symbol": {
        "000021.SZ": rows, "600000.SH": [{"source_ref": "excluded"}]},
        "query_confirmed_symbols": ["000021.SZ", "600000.SH"], "coverage": 1.0}}
    original = deepcopy(data)
    snapshot = FrozenInputSnapshot(snapshot_id="frozen", snapshot_hash="a"*64,
        data=data, as_of="2026-11-03T03:30:08+08:00")
    projected = _restrict_snapshot_to_symbols(snapshot, {"000021.SZ"}).data[key]
    assert projected["by_symbol"] == {"000021.SZ": rows}
    assert projected["query_confirmed_symbols"] == ["000021.SZ"]
    assert projected["available"] is True
    assert data == original
    if key == "DISCLOSURE_EVENTS":
        assert _catalyst_factor(projected, "000021.SZ") == _catalyst_factor(data[key], "000021.SZ")
        assert _catalyst_factor(projected, "000021.SZ")[1] is True
    else:
        assert _hard_risk_events(projected) == _hard_risk_events(data[key])
        assert "000021.SZ" in _hard_risk_events(projected)
