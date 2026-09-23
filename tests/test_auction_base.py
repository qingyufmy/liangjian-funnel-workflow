import copy
from datetime import datetime, timedelta
import json
from types import SimpleNamespace
from unittest.mock import Mock
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.workflow import PreparedSnapshot, ResearchSnapshot, WorkflowError
from liangjian_funnel.runtime.auction_base import (
    load_auction_base, marker_path, prepare_auction_delta, project_auction_delta, run_auction_base,
)

TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 9, 24, 9, 26, tzinfo=TZ)


def fixture(tmp_path):
    data = {"g0_symbols": ["600000.SH"], "MARKET_DATA_AS_OF": "2026-09-24T07:00:00+08:00",
            "RISK_EVENTS": {"as_of": "2026-09-24T07:00:00+08:00", "items": []},
            "RECENT_DAILY_BARS": {"600000.SH": [{"date": "2026-09-23", "close": 10}]},
            "snapshot_manifest": {"raw_snapshot_hash": "raw-hash"}}
    prepared = PreparedSnapshot(ResearchSnapshot(snapshot_id="snapshot-base12345", snapshot_hash="hash",
        as_of=NOW.replace(hour=7, minute=30), data=data), tmp_path / "base.json", 5000, 4900, 4800, 1, 0)
    generation = SimpleNamespace(generation_id="a1-1", payload={})
    app = SimpleNamespace(settings=SimpleNamespace(workflow_output_dir=tmp_path),
        _load_research_snapshot_by_id=Mock(return_value=prepared), prepare_snapshot=Mock(return_value=prepared),
        a1_registry=SimpleNamespace(require_active=Mock(return_value=generation)),
        trading_calendar=SimpleNamespace(is_trading_day=lambda d: d.weekday() < 5),
        store=SimpleNamespace(acquire_lease=Mock(return_value=True), complete_lease=Mock(), release_lease=Mock()))
    marker = {"schema_version": "auction-base/1", "status": "READY", "a1_generation_id": "a1-1",
              "started_at": NOW.replace(hour=7, minute=0).isoformat(),
              "finished_at": NOW.replace(hour=7, minute=40).isoformat(), "scope_symbols": ["600000.SH"],
              "prepared_snapshot": prepared.as_dict()}
    path = marker_path(app, NOW.date())
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(marker), encoding="utf-8")
    return app, prepared, generation, marker


def test_verified_same_day_base_loads_without_fetch(tmp_path):
    app, base, generation, _ = fixture(tmp_path)
    assert load_auction_base(app, current=NOW, generation=generation, scope=("600000.SH",)) is base
    app.prepare_snapshot.assert_not_called()


@pytest.mark.parametrize("change", [
    {"status": "RUNNING"}, {"a1_generation_id": "other"}, {"scope_symbols": []},
    {"started_at": "2026-09-23T07:00:00+08:00"},
    {"started_at": "2026-09-24T06:00:00+08:00"},
    {"finished_at": "2026-09-24T10:00:00+08:00"},
    {"prepared_snapshot": {"snapshot_id": "snapshot-base12345", "snapshot_hash": "tampered"}},
])
def test_invalid_base_never_falls_back_to_full_sync(tmp_path, change):
    app, _, generation, marker = fixture(tmp_path)
    marker.update(change)
    marker_path(app, NOW.date()).write_text(json.dumps(marker), encoding="utf-8")
    with pytest.raises(WorkflowError):
        prepare_auction_delta(app, current=NOW, generation=generation, scope=("600000.SH",))
    app.prepare_snapshot.assert_not_called()


def deltas():
    return dict(hot={"available": True, "trade_date": "2026-09-24", "record_count": 100,
                     "records": [{"symbol": f"{600000+i}.SH", "rank": i+1} for i in range(100)]},
                boards={"available": True, "trade_date": "2026-09-24"},
                quotes={"available": True, "trade_date": "2026-09-24"})


def test_projection_preserves_risk_daily_dates_and_defers_missing_candidates(tmp_path):
    _, base, _, _ = fixture(tmp_path)
    original = copy.deepcopy(dict(base.snapshot.data))
    result = project_auction_delta(base, **deltas(), observed_at=NOW)
    assert base.snapshot.data == original
    for key in ("g0_symbols", "RISK_EVENTS", "RECENT_DAILY_BARS", "MARKET_DATA_AS_OF"):
        assert result[key] == original[key]
    context = result["AUCTION_REFRESH_CONTEXT"]
    assert "600001.SH" in context["new_hot_symbols_deferred_to_full_research"]
    assert len(context["new_hot_symbols_deferred_to_full_research"]) == 99
    assert context["post_base_disclosures_not_requeried"] is True
    assert context["execution_publication"] == "UNCHANGED"
    assert context["base_snapshot_hash"] == "hash"


@pytest.mark.parametrize("field", ["hot", "boards", "quotes"])
def test_previous_day_delta_is_rejected(tmp_path, field):
    _, base, _, _ = fixture(tmp_path)
    values = deltas()
    values[field]["trade_date"] = "2026-09-23"
    with pytest.raises(WorkflowError):
        project_auction_delta(base, **values, observed_at=NOW)


def test_base_job_has_no_models_or_plan_writes(tmp_path, monkeypatch):
    import liangjian_funnel.runtime.auction_base as module
    import liangjian_funnel.workflow as workflow
    app, _, _, _ = fixture(tmp_path)
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW.replace(hour=7, minute=40)
    monkeypatch.setattr(module, "datetime", Clock)
    monkeypatch.setattr(workflow, "_active_a1_downstream_scope", lambda _: ("600000.SH",))
    result = run_auction_base(app, now=NOW.replace(hour=7, minute=0))
    assert result["status"] == "READY" and result["model_calls"] == 0
    assert result["execution_publication"] == "UNCHANGED"
    assert app.prepare_snapshot.call_args.kwargs["materialize_feature_source"] is False
    app.store.complete_lease.assert_called_once()


def test_base_job_missing_window_does_not_start_slow_sync(tmp_path):
    app, _, _, _ = fixture(tmp_path)
    with pytest.raises(WorkflowError, match="START_WINDOW_MISSED"):
        run_auction_base(app, now=NOW)
    app.prepare_snapshot.assert_not_called()


def test_base_sigterm_flushes_failure_and_releases_lease(tmp_path, monkeypatch):
    import signal
    import liangjian_funnel.workflow as workflow
    app, _, _, _ = fixture(tmp_path)
    monkeypatch.setattr(workflow, "_active_a1_downstream_scope", lambda _: ("600000.SH",))
    app.prepare_snapshot.side_effect = lambda **_: signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
    old = signal.getsignal(signal.SIGTERM)
    with pytest.raises(WorkflowError, match="PROCESS_TERMINATED"):
        run_auction_base(app, now=NOW.replace(hour=7, minute=0))
    result = json.loads(marker_path(app, NOW.date()).read_text())
    assert result["status"] == "BLOCKED"
    assert signal.getsignal(signal.SIGTERM) == old
    app.store.release_lease.assert_called()
