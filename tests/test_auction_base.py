import copy
from dataclasses import replace
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


def test_explicit_g0_exclusion_is_not_a_missing_baseline(tmp_path):
    app, base, generation, marker = fixture(tmp_path)
    scope = ("600000.SH", "600001.SH")
    marker['scope_symbols'] = list(scope)
    marker_path(app, NOW.date()).write_text(json.dumps(marker), encoding='utf-8')
    candidates = [
        {'symbol': '600001.SH', 'research_eligible': False,
         'exclusion_reasons': ['MINIMUM_TURNOVER_NOT_MET']}]
    base = replace(base, snapshot=replace(base.snapshot, data={
        **base.snapshot.data, 'universe_candidates': candidates}))
    app._load_research_snapshot_by_id.return_value = base
    assert load_auction_base(app, current=NOW, generation=generation, scope=scope) is base
    invalid = replace(base, snapshot=replace(base.snapshot, data={
        **base.snapshot.data, 'universe_candidates': [
            {'symbol': '600001.SH', 'research_eligible': False, 'exclusion_reasons': []}]}))
    app._load_research_snapshot_by_id.return_value = invalid
    with pytest.raises(WorkflowError, match='AUCTION_BASE_A1_COVERAGE_INCOMPLETE'):
        load_auction_base(app, current=NOW, generation=generation, scope=scope)


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


def test_unavailable_optional_hot_entry_does_not_block_healthy_auction_delta(tmp_path):
    _, base, _, _ = fixture(tmp_path)
    values = deltas()
    original = {"available": False, "trade_date": "2026-09-23", "record_count": 1,
                "records": [{"symbol": "600001.SH", "rank": 1}],
                "reason_code": "HOT100_TRANSPORT_FAILED", "source_attempts": [{"status": "HTTP_502"}]}
    values["hot"] = copy.deepcopy(original)
    result = project_auction_delta(base, **values, observed_at=NOW)
    assert values["hot"] == original
    projected = result["EASTMONEY_HOT100_SNAPSHOT"]
    assert projected["available"] is False and projected["records"] == []
    assert projected["record_count"] == 0
    assert projected["trade_date"] == original["trade_date"]
    assert projected["reason_code"] == original["reason_code"]
    assert projected["source_attempts"] == original["source_attempts"]
    context = result["AUCTION_REFRESH_CONTEXT"]
    assert context["hot100_observation"]["status"] == "OPTIONAL_SOURCE_UNAVAILABLE"
    assert context["hot100_observation"]["absence_is_not_popularity_evidence"] is True
    assert len(context["hot100_observation"]["original_object_sha256"]) == 64
    assert context["new_hot_symbols_deferred_to_full_research"] == []
    assert result["g0_symbols"] == base.snapshot.data["g0_symbols"]
    assert result["RISK_EVENTS"] == base.snapshot.data["RISK_EVENTS"]
    assert context["execution_publication"] == "UNCHANGED"


@pytest.mark.parametrize("fault", ["partial", "duplicate_rank", "malformed_row", "unspecified_available"])
def test_optional_hot_contract_does_not_accept_false_success(tmp_path, fault):
    _, base, _, _ = fixture(tmp_path)
    values = deltas()
    if fault == "partial": values["hot"]["records"].pop()
    elif fault == "duplicate_rank": values["hot"]["records"][1]["rank"] = 1
    elif fault == "malformed_row": values["hot"]["records"][1] = "malformed"
    else: values["hot"].pop("available")
    with pytest.raises(WorkflowError, match="AUCTION_HOT100_UNAVAILABLE"):
        project_auction_delta(base, **values, observed_at=NOW)


@pytest.mark.parametrize("field", ["boards", "quotes"])
def test_missing_required_delta_still_blocks_with_optional_hot_failure(tmp_path, field):
    _, base, _, _ = fixture(tmp_path)
    values = deltas()
    values["hot"] = {"available": False, "reason_code": "HTTP_502"}
    values[field]["available"] = False
    expected = "AUCTION_CURRENT_ROTATION_UNAVAILABLE" if field == "boards" else "AUCTION_QUOTE_COVERAGE_INCOMPLETE"
    with pytest.raises(WorkflowError, match=expected):
        project_auction_delta(base, **values, observed_at=NOW)


@pytest.mark.parametrize("seconds", [0, 181])
def test_prepare_optional_hot_failure_keeps_required_collectors_and_deadline(tmp_path, monkeypatch, seconds):
    import liangjian_funnel.runtime.auction_base as module
    import liangjian_funnel.runtime.auction_refresh as refresh
    import liangjian_funnel.data.hithink_board_reference as references
    import liangjian_funnel.workflow as workflow
    app, base, generation, _ = fixture(tmp_path)
    app.settings = SimpleNamespace(workflow_output_dir=tmp_path, fact_store_dir=tmp_path / "facts",
        snapshot_dir=tmp_path / "deltas", source_config_path=tmp_path / "sources.yaml",
        rotation_theme_registry_path=tmp_path / "registry.yaml", rotation_membership_refresh_days=7,
        rotation_membership_warn_age_days=7, rotation_membership_max_age_days=14,
        rotation_fund_coverage_minimum=.8, rotation_price_coverage_minimum=.95, rotation_collection_workers=16)
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None): return NOW + timedelta(seconds=seconds)
    monkeypatch.setattr(module, "datetime", Clock)
    hot = Mock(return_value={"available": False, "reason_code": "HTTP_502", "records": []})
    boards = Mock(return_value=deltas()["boards"])
    quotes = Mock(return_value=deltas()["quotes"])
    monkeypatch.setattr(workflow, "collect_eastmoney_hot100", hot)
    monkeypatch.setattr(workflow, "collect_rotation_theme_snapshot", boards)
    monkeypatch.setattr(workflow, "load_yaml", lambda _: {})
    monkeypatch.setattr(references, "configured_rotation_memberships", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(references, "rotation_snapshot_directory", lambda _: tmp_path / "rotation")
    monkeypatch.setattr(refresh, "collect_fresh_quotes", quotes)
    if seconds:
        with pytest.raises(WorkflowError, match="AUCTION_DELTA_CAPTURE_DEADLINE_EXCEEDED"):
            prepare_auction_delta(app, current=NOW, generation=generation, scope=("600000.SH",))
        assert not app.settings.snapshot_dir.exists()
    else:
        prepared = prepare_auction_delta(app, current=NOW, generation=generation, scope=("600000.SH",))
        assert prepared.snapshot.data["EASTMONEY_HOT100_SNAPSHOT"]["available"] is False
        assert prepared.path.is_file()
        assert prepared.snapshot.data["RISK_EVENTS"] == base.snapshot.data["RISK_EVENTS"]
    boards.assert_called_once()
    quotes.assert_called_once_with(("600000.SH",), as_of=NOW)
    app.prepare_snapshot.assert_not_called()


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
    assert app.prepare_snapshot.call_args.kwargs['progress'].path.name.endswith('-progress.json')
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
