from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.runtime.auction_refresh import collect_fresh_quotes, run_auction_refresh
from liangjian_funnel.workflow import WorkflowApplication, WorkflowError

TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 9, 14, 9, 26, tzinfo=TZ)


def test_quotes_have_real_timestamps_and_do_not_claim_auction_final():
    quote = {"quote_time": NOW - timedelta(minutes=1), "latest_price": 10}
    result = collect_fresh_quotes(["600000.SH"], as_of=NOW,
                                 fetch=lambda _: {"600000.SH": quote}, clock=lambda: NOW)
    assert result["coverage"] == 1
    assert result["auction_final_price_claimed"] is False
    assert result["by_symbol"]["600000.SH"]["quote_time"].endswith("09:25:00+08:00")


@pytest.mark.parametrize("stamp,price", [
    (NOW - timedelta(days=1), 10), (NOW + timedelta(seconds=1), 10),
    (NOW - timedelta(minutes=2), 10), (None, 10), (NOW.replace(tzinfo=None), 10),
    (NOW, 0), (NOW, float("nan")), (NOW, float("inf")),
])
def test_quotes_reject_stale_unfinalized_missing_or_invalid(stamp, price):
    with pytest.raises(WorkflowError, match="AUCTION_QUOTE_COVERAGE_INCOMPLETE"):
        collect_fresh_quotes(["600000.SH"], as_of=NOW,
                             fetch=lambda _: {"600000.SH": {"quote_time": stamp, "latest_price": price}},
                             clock=lambda: NOW)


def _app(tmp_path, result=None):
    return SimpleNamespace(
        settings=SimpleNamespace(workflow_output_dir=tmp_path),
        trading_calendar=SimpleNamespace(is_trading_day=lambda d: d.weekday() < 5),
        store=SimpleNamespace(acquire_lease=Mock(return_value=True), complete_lease=Mock(), release_lease=Mock()),
        run_research=Mock(return_value=result or {"status": "READY", "run_id": "test"}),
    )


def test_refresh_reuses_a1_and_never_publishes_or_resumes_old_snapshot(tmp_path):
    app = _app(tmp_path)
    receipt = run_auction_refresh(app, now=NOW)
    assert receipt["status"] == "READY"
    assert receipt["execution_publication"] == "UNCHANGED"
    kwargs = app.run_research.call_args.kwargs
    assert kwargs["from_active_a1"] and kwargs["primary_only"] and kwargs["auction_refresh"]
    assert not kwargs["publish_plans"] and not kwargs["reuse_resume_snapshot"]
    assert not kwargs["schedule_comparison"]
    app.store.complete_lease.assert_called_once()
    app.store.release_lease.assert_not_called()


@pytest.mark.parametrize("now", [NOW.replace(hour=9, minute=25), NOW.replace(minute=30), NOW.replace(hour=10)])
def test_capture_start_window_is_bounded(tmp_path, now):
    app = _app(tmp_path)
    with pytest.raises(WorkflowError, match="START_WINDOW_MISSED"):
        run_auction_refresh(app, now=now)
    app.run_research.assert_not_called()


def test_weekend_and_duplicate_do_not_run_research(tmp_path):
    app = _app(tmp_path)
    assert run_auction_refresh(app, now=NOW.replace(day=12))["reason_code"] == "NON_TRADING_DAY"
    app.store.acquire_lease.return_value = False
    assert run_auction_refresh(app, now=NOW)["reason_code"] == "AUCTION_REFRESH_ALREADY_DISPATCHED"
    app.run_research.assert_not_called()


def test_blocked_result_leaves_failure_receipt_and_releases_lease(tmp_path):
    app = _app(tmp_path, {"status": "BLOCKED"})
    with pytest.raises(WorkflowError, match="RESEARCH_NOT_READY"):
        run_auction_refresh(app, now=NOW)
    assert '"status": "BLOCKED"' in (tmp_path / "runs/2026-09-14-auction-refresh.json").read_text()
    app.store.release_lease.assert_called_once()
    app.store.complete_lease.assert_not_called()


def test_sigterm_seals_receipt_progress_and_releases_lease(tmp_path):
    import signal
    import json
    app = _app(tmp_path)
    path = tmp_path/'auction_progress/2026-09-14-auction-refresh-092600.json'
    path.parent.mkdir()
    path.write_text(json.dumps({'run_id':'2026-09-14-auction-refresh-092600','status':'RUNNING'}))
    old_handler = signal.getsignal(signal.SIGTERM)
    app.run_research.side_effect = lambda *a, **k: signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
    with pytest.raises(WorkflowError, match='PROCESS_TERMINATED'):
        run_auction_refresh(app, now=NOW)
    assert json.loads(path.read_text())['status'] == 'BLOCKED'
    assert json.loads((tmp_path/'runs/2026-09-14-auction-refresh.json').read_text())['status'] == 'BLOCKED'
    assert signal.getsignal(signal.SIGTERM) == old_handler
    app.store.complete_lease.assert_not_called()


def test_failure_receipt_retains_specific_source_and_date(tmp_path):
    import json
    app = _app(tmp_path)
    diagnostics = {"expected_closed_trade_date": "2026-09-11",
                   "facts": {"LIMIT_UP_LADDER": {"reason_code": "MARKET_TRADE_DATE_MISMATCH",
                                                    "observed_latest_market_trade_date": "2026-09-14"}}}
    app.run_research.side_effect = WorkflowError("MARKET_EMOTION_FACTS_NOT_READY", diagnostics=diagnostics)
    with pytest.raises(WorkflowError):
        run_auction_refresh(app, now=NOW)
    receipt = json.loads((tmp_path / "runs/2026-09-14-auction-refresh.json").read_text())
    assert receipt["diagnostics"] == diagnostics
    app.store.release_lease.assert_called_once()


def test_refresh_cannot_be_used_to_bypass_publication_guard():
    with pytest.raises(WorkflowError, match="AUCTION_REFRESH_ARGUMENTS_INVALID"):
        WorkflowApplication.run_research(SimpleNamespace(), "morning", as_of=NOW,
                                         from_active_a1=True, primary_only=True,
                                         auction_refresh=True, publish_plans=True)


def test_manual_rerun_uses_actual_time_and_keeps_morning_receipt(tmp_path):
    app = _app(tmp_path)
    (tmp_path / "runs").mkdir()
    original = tmp_path / "runs/2026-09-14-auction-refresh.json"
    original.write_text('{"status":"BLOCKED"}')
    current = NOW.replace(hour=11, minute=20)
    receipt = run_auction_refresh(app, now=current, manual_current=True)
    assert receipt["research_mode"] == "MANUAL_CURRENT_SESSION"
    assert app.run_research.call_args.kwargs["as_of"] == current
    assert not app.run_research.call_args.kwargs["publish_plans"]
    assert original.read_text() == '{"status":"BLOCKED"}'
    assert (tmp_path / "runs/2026-09-14-manual-current-refresh-112000.json").exists()


def test_legacy_auction_snapshot_path_cannot_reenter_heavy_sync(tmp_path, monkeypatch):
    import liangjian_funnel.workflow as workflow
    import liangjian_funnel.runtime.auction_refresh as refresh

    current = datetime.now(TZ)
    settings = SimpleNamespace(source_config_path=tmp_path / "source.yml", fact_store_dir=tmp_path,
        rotation_theme_registry_path=tmp_path, rotation_membership_refresh_days=7,
        rotation_membership_warn_age_days=14, rotation_membership_max_age_days=30,
        rotation_fund_coverage_minimum=.8, rotation_price_coverage_minimum=.8, rotation_collection_workers=4)
    hot, boards = Mock(return_value={"available": True, "records": []}), Mock(return_value={"available": False})
    monkeypatch.setattr(workflow, "load_yaml", lambda _: {})
    monkeypatch.setattr(workflow, "_latest_closed_market_trade_date", lambda *_: current.date() - timedelta(days=1))
    monkeypatch.setattr(workflow, "collect_eastmoney_hot100", hot)
    monkeypatch.setattr(workflow, "collect_rotation_theme_snapshot", boards)
    monkeypatch.setattr(refresh, "collect_fresh_quotes", lambda *_args, **_kwargs: {"available": True})
    app = SimpleNamespace(settings=settings, trading_calendar=object(), lark_publisher=Mock())
    with pytest.raises(WorkflowError, match="AUCTION_FULL_SYNC_FORBIDDEN_USE_VERIFIED_BASE"):
        WorkflowApplication.prepare_snapshot(app, as_of=current, candidate_symbols=("600000.SH",), auction_refresh=True)
    hot.assert_not_called()
    boards.assert_not_called()
    app.lark_publisher.publish_rotation_theme_health.assert_not_called()
