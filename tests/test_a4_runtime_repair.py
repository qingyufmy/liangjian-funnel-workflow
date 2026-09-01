from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from liangjian_funnel.data.tencent_minute import MarketQuote, QuoteResult
from liangjian_funnel.pipeline.research import LaneResult, ResearchRunResult
from liangjian_funnel.runtime.state import PlanStatus, RuntimeStore
from liangjian_funnel.workflow import WorkflowApplication, _a4_price_contract_valid, _a4_required_bars
import liangjian_funnel.cli as cli


TZ = ZoneInfo("Asia/Shanghai")


class _Quotes:
    def __init__(self, prices: dict[str, float]):
        self.prices = prices

    def fetch_quote(self, symbol: str, *, as_of: datetime) -> QuoteResult:
        price = self.prices[symbol]
        return QuoteResult(
            symbol=symbol,
            reason_code="OK",
            complete=True,
            quote=MarketQuote(
                symbol=symbol,
                name="测试股票",
                quote_time=as_of,
                price=price,
                open=price,
                previous_close=price,
                volume=1000,
                amount=price * 1000,
            ),
        )


def test_live_bar_requirement_only_counts_closed_current_session_bars():
    assert _a4_required_bars(datetime(2026, 9, 1, 9, 30, tzinfo=TZ), "1m") == 0
    assert _a4_required_bars(datetime(2026, 9, 1, 9, 31, tzinfo=TZ), "1m") == 1
    assert _a4_required_bars(datetime(2026, 9, 1, 9, 34, tzinfo=TZ), "1m") == 4
    assert _a4_required_bars(datetime(2026, 9, 1, 9, 35, tzinfo=TZ), "5m") == 1
    assert _a4_required_bars(datetime(2026, 9, 1, 13, 0, tzinfo=TZ), "1m") == 120
    assert _a4_required_bars(datetime(2026, 9, 1, 15, 0, tzinfo=TZ), "1m") == 240


def test_a4_activation_rejects_non_finite_or_inconsistent_price_contract():
    assert not _a4_price_contract_valid(float("nan"), 9, 11, 11.5)
    assert not _a4_price_contract_valid(float("inf"), 9, 11, 11.5)
    assert not _a4_price_contract_valid(10, 0, 11, 11.5)
    assert not _a4_price_contract_valid(10, 12, 11, 11.5)
    assert not _a4_price_contract_valid(10, 9, 11, 10)
    assert _a4_price_contract_valid(10, 9, 11, 11.5)


def test_explicit_latest_a3_activation_keeps_valid_subset_and_is_idempotent(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    source = "a3-close-2026-08-28"
    expiry = datetime(2026, 9, 2, 15, 0, tzinfo=TZ)
    payload = {"source_run_id": source, "trigger_high": 11, "stop_level": 9}
    store.publish_plan_batch(
        [
            {
                "plan_id": "valid",
                "lane_id": "lane_1",
                "symbol": "600519.SH",
                "status": PlanStatus.PENDING_MORNING_REVIEW.value,
                "expires_at": expiry,
                "payload": payload,
            },
            {
                "plan_id": "invalid",
                "lane_id": "lane_1",
                "symbol": "000001.SZ",
                "status": PlanStatus.PENDING_MORNING_REVIEW.value,
                "expires_at": expiry,
                "payload": payload,
            },
        ]
    )
    app = SimpleNamespace(
        store=store,
        brokers={"lane_1": object()},
        market_data=_Quotes({"600519.SH": 10.5, "000001.SZ": 8.5}),
    )
    now = datetime(2026, 9, 1, 14, 0, tzinfo=TZ)
    first = WorkflowApplication.activate_latest_a3_for_a4(app, now=now)
    assert first["status"] == "READY"
    assert first["activated"] == ["valid"]
    assert first["invalidated"] == ["invalid"]
    assert store.get_execution_plan("valid")["status"] == PlanStatus.ACTIVE_TODAY.value
    assert store.get_execution_plan("valid")["valid_from"] == now.isoformat()
    assert store.get_execution_plan("valid")["expires_at"] == datetime(2026, 9, 1, 15, 0, tzinfo=TZ).isoformat()
    assert store.get_execution_plan("invalid")["status"] == PlanStatus.INVALIDATED.value

    second = WorkflowApplication.activate_latest_a3_for_a4(app, now=now)
    assert second["reason_code"] == "NO_LATEST_A3_PENDING_PLAN"
    assert second["activated"] == []


def test_explicit_latest_a3_activation_fails_closed_outside_session(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    app = SimpleNamespace(store=store, brokers={"lane_1": object()}, market_data=None)
    result = WorkflowApplication.activate_latest_a3_for_a4(
        app,
        now=datetime(2026, 9, 1, 15, 1, tzinfo=TZ),
    )
    assert result["status"] == "BLOCKED"
    assert result["reason_code"] == "A3_A4_ACTIVATION_WINDOW_CLOSED"


def test_explicit_latest_a3_activation_has_safe_cli_entrypoint(monkeypatch, capsys):
    class _Application:
        def __init__(self, _settings):
            pass

        def activate_latest_a3_for_a4(self, *, now):
            assert now == datetime(2026, 9, 1, 14, 0, tzinfo=TZ)
            return {"status": "READY", "reason_code": "NO_LATEST_A3_PENDING_PLAN"}

    monkeypatch.setattr(cli, "WorkflowApplication", _Application)
    settings = SimpleNamespace(timezone="Asia/Shanghai")
    assert cli.main(
        ["activate-latest-a3-for-a4", "--as-of", "2026-09-01T14:00:00+08:00"],
        settings=settings,
    ) == 0
    assert "NO_LATEST_A3_PENDING_PLAN" in capsys.readouterr().out


def test_close_publication_does_not_expire_existing_active_today_plan(tmp_path):
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    current = datetime(2026, 9, 1, 10, 12, tzinfo=TZ)
    store.create_execution_plan(
        "active-plan",
        "lane_1",
        "600519.SH",
        status=PlanStatus.ACTIVE_TODAY,
        expires_at=datetime(2026, 9, 1, 15, 0, tzinfo=TZ),
        payload={"source_run_id": "older", "stop_level": 9},
    )
    result = ResearchRunResult(
        run_id="new-close",
        generated_at=current,
        snapshot_id="snapshot",
        snapshot_hash="a" * 64,
        status="READY",
        lanes=(
            LaneResult(
                "lane_1",
                "model",
                "READY",
                (),
                {
                    "core_watch_pool": [
                        {
                            "plan_id": "new-plan",
                            "symbol": "000001.SZ",
                            "risk_unit": "STANDARD",
                            "strategy_profile": "MA520_SWING",
                            "eligibility": "QUALIFIED",
                            "review_status": "PASS",
                            "trigger_zone": {"low": 10, "high": 11},
                            "invalidation_level": 9,
                        }
                    ],
                    "secondary_watch_pool": [],
                },
            ),
        ),
        audit_paths=(),
        markdown_path=None,
    )
    WorkflowApplication._publish_plans(
        SimpleNamespace(store=store),
        result,
        "close",
        current,
        snapshot_data={"TRADABILITY_FLAGS": {"000001.SZ": {"tradable": True}}},
    )
    assert store.get_execution_plan("active-plan")["status"] == PlanStatus.ACTIVE_TODAY.value

    # Only the formal 15:10 close is allowed to retire the old session.
    WorkflowApplication._publish_plans(
        SimpleNamespace(store=store),
        result,
        "close",
        datetime(2026, 9, 1, 15, 10, tzinfo=TZ),
        snapshot_data={"TRADABILITY_FLAGS": {"000001.SZ": {"tradable": True}}},
    )
    assert store.get_execution_plan("active-plan")["status"] == PlanStatus.EXPIRED.value
