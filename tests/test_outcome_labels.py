from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pytest

from liangjian_funnel.cli import main
from liangjian_funnel.evaluation.outcome_labels import (
    BASELINE_INSUFFICIENT,
    backfill_forward_returns,
    conditional_random_baseline,
    record_stage_decisions,
)
from liangjian_funnel.runtime.state import OutcomeLabelConflictError, RuntimeStore
from liangjian_funnel.settings import Settings


def _label(*, run_id: str = "run-1", lane_id: str = "lane-1", symbol: str = "600001.SH") -> dict:
    return {
        "run_id": run_id,
        "lane_id": lane_id,
        "trade_date": "2026-08-28",
        "stage": "A1",
        "symbol": symbol,
        "decision": "PASSED",
        "reason_codes": ["EVIDENCE_OK"],
        "selection_basis": "DETERMINISTIC_SCORE",
        "score": 0.8,
        "snapshot_id": "snapshot-2026-08-28",
        "config_hash": "config-hash-1",
        "metadata": {
            "industry": "TH_AI",
            "market_cap": 100.0,
            "volatility": 0.2,
        },
    }


def _bar(symbol: str, day: date, close: float, *, factor: float = 1.0, high: float | None = None, low: float | None = None) -> dict:
    return {
        "symbol": symbol,
        "trade_date": day.isoformat(),
        "close": close,
        "high": high if high is not None else close + 1.0,
        "low": low if low is not None else close - 1.0,
        "adjust": "raw",
        "adjust_factor": factor,
    }


def test_stage_decisions_are_idempotent_but_same_day_runs_and_lanes_are_distinct(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "state.sqlite3")
    first = record_stage_decisions(
        store,
        trade_date="2026-08-28",
        stage="A1",
        decisions={"600001.SH": {"decision": "PASSED", "selection_basis": "LLM_REVIEWED"}},
        snapshot_id="snap-1",
        config_hash="cfg-1",
        run_id="run-1",
        lane_id="lane-1",
    )
    replay = record_stage_decisions(
        store,
        trade_date="2026-08-28",
        stage="A1",
        decisions={"600001.SH": {"decision": "PASSED", "selection_basis": "LLM_REVIEWED"}},
        snapshot_id="snap-1",
        config_hash="cfg-1",
        run_id="run-1",
        lane_id="lane-1",
    )
    record_stage_decisions(
        store,
        trade_date="2026-08-28",
        stage="A1",
        decisions={"600001.SH": {"decision": "PASSED", "selection_basis": "LLM_REVIEWED"}},
        snapshot_id="snap-1",
        config_hash="cfg-1",
        run_id="run-1",
        lane_id="lane-2",
    )
    record_stage_decisions(
        store,
        trade_date="2026-08-28",
        stage="A1",
        decisions={"600001.SH": {"decision": "PASSED", "selection_basis": "LLM_REVIEWED"}},
        snapshot_id="snap-1",
        config_hash="cfg-1",
        run_id="run-2",
        lane_id="lane-1",
    )

    assert first[0]["label_id"] == replay[0]["label_id"]
    assert store.count_outcome_labels(stage="A1") == 3
    assert {row["lane_id"] for row in store.list_outcome_labels()} == {"lane-1", "lane-2"}
    assert {row["run_id"] for row in store.list_outcome_labels()} == {"run-1", "run-2"}

    conflicting = _label()
    conflicting["snapshot_id"] = "different-snapshot"
    with pytest.raises(OutcomeLabelConflictError) as error:
        store.record_outcome_label(conflicting)
    assert error.value.reason_code == "OUTCOME_LABEL_IMMUTABLE_CONFLICT"


def test_outcome_label_accepts_half_year_fundamental_selection_basis(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "state.sqlite3")
    label = _label()
    label["selection_basis"] = "HALF_YEAR_FUNDAMENTAL"

    stored = store.record_outcome_label(label)

    assert stored["selection_basis"] == "HALF_YEAR_FUNDAMENTAL"


def test_explicit_stage_decision_wins_over_unsent_flag_and_keeps_gate_attribution(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "state.sqlite3")
    rows = record_stage_decisions(
        store,
        trade_date="2026-08-28",
        stage="A2",
        decisions=[
            {
                "symbol": "600001.SH",
                "decision": "REJECTED",
                "sent_to_llm": False,
                "reason_codes": ["A2_CRITICAL_DATA_INSUFFICIENT"],
                "gate_results": {
                    "LOCAL_DATA_SUFFICIENCY": {"passed": False, "available": True},
                    "SENT_TO_LLM": {"passed": False, "available": False},
                },
                "first_blocking_gate": "LOCAL_DATA_SUFFICIENCY",
                "all_failed_gates": ["LOCAL_DATA_SUFFICIENCY"],
            }
        ],
        snapshot_id="snap-1",
        config_hash="cfg-1",
        run_id="run-1",
        lane_id="lane-1",
    )

    assert rows[0]["decision"] == "REJECTED"
    reasons = json.loads(rows[0]["reason_codes"])
    assert reasons["reason_codes"] == ["A2_CRITICAL_DATA_INSUFFICIENT"]
    assert reasons["first_blocking_gate"] == "LOCAL_DATA_SUFFICIENCY"
    assert reasons["gate_results"]["SENT_TO_LLM"]["available"] is False


def test_forward_metrics_use_raw_prices_and_explicit_factor_and_leave_short_windows_null(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "state.sqlite3")
    recorded = store.record_outcome_label(_label())
    start = date(2026, 8, 28)
    prices = [_bar("600001.SH", start + timedelta(days=index), 100.0 + index, factor=2.0) for index in range(6)]
    result = backfill_forward_returns(store, as_of_date="2026-09-02", price_source=prices)
    assert result["labels_updated"] == 1
    row = store.get_outcome_label(recorded["label_id"])
    assert row is not None
    assert row["fwd_return_1d"] == pytest.approx(0.01)
    assert row["fwd_return_3d"] == pytest.approx(0.03)
    assert row["fwd_return_5d"] == pytest.approx(0.05)
    assert row["fwd_return_10d"] is None
    assert row["mfe_5d"] is not None
    assert row["mae_5d"] is not None
    assert row["labeled_at"] is None

    # A source claiming adjusted prices without a factor cannot be silently
    # reinterpreted as raw data.
    bad_source = [dict(prices[0], adjust="qfq", adjust_factor=None)]
    bad = backfill_forward_returns(store, as_of_date="2026-08-28", price_source=bad_source)
    assert "RAW_PLUS_EXPLICIT_FACTOR_REQUIRED" in bad["source_errors"]


def test_conditional_random_baseline_is_deterministic_and_requires_n_peers() -> None:
    target = {
        "symbol": "600001.SH",
        "trade_date": "2026-08-28",
        "industry": "TH_AI",
        "market_cap_quintile": 3,
        "volatility_quintile": 4,
    }
    peers = [
        {
            "symbol": f"600{index:03d}.SH",
            "trade_date": "2026-08-28",
            "industry": "TH_AI",
            "market_cap_quintile": 3,
            "volatility_quintile": 4,
            "fwd_return_5d": index / 1000,
            "tradable": True,
        }
        for index in range(2, 52)
    ]
    first = conditional_random_baseline(target, [target, *peers])
    second = conditional_random_baseline(target, [target, *peers])
    assert first == second
    assert first["status"] == "OK"
    assert first["sample_size"] == 50
    assert first.value is not None

    insufficient = conditional_random_baseline(target, peers[:-1])
    assert insufficient["status"] == BASELINE_INSUFFICIENT
    assert insufficient["reason_code"] == "BASELINE_PEER_COUNT_BELOW_N"
    assert insufficient["sample_size"] == 49


def test_evaluation_cli_is_local_only_and_emits_structured_json(tmp_path: Path, capsys) -> None:
    settings = Settings.from_env({}, root=tmp_path)
    store = RuntimeStore(settings.state_db_path)
    store.record_outcome_label(_label())
    source = tmp_path / "prices.json"
    source.write_text(json.dumps([_bar("600001.SH", date(2026, 8, 28), 100.0), _bar("600001.SH", date(2026, 8, 29), 101.0)]), encoding="utf-8")

    assert main(
        ["label-outcomes", "--as-of", "2026-08-29", "--price-source", str(source)],
        settings=settings,
    ) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["network_used"] is False
    assert payload["models_called"] is False
    assert payload["runtime_mutation"] is True
