from __future__ import annotations

from liangjian_funnel.evaluation.replay_window import layer_attribution


def _row(stage: str, decision: str, excess: float | None, day: str = "2026-08-28") -> dict:
    return {
        "run_id": "run-1",
        "lane_id": "lane-1",
        "trade_date": day,
        "stage": stage,
        "symbol": f"{stage}-{decision}-{abs(int((excess or 0) * 1000))}-{day}",
        "decision": decision,
        "excess_return_5d": excess,
    }


def test_layer_attribution_reports_gain_loss_pass_rate_and_deterministic_ci() -> None:
    labels = [
        _row("G0", "PASSED", 0.10),
        _row("G0", "REJECTED", -0.10),
        _row("A1", "PASSED", 0.30),
        _row("A1", "REJECTED", -0.20),
        _row("A1", "NOT_SENT_TO_LLM", None),
        _row("A2", "PASSED", 0.40),
        _row("A2", "REJECTED", -0.10),
        _row("A3", "PASSED", 0.50),
        _row("A3", "REJECTED", -0.05),
        _row("A4", "PASSED", 0.60),
        _row("A4", "REJECTED", -0.02),
        _row("G0", "PASSED", 0.20, "2026-08-29"),
        _row("G0", "REJECTED", -0.05, "2026-08-29"),
    ]
    runs = [
        {"trade_date": "2026-08-28", "stages": {"A3": {"selected_count": 2}}},
        {"trade_date": "2026-08-29", "stages": {"A3": {"selected_count": 4}}},
    ]
    first = layer_attribution(runs, labels, bootstrap_samples=60, minimum_sample=2)
    second = layer_attribution(runs, labels, bootstrap_samples=60, minimum_sample=2)
    assert first == second
    assert first["status"] == "READY"
    assert first["A1"]["pass_rate"] == 1 / 3
    assert first["A1"]["loss"] == -0.2
    assert first["A1"]["gain"] is not None
    assert first["A1"]["pass_rate_ci"]["observations"] == 3
    assert first["core_count_distribution"]["status"] == "READY"
    assert first["core_count_distribution"]["counts_by_trade_date"] == {
        "2026-08-28": 2,
        "2026-08-29": 4,
    }


def test_layer_attribution_distinguishes_insufficient_sample_from_zero_opportunity() -> None:
    report = layer_attribution(
        (),
        [_row("A2", "REJECTED", None)],
        bootstrap_samples=20,
        minimum_sample=2,
    )
    assert report["status"] == "INSUFFICIENT_SAMPLE"
    assert "A2" in report["insufficient_layers"]
    assert report["A2"]["pass_rate"] is None
    assert report["A2"]["loss"] is None
    assert report["A2"]["gain"] is None
    assert report["A2"]["reason_code"] == "PREVIOUS_OR_PASSED_EXCESS_INSUFFICIENT"
