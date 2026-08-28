from __future__ import annotations

from liangjian_funnel.workflow import _compact_fundamental_rows


def test_fundamental_projection_is_field_and_period_bounded() -> None:
    rows: list[dict] = []
    for dataset in ("INCOME", "BALANCE", "CASH_FLOW"):
        for period in range(10):
            rows.append(
                {
                    "_dataset": dataset,
                    "fiscal_year": 2026 - period,
                    "report_date_ms": 2_000_000_000_000 - period,
                    "operating_income": period,
                    "assets_total": period,
                    "act_cash_flow_net": period,
                    "provider_wide_blob": "x" * 10_000,
                }
            )
    rows.append(
        {
            "_dataset": "INDICATORS",
            "ability": "growth",
            "index_id": "index_weighted_avg_roe",
            "value": 12.5,
            "provider_wide_blob": "x" * 10_000,
        }
    )
    for index in range(80):
        rows.append(
            {
                "_dataset": "INDICATORS",
                "ability": "other",
                "index_id": f"z_metric_{index:03d}",
                "value": index,
                "provider_wide_blob": "x" * 10_000,
            }
        )

    compact = _compact_fundamental_rows(rows)

    assert all(len(compact["statements"][dataset]) == 4 for dataset in ("INCOME", "BALANCE", "CASH_FLOW"))
    assert len(compact["indicators"]) == 40
    assert compact["indicators"][0]["index_id"] == "index_weighted_avg_roe"
    retained = [
        *compact["statements"]["INCOME"],
        *compact["statements"]["BALANCE"],
        *compact["statements"]["CASH_FLOW"],
        *compact["indicators"],
    ]
    assert all("provider_wide_blob" not in row for row in retained)
    assert compact["dataset_coverage"] == {
        "core_reports_complete": True,
        "indicators_available": True,
        "missing_datasets": [],
    }
