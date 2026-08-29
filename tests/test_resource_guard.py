from pathlib import Path

from liangjian_funnel.runtime.resource_guard import (
    ResourceSnapshot,
    evaluate_resources,
    measure_resources,
)


def _snapshot(**updates):
    values = {
        "rss_current_mb": 100.0,
        "rss_peak_mb": 120.0,
        "system_mem_available_mb": 1024.0,
        "swap_used_mb": 100.0,
        "disk_free_mb": 10_000.0,
        "disk_free_ratio": 0.30,
        "open_file_descriptors": 10,
    }
    values.update(updates)
    return ResourceSnapshot(**values)


def test_resource_gate_allows_healthy_host(tmp_path: Path):
    decision = evaluate_resources(tmp_path, probe=lambda _: _snapshot())
    assert decision.allowed is True
    assert decision.reason_codes == ()


def test_resource_gate_reports_all_direct_pressure_reasons(tmp_path: Path):
    decision = evaluate_resources(
        tmp_path,
        probe=lambda _: _snapshot(
            rss_current_mb=1300,
            system_mem_available_mb=200,
            disk_free_mb=1000,
            disk_free_ratio=0.05,
        ),
    )
    assert decision.allowed is False
    assert decision.reason_codes == (
        "BLOCKED_RESOURCE_MEMORY_LOW",
        "BLOCKED_RESOURCE_PROCESS_RSS_HIGH",
        "BLOCKED_RESOURCE_DISK_LOW",
    )


def test_live_measurement_is_bounded_and_serialisable(tmp_path: Path):
    measured = measure_resources(tmp_path)
    payload = measured.as_dict()
    assert payload["rss_current_mb"] >= 0
    assert payload["rss_peak_mb"] >= 0
    assert 0 <= payload["disk_free_ratio"] <= 1
