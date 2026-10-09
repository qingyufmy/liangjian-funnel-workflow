"""Pure fixture tests; these do not measure the host or a natural run."""
from __future__ import annotations

import ast
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import pytest

from liangjian_funnel.runtime.resource_evidence import (
    MetricObservation, ResourceObservation, RunResourceWindow,
    RunResourceEvidenceBuilder, evidence_hash, validate_run_resource_evidence,
)

T = datetime(2026, 10, 10, tzinfo=timezone.utc)
M = 1024 * 1024


def metrics(rss=100, peak=900, swap=50, vmswap=4):
    values = {
        "RSS_CURRENT": (rss, "MiB"), "PROCESS_RSS_LIFETIME_PEAK": (peak, "MiB"),
        "SYSTEM_SWAP_USED": (swap, "MiB"), "PROCESS_VMSWAP": (vmswap, "MiB"),
        "PAGECACHE_CACHED": (200, "MiB"), "PAGECACHE_DIRTY": (2, "MiB"),
        "PAGECACHE_WRITEBACK": (1, "MiB"), "PSI_MEMORY_SOME_TOTAL_US": (30, "us"),
        "PSI_MEMORY_FULL_TOTAL_US": (10, "us"), "CGROUP_MEMORY_CURRENT": (300, "MiB"),
        "CGROUP_MEMORY_EVENTS_OOM": (0, "count"),
        "CGROUP_MEMORY_EVENTS_OOM_KILL": (0, "count"),
    }
    return tuple(MetricObservation(k, v, u, "AVAILABLE", "FIXTURE") for k, (v, u) in values.items())


def window(**kwargs):
    return replace(RunResourceWindow("run-1", "invocation-1", 123, T-timedelta(hours=1),
        "host-1", T, T+timedelta(seconds=20), "SUCCEEDED",
        ended_at=T+timedelta(seconds=20), cgroup_id="cgroup-1"), **kwargs)


def sample(seconds, **kwargs):
    return ResourceObservation("run-1", "invocation-1", 123, T-timedelta(hours=1),
        "host-1", T+timedelta(seconds=seconds), metrics(**kwargs), cgroup_id="cgroup-1")


def fixture():
    return window(), (sample(0), sample(10, rss=120), sample(20, rss=110, peak=950, swap=40, vmswap=6))


def build(w=None, samples=None):
    base, obs = fixture()
    return RunResourceEvidenceBuilder().build(w or base, obs if samples is None else samples)


def test_sampled_peak_not_lifetime_or_delta_and_system_swap_can_decrease():
    receipt = build()
    assert receipt["evidence_status"] == "LOCAL_OBSERVATION_BOUND"
    assert receipt["sampled_rss_peak_lower_bound_bytes"] == 120*M
    assert receipt["process_lifetime_peak_reported_max_bytes"] == 950*M
    assert receipt["lifetime_peak_is_run_peak"] is False
    assert receipt["system_swap"]["end_minus_start_bytes"] == -10*M
    assert receipt["system_swap"]["attributable_to_this_run"] is False
    assert receipt["process_vmswap"]["end_bytes"] == 6*M
    assert receipt["eligibility_released"] is False
    assert receipt["acquisition_authenticated"] is False
    assert receipt["run_execution_authenticated"] is False
    assert json.loads(json.dumps(receipt, allow_nan=False)) == receipt
    assert validate_run_resource_evidence(receipt, *fixture())["valid"] is True


@pytest.mark.parametrize("field,value", [
    ("run_id", "other-run"), ("invocation_id", "reentry"), ("pid", 124),
    ("process_started_at", T-timedelta(minutes=1)), ("host_id", "other-host"),
    ("cgroup_id", "other-cgroup"),
])
def test_cross_identity_rejected(field, value):
    with pytest.raises(ValueError):
        build(samples=(sample(0), replace(sample(20), **{field: value})))


@pytest.mark.parametrize("seconds", [-1, 21])
def test_samples_outside_window_rejected(seconds):
    with pytest.raises(ValueError):
        build(samples=(sample(seconds),))


@pytest.mark.parametrize("samples", [(sample(20), sample(0)), (sample(0), sample(0))])
def test_disordered_or_duplicate_sample_not_sorted_away(samples):
    with pytest.raises(ValueError):
        build(samples=samples)


@pytest.mark.parametrize("field", ["started_at", "observed_until", "ended_at", "process_started_at"])
def test_naive_window_time_rejected(field):
    with pytest.raises(ValueError):
        window(**{field: T.replace(tzinfo=None)})


def test_naive_observation_time_rejected():
    with pytest.raises(ValueError):
        replace(sample(0), observed_at=T.replace(tzinfo=None))


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -1, True, "1", 2**54])
def test_invalid_numeric_values_rejected(value):
    with pytest.raises(ValueError):
        MetricObservation("RSS_CURRENT", value, "bytes", "AVAILABLE", "FIXTURE")


@pytest.mark.parametrize("unit", ["MB", "pages", "count", "us", None])
def test_memory_wrong_unit_rejected(unit):
    with pytest.raises(ValueError):
        MetricObservation("RSS_CURRENT", 1, unit, "AVAILABLE", "FIXTURE")


@pytest.mark.parametrize("source,unit", [("GETRUSAGE_LINUX", "bytes"), ("GETRUSAGE_DARWIN", "KiB")])
def test_peak_platform_units_not_guessed(source, unit):
    with pytest.raises(ValueError):
        MetricObservation("PROCESS_RSS_LIFETIME_PEAK", 100, unit, "AVAILABLE", source)


def test_explicit_kib_and_bytes_same_normalized_value():
    w, obs = fixture()
    baseline = build()
    changed = tuple(replace(o, metrics=tuple(replace(m, value=m.value*1024, unit="KiB")
        if m.unit == "MiB" else m for m in o.metrics)) for o in obs)
    converted = build(w, changed)
    assert converted["sampled_rss_peak_lower_bound_bytes"] == baseline["sampled_rss_peak_lower_bound_bytes"]
    assert converted["evidence_hash"] != baseline["evidence_hash"]  # Raw units remain bound.


@pytest.mark.parametrize("support", ["UNSUPPORTED", "UNAVAILABLE", "ERROR"])
def test_unsupported_value_cannot_be_zero(support):
    with pytest.raises(ValueError):
        MetricObservation("RSS_CURRENT", 0, None, support, "NONE")
    unavailable = MetricObservation("RSS_CURRENT", None, None, support, "NONE")
    obs = tuple(replace(o, metrics=tuple(unavailable if m.metric == "RSS_CURRENT" else m
        for m in o.metrics)) for o in fixture()[1])
    receipt = build(samples=obs)
    assert receipt["sampled_rss_peak_lower_bound_bytes"] is None
    assert receipt["evidence_status"] == "DATA_LIMITED"
    assert receipt["samples"][0]["metrics"]["RSS_CURRENT"]["support"] == support


def test_missing_pressure_not_filled_and_marked_gap():
    obs = tuple(replace(o, metrics=tuple(m for m in o.metrics if m.metric in {
        "RSS_CURRENT", "PROCESS_RSS_LIFETIME_PEAK", "SYSTEM_SWAP_USED", "PROCESS_VMSWAP"}))
        for o in fixture()[1])
    receipt = build(samples=obs)
    assert receipt["evidence_status"] == "DATA_LIMITED"
    assert "METRIC_MISSING:0:PSI_MEMORY_SOME_TOTAL_US" in receipt["gap_codes"]
    assert "PSI_MEMORY_SOME_TOTAL_US" not in receipt["samples"][0]["metrics"]


def test_completed_claim_without_end_sample_not_complete():
    receipt = build(samples=(sample(0), sample(10)))
    assert receipt["declared_status"] == "SUCCEEDED"
    assert receipt["window_coverage_complete"] is False
    assert receipt["system_swap"]["end_bytes"] is None
    assert receipt["system_swap"]["end_minus_start_bytes"] is None
    assert "END_SAMPLE_MISSING" in receipt["gap_codes"]


def test_no_start_sample_does_not_invent_swap_baseline():
    receipt = build(samples=(sample(10), sample(20)))
    assert receipt["system_swap"]["start_bytes"] is None
    assert receipt["system_swap"]["end_minus_start_bytes"] is None
    assert "START_SAMPLE_MISSING" in receipt["gap_codes"]


def test_running_only_last_observation_survives_without_end():
    receipt = build(window(status="RUNNING", ended_at=None), (sample(0), sample(10)))
    assert receipt["observation_status"] == "RUNNING"
    assert receipt["run_completed"] is False
    assert receipt["last_observed_at"] == sample(10).observed_at.isoformat()
    assert receipt["system_swap"]["end_bytes"] is None
    assert "RUN_NOT_FINISHED" in receipt["gap_codes"]


@pytest.mark.parametrize("termination", ["SIGKILL", "SIGTERM", "UNKNOWN_TERMINATION"])
def test_killed_running_process_never_becomes_completed(termination):
    receipt = build(window(status="TERMINATED", ended_at=None, termination=termination), (sample(0), sample(10)))
    assert receipt["observation_status"] == "INTERRUPTED"
    assert receipt["run_completed"] is False
    assert receipt["system_swap"]["end_minus_start_bytes"] is None
    assert receipt["gap_codes"]


def test_failed_window_not_completed_even_with_end_sample():
    receipt = build(window(status="FAILED", termination="EXIT_FAILURE"))
    assert receipt["run_completed"] is False
    assert receipt["observation_status"] == "FAILED"


def test_pid_reuse_and_reentry_require_separate_receipts():
    w, obs = fixture()
    new = replace(w, invocation_id="invocation-2", process_started_at=T)
    with pytest.raises(ValueError):
        build(new, obs)
    adapted = tuple(replace(o, invocation_id=new.invocation_id,
        process_started_at=new.process_started_at) for o in obs)
    receipt = build(new, adapted)
    assert receipt["evidence_hash"] != build()["evidence_hash"]


def test_lifetime_peak_cannot_be_lower_than_rss_or_decrease():
    with pytest.raises(ValueError):
        build(samples=(sample(0, peak=50), sample(20)))
    with pytest.raises(ValueError):
        build(samples=(sample(0, peak=950), sample(20, peak=900)))


def test_source_switch_for_same_metric_rejected():
    last = sample(20)
    changed = replace(last, metrics=tuple(replace(m, source="LINUX_PROC_STATUS", unit="KiB", value=m.value*1024)
        if m.metric == "RSS_CURRENT" else m for m in last.metrics))
    with pytest.raises(ValueError):
        build(samples=(sample(0), changed))


@pytest.mark.parametrize("metric,source", [("UNKNOWN_METRIC", "FIXTURE"), ("RSS_CURRENT", "some-key-text"),
    ("SYSTEM_SWAP_USED", "LINUX_PROC_STATM")])
def test_unknown_or_wrong_metric_source_rejected(metric, source):
    with pytest.raises(ValueError):
        MetricObservation(metric, 1, "bytes", "AVAILABLE", source)


def test_fractional_counter_and_wrong_psi_unit_rejected():
    with pytest.raises(ValueError):
        MetricObservation("CGROUP_MEMORY_EVENTS_OOM", 0.5, "count", "AVAILABLE", "FIXTURE")
    with pytest.raises(ValueError):
        MetricObservation("PSI_MEMORY_FULL_TOTAL_US", 1, "percent", "AVAILABLE", "FIXTURE")


def test_cgroup_metrics_need_explicit_cgroup_identity():
    with pytest.raises(ValueError):
        build(window(cgroup_id=None), tuple(replace(o, cgroup_id=None) for o in fixture()[1]))


def test_duplicate_metrics_rejected():
    first = sample(0)
    with pytest.raises(ValueError):
        replace(first, metrics=(*first.metrics, first.metrics[0]))


@pytest.mark.parametrize("kwargs", [{"status": "RUNNING"}, {"ended_at": None},
    {"termination": "SIGKILL"}, {"process_started_at": T+timedelta(seconds=1)},
    {"observed_until": T-timedelta(seconds=1)}, {"pid": 0}, {"pid": True}])
def test_invalid_window_rejected(kwargs):
    with pytest.raises(ValueError):
        window(**kwargs)


def test_original_inputs_unchanged_and_list_captured_normally():
    w, obs = fixture()
    snapshot = deepcopy((w, obs))
    receipt = build(w, list(obs))
    assert (w, obs) == snapshot
    assert receipt["sample_count"] == 3


def test_rehashing_tampered_receipt_cannot_validate_against_original_inputs():
    receipt = build()
    receipt["system_swap"]["attributable_to_this_run"] = True
    receipt["evidence_hash"] = evidence_hash({k: v for k, v in receipt.items() if k != "evidence_hash"})
    assert validate_run_resource_evidence(receipt, *fixture())["valid"] is False


def test_wrong_hash_and_wrong_original_window_rejected_by_validator():
    receipt = build()
    receipt["evidence_hash"] = "0"*64
    assert validate_run_resource_evidence(receipt, *fixture())["valid"] is False
    assert validate_run_resource_evidence(build(), window(run_id="other"), fixture()[1])["valid"] is False


def test_empty_samples_explicit_gap_and_no_measurements():
    receipt = build(samples=())
    assert receipt["sampled_rss_peak_lower_bound_bytes"] is None
    assert receipt["last_observed_at"] is None
    assert "NO_SAMPLES" in receipt["gap_codes"]


def test_module_has_no_io_or_application_dependencies():
    from liangjian_funnel.runtime import resource_evidence
    source = Path(resource_evidence.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    modules = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
    modules |= {alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names}
    assert not any(name and any(part in name for part in ("resource_guard", "settings", "state", "workflow", "http", "os", "pathlib", "psutil")) for name in modules)
    assert not any(isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "open" for node in ast.walk(tree))


def test_huge_integer_rejected_with_contract_error_not_conversion_overflow():
    with pytest.raises(ValueError):
        MetricObservation("RSS_CURRENT", 2**10000, "bytes", "AVAILABLE", "FIXTURE")


@pytest.mark.parametrize("metric,source,unit", [
    ("RSS_CURRENT", "LINUX_PROC_STATUS", "MiB"),
    ("PROCESS_VMSWAP", "LINUX_PROC_STATUS", "bytes"),
    ("SYSTEM_SWAP_USED", "LINUX_PROC_MEMINFO", "bytes"),
])
def test_original_proc_kib_unit_is_explicit_not_inferred(metric, source, unit):
    with pytest.raises(ValueError):
        MetricObservation(metric, 1, unit, "AVAILABLE", source)


@pytest.mark.parametrize("metric,source,unit,value,expected", [
    ("RSS_CURRENT", "LINUX_PROC_STATM", "bytes", 4096, 4096),
    ("PROCESS_VMSWAP", "LINUX_PROC_STATUS", "KiB", 4, 4096),
    ("SYSTEM_SWAP_USED", "LINUX_PROC_MEMINFO", "KiB", 4, 4096),
    ("PROCESS_RSS_LIFETIME_PEAK", "GETRUSAGE_LINUX", "KiB", 4, 4096),
    ("PROCESS_RSS_LIFETIME_PEAK", "GETRUSAGE_DARWIN", "bytes", 4096, 4096),
])
def test_explicit_source_unit_conversion(metric, source, unit, value, expected):
    observation = MetricObservation(metric, value, unit, "AVAILABLE", source)
    assert observation.normalized_value() == expected


def test_real_supported_zero_differs_from_unsupported_null():
    assert MetricObservation("SYSTEM_SWAP_USED", 0, "KiB", "AVAILABLE", "LINUX_PROC_MEMINFO").normalized_value() == 0
    assert MetricObservation("SYSTEM_SWAP_USED", None, None, "UNSUPPORTED", "NONE").normalized_value() is None


def test_pressure_counter_reset_cannot_silently_bind_same_host_epoch():
    last = sample(20)
    changed = replace(last, metrics=tuple(replace(m, value=1)
        if m.metric == "PSI_MEMORY_SOME_TOTAL_US" else m for m in last.metrics))
    with pytest.raises(ValueError):
        build(samples=(sample(0), changed))


def test_terminal_window_rejects_later_sample_even_if_observed_until_later():
    w = window(observed_until=T+timedelta(seconds=30))
    with pytest.raises(ValueError):
        build(w, (sample(0), sample(21)))


def test_hash_self_consistent_data_limited_receipt_can_validate_but_not_complete():
    w, obs = fixture()
    limited = build(w, obs[:2])
    checked = validate_run_resource_evidence(limited, w, obs[:2])
    assert checked["valid"] is True
    assert checked["eligibility_released"] is False
    assert limited["window_coverage_complete"] is False


def test_success_declaration_with_missing_end_is_not_observed_success_status():
    receipt = build(samples=(sample(0), sample(10)))
    assert receipt["declared_status"] == "SUCCEEDED"
    assert receipt["observation_status"] == "INCOMPLETE_WINDOW"
    assert receipt["run_completed"] is False
