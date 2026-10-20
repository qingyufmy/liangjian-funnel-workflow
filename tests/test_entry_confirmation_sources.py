import hashlib
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import liangjian_funnel.runtime.entry_confirmation_sources as sources

from liangjian_funnel.runtime.entry_confirmation_sources import (
    ENTRY_CONFIRMATION_FACTS_SCHEMA,
    build_entry_confirmation_daily_facts,
    project_entry_confirmation_daily_facts_from_rows,
    read_entry_confirmation_daily_facts,
    read_entry_confirmation_daily_facts_batch,
    read_entry_confirmation_daily_facts_small_batch,
)
from liangjian_funnel.runtime.publication_receipt import object_hash


TZ = ZoneInfo("Asia/Shanghai")
TARGET = date(2026, 10, 27)


class WeekdayCalendar:
    def previous_trading_day(self, value):
        candidate = value - timedelta(days=1)
        while candidate.weekday() >= 5:
            candidate -= timedelta(days=1)
        return candidate


class HolidayCalendar(WeekdayCalendar):
    holidays = {date(2026, 10, index) for index in range(1, 9)}

    def previous_trading_day(self, value):
        candidate = value - timedelta(days=1)
        while candidate.weekday() >= 5 or candidate in self.holidays:
            candidate -= timedelta(days=1)
        return candidate


def prior_bars(target: date, calendar, count: int = 20) -> list[dict]:
    dates = []
    candidate = target - timedelta(days=1)
    while len(dates) < count:
        if calendar.previous_trading_day(candidate + timedelta(days=1)) == candidate:
            dates.append(candidate)
        candidate -= timedelta(days=1)
    dates.reverse()
    return [{"date": value.isoformat(), "close": 100 + index, "high": 102 + index}
            for index, value in enumerate(dates)]


def rows(symbol_offset: int = 0) -> list[dict]:
    result = []
    for index in range(26):
        day = date(2026, 10, 1) + timedelta(days=index)
        close = 100 + symbol_offset + index
        result.append({"date": day.isoformat(), "close": close, "high": close + 2})
    # Current/future observations are deliberately present and must not enter
    # the 20-day projection.
    result.extend([
        {"date": TARGET.isoformat(), "close": 999, "high": 1000},
        {"date": (TARGET + timedelta(days=1)).isoformat(), "close": 1001, "high": 1002},
    ])
    return result


def snapshot(symbols=("600001.SH", "600002.SH")) -> dict:
    data = {
        "g0_symbols": list(symbols),
        "RECENT_DAILY_BARS": {
            symbol: rows(index * 10) for index, symbol in enumerate(symbols)
        },
    }
    return {
        "snapshot_id": "snapshot-20261026T151000+0800-fixture",
        "snapshot_hash": object_hash(data),
        "as_of": "2026-10-26T15:00:00+08:00",
        "data": data,
    }


def source_pin(snap: dict, *, symbol: str = "600001.SH") -> dict:
    return {
        "source_run_id": "run-20261026-close",
        "lane_id": "lane_1",
        "plan_id": f"plan-{symbol}",
        "snapshot_id": snap["snapshot_id"],
        "snapshot_hash": snap["snapshot_hash"],
        "manifest_sha256": "a" * 64,
        "manifest_file_sha256": "b" * 64,
        "snapshot_file_sha256": "c" * 64,
    }


def test_build_projects_exactly_twenty_closed_prior_days_and_calculates_ma5():
    snap = snapshot()
    facts = build_entry_confirmation_daily_facts(
        snap, symbol="600001.SH", current_trade_date=TARGET,
        source_pin=source_pin(snap), calendar=WeekdayCalendar(),
    )

    assert facts["status"] == "READY"
    assert len(facts["daily_closes"]) == 20
    assert [item["as_of"][:10] for item in facts["daily_closes"]] == [
        (date(2026, 10, 7) + timedelta(days=index)).isoformat() for index in range(20)
    ]
    assert facts["daily_closes"][-1]["close"] == 125.0
    assert facts["previous_day_high"] == {
        "value": 127.0, "as_of": "2026-10-26T15:00:00+08:00"
    }
    assert facts["daily_ma5"] == {
        "value": 123.0, "as_of": "2026-10-26T15:00:00+08:00"
    }


def test_build_fails_closed_for_hash_pin_duplicate_or_insufficient_source():
    snap = snapshot()
    assert build_entry_confirmation_daily_facts(
        snap, symbol="600001.SH", current_trade_date=TARGET, source_pin=None,
    )["status"] == "UNPROVEN"
    bad_hash = {**snap, "snapshot_hash": "d" * 64}
    assert build_entry_confirmation_daily_facts(
        bad_hash, symbol="600001.SH", current_trade_date=TARGET,
        source_pin=source_pin(snap), calendar=WeekdayCalendar(),
    )["status"] == "UNPROVEN"

    duplicate = snapshot()
    duplicate["data"]["RECENT_DAILY_BARS"]["600001.SH"][-1]["date"] = "2026-10-24"
    duplicate["snapshot_hash"] = object_hash(duplicate["data"])
    result = build_entry_confirmation_daily_facts(
        duplicate, symbol="600001.SH", current_trade_date=TARGET,
        source_pin=source_pin(duplicate), calendar=WeekdayCalendar(),
    )
    assert result["status"] == "UNPROVEN"
    assert "DAILY_BAR_DUPLICATE_DATE" in result["reason_codes"]

    short = snapshot()
    short["data"]["RECENT_DAILY_BARS"]["600001.SH"] = rows()[:19]
    short["snapshot_hash"] = object_hash(short["data"])
    result = build_entry_confirmation_daily_facts(
        short, symbol="600001.SH", current_trade_date=TARGET,
        source_pin=source_pin(short), calendar=WeekdayCalendar(),
    )
    assert result["status"] == "UNPROVEN"
    assert "DAILY20_INSUFFICIENT" in result["reason_codes"]


def test_previous_exchange_session_is_required_across_holiday_and_missing_day():
    calendar = HolidayCalendar()
    target = date(2026, 10, 9)
    daily_rows = prior_bars(target, calendar)
    source = {
        "source_run_id": "run", "lane_id": "lane_1", "plan_id": "plan",
        "snapshot_id": "snapshot", "snapshot_hash": "a" * 64,
        "snapshot_file_sha256": "b" * 64,
        "raw_plan_sha256": "c" * 64,
    }
    ready = project_entry_confirmation_daily_facts_from_rows(
        daily_rows, symbol="600001.SH", current_trade_date=target,
        source_pin=source, calendar=calendar,
    )
    assert ready["status"] == "READY"
    assert ready["previous_day_high"]["as_of"].startswith("2026-09-30")

    missing_yesterday = project_entry_confirmation_daily_facts_from_rows(
        daily_rows[:-1], symbol="600001.SH", current_trade_date=target,
        source_pin=source, calendar=calendar,
    )
    assert missing_yesterday["status"] == "UNPROVEN"
    assert "DAILY20_INSUFFICIENT" in missing_yesterday["reason_codes"] or \
        "PREVIOUS_TRADING_DAY_MISSING" in missing_yesterday["reason_codes"]

    # A stale but still 20-row source must not pass merely because it has
    # enough observations; the final row must be the actual prior exchange
    # session after the holiday span.
    stale_rows = [*daily_rows[:-1], {"date": "2026-09-26", "close": 999, "high": 1001}]
    stale = project_entry_confirmation_daily_facts_from_rows(
        stale_rows, symbol="600001.SH", current_trade_date=target,
        source_pin=source, calendar=calendar,
    )
    assert stale["status"] == "UNPROVEN"
    assert "PREVIOUS_TRADING_DAY_MISSING" in stale["reason_codes"]


def write_publication(tmp_path: Path) -> tuple[Path, dict, dict]:
    snap = snapshot()
    encoded_snapshot = json.dumps(snap, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    snapshot_path = tmp_path / "snapshot.json"
    snapshot_path.write_bytes(encoded_snapshot)
    snapshot_file_sha = hashlib.sha256(encoded_snapshot).hexdigest()
    entries = [
        {
            "plan_id": f"run-20261026-close:lane_1:plan-{symbol}",
            "target_trade_date": TARGET.isoformat(),
            "raw_plan_sha256": "1" * 64,
            "a3_audit_sha256": "2" * 64,
            "published_payload": {"value": {"symbol": symbol}, "sha256": object_hash({"symbol": symbol})},
        }
        for symbol in snap["data"]["RECENT_DAILY_BARS"]
    ]
    for entry in entries:
        symbol = entry["published_payload"]["value"]["symbol"]
        facts = project_entry_confirmation_daily_facts_from_rows(
            snap["data"]["RECENT_DAILY_BARS"][symbol], symbol=symbol,
            current_trade_date=TARGET,
            source_pin={
                "source_run_id": "run-20261026-close", "lane_id": "lane_1",
                "plan_id": entry["plan_id"], "snapshot_id": snap["snapshot_id"],
                "snapshot_hash": snap["snapshot_hash"],
                "snapshot_file_sha256": snapshot_file_sha,
                "raw_plan_sha256": entry["raw_plan_sha256"],
                "target_trade_date": TARGET.isoformat(),
            }, source_as_of=snap["as_of"], calendar=WeekdayCalendar(),
        )
        entry["entry_confirmation_daily_facts"] = {
            "schema_version": ENTRY_CONFIRMATION_FACTS_SCHEMA,
            "value": facts,
            "sha256": object_hash(facts),
        }
    manifest = {
        "schema_version": "publication-projection-manifest/1",
        "run_id": "run-20261026-close",
        "lane_id": "lane_1",
        "status": "CAPTURED_NOT_VALIDATED",
        "base_snapshot": {
            "path": str(snapshot_path.resolve()), "sha256": snapshot_file_sha,
            "snapshot_id": snap["snapshot_id"], "snapshot_hash": snap["snapshot_hash"],
        },
        "plans": entries,
    }
    manifest["manifest_sha256"] = object_hash(manifest)
    manifest_path = tmp_path / "publication.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    return manifest_path, snap, manifest


def test_reader_verifies_manifest_snapshot_chain_and_batch_reads_requested_rows_once(tmp_path):
    path, snap, manifest = write_publication(tmp_path)
    before_manifest = path.read_bytes()
    before_snapshot = (tmp_path / "snapshot.json").read_bytes()
    first = read_entry_confirmation_daily_facts(
        path, symbol="600001.SH", current_trade_date=TARGET,
        source_run_id="run-20261026-close", lane_id="lane_1",
        plan_id="run-20261026-close:lane_1:plan-600001.SH",
        calendar=WeekdayCalendar(),
    )
    assert first["status"] == "READY", first
    assert first["source_pin"]["snapshot_hash"] == snap["snapshot_hash"]
    assert first["source_pin"]["manifest_sha256"] == manifest["manifest_sha256"]

    batch = read_entry_confirmation_daily_facts_batch(
        path, symbols=("600001.SH", "600002.SH"), current_trade_date=TARGET,
        source_run_id="run-20261026-close", lane_id="lane_1",
        calendar=WeekdayCalendar(),
    )
    assert set(batch) == {"600001.SH", "600002.SH"}
    assert all(value["status"] == "READY" for value in batch.values())
    assert all(len(value["daily_closes"]) == 20 for value in batch.values())
    assert path.read_bytes() == before_manifest
    assert (tmp_path / "snapshot.json").read_bytes() == before_snapshot


def test_reader_returns_unproven_on_manifest_or_snapshot_pin_mutation(tmp_path):
    path, snap, manifest = write_publication(tmp_path)
    manifest["base_snapshot"]["sha256"] = "f" * 64
    # The canonical manifest self-hash is intentionally not resealed.
    path.write_text(json.dumps(manifest, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    result = read_entry_confirmation_daily_facts(
        path, symbol="600001.SH", current_trade_date=TARGET,
        source_run_id="run-20261026-close", lane_id="lane_1",
        plan_id="run-20261026-close:lane_1:plan-600001.SH",
    )
    assert result["status"] == "UNPROVEN"
    assert "PUBLICATION_MANIFEST_INVALID" in result["reason_codes"]


def test_reader_does_not_cross_bind_a_plan_to_another_symbol(tmp_path):
    path, _, _ = write_publication(tmp_path)
    result = read_entry_confirmation_daily_facts(
        path, symbol="600002.SH", current_trade_date=TARGET,
        source_run_id="run-20261026-close", lane_id="lane_1",
        plan_id="run-20261026-close:lane_1:plan-600001.SH",
        calendar=WeekdayCalendar(),
    )
    assert result["status"] == "UNPROVEN"
    assert "PUBLICATION_SYMBOL_CONFLICT" in result["reason_codes"]


def test_small_runtime_reader_does_not_fallback_to_the_base_snapshot(tmp_path):
    path, _, manifest = write_publication(tmp_path)
    manifest["plans"][0].pop("entry_confirmation_daily_facts")
    manifest["manifest_sha256"] = object_hash({key: value for key, value in manifest.items() if key != "manifest_sha256"})
    path.write_text(json.dumps(manifest, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    result = read_entry_confirmation_daily_facts_small_batch(
        path, symbols=("600001.SH",), current_trade_date=TARGET,
        source_run_id="run-20261026-close", lane_id="lane_1",
        plan_ids={"600001.SH": "run-20261026-close:lane_1:plan-600001.SH"},
        expected_manifest_file_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        calendar=WeekdayCalendar(),
    )
    assert result["600001.SH"]["status"] == "UNPROVEN"
    assert "ENTRY_CONFIRMATION_FACTS_MISSING" in result["600001.SH"]["reason_codes"]


def test_runtime_wrapper_discovers_manifest_from_run_summary_and_rejects_tampered_small_facts(tmp_path, monkeypatch):
    manifest_path, snap, manifest = write_publication(tmp_path)
    run_dir = tmp_path / "runs"
    run_dir.mkdir()
    manifest_sha = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    (run_dir / "run-20261026-close.json").write_text(json.dumps({
        "run_id": "run-20261026-close",
        "plan_publication": {"projection_manifests": [{
            "lane_id": "lane_1", "path": str(manifest_path), "sha256": manifest_sha,
        }]},
    }), encoding="utf-8")
    settings = SimpleNamespace(workflow_output_dir=tmp_path)
    plan_rows = [{
        "plan_id": "run-20261026-close:lane_1:plan-600001.SH",
        "symbol": "600001.SH", "lane_id": "lane_1",
        "source_run_id": "run-20261026-close",
        # A caller-provided path is intentionally ignored by the wrapper.
        "publication_manifest_path": str(tmp_path / "attacker.json"),
    }]

    from liangjian_funnel.runtime.entry_confirmation_sources import read_runtime_daily_facts
    monkeypatch.setattr(sources, "_TRADING_CALENDAR", WeekdayCalendar())
    result = read_runtime_daily_facts(settings, plan_rows, current_trade_date=TARGET)
    outer_id = plan_rows[0]["plan_id"]
    assert result[outer_id]["status"] == "READY"
    first_pin = result[outer_id]["source_pin"]

    # Runtime uses only the pinned small facts object; mutating that object
    # without resealing the manifest is rejected before any source snapshot is
    # considered.
    tampered = json.loads(manifest_path.read_text(encoding="utf-8"))
    tampered["plans"][0]["entry_confirmation_daily_facts"]["value"]["daily_closes"][0]["close"] = 9999
    manifest_path.write_text(json.dumps(tampered, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    changed = read_runtime_daily_facts(settings, plan_rows, current_trade_date=TARGET)
    assert changed[outer_id]["status"] == "UNPROVEN"
    assert "PUBLICATION_MANIFEST_INVALID" in changed[outer_id]["reason_codes"]
