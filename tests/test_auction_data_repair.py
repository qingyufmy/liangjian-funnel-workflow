from datetime import date, datetime, timedelta
from unittest.mock import Mock
from zoneinfo import ZoneInfo
import json

import pytest

from liangjian_funnel.facts.hithink import project_closed_ladder
from liangjian_funnel.pipeline.data_source import HithinkFetchResult, HithinkRow
from liangjian_funnel.data.ths_taxonomy import collect_ths_taxonomy_membership
from liangjian_funnel.data.ths_industry import collect_ths_industry_membership

TZ = ZoneInfo("Asia/Shanghai")
FRIDAY = datetime(2026, 9, 11, 15, 10, tzinfo=TZ)
MONDAY = datetime(2026, 9, 14, 9, 26, tzinfo=TZ)


def result(rows, **kwargs):
    return HithinkFetchResult(endpoint="/fixture", ok=True, complete=True, reason_code="OK",
        fetch_time=FRIDAY, items=tuple(HithinkRow.model_validate(row) for row in rows), **kwargs)


def ladder():
    days = ["2026-09-14", "2026-09-11", "2026-09-10"]
    return result([{"date": d, "boards": {"two_board": [{"thscode": "600000.SH", "board_num": 2,
                                                             "seal_nextday": True, "sign_level": 1}]}} for d in days],
                  total=3, metadata={"window": {"date_list": days}, "timestamp": MONDAY.isoformat()})


def test_live_window_contains_target_and_excludes_future_annotations():
    raw = ladder()
    before = raw.model_dump(mode="json")
    fixed = project_closed_ladder(raw, market_trade_date=date(2026, 9, 11))
    assert fixed.ok and fixed.complete
    rows = [r.model_dump() for r in fixed.items]
    assert [r["date"] for r in rows] == ["2026-09-11", "2026-09-10"]
    assert "seal_nextday" not in rows[0]["boards"]["two_board"][0]
    assert "sign_level" not in rows[0]["boards"]["two_board"][0]
    assert rows[1]["boards"]["two_board"][0]["seal_nextday"] is True
    assert raw.model_dump(mode="json") == before
    assert fixed.metadata["observed_latest_market_trade_date"] == "2026-09-14"
    assert fixed.metadata["market_trade_date"] == "2026-09-11"
    assert fixed.total == 2


@pytest.mark.parametrize("defect", ["missing_target", "duplicate", "missing_row", "bad_date", "partial", "boards_invalid"])
def test_bad_ladder_is_never_promoted(defect):
    raw = ladder().model_dump(mode="json")
    target = date(2026, 9, 11)
    if defect == "missing_target": target = date(2026, 9, 9)
    if defect == "duplicate": raw["items"][1] = raw["items"][0]
    if defect == "missing_row": raw["items"] = raw["items"][:1]
    if defect == "bad_date": raw["metadata"]["window"]["date_list"][0] = "bogus"
    if defect == "partial": raw.update(ok=False, complete=False, reason_code="REQUEST_FAILED")
    if defect == "boards_invalid": raw["items"][1]["boards"] = {"two_board": None}
    fixed = project_closed_ladder(HithinkFetchResult.model_validate(raw), market_trade_date=target)
    assert not fixed.ok and not fixed.complete


@pytest.mark.parametrize("kind", ["industry", "concept"])
def test_reference_reuses_friday_graph_without_any_constituent_fetch(tmp_path, kind):
    catalog = result([{"thscode": "881101.TI" if kind == "industry" else "885001.TI", "name": "fixture"}])
    client = Mock()
    client.ths_index_constituents.return_value = result([{"thscode": "600000.SH", "name": "fixture"}])
    def collect(at, **kwargs):
        common = dict(cache_dir=tmp_path, as_of=at, **kwargs)
        if kind == "industry": return collect_ths_industry_membership(client, catalog, ["600000.SH"], **common)
        return collect_ths_taxonomy_membership(client, catalog, ["600000.SH"], taxonomy="concept", **common)
    collect(FRIDAY)
    client.reset_mock()
    read = collect(MONDAY, cache_max_age_days=7)
    client.ths_index_constituents.assert_not_called()
    assert read.ok and read.metadata["cache_hit"]
    assert read.metadata["cache_trade_date"] == "2026-09-11"
    assert read.fetch_time == FRIDAY
    assert not (tmp_path / f"ths-{kind}-2026-09-14.json").exists()


@pytest.mark.parametrize("defect", ["checksum", "catalog_changed", "expired", "future_capture", "incomplete"])
def test_reference_rejects_invalid_or_expired_cache(tmp_path, defect):
    catalog = result([{"thscode": "885001.TI", "name": "fixture"}])
    client = Mock()
    client.ths_index_constituents.return_value = result([{"thscode": "600000.SH"}])
    collect_ths_taxonomy_membership(client, catalog, ["600000.SH"], taxonomy="concept", cache_dir=tmp_path, as_of=FRIDAY)
    path = tmp_path / "ths-concept-2026-09-11.json"
    cached = json.loads(path.read_text())
    if defect == "checksum": cached["memberships_sha256"] = "bad"
    if defect == "catalog_changed": cached["catalog_hash"] = "bad"
    if defect == "incomplete": cached["complete"] = False
    if defect == "future_capture": cached["fetched_at"] = (MONDAY + timedelta(hours=1)).isoformat()
    path.write_text(json.dumps(cached))
    client.reset_mock()
    collect_ths_taxonomy_membership(client, catalog, ["600000.SH"], taxonomy="concept", cache_dir=tmp_path,
                                   as_of=MONDAY + timedelta(days=7) if defect == "expired" else MONDAY, cache_max_age_days=7)
    client.ths_index_constituents.assert_called_once()
