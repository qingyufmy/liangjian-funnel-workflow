from datetime import datetime
from zoneinfo import ZoneInfo

from liangjian_funnel.data.ths_taxonomy import collect_ths_taxonomy_membership
from liangjian_funnel.pipeline.data_source import HithinkFetchResult, HithinkRow


NOW = datetime(2026, 8, 27, 15, 10, tzinfo=ZoneInfo("Asia/Shanghai"))


def _result(rows, *, ok=True, reason="OK"):
    return HithinkFetchResult(
        endpoint="/test",
        ok=ok,
        complete=ok,
        reason_code=reason,
        items=tuple(HithinkRow.model_validate(row) for row in rows),
        pages=1,
        total=len(rows),
        fetch_time=NOW,
    )


def test_concept_membership_is_complete_versioned_and_cached(tmp_path):
    catalog = _result([{"thscode": "885001.TI", "name": "算力概念"}])

    class Client:
        calls = 0

        def ths_index_constituents(self, code):
            self.calls += 1
            assert code == "885001.TI"
            return _result([{"thscode": "600000.SH", "name": "测试公司"}])

    client = Client()
    first = collect_ths_taxonomy_membership(
        client,
        catalog,
        ["600000.SH", "000001.SZ"],
        taxonomy="concept",
        cache_dir=tmp_path,
        as_of=NOW,
        sleep=lambda _: None,
    )
    assert first.ok and first.complete and client.calls == 1
    rows = {item.model_dump()["thscode"]: item.model_dump() for item in first.items}
    assert rows["600000.SH"]["memberships"][0]["concept_thscode"] == "885001.TI"
    assert rows["000001.SZ"]["mapping_status"] == "UNMAPPED"

    cached_client = Client()
    cached_client.calls = 0
    second = collect_ths_taxonomy_membership(
        cached_client,
        catalog,
        ["600000.SH"],
        taxonomy="concept",
        cache_dir=tmp_path,
        as_of=NOW,
    )
    assert second.ok and second.metadata["cache_hit"] is True and cached_client.calls == 0
