from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from liangjian_funnel.data.open_macro import (
    DEFAULT_ETFS,
    OpenMacroDataCollector,
    build_asset_rotation_snapshot,
    build_industry_activity_data,
    build_macro_economic_data,
)


SHANGHAI = ZoneInfo("Asia/Shanghai")
AS_OF = datetime(2026, 8, 27, 15, 0, tzinfo=SHANGHAI)


def _macro_datasets() -> dict[str, dict[str, object]]:
    return {
        "PMI": {
            "source_ref": "fake:pmi",
            "rows": [
                {"月份": "2026年07月", "制造业-指数": 50.1, "发布时间": "2026-08-10T09:00:00+08:00"},
                # The observation is before as_of, but its publication is not.
                {"月份": "2026年08月", "制造业-指数": 99.9, "发布时间": "2026-09-01T09:00:00+08:00"},
            ],
        },
        "CPI": {
            "source_ref": "fake:cpi",
            "rows": [{"月份": "2026年07月", "全国-当月同比": 1.2, "发布时间": "2026-08-10"}],
        },
        "PPI": {
            "source_ref": "fake:ppi",
            "rows": [{"月份": "2026年07月", "当月同比": -0.5, "发布时间": "2026-08-10"}],
        },
        "M1_YOY": {
            "source_ref": "fake:money",
            "rows": [{"月份": "2026年07月", "货币(M1)-同比": 5.0, "发布时间": "2026-08-10"}],
        },
        "M2_YOY": {
            "source_ref": "fake:money",
            "rows": [{"月份": "2026年07月", "货币和准货币(M2)-同比": 8.0, "发布时间": "2026-08-10"}],
        },
        "SOCIAL_FINANCING": {
            "source_ref": "fake:social-financing",
            "rows": [{"月份": "2026年07月", "社会融资规模增量": 1234, "发布时间": "2026-08-10"}],
        },
    }


def _etf_rows(*, future: bool = True) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    start = date(2026, 5, 1)
    for index in range(70):
        rows.append({"日期": (start + timedelta(days=index)).isoformat(), "收盘": 100 + index})
    if future:
        rows.append({"日期": "2026-08-28", "收盘": 9999})
    return rows


class FakeProvider:
    def macro_china_pmi(self):
        return _macro_datasets()["PMI"]["rows"]

    def macro_china_cpi(self):
        return _macro_datasets()["CPI"]["rows"]

    def macro_china_ppi(self):
        return _macro_datasets()["PPI"]["rows"]

    def macro_china_money_supply(self):
        rows = [{"月份": "2026年07月", "货币(M1)-同比": 5.0, "货币和准货币(M2)-同比": 8.0, "发布时间": "2026-08-10"}]
        return rows

    def macro_china_shrzgm(self):
        return _macro_datasets()["SOCIAL_FINANCING"]["rows"]

    def fund_etf_hist_em(self, symbol, period, start_date, end_date, adjust):
        assert symbol in {item["symbol"] for item in DEFAULT_ETFS.values()}
        return _etf_rows()


class RatesProvider(FakeProvider):
    def bond_zh_us_rate(self, period, start_date, end_date, adjust):
        return [{"日期": "2026-08-26", "中国国债10年": 2.1, "美国国债10年": 4.0}]

    def index_global_hist_em(self, symbol, period, start_date, end_date, adjust):
        if symbol != "美元指数":
            return []
        return _etf_rows(future=False)


def test_macro_builder_filters_future_publication_and_preserves_missing_values() -> None:
    result = build_macro_economic_data(_macro_datasets(), as_of=AS_OF)

    assert result["available"] is True
    assert result["latest"]["PMI"]["value"] == 50.1
    assert all(point["value"] != 99.9 for point in result["series"])
    assert result["values"]["m1_m2_gap"] == -3.0
    assert result["values"]["credit_impulse"] is None
    assert len(result["content_hash"]) == 64
    assert result["rules"]["missing_values_are_not_zero"] is True
    assert all(point["publish_time_available"] is True for point in result["series"])
    assert all(point["pit_verified"] is True for point in result["series"])


def test_observation_date_only_is_realtime_usable_but_not_strict_pit() -> None:
    result = build_macro_economic_data(
        {"PMI": {"rows": [{"月份": "2026年07月", "制造业-指数": 50.1}]}},
        as_of=AS_OF,
    )

    point = result["series"][0]
    assert point["publish_time_available"] is False
    assert point["pit_verified"] is False
    assert point["pit_status"] == "OBSERVATION_DATE_ONLY"
    assert result["quality"] == "T2_OPEN_OBSERVATION_DATE_ONLY"


def test_asset_builder_emits_all_four_assets_and_excludes_future_bar() -> None:
    histories = {asset: _etf_rows() for asset in DEFAULT_ETFS}
    result = build_asset_rotation_snapshot(histories, as_of=AS_OF)

    assert result["status"] == "READY"
    assert set(result["assets"]) == set(DEFAULT_ETFS)
    assert all(item["close"] != 9999 for item in result["assets"].values())
    assert all(item["momentum_20d"] is not None for item in result["assets"].values())
    assert all(item["momentum_60d"] is not None for item in result["assets"].values())
    assert all(item["fund_flow_percentile"] is None for item in result["assets"].values())


def test_collector_injected_provider_returns_four_contracts_and_degrades_optional_sources() -> None:
    result = OpenMacroDataCollector(FakeProvider()).collect(AS_OF)

    assert set(result) >= {
        "MACRO_ECONOMIC_DATA",
        "ASSET_ROTATION_SNAPSHOT",
        "GLOBAL_MACRO_SNAPSHOT",
        "CROSS_MARKET_LEAD_SNAPSHOT",
        "source_manifest",
        "content_hash",
        "as_of",
        "freshness",
        "quality",
    }
    assert result["MACRO_ECONOMIC_DATA"]["available"] is True
    assert result["MACRO_ECONOMIC_DATA"]["values"]["credit_series"] == "SOCIAL_FINANCING"
    assert "NEW_CREDIT" not in result["MACRO_ECONOMIC_DATA"]["latest"]
    assert result["ASSET_ROTATION_SNAPSHOT"]["status"] == "READY"
    assert result["GLOBAL_MACRO_SNAPSHOT"]["available"] is False
    assert result["CROSS_MARKET_LEAD_SNAPSHOT"]["available"] is False
    assert result["GLOBAL_MACRO_SNAPSHOT"]["values"] == {}
    assert any(item["status"] == "UNAVAILABLE" for item in result["source_manifest"])


def test_provider_failure_is_structured_and_does_not_raise() -> None:
    class BrokenProvider:
        def __getattr__(self, name):
            def broken(**kwargs):
                raise ConnectionError(name)

            return broken

    result = OpenMacroDataCollector(BrokenProvider()).collect(AS_OF)

    assert result["MACRO_ECONOMIC_DATA"]["reason_code"] == "SOURCE_UNAVAILABLE"
    assert result["ASSET_ROTATION_SNAPSHOT"]["reason_code"] == "SOURCE_UNAVAILABLE"
    assert result["GLOBAL_MACRO_SNAPSHOT"]["reason_code"] == "SOURCE_UNAVAILABLE"
    assert result["CROSS_MARKET_LEAD_SNAPSHOT"]["reason_code"] == "SOURCE_UNAVAILABLE"
    assert all(item["status"] == "UNAVAILABLE" for item in result["source_manifest"])


def test_collector_maps_shared_china_us_rate_table_without_faking_fed_data() -> None:
    result = OpenMacroDataCollector(RatesProvider()).collect(AS_OF)

    global_macro = result["GLOBAL_MACRO_SNAPSHOT"]
    assert global_macro["values"]["cn_rate"] == 2.1
    assert global_macro["values"]["us_rate"] == 4.0
    assert "fed_easing_probability_percentile" in global_macro["missing_fields"]
    assert global_macro["values"].get("fed_easing_probability_percentile") is None
    assert global_macro["available"] is True


def test_collector_uses_sina_etf_history_when_eastmoney_endpoint_fails() -> None:
    class SinaProvider(FakeProvider):
        def fund_etf_hist_em(self, **kwargs):
            raise ConnectionError("eastmoney reset")

        def fund_etf_hist_sina(self, symbol):
            assert symbol.startswith(("sh", "sz"))
            return _etf_rows(future=False)

    result = OpenMacroDataCollector(SinaProvider()).collect(AS_OF)

    assert result["ASSET_ROTATION_SNAPSHOT"]["status"] == "READY"
    assert all("fund_etf_hist_sina" in item["source_ref"] for item in result["source_manifest"] if item["source_ref"].endswith((":510300", ":518880", ":511010", ":511880")))


def test_collector_uses_tencent_etf_history_before_sina() -> None:
    class TencentProvider(FakeProvider):
        def fund_etf_hist_em(self, **kwargs):
            raise ConnectionError("eastmoney reset")

        def stock_zh_a_hist_tx(self, symbol, start_date, end_date, adjust):
            assert symbol.startswith(("sh", "sz"))
            return _etf_rows(future=False)

    result = OpenMacroDataCollector(TencentProvider()).collect(AS_OF)

    assert result["ASSET_ROTATION_SNAPSHOT"]["status"] == "READY"
    assert all("stock_zh_a_hist_tx" in item["source_ref"] for item in result["source_manifest"] if item["source_ref"].endswith((":510300", ":518880", ":511010", ":511880")))


def test_industry_activity_is_not_mislabeled_as_profit_and_filters_future_rows() -> None:
    result = build_industry_activity_data(
        [
            {"统计时间": "2026年07月", "行业名称": "煤炭开采和洗选业", "当月同比": 4.2, "累计同比": 3.1, "发布时间": "2026-08-20"},
            {"统计时间": "2026年08月", "行业名称": "化学原料和化学制品制造业", "当月同比": 99.0, "发布时间": "2026-09-20"},
        ],
        as_of=AS_OF,
        source_ref="akshare_open:macro_china_nbs_nation:工业增加值",
    )

    assert result["available"] is True
    assert result["metric_scope"] == "INDUSTRIAL_VALUE_ADDED_GROWTH_NOT_PROFIT"
    assert [item["industry"] for item in result["items"]] == ["煤炭开采和洗选业"]
    assert result["quality"] == "T1_OFFICIAL_NORMALIZED"


def test_industry_activity_supports_nbs_index_by_month_dataframe_shape() -> None:
    class FakeRow:
        def __init__(self, values):
            self._values = values

        def to_dict(self):
            return dict(self._values)

    class FakeILoc:
        def __init__(self, rows):
            self._rows = rows

        def __getitem__(self, index):
            return FakeRow(self._rows[index])

    class FakeNbsFrame:
        index = (
            "煤炭开采和洗选业增加值_同比增长(%)",
            "化学原料和化学制品制造业增加值_累计增长(%)",
        )
        columns = ("2026年7月", "2026年8月", "2026年9月")
        iloc = FakeILoc(
            [
                {"2026年7月": 4.2, "2026年8月": 5.0, "2026年9月": 99.0},
                {"2026年7月": 3.1, "2026年8月": 3.8, "2026年9月": 88.0},
            ]
        )

    result = build_industry_activity_data(FakeNbsFrame(), as_of=AS_OF)

    assert result["available"] is True
    assert len(result["items"]) == 4
    assert all("2026-09" not in item["observation_time"] for item in result["items"])
    assert {item["industry"] for item in result["items"]} == {
        "煤炭开采和洗选业增加值",
        "化学原料和化学制品制造业增加值",
    }
    assert result["quality"] == "T2_OPEN_OBSERVATION_DATE_ONLY"


def test_successful_snapshot_is_used_as_pit_safe_stale_fallback(tmp_path) -> None:
    live = OpenMacroDataCollector(FakeProvider(), cache_dir=tmp_path).collect(AS_OF)
    stale = OpenMacroDataCollector(BrokenProviderForCache(), cache_dir=tmp_path).collect(
        datetime(2026, 8, 28, 15, 0, tzinfo=SHANGHAI)
    )

    assert live["cache_status"] == "LIVE"
    assert stale["cache_status"] == "STALE_FALLBACK"
    assert stale["cache_original_content_hash"] == live["content_hash"]
    assert stale["cache_age_days"] == 1
    assert stale["requested_as_of"].startswith("2026-08-28")
    assert stale["MACRO_ECONOMIC_DATA"]["latest"]["PMI"]["value"] == 50.1


class BrokenProviderForCache:
    def __getattr__(self, name):
        def broken(**kwargs):
            raise ConnectionError(name)

        return broken
