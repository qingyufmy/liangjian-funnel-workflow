import io
import struct
import zipfile

import pytest

from liangjian_funnel.data import a2_market
from liangjian_funnel.data.tdx_daily_package import decode_daily_package
from liangjian_funnel.data.sina_finance_shadow import normalize_finance_report


def package(*, bad_price=False, duplicate=False, no_trade=False):
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
        for market, code in (("sh", "600519"), ("sz", "000001"), ("bj", "920001")):
            record = bytearray(150)
            record[:6] = code.encode()
            record[40:44] = "测试".encode("gbk")
            bar = bytearray(512)
            struct.pack_into("<5d", bar, 4, 9, 10, 12, 8, float("nan") if bad_price else 11)
            struct.pack_into("<Q", bar, 56, 12300)
            struct.pack_into("<d", bar, 72, 135300)
            if no_trade:
                struct.pack_into("<5d", bar, 4, 9, 0, 0, 0, 9)
                struct.pack_into("<Q", bar, 56, 0)
                struct.pack_into("<d", bar, 72, 0)
            z.writestr(f"{market}260922.cod", record * (2 if duplicate else 1))
            z.writestr(f"{market}260922.md1", bar * (2 if duplicate else 1))
    return out.getvalue()


def test_daily_units_identity_and_missing():
    got = decode_daily_package(package(), "2026-09-22", equity_symbols=["600519.SH", "600000.SH"])
    assert len(got["rows"]) == 1
    assert got["rows"][0]["volume_shares"] == 12300
    assert got["rows"][0]["amount_cny"] == 135300
    assert got["missing_symbols"] == ["600000.SH"]
    assert got["shadow_only"] and not got["execution_authority"]
    assert not got["historical_availability_verified"]


@pytest.mark.parametrize("kwargs", [{"bad_price": True}, {"duplicate": True}])
def test_daily_bad_records_fail_closed(kwargs):
    with pytest.raises(ValueError):
        decode_daily_package(package(**kwargs), "2026-09-22", equity_symbols=["600519.SH"])


def test_daily_wrong_archive_date():
    with pytest.raises(ValueError):
        decode_daily_package(package(), "2026-09-21", equity_symbols=[])


def test_no_trade_is_not_usable_or_fabricated_bar():
    got = decode_daily_package(package(no_trade=True), "2026-09-22", equity_symbols=["600519.SH"])
    assert got["rows"] == []
    assert got["missing_symbols"] == ["600519.SH"]
    assert got["unavailable_records"] == [{"symbol": "600519.SH", "reason": "NO_TRADING_BAR_STATUS_UNVERIFIED"}]


def test_archive_extra_member_not_extracted():
    raw = io.BytesIO(package())
    with zipfile.ZipFile(raw, "a") as z:
        z.writestr("../unexpected", "not extracted")
    with pytest.raises(ValueError):
        decode_daily_package(raw.getvalue(), "2026-09-22", equity_symbols=[])


@pytest.mark.parametrize("pages", [
    [(2, [{"f12": "BK1"}]), (2, [{"f12": "BK1"}])],
    [(2, [{"f12": "BK1"}]), (3, [{"f12": "BK2"}])],
    [(1, [{"f12": "BK1"}, {"f12": "BK2"}])],
    [(1, [None])],
    [(None, [])],
])
def test_board_incomplete_pagination_rejected(monkeypatch, pages):
    responses = iter(pages)
    def get(*args, **kwargs):
        total, rows = next(responses)
        return {"data": {"total": total, "diff": rows}}, "test"
    monkeypatch.setattr(a2_market, "_eastmoney_get_json", get)
    with pytest.raises(a2_market.CapitalFlowError):
        a2_market._eastmoney_board_flow_fetcher("concept", "today")


def test_board_complete_pages(monkeypatch):
    pages = iter([{"total": 2, "diff": [{"f12": "BK1"}]}, {"total": 2, "diff": [{"f12": "BK2"}]}])
    monkeypatch.setattr(a2_market, "_eastmoney_get_json", lambda *a, **kw: ({"data": next(pages)}, "test"))
    assert a2_market._eastmoney_board_flow_fetcher("concept", "today")["total"] == 2


def test_finance_missing_is_not_zero_or_business_evidence():
    body = {"result": {"status": {"code": 0}, "data": {"report_list": {
        "20260630": {"data": [{"item_field": "PROFIT", "item_value": "--"}]}
    }}}}
    got = normalize_finance_report(body, symbol="600519.SH", statement="lrb")
    assert got["rows"][0]["fields"]["PROFIT"]["value"] is None
    assert got["rows"][0]["publish_date"] is None
    assert not got["feature_ready"] and not got["historical_availability_verified"]


@pytest.mark.parametrize("body", [{}, {"result": {"status": None}}, {"result": {"status": {"code": 1}, "data": {"report_list": {}}}}])
def test_finance_business_error_is_not_empty_success(body):
    with pytest.raises(ValueError):
        normalize_finance_report(body, symbol="600519.SH", statement="lrb")


@pytest.mark.parametrize("fields,published", [
    ([{"item_field": "X", "item_value": "NaN"}], "20260815"),
    ([{"item_field": "X", "item_value": "1"}, {"item_field": "X", "item_value": "2"}], "20260815"),
    ([], "20260101"),
])
def test_finance_conflicting_evidence_rejected(fields, published):
    body = {"result": {"status": {"code": 0}, "data": {"report_list": {
        "20260630": {"data": fields, "publish_date": published}
    }}}}
    with pytest.raises(ValueError):
        normalize_finance_report(body, symbol="600519.SH", statement="lrb")


def test_finance_section_headings_and_group_identity():
    fields = [{"item_field": "", "item_title": "流动资产", "item_value": "", "item_group_no": 1},
              {"item_field": "X", "item_value": "10", "item_group_no": 1},
              {"item_field": "X", "item_value": "20", "item_group_no": 2}]
    body = {"result": {"status": {"code": 0}, "data": {"report_list": {"20260630": {"data": fields}}}}}
    row = normalize_finance_report(body, symbol="600519.SH", statement="fzb")["rows"][0]
    assert row["fields"]["1:X"]["value"] == "10"
    assert row["fields"]["2:X"]["value"] == "20"
    assert len(row["headings"]) == 1


def test_finance_exact_duplicates_counted_not_added():
    item = {"item_field": "X", "item_value": "1"}
    body = {"result": {"status": {"code": 0}, "data": {"report_list": {"20260630": {"data": [item, item]}}}}}
    row = normalize_finance_report(body, symbol="600519.SH", statement="fzb")["rows"][0]
    assert len(row["fields"]) == 1
    assert row["exact_duplicate_count"] == 1
