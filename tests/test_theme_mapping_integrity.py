from copy import deepcopy

import pytest

from liangjian_funnel.data.business_disclosure import financial_business_kind
from liangjian_funnel.data.cninfo_pdf import _build_snippets
from liangjian_funnel.data.rotation_theme import load_rotation_theme_config, validate_board_identity
from liangjian_funnel.workflow import _main_business_evidence
from test_rotation_theme import (
    AS_OF, CAPTURE, DAY, _registry_payload, _write_registry, _membership,
    _top_level_fetchers, _east_page, _board,
)
from liangjian_funnel.data.rotation_theme import write_membership_snapshot, collect_rotation_theme_snapshot


def test_registry_separates_industry_concept_and_rotation_domains():
    c = load_rotation_theme_config()
    assert c.get("ROBOTICS_ADVANCED_MANUFACTURING").eastmoney_board_codes == ("BK1090",)
    assert c.get("AI_APPLICATIONS_DIGITAL_ECONOMY").parent is None
    assert c.get("CONSUMER_SERVICES").eastmoney_board_codes == ("BK1214",)
    assert c.alias_to_theme["银行"] == "BANKS"
    assert c.alias_to_theme["数字经济"] == "DIGITAL_ECONOMY"
    assert "工业母机" not in c.alias_to_theme
    assert c.get("RETAIL_GENERAL").parent == "COMMERCE_RETAIL"
    assert c.get("SHIPPING").eastmoney_board_codes == ("BK1482",)


def test_code_exists_but_vendor_name_wrong_is_rejected():
    theme = load_rotation_theme_config().get("ROBOTICS_ADVANCED_MANUFACTURING")
    assert validate_board_identity(theme, {"available": True, "records": [
        {"board_code": "BK1090", "board_name": "机器人"}
    ]}) == "ROTATION_BOARD_IDENTITY_MISMATCH"


@pytest.mark.parametrize("refresh_ok", [True, False])
def test_fresh_cache_from_old_board_cannot_be_reused(tmp_path, refresh_ok):
    registry = _write_registry(tmp_path / "registry.yaml")
    cache = _membership()
    # Rebuild the hash-bound snapshot so this tests identity, not corruption.
    from liangjian_funnel.data.rotation_theme import _content_hash
    cache["pagination_evidence"]["pages"][0]["board_code"] = "BK9999"
    cache.pop("content_hash")
    cache["content_hash"] = _content_hash(cache)
    write_membership_snapshot(tmp_path / "daily" / "memberships", cache)
    calls = []
    def fetch(*args):
        calls.append(args)
        return _east_page(*args) if refresh_ok else {"total": 1, "rows": []}
    r = collect_rotation_theme_snapshot(as_of=AS_OF, expected_trade_date=DAY,
        registry_path=registry, snapshot_dir=tmp_path / "daily",
        fetchers=_top_level_fetchers(member_fetcher=fetch), tencent_capture_timestamp=CAPTURE)
    assert calls
    if refresh_ok:
        assert r["available"] is True
    else:
        assert "TEST_THEME" in r["source_health"]["unavailable_membership"]
        assert not r.get("boards")


def test_catalog_outage_uses_only_identity_verified_local_membership(tmp_path):
    themes = deepcopy(_registry_payload()["themes"])
    themes[0]["eastmoney_board_names"] = {"BK0001": "测试方向"}
    registry = _write_registry(tmp_path / "registry.yaml", themes=themes)
    cache = _membership()
    from liangjian_funnel.data.rotation_theme import _content_hash
    cache["pagination_evidence"]["pages"][0]["board_name"] = "测试方向"
    cache.pop("content_hash")
    cache["content_hash"] = _content_hash(cache)
    write_membership_snapshot(tmp_path / "daily" / "memberships", cache)
    def fail(*args):
        import requests
        raise requests.Timeout("catalog offline")
    r = collect_rotation_theme_snapshot(as_of=AS_OF, expected_trade_date=DAY,
        registry_path=registry, snapshot_dir=tmp_path / "daily",
        fetchers=_top_level_fetchers(catalog_fetcher=fail), tencent_capture_timestamp=CAPTURE)
    assert r["available"] is True
    assert r["source_health"]["membership_update_warnings"]["TEST_THEME"] == "CATALOG_UNAVAILABLE_VERIFIED_MEMBERSHIP_REUSED"


@pytest.mark.parametrize("text,kind", [
    ("保险服务收入 未以保费分配法计量的保险合同 预计当期发生的保险服务费用 10,883", "INSURANCE_BUSINESS_DISCLOSURE"),
    ("人保财险各险种经营信息 保险服务 收入 机动车辆险 承保利润", "INSURANCE_BUSINESS_DISCLOSURE"),
    ("利息净收入 客户贷款 公司银行 100亿元", "BANK_BUSINESS_DISCLOSURE"),
    ("证券经纪业务 手续费及佣金净收入 100亿元", "SECURITIES_BUSINESS_DISCLOSURE"),
    ("合并现金流量表 收到签发保险合同保费取得的现金 保险服务收入", None),
    ("本公司购买车险 支付保险费", None),
    ("目录 保险服务收入 险种", None),
    ("该准则以保险合同的确认、计量为核心，规范保险服务收入确认和合同服务边际", None),
])
def test_financial_disclosure_is_not_generic_cashflow_or_company_name(text, kind):
    assert financial_business_kind(text) == kind
    record = {"pdf_evidence_available": True, "announcement_id": "ann-1", "content_hash": "a" * 64,
              "pdf_evidence_snippets": [{"page_number": 5, "text": text}]}
    result = _main_business_evidence({"by_symbol": {"601336.SH": [record]}}, ["601336.SH"])
    assert result["601336.SH"]["available"] is (kind is not None)


def test_financial_business_survives_keyword_heavy_cashflow_pages():
    pages = [(i, "现金流量表 收入 营收 合同 金额 风险 净利润 同比 报告期 客户") for i in range(1, 20)]
    pages.append((90, "保险服务收入 保险合同 合同服务边际 30,301"))
    assert _build_snippets(pages)[0].page_number == 90


def test_revenue_table_beyond_first_500_characters_survives():
    text = "其他事项" * 180 + " 营业收入构成 分产品 机械设备 100,000 65.5% 分地区 华东 "
    snippets = _build_snippets([(10, text)])
    assert "机械设备" in snippets[0].text
    assert "65.5%" in snippets[0].text


def test_financing_eligibility_and_cross_industry_names_do_not_prove_business():
    from liangjian_funnel.pipeline.mature_theme_registry import taxonomy_is_business_related
    assert not taxonomy_is_business_related("FINANCIAL_HIGH_DIVIDEND", "CONCEPT", "融资融券")
    assert not taxonomy_is_business_related("FINANCIAL_HIGH_DIVIDEND", "CONCEPT", "参股保险")
    assert not taxonomy_is_business_related("CONSUMER_SERVICES", "CONCEPT", "消费电子概念")
    assert not taxonomy_is_business_related("SEMICONDUCTOR_LOCALIZATION", "CONCEPT", "国产操作系统")
    assert taxonomy_is_business_related("FINANCIAL_HIGH_DIVIDEND", "INDUSTRY", "保险")


def test_child_cannot_borrow_parent_eligibility_with_negative_flow():
    from test_rotation_theme import _metric_row
    from liangjian_funnel.data.rotation_theme import calculate_rotation_strength, CHILD
    child = _metric_row("CHILD_BAD", kind=CHILD, parent="PARENT_GOOD")
    child["tencent_main_net_inflow_cny"] = -1
    result = calculate_rotation_strength([_metric_row("PARENT_GOOD"), child], expected_trade_date=DAY)
    assert _board(result, "PARENT_GOOD")["selected_for_rotation"] is True
    assert _board(result, "CHILD_BAD")["selected_for_rotation"] is False


def test_old_pdf_extraction_version_does_not_hide_new_business_parser():
    from types import SimpleNamespace
    from liangjian_funnel.workflow import WorkflowApplication
    app = object.__new__(WorkflowApplication)
    # Rejected before accessing paths, making stale success a cache miss.
    assert app._cached_cninfo_pdf_evidence_from_record(SimpleNamespace(announcement_id="a"), {
        "payload": {"available": True, "announcement_id": "a"}
    }) is None


@pytest.mark.parametrize("usable,url_matches", [(True, True), (False, True), (True, False)])
def test_legacy_pdf_reuse_requires_current_business_evidence_and_same_url(tmp_path, usable, url_matches):
    from types import SimpleNamespace
    from liangjian_funnel.data.cninfo_pdf import CninfoPdfEvidence, PdfEvidenceSnippet
    from liangjian_funnel.workflow import WorkflowApplication
    url = "https://static.cninfo.com.cn/finalpage/2026-08-20/example.pdf"
    text = "利息净收入 客户贷款 公司银行 100亿元" if usable else "报告期 风险提示 公司治理"
    evidence = CninfoPdfEvidence(
        announcement_id="a", pdf_url=url, available=True, reason_code="OK", fetched_at=AS_OF,
        pdf_sha256="a" * 64, cache_relative_path="raw/example.pdf", page_count=1,
        extracted_chars=len(text), snippets=(PdfEvidenceSnippet(page_number=1, text=text),),
    )
    payload = evidence.model_dump(mode="json")
    payload.pop("extraction_version")
    app = object.__new__(WorkflowApplication)
    app.settings = SimpleNamespace(cninfo_pdf_cache_dir=tmp_path, cninfo_pdf_retain_raw=False)
    ann = SimpleNamespace(announcement_id="a", sec_code="000001",
                          pdf_url=url if url_matches else url.replace("example", "another"))
    result = app._cached_cninfo_pdf_evidence_from_record(ann, {"payload": payload})
    if usable and url_matches:
        assert result.cache_hit is True
        assert result.extraction_version == "legacy"
    else:
        assert result is None
