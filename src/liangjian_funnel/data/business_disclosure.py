"""Recognize source business disclosures, never infer revenue percentages."""
from __future__ import annotations

import re

BUSINESS_DESCRIPTION_PATTERN = (
    r"(?<![向为子])(?:本公司|公司|本集团|集团|发行人|本行)(?:的)?"
    r"(?:目前|主要|主营|报告期内|的主营业务|的主要业务){0,3}"
    r"(?:从事|(?:向客户|为客户)提供|是一家|主营的|为|业务为|业务包括|业务主要包括|业务主要为|业务主要是)"
)


def financial_business_kind(text: str) -> str | None:
    compact = re.sub(r"\s+", "", text)
    # Cash receipts, balance-sheet assets and a table of contents are not
    # segment revenue evidence, even when they contain insurance keywords.
    if any(word in compact for word in (
        "现金流量表", "收到签发保险合同", "支付原保险合同", "目录",
        "该准则", "会计政策变更", "规范保险服务收入确认", "保险合同的确认、计量",
    )):
        return None
    if any(word in compact for word in ("保险服务收入", "原保险保费收入", "保险业务收入")) and any(
        word in compact for word in ("险种", "保险合同", "合同服务边际", "承保", "寿险", "健康险", "财产险", "车险")
    ):
        return "INSURANCE_BUSINESS_DISCLOSURE"
    if any(word in compact for word in ("利息净收入", "净利息收入")) and any(word in compact for word in ("客户贷款", "发放贷款", "存贷款", "公司银行", "零售银行")) and not any(
        word in compact for word in ("除以", "之比", "的比率", "计算公式")
    ):
        return "BANK_BUSINESS_DISCLOSURE"
    if any(word in compact for word in ("证券经纪业务", "投资银行业务", "资产管理业务")) and any(
        word in compact for word in ("营业收入", "手续费及佣金净收入", "手续费净收入")
    ):
        return "SECURITIES_BUSINESS_DISCLOSURE"
    return None


def business_disclosure_kind(text: str) -> str | None:
    """One shared extraction/compaction/projection contract for business proof.

    A company-wide income statement is not a product/industry breakdown.
    Descriptions may prove the business, but never an invented revenue share.
    """
    compact = re.sub(r"\s+", "", text)
    if any(term in compact for term in ("目录", "会计政策变更", "该准则", "现金流量表")):
        return None
    if kind := financial_business_kind(text):
        return kind
    axes = (
        "分行业", "分产品", "分地区", "分业务", "分部信息", "业务分部", "业务板块",
        "营业收入构成", "收入构成", "产品或服务", "产品名称", "业务类型", "业务类别",
        "主营业务分", "占营业收入的",
    )
    if not re.search(r"(?:参见|参阅|详见).{0,30}(?:分部|附注|报告)", compact) and any(axis in compact for axis in axes) and any(
        metric in compact for metric in ("营业收入", "收入", "营业成本", "毛利率", "销售额")
    ) and re.search(r"\d", compact):
        return "SEGMENT_REVENUE_DISCLOSURE"
    for match in re.finditer(r"([\u4e00-\u9fff]{2,20})(?:业务|产品)(?:收入|营业收入)(?:为|实现|达)?[\d,，.]", compact):
        if not match.group(1).endswith(("主营", "其他", "其它", "全部", "各项")):
            return "BUSINESS_REVENUE_NARRATIVE"
    # Require an actual activity following a company-scoped statement, not
    # a section title, an unchanged-business notice or a forward reference.
    for match in re.finditer(BUSINESS_DESCRIPTION_PATTERN, compact):
        tail = re.split(r"[。；]", compact[match.end():match.end()+220])[0]
        if tail.startswith(("的主要业务", "主要业务", "业务情况")):
            continue
        if len(tail) >= 8 and not any(t in tail for t in ("参见", "详见", "未发生", "没有发生")) and any(
            activity in tail for activity in ("研发", "生产", "销售", "制造", "提供", "服务", "开发", "经营", "运营", "开采", "建设", "加工", "设计", "租赁", "运输", "种植", "养殖")
        ):
            return "COMPANY_BUSINESS_DESCRIPTION"
    return None
