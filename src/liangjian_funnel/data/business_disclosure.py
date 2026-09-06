"""Recognize source business disclosures, never infer revenue percentages."""
from __future__ import annotations

import re


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
    if "利息净收入" in compact and any(word in compact for word in ("客户贷款", "发放贷款", "存贷款", "公司银行", "零售银行")):
        return "BANK_BUSINESS_DISCLOSURE"
    if any(word in compact for word in ("证券经纪业务", "投资银行业务", "资产管理业务")) and any(
        word in compact for word in ("营业收入", "手续费及佣金净收入", "手续费净收入")
    ):
        return "SECURITIES_BUSINESS_DISCLOSURE"
    return None
