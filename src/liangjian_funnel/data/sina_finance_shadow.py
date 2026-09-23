"""Sina statement sidecar: values are leads/cross-checks, not A1 admission evidence."""
from datetime import datetime
from decimal import Decimal, InvalidOperation
from collections.abc import Mapping


def normalize_finance_report(body: dict, *, symbol: str, statement: str) -> dict:
    if statement not in {"lrb", "fzb", "llb"}:
        raise ValueError("unknown statement")
    if len(symbol) != 9 or symbol[6:] not in {".SH", ".SZ", ".BJ"} or not symbol[:6].isascii() or not symbol[:6].isdigit():
        raise ValueError("invalid symbol")
    result = body.get("result")
    status = result.get("status") if isinstance(result, Mapping) else None
    if not isinstance(status, Mapping) or type(status.get("code")) is not int or status["code"] != 0:
        raise ValueError("financial provider business failure")
    data = result.get("data")
    if not isinstance(data, Mapping) or not isinstance(data.get("report_list"), Mapping):
        raise ValueError("financial provider schema invalid")
    rows = []
    for period, report in data["report_list"].items():
        period_date = datetime.strptime(period, "%Y%m%d").date()
        if not isinstance(report, Mapping) or not isinstance(report.get("data"), list):
            raise ValueError("financial report schema invalid")
        published = report.get("publish_date")
        publish_date = datetime.strptime(published, "%Y%m%d").date() if published else None
        if publish_date and publish_date < period_date:
            raise ValueError("financial publication precedes report period")
        fields = {}
        headings = []
        original_items = {}
        duplicate_count = 0
        for item in report["data"]:
            if not isinstance(item, Mapping):
                raise ValueError("financial item invalid")
            key = item.get("item_field")
            if key == "" and item.get("item_value") in (None, "") and item.get("item_title"):
                headings.append(dict(title=item["item_title"], group=item.get("item_group_no")))
                continue
            original_key = key
            if isinstance(key, str) and key and item.get("item_group_no") is not None:
                key = f"{item['item_group_no']}:{key}"
            if key in original_items and original_items[key] == item:
                duplicate_count += 1
                continue
            if not isinstance(key, str) or not key or key in fields:
                raise ValueError("financial field missing or duplicated")
            original_items[key] = dict(item)
            value = item.get("item_value")
            if value not in (None, "", "--"):
                try:
                    if not Decimal(str(value)).is_finite():
                        raise ValueError("financial value nonfinite")
                except InvalidOperation as exc:
                    raise ValueError("financial numeric value invalid") from exc
            fields[key] = dict(value=None if value in (None, "", "--") else str(value),
                               title=item.get("item_title"), source_field=original_key,
                               group=item.get("item_group_no"), unit_verified=False)
        rows.append(dict(report_period=period_date.isoformat(),
                         publish_date=publish_date.isoformat() if publish_date else None,
                         provider_update_time=report.get("update_time"),
                         currency=report.get("rCurrency"), fields=fields, headings=headings,
                         exact_duplicate_count=duplicate_count, raw_item_count=len(report["data"])))
    return dict(symbol=symbol, statement=statement, rows=rows,
                shadow_only=True, execution_authority=False, feature_ready=False,
                historical_availability_verified=False,
                reason="PROVIDER_STATEMENT_REQUIRES_ORIGINAL_DISCLOSURE_AND_UNIT_RECONCILIATION")
