"""Bounded six-request local financial sidecar probe; no production writes."""
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

from liangjian_funnel.data.sina_finance_shadow import normalize_finance_report
from liangjian_funnel.facts.store import FactStore


def main():
    root = Path(__file__).resolve().parents[1] / "artifacts/source_audit"
    store = FactStore(root)
    summaries = []
    with httpx.Client(timeout=15, follow_redirects=False) as client:
        for symbol in ("600519.SH", "000001.SZ"):
            for statement in ("lrb", "fzb", "llb"):
                response = client.get(
                    "https://quotes.sina.cn/cn/api/openapi.php/CompanyFinanceService.getFinanceReport2022",
                    params=dict(paperCode=symbol[-2:].lower()+symbol[:6], source=statement, type=0, page=1, num=2))
                response.raise_for_status()
                raw = response.json()
                observed = datetime.now(timezone.utc).isoformat()
                path = root / f"sina-{symbol}-{statement}-{observed.replace(':', '').replace('+', '_')}.json"
                store.write_json(path, dict(raw=raw, fetched_at=observed, requested_symbol=symbol, statement=statement))
                normalized = normalize_finance_report(raw, symbol=symbol, statement=statement)
                store.write_json(path.with_suffix('.normalized.json'), normalized)
                summaries.append(dict(symbol=symbol, statement=statement, periods=[
                    dict(period=row["report_period"], published=row["publish_date"], fields=len(row["fields"]))
                    for row in normalized["rows"]]))
                time.sleep(1)
    store.write_json(root / "sina-six-request-summary.json", summaries)
    print(json.dumps(summaries, ensure_ascii=False))


if __name__ == "__main__":
    main()
