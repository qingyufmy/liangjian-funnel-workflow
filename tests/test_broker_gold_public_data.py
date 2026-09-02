from __future__ import annotations

import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from liangjian_funnel.evaluation.broker_gold import import_broker_gold


DATASET = Path(__file__).resolve().parents[1] / "storage" / "benchmarks" / "broker_gold" / "2026-09.json"
CUTOFF = datetime(2026, 9, 2, 12, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
SYMBOL_RE = re.compile(r"^\d{6}\.(?:SH|SZ|BJ)$")


def test_september_2026_public_broker_gold_dataset_is_strict_and_traceable() -> None:
    dataset = import_broker_gold(DATASET, as_of=CUTOFF)
    records = dataset.records
    symbols = {record.symbol for record in records}

    assert dataset.months == ("2026-09",)
    assert len({record.broker for record in records}) == 16
    assert len(records) == 142
    assert len(symbols) == 117
    assert Counter(record.symbol for record in records)["603259.SH"] == 3
    assert Counter(record.symbol for record in records)["300750.SZ"] == 4
    assert "920403.BJ" in symbols
    assert all(record.name for record in records)
    assert all(SYMBOL_RE.fullmatch(record.symbol) for record in records)
    assert all(record.source_ref.startswith(("http://", "https://")) for record in records)
    assert all(record.publish_time is not None and record.publish_time <= CUTOFF for record in records)
