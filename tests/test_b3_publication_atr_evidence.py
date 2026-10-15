from copy import deepcopy
from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.pipeline.local_fact_cache import canonical_json_hash
from liangjian_funnel.runtime.publication_indicators import publication_atr_evidence

TZ = ZoneInfo("Asia/Shanghai")
AT = datetime(2026, 10, 15, 15, 10, tzinfo=TZ)


def rows():
    result = []
    for offset in range(80):
        day = AT.date() - timedelta(days=79-offset)
        payload = {"symbol":"600000.SH", "date":day.isoformat(), "open":10.,
            "high":12. if offset < 20 else 10.2, "low":9.8, "close":10.,
            "volume":100., "amount":1000., "closed":True, "adjust":"none"}
        result.append({"symbol":"600000.SH", "timestamp":f"{day}T15:00:00+08:00",
            "adjust":"none", "fetched_at":AT.isoformat(),
            "content_hash":canonical_json_hash(payload), "payload":payload})
    return result


class Cache:
    def __init__(self, values): self.values, self.calls = values, []
    def query_daily_bars(self, symbol, **kwargs):
        self.calls.append((symbol,kwargs))
        return self.values


def test_wilder_uses_all_pit_history_not_a_late_fourteen_bar_mean():
    values = rows()
    cache = Cache(values)
    original = deepcopy(values)
    result = publication_atr_evidence(cache=cache, symbol="600000.SH", knowledge_at=AT,
        last_closed_at=AT.replace(hour=15,minute=0), captured_at=AT)
    assert result["status"] == "AVAILABLE"
    assert result["source_rows_count"] == 80
    assert result["atr14"] > .4
    assert result["atr14"] == pytest.approx(2.2*(13/14)**60 + .4*(1-(13/14)**60))
    assert result["knowledge_at"] == AT.isoformat()
    assert result["last_closed_daily_bar_end"] == "2026-10-15T15:00:00+08:00"
    assert result["listing_history_completeness"] == "NOT_CLAIMED"
    assert result["execution_authority"] is False
    assert cache.calls[0][1]["limit"] is None
    assert cache.calls[0][1]["as_of"] == AT
    assert values == original


@pytest.mark.parametrize("mutation", ["future", "hash", "symbol", "adjust", "forming", "short", "old"])
def test_invalid_or_partial_source_is_not_promoted_to_complete_atr(mutation):
    values = rows()
    if mutation == "future": values[-1]["fetched_at"] = (AT+timedelta(seconds=1)).isoformat()
    if mutation == "hash": values[-1]["content_hash"] = "f"*64
    if mutation == "symbol": values[-1]["symbol"] = "000001.SZ"
    if mutation == "adjust": values[-1]["adjust"] = "qfq"
    if mutation == "forming":
        values[-1]["payload"]["closed"] = False
        values[-1]["content_hash"] = canonical_json_hash(values[-1]["payload"])
    if mutation == "short": values = values[-14:]
    if mutation == "old": values = values[:-1]
    result = publication_atr_evidence(cache=Cache(values), symbol="600000.SH", knowledge_at=AT,
        last_closed_at=AT.replace(hour=15,minute=0), captured_at=AT)
    assert result["status"] == "DATA_LIMITED"
    assert result["atr14"] is None


def test_real_publisher_pins_evidence_without_changing_model_output_or_execution_lines(monkeypatch):
    from test_w2_shadow_inputs_publication import publisher_fixture, AT as RESEARCH_AT, TARGET
    import liangjian_funnel.runtime.publication_indicators as indicators
    from liangjian_funnel.workflow import WorkflowApplication
    app, result, data, raw = publisher_fixture()
    original = deepcopy(raw)
    evidence = {"schema_version":"publication-atr-evidence/1", "status":"AVAILABLE",
        "atr14":1.234, "execution_authority":False, "test_fixture":True}
    monkeypatch.setattr(indicators, "publication_atr_evidence", lambda **kwargs:deepcopy(evidence))
    publication = WorkflowApplication._publish_plans(app,result,"close",RESEARCH_AT,
        snapshot_data=data,minimum_trade_date=TARGET)
    assert len(publication["created"]) == 1
    assert app.store.batch[0]["payload"]["strategy_facts"]["shadow_inputs"]["atr14_evidence"] == evidence
    for key in ("trigger_zone","stop_level","no_chase_price","invalidation_level"):
        if key in original:
            assert raw[key] == original[key]
    assert raw == original
