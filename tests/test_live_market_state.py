import json
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

import liangjian_funnel.runtime.live_market as live_market
from liangjian_funnel.runtime.live_market import (
    classify_full_market,
    classify_index_fallback,
    load_or_refresh_live_market_state,
    load_recent_ready_live_market_state,
)
from liangjian_funnel.workflow import _bounded_live_market_index_fallback


NOW = datetime(2026, 9, 3, 10, 5, tzinfo=ZoneInfo("Asia/Shanghai"))


def _rows(up: int, down: int, *, up_change: float = 1.0, down_change: float = -1.0):
    return ([{"price_change_ratio_pct": up_change}] * up) + (
        [{"price_change_ratio_pct": down_change}] * down
    )


def test_full_market_allows_normal_breadth() -> None:
    state = classify_full_market(_rows(700, 300), as_of=NOW, expected_count=1000)
    assert state["status"] == "READY"
    assert state["entry_permission"] == "ALLOW"
    assert state["trade_date"] == "2026-09-03"
    assert state["breadth"] == 0.7


def test_full_market_weak_rotation_is_caution_not_block() -> None:
    state = classify_full_market(_rows(400, 600), as_of=NOW, expected_count=1000)
    assert state["status"] == "READY"
    assert state["entry_permission"] == "CAUTION"
    assert state["suggested_position_cap_pct"] == 0.5


def test_full_market_only_blocks_confirmed_systemic_selloff() -> None:
    state = classify_full_market(
        _rows(100, 900, up_change=0.2, down_change=-2.5),
        as_of=NOW,
        expected_count=1000,
    )
    assert state["status"] == "READY"
    assert state["entry_permission"] == "BLOCK_NEW_ENTRY"
    assert state["reason_code"] == "A4_LIVE_SYSTEMIC_SELL_OFF"


def test_full_market_rejects_partial_coverage() -> None:
    state = classify_full_market(_rows(400, 100), as_of=NOW, expected_count=1000)
    assert state["status"] == "DATA_BLOCKED"
    assert state["entry_permission"] == "UNKNOWN"


def test_index_fallback_retains_degraded_quality_and_permission() -> None:
    quotes = [
        {"price": 101, "previous_close": 100},
        {"price": 100.5, "previous_close": 100},
        {"price": 99.8, "previous_close": 100},
    ]
    state = classify_index_fallback(quotes, as_of=NOW)
    assert state["status"] == "READY_DEGRADED"
    assert state["entry_permission"] == "ALLOW"
    assert state["breadth"] is None


def test_index_fallback_cannot_claim_readiness_without_three_indices() -> None:
    state = classify_index_fallback(
        [{"price": 101, "previous_close": 100}],
        as_of=NOW,
    )
    assert state["status"] == "DATA_BLOCKED"
    assert state["reason_code"] == "A4_LIVE_MARKET_SOURCE_UNAVAILABLE"


def test_bounded_index_fallback_is_independent_of_full_market_lane() -> None:
    calls: list[str] = []
    state = _bounded_live_market_index_fallback(
        _index_market(ready=True, calls=calls),
        current=NOW,
        deadline=__import__('time').monotonic() + 1.0,
    )
    assert state["status"] == "READY_DEGRADED"
    assert state["source"] == "TENCENT_INDEX_FALLBACK"
    assert state["entry_permission"] == "ALLOW"
    assert sorted(calls) == sorted(("000001.SH", "399001.SZ", "399006.SZ", "000300.SH"))
    assert all(item["status"] == "READY" for item in state["diagnostics"]["index_quotes"])


def test_recent_ready_bucket_bridges_only_within_existing_five_minute_sla(tmp_path) -> None:
    settings = SimpleNamespace(fact_store_dir=Path(tmp_path))
    root = Path(tmp_path) / "a4_live_market" / NOW.date().isoformat()
    root.mkdir(parents=True)
    prior = classify_full_market(_rows(700, 300), as_of=NOW.replace(minute=0), expected_count=1000)
    prior["cache_bucket"] = NOW.replace(minute=0).isoformat()
    (root / "1000.json").write_text(json.dumps(prior), encoding="utf-8")

    bridged = load_recent_ready_live_market_state(settings, as_of=NOW)
    expired = load_recent_ready_live_market_state(settings, as_of=NOW.replace(minute=6))

    assert bridged is not None
    assert bridged["status"] == "READY_DEGRADED"
    assert bridged["cache_reused_previous_bucket"] is True
    assert bridged["diagnostics"]["cache_age_seconds"] == 300.0
    assert expired is None


class _Row:
    def __init__(self, change: float = 1.0):
        self.change = change

    def model_dump(self, mode: str = "json") -> dict[str, float]:
        del mode
        return {"price_change_ratio_pct": self.change}


class _Quote:
    def model_dump(self, mode: str = "json") -> dict[str, float]:
        del mode
        return {"price": 101.0, "previous_close": 100.0}


class _FullMarketClient:
    def __init__(self, result: object, calls: list[int]):
        self.result = result
        self.calls = calls

    def __enter__(self):
        self.calls.append(1)
        return self

    def __exit__(self, *_args):
        return None

    def market_snapshot(self, **_kwargs):
        return self.result


def test_opt_in_team_observer_consumes_same_result_and_cache_without_second_request(tmp_path,monkeypatch):
    original=_full_result(ready=True);calls=[];seen=[]
    monkeypatch.setattr(live_market,'HithinkClient',lambda _: _FullMarketClient(original,calls))
    def observe(result):
        seen.append(result)
        return {'status':'READY','source_as_of':'PROVIDER_ORIGINAL_NOT_COLLECTION_CLOCK'}
    settings=SimpleNamespace(fact_store_dir=tmp_path)
    first=load_or_refresh_live_market_state(settings,_index_market(ready=False,calls=[]),
        as_of=NOW,snapshot_observer=observe)
    second=load_or_refresh_live_market_state(settings,_index_market(ready=False,calls=[]),
        as_of=NOW,snapshot_observer=observe)
    assert calls==[1] and seen==[original] and first['team_v2_source']==second['team_v2_source']


def test_observer_exception_does_not_damage_existing_primary_market_contract(tmp_path,monkeypatch):
    calls=[];monkeypatch.setattr(live_market,'HithinkClient',lambda _: _FullMarketClient(_full_result(ready=True),calls))
    def broken(result):raise ValueError('PRIVATE_DO_NOT_PERSIST')
    actual=load_or_refresh_live_market_state(SimpleNamespace(fact_store_dir=tmp_path),
        _index_market(ready=False,calls=[]),as_of=NOW,snapshot_observer=broken)
    assert actual['status']=='READY' and actual['fallback'] is False
    assert actual['team_v2_source']['reason_code']=='TEAM_LIVE_SNAPSHOT_OBSERVER_FAILED'
    assert 'PRIVATE_DO_NOT_PERSIST' not in json.dumps(actual)


@pytest.mark.parametrize('capture_fails', [False, True])
def test_dual_capture_does_not_inject_team_fields_into_legacy_market(tmp_path,monkeypatch,capture_fails):
    original=_full_result(ready=True);calls=[];seen=[]
    monkeypatch.setattr(live_market,'HithinkClient',lambda _: _FullMarketClient(original,calls))
    def capture(value):
        seen.append(value)
        if capture_fails:raise ValueError('PRIVATE_CAPTURE_FAILURE')
    result=load_or_refresh_live_market_state(SimpleNamespace(fact_store_dir=tmp_path),
        _index_market(ready=False,calls=[]),as_of=NOW,snapshot_capture=capture)
    assert result['status']=='READY' and result['entry_permission']=='ALLOW'
    assert 'team_v2_source' not in result and 'PRIVATE' not in json.dumps(result)
    assert calls==[1] and seen==[original]


def test_team_request_requires_new_provider_bucket_not_just_ready_cached_collection_clock(tmp_path,monkeypatch):
    calls=[];monkeypatch.setattr(live_market,'HithinkClient',lambda _: _FullMarketClient(_full_result(ready=True),calls))
    settings=SimpleNamespace(fact_store_dir=tmp_path)
    first=load_or_refresh_live_market_state(settings,_index_market(ready=False,calls=[]),as_of=NOW,
        snapshot_observer=lambda _: {'source_as_of':NOW.replace(minute=4).isoformat()})
    second=load_or_refresh_live_market_state(settings,_index_market(ready=False,calls=[]),as_of=NOW,
        minimum_source_as_of=NOW,snapshot_observer=lambda _: {'source_as_of':NOW.isoformat()})
    assert calls==[1,1] and first['team_v2_source']!=second['team_v2_source']


def _full_result(*, ready: bool) -> SimpleNamespace:
    return SimpleNamespace(
        ok=ready,
        complete=ready,
        reason_code="OK" if ready else "REQUEST_FAILED",
        items=tuple(_Row(1.0) for _ in range(1_000 if ready else 0)),
        total=1_000,
    )


def _index_market(*, ready: bool, calls: list[str]):
    class Market:
        def fetch_quote(self, symbol, *, as_of, max_age_seconds):
            del as_of, max_age_seconds
            calls.append(symbol)
            return SimpleNamespace(
                complete=ready,
                quote=_Quote() if ready else None,
                reason_code="OK" if ready else "REQUEST_FAILED",
            )

    return Market()


def test_live_market_retries_full_market_then_recovers_without_real_sleep(tmp_path, monkeypatch):
    full_calls: list[int] = []
    results = iter((_full_result(ready=False), _full_result(ready=True)))
    monkeypatch.setattr(
        live_market,
        "HithinkClient",
        lambda _settings: _FullMarketClient(next(results), full_calls),
    )
    index_calls: list[str] = []
    sleeps: list[float] = []
    settings = SimpleNamespace(fact_store_dir=Path(tmp_path))

    state = load_or_refresh_live_market_state(
        settings,
        _index_market(ready=False, calls=index_calls),
        as_of=NOW,
        sleep=sleeps.append,
    )

    assert state["status"] == "READY"
    assert state["source"] == "HITHINK_FULL_MARKET"
    assert full_calls == [1, 1]
    assert len(index_calls) == 4
    assert sleeps == [pytest.approx(0.5)]
    assert state["diagnostics"]["total_attempts"] == 2
    assert state["diagnostics"]["recovered_after_retry"] is True
    assert len(state["diagnostics"]["attempts"]) == 2
    assert state["diagnostics"]["attempts"][0]["full_market"]["status"] == "DATA_BLOCKED"
    assert state["diagnostics"]["attempts"][1]["full_market"]["status"] == "READY"


def test_live_market_exhaustion_stays_blocked_and_blocked_cache_is_not_reused(tmp_path, monkeypatch):
    full_calls: list[int] = []
    monkeypatch.setattr(
        live_market,
        "HithinkClient",
        lambda _settings: _FullMarketClient(_full_result(ready=False), full_calls),
    )
    index_calls: list[str] = []
    settings = SimpleNamespace(fact_store_dir=Path(tmp_path))
    sleeps: list[float] = []

    first = load_or_refresh_live_market_state(
        settings,
        _index_market(ready=False, calls=index_calls),
        as_of=NOW,
        sleep=sleeps.append,
    )
    second = load_or_refresh_live_market_state(
        settings,
        _index_market(ready=False, calls=index_calls),
        as_of=NOW,
        sleep=sleeps.append,
    )

    assert first["status"] == second["status"] == "DATA_BLOCKED"
    assert first["diagnostics"]["total_attempts"] == 2
    assert second["diagnostics"]["total_attempts"] == 2
    # The second invocation collected again instead of reusing the blocked
    # five-minute file.
    assert len(full_calls) == 4
    assert len(index_calls) == 16
    assert len(sleeps) == 2
    path = Path(tmp_path) / "a4_live_market" / "2026-09-03" / "1005.json"
    assert json.loads(path.read_text(encoding="utf-8"))["status"] == "DATA_BLOCKED"


def test_live_market_ready_cache_is_reused_and_future_cache_is_rejected(tmp_path, monkeypatch):
    full_calls: list[int] = []
    monkeypatch.setattr(
        live_market,
        "HithinkClient",
        lambda _settings: _FullMarketClient(_full_result(ready=True), full_calls),
    )
    settings = SimpleNamespace(fact_store_dir=Path(tmp_path))
    market = _index_market(ready=False, calls=[])
    first = load_or_refresh_live_market_state(settings, market, as_of=NOW, sleep=lambda _delay: None)

    future_calls: list[int] = []

    class FailClient:
        def __enter__(self):
            future_calls.append(1)
            return self

        def __exit__(self, *_args):
            return None

        def market_snapshot(self, **_kwargs):
            return _full_result(ready=False)

    monkeypatch.setattr(live_market, "HithinkClient", lambda _settings: FailClient())
    cached = load_or_refresh_live_market_state(settings, market, as_of=NOW, sleep=lambda _delay: None)
    assert first["status"] == cached["status"] == "READY"
    assert cached["cache_hit"] is True
    assert cached["cache_reused"] is True
    assert cached["diagnostics"]["cache_hit"] is True
    assert len(full_calls) == 1

    # A future observation in the same bucket cannot be used for an earlier
    # decision tick; the call therefore attempts fresh collection.
    future_path = Path(tmp_path) / "a4_live_market" / "2026-09-03" / "1005.json"
    future = dict(first)
    future["as_of"] = "2026-09-03T10:06:00+08:00"
    future_path.write_text(json.dumps(future, ensure_ascii=False), encoding="utf-8")
    refreshed = load_or_refresh_live_market_state(
        settings,
        market,
        as_of=NOW,
        sleep=lambda _delay: None,
    )
    assert refreshed["status"] == "DATA_BLOCKED"
    assert len(future_calls) == 2
