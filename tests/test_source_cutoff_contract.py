"""Explicit PIT metadata fixtures, never inferred from mtime or quote proxies."""
from copy import deepcopy
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.data.source_cutoff import SourceCutoffs, inspect_source_cutoff

TZ = ZoneInfo("Asia/Shanghai")


def at(hour, minute=0, day=9):
    return datetime(2026, 10, day, hour, minute, tzinfo=TZ)


def entry(**overrides):
    return {"cutoff_contract": "source-dual-cutoff/1", "source_id": "VENDOR",
            "trade_date": "2026-10-09", "market_observed_at": at(9, 25).isoformat(),
            "ingested_at": at(9, 26).isoformat(), "observation_basis": "PROVIDER_EVENT_TIMESTAMP",
            "end_of_session_semantics": False, **overrides}


def inspect(value, market=None, sealed=None):
    return inspect_source_cutoff(value, cutoffs=SourceCutoffs(market or at(9, 25), sealed or at(9, 27)),
                                 expected_source_id="VENDOR", expected_trade_date="2026-10-09")


def test_own_run_received_before_seal_is_allowed_and_input_unchanged():
    value = entry()
    before = deepcopy(value)
    result = inspect(value)
    assert result["status"] == "CUTOFF_BOUND"
    assert result["usable"] is True
    assert result["acquisition_authenticated"] is False
    assert value == before


@pytest.mark.parametrize("field,value", [("market_observed_at", at(16).isoformat()),
                                       ("ingested_at", at(9, 28).isoformat())])
def test_each_future_bound_blocks_before_freshness_checks(field, value):
    result = inspect(entry(**{field: value}))
    assert result["usable"] is False
    assert result["reason_code"] == "SOURCE_CACHE_FROM_FUTURE"
    assert field in result["violated_fields"]


def test_original_replay_seal_cannot_be_replaced_with_wall_clock():
    value = entry(ingested_at=at(9, 28).isoformat())
    assert not inspect(value, sealed=at(9, 27))["usable"]
    assert inspect(value, sealed=at(9, 30))["usable"]


@pytest.mark.parametrize("missing", ["market_observed_at", "ingested_at", "cutoff_contract"])
def test_legacy_metadata_is_limited_not_repaired(missing):
    value = entry()
    del value[missing]
    value["mtime"] = at(9, 24).isoformat()
    result = inspect(value)
    assert result["status"] == "LEGACY_UNVERSIONED"
    assert result["usable"] is False


def test_closed_session_observation_after_close_needs_explicit_semantics():
    value = entry(market_observed_at=at(16).isoformat(), ingested_at=at(16, 1).isoformat(),
                  observation_basis="CLOSED_SESSION_VALUE", end_of_session_semantics=True)
    result = inspect(value, market=at(15), sealed=at(16, 2))
    assert result["usable"] is True
    assert result["effective_market_observed_at"] == at(15).isoformat()
    assert result["declared_market_observed_at"] == at(16).isoformat()
    value["end_of_session_semantics"] = False
    assert not inspect(value, market=at(15), sealed=at(16, 2))["usable"]


def test_intraday_no_provider_timestamp_cannot_use_end_of_session_exception():
    value = entry(market_observed_at=at(9, 26).isoformat(),
                  observation_basis="CLOSED_SESSION_VALUE", end_of_session_semantics=True)
    result = inspect(value)
    assert result["usable"] is False
    assert result["reason_code"] == "OBSERVED_AT_UNPROVEN"


@pytest.mark.parametrize("basis", ["ACQUISITION_END", "QUOTE_DATE_ONLY", "REQUEST_AS_OF", "UNKNOWN"])
def test_request_timestamp_or_paired_quote_date_does_not_prove_flow_event_time(basis):
    assert inspect(entry(observation_basis=basis))["reason_code"] == "OBSERVED_AT_UNPROVEN"


@pytest.mark.parametrize("field,value", [("source_id", "OTHER"), ("trade_date", "2026-10-08"),
                                       ("market_observed_at", "2026-10-09T09:25:00"),
                                       ("ingested_at", "invalid"), ("end_of_session_semantics", 1)])
def test_malformed_or_wrong_identity_is_not_time_bound(field, value):
    assert inspect(entry(**{field: value}))["usable"] is False


def test_provider_future_timestamp_cannot_be_hidden_under_eod_flag():
    value = entry(market_observed_at=at(16).isoformat(), ingested_at=at(16, 1).isoformat(),
                  observation_basis="PROVIDER_EVENT_TIMESTAMP", end_of_session_semantics=True)
    assert inspect(value, market=at(15), sealed=at(16, 2))["reason_code"] == "SOURCE_CACHE_FROM_FUTURE"


def test_cross_session_closed_value_and_received_before_market_time_are_invalid():
    value = entry(market_observed_at=at(16, day=10).isoformat(), ingested_at=at(16, 1, day=10).isoformat(),
                  observation_basis="CLOSED_SESSION_VALUE", end_of_session_semantics=True)
    assert not inspect(value, market=at(15), sealed=at(16, 2, day=10))["usable"]
    assert not inspect(entry(ingested_at=at(9, 24).isoformat()))["usable"]


def test_cutoff_requires_aware_timestamps():
    with pytest.raises(ValueError):
        SourceCutoffs(at(9, 25).replace(tzinfo=None), at(9, 27))


def test_provider_timestamp_previous_day_cannot_be_relabelled_by_cache_filename():
    result = inspect(entry(market_observed_at=at(15, day=8).isoformat()))
    assert result["usable"] is False
    assert result["reason_code"] == "SOURCE_OBSERVATION_TRADE_DATE_MISMATCH"


@pytest.mark.parametrize("source", [None, "", " ", False, 1])
def test_missing_or_invalid_expected_identity_cannot_authenticate_itself(source):
    result = inspect_source_cutoff(entry(source_id=source),
        cutoffs=SourceCutoffs(at(9, 25), at(9, 27)),
        expected_source_id=source, expected_trade_date="2026-10-09")
    assert result["usable"] is False
    assert result["reason_code"] == "SOURCE_EXPECTED_ID_REQUIRED"


def stock_cache(tmp_path, **overrides):
    from liangjian_funnel.data import a2_market
    value = a2_market.build_capital_flow_snapshot({"today": [
        {"symbol": "600000.SH", "net_inflow_ratio": 2, "net_inflow_amount": 100}]},
        as_of=at(9, 25), expected_symbols=["600000.SH"], ingested_at=at(9, 26), source_id="VENDOR")
    value.update(entry(**overrides))
    value.pop("content_hash")
    value["content_hash"] = a2_market._content_hash(value)
    a2_market.write_capital_flow_snapshot(tmp_path, value)
    return value


def test_stock_cache_reader_applies_cutoff_before_optional_ttl_without_rewriting(tmp_path):
    from liangjian_funnel.data import a2_market
    value = stock_cache(tmp_path, ingested_at=at(16).isoformat())
    path = tmp_path / "capital-flow-2026-10-09.json"
    original = path.read_bytes()
    payload, state = a2_market._load_capital_flow_cache_state(tmp_path, "2026-10-09",
        now=at(17), max_age_seconds=0, cutoffs=SourceCutoffs(at(9, 25), at(9, 27)),
        expected_source_id="VENDOR")
    assert payload is None and state == "SOURCE_CACHE_FROM_FUTURE"
    assert path.read_bytes() == original
    # Legacy callers keep their existing behavior; no production switch.
    assert a2_market.load_capital_flow_snapshot(tmp_path, "2026-10-09") == value


def test_stock_cache_reader_never_relabels_wrong_provider_as_tencent(tmp_path):
    from liangjian_funnel.data import a2_market
    stock_cache(tmp_path)
    payload, state = a2_market._load_capital_flow_cache_state(tmp_path, "2026-10-09",
        cutoffs=SourceCutoffs(at(9, 25), at(9, 27)),
        expected_source_id=a2_market.TENCENT_CAPITAL_FLOW_PROVIDER)
    assert payload is None and state == "SOURCE_IDENTITY_MISMATCH"


def test_explicit_reader_requires_expected_source_and_public_loader_forwards_contract(tmp_path):
    from liangjian_funnel.data import a2_market
    value = stock_cache(tmp_path)
    context = SourceCutoffs(at(9, 25), at(9, 27))
    with pytest.raises(ValueError, match="SOURCE_EXPECTED_ID_REQUIRED"):
        a2_market.load_capital_flow_snapshot(tmp_path, "2026-10-09", cutoffs=context)
    assert a2_market.load_capital_flow_snapshot(tmp_path, "2026-10-09", cutoffs=context,
                                               expected_source_id="VENDOR") == value


def test_explicit_reader_does_not_accept_legacy_source_by_file_mtime(tmp_path):
    from liangjian_funnel.data import a2_market
    value = stock_cache(tmp_path)
    del value["market_observed_at"]
    value.pop("content_hash")
    value["content_hash"] = a2_market._content_hash(value)
    a2_market.write_capital_flow_snapshot(tmp_path, value)
    payload, state = a2_market._load_capital_flow_cache_state(tmp_path, "2026-10-09",
        cutoffs=SourceCutoffs(at(9, 25), at(9, 27)), expected_source_id="VENDOR")
    assert payload is None and state == "LEGACY_UNVERSIONED"


def test_board_cache_reader_uses_two_cutoffs_before_ttl(tmp_path):
    from liangjian_funnel.data import a2_market
    value = a2_market.build_board_capital_flow_snapshot([
        {"code": "BK0475", "name": "银行", "rank": 1, "main_net_cny": 100}],
        as_of=at(9, 25), board_type="industry", period="today", ingested_at=at(9, 26))
    value.update(entry(source_id=a2_market.BOARD_FLOW_PROVIDER, ingested_at=at(16).isoformat()))
    value.pop("content_hash")
    value["content_hash"] = a2_market._content_hash(value)
    a2_market.write_board_capital_flow_snapshot(tmp_path, value)
    result = a2_market.inspect_board_capital_flow_snapshot(tmp_path, "industry", "today", "2026-10-09",
        now=at(17), max_age_seconds=0, cutoffs=SourceCutoffs(at(9, 25), at(9, 27)))
    assert result["available"] is False
    assert result["reason_code"] == "SOURCE_CACHE_FROM_FUTURE"
    assert result["snapshot"] is None
    assert a2_market.load_board_capital_flow_snapshot(tmp_path, "industry", "today", "2026-10-09",
        cutoffs=SourceCutoffs(at(9, 25), at(9, 27))) is None
