"""Local bytes/clock fixtures only; no provider, credentials or production IO."""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta
import hashlib
import json

import pytest

from liangjian_funnel.data.tencent_minute import TencentIntradayAdapter
from liangjian_funnel.evaluation.ablation.price_limits import RULE_REVIEWED_THROUGH
from liangjian_funnel.runtime.shadow_pit_capture import build_plan_pit_capture
from liangjian_funnel.runtime.shadow_pit_sources import (
    JSONFieldSource, RawReceiptArchive, RecordingTextFetcher, SourceEvidenceError,
    TickerCatalogPackage, build_tencent_pit_source, record_raw_response,
)

SYMBOL = "600001.SH"


def at(time="09:26:00", day="2026-10-09"):
    return datetime.fromisoformat(f"{day}T{time}+08:00")


def raw_quote(*, name="测试证券", preclose="10.00", symbol="sh600001", stamp="20261009092559"):
    fields = [""] * 85
    fields[1], fields[2] = name, symbol[2:]
    fields[3], fields[4], fields[5], fields[6] = "10.01", preclose, "10.01", "100"
    fields[30], fields[37] = stamp, "1"
    fields[47], fields[48] = "999", "0.01"  # Never guess these are limit fields.
    return f'v_{symbol}="{"~".join(fields)}";\n'.encode("gbk")


def receipt(raw=None, *, endpoint="https://qt.gtimg.cn/q", params=None, received=None, **kw):
    return record_raw_response(raw if raw is not None else raw_quote(), source_ref="fixture:raw-response",
        endpoint=endpoint, request_parameters=params if params is not None else {"q": "sh600001"},
        request_started_at=(received or at())-timedelta(seconds=1), response_received_at=received or at(),
        byte_kind="CONSUMER_INPUT_BYTES", **kw)


def catalog(*, day="2026-10-09", complete=True, rows=None, total=None):
    row = dict(symbol=SYMBOL, listing_date="2020-01-02", board="SSE_MAIN", security_type="CASH_A_SHARE",
               security_status="ORDINARY", is_st=False, limit_regime="NORMAL")
    rows = rows if rows is not None else [row]
    body = {"code": 0, "data": {"items": rows, "total": len(rows) if total is None else total}}
    page = receipt(json.dumps(body).encode(), endpoint="/api/meta/tickers/list",
                   params={"exchange": "SH,SZ,BJ", "asset_type": "a-share", "limit": 1000, "offset": 0},
                   received=at(day=day))
    return TickerCatalogPackage((page,), complete=complete)


def prior(**kw):
    body = dict(symbol=SYMBOL, trade_date="2026-10-08", adjust_mode="raw", close="10.00") | kw
    received = at(day="2026-10-12") if body["trade_date"] == "2026-10-09" else at()
    return JSONFieldSource(receipt(json.dumps(body).encode(), endpoint="local:T1-raw-daily", params={}, received=received))


def ca(**kw):
    body = dict(symbol=SYMBOL, ex_date="2026-10-09", reference_price="9.50",
                reference_basis="EXCHANGE_CORPORATE_ACTION_REFERENCE", event_id="ca-fixture-1") | kw
    return JSONFieldSource(receipt(json.dumps(body).encode(), endpoint="local:corporate-action-reference", params={}))


def build(quote=None, **kw):
    return build_tencent_pit_source(SYMBOL, quote or receipt(), observed_at=at(),
        ticker_catalog=kw.pop("ticker_catalog", catalog()), prior_close=kw.pop("prior_close", prior()), **kw)


def plan():
    return dict(plan_id="p1", symbol=SYMBOL, status="PENDING_MORNING_REVIEW", valid_from=None,
                expires_at=at("15:00:00").isoformat(), payload={"symbol": SYMBOL, "target_trade_date": "2026-10-09"})


def test_same_call_wrapper_returns_original_text_without_extra_requests(tmp_path):
    calls, clocks = [], iter([at("09:25:59"), at()])
    raw = raw_quote()
    class Response:
        content = raw
        status_code = 200
        def raise_for_status(self):
            return None
    archive = RawReceiptArchive(tmp_path / "isolated-raw")
    wrapper = RecordingTextFetcher(lambda *args: calls.append(args) or Response(), clock=lambda: next(clocks), archive=archive)
    result = TencentIntradayAdapter(text_fetcher=wrapper).fetch_quote(SYMBOL, as_of=at())
    assert result.complete and len(calls) == 1
    saved = wrapper.receipts[0]
    assert saved.raw_response == raw
    assert saved.raw_sha256 == hashlib.sha256(raw).hexdigest()
    assert saved.response_received_at == at().isoformat()
    assert saved.clock_basis == "INJECTED_CLOCK_NOT_AUTHENTICATED"
    assert saved.request_parameters == {"q": "sh600001"}
    assert saved.request_options == {"timeout_argument": 12.0}
    assert archive.load(saved.receipt_sha256).raw_response == raw


def test_unwired_wrapper_never_creates_default_network_source():
    wrapper = RecordingTextFetcher()
    with pytest.raises(SourceEvidenceError, match="SOURCE_UNWIRED"):
        wrapper("https://qt.gtimg.cn/q", {"q": "sh600001"}, 12)
    assert wrapper.receipts == []


def test_archive_failure_does_not_change_normal_provider_quote():
    class Response:
        content, status_code = raw_quote(), 200
        def raise_for_status(self):
            pass
    class Broken:
        def store(self, value):
            raise OSError("not leaked")
    clocks = iter([at("09:25:59"), at()])
    wrapper = RecordingTextFetcher(lambda *_: Response(), clock=lambda: next(clocks), archive=Broken())
    assert TencentIntradayAdapter(text_fetcher=wrapper).fetch_quote(SYMBOL, as_of=at()).complete
    assert wrapper.archive_errors == ["SOURCE_ARCHIVE_WRITE_FAILED"]


def test_vendor_preclose_crosscheck_and_consumer_bytes_distinction():
    q = receipt()
    result = build(q)
    assert result["status"] == "COMPLETE"
    assert result["evidence"]["preclose_basis"] == "EXCHANGE_DISPLAYED_PRECLOSE"
    assert result["preclose_crosscheck"]["source_basis"] == "VENDOR_RELAYED_EXCHANGE_PRECLOSE"
    assert result["preclose_crosscheck"]["status"] == "MATCHED_T1_RAW_CLOSE"
    src = result["source_receipt"]
    assert src.byte_kind == "CONSUMER_INPUT_BYTES"
    assert src.raw_response != q.raw_response
    envelope = json.loads(src.raw_response)
    assert envelope["source_bindings"]["quote"]["raw_sha256"] == q.raw_sha256
    assert envelope["original_responses"]["quote"]["encoding"] == "BASE64"
    row = build_plan_pit_capture(plan(), src, observed_at=at())
    assert row["status"] == "COMPLETE" and row["limits"]["status"] == "KNOWN"
    assert row["source_lineage"]["raw_http_bytes_available"] is False
    assert row["evidence"]["source_input_sha256"] == hashlib.sha256(src.raw_response).hexdigest()
    assert row["evidence"]["upper_limit"] is row["evidence"]["lower_limit"] is None
    assert row["limits"]["basis"] == "DERIVED_FROM_PRECLOSE_AND_BOARD_RULE"


@pytest.mark.parametrize("kw,reason", [
    ({"prior_close": None}, "PRECLOSE_CROSSCHECK_UNPROVEN"),
    ({"prior_close": "mismatch"}, "PRECLOSE_CROSSCHECK_MISMATCH"),
    ({"prior_close": "qfq"}, "T1_RAW_CLOSE_UNPROVEN"),
    ({"prior_close": "old"}, "T1_RAW_CLOSE_UNPROVEN"),
    ({"prior_close": "wrong"}, "T1_RAW_CLOSE_UNPROVEN"),
])
def test_preclose_never_unchecked_displayed_truth(kw, reason):
    choices = {"mismatch": prior(close="9.99"), "qfq": prior(adjust_mode="qfq"),
               "old": prior(trade_date="2026-09-30"), "wrong": prior(symbol="600002.SH")}
    value = kw["prior_close"]
    result = build(prior_close=choices.get(value) if value is not None else None)
    assert result["status"] == "DATA_LIMITED" and reason in result["reason_codes"]
    assert result["evidence"]["preclose_basis"] == "VENDOR_RELAYED_EXCHANGE_PRECLOSE"


def test_ex_rights_requires_bound_same_symbol_day_reference_not_price_difference():
    result = build(receipt(raw_quote(preclose="9.50")), corporate_action=ca())
    assert result["preclose_crosscheck"]["status"] == "MATCHED_CORPORATE_ACTION_REFERENCE"
    assert "EX_RIGHTS_REFERENCE_OBSERVED" in result["annotations"]
    bad = build(receipt(raw_quote(preclose="9.50")), corporate_action=ca(ex_date="2026-10-08"))
    assert bad["status"] == "DATA_LIMITED"
    assert bad["evidence"]["preclose_basis"] != "EXCHANGE_DISPLAYED_PRECLOSE"
    no_event = build(receipt(raw_quote(preclose="9.50")))
    assert no_event["preclose_crosscheck"]["status"] == "MISMATCH"


@pytest.mark.parametrize("bad", ["false_basis", "wrong_symbol", "no_event_id", "bool_price"])
def test_ca_reference_cannot_be_guessed_or_wrong_security(bad):
    kw = {"false_basis": {"reference_basis": "QFQ_DERIVED"}, "wrong_symbol": {"symbol": "600002.SH"},
          "no_event_id": {"event_id": None}, "bool_price": {"reference_price": True}}[bad]
    result = build(receipt(raw_quote(preclose="9.50")), corporate_action=ca(**kw))
    assert result["status"] == "DATA_LIMITED"
    assert result["evidence"]["preclose_basis"] != "EXCHANGE_DISPLAYED_PRECLOSE"


@pytest.mark.parametrize("kind", ["missing", "old", "partial", "incomplete_total", "projected", "future"])
def test_listing_requires_actual_complete_dated_catalog_package(kind):
    cat = {"missing": None, "old": catalog(day="2026-10-08"), "partial": catalog(complete=False),
           "incomplete_total": catalog(total=2), "projected": {"SecurityRecord": {"listing_date": "2020-01-02"}},
           "future": catalog(day="2026-10-12")}[kind]
    result = build(ticker_catalog=cat)
    assert result["status"] == "DATA_LIMITED"
    assert result["evidence"]["listing_date"] is None


def test_no_st_name_does_not_prove_ordinary_identity():
    cat = catalog(rows=[{"symbol": SYMBOL, "listing_date": "2020-01-02"}])
    result = build(ticker_catalog=cat)
    assert result["evidence"]["security_name"] == "测试证券"
    assert result["evidence"]["is_st"] is None
    assert result["evidence"]["board"] is None
    assert result["status"] == "DATA_LIMITED"


def test_st_name_is_positive_evidence_and_cannot_be_overridden_by_false_catalog():
    result = build(receipt(raw_quote(name="*ST测试")))
    assert result["evidence"]["is_st"] is True
    assert result["status"] == "DATA_LIMITED"
    assert result["limits"]["status"] == "UNKNOWN"


@pytest.mark.parametrize("kw,reason", [
    ({"raw": raw_quote(symbol="sh600002")}, "QUOTE_SYMBOL_MISMATCH"),
    ({"raw": raw_quote(stamp="20261008092559")}, "QUOTE_DATE_MISMATCH"),
    ({"raw": raw_quote(stamp="20261009092700")}, "QUOTE_FUTURE"),
    ({"raw": raw_quote(stamp="20261009092100")}, "QUOTE_AUCTION_EXPIRED"),
    ({"complete": False}, "SOURCE_PARTIAL"),
])
def test_invalid_quote_not_promoted(kw, reason):
    raw = kw.pop("raw", None)
    result = build(receipt(raw, **kw))
    assert result["status"] == "DATA_LIMITED" and reason in result["reason_codes"]
    assert result["source_receipt"] is None


def test_rule_date_is_not_silently_extended():
    q = receipt(raw_quote(stamp="20261012092559"), received=at(day="2026-10-12"))
    p = prior(trade_date="2026-10-09")
    result = build_tencent_pit_source(SYMBOL, q, observed_at=at(day="2026-10-12"),
                                      ticker_catalog=catalog(day="2026-10-12"), prior_close=p)
    if at(day="2026-10-12").date() > RULE_REVIEWED_THROUGH:
        assert result["limits"]["status"] == "UNKNOWN"
        assert "RULE_DATE_UNPROVEN" in result["reason_codes"]


def test_explicit_limits_not_read_from_unverified_tencent_indices():
    result = build()
    assert result["explicit_limit_mapping_status"] == "PENDING_REAL_0926_MULTI_BOARD_SAMPLES"
    assert result["evidence"]["upper_limit"] is result["evidence"]["lower_limit"] is None


def test_catalog_duplicate_conflict_not_arbitrary_first_row():
    rows = [{"symbol": SYMBOL, "listing_date": "2020-01-02"}, {"symbol": SYMBOL, "listing_date": "2026-10-09"}]
    result = build(ticker_catalog=catalog(rows=rows))
    assert result["status"] == "DATA_LIMITED"
    assert "CATALOG_DUPLICATE_SYMBOL" in result["reason_codes"]


def test_original_evidence_and_params_unchanged_and_tamper_rejected():
    params = {"q": "sh600001"}
    q = receipt(params=params)
    original = deepcopy(params)
    result = build(q)
    assert params == original and q.raw_response == raw_quote()
    bad = replace(q, raw_response=raw_quote(preclose="9.99"))
    failed = build(bad)
    assert failed["status"] == "DATA_LIMITED" and "SOURCE_HASH_MISMATCH" in failed["reason_codes"]
    assert result["source_authenticated"] is False


@pytest.mark.parametrize("params", [{"api_key": "do-not-store"}, {"token": "private"}, {1: "numeric-key"}])
def test_credentials_and_non_json_request_keys_rejected(params):
    with pytest.raises(SourceEvidenceError):
        receipt(params=params)


def test_archive_duplicate_idempotent_and_corruption_not_overwritten(tmp_path):
    archive = RawReceiptArchive(tmp_path / "new")
    q = receipt()
    first = archive.store(q)
    assert archive.store(q) == first
    path = archive.root / q.receipt_sha256 / "response.bin"
    path.write_bytes(b"corruption")
    with pytest.raises(SourceEvidenceError, match="SOURCE_ARCHIVE_CONFLICT"):
        archive.store(q)
    assert path.read_bytes() == b"corruption"


def test_paginated_catalog_requires_all_bound_pages():
    p1 = receipt(json.dumps({"code": 0, "data": {"items": [{"symbol": "600002.SH"}], "total": 2}}).encode(),
                 endpoint="/api/meta/tickers/list", params={"exchange": "SH,SZ,BJ", "asset_type": "a-share", "limit": 1, "offset": 0})
    row = dict(symbol=SYMBOL, listing_date="2020-01-02", board="SSE_MAIN", security_type="CASH_A_SHARE",
               security_status="ORDINARY", is_st=False, limit_regime="NORMAL")
    p2 = receipt(json.dumps({"code": 0, "data": {"items": [row], "total": 2}}).encode(),
                 endpoint="/api/meta/tickers/list", params={"exchange": "SH,SZ,BJ", "asset_type": "a-share", "limit": 1, "offset": 1})
    result = build(ticker_catalog=TickerCatalogPackage((p1, p2), complete=True))
    assert result["evidence"]["listing_date"] == "2020-01-02"
    bad = build(ticker_catalog=TickerCatalogPackage((p2,), complete=True))
    assert bad["evidence"]["listing_date"] is None


def test_zero_requests_models_notifications_and_db_dependencies():
    result = build()
    assert result["source_fetch_count"] == result["model_calls"] == result["customer_notification_calls"] == 0
    assert result["production_mutation"] == "NONE"


def test_extraction_paths_and_original_raw_recovery_are_bound():
    import base64
    q, p = receipt(), prior()
    result = build(q, prior_close=p)
    body = json.loads(result["source_receipt"].raw_response)
    assert body["extraction_proofs"]["quote"]["previous_close_index"] == 4
    assert body["extraction_proofs"]["prior_close"]["field_map"]["close"] == "/close"
    assert body["extraction_proofs"]["catalog"]["listing_date_pointer"] == "/data/items/0/listing_date"
    assert base64.b64decode(body["original_responses"]["quote"]["bytes"]) == q.raw_response
    assert base64.b64decode(body["original_responses"]["prior_close"]["bytes"]) == p.receipt.raw_response
    assert len(body["source_implementation_sha256"]["shadow_pit_sources.py"]) == 64


def test_bad_quote_encoding_cannot_prove_displayed_name():
    raw = raw_quote().replace("测试证券".encode("gbk"), b"\xff")
    result = build(receipt(raw))
    assert result["status"] == "DATA_LIMITED"
    assert "QUOTE_ENCODING_UNPROVEN" in result["reason_codes"]


@pytest.mark.parametrize("raw", [b"", "decoded text"])
def test_original_bytes_required_not_reconstructed_from_text(raw):
    with pytest.raises(SourceEvidenceError, match="RAW_RESPONSE_BYTES_REQUIRED"):
        record_raw_response(raw, source_ref="fixture:bytes", endpoint="https://qt.gtimg.cn/q",
            request_parameters={"q":"sh600001"}, request_started_at=at(), response_received_at=at())


def test_missing_preclose_or_name_not_normalized_to_zero():
    assert build(receipt(raw_quote(preclose="")))["status"] == "DATA_LIMITED"
    result = build(receipt(raw_quote(name="")))
    assert result["status"] == "DATA_LIMITED"
    assert "IDENTITY_FIELDS_INCOMPLETE" in result["reason_codes"]


def test_new_listing_and_bj_not_promoted_to_normal_limits():
    row = dict(symbol=SYMBOL, listing_date="2026-10-09", board="SSE_MAIN", security_type="CASH_A_SHARE",
               security_status="ORDINARY", is_st=False, limit_regime="NORMAL")
    result = build(ticker_catalog=catalog(rows=[row]))
    assert result["limits"]["status"] == "UNKNOWN"
    assert "NORMAL_LISTING_PERIOD_UNPROVEN" in result["reason_codes"]
    row.update(symbol="920001.BJ", listing_date="2020-01-02", board="BJ")
    q = receipt(raw_quote(symbol="bj920001"), params={"q":"bj920001"})
    result = build_tencent_pit_source("920001.BJ", q, observed_at=at(), ticker_catalog=catalog(rows=[row]),
        prior_close=prior(symbol="920001.BJ"))
    assert result["limits"]["status"] == "UNKNOWN"


def test_source_receive_future_or_http_partial_not_used():
    future = build(receipt(received=at("09:27:00")))
    assert future["reason_codes"] == ["SOURCE_FUTURE"]
    assert build(receipt(http_status=503))["reason_codes"] == ["SOURCE_PARTIAL"]


def test_local_close_pointer_map_does_not_drop_its_original_field_path():
    data = {"source":{"symbol":SYMBOL,"date":"2026-10-08","mode":"none","raw_close":"10.00"}}
    source = JSONFieldSource(receipt(json.dumps(data).encode(), endpoint="local:T1-raw-daily", params={}),
        {"symbol":"/source/symbol","trade_date":"/source/date","adjust_mode":"/source/mode","close":"/source/raw_close"})
    result = build(prior_close=source)
    assert result["preclose_crosscheck"]["status"] == "MATCHED_T1_RAW_CLOSE"
    body = json.loads(result["source_receipt"].raw_response)
    assert body["extraction_proofs"]["prior_close"]["field_map"]["close"] == "/source/raw_close"


def test_t1_postclose_package_is_legal_and_ordinary_type_does_not_fake_ca():
    body = dict(symbol=SYMBOL, trade_date="2026-10-08", adjust_mode="raw", close="10.00")
    source = JSONFieldSource(receipt(json.dumps(body).encode(), endpoint="local:T1-raw-daily", params={},
                                    received=at("15:10:00", "2026-10-08")))
    result = build(prior_close=source)
    assert result["status"] == "COMPLETE"
    assert result["evidence"]["prior_raw_close"] == result["evidence"]["preclose"]
    ordinary = build()
    assert ordinary["evidence"]["prior_raw_close"] == ordinary["evidence"]["preclose"]


def test_ca_keeps_valid_prior_raw_close_in_capture_vocabulary():
    result = build(receipt(raw_quote(preclose="9.50")), corporate_action=ca())
    assert result["evidence"]["prior_raw_close"] == 10.0
    assert result["evidence"]["prior_raw_close"] != result["evidence"]["preclose"]


def test_ca_cannot_hide_unbound_extra_source():
    source = prior()
    bad = JSONFieldSource(replace(source.receipt, raw_response=b"tampered"))
    result = build(receipt(raw_quote(preclose="9.50")), corporate_action=ca(), prior_close=bad)
    assert result["status"] == "DATA_LIMITED"
    assert "SOURCE_HASH_MISMATCH" in result["reason_codes"]
