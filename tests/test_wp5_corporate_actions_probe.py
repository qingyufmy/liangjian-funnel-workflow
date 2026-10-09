from copy import deepcopy
from datetime import datetime
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import httpx
from pydantic import SecretStr
import pytest

from liangjian_funnel.pipeline.data_source import HithinkClient
from liangjian_funnel.settings import Settings

spec = importlib.util.spec_from_file_location("wp5_corporate_probe", Path(__file__).parents[1] /
    "scripts/probe_wp5_corporate_actions_readonly.py")
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


def settings(**updates):
    return Settings.from_env({}, root=Path(__file__).parents[1]).model_copy(update=updates)


def reference(symbol="601208.SH", day="2026-09-29"):
    return {"symbol": symbol, "ex_date": day, "pdf_sha256": "a" * 64,
            "announcement_url": "https://static.cninfo.com.cn/finalpage/2026-09-22/1225575752.PDF",
            "cash_per_share": 0.1, "capital_reserve_transfer_per_share": None,
            "price_reference_cash_per_share": None, "differential_distribution": False}


def envelope(ref=None):
    ref = ref or reference()
    ms = int(datetime.fromisoformat(ref["ex_date"]).replace(tzinfo=ZoneInfo("Asia/Shanghai")).timestamp() * 1000)
    return {"code": 0, "data": {"thscode": ref["symbol"], "ticker": ref["symbol"][:6],
            "item": [{"ticker": ref["symbol"][:6], "ex_date_ms": ms,
                      "dividend_per_share": 0.1, "per_share_bonus": 0}]}}


def inspect(body, ref=None, status=200):
    return probe.inspect_response(httpx.Response(status, content=json.dumps(body).encode()), ref or reference())


@pytest.mark.parametrize("body,reason", [
    ([], "INVALID_ENVELOPE"), ({"code": False, "data": {}}, "INVALID_ENVELOPE"),
    ({"code": 2001, "message": "secret"}, "BUSINESS_ERROR"),
    ({"code": 0, "data": []}, "INVALID_ENVELOPE"),
])
def test_bad_envelope(body, reason):
    result = inspect(body)
    assert not result["schema_valid"] and result["reason_code"] == reason


def test_invalid_json_and_raw_hash():
    response = httpx.Response(200, content=b"not-json")
    result = probe.inspect_response(response, reference())
    assert result["reason_code"] == "INVALID_JSON"
    assert result["response_sha256"] == probe.digest(b"not-json")


@pytest.mark.parametrize("mutation,reason", [
    (lambda d: d.update(thscode="002653.SZ"), "ENVELOPE_SYMBOL_CONFLICT"),
    (lambda d: d["item"].append({**d["item"][0], "ticker": "000001"}), "ROW_SYMBOL_CONFLICT"),
    (lambda d: d["item"].append({**d["item"][0], "ex_date_ms": True}), "ROW_DATE_INVALID"),
    (lambda d: d["item"].append({**d["item"][0], "ex_date_ms": d["item"][0]["ex_date_ms"] + 1}),
     "ROW_DATE_OUTSIDE_WINDOW_OR_NOT_MIDNIGHT"),
    (lambda d: d["item"].append({**d["item"][0], "dividend_per_share": "0.1"}), "DUPLICATE_EVENT_DATE"),
    (lambda d: d["item"][0].update(dividend_per_share=float("nan")), "ROW_AMOUNT_INVALID"),
    (lambda d: d["item"][0].update(per_share_bonus=True), "ROW_AMOUNT_INVALID"),
    (lambda d: d["item"].append(deepcopy(d["item"][0])), "DUPLICATE_EVENT_DATE"),
    (lambda d: d.update(item=None), "INVALID_ENVELOPE"),
])
def test_all_rows_fail_closed(mutation, reason):
    payload = envelope()
    mutation(payload["data"])
    result = inspect(payload)
    assert not result["schema_valid"] and result["rows"] == []
    assert result["reason_code"] == reason


def test_bad_second_row_amount_not_hidden_by_good_first_row():
    payload = envelope()
    payload["data"]["item"].append({**payload["data"]["item"][0],
        "ex_date_ms": payload["data"]["item"][0]["ex_date_ms"] - 86400000,
        "dividend_per_share": "0.1"})
    assert inspect(payload)["reason_code"] == "ROW_AMOUNT_INVALID"


def test_cash_date_and_safe_hash_matching():
    result = inspect(envelope())
    assert result["schema_valid"] and result["date_match_status"] == "MATCH"
    assert result["cash_comparison"] == "MATCH"
    assert result["safe_rows_sha256"] == probe.digest(probe.canonical(result["rows"]))
    assert result["factor_contract_status"] == "UNVERIFIED"
    assert inspect(envelope(reference(day="2026-09-28")))["date_match_status"].startswith("CONFLICT")
    payload = envelope()
    payload["data"]["item"][0]["dividend_per_share"] = .2
    assert inspect(payload)["cash_comparison"] == "CONFLICT"
    payload["data"]["item"] = []
    assert inspect(payload)["date_match_status"].startswith("CONFLICT")


def test_no_provider_secrets_or_untrusted_strings_are_exported():
    payload = envelope()
    secret = "PRIVATE_PROVIDER_SECRET_MUST_NOT_EXPORT"
    payload.update(message=secret, request_id=secret, api_key=secret)
    payload["data"].update(password=secret, arbitrary=secret)
    payload["data"]["item"][0].update(token=secret, note=secret, event_type=secret,
                                        rights_issue_price=secret, factor_basis=secret)
    result = inspect(payload)
    assert secret not in json.dumps(result)
    assert "token" not in result["safe_field_names"]
    assert "rights_issue_price" in result["safe_field_names"]


def test_differential_cash_and_transfer_remain_distinct():
    ref = reference()
    ref.update(cash_per_share=.2, price_reference_cash_per_share=.1974617,
               differential_distribution=True)
    payload = envelope(ref)
    payload["data"]["item"][0]["dividend_per_share"] = .2
    result = inspect(payload, ref)
    assert result["cash_comparison"] == "MATCH"
    assert result["price_reference_cash_comparison"] == "CONFLICT"
    assert result["factor_contract_status"].startswith("CONFLICT")
    ref.update(cash_per_share=None, price_reference_cash_per_share=None,
               capital_reserve_transfer_per_share=.48)
    payload["data"]["item"][0]["per_share_bonus"] = .48
    result = inspect(payload, ref)
    assert result["cash_comparison"].startswith("UNKNOWN")
    assert result["bonus_comparison"].startswith("UNKNOWN_CAPITAL_RESERVE")


@pytest.mark.parametrize("field,value,reason", [
    ("pdf_sha256", "a" * 63, "REFERENCE_HASH_INVALID"),
    ("ex_date", "2026-10-01", "REFERENCE_DATE_OUTSIDE_WINDOW"),
    ("cash_per_share", True, "REFERENCE_AMOUNT_INVALID"),
    ("announcement_url", "https://evil.example/?api_key=secret", "REFERENCE_URL_INVALID"),
])
def test_reference_fail_closed(field, value, reason):
    ref = reference()
    ref[field] = value
    with pytest.raises(ValueError, match=reason):
        probe.validate_reference({"evidence_type": "OFFICIAL_ANNOUNCEMENT_REVERSE_LOOKUP_NOT_ENDPOINT_RESPONSE",
                                  "records": [ref]}, expected_count=1)


def test_unique_reference_required():
    with pytest.raises(ValueError, match="REFERENCE_SYMBOL_DUPLICATE"):
        probe.validate_reference({"evidence_type": "OFFICIAL_ANNOUNCEMENT_REVERSE_LOOKUP_NOT_ENDPOINT_RESPONSE",
                                  "records": [reference(), reference()]}, expected_count=2)


def test_actual_transport_header_params_and_no_paginator():
    requests = []
    configuration = settings(hithink_api_key=SecretStr("TEST_KEY_NEVER_REPORT"))
    def handler(request):
        requests.append(request)
        assert request.headers["X-api-key"] == "TEST_KEY_NEVER_REPORT"
        assert request.url.params == httpx.QueryParams({"thscode": "601208.SH", "from": probe.START, "to": probe.END})
        return httpx.Response(200, json=envelope())
    with HithinkClient(configuration, transport=httpx.MockTransport(handler)) as client:
        client._paginate = lambda *_a, **_k: pytest.fail("paginator must never be called")
        result = probe.run_probe(configuration, [reference()], client=client)
    assert len(requests) == 1 and result["exit_code"] == 0
    assert "TEST_KEY_NEVER_REPORT" not in json.dumps(result)


def test_missing_key_and_reduced_rate_limit():
    assert probe.run_probe(settings(), [reference()])["exit_code"] == 3
    with pytest.raises(ValueError, match="REFUSE_REDUCED_RATE_LIMIT"):
        probe.run_probe(settings(hithink_min_request_interval_seconds=.1), [reference()])


def test_transport_errors_not_leaked_and_remaining_symbols_attempted():
    class Failing:
        def _get_with_retries(self, *_a):
            raise RuntimeError("https://private/?api_key=SECRET")
    result = probe.run_probe(settings(hithink_api_key=SecretStr("dummy")),
                             [reference(), reference("603999.SH")], client=Failing())
    assert len(result["records"]) == 2 and result["exit_code"] == 2
    assert "SECRET" not in json.dumps(result)


def test_rate_limit_metadata_only_safe_counters():
    client = SimpleNamespace(_get_with_retries=lambda *_: SimpleNamespace(
        response=None, http_status=429, reason_code="RATE_LIMITED",
        metadata={"attempts": 3, "error": "SECRET"}))
    result = probe.run_probe(settings(hithink_api_key=SecretStr("dummy")), [reference()], client=client)
    assert result["records"][0]["transport_attempts"] == 3
    assert result["records"][0]["http_status"] == 429
    assert "SECRET" not in json.dumps(result)
