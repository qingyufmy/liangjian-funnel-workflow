from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Barrier, Lock
import socket
import time

import pytest

from liangjian_funnel.data.provider_governance import (
    ProviderCapability,
    ProviderEnvelope,
    ProviderGovernor,
    ProviderRegistry,
    ProviderRequest,
    ProviderStatus,
    validate_external_url,
    validate_fallback,
)
from liangjian_funnel.runtime.state import RuntimeStore


NOW = datetime(2026, 9, 20, 9, 30, tzinfo=timezone(timedelta(hours=8)))


def capability(
    provider: str = "primary",
    capability_name: str = "minute",
    *,
    quota_scope: str = "shared-market",
    upstream: str | None = None,
    authorization: str = "FREE_PUBLIC",
    fields: tuple[str, ...] = ("close", "volume"),
) -> ProviderCapability:
    return ProviderCapability(
        provider=provider,
        capability=capability_name,
        quota_scope=quota_scope,
        account_scope="public",
        authorization=authorization,
        supported_fields=fields,
        markets=("SSE",),
        unit="CNY_SHARE",
        adjustment="RAW",
        time_precision="MINUTE",
        rate_window_seconds=60,
        max_requests=100,
        max_concurrency=8,
        ttl_seconds=1,
        stale_action="BLOCK_NEW_POSITION",
        priority=10,
        fallbacks=(),
        upstream_identity=upstream or provider,
        allowed_hosts=("example.com",),
        max_response_bytes=1024,
    )


def request(*, provider: str = "primary", capability_name: str = "minute", as_of: str = "2026-09-20T09:30:00+08:00") -> ProviderRequest:
    return ProviderRequest(
        provider=provider,
        capability=capability_name,
        object_id="600000.SH",
        as_of=as_of,
        fields=("close", "volume"),
        semantics={"unit": "CNY_SHARE", "adjustment": "RAW"},
    )


def ok_envelope(payload=None, *, fetched_at: datetime = NOW) -> ProviderEnvelope:
    return ProviderEnvelope(
        ProviderStatus.OK,
        "OK",
        payload={"close": 10.0} if payload is None else payload,
        effective_at=NOW.isoformat(),
        fetched_at=fetched_at.isoformat(),
        ingested_at=fetched_at.isoformat(),
        time_verified=True,
    )


def make_governor(tmp_path: Path, *specs: ProviderCapability, owner: str | None = None) -> ProviderGovernor:
    return ProviderGovernor(
        store=RuntimeStore(tmp_path / "runtime.sqlite3"),
        registry=ProviderRegistry(specs or (capability(),)),
        owner=owner,
        poll_seconds=0.002,
    )


def test_src_01_fifty_concurrent_identical_requests_are_singleflight(tmp_path: Path) -> None:
    governor = make_governor(tmp_path, capability())
    start = Barrier(50)
    calls = 0
    guard = Lock()

    def remote(_request: ProviderRequest, _timeout: float) -> ProviderEnvelope:
        nonlocal calls
        with guard:
            calls += 1
        time.sleep(0.03)
        return ok_envelope({"close": 10.0, "rows": [1, 2, 3]})

    def run(_index: int) -> ProviderEnvelope:
        start.wait(timeout=3)
        return governor.fetch(request(), remote, deadline_seconds=1.5)

    with ThreadPoolExecutor(max_workers=50) as pool:
        results = list(pool.map(run, range(50)))
    assert calls == 1
    assert all(item.status == ProviderStatus.OK for item in results)
    assert len({id(item.payload) for item in results}) > 1
    assert all(item.payload == {"close": 10.0, "rows": [1, 2, 3]} for item in results)


def test_src_02_quota_and_cooldown_survive_restart_and_share_scope(tmp_path: Path) -> None:
    path = tmp_path / "runtime.sqlite3"
    first = RuntimeStore(path)
    assert first.reserve_provider_quota("shared", now=NOW, window_seconds=60, max_requests=1)["allowed"]
    second = RuntimeStore(path)
    denied = second.reserve_provider_quota("shared", now=NOW + timedelta(seconds=1), window_seconds=60, max_requests=1)
    assert denied["reason_code"] == "PROVIDER_QUOTA_EXHAUSTED"
    second.update_provider_health("shared", now=NOW, success=False, cooldown_seconds=30)
    third = RuntimeStore(path)
    cooling = third.reserve_provider_quota("shared", now=NOW + timedelta(seconds=2), window_seconds=60, max_requests=99)
    assert cooling["reason_code"] == "PROVIDER_COOLDOWN"


def test_completed_snapshot_is_reused_until_ttl_without_another_remote_call(tmp_path: Path) -> None:
    governor = make_governor(tmp_path, capability())
    governor.wall_clock = lambda: NOW
    calls = 0

    def remote(*_args) -> ProviderEnvelope:
        nonlocal calls
        calls += 1
        return ok_envelope()

    assert governor.fetch(request(), remote, deadline_seconds=0.5).status == ProviderStatus.OK
    assert governor.fetch(request(), remote, deadline_seconds=0.5).status == ProviderStatus.OK
    assert calls == 1


def test_concurrency_slots_are_durable_bounded_and_expire(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    assert store.acquire_provider_concurrency("shared", "a", now=NOW, ttl_seconds=1, max_concurrency=1) == 1
    assert store.acquire_provider_concurrency("shared", "b", now=NOW, ttl_seconds=1, max_concurrency=1) is None
    assert store.acquire_provider_concurrency(
        "shared", "b", now=NOW + timedelta(seconds=2), ttl_seconds=1, max_concurrency=1
    ) == 1
    assert not store.release_provider_concurrency("shared", 1, "a")
    assert store.release_provider_concurrency("shared", 1, "b")


def test_src_03_rate_limit_timeout_then_recovery_has_bounded_attempts(tmp_path: Path) -> None:
    governor = make_governor(tmp_path, capability())
    sequence: list[str] = []

    def remote(_request: ProviderRequest, _timeout: float) -> ProviderEnvelope:
        sequence.append("call")
        if len(sequence) == 1:
            return ProviderEnvelope(ProviderStatus.RATE_LIMITED, "HTTP_429", retry_after_seconds=0.01)
        if len(sequence) == 2:
            raise TimeoutError("temporary")
        return ok_envelope()

    result = governor.fetch(request(), remote, deadline_seconds=1, max_attempts=3)
    assert result.status == ProviderStatus.OK
    assert len(sequence) == 3
    attempts = governor.store.list_provider_attempts(request().key())
    assert [item["outcome"] for item in attempts] == ["RATE_LIMITED", "NETWORK_TRANSIENT", "OK"]


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        (ProviderStatus.EMPTY, "LEGAL_EMPTY_SET"),
        (ProviderStatus.NOT_PUBLISHED, "UPSTREAM_NOT_PUBLISHED"),
        (ProviderStatus.SCHEMA_ERROR, "UPSTREAM_SCHEMA_CHANGED"),
        (ProviderStatus.AUTH_FAILURE, "UPSTREAM_AUTH_FAILED"),
    ],
)
def test_src_04_terminal_source_states_are_distinct_and_not_retried(
    tmp_path: Path, status: ProviderStatus, reason: str
) -> None:
    governor = make_governor(tmp_path / status.value, capability())
    calls = 0

    def remote(_request: ProviderRequest, _timeout: float) -> ProviderEnvelope:
        nonlocal calls
        calls += 1
        return ProviderEnvelope(status, reason)

    result = governor.fetch(request(), remote, deadline_seconds=0.5, max_attempts=5)
    assert result.status == status
    assert result.reason_code == reason
    assert calls == 1


def test_src_05_bad_response_keeps_last_good_and_stale_is_not_tradable(tmp_path: Path) -> None:
    governor = make_governor(tmp_path, capability())
    req = request()
    assert governor.fetch(req, lambda *_: ok_envelope(), deadline_seconds=0.5).status == ProviderStatus.OK
    original = governor.store.get_provider_last_good(req.key())
    assert original is not None

    bad = governor.fetch(
        req,
        lambda *_: ProviderEnvelope(ProviderStatus.SCHEMA_ERROR, "UPSTREAM_SCHEMA_CHANGED"),
        deadline_seconds=0.5,
    )
    assert bad.status == ProviderStatus.SCHEMA_ERROR
    assert governor.store.get_provider_last_good(req.key())["content_hash"] == original["content_hash"]

    governor.wall_clock = lambda: NOW + timedelta(seconds=2)
    display = governor.read_last_good(req, for_new_position=False)
    blocked = governor.read_last_good(req, for_new_position=True)
    assert display is not None and display.reason_code == "LAST_GOOD_STALE_DISPLAY_ONLY"
    assert blocked is not None and blocked.reason_code == "STALE_LAST_GOOD_NOT_TRADABLE"


def test_src_06_fallback_requires_semantic_match_and_independent_upstream() -> None:
    primary = capability(upstream="upstream-a")
    fallback = capability("fallback", upstream="upstream-b")
    base = {
        "symbol": "600000.SH",
        "trade_date": "2026-09-20",
        "interval": "1m",
        "unit": "CNY_SHARE",
        "adjustment": "RAW",
        "time_verified": True,
    }
    accepted, reasons = validate_fallback(primary, fallback, primary_metadata=base, fallback_metadata=base)
    assert accepted and reasons == ()

    bad = {**base, "unit": "LOT", "adjustment": "QFQ", "trade_date": "2026-09-19"}
    same_upstream = capability("fallback", upstream="upstream-a")
    accepted, reasons = validate_fallback(primary, same_upstream, primary_metadata=base, fallback_metadata=bad)
    assert not accepted
    assert set(reasons) >= {
        "FALLBACK_UNIT_CONFLICT",
        "FALLBACK_ADJUSTMENT_CONFLICT",
        "FALLBACK_TRADE_DATE_CONFLICT",
        "FALLBACK_NOT_INDEPENDENT",
    }


def test_src_07_total_deadline_bounds_nonresponsive_adapter(tmp_path: Path) -> None:
    governor = make_governor(tmp_path, capability())

    def hanging(_request: ProviderRequest, _timeout: float) -> ProviderEnvelope:
        time.sleep(1)
        return ok_envelope()

    started = time.monotonic()
    result = governor.fetch(request(), hanging, deadline_seconds=0.05, max_attempts=3)
    elapsed = time.monotonic() - started
    assert result.status == ProviderStatus.DEADLINE_EXCEEDED
    assert elapsed < 0.25
    assert len(governor.store.list_provider_attempts(request().key())) == 1


def test_response_size_and_unverified_time_are_fail_closed(tmp_path: Path) -> None:
    tiny = replace(capability(), max_response_bytes=8)
    governor = make_governor(tmp_path / "size", tiny)
    oversized = governor.fetch(request(), lambda *_: ok_envelope({"long": "payload"}), deadline_seconds=0.5)
    assert oversized.reason_code == "PROVIDER_RESPONSE_TOO_LARGE"
    assert governor.store.get_provider_last_good(request().key()) is None

    governor = make_governor(tmp_path / "time", capability())
    unverified = governor.fetch(
        request(),
        lambda *_: ProviderEnvelope(
            ProviderStatus.OK,
            "OK",
            payload={"close": 10},
            fetched_at=NOW.isoformat(),
            ingested_at=NOW.isoformat(),
            time_verified=False,
        ),
        deadline_seconds=0.5,
    )
    assert unverified.status == ProviderStatus.TIME_UNVERIFIED
    assert governor.store.get_provider_last_good(request().key()) is None


def test_src_08_expired_lease_recovers_and_stale_owner_cannot_publish(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    key = request().key()
    token_a = store.acquire_provider_request(key, "old", now=NOW, ttl_seconds=1)
    token_b = store.acquire_provider_request(key, "new", now=NOW + timedelta(seconds=2), ttl_seconds=30)
    assert token_a == 1 and token_b == 2
    assert not store.publish_provider_result(
        request_key=key,
        owner="old",
        fencing_token=token_a,
        result=ok_envelope({"owner": "old"}).serializable(),
        now=NOW + timedelta(seconds=3),
    )
    assert store.publish_provider_result(
        request_key=key,
        owner="new",
        fencing_token=token_b,
        result=ok_envelope({"owner": "new"}).serializable(),
        now=NOW + timedelta(seconds=3),
    )
    row = store.get_provider_request(key)
    assert '"new"' in row["result_json"] and '"old"' not in row["result_json"]


def test_registry_requires_full_contract_and_blocks_unlicensed_capability(tmp_path: Path) -> None:
    config = Path(__file__).parents[2] / "config" / "capability_specs.yaml"
    registry = ProviderRegistry.load(config)
    assert registry.require("tencent", "intraday_bars").quota_scope == "tencent_public_market"
    blocked = capability(authorization="UNAUTHORIZED")
    with pytest.raises(PermissionError):
        ProviderRegistry((blocked,)).require("primary", "minute")

    invalid = tmp_path / "invalid.yaml"
    invalid.write_text("governance:\n  capabilities:\n    - provider: incomplete\n", encoding="utf-8")
    with pytest.raises(ValueError, match="missing fields"):
        ProviderRegistry.load(invalid)


def test_external_url_guard_rejects_non_https_and_unapproved_hosts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "liangjian_funnel.data.provider_governance.socket.getaddrinfo",
        lambda *_args, **_kwargs: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))],
    )
    assert validate_external_url("https://example.com/data", allowed_hosts=("example.com",))
    with pytest.raises(ValueError, match="EXTERNAL_URL_NOT_ALLOWED"):
        validate_external_url("http://example.com/data", allowed_hosts=("example.com",))
    with pytest.raises(ValueError, match="EXTERNAL_HOST_NOT_ALLOWED"):
        validate_external_url("https://evil.example/data", allowed_hosts=("example.com",))

    monkeypatch.setattr(
        "liangjian_funnel.data.provider_governance.socket.getaddrinfo",
        lambda *_args, **_kwargs: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443))],
    )
    with pytest.raises(ValueError, match="EXTERNAL_HOST_PRIVATE_ADDRESS"):
        validate_external_url("https://example.com/data", allowed_hosts=("example.com",))
