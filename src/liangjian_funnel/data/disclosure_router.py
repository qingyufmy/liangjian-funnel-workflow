"""Route public company-disclosure queries across official free sources.

CNINFO remains the preferred full-market index.  The exchange web sites are
independent official fallbacks and are only queried when CNINFO is genuinely
unavailable or incomplete.  A confirmed empty CNINFO response is authoritative
and is never converted into a fallback request.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Protocol
from zoneinfo import ZoneInfo

from .cninfo import CninfoFetchResult


SHANGHAI = ZoneInfo("Asia/Shanghai")


class DisclosureClient(Protocol):
    def fetch_announcements(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        *,
        search_keyword: str = "",
    ) -> CninfoFetchResult: ...


def _provider_attempt(result: CninfoFetchResult) -> dict[str, object]:
    return {
        "source_id": result.source_id,
        "reason_code": result.reason_code,
        "ok": result.ok,
        "complete": result.complete,
        "announcement_count": len(result.announcements),
        "fetched_at": result.fetched_at.isoformat(),
        "http_status": result.http_status,
        "availability_state": _availability_state(result),
    }


def _availability_state(result: CninfoFetchResult) -> str:
    if result.ok and result.complete:
        return "NO_DISCLOSURE_CONFIRMED" if result.reason_code == "NO_RECORDS" else "READY"
    reason = result.reason_code.upper()
    if "CONTRACT" in reason or "INVALID_JSON" in reason:
        return "CONTRACT_CHANGED"
    if "PAGINATION" in reason:
        return "INCOMPLETE"
    if any(token in reason for token in ("HTTP", "REQUEST", "RATE_LIMIT", "ACCESS_DENIED", "TIMEOUT")):
        return "SOURCE_UNAVAILABLE"
    return "FAILED"


class OfficialDisclosureRouter:
    """Fail over from CNINFO to the symbol's listing exchange.

    The returned object keeps the existing ``CninfoFetchResult`` contract so
    downstream facts do not need provider-specific branches.  Provenance and
    every attempted provider are retained in metadata.
    """

    def __init__(
        self,
        cninfo: DisclosureClient,
        *,
        sse: DisclosureClient | None = None,
        szse: DisclosureClient | None = None,
        bse: DisclosureClient | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.cninfo = cninfo
        self.sse = sse
        self.szse = szse
        self.bse = bse
        self._now = now or (lambda: datetime.now(SHANGHAI))

    def fetch_announcements(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        *,
        search_keyword: str = "",
    ) -> CninfoFetchResult:
        primary = self.fetch_primary(
            symbol, start_date, end_date, search_keyword=search_keyword
        )
        if primary.ok and primary.complete:
            return primary
        return self.fetch_after_primary_failure(
            primary,
            symbol,
            start_date,
            end_date,
            search_keyword=search_keyword,
        )

    def fetch_primary(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        *,
        search_keyword: str = "",
    ) -> CninfoFetchResult:
        canonical = str(symbol).strip().upper()
        exchange = canonical.rsplit(".", 1)[-1] if "." in canonical else ""
        fallback = {"SH": self.sse, "SZ": self.szse, "BJ": self.bse}.get(exchange)

        # BSE is queried directly because its official endpoint is already a
        # complete and independently tested source.  This also avoids wasting
        # CNINFO requests for a market that has a dedicated adapter.
        if exchange == "BJ" and fallback is not None:
            result = fallback.fetch_announcements(
                canonical, start_date, end_date, search_keyword=search_keyword
            )
            return self._annotate(result, [_provider_attempt(result)], fallback_used=False)

        primary = self.cninfo.fetch_announcements(
            canonical, start_date, end_date, search_keyword=search_keyword
        )
        attempts = [_provider_attempt(primary)]
        return self._annotate(primary, attempts, fallback_used=False)

    def fetch_after_primary_failure(
        self,
        primary: CninfoFetchResult,
        symbol: str,
        start_date: str,
        end_date: str,
        *,
        search_keyword: str = "",
    ) -> CninfoFetchResult:
        canonical = str(symbol).strip().upper()
        exchange = canonical.rsplit(".", 1)[-1] if "." in canonical else ""
        fallback = {"SH": self.sse, "SZ": self.szse, "BJ": self.bse}.get(exchange)
        attempts = list(primary.metadata.get("provider_attempts") or [_provider_attempt(primary)])
        if primary.ok and primary.complete:
            return primary
        if exchange == "BJ":
            return primary
        if fallback is None:
            return primary

        secondary = fallback.fetch_announcements(
            canonical, start_date, end_date, search_keyword=search_keyword
        )
        attempts.append(_provider_attempt(secondary))
        if secondary.ok and secondary.complete:
            return self._annotate(secondary, attempts, fallback_used=True)

        # Do not hide the primary failure behind another generic error.  Both
        # reason codes remain machine-readable in metadata.
        return primary.model_copy(
            update={
                "metadata": {
                    **primary.metadata,
                    "source_system": "OFFICIAL_DISCLOSURE_ROUTER",
                    "provider_attempts": attempts,
                    "fallback_used": True,
                    "fallback_succeeded": False,
                    "availability_state": _availability_state(primary),
                }
            }
        )

    def _annotate(
        self,
        result: CninfoFetchResult,
        attempts: list[dict[str, object]],
        *,
        fallback_used: bool,
    ) -> CninfoFetchResult:
        return result.model_copy(
            update={
                "metadata": {
                    **result.metadata,
                    "source_system": "OFFICIAL_DISCLOSURE_ROUTER",
                    "provider_attempts": attempts,
                    "fallback_used": fallback_used,
                    "fallback_succeeded": bool(fallback_used and result.ok and result.complete),
                    "availability_state": _availability_state(result),
                    "router_checked_at": self._now().isoformat(),
                }
            }
        )


__all__ = ["DisclosureClient", "OfficialDisclosureRouter"]
