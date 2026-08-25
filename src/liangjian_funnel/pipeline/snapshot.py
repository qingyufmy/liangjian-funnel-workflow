"""Universe validation and immutable input-snapshot construction."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import tempfile
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .data_source import HithinkFetchResult, HithinkRow


SHANGHAI = ZoneInfo("Asia/Shanghai")
Exchange = Literal["SH", "SZ", "BJ", "INVALID"]
_CODE = re.compile(r"^\d{6}$")
_CANONICAL = re.compile(r"^(\d{6})[.]([A-Z]{2,4})$")
_ST = re.compile(r"(?:^|[^A-Z])(?:\*?ST|S\*?ST)(?:[^A-Z]|$)")
_DELIST_WORDS = ("退市", "退", "终止上市", "delist")


class SecurityRecord(BaseModel):
    """One normalized row in the full initial security universe."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    code: str
    exchange: Exchange
    name: str
    price: float | None = None
    volume: float | None = None
    amount: float | None = None
    change_ratio_pct: float | None = None
    research_eligible: bool = False
    trade_eligible: bool = False
    exclusion_reasons: tuple[str, ...] = ()
    source: str = "HITHINK"

    @field_validator("price", "volume", "amount", "change_ratio_pct")
    @classmethod
    def finite_or_none(cls, value: float | None) -> float | None:
        if value is not None and not math.isfinite(float(value)):
            raise ValueError("security numeric fields must be finite")
        return value


class UniverseGatePolicy(BaseModel):
    """Deterministic, configuration-derived subset of the G0 contract."""

    model_config = ConfigDict(frozen=True)

    minimum_daily_turnover_cny: float = Field(default=0.0, ge=0)
    newly_listed_min_days: int = Field(default=0, ge=0)
    block_suspended: bool = False
    block_no_price_limit_new_listing: bool = False


class UniverseLineage(BaseModel):
    """Counts and deterministic exclusion reasons for the entire universe."""

    model_config = ConfigDict(frozen=True)

    catalog_record_count: int = Field(ge=0)
    market_snapshot_record_count: int = Field(ge=0)
    total_record_count: int = Field(ge=0)
    research_candidate_count: int = Field(ge=0)
    trade_candidate_count: int = Field(ge=0)
    excluded_by_reason: dict[str, int] = Field(default_factory=dict)
    preselect_limit: int | None = Field(default=None, ge=1)
    preselect_count: int = Field(default=0, ge=0)


class PreselectResult(BaseModel):
    """Deterministic preselection output with complete lineage."""

    model_config = ConfigDict(frozen=True)

    candidates: tuple[SecurityRecord, ...]
    limit: int = Field(ge=1)
    source_total: int = Field(ge=0)
    eliminated_by_limit: int = Field(ge=0)

    def __iter__(self):
        return iter(self.candidates)

    def __len__(self) -> int:
        return len(self.candidates)

    def __getitem__(self, index: int) -> SecurityRecord:
        return self.candidates[index]


class UniverseSnapshot(BaseModel):
    """Validated G0 universe; BJ is research-only by construction."""

    model_config = ConfigDict(frozen=True)

    as_of: datetime
    fetched_at: datetime
    records: tuple[SecurityRecord, ...]
    research_candidates: tuple[SecurityRecord, ...] = ()
    trade_candidates: tuple[SecurityRecord, ...] = ()
    ready: bool = False
    blocking_reasons: tuple[str, ...] = ()
    lineage: UniverseLineage

    @field_validator("as_of", "fetched_at")
    @classmethod
    def shanghai_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("snapshot timestamps must be timezone-aware")
        return value.astimezone(SHANGHAI)

    @classmethod
    def from_records(
        cls,
        catalog: HithinkFetchResult | Sequence[Mapping[str, Any] | HithinkRow],
        market_snapshot: HithinkFetchResult | Sequence[Mapping[str, Any] | HithinkRow] | None = None,
        *,
        as_of: datetime,
        fetched_at: datetime | None = None,
        gate_policy: UniverseGatePolicy | None = None,
    ) -> "UniverseSnapshot":
        policy = gate_policy or UniverseGatePolicy()
        catalog_rows, catalog_complete, catalog_reason = _rows_and_status(catalog)
        market_rows, market_complete, market_reason = _rows_and_status(market_snapshot) if market_snapshot is not None else ((), True, "NOT_PROVIDED")
        catalog_map: dict[str, dict[str, Any]] = {}
        snapshot_map: dict[str, dict[str, Any]] = {}
        blocking: list[str] = []
        invalid_count = 0

        for index, row in enumerate(catalog_rows):
            data = _row_dict(row)
            canonical = _canonical_from_row(data)
            if canonical is None:
                blocking.append("CATALOG_MISSING_OR_INVALID_CODE")
                invalid_count += 1
                continue
            if canonical in catalog_map:
                blocking.append("DUPLICATE_CATALOG_SYMBOL")
                invalid_count += 1
                continue
            catalog_map[canonical] = data
        for row in market_rows:
            data = _row_dict(row)
            canonical = _canonical_from_row(data)
            if canonical is None:
                blocking.append("SNAPSHOT_MISSING_OR_INVALID_CODE")
                invalid_count += 1
                continue
            if canonical in snapshot_map:
                # A duplicate snapshot row with a different price is unsafe.
                old_price = _first_value(snapshot_map[canonical], _PRICE_KEYS)
                new_price = _first_value(data, _PRICE_KEYS)
                if old_price != new_price:
                    blocking.append("DUPLICATE_SNAPSHOT_SYMBOL")
                    invalid_count += 1
                continue
            snapshot_map[canonical] = data

        symbols = list(catalog_map)
        symbols.extend(symbol for symbol in snapshot_map if symbol not in catalog_map)
        records: list[SecurityRecord] = []
        excluded: dict[str, int] = {}
        for index, symbol in enumerate(symbols):
            catalog_data = catalog_map.get(symbol, {})
            snapshot_data = snapshot_map.get(symbol, {})
            merged = {**catalog_data, **snapshot_data}
            record, reasons = _security_record(
                merged,
                symbol=symbol,
                index=index,
                in_catalog=symbol in catalog_map,
                in_snapshot=symbol in snapshot_map,
                as_of=as_of.date(),
                gate_policy=policy,
            )
            records.append(record)
            for reason in reasons:
                excluded[reason] = excluded.get(reason, 0) + 1
            if any(reason in {"INVALID_PRICE", "INVALID_VOLUME", "INVALID_AMOUNT", "MISSING_NAME", "MISSING_CODE", "MISSING_MARKET_SNAPSHOT"} for reason in reasons):
                invalid_count += 1

        if not catalog_complete:
            blocking.append(f"CATALOG_FETCH_{catalog_reason}")
        if not market_complete:
            blocking.append(f"SNAPSHOT_FETCH_{market_reason}")
        if not records:
            blocking.append("EMPTY_UNIVERSE")
        # A failed API page blocks the whole freeze.  A malformed individual
        # quote is excluded at row level (including suspended/zero-price
        # instruments) so one bad security cannot poison the otherwise
        # complete full-market range; the exclusion remains in lineage.
        structural_block = not catalog_complete or not market_complete
        research = tuple(record for record in records if record.research_eligible)
        trade = tuple(record for record in records if record.trade_eligible)
        if structural_block:
            research = ()
            trade = ()
            blocking.append("UNIVERSE_DATA_QUALITY_BLOCK")
        elif invalid_count:
            blocking.append("ROW_DATA_QUALITY_EXCLUSIONS")
        unique_blocking = tuple(dict.fromkeys(blocking))
        fetched = fetched_at or as_of
        lineage = UniverseLineage(
            catalog_record_count=len(catalog_rows),
            market_snapshot_record_count=len(market_rows),
            total_record_count=len(records),
            research_candidate_count=len(research),
            trade_candidate_count=len(trade),
            excluded_by_reason=excluded,
        )
        return cls(
            as_of=as_of,
            fetched_at=fetched,
            records=tuple(records),
            research_candidates=research,
            trade_candidates=trade,
            ready=not structural_block and bool(trade),
            blocking_reasons=unique_blocking,
            lineage=lineage,
        )

    @classmethod
    def build(cls, *args: Any, **kwargs: Any) -> "UniverseSnapshot":
        return cls.from_records(*args, **kwargs)

    def deterministic_preselect(self, limit: int) -> tuple[SecurityRecord, ...]:
        """Select by descending turnover, then canonical symbol."""

        return self.preselect_with_lineage(limit).candidates

    def preselect_with_lineage(self, limit: int) -> PreselectResult:
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
            raise ValueError("preselect limit must be a positive integer")
        ordered = tuple(sorted(self.trade_candidates, key=lambda item: (-float(item.amount or 0.0), item.symbol)))
        selected = ordered[:limit]
        return PreselectResult(
            candidates=selected,
            limit=limit,
            source_total=len(ordered),
            eliminated_by_limit=max(0, len(ordered) - len(selected)),
        )

    preselect = deterministic_preselect

    @property
    def blocked(self) -> bool:
        return not self.ready


class CandidateFailure(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    stage: Literal["history", "fundamental"]
    reason_code: str


class FrozenInputSnapshot(BaseModel):
    """Canonical, replayable input boundary for A1/A2/A3."""

    model_config = ConfigDict(frozen=True)

    snapshot_id: str
    as_of: datetime
    fetch_timestamps: dict[str, datetime]
    source_checksums: dict[str, str]
    universe_candidates: tuple[SecurityRecord, ...]
    research_candidates: tuple[SecurityRecord, ...]
    trade_candidates: tuple[SecurityRecord, ...]
    daily_payload: dict[str, Any] = Field(default_factory=dict)
    fundamental_payload: dict[str, Any] = Field(default_factory=dict)
    technical_payload: dict[str, Any] = Field(default_factory=dict)
    fact_payload: dict[str, Any] = Field(default_factory=dict)
    candidate_failures: tuple[CandidateFailure, ...] = ()
    max_candidates: int = Field(ge=1)
    snapshot_hash: str = Field(min_length=64, max_length=64)

    @field_validator("as_of")
    @classmethod
    def as_of_shanghai(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("snapshot as_of must be timezone-aware")
        return value.astimezone(SHANGHAI)

    @field_validator("fetch_timestamps", mode="before")
    @classmethod
    def timestamps_shanghai(cls, value: Mapping[str, datetime]) -> dict[str, datetime]:
        if not isinstance(value, Mapping):
            raise ValueError("fetch_timestamps must be a mapping")
        result: dict[str, datetime] = {}
        for key, timestamp in value.items():
            if isinstance(timestamp, str):
                try:
                    timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                except ValueError as exc:
                    raise ValueError("fetch timestamps must be timezone-aware") from exc
            if not isinstance(timestamp, datetime) or timestamp.tzinfo is None or timestamp.utcoffset() is None:
                raise ValueError("fetch timestamps must be timezone-aware")
            result[str(key)] = timestamp.astimezone(SHANGHAI)
        return result

    @model_validator(mode="after")
    def hash_matches(self) -> "FrozenInputSnapshot":
        if self.snapshot_hash != _snapshot_hash(self.model_dump(exclude={"snapshot_hash"})):
            raise ValueError("snapshot_hash does not match canonical content")
        return self

    @property
    def factor_payload(self) -> dict[str, Any]:
        """Alias used by A3 callers; it is part of the hashed payload."""

        return self.technical_payload

    @classmethod
    def freeze(
        cls,
        universe: UniverseSnapshot,
        *,
        as_of: datetime | None = None,
        snapshot_id: str | None = None,
        fetch_timestamps: Mapping[str, datetime] | None = None,
        source_checksums: Mapping[str, str] | None = None,
        daily_payload: Mapping[str, Any] | None = None,
        fundamental_payload: Mapping[str, Any] | None = None,
        technical_payload: Mapping[str, Any] | None = None,
        fact_payload: Mapping[str, Any] | None = None,
        factor_payload: Mapping[str, Any] | None = None,
        history_fetcher: Callable[[str], Any] | None = None,
        fundamental_fetcher: Callable[[str], Any] | None = None,
        max_candidates: int = 50,
    ) -> "FrozenInputSnapshot":
        if not isinstance(max_candidates, int) or isinstance(max_candidates, bool) or max_candidates < 1:
            raise ValueError("max_candidates must be a positive integer")
        effective_as_of = as_of or universe.as_of
        selected = universe.deterministic_preselect(max_candidates) if universe.ready else ()
        daily: dict[str, Any] = {_payload_key(key): _json_ready(value) for key, value in (daily_payload or {}).items()}
        fundamental: dict[str, Any] = {_payload_key(key): _json_ready(value) for key, value in (fundamental_payload or {}).items()}
        failures: list[CandidateFailure] = []
        accepted: list[SecurityRecord] = []
        for candidate in selected:
            symbol = candidate.symbol
            history_value = daily.get(symbol)
            if history_fetcher is not None:
                history_result = history_fetcher(symbol)
                ok, payload, reason = _consume_fetch_result(history_result)
                if not ok:
                    failures.append(CandidateFailure(symbol=symbol, stage="history", reason_code=reason))
                    continue
                daily[symbol] = payload
                history_value = payload
            if not _payload_has_rows(history_value):
                failures.append(CandidateFailure(symbol=symbol, stage="history", reason_code="HISTORY_DATA_MISSING"))
                continue
            fundamental_value = fundamental.get(symbol)
            if fundamental_fetcher is not None:
                fundamental_result = fundamental_fetcher(symbol)
                ok, payload, reason = _consume_fetch_result(fundamental_result)
                if not ok:
                    failures.append(CandidateFailure(symbol=symbol, stage="fundamental", reason_code=reason))
                    continue
                fundamental[symbol] = payload
                fundamental_value = payload
            if not _payload_has_rows(fundamental_value):
                failures.append(CandidateFailure(symbol=symbol, stage="fundamental", reason_code="FUNDAMENTAL_DATA_MISSING"))
                continue
            accepted.append(candidate)

        timestamps = dict(fetch_timestamps or {"universe": universe.fetched_at})
        timestamps.setdefault("as_of", effective_as_of)
        checksums = dict(source_checksums or {})
        checksums.setdefault("universe", _checksum(universe.records))
        checksums.setdefault("daily", _checksum(daily))
        checksums.setdefault("fundamental", _checksum(fundamental))
        normalized_facts = {
            str(key): _json_ready(value) for key, value in (fact_payload or {}).items()
        }
        checksums.setdefault("facts", _checksum(normalized_facts))
        # A caller-supplied id remains stable; otherwise the canonical body is
        # hashed first and a short deterministic id is derived from it.
        body = dict(
            snapshot_id=snapshot_id or "pending",
            as_of=effective_as_of,
            fetch_timestamps=timestamps,
            source_checksums=checksums,
            universe_candidates=universe.records,
            research_candidates=universe.research_candidates,
            trade_candidates=tuple(accepted),
            daily_payload=daily,
            fundamental_payload=fundamental,
            technical_payload={
                _payload_key(key): _json_ready(value)
                for key, value in (technical_payload or factor_payload or {}).items()
            },
            fact_payload=normalized_facts,
            candidate_failures=tuple(failures),
            max_candidates=max_candidates,
        )
        if not snapshot_id:
            body["snapshot_id"] = "snap-" + _snapshot_hash(body)[:24]
        digest = _snapshot_hash(body)
        return cls(**body, snapshot_hash=digest)

    create = freeze
    from_universe = freeze

    def verify_hash(self) -> bool:
        return self.snapshot_hash == _snapshot_hash(self.model_dump(exclude={"snapshot_hash"}))

    def write_json(self, path: str | Path) -> Path:
        """Write canonical JSON with an atomic same-directory replacement."""

        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        content = _canonical_json(self.model_dump(mode="json"))
        handle = tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=target.parent, prefix=f".{target.name}.", suffix=".tmp", delete=False)
        temporary = Path(handle.name)
        try:
            handle.write(content)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
            handle.close()
            os.replace(temporary, target)
        except Exception:
            try:
                handle.close()
            except Exception:
                pass
            try:
                temporary.unlink(missing_ok=True)
            except Exception:
                pass
            raise
        return target

    @classmethod
    def read_json(cls, path: str | Path) -> "FrozenInputSnapshot":
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls.model_validate(data)


def _rows_and_status(value: Any) -> tuple[tuple[Any, ...], bool, str]:
    if isinstance(value, HithinkFetchResult):
        return value.items, bool(value.ok and value.complete), value.reason_code
    if value is None:
        return (), True, "NOT_PROVIDED"
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(value), True, "OK"
    return (), False, "MALFORMED_INPUT"


def _row_dict(row: Mapping[str, Any] | HithinkRow) -> dict[str, Any]:
    if isinstance(row, HithinkRow):
        return row.model_dump(mode="python")
    if isinstance(row, Mapping):
        return {str(key): value for key, value in row.items()}
    return {}


def _canonical_from_row(row: Mapping[str, Any]) -> str | None:
    for key in ("thscode", "ths_code", "thsCode", "symbol", "ticker", "security_code", "code"):
        value = row.get(key)
        if value in (None, ""):
            continue
        parsed = _canonical_symbol(str(value), row.get("exchange") or row.get("market") or row.get("交易所"))
        if parsed:
            return parsed
    return None


def _canonical_symbol(value: str, exchange: Any = None) -> str | None:
    text = str(value).strip().upper().replace("XSHG", "SH").replace("XSHE", "SZ")
    text = text.replace("-", ".")
    match = _CANONICAL.fullmatch(text)
    if match:
        code, suffix = match.groups()
        suffix = {"SH": "SH", "SZ": "SZ", "BJ": "BJ"}.get(suffix)
        return f"{code}.{suffix}" if suffix else None
    if "." in text:
        left, right = text.split(".", 1)
        if _CODE.fullmatch(left):
            text, exchange = left, right
    digits = re.sub(r"\D", "", text)
    if len(digits) != 6:
        return None
    exch = str(exchange or "").strip().upper()
    exch = {"XSHG": "SH", "XSHE": "SZ", "1": "SH", "2": "SZ", "SH": "SH", "SZ": "SZ", "BJ": "BJ", "BSE": "BJ"}.get(exch, exch)
    if exch not in {"SH", "SZ", "BJ"}:
        if digits.startswith("6"):
            exch = "SH"
        elif digits.startswith(("0", "2", "3")):
            exch = "SZ"
        elif digits.startswith(("4", "8")):
            exch = "BJ"
        else:
            return None
    return f"{digits}.{exch}"


def _security_record(
    row: Mapping[str, Any],
    *,
    symbol: str,
    index: int,
    in_catalog: bool,
    in_snapshot: bool,
    as_of: date,
    gate_policy: UniverseGatePolicy,
) -> tuple[SecurityRecord, tuple[str, ...]]:
    match = _CANONICAL.fullmatch(symbol)
    code, exchange = (match.group(1), match.group(2)) if match else ("INVALID", "INVALID")
    name_value = _first_value(row, ("name", "security_name", "stock_name", "name_zh", "简称", "名称"))
    name = str(name_value).strip() if name_value not in (None, "") else ""
    reasons: list[str] = []
    if not match or not _CODE.fullmatch(code):
        reasons.append("MISSING_CODE")
    if not name:
        reasons.append("MISSING_NAME")
    if not in_snapshot:
        reasons.append("MISSING_MARKET_SNAPSHOT")
    if not in_catalog:
        reasons.append("NOT_IN_CATALOG")
    price = _numeric(_first_value(row, ("price", "current", "last", "last_price", "close", "close_price", "最新价", "收盘价")))
    volume = _numeric(_first_value(row, ("volume", "vol", "成交量")))
    amount = _numeric(_first_value(row, ("amount", "turnover", "turnover_amount", "成交额")))
    change_ratio_pct = _numeric(
        _first_value(row, ("price_change_ratio_pct", "change_ratio_pct", "change_pct", "pct_chg", "涨跌幅"))
    )
    if change_ratio_pct is None:
        previous = _numeric(_first_value(row, ("prev_price", "previous_close", "pre_close", "昨收")))
        if price is not None and previous is not None and previous > 0:
            change_ratio_pct = (price / previous - 1.0) * 100.0
    if price is None or price <= 0:
        reasons.append("INVALID_PRICE")
    if volume is None or volume < 0:
        reasons.append("INVALID_VOLUME")
    if amount is None or amount < 0:
        reasons.append("INVALID_AMOUNT")
    elif amount < gate_policy.minimum_daily_turnover_cny:
        reasons.append("MINIMUM_TURNOVER_NOT_MET")
    upper_name = name.upper()
    if _ST.search(upper_name) or bool(row.get("is_st") or row.get("st_flag") or row.get("risk_warning")):
        reasons.append("ST_RISK")
    status = str(_first_value(row, ("status", "listing_status", "state")) or "").lower()
    suspended = bool(
        row.get("is_suspended")
        or row.get("suspended")
        or row.get("suspend_flag")
        or "停牌" in status
        or (volume == 0 and price is not None and price > 0)
    )
    if gate_policy.block_suspended and suspended:
        reasons.append("SUSPENDED")
    if any(word in upper_name or word in status for word in _DELIST_WORDS):
        reasons.append("DELIST_RISK")
    listing_date = _date_value(
        _first_value(row, ("listing_date", "listed_date", "list_date", "ipo_date", "上市日期"))
    )
    if listing_date is not None:
        listed_days = (as_of - listing_date).days
        if listed_days < gate_policy.newly_listed_min_days:
            reasons.append("NEWLY_LISTED")
        if gate_policy.block_no_price_limit_new_listing and listed_days < 5:
            reasons.append("NO_PRICE_LIMIT_NEW_LISTING")
    if exchange == "BJ":
        reasons.append("BJ_RESEARCH_ONLY")
    reasons = list(dict.fromkeys(reasons))
    structural = {"MISSING_CODE", "MISSING_NAME", "MISSING_MARKET_SNAPSHOT", "INVALID_PRICE", "INVALID_VOLUME", "INVALID_AMOUNT"}
    gate_blocks = {
        "ST_RISK",
        "DELIST_RISK",
        "NOT_IN_CATALOG",
        "MINIMUM_TURNOVER_NOT_MET",
        "SUSPENDED",
        "NEWLY_LISTED",
        "NO_PRICE_LIMIT_NEW_LISTING",
    }
    eligible = not any(reason in structural or reason in gate_blocks for reason in reasons)
    research_eligible = eligible and exchange in {"SH", "SZ", "BJ"}
    trade_eligible = research_eligible and exchange in {"SH", "SZ"}
    return SecurityRecord(
        symbol=symbol if match else f"INVALID:{index}",
        code=code,
        exchange=exchange,  # type: ignore[arg-type]
        name=name,
        price=price,
        volume=volume,
        amount=amount,
        change_ratio_pct=change_ratio_pct,
        research_eligible=research_eligible,
        trade_eligible=trade_eligible,
        exclusion_reasons=tuple(reasons),
    ), tuple(reasons)


def _first_value(row: Mapping[str, Any], keys: Sequence[str]) -> Any:
    lowered = {str(key).lower(): value for key, value in row.items()}
    for key in keys:
        if key in row:
            return row[key]
        if key.lower() in lowered:
            return lowered[key.lower()]
    return None


def _numeric(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _date_value(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value in (None, ""):
        return None
    text = str(value).strip()
    if text.isdigit() and len(text) == 8:
        text = f"{text[:4]}-{text[4:6]}-{text[6:]}"
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _payload_has_rows(value: Any) -> bool:
    if isinstance(value, HithinkFetchResult):
        return bool(value.ok and value.complete and value.items)
    if isinstance(value, Mapping):
        if "items" in value or "item" in value:
            rows = value.get("items", value.get("item"))
            return isinstance(rows, Sequence) and not isinstance(rows, (str, bytes, bytearray)) and bool(rows)
        return bool(value)
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)) and bool(value)


def _payload_key(value: Any) -> str:
    parsed = _canonical_symbol(str(value))
    return parsed or str(value).strip().upper()


def _consume_fetch_result(value: Any) -> tuple[bool, Any, str]:
    if isinstance(value, HithinkFetchResult):
        if not value.ok or not value.complete:
            return False, None, value.reason_code
        payload = [row.model_dump(mode="json") for row in value.items]
        return bool(payload), payload, "OK" if payload else "EMPTY_DATA"
    if isinstance(value, Mapping) or (isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))):
        return _payload_has_rows(value), _json_ready(value), "OK" if _payload_has_rows(value) else "EMPTY_DATA"
    return False, None, "MALFORMED_RESULT"


def _json_ready(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return _json_ready(value.model_dump(mode="python"))
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_ready(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("non-finite value cannot enter a frozen snapshot")
    return value


def _canonical_json(value: Any) -> str:
    return json.dumps(_json_ready(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _snapshot_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _checksum(value: Any) -> str:
    return _snapshot_hash(value)


__all__ = [
    "CandidateFailure",
    "FrozenInputSnapshot",
    "PreselectResult",
    "SecurityRecord",
    "UniverseLineage",
    "UniverseGatePolicy",
    "UniverseSnapshot",
]
