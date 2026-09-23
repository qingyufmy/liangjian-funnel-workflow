"""Strict, offline-only TDX daily archive decoder for research sidecars.

Binary layout independently implemented using the protocol notes in
simonlin1212/a-stock-data (2e0ae638, Apache-2.0). No automatic fallback or
production ingestion: archive date is not proof of historical availability.
"""
from __future__ import annotations

import hashlib
import io
import math
import struct
import zipfile
from datetime import date
from collections.abc import Collection


def decode_daily_package(raw: bytes, trade_date: str, *, equity_symbols: Collection[str]) -> dict:
    """Select only equities from an externally verified universe (000001.SZ).

    Retain missing symbols explicitly; never infer stock identity from prefixes,
    fabricate missing bars, or assign stock units to funds/indices/bonds.
    """
    day = date.fromisoformat(trade_date)
    if len(raw) > 16 * 1024 * 1024:
        raise ValueError("archive compressed size exceeded")
    universe = set(equity_symbols)
    for symbol in universe:
        if (len(symbol) != 9 or symbol[6:] not in (".SH", ".SZ", ".BJ")
                or not symbol[:6].isascii() or not symbol[:6].isdigit()):
            raise ValueError("invalid equity universe symbol")
    expected = {f"{m}{day:%y%m%d}.{ext}" for m in ("sh", "sz", "bj") for ext in ("cod", "md1")}
    rows = []
    unavailable = []
    counts = {}
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        infos = archive.infolist()
        if len(infos) != 6 or {x.filename for x in infos} != expected:
            raise ValueError("archive date, markets or duplicate members invalid")
        if sum(x.file_size for x in infos) > 64 * 1024 * 1024 or any(x.flag_bits & 1 for x in infos):
            raise ValueError("archive expanded size or encryption invalid")
        for market in ("sh", "sz", "bj"):
            cod = archive.read(f"{market}{day:%y%m%d}.cod")
            bars = archive.read(f"{market}{day:%y%m%d}.md1")
            if not cod or len(cod) % 150 or len(bars) % 512 or len(cod)//150 != len(bars)//512:
                raise ValueError("archive record lengths inconsistent")
            counts[market] = len(cod)//150
            codes, sequences = set(), set()
            for offset in range(0, len(cod), 150):
                record = cod[offset:offset+150]
                code = record[:6].decode("ascii")
                seq = struct.unpack_from("<H", record, 32)[0]
                if not code.isdigit() or code in codes or seq in sequences or seq >= len(bars)//512:
                    raise ValueError("archive identity/index invalid")
                codes.add(code)
                sequences.add(seq)
                symbol = f"{code}.{market.upper()}"
                if symbol not in universe:
                    continue
                name = record[40:72].split(b"\0", 1)[0].decode("gbk").strip()
                block = bars[seq*512:(seq+1)*512]
                previous, opening, high, low, close = struct.unpack_from("<5d", block, 4)
                volume = struct.unpack_from("<Q", block, 56)[0]
                amount = struct.unpack_from("<d", block, 72)[0]
                if not name or not all(math.isfinite(x) for x in (previous, opening, high, low, close, amount)):
                    raise ValueError("archive nonfinite price or missing name")
                if close <= 0:
                    unavailable.append(dict(symbol=symbol, reason="NO_VALID_CLOSE"))
                    continue
                if opening == high == low == volume == amount == 0 and previous == close:
                    unavailable.append(dict(symbol=symbol, reason="NO_TRADING_BAR_STATUS_UNVERIFIED"))
                    continue
                if min(previous, opening, low) <= 0 or not low <= min(opening, close) <= max(opening, close) <= high or amount < 0:
                    raise ValueError(f"archive OHLC/amount invalid: {symbol} {previous, opening, high, low, close, volume, amount}")
                rows.append(dict(symbol=symbol, name=name, trade_date=trade_date,
                                 previous_close=previous, open=opening, high=high, low=low,
                                 close=close, volume_shares=volume, amount_cny=amount))
    found = {x["symbol"] for x in rows}
    return dict(source="TDX_OFFICIAL_DAILY_PACKAGE", trade_date=trade_date,
                archive_sha256=hashlib.sha256(raw).hexdigest(), adjustment="RAW",
                shadow_only=True, execution_authority=False, historical_availability_verified=False,
                archive_security_counts=counts, requested_equity_count=len(universe),
                unavailable_records=unavailable,
                missing_symbols=sorted(universe-found), rows=sorted(rows, key=lambda x:x["symbol"]))
