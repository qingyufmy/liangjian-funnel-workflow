"""One current-session OHLCV view for quant, model context and diagnostics."""
from __future__ import annotations

import hashlib
import json
import math

from .session_windows import closed_window_ends


def execution_evidence(one_minute, native_five, *, as_of):
    from ..runtime.strategies import aggregate_closed_bars

    rows = [bar.model_dump(mode="json") for bar in one_minute]
    aggregated = aggregate_closed_bars(rows, as_of=as_of)
    native = {bar.bar_end.isoformat(): bar for bar in native_five}
    mismatches = []
    limited = []
    compared = 0
    one_sources = {bar.source_id for bar in one_minute}
    native_sources = {bar.source_id for bar in native_five}
    independent = bool(native_five) and not bool(native_sources & one_sources)
    for row in aggregated["5m"]:
        other = native.get(row["bar_end"])
        if other is None:
            continue
        # The frozen execution series excludes Tencent's separate 09:30
        # auction print; native 09:35 includes it. These are different scopes,
        # not interchangeable candles. Preserve the comparison limitation.
        if row["bar_end"][11:16] == "09:35" and other.source_id == "TENCENT:ifzq.gtimg.cn":
            limited.append({"end": row["bar_end"], "reason": "OPEN_AUCTION_SCOPE_DIFFERENT"})
            continue
        compared += 1
        # Tencent amount is OHLC-estimated: an aggregated estimate is not
        # comparable with a separately estimated native five-minute notional.
        for field in ("open", "high", "low", "close", "volume"):
            left, right = float(row[field]), float(getattr(other, field))
            if not math.isclose(left, right, rel_tol=0, abs_tol=1e-7):
                if not independent:
                    # A provider's native five-minute series is not an
                    # independent check of its own one-minute rows.  Tencent
                    # in particular includes session-opening prints in the
                    # 09:35/13:05 native candle that are deliberately absent
                    # from the continuous-session execution series.  Preserve
                    # the difference for audit, but never turn it into an A4
                    # execution-data conflict.
                    limited.append({"end": row["bar_end"], "field": field,
                        "derived": left, "native": right,
                        "reason": "SAME_SOURCE_AGGREGATION_SCOPE_DIFFERENT"})
                    continue
                if (field == "volume" and other.source_id == "TENCENT:ifzq.gtimg.cn"
                        and not other.symbol.startswith(("688", "689"))
                        and abs(left-right) <= 100 and left % 100 == 0 and right % 100 == 0):
                    # Retain the one-reported-lot difference as DATA_LIMITED,
                    # not MATCH. No strategy-volume threshold is changed and
                    # neither native volume nor auction data enters execution.
                    limited.append({"end": row["bar_end"], "field": field,
                        "derived": left, "native": right, "reason": "ONE_LOT_PRECISION_UNVERIFIED"})
                    continue
                mismatches.append({"end": row["bar_end"], "field": field,
                                   "derived": left, "native": right})
    expected = closed_window_ends(as_of, "1m")
    ready = tuple(bar.bar_end for bar in one_minute) == expected and bool(expected)
    evidence = {
        "contract_version": "closed-session-execution-v2",
        "market_cutoff": as_of.isoformat(),
        "source_ids": sorted({bar.source_id for bar in one_minute}),
        "normalizer_versions": sorted({bar.normalizer_version for bar in one_minute}),
        "volume_unit": "shares", "amount_kinds": sorted({bar.amount_kind for bar in one_minute}),
        "input_sha256": hashlib.sha256(json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
        "timestamp_convention": "end", "entry_window_complete": ready,
        "last_ends": {key: value[-1]["bar_end"] if value else None for key, value in aggregated.items()},
        "native_5m_comparison": {"compared": compared, "conflicts": mismatches,
            "status": "CONFLICT" if mismatches else "DATA_LIMITED" if limited else "MATCH" if compared else "UNAVAILABLE",
            "limitations": limited,
            "independent_source": independent,
            "amount_comparison": "NOT_COMPARED"},
    }
    return aggregated, evidence
