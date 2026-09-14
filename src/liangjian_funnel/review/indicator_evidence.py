"""Independent formula checks; a formula match is not a source-provenance match."""
from __future__ import annotations

import hashlib
import json
import math
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import datetime
from statistics import median
from typing import Any


def macd(values: Sequence[float]) -> dict[str, float]:
    if len(values) < 2:
        return {}
    fast = slow = float(values[0])
    signal = 0.0
    for value in values[1:]:
        fast += (value - fast) * (2.0 / 13)
        slow += (value - slow) * (2.0 / 27)
        signal += (fast - slow - signal) * 0.2
    return {"dif": fast - slow, "dea": signal, "hist": 2 * (fast - slow - signal)}


def kdj(rows: Sequence[Mapping[str, Any]]) -> dict[str, float]:
    if len(rows) < 9:
        return {}
    k = d = 50.0
    for index in range(8, len(rows)):
        window = rows[index - 8:index + 1]
        low, high = min(float(r["low"]) for r in window), max(float(r["high"]) for r in window)
        rsv = 50.0 if high <= low + 1e-9 else 100 * (float(rows[index]["close"]) - low) / (high - low)
        k = (2 * k + rsv) / 3
        d = (2 * d + k) / 3
    return {"k": k, "d": d, "j": 3 * k - 2 * d}


def formula_status(declared: Mapping[str, Any], computed: Mapping[str, float]) -> str:
    if not computed or any(not isinstance(declared.get(k), (int, float))
                           or isinstance(declared[k], bool) or not math.isfinite(declared[k]) for k in computed):
        return "DATA_LIMITED"
    return "MATCH" if all(abs(float(declared[k]) - v) <= 0.000002 for k, v in computed.items()) else "MISMATCH"


def daily_macd_check(declared: Mapping[str, Any], closes: Sequence[float]) -> dict[str, Any]:
    digest = hashlib.sha256(json.dumps(list(closes), separators=(",", ":")).encode()).hexdigest()
    computed = macd(closes) if len(closes) >= 35 else {}
    return {"formula_status": formula_status(declared, computed), "recomputed": computed,
            "bar_count": len(closes), "input_hash": digest,
            "input_hash_status": ("MATCH" if declared["input_hash"] == digest else "MISMATCH")
            if declared.get("input_hash") else "MISSING_DECLARATION"}


def audit_event_indicators(events: Sequence[Mapping[str, Any]], *, strategy_profile: str = "", window_loader=None, symbol: str = "") -> dict[str, Any]:
    counts: dict[str, Counter] = {"m15_macd": Counter(), "kdj": Counter(), "volume_5m": Counter()}
    issues = []
    for event in events:
        payload = event.get("payload_json", {})
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except (ValueError, TypeError):
                payload = {}
        strategy = payload.get("strategy") or {}
        observation = strategy.get("indicator_observations") or {}
        profile = strategy.get("strategy_profile") or strategy_profile
        for name, counter in counts.items():
            declared = observation.get(name)
            if not isinstance(declared, Mapping):
                early = set(strategy.get("reason_codes") or []) & {"NO_CLOSED_5M", "NO_CLOSED_15M"}
                counter["NOT_YET_EVALUABLE" if early else "DECLARATION_MISSING" if profile == "MA520_SWING" or (name == "volume_5m" and profile in {"TREND_MA5", "LEADER_INTRADAY"}) else "NOT_APPLICABLE" if profile in {"TREND_MA5", "LEADER_INTRADAY"} else "APPLICABILITY_UNKNOWN"] += 1
                continue
            status = "WARMUP_INCOMPLETE" if not declared.get("available") else "INPUT_EVIDENCE_MISSING"
            series = declared.get("input_series")
            if series is None and declared.get("input_ref") and window_loader:
                try:
                    window = window_loader(declared["input_ref"])
                    if (window.get("symbol") != (symbol or payload.get("symbol")) or window.get("indicator") != name
                            or window.get("timeframe") != declared.get("timeframe") or window.get("parameters") != declared.get("parameters")):
                        raise ValueError("INDICATOR_REFERENCE_IDENTITY_MISMATCH")
                    series = window["input_series"]
                except (OSError, ValueError, KeyError, TypeError):
                    status = "INVALID_INPUT_EVIDENCE"
            if declared.get("available") and isinstance(series, list) and series:
                try:
                    cutoff = datetime.fromisoformat(str(event["minute_end"]))
                    times = [datetime.fromisoformat(str(r["end"])) for r in series]
                    valid = (all(t.tzinfo and t <= cutoff for t in times)
                             and times == sorted(set(times)) and len(series) == declared.get("bar_count")
                             and str(series[-1]["end"]) == declared.get("closed_bar_end"))
                    parameters = {"m15_macd": {"fast": 12, "slow": 26, "signal": 9, "hist_scale": 2, "initialization": "FIRST_CLOSE"},
                                  "kdj": {"period": 9, "k_smooth": 3, "d_smooth": 3, "initial_k": 50, "initial_d": 50},
                                  "volume_5m": {"baseline": "MEDIAN_PREVIOUS_CURRENT_SESSION"}}
                    valid = valid and (declared.get("parameters") is None or declared["parameters"] == parameters[name])
                    fields = ("close",) if name == "m15_macd" else ("volume",) if name == "volume_5m" else ("high", "low", "close")
                    valid = valid and all(math.isfinite(float(r[k])) and (float(r[k]) >= 0 if k == "volume" else float(r[k]) > 0) for r in series for k in fields)
                    if name == "volume_5m":
                        valid = valid and declared.get("volume_unit") == "shares"
                        computed = {"latest_volume": float(series[-1]["volume"]), "median_previous_volume": median(float(r["volume"]) for r in series[:-1])} if len(series) > 1 else {}
                    else:
                        computed = macd([float(r["close"]) for r in series]) if name == "m15_macd" else kdj(series)
                    status = formula_status(declared, computed) if valid else "INVALID_INPUT_EVIDENCE"
                except (ValueError, KeyError, TypeError, OverflowError):
                    status = "INVALID_INPUT_EVIDENCE"
            counter[status] += 1
            if name == "m15_macd" and declared.get("available") and not declared.get("warmup_complete"):
                counter["AVAILABLE_BUT_NOT_WARMED"] += 1
            if status != "MATCH" and len(issues) < 20:
                issues.append({"minute_end": event.get("minute_end"), "indicator": name, "status": status})
    return {"scope": "FROZEN_DECLARED_INPUT_FORMULA_ONLY", "raw_source_independently_verified": False,
            "expected_event_count": len(events), "strategy_profile": strategy_profile,
            "counts": {k: dict(v) for k, v in counts.items()}, "issue_samples": issues}
