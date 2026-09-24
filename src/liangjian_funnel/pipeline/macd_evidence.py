"""MACD evidence computed only from supplied, closed prices; no trade vote."""
import hashlib
import json
import math
from collections.abc import Sequence


def macd_evidence(closes: Sequence[float], *, as_of: str | None = None, adjust_mode: str | None = None, parameters: tuple[int, int, int] = (12, 26, 9)) -> dict:
    if len(parameters) != 3 or any(type(p) is not int or p <= 0 for p in parameters) or parameters[0] >= parameters[1]:
        raise ValueError("INVALID_MACD_PARAMETERS")
    fast_period, slow_period, signal_period = parameters
    values = [float(value) for value in closes]
    ready = len(values) >= max(35, slow_period + signal_period) and all(math.isfinite(v) and v > 0 for v in values)
    result = {"available": ready, "bar_count": len(values), "as_of": as_of,
              "adjust_mode": adjust_mode, "parameters": list(parameters), "hist_multiplier": 2,
              "initialization": "FIRST_CLOSE_EMA", "version": "macd-evidence/1",
              "input_hash": hashlib.sha256(json.dumps(values, separators=(",", ":")).encode()).hexdigest(),
              "dif": None, "dea": None, "hist": None}
    if not ready:
        return result
    fast = slow = values[0]
    dea = 0.0
    for value in values[1:]:
        fast = value * (2 / (fast_period + 1)) + fast * (1 - 2 / (fast_period + 1))
        slow = value * (2 / (slow_period + 1)) + slow * (1 - 2 / (slow_period + 1))
        dif = fast - slow
        dea = dif * (2 / (signal_period + 1)) + dea * (1 - 2 / (signal_period + 1))
    result.update(dif=round(dif, 6), dea=round(dea, 6), hist=round(2 * (dif - dea), 6))
    return result
