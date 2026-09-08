"""MACD evidence computed only from supplied, closed prices; no trade vote."""
import hashlib
import json
import math
from collections.abc import Sequence


def macd_evidence(closes: Sequence[float], *, as_of: str | None = None, adjust_mode: str | None = None) -> dict:
    values = [float(value) for value in closes]
    ready = len(values) >= 35 and all(math.isfinite(v) and v > 0 for v in values)
    result = {"available": ready, "bar_count": len(values), "as_of": as_of,
              "adjust_mode": adjust_mode, "parameters": [12, 26, 9], "hist_multiplier": 2,
              "initialization": "FIRST_CLOSE_EMA", "version": "macd-evidence/1",
              "input_hash": hashlib.sha256(json.dumps(values, separators=(",", ":")).encode()).hexdigest(),
              "dif": None, "dea": None, "hist": None}
    if not ready:
        return result
    fast = slow = values[0]
    dea = 0.0
    for value in values[1:]:
        fast = value * (2 / 13) + fast * (11 / 13)
        slow = value * (2 / 27) + slow * (25 / 27)
        dif = fast - slow
        dea = dif * .2 + dea * .8
    result.update(dif=round(dif, 6), dea=round(dea, 6), hist=round(2 * (dif - dea), 6))
    return result
