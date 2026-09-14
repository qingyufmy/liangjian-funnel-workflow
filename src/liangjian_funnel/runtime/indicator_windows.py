"""Content-addressed indicator evidence, separate from mutable plan state."""
import hashlib
import json
from pathlib import Path
import re


def _bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def freeze_observations(root: Path, observations: dict, symbol: str) -> dict:
    result = dict(observations)
    for name, declared in observations.items():
        if not isinstance(declared, dict) or not isinstance(declared.get("input_series"), list):
            continue
        window = {"symbol": symbol, "indicator": name, "timeframe": declared.get("timeframe"),
                  "parameters": declared.get("parameters"), "input_series": declared["input_series"]}
        raw = _bytes(window)
        digest = hashlib.sha256(raw).hexdigest()
        root.mkdir(parents=True, exist_ok=True)
        path = root / f"{digest}.json"
        try:
            with path.open("xb") as f:
                f.write(raw)
        except FileExistsError:
            if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
                raise ValueError("INDICATOR_WINDOW_HASH_MISMATCH")
        result[name] = {k: v for k, v in declared.items() if k != "input_series"}
        result[name]["input_ref"] = digest
    return result


def load_window(root: Path, digest: str) -> dict:
    if not re.fullmatch(r"[a-f0-9]{64}", digest):
        raise ValueError("INVALID_INDICATOR_REFERENCE")
    path = root / f"{digest}.json"
    if path.stat().st_size > 1_000_000:
        raise ValueError("INDICATOR_WINDOW_TOO_LARGE")
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != digest:
        raise ValueError("INDICATOR_WINDOW_HASH_MISMATCH")
    return json.loads(raw)
