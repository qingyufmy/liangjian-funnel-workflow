"""Offline-only consumer of caller-supplied, SHA-bound original Bar15m JSON.

No example prices are generated. This is not provider attestation or permission.
"""
from pathlib import Path
import argparse
from datetime import datetime
import hashlib
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bootstrap import enable
ROOT = enable()
sys.path.insert(0, str(ROOT / "snapshot" / "scripts"))
from track2_archive_dk import decode_bar
from liangjian_funnel.market_state.calendar import market_calendar
from liangjian_funnel.market_state.dk_shadow import json_value
from liangjian_funnel.market_state.ssl import dk_state, evaluate_ssl
from dataclasses import asdict

def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("DK_EXAMPLE_DUPLICATE_JSON_KEY")
        result[key] = value
    return result

def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--observed-at", required=True)
    parser.add_argument("--period", type=int, choices=(10, 20, 30), default=20)
    args = parser.parse_args(argv)
    raw = args.input.read_bytes()
    if hashlib.sha256(raw).hexdigest() != args.sha256:
        raise ValueError("DK_EXAMPLE_INPUT_SHA_MISMATCH")
    payload = json.loads(raw, object_pairs_hook=_unique)
    if not isinstance(payload, list) or not payload:
        raise ValueError("DK_EXAMPLE_REAL_INPUT_REQUIRED")
    bars = [decode_bar(row) for row in payload]
    observed = datetime.fromisoformat(args.observed_at)
    calendar = market_calendar()
    point = dk_state(bars, period=args.period, observed_at=observed, calendar=calendar)
    result = dict(authority="SHADOW_ONLY", enabled=False,
        input_evidence="CALLER_SUPPLIED_HASH_CHECKED_NOT_PROVIDER_ATTESTATION",
        input_sha256=args.sha256, current=json_value(asdict(point)),
        evaluation=evaluate_ssl(bars, period=args.period, observed_at=observed, calendar=calendar))
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return result

if __name__ == "__main__":
    main()
