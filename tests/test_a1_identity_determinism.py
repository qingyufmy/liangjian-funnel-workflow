"""A1 identity selection must be stable across independent Python processes."""

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from liangjian_funnel.pipeline.a1_registry import _symbol_from_item


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        ({"symbol": " 000001.sz ", "code": "000001"}, "000001.SZ"),
        ({"symbol": "", "security_symbol": "600519.SH", "code": "600519"}, "600519.SH"),
        ({"symbol": None, "ticker": "300750.SZ", "code": "300750"}, "300750.SZ"),
        ({"symbol": " ", "code": "000001", "thscode": "000001.SZ"}, "000001"),
        ({"thscode": "600519.SH", "security_code": "600519"}, "600519.SH"),
        ({"security_code": "600519"}, "600519"),
        ({"symbol": "600519.SH", "code": "000001.SZ"}, "600519.SH"),
        ({}, ""),
    ],
)
def test_identity_priority_preserves_fallback_without_alias_union(row, expected):
    # No suffix inference or new conflict rejection: the first explicit field
    # remains authoritative, and fallback-only legacy rows keep their spelling.
    assert _symbol_from_item(row) == expected


_PROCESS_PROBE = """
import json
import sys
from dataclasses import asdict
from liangjian_funnel.pipeline.a1_registry import (
    _candidate_map, _symbol_from_item, build_a1_manifest,
    compute_incremental_scope, merge_a1_partitions,
)
request = json.load(sys.stdin)
data = request['snapshot']
output = request['output']
manifest = build_a1_manifest(
    data, {'lane_1': output}, mode='FULL', snapshot_id='identity-fixture',
    snapshot_hash='a' * 64, as_of='2026-10-22T15:00:00+08:00',
)
base = request.get('base_manifest', manifest)
scope = compute_incremental_scope(data, base, base_output=output)
merged = merge_a1_partitions(
    output, {'active_research_pool': [], 'monitor_pool': [], 'rejected_candidates': []},
    updated_symbols=(),
)
print(json.dumps({
    'identities': [_symbol_from_item(row) for row in data['g0_candidates']],
    'candidate_keys': sorted(_candidate_map(data)),
    'fingerprints': manifest['candidate_fingerprints'],
    'manifest': manifest, 'scope': asdict(scope), 'merged': merged,
}, sort_keys=True))
"""


def _probe(seed, request):
    env = dict(os.environ, PYTHONHASHSEED=str(seed))
    source = str(Path(__file__).resolve().parents[1] / "src")
    env["PYTHONPATH"] = os.pathsep.join(filter(None, (source, env.get("PYTHONPATH"))))
    result = subprocess.run(
        [sys.executable, "-B", "-c", _PROCESS_PROBE],
        input=json.dumps(request), text=True, encoding="utf-8", env=env,
        capture_output=True, check=True, timeout=20,
    )
    return json.loads(result.stdout)


def test_candidate_fingerprints_scope_and_merge_are_hash_seed_independent():
    symbols = ["000001.SZ", "600519.SH"]
    request = {
        "snapshot": {
            "g0_symbols": symbols,
            "g0_candidates": [
                {"symbol": symbols[0], "code": "000001", "research_eligible": True},
                {"symbol": symbols[1], "code": "600519", "research_eligible": True},
            ],
        },
        "output": {
            "active_research_pool": [{"symbol": symbols[0], "code": "000001", "score": 1}],
            "monitor_pool": [{"symbol": symbols[1], "code": "600519", "score": 2}],
            "rejected_candidates": [],
        },
    }
    baseline = _probe(1, request)
    request["base_manifest"] = baseline["manifest"]
    for seed in (0, 1, 2, 8, 37):
        actual = _probe(seed, request)
        assert actual == baseline
        assert actual["identities"] == symbols
        assert actual["candidate_keys"] == symbols
        assert actual["scope"]["symbols"] == []
        assert actual["merged"]["active_research_pool"] == request["output"]["active_research_pool"]
        assert actual["merged"]["monitor_pool"] == request["output"]["monitor_pool"]

    changed = json.loads(json.dumps(request))
    changed["snapshot"]["g0_candidates"][0]["research_eligible"] = False
    expected = _probe(1, changed)
    assert expected["scope"]["changed_symbols"] == [symbols[0]]
    assert expected["scope"]["symbols"] == [symbols[0]]
    for seed in (2, 8):
        assert _probe(seed, changed) == expected
