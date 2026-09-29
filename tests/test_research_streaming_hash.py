import hashlib
from datetime import date
from pathlib import Path

import pytest

from liangjian_funnel.pipeline.research import _canonical_json, _sha256_json
from liangjian_funnel.pipeline import research, a1_registry


@pytest.mark.parametrize("value", [
    {"中文": [1, 1.5, None, True, "行情"], "date": date(2026, 9, 29)},
    {"path": Path("facts/证据"), "nested": {3: (1, 2)}, "empty": []},
    {"large": [{"symbol": f"{i:06d}", "evidence": "原始事实" * 100} for i in range(2000)]},
])
def test_streaming_hash_preserves_existing_evidence_identity(value):
    assert _sha256_json(value) == hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()
    assert research._canonical_json_size(value) == len(_canonical_json(value))


def test_native_snapshot_does_not_duplicate_object_graph(monkeypatch):
    value = {"stocks": [{"symbol": "600000.SH", "data": [1, None, "证据"]}]}
    expected = hashlib.sha256(_canonical_json(value).encode()).hexdigest()
    def forbid_copy(_):
        raise AssertionError("native snapshot must not be recursively copied")
    monkeypatch.setattr(research, "_jsonable", forbid_copy)
    assert research._json_value(value) is value
    assert _sha256_json(value) == expected
    value["stocks"][0]["data"].append(2)
    assert _sha256_json(value) != expected  # no stale identity cache


def test_special_values_preserve_legacy_normalization():
    value = {3: (date(2026, 9, 29), Path("证据")), "sets": frozenset([1, 2])}
    import json
    expected = json.dumps(research._jsonable(value), ensure_ascii=False,
                          sort_keys=True, separators=(",", ":"), default=str)
    assert _canonical_json(value) == expected
    assert _sha256_json(value) == hashlib.sha256(expected.encode()).hexdigest()


def test_registry_hash_is_byte_compatible():
    value = {"manifest": [{"date": date(2026, 9, 29), "evidence": "中文" * 1000}]}
    assert a1_registry.content_hash(value) == hashlib.sha256(a1_registry.canonical_json(value).encode()).hexdigest()
