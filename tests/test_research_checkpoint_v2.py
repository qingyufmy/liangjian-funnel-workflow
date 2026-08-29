import json
from pathlib import Path

from liangjian_funnel.pipeline.research_checkpoint import (
    FileResearchCheckpointStore,
    InMemoryResearchCheckpointStore,
    ResearchCheckpointKey,
)


def _key(**overrides) -> ResearchCheckpointKey:
    values = {
        "run_id": "run-1",
        "lane": "lane_1",
        "stage": "A2",
        "model": "model-a",
        "prompt_hash": "prompt-a",
        "snapshot_hash": "snapshot-a",
        "batch_symbols_hash": "symbols-a",
    }
    values.update(overrides)
    return ResearchCheckpointKey(**values)


def test_v2_identity_fields_change_digest_and_are_serialized():
    legacy = _key()
    current = _key(
        generation_id="feature-g1",
        pipeline_contract_hash="pipeline-h1",
        feature_contract_hash="feature-h1",
        code_commit="abc123",
        provider_contract_hash="provider-h1",
    )

    assert legacy.has_v2_identity is False
    assert current.has_v2_identity is True
    assert current.digest != legacy.digest
    assert current.as_dict()["generation_id"] == "feature-g1"
    assert current.as_dict()["provider_contract_hash"] == "provider-h1"
    assert current.legacy_as_dict() == legacy.legacy_as_dict()


def test_in_memory_store_rejects_mismatched_v2_contract_and_keeps_legacy_compatibility():
    store = InMemoryResearchCheckpointStore()
    legacy = _key()
    current = _key(generation_id="feature-g1", pipeline_contract_hash="pipeline-h1")
    record = {"status": "VALIDATED", "audit": {"symbols": ["600000.SH"]}}

    # Simulate a v1 record written before v2 fields existed.
    store._records[legacy.legacy_digest] = {"key": legacy.legacy_as_dict(), **record}
    assert store.load(legacy) == {"key": legacy.legacy_as_dict(), **record}
    assert store.load_strict(legacy) is None
    assert store.load(current) is None

    store.save(current, record)
    assert store.load(current)["key"] == current.as_dict()
    assert store.load_strict(current)["key"] == current.as_dict()
    assert store.load(_key(generation_id="feature-g2", pipeline_contract_hash="pipeline-h1")) is None


def test_file_store_can_read_v1_key_only_non_strict_but_strict_rejects_it(tmp_path: Path):
    store = FileResearchCheckpointStore(tmp_path / "checkpoints")
    legacy = _key()
    current = _key(generation_id="feature-g1", pipeline_contract_hash="pipeline-h1")
    old_path = store._legacy_path(legacy)
    old_path.parent.mkdir(parents=True)
    old_path.write_text(
        json.dumps({"key": legacy.legacy_as_dict(), "status": "VALIDATED"}),
        encoding="utf-8",
    )

    assert store.load(legacy)["status"] == "VALIDATED"
    assert store.load_strict(legacy) is None
    assert store.load(current) is None

    store.save(current, {"status": "VALIDATED"})
    assert store.load_strict(current)["key"] == current.as_dict()
