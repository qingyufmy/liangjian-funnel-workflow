from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from liangjian_funnel.pipeline.feature_store import FeatureGenerationError, ResearchFeatureStore


UTC = timezone.utc
LIVE_AS_OF = datetime(2026, 8, 29, 15, 0, tzinfo=UTC)
REPLAY_AS_OF = LIVE_AS_OF - timedelta(days=1)


def _create_and_seal(
    store: ResearchFeatureStore,
    generation_id: str,
    *,
    purpose: str,
    as_of: datetime,
) -> str:
    store.create_feature_generation(
        generation_id=generation_id,
        domain="RESEARCH",
        as_of=as_of,
        contract_version="replay-isolation/1.0.0",
        algorithm_version="replay-isolation",
        source_manifest_hash=f"source-{generation_id}",
        created_at=as_of,
        metadata={"snapshot_id": f"snapshot-{generation_id}"},
        purpose=purpose,
        activation_eligible=purpose in {"LIVE_FULL", "LIVE_INCREMENTAL"},
    )
    store.validate_feature_generation(
        generation_id,
        validated_at=as_of,
        validation={"as_of": as_of.isoformat()},
    )
    store.seal_generation(
        generation_id,
        validation_manifest={"as_of": as_of.isoformat()},
        purpose=purpose,
        activation_eligible=purpose in {"LIVE_FULL", "LIVE_INCREMENTAL"},
        sealed_at=as_of,
    )
    return generation_id


def test_historical_replay_seals_and_binds_without_mutating_active(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    live = _create_and_seal(
        store, "live", purpose="LIVE_FULL", as_of=LIVE_AS_OF
    )
    store.activate_generation(live, None, "live-bootstrap", activated_at=LIVE_AS_OF)
    replay = _create_and_seal(
        store, "replay", purpose="HISTORICAL_REPLAY", as_of=REPLAY_AS_OF
    )

    binding = store.bind_run_generation(
        run_id="historical-run",
        generation_id=replay,
        contract_hash="replay-contract",
        bound_at=REPLAY_AS_OF,
    )

    assert binding["generation_id"] == replay
    assert store.get_run_feature_binding(run_id="historical-run", strict=True)["generation_id"] == replay
    assert store.get_feature_generation(replay)["status"] == "SEALED"
    assert store.get_feature_generation(replay)["activation_eligible"] is False
    assert store.get_active_feature_generation()["generation_id"] == live
    with pytest.raises(FeatureGenerationError, match="PURPOSE_NOT_ACTIVATABLE"):
        store.activate_generation(
            replay,
            expected_current_id=live,
            activation_reason="historical-replay-must-not-activate",
        )
    assert store.get_active_feature_generation()["generation_id"] == live


def test_replay_generation_can_be_read_after_new_live_generation_activates(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    live = _create_and_seal(store, "live", purpose="LIVE_FULL", as_of=LIVE_AS_OF)
    store.activate_generation(live, None, "live", activated_at=LIVE_AS_OF)
    replay = _create_and_seal(
        store, "replay", purpose="HISTORICAL_REPLAY", as_of=REPLAY_AS_OF
    )
    store.bind_run_generation(
        run_id="replay-run",
        generation_id=replay,
        contract_hash="replay-contract",
    )
    newer = _create_and_seal(
        store,
        "newer",
        purpose="LIVE_INCREMENTAL",
        as_of=LIVE_AS_OF + timedelta(minutes=1),
    )
    store.activate_generation(newer, expected_current_id=live, activation_reason="incremental")

    assert store.get_active_feature_generation()["generation_id"] == newer
    assert store.get_run_feature_binding(run_id="replay-run", strict=True)["generation_id"] == replay
