import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from liangjian_funnel.pipeline.feature_store import (
    FeatureGenerationError,
    ResearchFeatureStore,
)


NOW = datetime(2026, 8, 29, 9, 30, tzinfo=timezone.utc)


def _generation(
    store: ResearchFeatureStore,
    generation_id: str,
    *,
    as_of: datetime = NOW,
    purpose: str = "LIVE_FULL",
) -> str:
    return store.create_feature_generation(
        generation_id=generation_id,
        domain="RESEARCH",
        as_of=as_of,
        contract_version="feature-test/3.0.0",
        algorithm_version="feature-test",
        source_manifest_hash=f"manifest-{generation_id}",
        created_at=NOW,
        purpose=purpose,
        activation_eligible=purpose in {"LIVE_FULL", "LIVE_INCREMENTAL"},
    )


def _seal(store: ResearchFeatureStore, generation_id: str, *, purpose: str = "LIVE_FULL") -> None:
    store.validate_feature_generation(
        generation_id,
        validated_at=NOW,
        validation={"generation_id": generation_id},
    )
    store.seal_generation(
        generation_id,
        validation_manifest={"generation_id": generation_id},
        purpose=purpose,
        activation_eligible=purpose in {"LIVE_FULL", "LIVE_INCREMENTAL"},
        sealed_at=NOW,
    )


def test_seal_bind_activate_are_independent_and_audit_activation(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    generation = _generation(store, "live-1")
    _seal(store, generation)

    assert store.get_active_feature_generation() is None
    binding = store.bind_run_generation(
        run_id="run-1",
        generation_id=generation,
        contract_hash="contract-1",
        bound_at=NOW,
    )
    assert binding["generation_id"] == generation

    activated = store.activate_generation(
        generation,
        expected_current_id=None,
        activation_reason="fixture-live",
        actor="pytest",
        activated_at=NOW,
    )
    assert activated["status"] == "SEALED"
    assert activated["purpose"] == "LIVE_FULL"
    assert activated["activation_eligible"] is True
    assert len(activated["activation_hash"]) == 64

    connection = sqlite3.connect(store.path)
    try:
        audit = connection.execute(
            "SELECT actor,activation_reason,previous_generation_id,generation_id "
            "FROM feature_generation_activation_audit"
        ).fetchone()
    finally:
        connection.close()
    assert audit == ("pytest", "fixture-live", None, generation)


def test_activate_uses_cas_and_rejects_as_of_regression(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    first = _generation(store, "first")
    _seal(store, first)
    store.activate_generation(first, None, "first", activated_at=NOW)

    second = _generation(store, "second", as_of=NOW + timedelta(minutes=1))
    _seal(store, second)
    with pytest.raises(FeatureGenerationError, match="ACTIVE_CAS_MISMATCH"):
        store.activate_generation(second, expected_current_id="wrong", activation_reason="cas")
    store.activate_generation(second, expected_current_id=first, activation_reason="second")

    older = _generation(store, "older", as_of=NOW - timedelta(days=1))
    _seal(store, older)
    with pytest.raises(FeatureGenerationError, match="AS_OF_REGRESSION"):
        store.activate_generation(older, expected_current_id=second, activation_reason="older")
    assert store.get_active_feature_generation()["generation_id"] == second


def test_sqlite_guard_rejects_unknown_or_replay_generation(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    replay = _generation(store, "replay", purpose="HISTORICAL_REPLAY")
    _seal(store, replay, purpose="HISTORICAL_REPLAY")

    with pytest.raises(FeatureGenerationError, match="PURPOSE_NOT_ACTIVATABLE"):
        store.activate_generation(replay, None, "must-fail")

    connection = sqlite3.connect(store.path)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="NOT_ACTIVATION_ELIGIBLE"):
            connection.execute(
                "INSERT INTO active_feature_generations "
                "(domain,generation_id,activated_at,previous_generation_id) "
                "VALUES(?,?,?,?)",
                ("RESEARCH", replay, NOW.isoformat(), None),
            )
    finally:
        connection.close()
    assert store.get_active_feature_generation() is None


def test_sealed_purpose_and_eligibility_cannot_be_downgraded(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    generation = _generation(store, "immutable")
    _seal(store, generation)
    with pytest.raises(FeatureGenerationError, match="SEALED_IMMUTABLE"):
        store.seal_generation(
            generation,
            purpose="HISTORICAL_REPLAY",
            activation_eligible=False,
        )
    row = store.get_feature_generation(generation)
    assert row["purpose"] == "LIVE_FULL"
    assert row["activation_eligible"] is True


def test_v2_generation_migration_preserves_active_and_classifies_purpose(tmp_path: Path):
    path = tmp_path / "v2.sqlite3"
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            CREATE TABLE feature_store_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO feature_store_meta VALUES('schema', 'liangjian-research-feature-store/2.0.0');
            CREATE TABLE feature_generations(
                generation_id TEXT PRIMARY KEY,
                domain TEXT NOT NULL,
                as_of TEXT NOT NULL,
                contract_version TEXT NOT NULL,
                algorithm_version TEXT NOT NULL,
                source_manifest_hash TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('STAGING','VALIDATED','PUBLISHED','FAILED','LEGACY')),
                created_at TEXT NOT NULL,
                validated_at TEXT,
                published_at TEXT,
                failed_at TEXT,
                failure_reason TEXT,
                metadata_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX idx_feature_generations_domain_status
                ON feature_generations(domain,status,created_at);
            CREATE TABLE active_feature_generations(
                domain TEXT PRIMARY KEY,
                generation_id TEXT NOT NULL,
                activated_at TEXT NOT NULL,
                previous_generation_id TEXT,
                FOREIGN KEY(generation_id) REFERENCES feature_generations(generation_id),
                FOREIGN KEY(previous_generation_id) REFERENCES feature_generations(generation_id)
            );
            """
        )
        connection.execute(
            "INSERT INTO feature_generations VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "live-old",
                "RESEARCH",
                "2026-08-29T09:00:00+00:00",
                "feature",
                "feature",
                "manifest-live",
                "PUBLISHED",
                "2026-08-29T09:00:00+00:00",
                None,
                "2026-08-29T09:01:00+00:00",
                None,
                None,
                '{"rebuild_mode":"FULL"}',
            ),
        )
        connection.execute(
            "INSERT INTO feature_generations VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "replay-old",
                "RESEARCH",
                "2026-08-28T09:00:00+00:00",
                "feature",
                "feature",
                "manifest-replay",
                "PUBLISHED",
                "2026-08-28T09:00:00+00:00",
                None,
                "2026-08-28T09:01:00+00:00",
                None,
                None,
                '{"snapshot_id":"snapshot-old"}',
            ),
        )
        connection.execute(
            "INSERT INTO active_feature_generations VALUES(?,?,?,?)",
            ("RESEARCH", "live-old", "2026-08-29T09:01:00+00:00", None),
        )
        connection.commit()
    finally:
        connection.close()

    store = ResearchFeatureStore(path)
    live = store.get_feature_generation("live-old")
    replay = store.get_feature_generation("replay-old")
    assert live["status"] == "SEALED"
    assert live["purpose"] == "LIVE_FULL"
    assert live["activation_eligible"] is True
    assert replay["status"] == "SEALED"
    assert replay["purpose"] == "HISTORICAL_REPLAY"
    assert replay["activation_eligible"] is False
    assert store.get_active_feature_generation()["generation_id"] == "live-old"


def test_generation_validation_failure_and_strict_read_guards(tmp_path: Path):
    """The lifecycle is fail-closed before a generation can serve readers."""

    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    with pytest.raises(FeatureGenerationError, match="FEATURE_GENERATION_MISSING"):
        store.assert_generation_usable("")
    with pytest.raises(FeatureGenerationError, match="FEATURE_GENERATION_NOT_FOUND"):
        store.assert_generation_usable("does-not-exist")
    with pytest.raises(ValueError, match="feature domain must not be empty"):
        store.create_feature_generation(
            domain="   ",
            as_of=NOW,
            contract_version="c",
            algorithm_version="a",
            source_manifest_hash="m",
        )
    with pytest.raises(ValueError, match="invalid feature generation purpose"):
        store.create_feature_generation(
            as_of=NOW,
            contract_version="c",
            algorithm_version="a",
            source_manifest_hash="m",
            purpose="NOT_A_PURPOSE",
        )

    generation = _generation(store, "staging")
    assert store.get_feature_generation(generation)["status"] == "STAGING"
    with pytest.raises(FeatureGenerationError, match="FEATURE_GENERATION_NOT_PUBLISHED"):
        store.assert_generation_usable(generation)

    validated = store.validate_feature_generation(
        generation,
        validated_at=NOW,
        validation={"coverage": {"actual": 3, "required": 3}},
    )
    assert validated["status"] == "VALIDATED"
    assert validated["metadata"]["validation"]["coverage"]["actual"] == 3
    # Validation is idempotent while the generation is still mutable.
    assert store.validate_feature_generation(generation, validated_at=NOW)["status"] == "VALIDATED"
    sealed = store.seal_generation(generation, sealed_at=NOW)
    assert sealed["status"] == "SEALED"
    assert store.assert_generation_usable(generation)["generation_id"] == generation


def test_failed_generation_records_diagnostics_without_touching_active(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    live = _generation(store, "live")
    _seal(store, live)
    store.activate_generation(live, None, "bootstrap", activated_at=NOW)

    failed = _generation(store, "failed")
    result = store.fail_feature_generation(
        failed,
        reason="provider timeout",
        failed_at=NOW,
        diagnostics={"provider": "test", "attempt": 2},
    )
    assert result["status"] == "FAILED"
    assert result["failure_reason"] == "provider timeout"
    assert result["metadata"]["failure_diagnostics"]["attempt"] == 2
    assert store.get_active_feature_generation()["generation_id"] == live
    with pytest.raises(FeatureGenerationError, match="FAILED_NOT_VALIDATABLE"):
        store.validate_feature_generation(failed)
    with pytest.raises(FeatureGenerationError, match="FAILED_NOT_SEALABLE"):
        store.seal_generation(failed)
    with pytest.raises(FeatureGenerationError, match="FAILED_NOT_PUBLISHABLE"):
        store.publish_feature_generation(failed)
    with pytest.raises(FeatureGenerationError, match="PUBLISHED_NOT_FAILABLE"):
        store.fail_feature_generation(live, reason="must not replace live")


def test_run_binding_is_idempotent_but_cannot_drift_or_bypass_strict_reads(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    generation = _generation(store, "run-scoped")

    first = store.bind_run_feature_generation(
        run_id="run-1",
        generation_id=generation,
        contract_hash="contract-1",
        bound_at=NOW,
        allow_unpublished=True,
    )
    repeat = store.bind_run_feature_generation(
        run_id="run-1",
        generation_id=generation,
        contract_hash="contract-1",
        bound_at=NOW + timedelta(seconds=1),
        allow_unpublished=True,
    )
    assert repeat == first
    with pytest.raises(FeatureGenerationError, match="RUN_CONTRACT_MISMATCH"):
        store.bind_run_feature_generation(
            run_id="run-1",
            generation_id=generation,
            contract_hash="different-contract",
            allow_unpublished=True,
        )
    other = _generation(store, "other")
    with pytest.raises(FeatureGenerationError, match="RUN_ALREADY_BOUND"):
        store.bind_run_feature_generation(
            run_id="run-1",
            generation_id=other,
            contract_hash="contract-1",
            allow_unpublished=True,
        )
    with pytest.raises(FeatureGenerationError, match="NOT_PUBLISHED"):
        store.get_run_feature_binding(run_id="run-1", strict=True)
    with pytest.raises(FeatureGenerationError, match="RUN_CONTRACT_MISMATCH"):
        store.get_run_feature_binding(run_id="run-1", expected_contract_hash="wrong")

    _seal(store, generation)
    bound = store.get_run_feature_binding(run_id="run-1", strict=True)
    assert bound["generation_id"] == generation

    with pytest.raises(FeatureGenerationError, match="ACTIVE_NOT_FOUND"):
        store.bind_run_to_active_generation(run_id="run-without-active")


def test_activation_concurrent_cas_allows_one_winner_and_audit_filters(tmp_path: Path):
    path = tmp_path / "features.sqlite3"
    store = ResearchFeatureStore(path)
    generations = []
    for index in (1, 2):
        generation = _generation(store, f"candidate-{index}", as_of=NOW + timedelta(minutes=index))
        _seal(store, generation)
        generations.append(generation)

    def activate(generation: str):
        local = ResearchFeatureStore(path)
        try:
            return (generation, "ok", local.activate_generation(generation, None, "concurrent"))
        except FeatureGenerationError as exc:
            return (generation, "error", str(exc))

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = [future.result() for future in as_completed([executor.submit(activate, generation) for generation in generations])]
    winners = [item for item in results if item[1] == "ok"]
    losers = [item for item in results if item[1] == "error"]
    assert len(winners) == 1
    assert len(losers) == 1
    assert "ACTIVE_CAS_MISMATCH" in losers[0][2]
    active = store.get_active_feature_generation()
    assert active["generation_id"] == winners[0][0]
    assert len(store.list_generation_activation_audit(domain="RESEARCH", limit=0)) == 1
    assert store.list_generation_activation_audit(generation_id=winners[0][0])[0]["activation_reason"] == "concurrent"


def test_compat_publish_active_binding_and_generation_filters(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    generation = _generation(store, "compat-publish")
    store.validate_feature_generation(
        generation,
        validated_at=NOW,
        validation={"source": "fixture", "required": 1, "actual": 1},
    )
    published = store.publish_feature_generation(generation, activated_at=NOW)
    assert published["status"] == "SEALED"
    # Validation evidence is durable in both the generation metadata and the
    # dedicated manifest column used by strict readers.
    assert published["metadata"]["validation"]["source"] == "fixture"
    assert published["validation_manifest"] == {"source": "fixture", "required": 1, "actual": 1}

    binding = store.bind_run_to_active_generation(
        run_id="active-run",
        contract_hash="contract",
        bound_at=NOW,
    )
    assert binding["generation_id"] == generation
    assert store.get_run_feature_binding(run_id="missing") is None
    assert [item["generation_id"] for item in store.list_feature_generations(domain="RESEARCH", statuses=["SEALED"], limit=1)] == [generation]
    assert store.list_feature_generations(domain="research", statuses=["FAILED"], limit=0) == []


def test_compat_publish_retries_only_internally_derived_cas_conflict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    store = ResearchFeatureStore(tmp_path / "features.sqlite3")
    generation = _generation(store, "compat-publish-race")
    store.validate_feature_generation(generation, validated_at=NOW)
    original_activate = store.activate_generation
    calls = 0

    def activate_with_one_race(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise FeatureGenerationError(
                "FEATURE_GENERATION_ACTIVE_CAS_MISMATCH:NONE:concurrent-generation"
            )
        return original_activate(*args, **kwargs)

    monkeypatch.setattr(store, "activate_generation", activate_with_one_race)
    published = store.publish_feature_generation(generation, activated_at=NOW)

    assert published["generation_id"] == generation
    assert calls == 2


def test_generation_timestamp_and_directory_path_validation(tmp_path: Path):
    directory_store = ResearchFeatureStore(tmp_path / "directory-store")
    generation = directory_store.create_feature_generation(
        as_of="2026-08-29T09:30:00Z",
        contract_version="test",
        algorithm_version="test",
        source_manifest_hash="manifest",
        created_at="2026-08-29T09:30:00+00:00",
        metadata={"rebuild_mode": "INCREMENTAL"},
    )
    row = directory_store.get_feature_generation(generation)
    assert row["purpose"] == "LIVE_INCREMENTAL"
    assert row["activation_eligible"] is True
    with pytest.raises(FeatureGenerationError, match="AS_OF_INVALID"):
        ResearchFeatureStore._parse_timestamp("not-an-iso-time", field="as_of")
    with pytest.raises(FeatureGenerationError, match="ALREADY_EXISTS"):
        directory_store.create_feature_generation(
            generation_id=generation,
            as_of=NOW,
            contract_version="test",
            algorithm_version="test",
            source_manifest_hash="manifest",
        )


def test_validate_returns_sealed_rows_and_recovers_non_mapping_metadata(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "validate-edges.sqlite3")
    sealed = _generation(store, "already-sealed")
    _seal(store, sealed)

    # A sealed generation is immutable; validation is deliberately an
    # idempotent read and must not overwrite its manifest.
    result = store.validate_feature_generation(
        sealed,
        validated_at=NOW + timedelta(minutes=1),
        validation={"must_not_replace": True},
    )
    assert result["status"] == "SEALED"
    assert result["metadata"]["validation"]["generation_id"] == sealed

    staging = _generation(store, "metadata-list")
    connection = sqlite3.connect(store.path)
    try:
        # The JSON column is an untrusted persistence boundary.  Validation
        # should treat a valid JSON list as malformed metadata and rebuild a
        # dictionary rather than crashing or leaking a list downstream.
        connection.execute(
            "UPDATE feature_generations SET metadata_json=? WHERE generation_id=?",
            ("[]", staging),
        )
        connection.commit()
    finally:
        connection.close()
    validated = store.validate_feature_generation(staging, validated_at=NOW)
    assert validated["status"] == "VALIDATED"
    assert validated["metadata"] == {}


def test_seal_handles_migrated_published_rows_metadata_fallback_and_immutability(
    tmp_path: Path,
):
    store = ResearchFeatureStore(tmp_path / "seal-edges.sqlite3")
    generation = _generation(store, "published-row")
    manifest = {"source": "metadata", "actual": 3}
    store.validate_feature_generation(generation, validated_at=NOW, validation=manifest)

    connection = sqlite3.connect(store.path)
    try:
        # Simulate a v2 row being observed before its next startup migration.
        # Clearing the dedicated column forces seal_generation to recover the
        # validation evidence from metadata.validation.
        connection.execute(
            "UPDATE feature_generations SET status='PUBLISHED', validation_manifest_json='{}' "
            "WHERE generation_id=?",
            (generation,),
        )
        connection.commit()
    finally:
        connection.close()
    sealed = store.seal_generation(
        generation,
        validation_manifest=None,
        purpose="LIVE_FULL",
        activation_eligible=True,
        sealed_at=NOW,
    )
    assert sealed["status"] == "SEALED"
    assert sealed["validation_manifest"] == manifest

    malformed = _generation(store, "seal-metadata-list")
    store.validate_feature_generation(malformed, validated_at=NOW)
    connection = sqlite3.connect(store.path)
    try:
        connection.execute(
            "UPDATE feature_generations SET metadata_json=? WHERE generation_id=?",
            ("[]", malformed),
        )
        connection.commit()
    finally:
        connection.close()
    malformed_sealed = store.seal_generation(malformed, sealed_at=NOW)
    assert malformed_sealed["status"] == "SEALED"
    assert malformed_sealed["metadata"]["purpose"] == "LIVE_FULL"

    # Matching values are a safe idempotent seal; a changed immutable field
    # must be rejected before any update is attempted.
    assert store.seal_generation(
        malformed,
        purpose="LIVE_FULL",
        activation_eligible=True,
        sealed_at=NOW,
    )["status"] == "SEALED"
    with pytest.raises(FeatureGenerationError, match="SEALED_IMMUTABLE"):
        store.seal_generation(
            malformed,
            purpose="LIVE_FULL",
            activation_eligible=False,
        )


def test_activate_rejects_reason_domain_eligibility_unsealed_and_stale_active(
    tmp_path: Path,
):
    store = ResearchFeatureStore(tmp_path / "activate-edges.sqlite3")
    generation = _generation(store, "activation-reason")
    _seal(store, generation)
    with pytest.raises(ValueError, match="activation_reason must not be empty"):
        store.activate_generation(generation, None, "   ")

    foreign = store.create_feature_generation(
        generation_id="foreign-domain",
        domain="OTHER",
        as_of=NOW,
        contract_version="feature-test/3.0.0",
        algorithm_version="feature-test",
        source_manifest_hash="manifest-foreign",
        created_at=NOW,
        purpose="LIVE_FULL",
        activation_eligible=True,
    )
    store.validate_feature_generation(foreign, validated_at=NOW)
    store.seal_generation(foreign, purpose="LIVE_FULL", activation_eligible=True, sealed_at=NOW)
    with pytest.raises(FeatureGenerationError, match="DOMAIN_MISMATCH"):
        store.activate_generation(foreign, None, "wrong-domain", domain="RESEARCH")

    ineligible = store.create_feature_generation(
        generation_id="ineligible",
        domain="RESEARCH",
        as_of=NOW + timedelta(minutes=1),
        contract_version="feature-test/3.0.0",
        algorithm_version="feature-test",
        source_manifest_hash="manifest-ineligible",
        created_at=NOW,
        purpose="LIVE_FULL",
        activation_eligible=False,
    )
    store.validate_feature_generation(ineligible, validated_at=NOW)
    store.seal_generation(ineligible, purpose="LIVE_FULL", activation_eligible=False, sealed_at=NOW)
    with pytest.raises(FeatureGenerationError, match="NOT_ACTIVATION_ELIGIBLE"):
        store.activate_generation(ineligible, None, "ineligible")

    unsealed = _generation(store, "unsealed")
    original_assert = store._assert_generation

    def allow_strict_probe(connection, generation_id, **kwargs):
        # The production method asks the strict guard first.  Bypassing only
        # that guard here lets the following status check prove that a
        # VALIDATED/STAGING row can never be activated accidentally.
        kwargs.pop("strict", None)
        return original_assert(connection, generation_id, strict=False, **kwargs)

    with patch.object(store, "_assert_generation", side_effect=allow_strict_probe):
        with pytest.raises(FeatureGenerationError, match="NOT_SEALED"):
            store.activate_generation(unsealed, None, "unsealed")

    first = _generation(store, "stale-active", as_of=NOW + timedelta(minutes=2))
    _seal(store, first)
    store.activate_generation(first, None, "stale-bootstrap", activated_at=NOW)
    candidate = _generation(store, "after-stale", as_of=NOW + timedelta(minutes=3))
    _seal(store, candidate)
    connection = sqlite3.connect(store.path)
    try:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute("DELETE FROM feature_generations WHERE generation_id=?", (first,))
        connection.commit()
    finally:
        connection.close()
    with pytest.raises(FeatureGenerationError, match="ACTIVE_NOT_FOUND"):
        store.activate_generation(candidate, first, "repair-stale-active")


def test_publish_missing_generation_and_binding_domain_guard(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "compat-edges.sqlite3")
    with pytest.raises(FeatureGenerationError, match="FEATURE_GENERATION_NOT_FOUND"):
        store.publish_feature_generation("missing-generation")

    foreign = store.create_feature_generation(
        generation_id="binding-foreign",
        domain="OTHER",
        as_of=NOW,
        contract_version="feature-test/3.0.0",
        algorithm_version="feature-test",
        source_manifest_hash="manifest-binding-foreign",
        created_at=NOW,
        purpose="LIVE_FULL",
        activation_eligible=True,
    )
    with pytest.raises(FeatureGenerationError, match="DOMAIN_MISMATCH"):
        store.bind_run_feature_generation(
            run_id="run-domain-mismatch",
            generation_id=foreign,
            domain="RESEARCH",
            allow_unpublished=True,
        )


def test_generation_guards_reject_empty_ids_and_unknown_statuses(tmp_path: Path):
    store = ResearchFeatureStore(tmp_path / "guard-edges.sqlite3")
    generation = _generation(store, "guard-target")

    with pytest.raises(ValueError, match="run_id must not be empty"):
        store.bind_run_feature_generation(run_id=" ", generation_id=generation)
    with pytest.raises(FeatureGenerationError, match="FEATURE_GENERATION_MISSING"):
        store.bind_run_feature_generation(run_id="run-missing-generation", generation_id=" ")

    # The schema currently constrains status values, but both lifecycle
    # methods retain an explicit invalid-status guard for stores written by an
    # older/foreign implementation.  Return a row-shaped mapping only for
    # this probe so the test does not weaken the real persistence contract.
    original_assert = store._assert_generation

    def unknown_status_row(connection, generation_id, **kwargs):
        kwargs.pop("strict", None)
        row = original_assert(connection, generation_id, strict=False, **kwargs)
        result = {key: row[key] for key in row.keys()}
        result["status"] = "UNKNOWN"
        return result

    with patch.object(store, "_assert_generation", side_effect=unknown_status_row):
        with pytest.raises(FeatureGenerationError, match="INVALID_STATUS:UNKNOWN"):
            store.validate_feature_generation(generation)
        with pytest.raises(FeatureGenerationError, match="NOT_VALIDATED"):
            store.seal_generation(generation)
