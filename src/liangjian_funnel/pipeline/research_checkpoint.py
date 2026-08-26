"""Durable, content-addressed checkpoints for research batches.

The research pipeline deliberately keeps this store small and boring: a
successful, already validated :class:`StageAudit` is serialized together with
the complete identity of the request that produced it.  A checkpoint can only
be reused when every identity field matches.  In particular, a changed prompt
or stage snapshot never silently reuses an older model response.

The store protocol is intentionally tiny so callers can provide an in-memory,
database, or object-storage implementation without coupling the research
orchestrator to a persistence technology.
"""

from __future__ import annotations

import copy
import hashlib
import json
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from ..reporting import atomic_write_json


@dataclass(frozen=True, slots=True)
class ResearchCheckpointKey:
    """Identity of one model request for one research batch.

    ``batch_symbols_hash`` is the digest of the sorted canonical symbols, not
    the symbols themselves.  This keeps filenames/logs bounded while still
    binding a checkpoint to the exact batch scope.
    """

    run_id: str
    lane: str
    stage: str
    model: str
    prompt_hash: str
    snapshot_hash: str
    batch_symbols_hash: str

    def as_dict(self) -> dict[str, str]:
        return {
            "run_id": self.run_id,
            "lane": self.lane,
            "stage": self.stage,
            "model": self.model,
            "prompt_hash": self.prompt_hash,
            "snapshot_hash": self.snapshot_hash,
            "batch_symbols_hash": self.batch_symbols_hash,
        }

    @property
    def digest(self) -> str:
        raw = json.dumps(self.as_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class ResearchCheckpointStore(Protocol):
    """Minimal protocol accepted by :class:`ResearchPipeline`.

    ``load`` returns the record previously passed to ``save`` or ``None``.
    Implementations must treat records as opaque and must not return a record
    for a different key.
    """

    def load(self, key: ResearchCheckpointKey) -> Mapping[str, Any] | None:
        """Load a record for ``key`` if one exists."""

    def save(self, key: ResearchCheckpointKey, record: Mapping[str, Any]) -> None:
        """Persist a successful record for ``key``."""


class InMemoryResearchCheckpointStore:
    """Thread-safe test and embedded-runtime checkpoint store."""

    def __init__(self) -> None:
        self._records: dict[str, dict[str, Any]] = {}
        self._lock = threading.RLock()

    def load(self, key: ResearchCheckpointKey) -> Mapping[str, Any] | None:
        with self._lock:
            record = self._records.get(key.digest)
            return copy.deepcopy(record) if record is not None else None

    def save(self, key: ResearchCheckpointKey, record: Mapping[str, Any]) -> None:
        with self._lock:
            self._records[key.digest] = copy.deepcopy(dict(record))

    def __len__(self) -> int:
        with self._lock:
            return len(self._records)


class FileResearchCheckpointStore:
    """Atomic JSON-file checkpoint store suitable for a local workflow host."""

    def __init__(self, directory: str | Path) -> None:
        self.directory = Path(directory).expanduser().resolve()
        self._lock = threading.RLock()

    def _path(self, key: ResearchCheckpointKey) -> Path:
        return self.directory / f"checkpoint_{key.digest}.json"

    def load(self, key: ResearchCheckpointKey) -> Mapping[str, Any] | None:
        path = self._path(key)
        try:
            with self._lock:
                raw = json.loads(path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, UnicodeError, json.JSONDecodeError):
            return None
        if not isinstance(raw, Mapping):
            return None
        stored_key = raw.get("key")
        if not isinstance(stored_key, Mapping) or dict(stored_key) != key.as_dict():
            return None
        return dict(raw)

    def save(self, key: ResearchCheckpointKey, record: Mapping[str, Any]) -> None:
        payload = dict(record)
        payload["key"] = key.as_dict()
        # ``atomic_write_json`` applies the repository redaction policy before
        # writing.  Checkpoint contents are already reasoning-free, but this
        # is a second safety boundary for custom model clients.
        with self._lock:
            atomic_write_json(self._path(key), payload)


JsonResearchCheckpointStore = FileResearchCheckpointStore


__all__ = [
    "FileResearchCheckpointStore",
    "InMemoryResearchCheckpointStore",
    "JsonResearchCheckpointStore",
    "ResearchCheckpointKey",
    "ResearchCheckpointStore",
]
