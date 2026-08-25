"""Small atomic JSON store for fact envelopes and frozen manifests."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any
from uuid import uuid4

from pydantic import BaseModel

from .contracts import (
    FactEnvelope,
    FactSnapshotManifest,
    RealtimeFactEnvelope,
    canonical_json_bytes,
)


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


_SAFE_PATH_TOKEN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")


def _path_token(identifier: str) -> str:
    """Return a deterministic filename safe on Windows and POSIX."""

    if _SAFE_PATH_TOKEN_RE.fullmatch(identifier):
        return identifier
    return hashlib.sha256(identifier.encode("utf-8")).hexdigest()


def _atomic_write_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        dir=str(path.parent),
        prefix=f".{path.name}.{uuid4().hex}.",
        suffix=".tmp",
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        try:
            directory_fd = os.open(path.parent, os.O_RDONLY)
        except OSError:
            directory_fd = None
        if directory_fd is not None:
            try:
                os.fsync(directory_fd)
            except OSError:
                pass
            finally:
                os.close(directory_fd)
    finally:
        if temporary.exists():
            temporary.unlink()


class FactStore:
    """Root-constrained, hash-checking JSON persistence.

    Every written JSON document gets a same-directory ``.sha256`` companion.
    The companion is itself written atomically.  Reads verify it by default,
    while callers may also supply an independent expected hash (for example,
    the hash recorded in a parent manifest).
    """

    def __init__(self, root: str | os.PathLike[str]) -> None:
        root_path = Path(root)
        if "\x00" in str(root_path):
            raise ValueError("fact store root contains a NUL byte")
        root_path.mkdir(parents=True, exist_ok=True)
        self.root = root_path.resolve()
        if not self.root.is_dir():
            raise ValueError("fact store root must be a directory")

    def resolve(self, path: str | os.PathLike[str]) -> Path:
        candidate = Path(path)
        if "\x00" in str(candidate):
            raise ValueError("fact store path contains a NUL byte")
        resolved = candidate.resolve() if candidate.is_absolute() else (self.root / candidate).resolve()
        try:
            resolved.relative_to(self.root)
        except ValueError as exc:
            raise ValueError("fact store path escapes root") from exc
        if resolved == self.root:
            raise ValueError("fact store path must name a file")
        return resolved

    def write_json(self, path: str | os.PathLike[str], value: Any) -> Path:
        target = self.resolve(path)
        if isinstance(value, BaseModel):
            value = value.model_dump(mode="json")
        content = canonical_json_bytes(value)
        digest = _sha256_bytes(content)
        _atomic_write_bytes(target, content)
        _atomic_write_bytes(Path(f"{target}.sha256"), f"{digest}\n".encode("ascii"))
        # Read immediately after replace.  This catches a failed write or an
        # unexpected filesystem transformation before the caller can bind it.
        self.read_json(target, expected_sha256=digest)
        return target

    def read_json(
        self,
        path: str | os.PathLike[str],
        *,
        expected_sha256: str | None = None,
    ) -> Any:
        target = self.resolve(path)
        content = target.read_bytes()
        actual = _sha256_bytes(content)
        expected = expected_sha256
        if expected is None:
            sidecar = Path(f"{target}.sha256")
            if not sidecar.is_file():
                raise ValueError("stored JSON checksum sidecar is missing")
            expected = sidecar.read_text(encoding="ascii").strip()
        if expected is not None and actual.lower() != expected.lower():
            raise ValueError("stored JSON content hash mismatch")
        try:
            return json.loads(content.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("stored JSON is invalid") from exc

    def content_hash(self, path: str | os.PathLike[str]) -> str:
        target = self.resolve(path)
        return _sha256_bytes(target.read_bytes())

    def write_fact(
        self,
        fact: FactEnvelope | RealtimeFactEnvelope,
        path: str | os.PathLike[str] | None = None,
    ) -> Path:
        relative = path or Path("normalized") / f"{_path_token(fact.fact_id)}.json"
        return self.write_json(relative, fact)

    def read_fact(self, path: str | os.PathLike[str]) -> FactEnvelope | RealtimeFactEnvelope:
        data = self.read_json(path)
        if not isinstance(data, dict):
            raise ValueError("stored fact must be a JSON object")
        if data.get("realtime") is True:
            return RealtimeFactEnvelope.model_validate(data)
        return FactEnvelope.model_validate(data)

    def write_manifest(
        self,
        manifest: FactSnapshotManifest,
        path: str | os.PathLike[str] | None = None,
    ) -> Path:
        relative = path or Path("snapshots") / f"{_path_token(manifest.snapshot_id)}.json"
        return self.write_json(relative, manifest)

    def read_manifest(self, path: str | os.PathLike[str]) -> FactSnapshotManifest:
        data = self.read_json(path)
        if not isinstance(data, dict):
            raise ValueError("stored manifest must be a JSON object")
        return FactSnapshotManifest.model_validate(data)


__all__ = ["FactStore"]
