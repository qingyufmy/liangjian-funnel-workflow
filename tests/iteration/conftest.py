from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from liangjian_funnel.runtime.state import RuntimeStore


@dataclass
class FakeClock:
    current: datetime

    def now(self) -> datetime:
        return self.current

    def set(self, value: datetime) -> None:
        self.current = value


class FakeProvider:
    def __init__(self, values: dict[str, Any] | None = None) -> None:
        self.values = values or {}
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def fetch(self, key: str, *args: Any, **kwargs: Any) -> Any:
        self.calls.append((key, args, kwargs))
        if key not in self.values:
            raise KeyError(key)
        return self.values[key]


class FakeLLM:
    def __init__(self, response: Any) -> None:
        self.response = response
        self.calls: list[Any] = []

    def invoke(self, payload: Any) -> Any:
        self.calls.append(payload)
        return self.response


@pytest.fixture(autouse=True)
def iteration_offline_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail loudly if an iteration test attempts network or notification I/O."""

    for key in tuple(os.environ):
        upper = key.upper()
        if any(token in upper for token in ("WEBHOOK", "API_KEY", "ACCESS_TOKEN", "SECRET_KEY")):
            monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("LIANGJIAN_ITERATION_OFFLINE", "1")
    monkeypatch.setenv("LIANGJIAN_DISABLE_NOTIFICATIONS", "1")

    def blocked_connect(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("network access is forbidden in iteration offline tests")

    monkeypatch.setattr(socket.socket, "connect", blocked_connect)


@pytest.fixture
def fake_clock() -> FakeClock:
    return FakeClock(datetime.fromisoformat("2026-09-18T10:00:00+08:00"))


@pytest.fixture
def fake_provider() -> FakeProvider:
    return FakeProvider()


@pytest.fixture
def fake_llm() -> FakeLLM:
    return FakeLLM({"llm_veto": True, "reason_code": "TEST_ONLY"})


@pytest.fixture
def runtime_store(tmp_path: Path) -> RuntimeStore:
    return RuntimeStore(tmp_path / "iteration-runtime.sqlite3")
