"""Explicit nominal filesystem input for algorithm/report fixture tests only.

These tests exercise source coverage and acceptance facts, not the host disk.
Storage-governance tests and each explicit blocked-resource case keep their
own input overrides. This fixture never changes production thresholds.
"""
from collections import namedtuple
import shutil
import pytest


@pytest.fixture(autouse=True)
def nominal_disk_capacity(monkeypatch):
    usage=namedtuple('ControlledDiskUsage','total used free')
    value=usage(100*1024**3,70*1024**3,30*1024**3)
    monkeypatch.setattr(shutil,'disk_usage',lambda _path:value)
