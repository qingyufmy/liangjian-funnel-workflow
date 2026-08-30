"""Durable monitor scheduling and internal paper simulation.

Keep this package initializer lightweight.  Operational observers import
``runtime.storage_governance`` in environments where research-only optional
dependencies (for example PDF parsers) are deliberately absent.  Eagerly
importing the whole scheduler/monitor graph made that read-only import depend
on those unrelated packages and produced order-dependent acceptance reports.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing only; runtime imports stay lazy
    from .monitor import MonitorEngine
    from .scheduler import Scheduler
    from .simulation import PaperBroker, SimulationConfig
    from .state import RuntimeStore

__all__ = ["MonitorEngine", "PaperBroker", "RuntimeStore", "Scheduler", "SimulationConfig"]

_EXPORTS = {
    "MonitorEngine": (".monitor", "MonitorEngine"),
    "PaperBroker": (".simulation", "PaperBroker"),
    "RuntimeStore": (".state", "RuntimeStore"),
    "Scheduler": (".scheduler", "Scheduler"),
    "SimulationConfig": (".simulation", "SimulationConfig"),
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(name)
    module_name, attribute = target
    value = getattr(import_module(module_name, __name__), attribute)
    globals()[name] = value
    return value
