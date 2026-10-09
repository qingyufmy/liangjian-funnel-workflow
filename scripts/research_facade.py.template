"""Stable research import surface; relocated bodies share one global namespace.

The shared namespace retains the legacy monkeypatch/injected-dependency
contract. The bootstrap changes code location only, not a business expression.
"""
from __future__ import annotations

import sys as _sys
import types as _types

from . import common as _common
from . import a1 as _a1, a2 as _a2, a3 as _a3

for _stage in (_a1, _a2, _a3):
    for _name in _stage._MOVED_NAMES:
        _source = getattr(_stage, _name)
        _moved = _types.FunctionType(_source.__code__, _common.__dict__,
            _source.__name__, _source.__defaults__, _source.__closure__)
        _moved.__kwdefaults__ = _source.__kwdefaults__
        _moved.__annotations__ = _source.__annotations__
        _moved.__doc__ = _source.__doc__
        _moved.__dict__.update(_source.__dict__)
        _moved.__module__ = __name__
        _moved.__qualname__ = _stage._METHOD_BINDINGS.get(_name, _source.__qualname__)
        setattr(_common, _name, _moved)
        setattr(_stage, _name, _moved)
        if _name in _stage._METHOD_BINDINGS:
            setattr(_common.ResearchPipeline, _name, _moved)

for _name, _value in list(vars(_common).items()):
    if not _name.startswith('__'):
        if isinstance(_value, (type, _types.FunctionType)) and _value.__module__ == _common.__name__:
            _value.__module__ = __name__
        globals()[_name] = _value

_shared_ns = vars(_common)
__all__ = [name for name in _shared_ns if not name.startswith('_')]


class _ResearchFacade(_types.ModuleType):
    def __getattribute__(self, name):
        shared = _types.ModuleType.__getattribute__(self, '_shared_ns')
        if not name.startswith('__') and name in shared:
            return shared[name]
        return _types.ModuleType.__getattribute__(self, name)

    def __setattr__(self, name, value):
        shared = _types.ModuleType.__getattribute__(self, '_shared_ns')
        if not name.startswith('__') and name != '_shared_ns':
            shared[name] = value
        _types.ModuleType.__setattr__(self, name, value)

    def __delattr__(self, name):
        shared = _types.ModuleType.__getattribute__(self, '_shared_ns')
        if not name.startswith('__') and name in shared:
            del shared[name]
        _types.ModuleType.__delattr__(self, name)


_sys.modules[__name__].__class__ = _ResearchFacade
