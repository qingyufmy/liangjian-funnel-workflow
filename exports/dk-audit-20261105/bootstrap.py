"""Load preserved DK modules without executing unrelated project facades."""
from pathlib import Path
from types import ModuleType
from importlib.machinery import ModuleSpec
import sys

ROOT = Path(__file__).resolve().parent

def enable():
    source = ROOT / "snapshot" / "src"
    for name in ("liangjian_funnel", "liangjian_funnel.data", "liangjian_funnel.market_state",
                 "liangjian_funnel.pipeline", "liangjian_funnel.runtime"):
        directory = source.joinpath(*name.split("."))
        existing = sys.modules.get(name)
        if existing is not None:
            if list(getattr(existing, "__path__", ())) != [str(directory)]:
                raise ImportError("DK_EXPORT_EXTERNAL_PACKAGE_ALREADY_IMPORTED: " + name)
            continue
        package = ModuleType(name)
        package.__path__ = [str(directory)]
        package.__package__ = name
        package.__spec__ = ModuleSpec(name, loader=None, is_package=True)
        package.__spec__.submodule_search_locations = package.__path__
        sys.modules[name] = package
    return ROOT
