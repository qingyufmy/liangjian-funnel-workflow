"""Same compatibility tests run before and after the mechanical relocation."""
import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('wp0_equivalence', ROOT/'scripts/verify_research_equivalence.py')
equivalence = importlib.util.module_from_spec(spec)
spec.loader.exec_module(equivalence)


def test_equivalence_detects_changed_body_and_signature():
    original = 'def f(value: int = 1):\n    return value + 1\n'
    for changed in ('def f(value: int = 2):\n    return value + 1\n',
                    'def f(value: int = 1):\n    return value + 2\n'):
        assert equivalence.definitions(original, 'pkg') != equivalence.definitions(changed, 'pkg')


def test_equivalence_resolves_relative_imports_without_changing_edges():
    assert equivalence.external_imports('from ..runtime import x', 'pkg.pipeline') == (
        equivalence.external_imports('from ...runtime import x', 'pkg.pipeline.research'))


def test_legacy_research_module_retains_injected_global_namespace(monkeypatch):
    import liangjian_funnel.pipeline.research as research
    function = research._a2_candidate_pool_max
    assert function.__module__ == 'liangjian_funnel.pipeline.research'
    monkeypatch.setattr(research, '_wp0_test_dependency', object(), raising=False)
    # Existing helpers must see globals injected through the legacy import.
    assert function.__globals__['_wp0_test_dependency'] is research._wp0_test_dependency


def test_deploy_verifies_target_receipt_before_pull_or_install():
    script = (ROOT/'deploy.sh').read_text(encoding='utf-8')
    gate = script.index('LIANGJIAN_TEST_EVIDENCE:-')
    assert gate < script.index('git pull --ff-only') < script.index('pip install')
    assert 'git show "${target_commit}:scripts/full_test_baseline.py"' in script
