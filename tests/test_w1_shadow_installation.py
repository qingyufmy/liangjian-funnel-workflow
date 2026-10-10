"""Explicit checkout/helper lineage with installed Python module paths."""
from datetime import datetime
import hashlib
import importlib.util
from pathlib import Path
import shutil
import sys

import pytest

from liangjian_funnel.runtime import shadow_session,shadow_variants

ROOT=Path(__file__).resolve().parents[1]


def installed(tmp_path,monkeypatch):
    path=tmp_path/'venv/lib/python3.11/site-packages/liangjian_funnel/runtime/shadow_session.py'
    path.parent.mkdir(parents=True)
    shutil.copyfile(Path(shadow_session.__file__),path)
    name='liangjian_funnel.runtime._installation_session_fixture'
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules,name,module)
    spec.loader.exec_module(module)
    return module,path


def test_installed_source_without_explicit_checkout_is_accurately_unwired(tmp_path,monkeypatch):
    module,path=installed(tmp_path,monkeypatch)
    with pytest.raises(ValueError,match='SHADOW_CHECKOUT_ROOT_UNWIRED'):
        module.ReadOnlyShadowSource('UNOPENED-state','UNOPENED-minute',lanes=('lane_1',))
    with pytest.raises(ValueError,match='SHADOW_CHECKOUT_ROOT_UNWIRED'):
        module.source_hashes()


def test_explicit_checkout_loads_pure_helpers_and_hashes_actual_imported_files(tmp_path,monkeypatch):
    module,path=installed(tmp_path,monkeypatch)
    source=module.ReadOnlyShadowSource('UNOPENED-state','UNOPENED-minute',lanes=('lane_1',),checkout_root=ROOT)
    assert source.checkout_root==ROOT.resolve()
    closed,dispatch,basis=source.helpers.replay_clocks({'strategy':{
        'as_of':'2026-10-12T09:59:00+08:00'}},'2026-10-12T10:00:00+08:00')
    assert closed<dispatch and basis=='STRATEGY_AS_OF'
    refs=source.source_references
    session=refs['src/liangjian_funnel/runtime/shadow_session.py']
    assert session['actual_path']==str(path.resolve()) and session['source_kind']=='ACTUAL_IMPORTED_PYTHON_SOURCE'
    assert session['sha256']==hashlib.sha256(path.read_bytes()).hexdigest()
    helper=refs['scripts/audit_frozen_a4_decisions.py']
    assert helper['actual_path']==str((ROOT/'scripts/audit_frozen_a4_decisions.py').resolve())
    assert helper['sha256']==source.helpers.source_reference['sha256']
    assert source.helpers.source_reference['loader']=='AST_PURE_HELPER_FUNCTIONS_ONLY'
    assert module.source_hashes(repo_root=ROOT)=={key:value['sha256'] for key,value in refs.items()}


def test_checkout_legacy_api_is_preserved_only_for_verified_source_layout():
    source=shadow_session.ReadOnlyShadowSource('UNOPENED-state','UNOPENED-minute',lanes=('lane_1',))
    assert source.checkout_root==ROOT.resolve()
    assert shadow_session.source_hashes()==source.source_sha256


@pytest.mark.parametrize('kind',['missing','wrong_checkout'])
def test_wrong_explicit_root_is_setup_failure_not_path_discovery(tmp_path,monkeypatch,kind):
    module,path=installed(tmp_path,monkeypatch)
    root=tmp_path/kind
    if kind=='wrong_checkout': root.mkdir()
    with pytest.raises(ValueError,match='SHADOW_CHECKOUT_ROOT_INVALID'):
        module.ReadOnlyShadowSource('UNOPENED-state','UNOPENED-minute',lanes=('lane_1',),checkout_root=root)


def test_drifted_installed_session_is_rejected_against_checkout(tmp_path,monkeypatch):
    module,path=installed(tmp_path,monkeypatch)
    with path.open('ab') as target: target.write(b'\n# DRIFTED_INSTALLED_SOURCE\n')
    with pytest.raises(ValueError,match='SHADOW_IMPORTED_CHECKOUT_SOURCE_MISMATCH'):
        module.ReadOnlyShadowSource('UNOPENED-state','UNOPENED-minute',lanes=('lane_1',),checkout_root=ROOT)


def test_actual_imported_variant_path_not_checkout_copy_is_verified(tmp_path,monkeypatch):
    module,path=installed(tmp_path,monkeypatch)
    drift=tmp_path/'installed-variant.py'
    shutil.copyfile(shadow_variants.__file__,drift)
    with drift.open('ab') as target: target.write(b'\n# SOURCE_DRIFT\n')
    monkeypatch.setattr(shadow_variants,'__file__',str(drift))
    with pytest.raises(ValueError,match='SHADOW_IMPORTED_CHECKOUT_SOURCE_MISMATCH'):
        module.source_hashes(repo_root=ROOT)


def test_loader_never_executes_script_top_level_main_or_remote(tmp_path,monkeypatch):
    # Explicit isolated checkout-shaped fixture. Only helper file is read by
    # the pure helper loader; no DB/App/Store or script imports are executed.
    root=tmp_path/'helper-only-checkout'
    (root/'src/liangjian_funnel/runtime').mkdir(parents=True)
    shutil.copyfile(ROOT/'pyproject.toml',root/'pyproject.toml')
    shutil.copyfile(shadow_session.__file__,root/'src/liangjian_funnel/runtime/shadow_session.py')
    scripts=root/'scripts'; scripts.mkdir()
    original=(ROOT/'scripts/audit_frozen_a4_decisions.py').read_text(encoding='utf-8')
    marker=tmp_path/'SCRIPT_SHOULD_NOT_RUN'
    text='raise AssertionError("TOP_LEVEL_EXECUTED")\n'+original
    (scripts/'audit_frozen_a4_decisions.py').write_text(text,encoding='utf-8')
    helper=shadow_session._audit_helpers(checkout_root=root)
    assert not hasattr(helper,'REMOTE') and not hasattr(helper,'main') and not marker.exists()
    result=helper.frozen_market_overlay({}, {'market_gate':{'state_status':'READY','decision':'ALLOW'}})
    assert result['live_market_state']['decision']=='ALLOW'


def test_unavailable_imported_python_source_is_explicit_setup_gap(tmp_path,monkeypatch):
    module,path=installed(tmp_path,monkeypatch)
    monkeypatch.setattr(shadow_variants,'__file__',str(tmp_path/'not_available.py'))
    with pytest.raises(ValueError,match='SHADOW_IMPORTED_SOURCE_UNAVAILABLE'):
        module.source_hashes(repo_root=ROOT)
