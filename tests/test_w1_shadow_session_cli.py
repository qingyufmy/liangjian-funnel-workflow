import importlib.util
from pathlib import Path

import pytest


@pytest.mark.parametrize('supplied',[True,False])
def test_cli_passes_explicit_completion_path_without_discovering_it(tmp_path,monkeypatch,supplied):
    path=Path(__file__).resolve().parents[1]/'scripts/run_shadow_session.py'
    spec=importlib.util.spec_from_file_location('_shadow_cli_fixture',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    received=[]
    def source(*args,**kwargs):received.append(kwargs);return object()
    monkeypatch.setattr(module,'ReadOnlyShadowSource',source)
    monkeypatch.setattr(module,'ShadowEvidenceLedger',lambda *args:object())
    monkeypatch.setattr(module,'ShadowSession',lambda *args,**kwargs:object())
    ticks=iter([0.,1.]);monkeypatch.setattr(module.time,'monotonic',lambda:next(ticks))
    state=tmp_path/'state.db';minute=tmp_path/'minute.db'
    state.touch();minute.touch()
    args=['--state-db',str(state),'--minute-db',str(minute),'--lane','lane_1',
        '--shadow-db',str(tmp_path/'shadow.db'),'--shadow-jsonl',str(tmp_path/'shadow.jsonl'),
        '--receipt-jsonl',str(tmp_path/'receipts.jsonl'),'--duration-seconds','.1']
    marker=tmp_path/'original-latest.json'
    if supplied:args+=['--monitor-latest',str(marker)]
    assert module.main(args)==0
    assert received==[{'lanes':['lane_1'],'monitor_latest':marker if supplied else None,
        'checkout_root':path.resolve().parents[1]}]


def test_completion_output_alias_rejected_before_creating_ledger(tmp_path):
    from liangjian_funnel.runtime.shadow_session import ReadOnlyShadowSource,ShadowSession
    state=tmp_path/'state.db';minute=tmp_path/'minute.db';state.touch();minute.touch()
    class Ledger:
        db_path=tmp_path/'shadow.db'
        jsonl_path=tmp_path/'shadow.jsonl'
    from datetime import datetime
    with pytest.raises(ValueError,match='COMPLETION_AND_OUTPUT_PATH_ALIAS_REFUSED'):
        ShadowSession(ReadOnlyShadowSource(state,minute,lanes=['lane_1'],monitor_latest=Ledger.db_path),
            ledger=Ledger(),started_at=datetime.fromisoformat('2026-10-12T09:32:00+08:00'))
    assert not Ledger.db_path.exists() and not Ledger.jsonl_path.exists()


@pytest.mark.parametrize('alias',['shadow.db','shadow.jsonl','receipt.jsonl'])
def test_cli_rejects_completion_alias_before_any_output_creation(tmp_path,alias):
    path=Path(__file__).resolve().parents[1]/'scripts/run_shadow_session.py'
    spec=importlib.util.spec_from_file_location('_shadow_alias_cli_fixture',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    state=tmp_path/'state.db';minute=tmp_path/'minute.db'
    state.touch();minute.touch()
    args=['--state-db',str(state),'--minute-db',str(minute),'--lane','lane_1',
        '--shadow-db',str(tmp_path/'shadow.db'),'--shadow-jsonl',str(tmp_path/'shadow.jsonl'),
        '--receipt-jsonl',str(tmp_path/'receipt.jsonl'),'--monitor-latest',str(tmp_path/alias),
        '--duration-seconds','.1']
    assert module.main(args)==2
    assert not (tmp_path/'shadow.db').exists()
    assert not (tmp_path/'shadow.jsonl').exists()
    assert not (tmp_path/'receipt.jsonl').exists()


def test_explicit_preopen_sidecar_lookup_only_passed_to_source(tmp_path,monkeypatch):
    path=Path(__file__).resolve().parents[1]/'scripts/run_shadow_session.py'
    spec=importlib.util.spec_from_file_location('_shadow_preopen_cli_fixture',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    received=[]
    monkeypatch.setattr(module,'ReadOnlyShadowSource',lambda *args,**kw:(received.append(kw) or object()))
    monkeypatch.setattr(module,'ShadowEvidenceLedger',lambda *args:object())
    monkeypatch.setattr(module,'ShadowSession',lambda *args,**kw:object())
    ticks=iter([0.,1.]);monkeypatch.setattr(module.time,'monotonic',lambda:next(ticks))
    state=tmp_path/'state.db';minute=tmp_path/'minute.db';sidecar=tmp_path/'sidecar.db'
    for source in (state,minute,sidecar):source.touch()
    args=['--state-db',str(state),'--minute-db',str(minute),'--lane','lane_1',
        '--shadow-db',str(tmp_path/'shadow.db'),'--shadow-jsonl',str(tmp_path/'shadow.jsonl'),
        '--receipt-jsonl',str(tmp_path/'receipts.jsonl'),'--duration-seconds','.1',
        '--preopen-inputs-db',str(sidecar)]
    assert module.main(args)==0
    assert received[0]['preopen_inputs_provider'].__self__.path==sidecar.resolve()
    assert set(tmp_path.iterdir())=={state,minute,sidecar,tmp_path/'receipts.jsonl'}


@pytest.mark.parametrize('source_name',['state.db','shadow.db','receipt.jsonl'])
def test_preopen_input_source_output_alias_never_initializes_ledger(tmp_path,source_name):
    path=Path(__file__).resolve().parents[1]/'scripts/run_shadow_session.py'
    spec=importlib.util.spec_from_file_location('_shadow_preopen_alias_cli',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    state=tmp_path/'state.db';minute=tmp_path/'minute.db';state.touch();minute.touch()
    if source_name!='state.db': (tmp_path/source_name).touch()
    before={p.name:p.read_bytes() for p in tmp_path.iterdir()}
    args=['--state-db',str(state),'--minute-db',str(minute),'--lane','lane_1',
        '--shadow-db',str(tmp_path/'shadow.db'),'--shadow-jsonl',str(tmp_path/'shadow.jsonl'),
        '--receipt-jsonl',str(tmp_path/'receipt.jsonl'),'--duration-seconds','.1',
        '--preopen-inputs-db',str(tmp_path/source_name)]
    assert module.main(args)==2
    assert {p.name:p.read_bytes() for p in tmp_path.iterdir()}==before
