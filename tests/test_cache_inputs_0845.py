"""Actual archived bytes through original pure readers; all requests prohibited."""
import hashlib,importlib.util,json,shutil,socket
from datetime import datetime,date
from pathlib import Path
import pytest
import liangjian_funnel.workflow as workflow

OPS=Path('D:/dev_A股/liangjian_team_v2_20261016/artifacts/a4-monitor-20261021')
spec=importlib.util.spec_from_file_location('cache_inputs_0845',OPS/'cache_inputs_0845.py')
adapter=importlib.util.module_from_spec(spec);spec.loader.exec_module(adapter)
ORIGINAL=OPS/'c0-0845-business-originals/rotation-theme-2026-10-27-original.json'
SHA='8097b760da753650bd3bd1ef39b03830630f14e9b12e83a5279144ac83b0ffba'
REGISTRY=Path(__file__).resolve().parents[1]/'config/rotation_themes_v1.yaml'

@pytest.fixture
def source(tmp_path,monkeypatch):
    def forbidden(*a,**kw):raise AssertionError('REAL_NETWORK_PROHIBITED')
    monkeypatch.setattr(socket.socket,'connect',forbidden)
    frozen=tmp_path/'rotation-theme-2026-10-27.json';shutil.copyfile(ORIGINAL,frozen)
    assert hashlib.sha256(frozen.read_bytes()).hexdigest()==SHA
    body=json.loads(frozen.read_bytes())
    refs={b['theme_id']:dict(source_reference_hash=b['membership_reference_hash'],
        source_catalog_hash=b['membership_catalog_hash'],binding_hash=b['membership_binding_hash']) for b in body['boards']}
    args=dict(as_of=datetime.fromisoformat('2026-10-27T18:00:00+08:00'),expected_trade_date=date(2026,10,27),
        registry_path=REGISTRY,snapshot_dir=tmp_path,reference_memberships=refs)
    return frozen,body,args

def test_original_positive_preserves_clock_hash_and_no_sources(source,tmp_path):
    frozen,body,args=source
    with adapter.install_cached_inputs(frozen_rotation=frozen,expected_sha256=SHA,audit_directory=tmp_path):
        out=workflow.collect_rotation_theme_snapshot(**args)
        assert out['content_hash']==body['content_hash'] and out['captured_at']==body['captured_at']
        assert out['archive_read_only'] is True
        missing=workflow.collect_tencent_capital_flow(as_of=args['as_of'],expected_symbols=['000858.SZ'],cache_dir=tmp_path/'missing')
        assert missing['available'] is False
        assert missing['reason_code']=='OPS_CACHE_ONLY_NOT_CAPTURED_NO_SOURCE_AUTHORITY'
        assert missing['by_symbol']['000858.SZ']
    assert hashlib.sha256(frozen.read_bytes()).hexdigest()==SHA
    audit=[json.loads(x) for x in (tmp_path/'CACHE_INPUTS.jsonl').read_text().splitlines()]
    assert len(audit)==2 and all(x['provider_http']==0 for x in audit)
    assert audit[1]['availability']=='NOT_AVAILABLE_NOT_ZERO'

@pytest.mark.parametrize('failure',['raw_hash','day','future','taxonomy','membership'])
def test_unknown_conflicting_or_future_evidence_fails_before_source(source,tmp_path,failure):
    frozen,body,args=source
    if failure=='raw_hash':frozen.write_bytes(frozen.read_bytes()+b' ')
    if failure=='day':args['expected_trade_date']=date(2026,10,28)
    if failure=='future':args['as_of']=datetime.fromisoformat('2026-10-27T15:00:00+08:00')
    if failure=='membership':next(iter(args['reference_memberships'].values()))['binding_hash']='CHANGED'
    if failure=='taxonomy':
        registry=tmp_path/'registry.yaml'
        registry.write_text(REGISTRY.read_text(encoding='utf8').replace('version:', 'version:',1)+'\n# Controlled unchanged parser\n',encoding='utf8')
        # A different valid taxonomy must be rejected, not a parse-failure substitute.
        from liangjian_funnel.data import rotation_theme
        original=rotation_theme.load_rotation_theme_config
        class Changed:
            def as_dict(self):return {'controlled':'DIFFERENT'}
        from unittest.mock import patch
        with patch.object(rotation_theme,'load_rotation_theme_config',return_value=Changed()),adapter.install_cached_inputs(
                frozen_rotation=frozen,expected_sha256=SHA,audit_directory=tmp_path),pytest.raises(adapter.CacheInputStop):
            workflow.collect_rotation_theme_snapshot(**args)
        return
    with adapter.install_cached_inputs(frozen_rotation=frozen,expected_sha256=SHA,audit_directory=tmp_path),pytest.raises(adapter.CacheInputStop):
        workflow.collect_rotation_theme_snapshot(**args)

def test_same_cache_adapter_and_budget_guard_with_exact_production_stream(source,tmp_path):
    import httpx
    from test_close_rerun_guard_0845 import guard
    from liangjian_funnel.settings import Settings
    from fixture_model_stream import sse_model_response
    import subprocess,sys,types
    frozen,body,args=source
    root=Path(__file__).resolve().parents[1]
    raw=subprocess.check_output(['git','-C',str(root),'show',
        '6eaaf7582e3a16c8e8e288d2521c3876a62a19ac:src/liangjian_funnel/pipeline/model_client.py'])
    module=types.ModuleType('liangjian_funnel.pipeline.cache_preflight_exact_6eaaf75')
    module.__package__='liangjian_funnel.pipeline';sys.modules[module.__name__]=module
    exec(compile(raw,'cache_preflight_exact_6eaaf75.py','exec'),module.__dict__)
    gate=guard(tmp_path);observed=[]
    from test_r4_scope_0858 import cache
    def transport(request):
        assert gate.counts['MODEL']==1
        assert (tmp_path/'MODEL_REQUEST_001.json').read_bytes()==request.content
        observed.append(request)
        return sse_model_response(200,json={'choices':[{'message':{'content':'{"ok":true}'}}]},request=request)
    settings=Settings.from_env({'LIANGJIAN_MODEL_API_KEY':'CONTROLLED'},root=tmp_path)
    client=module.OpenAICompatibleModelClient(settings,transport=httpx.MockTransport(transport),max_attempts=1)
    with adapter.install_cached_inputs(frozen_rotation=frozen,expected_sha256=SHA,audit_directory=tmp_path),\
            cache.install_global_typed_cache(directory=tmp_path/'globals',source_head='H',source_day='2026-10-27'),gate.install():
        out=workflow.collect_rotation_theme_snapshot(**args)
        result=client.complete(settings.review_model,[{'role':'user','content':'CONTROLLED_CACHE_PREFLIGHT'}],timeout_seconds=5)
    assert out['content_hash']==body['content_hash'] and result.output=={'ok':True}
    assert len(observed)==1 and gate.counts['GLOBAL']==0
