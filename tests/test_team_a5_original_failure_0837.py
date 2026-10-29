"""Actual 17:25 frozen pre-request context on exact deployed function bytes."""
from pathlib import Path
from datetime import datetime
import hashlib,json,subprocess,sys,types
import httpx,pytest
from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient
from liangjian_funnel.pipeline.team_v2_judgments import ModelJudgmentJournal
from liangjian_funnel.pipeline.team_v2_prompts import TeamPromptRepository
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
from liangjian_funnel.settings import Settings

ROOT=Path(__file__).resolve().parents[1]
ORIGINAL=Path('D:/dev_A股/liangjian_team_v2_20261016/artifacts/a4-monitor-20261021/c0-0845-business-originals/team-a5-postclose-context-original.json')

def test_exact_1725_original_is_rejected_locally_before_model_dispatch(tmp_path,monkeypatch):
    raw=ORIGINAL.read_bytes()
    assert hashlib.sha256(raw).hexdigest()=='08d853b826709a469b38c69d99b7ceffd351490ffb304021cfa8bfbcb6bcb81d'
    context=json.loads(raw)
    for name,sha in context['prompt_hashes'].items():
        assert hashlib.sha256((ROOT/'prompts/team_v2'/name).read_bytes()).hexdigest()==sha
    source=subprocess.check_output(['git','-C',str(ROOT),'show',
        '6eaaf7582e3a16c8e8e288d2521c3876a62a19ac:src/liangjian_funnel/runtime/team_v2_review.py'])
    module=types.ModuleType('liangjian_funnel.runtime.exact_a5_failure_6eaaf75')
    module.__package__='liangjian_funnel.runtime';sys.modules[module.__name__]=module
    exec(compile(source,'exact_team_review_6eaaf75.py','exec'),module.__dict__)
    # Keep the old failure proof on the deployed encoder too, after its fix.
    projection_source=subprocess.check_output(['git','-C',str(ROOT),'show',
        '6eaaf7582e3a16c8e8e288d2521c3876a62a19ac:src/liangjian_funnel/pipeline/team_v2_a5_projection.py'])
    projection=types.ModuleType('liangjian_funnel.pipeline.exact_a5_projection_6eaaf75')
    projection.__package__='liangjian_funnel.pipeline';sys.modules[projection.__name__]=projection
    exec(compile(projection_source,'exact_a5_projection_6eaaf75.py','exec'),projection.__dict__)
    from liangjian_funnel.pipeline import team_v2_a5_projection
    monkeypatch.setattr(team_v2_a5_projection,'project_review_facts',projection.project_review_facts)
    seen=[]
    def transport(request):
        seen.append(request)
        raise AssertionError('UNEXPECTED_MODEL_DISPATCH')
    settings=Settings.from_env({'LIANGJIAN_MODEL_API_KEY':'CONTROLLED'},root=tmp_path)
    client=OpenAICompatibleModelClient(settings,transport=httpx.MockTransport(transport),max_attempts=1)
    from liangjian_funnel.pipeline import team_v2_prompts
    original_contract=team_v2_prompts.with_runtime_contract;predicate={}
    def observed_contract(*args,**kwargs):
        request=original_contract(*args,**kwargs)
        predicate.update(request_chars=sum(len(m['content']) for m in request.messages),limit_chars=250000,
            message_sha256=request.rendered_prompt_hash if hasattr(request,'rendered_prompt_hash') else request.input_hash)
        return request
    monkeypatch.setattr(team_v2_prompts,'with_runtime_contract',observed_contract)
    args={k:v for k,v in context.items() if k!='prompt_hashes'}
    args.update(trade_date='2026-10-27',review_kind='POST_CLOSE',
        clock=lambda:datetime.fromisoformat(context['required_envelope']['as_of']),
        review_id=context['run_id'],allow_model_call=True,
        repository=TeamPromptRepository(ROOT/'prompts/team_v2'),client=client,
        journal=ModelJudgmentJournal(tmp_path/'model.db'),store=TeamMeetingStore(tmp_path/'team.db'))
    with pytest.raises(ValueError,match='A5_MODEL_CONTEXT_TOO_LARGE'):
        module.run_team_review(**args)
    assert seen==[] and predicate['request_chars']>predicate['limit_chars']
    print('ACTUAL_H_A5_FAILURE_PREDICATE',json.dumps(predicate,sort_keys=True))

@pytest.mark.parametrize('input_limit',[1024,1_000_000])
def test_same_actual_context_existing_configured_budget_reaches_controlled_stream(tmp_path,input_limit):
    from liangjian_funnel.pipeline.team_v2_a5_projection import project_review_facts,expand_review_facts
    from liangjian_funnel.runtime.team_v2_review import run_team_review
    from fixture_model_stream import sse_model_response
    context=json.loads(ORIGINAL.read_bytes());facts=context['facts']
    projected,proof=project_review_facts(facts)
    assert expand_review_facts(projected)==facts and proof['lossless_inverse_verified']
    print('NEW_ENCODER_PROJECTION',json.dumps(dict(proof,catalog_chars=len(json.dumps(facts['citation_catalog'],ensure_ascii=False,separators=(',',':')))),sort_keys=True))
    sizes={k:len(json.dumps(v,ensure_ascii=False,separators=(',',':'))) for k,v in facts.items()}
    texts=[]
    def inspect(v,path='$'):
        if isinstance(v,str) and len(v)>3000:texts.append((len(v),path))
        elif isinstance(v,dict):
            for k,x in v.items():inspect(x,path+'.'+k)
        elif isinstance(v,list):
            for i,x in enumerate(v):inspect(x,path+'['+str(i)+']')
    inspect(facts)
    print('ENCODER_SECTION_SIZES',json.dumps(dict(sizes=sizes,long_texts=sorted(texts,reverse=True)[:12]),sort_keys=True))
    seen=[];measured={}
    def transport(request):
        wire=json.loads(request.read());chars=sum(len(m['content']) for m in wire['messages'])
        from liangjian_funnel.pipeline.research.common import _estimate_message_tokens
        assert chars<=250000 and _estimate_message_tokens(wire['messages'])<=settings.model_max_input_tokens
        measured.update(request_chars=chars,estimated_input_tokens=_estimate_message_tokens(wire['messages']),input_token_limit=settings.model_max_input_tokens)
        # A controlled invalid answer tests transport, not fictional acceptance.
        seen.append(chars)
        return sse_model_response(200,json={'choices':[{'message':{'content':'{}'}}]},request=request)
    settings=Settings.from_env({'LIANGJIAN_MODEL_API_KEY':'CONTROLLED','LIANGJIAN_MODEL_MAX_INPUT_TOKENS':str(input_limit)},root=tmp_path)
    client=OpenAICompatibleModelClient(settings,transport=httpx.MockTransport(transport),max_attempts=1)
    args={k:v for k,v in context.items() if k!='prompt_hashes'}
    kwargs=dict(trade_date='2026-10-27',review_kind='POST_CLOSE',
        clock=lambda:datetime.fromisoformat(context['required_envelope']['as_of']),
        review_id=context['run_id'],allow_model_call=True,max_input_tokens=settings.model_max_input_tokens,
        repository=TeamPromptRepository(ROOT/'prompts/team_v2'),client=client,
        journal=ModelJudgmentJournal(tmp_path/'model.db'),store=TeamMeetingStore(tmp_path/'team.db'))
    if input_limit==1024:
        from liangjian_funnel.runtime.team_v2_review import TeamA5ContextError
        from contextlib import closing
        degraded=run_team_review(**args,**kwargs)
        assert not seen and degraded['status']=='A5_DEGRADED_TRUNCATED_FACTS_ONLY'
        assert degraded['deterministic_review']['context_budget']['input_token_limit']==1024
        with closing(kwargs['store']._connect()) as db:
            original=db.execute('select body_json from control_minute_context where decision_id=?',('a5-request:'+context['run_id'],)).fetchone()
            assert original and len(json.loads(original[0])['messages'])==2
        return
    try:result=run_team_review(**args,**kwargs)
    except ValueError as exc:
        print('NEW_REQUEST_FAILURE',getattr(exc,'diagnostics',{}));raise
    assert len(seen)==1 and result['status']=='A5_DEGRADED_NO_APPROVED_REPORT'
    print('EXISTING_CONFIGURED_BUDGET_ACTUAL_CONTEXT',json.dumps(dict(proof,**measured),sort_keys=True))

def test_typed_stage_failure_is_archived_and_notice_is_queued_once(tmp_path):
    from liangjian_funnel.runtime.team_v2_review import TeamA5ContextError
    from liangjian_funnel.runtime.team_v2_review_command import record_review_stage_failure
    from liangjian_funnel.runtime.team_v2_morning import _directory
    from contextlib import closing
    settings=Settings.from_env({},root=tmp_path);store=TeamMeetingStore(_directory(settings)/'team.db')
    exc=TeamA5ContextError(dict(phase='PRE_HTTP_CONTEXT_BUDGET',request_chars=480092,estimated_input_tokens=190000,input_token_limit=1024))
    clock=lambda:datetime.fromisoformat('2026-10-27T17:25:01+08:00')
    first=record_review_stage_failure(settings,'POST_CLOSE',exc,clock=clock)
    second=record_review_stage_failure(settings,'POST_CLOSE',exc,clock=lambda:datetime.fromisoformat('2026-10-27T17:26:01+08:00'))
    assert first['reason_code']==second['reason_code']=='A5_MODEL_CONTEXT_TOO_LARGE'
    detail=json.loads(Path(first['failure_artifact']['path']).read_bytes())
    assert detail['diagnostics']['request_chars']==480092
    with closing(store._connect()) as db:
        rows=db.execute("select kind,status from team_notifications where kind='TEAM_A5_STAGE_FAILURE'").fetchall()
    assert rows==[('TEAM_A5_STAGE_FAILURE','PENDING')]
    private=record_review_stage_failure(settings,'POST_CLOSE',ValueError('DO_NOT_ARCHIVE_PRIVATE_EXCEPTION_TEXT'),clock=clock)
    assert 'DO_NOT_ARCHIVE' not in Path(private['failure_artifact']['path']).read_text()


def test_oversize_is_counted_before_building_full_messages(tmp_path,monkeypatch):
    from liangjian_funnel.pipeline import team_v2_a5_projection
    from liangjian_funnel.runtime.team_v2_review import run_team_review,TeamA5ContextError
    from contextlib import closing
    context=json.loads(ORIGINAL.read_bytes());repository=TeamPromptRepository(ROOT/'prompts/team_v2')
    monkeypatch.setattr(team_v2_a5_projection,'project_review_facts',lambda facts:(facts,{'controlled_uncompressed':True}))
    monkeypatch.setattr(repository,'prepare_messages',lambda *a,**k:(_ for _ in ()).throw(AssertionError('OVERSIZED_REQUEST_CONSTRUCTED')))
    args={k:v for k,v in context.items() if k!='prompt_hashes'};store=TeamMeetingStore(tmp_path/'team.db')
    result=run_team_review(**args,trade_date='2026-10-27',review_kind='POST_CLOSE',clock=lambda:datetime.fromisoformat(context['required_envelope']['as_of']),
            review_id=context['run_id'],allow_model_call=True,repository=repository,client=object(),journal=object(),store=store)
    assert result['status']=='A5_DEGRADED_TRUNCATED_FACTS_ONLY'
    diagnostic=result['deterministic_review']['context_budget']
    assert diagnostic['phase']=='PRE_CONSTRUCTION_CONTEXT_BUDGET'and diagnostic['request_chars']>250000
    with closing(store._connect()) as db:
        assert db.execute('select count(*) from control_minute_context where decision_id=?',('a5-context-estimate:'+context['run_id'],)).fetchone()[0]==1
        assert db.execute('select count(*) from control_minute_context where decision_id=?',('a5-request:'+context['run_id'],)).fetchone()[0]==0


def test_original_transport_schema_remains_readable():
    from liangjian_funnel.pipeline.team_v2_a5_projection import expand_review_facts
    source=subprocess.check_output(['git','-C',str(ROOT),'show','6eaaf7582e3a16c8e8e288d2521c3876a62a19ac:src/liangjian_funnel/pipeline/team_v2_a5_projection.py'])
    module=types.ModuleType('liangjian_funnel.pipeline.old_transport_read');module.__package__='liangjian_funnel.pipeline'
    exec(compile(source,'old-transport.py','exec'),module.__dict__)
    facts={'items':[{'proof':'真实事实原件 '*40,'n':1}]*3}
    projected,_=module.project_review_facts(facts)
    assert expand_review_facts(projected)==facts


def test_real_raised_failure_keeps_frames_and_flushes_immediate_notice_once(tmp_path):
    from liangjian_funnel.runtime.team_v2_review_command import record_review_stage_failure
    from liangjian_funnel.runtime.team_v2_internal import InternalSender
    from liangjian_funnel.runtime.team_v2_morning import _directory
    settings=Settings.from_env({},root=tmp_path);TeamMeetingStore(_directory(settings)/'team.db');sent=[]
    def transport(request):
        sent.append(request.read());return httpx.Response(200,json={'code':0,'msg':'success'},request=request)
    sender=InternalSender(tmp_path/'absent.env',env={'SHADOW_RESEARCH_WEBHOOK':'https://open.feishu.cn/open-apis/bot/v2/hook/CONTROLLED'},transport=httpx.MockTransport(transport))
    try:raise ValueError('PRIVATE_NOT_ARCHIVED')
    except ValueError as exc:
        first=record_review_stage_failure(settings,'POST_CLOSE',exc,clock=lambda:datetime.fromisoformat('2026-10-27T17:25:00+08:00'),sender=sender)
        second=record_review_stage_failure(settings,'POST_CLOSE',exc,clock=lambda:datetime.fromisoformat('2026-10-27T17:25:01+08:00'),sender=sender)
    detail=json.loads(Path(first['failure_artifact']['path']).read_bytes())
    assert detail['frames'] and detail['notification_delivery']['sent']==1 and len(sent)==1
    assert 'PRIVATE_NOT_ARCHIVED' not in Path(first['failure_artifact']['path']).read_text()
