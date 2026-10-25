from pathlib import Path
import httpx,pytest
from fixture_model_stream import sse_model_response
from liangjian_funnel.settings import Settings
from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient,ModelClientError


def test_relocated_root_with_old_absolute_output_is_rejected_before_http(tmp_path):
    original=Settings.from_env({'LIANGJIAN_MODEL_API_KEY':'CONTROLLED_ONLY'},root=tmp_path/'original')
    wrong=original.model_copy(update=dict(root=tmp_path/'owned'));calls=[]
    def handler(req):calls.append(req);raise AssertionError('NO_HTTP_ON_BAD_LOCAL_SCOPE')
    with pytest.raises(ModelClientError,match='MODEL_STREAM_TELEMETRY_SCOPE_INVALID') as error:
        OpenAICompatibleModelClient(wrong,transport=httpx.MockTransport(handler)).complete('deepseek/deepseek-v4-pro',[dict(role='user',content='{}')])
    assert error.value.attempts==0 and calls==[]
    assert not (tmp_path/'original').exists() and not (tmp_path/'owned').exists()


def test_explicitly_rebased_output_preserves_successful_stream_receipt(tmp_path):
    original=Settings.from_env({'LIANGJIAN_MODEL_API_KEY':'CONTROLLED_ONLY'},root=tmp_path/'original')
    owned=tmp_path/'owned';correct=original.model_copy(update=dict(root=owned,workflow_output_dir=owned/'outputs'));calls=[]
    def handler(request):
        calls.append(request);return sse_model_response(200,json={'choices':[{'message':{'content':'{"ok":true}'}}]},request=request)
    result=OpenAICompatibleModelClient(correct,transport=httpx.MockTransport(handler),max_attempts=1).complete(
        'deepseek/deepseek-v4-pro',[dict(role='user',content='{}')])
    assert result.output=={'ok':True} and len(calls)==1
    assert Path(result.provider_stream_telemetry['receipt_path']).is_relative_to(owned)
    assert not (tmp_path/'original').exists()
