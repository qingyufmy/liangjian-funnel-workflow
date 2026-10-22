"""Explicit provider switch wiring; MockTransport only, no real credentials."""
import json

import httpx
import pytest
from pydantic import ValidationError

from liangjian_funnel.settings import MONITOR_MODEL, Settings
from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient


MODEL = 'deepseek-v4.1-flash'


def configured(tmp_path):
    return Settings.from_env({
        'LIANGJIAN_MODEL_BASE_URL': 'https://ai-api.finpoints.tech/v1',
        'LIANGJIAN_MODEL_API_KEY': 'synthetic-test-only-no-real-key',
        'LIANGJIAN_RESEARCH_MODEL': MODEL,
        'LIANGJIAN_REVIEW_MODEL': MODEL,
        'LIANGJIAN_MONITOR_MODEL': MODEL,
        'LIANGJIAN_TEAM_INTRADAY_MODEL': MODEL,
    }, root=tmp_path)


def test_all_explicit_routes_and_safe_summary(tmp_path):
    settings = configured(tmp_path)
    assert settings.research_models == (MODEL,)
    assert settings.review_model == settings.monitor_model == settings.team_intraday_model == MODEL
    assert settings.safe_summary()['team_intraday_model'] == MODEL
    assert 'synthetic-test-only-no-real-key' not in repr(settings)
    assert 'synthetic-test-only-no-real-key' not in json.dumps(settings.safe_summary())


def test_existing_defaults_remain_separate_and_team_does_not_follow_legacy(tmp_path):
    defaults = Settings.from_env({}, root=tmp_path)
    assert defaults.team_intraday_model == defaults.monitor_model == MONITOR_MODEL
    overridden = Settings.from_env({'LIANGJIAN_REVIEW_MODEL': MODEL,
        'LIANGJIAN_MONITOR_MODEL': MODEL}, root=tmp_path)
    assert overridden.team_intraday_model == MONITOR_MODEL
    with pytest.raises(ValidationError, match='TEAM_INTRADAY_MODEL_NOT_ALLOWED'):
        Settings.from_env({'LIANGJIAN_TEAM_INTRADAY_MODEL': 'deepseek-v4-pro'}, root=tmp_path)


@pytest.mark.parametrize('role,session', [('A2','RESEARCH'),('A3','RESEARCH'),('A6','PREMARKET'),('A3','PM_PREMARKET'),('A4','INTRADAY'),('A5','POST_CLOSE')])
def test_real_client_preserves_exact_url_model_and_json_for_each_role(tmp_path, role, session):
    seen = []
    def handler(request):
        assert str(request.url) == 'https://ai-api.finpoints.tech/v1/chat/completions'
        body = json.loads(request.read());seen.append(body)
        assert body['model'] == MODEL
        assert request.headers['authorization'] == 'Bearer synthetic-test-only-no-real-key'
        assert body['response_format'] == {'type': 'json_object'}
        # Provider switching must not leak Ark-specific options onto Finpoints.
        assert 'thinking' not in body
        return httpx.Response(200, json={'choices':[{'message':{'content':'{"ok":true}'}}]})
    client = OpenAICompatibleModelClient(configured(tmp_path), transport=httpx.MockTransport(handler))
    response = client.complete(MODEL, [{'role':'user','content':'synthetic facts'}],stage=role,team_session=session,max_output_tokens=1024)
    assert response.output == {'ok': True} and len(seen) == 1
