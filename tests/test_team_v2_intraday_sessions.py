from fixture_model_stream import sse_model_response
from datetime import datetime,timedelta
from pathlib import Path
import json
import sqlite3

import httpx
import pytest

from liangjian_funnel.runtime.team_v2_intraday_sessions import radar_market_context,call_intraday_research
from liangjian_funnel.pipeline.team_v2_model import call_team_model
from liangjian_funnel.pipeline.team_v2_prompts import TeamPromptRepository,_ENVELOPE_KEYS
from liangjian_funnel.pipeline.team_v2_judgments import ModelJudgmentJournal
from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient
from liangjian_funnel.settings import Settings
from test_team_v2_intraday_schema import radar,CAT,CANDIDATES,THEMES

AT=datetime.fromisoformat('2026-10-19T10:01:00+08:00')


def state():
    return dict(emotion_state='EMOTION_ON',live_authority=True,primary_status='READY',radar_open=True,leader_open=True,
        state=dict(last_tick='2026-10-19T10:00:00+08:00',emotion_confirmed='EMOTION_ON'))


@pytest.mark.parametrize('fault',['valid','old','future','unconfirmed','closed','no_primary'])
def test_g5_vocabulary_mapping_never_fabricates_an_on_permission(fault):
    raw=state();at=AT
    if fault=='old':at+=timedelta(minutes=5)
    elif fault=='future':at-=timedelta(minutes=2)
    elif fault=='unconfirmed':raw['state']['emotion_confirmed']='EMOTION_OFF'
    elif fault=='closed':raw['radar_open']=False
    elif fault=='no_primary':raw['primary_status']='DATA_BLOCK'
    out=radar_market_context(raw,observed_at=at)
    assert out['emotion_state']==('ON' if fault=='valid' else 'DATA_BLOCK')
    assert out['original_emotion_state']=='EMOTION_ON' and raw['emotion_state']=='EMOTION_ON'


def test_exact_authored_radar_http_to_schema_and_research_judgment_only(tmp_path):
    root=Path(__file__).resolve().parents[1];repo=TeamPromptRepository(root/'prompts/team_v2');seen=[]
    values={k:{} for k in repo.document('A2','INTRADAY_RADAR').placeholders}
    values.update(EVIDENCE_CATALOG=CAT,RADAR_CANDIDATES=CANDIDATES,THEME_STATE=THEMES,
        LIVE_MARKET_STATE=radar_market_context(state(),observed_at=AT))
    envelope={k:'FROZEN_TEST' for k in _ENVELOPE_KEYS}
    envelope.update(run_id='radar',stage_id='A2',model_name='deepseek-v4-flash-0731',source_trade_date='2026-10-19',
        execution_trade_date='2026-10-19',prompt_version='agent-2-radar/1.0.0')
    raw=radar();raw['envelope']=envelope
    def handler(request):
        seen.append(json.loads(request.read()))
        return sse_model_response(200,json={'choices':[{'message':{'content':json.dumps(raw)}}]},request=request)
    settings=Settings.from_env({'LIANGJIAN_MODEL_API_KEY':'UNIT_TEST_ONLY_NOT_A_CREDENTIAL'},root=tmp_path)
    client=OpenAICompatibleModelClient(settings,transport=httpx.MockTransport(handler),max_attempts=1)
    journal=ModelJudgmentJournal(tmp_path/'research.db')
    result=call_intraday_research(role='A2',repository=repo,client=client,journal=journal,call_id='A2:radar',
        run_id='radar',model=settings.team_intraday_model,source_trade_date='2026-10-19',execution_trade_date='2026-10-19',
        required_envelope=envelope,replacements=values,
        server_context=dict(candidates=CANDIDATES,themes=THEMES,emotion_state='ON'),clock=lambda:AT,
        max_output_tokens=4096,allow_model_call=True)
    assert result.validation.valid and len(seen)==1
    assert seen[0]['model'] == settings.team_intraday_model
    assert 'agent-2-radar/1.0.0' in seen[0]['messages'][0]['content']
    with sqlite3.connect(journal.path) as db:
        assert db.execute('SELECT role,session FROM model_calls').fetchall()==[('A2','INTRADAY_RADAR')]
        assert db.execute('SELECT count(*) FROM model_judgments').fetchone()==(1,)
        assert not db.execute('SELECT name FROM sqlite_master WHERE name=?',('execution_plans',)).fetchall()
    assert not (tmp_path/'paper.db').exists()
    request=repo.prepare_messages('A2',values,required_envelope=envelope,session='INTRADAY_RADAR')
    with pytest.raises(ValueError,match='TEAM_CALL_BUDGET_INVALID'):
        call_team_model(request,client,journal,call_id='too-long',run_id='radar',model=settings.team_intraday_model,
            session='INTRADAY_RADAR',source_trade_date='2026-10-19',execution_trade_date='2026-10-19',
            catalog=CAT,timeout_seconds=61,max_output_tokens=4096,validator=lambda x:result.validation,
            clock=lambda:AT,allow_model_call=True)
    assert len(seen)==1


@pytest.mark.parametrize('role', ['A2', 'A3'])
def test_radar_and_pm_intraday_reject_shared_pro_before_transport(role):
    with pytest.raises(ValueError, match='^TEAM_INTRADAY_MODEL_NOT_ALLOWED$'):
        call_intraday_research(role=role, repository=None, client=None, journal=None,
            call_id='not-called', run_id='not-called', model='deepseek-v4-pro',
            source_trade_date='2026-10-20', execution_trade_date='2026-10-20',
            required_envelope={}, replacements={}, server_context={}, clock=lambda: AT,
            max_output_tokens=4096, allow_model_call=True)


def test_pm_intraday_flash_http_schema_only_not_live_activation(tmp_path):
    from test_team_v2_intraday_schema import pmi, context
    repo = TeamPromptRepository(Path(__file__).resolve().parents[1] / 'prompts/team_v2')
    settings = Settings.from_env({'LIANGJIAN_MODEL_API_KEY': 'UNIT_TEST_ONLY_NOT_A_CREDENTIAL'}, root=tmp_path)
    values = {key: {} for key in repo.document('A3', 'PM_INTRADAY').placeholders}
    values['EVIDENCE_CATALOG'] = CAT
    envelope = {key: 'FROZEN_TEST' for key in _ENVELOPE_KEYS}
    envelope.update(run_id='pmi', stage_id='A3', model_name=settings.team_intraday_model,
        source_trade_date='2026-10-19', execution_trade_date='2026-10-19',
        prompt_version='agent-3-pmi/1.0.0')
    raw = pmi(); raw['envelope'] = envelope; seen = []
    def handler(request):
        seen.append(json.loads(request.read()))
        return sse_model_response(200, json={'choices': [{'message': {'content': json.dumps(raw)}}]}, request=request)
    server = context(tmp_path); server.pop('required_envelope')
    result = call_intraday_research(role='A3', repository=repo,
        client=OpenAICompatibleModelClient(settings, transport=httpx.MockTransport(handler), max_attempts=1),
        journal=ModelJudgmentJournal(tmp_path / 'pmi.db'), call_id='A3:pmi', run_id='pmi',
        model=settings.team_intraday_model, source_trade_date='2026-10-19', execution_trade_date='2026-10-19',
        required_envelope=envelope, replacements=values, server_context=server,
        clock=lambda: AT, max_output_tokens=4096, allow_model_call=True)
    assert result.validation.valid and len(seen) == 1
    assert seen[0]['model'] == settings.team_intraday_model
