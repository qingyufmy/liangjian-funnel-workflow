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
    envelope.update(run_id='radar',stage_id='A2',model_name='deepseek-v4-pro',source_trade_date='2026-10-19',
        execution_trade_date='2026-10-19',prompt_version='agent-2-radar/1.0.0')
    raw=radar();raw['envelope']=envelope
    def handler(request):
        seen.append(json.loads(request.read()))
        return httpx.Response(200,json={'choices':[{'message':{'content':json.dumps(raw)}}]},request=request)
    settings=Settings.from_env({'LIANGJIAN_MODEL_API_KEY':'UNIT_TEST_ONLY_NOT_A_CREDENTIAL'},root=tmp_path)
    client=OpenAICompatibleModelClient(settings,transport=httpx.MockTransport(handler),max_attempts=1)
    journal=ModelJudgmentJournal(tmp_path/'research.db')
    result=call_intraday_research(role='A2',repository=repo,client=client,journal=journal,call_id='A2:radar',
        run_id='radar',model='deepseek-v4-pro',source_trade_date='2026-10-19',execution_trade_date='2026-10-19',
        required_envelope=envelope,replacements=values,
        server_context=dict(candidates=CANDIDATES,themes=THEMES,emotion_state='ON'),clock=lambda:AT,
        max_output_tokens=4096,allow_model_call=True)
    assert result.validation.valid and len(seen)==1
    assert 'agent-2-radar/1.0.0' in seen[0]['messages'][0]['content']
    with sqlite3.connect(journal.path) as db:
        assert db.execute('SELECT role,session FROM model_calls').fetchall()==[('A2','INTRADAY_RADAR')]
        assert db.execute('SELECT count(*) FROM model_judgments').fetchone()==(1,)
        assert not db.execute('SELECT name FROM sqlite_master WHERE name=?',('execution_plans',)).fetchall()
    assert not (tmp_path/'paper.db').exists()
    request=repo.prepare_messages('A2',values,required_envelope=envelope,session='INTRADAY_RADAR')
    with pytest.raises(ValueError,match='TEAM_CALL_BUDGET_INVALID'):
        call_team_model(request,client,journal,call_id='too-long',run_id='radar',model='deepseek-v4-pro',
            session='INTRADAY_RADAR',source_trade_date='2026-10-19',execution_trade_date='2026-10-19',
            catalog=CAT,timeout_seconds=61,max_output_tokens=4096,validator=lambda x:result.validation,
            clock=lambda:AT,allow_model_call=True)
    assert len(seen)==1
