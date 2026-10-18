"""0315 author-owned independent session files must not borrow daily roles."""
from pathlib import Path

import pytest

from liangjian_funnel.pipeline.team_v2_prompts import TeamPromptRepository


@pytest.mark.parametrize('role,session,filename,version', [
    ('A2','INTRADAY_RADAR','agent_2_intraday_radar_v1.txt','agent-2-radar/1.0.0'),
    ('A3','PM_INTRADAY','agent_3_pm_intraday_v1.txt','agent-3-pmi/1.0.0'),
])
def test_independent_session_uses_its_exact_author_file(role,session,filename,version):
    repo=TeamPromptRepository(Path(__file__).resolve().parents[1]/'prompts/team_v2')
    doc=repo.document(role,session)
    values={key:{'status':'UNAVAILABLE'} for key in doc.placeholders}
    values['EVIDENCE_CATALOG']={'categories':[],'items':[]}
    request=repo.prepare(role,values,session=session)
    assert doc.name==filename and request.role==role and request.role_version==version
    assert request.source_hashes[filename]==doc.sha256 and len(request.source_hashes)==2
    assert '{{' not in request.system
    assert '═══════ 会话 PM_PREMARKET' not in request.system
    assert '═══════ 会话 PLAN_REVIEW' not in request.system
    assert repo.document(role).name!=filename


def test_a3_unknown_session_still_fails_closed():
    repo=TeamPromptRepository(Path(__file__).resolve().parents[1]/'prompts/team_v2')
    with pytest.raises(ValueError,match='TEAM_SESSION_REQUIRED'):
        repo.prepare('A3',{},session='UNKNOWN')
