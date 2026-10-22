"""Compose the approved research roles only at an actual research boundary.

No model request, legacy RuntimeStore or customer publisher is created here.
CLI research dispatch opts in; read-only/status/market jobs do not initialize
the owned journal merely because a WorkflowApplication was constructed.
"""
from copy import deepcopy
import hashlib
import json

from ..redaction import digest_text
from ..settings import load_yaml
from ..runtime.team_v2_morning import _directory
from ..runtime.team_v2_store import TeamMeetingStore
from .team_v2_a2_score import POLICY
from .team_v2_judgments import ModelJudgmentJournal
from .team_v2_prompts import TeamPromptRepository
from .team_v2_research import TeamResearchAdapter


RESEARCH_COMMANDS=frozenset({'run-a1-maintenance','run-research','run-comparison',
    'run-next-session-prep','run-close','run-auction-refresh','run-due',
    'run-a1-research-update','run-research-update'})


def configured_research_adapter(settings,*,calendar):
    """Bind approved roles to exact bytes; explicit 0323 A1 fallback only.

    There is no missing-file fallback to the old role or subjective A2 score.
    Loading the config uses the identical hash/version recipe as Workflow's
    frozen snapshot, rather than a separate risk.yaml identity for research.
    """
    config=load_yaml(settings.source_config_path)
    a2=config.get('agent_2')
    if not isinstance(a2,dict) or a2.get('team_theme_score_policy')!=POLICY:
        raise ValueError('TEAM_RESEARCH_CONFIGURATION_INVALID')
    a1=config.get('agent_1')
    a1_role_mode=a1.get('team_role_mode','team') if isinstance(a1,dict) else 'team'
    if a1_role_mode not in ('team','production'):
        raise ValueError('TEAM_A1_ROLE_MODE_INVALID')
    directory=settings.prompt_dir/'team_v2'
    manifest=json.loads((directory/'manifest.json').read_text(encoding='utf-8'))
    files=manifest.get('files',{})
    expected={'00_shared_system_v3.txt','agent_1_macro_chain_v3.txt','agent_2_theme_sentiment_v3.txt',
        'agent_3_portfolio_planner_v4.txt','agent_4_trader_v1.txt','agent_5_daily_reviewer_v2.txt',
        'agent_6_risk_governor_v1.txt','README_TEAM_V2_PROMPTS.md',
        'agent_2_intraday_radar_v1.txt','agent_3_pm_intraday_v1.txt'}
    if set(files)!=expected:raise ValueError('TEAM_RESEARCH_PROMPT_BUNDLE_INVALID')
    for name,proof in files.items():
        if '/' in name or '\\' in name or name in {'.','..'}:raise ValueError('TEAM_RESEARCH_PROMPT_BUNDLE_INVALID')
        raw=(directory/name).read_bytes()
        if len(raw)!=proof['bytes'] or hashlib.sha256(raw).hexdigest()!=proof['sha256']:
            raise ValueError('TEAM_RESEARCH_PROMPT_HASH_MISMATCH')
    # Reject scope conflicts before creating either owned database.
    owned=_directory(settings)
    return TeamResearchAdapter(repository=TeamPromptRepository(directory),
        journal=ModelJudgmentJournal(owned/'model.db'),store=TeamMeetingStore(owned/'team.db'),
        calendar=calendar,config_version=config.get('version') or config.get('funnel_version') or 'funnel-config-v2',
        config_hash=digest_text(json.dumps(config,ensure_ascii=False,sort_keys=True,default=str)),
        a2_config=deepcopy(a2),a1_role_mode=a1_role_mode)
