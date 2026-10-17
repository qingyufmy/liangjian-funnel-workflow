"""Exact author pins and A3 PLAN_REVIEW inheritance prerequisite; no model."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
from run_team_a5_frozen_dryrun import save


def section(raw,name):
    parts=raw.decode().split('═══════ 会话 ')
    return next(v for v in parts[1:] if v.startswith(name+'（')).encode()


def main():
    folder=ROOT/'prompts/team_v2';manifest=json.loads((folder/'manifest.json').read_bytes())
    actual={name:dict(bytes=len((folder/name).read_bytes()),sha256=hashlib.sha256((folder/name).read_bytes()).hexdigest())
        for name in manifest['files']}
    assert actual==manifest['files'],'AUTHOR_PIN_MISMATCH'
    previous='8fcd1a5b5876d0348458c136b546a5f5cbb0889c'
    old=subprocess.check_output(['git','show',previous+':prompts/team_v2/agent_3_portfolio_planner_v4.txt'],cwd=ROOT)
    new=(folder/'agent_3_portfolio_planner_v4.txt').read_bytes()
    shared=subprocess.check_output(['git','show',previous+':prompts/team_v2/00_shared_system_v3.txt'],cwd=ROOT)
    assert shared==(folder/'00_shared_system_v3.txt').read_bytes()
    assert section(old,'PLAN_REVIEW')==section(new,'PLAN_REVIEW')
    assert section(old,'PM_POST_CLOSE')==section(new,'PM_POST_CLOSE')
    assert old.split('═══════ 会话 '.encode())[0].replace(b'agent-3/4.0.3',b'agent-3/4.0.4')==new.split('═══════ 会话 '.encode())[0]
    result=dict(status='EXACT_AUTHOR_PINS_AND_PLAN_REVIEW_SECTION_IDENTICAL',
        actual_collected_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),previous_head=previous,
        files=actual,shared_bytes_identical=True,plan_review_section_bytes_identical=True,
        plan_review_section_sha256=hashlib.sha256(section(new,'PLAN_REVIEW')).hexdigest(),
        pm_post_close_section_bytes_identical=True,role_preface_only_version_changed=True,
        inheritance_gate='PENDING_ONE_REAL_1016_CONFIRMATION_AND_IDENTICAL_PASS_SET',model_calls=0)
    output=ROOT/'artifacts/team-v2-20261016/AUTHOR0298_PIN_AND_A3_SECTION_PARITY.json'
    if output.exists():raise ValueError('AUTHOR_AUDIT_ORIGINAL_ALREADY_EXISTS')
    save(output,result);print(json.dumps(result,ensure_ascii=True))


if __name__=='__main__':main()
