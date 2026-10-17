"""Bound only actual 0315 artifacts; missing/running work never becomes PASS."""
import argparse,hashlib,json,subprocess,sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'scripts')]
from run_team_a5_frozen_dryrun import save

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    output=a.output.resolve();base=ROOT/'artifacts/team-v2-20261016'
    if output.exists() or not output.is_relative_to(base.resolve()):raise ValueError('NEW_OWNED_OUTPUT_REQUIRED')
    def evidence(name):
        path=base/name/'RESULT.json'
        if not path.is_file():return dict(status='NO_COMPLETED_RECEIPT')
        raw=path.read_bytes();return dict(result=json.loads(raw),sha256=hashlib.sha256(raw).hexdigest(),path=str(path.relative_to(ROOT)))
    old=subprocess.check_output(['git','show','1e4f845^:prompts/team_v2/agent_3_portfolio_planner_v4.txt'],cwd=ROOT)
    new=(ROOT/'prompts/team_v2/agent_3_portfolio_planner_v4.txt').read_bytes()
    def section(raw):
        text=raw.decode();parts=text.split('═══════ 会话 ')
        return next(x for x in parts[1:] if x.startswith('PLAN_REVIEW（')).encode()
    old_shared=subprocess.check_output(['git','show','1e4f845^:prompts/team_v2/00_shared_system_v3.txt'],cwd=ROOT)
    current_shared=(ROOT/'prompts/team_v2/00_shared_system_v3.txt').read_bytes()
    prior=evidence('a3-author405-1016-inherit-confirm-real');current=evidence('a3-author406-1016-inherit-confirm-real')
    parity=dict(plan_review_section_identical=section(old)==section(new),
        section_sha256=hashlib.sha256(section(new)).hexdigest(),
        shared_system_identical=old_shared==current_shared,shared_sha256=hashlib.sha256(current_shared).hexdigest(),
        same_new_role_pass_set=prior.get('result',{}).get('new_core')==current.get('result',{}).get('new_core'),
        actual_confirmation=current)
    parity['inheritance_allowed_by_0315']=bool(parity['plan_review_section_identical'] and parity['shared_system_identical']
        and parity['same_new_role_pass_set'] and current.get('result',{}).get('status')=='VALIDATED')
    result=dict(actual_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
        head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
        pm406={day:evidence('pm-author406-from107-'+day+'-real') for day in ('1014','1015','1016')},
        a1_304_1015=evidence('a1-author304-1015-discovery-real'),a3_plan_review=parity,
        a4_single_probe=evidence('a4-author315-single-real'),
        eod_fixtures={day:evidence('eod-author315-'+day+'-real') for day in ('1014','1015','1016')},
        source_account_control_gate='UNCHANGED_NOT_PASSED_NO_FUNDED_OR_RETURNS_PROOF',
        production_writes=0,release_approved=False)
    output.mkdir(parents=True);save(output/'GATES.json',result)
    print(json.dumps(dict(path=str(output/'GATES.json'),sha256=hashlib.sha256((output/'GATES.json').read_bytes()).hexdigest(),
        pm_pass=[k for k,v in result['pm406'].items() if v.get('result',{}).get('status')=='PASS'],
        plan_review_inheritance=parity['inheritance_allowed_by_0315'],
        a4_status=result['a4_single_probe'].get('result',{}).get('status'))));return 0

if __name__=='__main__':sys.exit(main())
