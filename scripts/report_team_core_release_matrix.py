"""Actual current gate matrix, not a final candidate or release permission."""
import argparse,hashlib,json,subprocess,sys
import yaml
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'scripts'))
from run_team_a5_frozen_dryrun import save


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    base=ROOT/'artifacts/team-v2-20261016';out=a.output.resolve()
    if out.exists() or not out.is_relative_to(base.resolve()):raise ValueError('NEW_OWNED_OUTPUT_REQUIRED')
    def read(name,file='RESULT.json'):
        path=base/name/file
        if not path.is_file():return dict(status='NO_COMPLETED_RECEIPT',path=str(path.relative_to(ROOT)))
        raw=path.read_bytes();body=json.loads(raw)
        return dict(path=str(path.relative_to(ROOT)),sha256=hashlib.sha256(raw).hexdigest(),
            status=body.get('status','NOT_DECLARED'),reason_codes=body.get('reason_codes'),
            context_time=body.get('context_time'),actual_finished_at=body.get('actual_finished_at'),
            source_head=body.get('source_head',body.get('head')),model_requests=body.get('model_requests',body.get('model_calls')))
    source=base/'continuous-source-account-20261017T1427/SOURCE_ACCOUNT_AUDIT.json'
    audit=json.loads(source.read_bytes());denominators=[p['timeline']['legal_rate_denominator'] for p in audit['passes']]
    if denominators!=[0,0] or audit['control_release_gate']!='NOT_PASSED':raise ValueError('CONTROL_GATE_REVIEW_REQUIRED_SOURCE_CHANGED')
    config_path=ROOT/'config/funnel_config_v2.yaml'
    config=yaml.safe_load(config_path.read_text(encoding='utf-8'))
    a1_mode=config['agent_1'].get('team_role_mode','team')
    if a1_mode not in ('team','production'):raise ValueError('TEAM_A1_ROLE_MODE_INVALID')
    result=dict(actual_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
        head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
        preliminary=True,release_approved=False,final_candidate=False,
        a1_discovery306={d:read('a1-author306-'+d+'-discovery-real') for d in ('1015','1014','1016')},
        a1_release_role=dict(mode=a1_mode,authority='0323_EXPLICIT_PRODUCTION_A1_FALLBACK_IF_ANY306_DAY_FAILS',
            config_sha256=hashlib.sha256(config_path.read_bytes()).hexdigest(),
            new_role_release_gate='NOT_PASSED',original_pool='EXISTING_PRODUCTION_A1_GENERATION_UNCHANGED'),
        a1_mapping='NOT_RUN_NEW_A1_DISABLED' if a1_mode=='production' else 'ONLY_AFTER_ALL_THREE_CURRENT_DISCOVERY_DAYS_PASS',
        a2_305={d:read('a2-author305-'+('feedback308-' if d=='1016' else '')+d+'-real') for d in ('1014','1015','1016')},
        a3_plan_review406=read('a3-author406-1016-inherit-confirm-real'),
        pm406={d:read('pm-author406-from107-'+d+'-real') for d in ('1014','1015','1016')},
        a6_premarket107={d:read('a6-author107-'+d+'-real') for d in ('1014','1015','1016')},
        a5_207={d:read('a5-author207-'+d+'-catalog-bound-real') for d in ('1014','1015','1016')},
        eod_pm_a6={d:read('eod-author315-'+d+'-real') for d in ('1014','1015','1016')},
        a4_probe=read('a4-author315-single-real'),a4_probe_authority='0319_CONDITIONAL_FORMAT_LATENCY_ONLY_NOT_CONTROL_GATE',
        control_correctness=dict(source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
            source_status=audit['status'],actual_model_denominators=denominators,legal_rate=None,
            authority='0277_A_GATE_NOT_PASSED_RECORD_REQUIRED_UNLESS_LATER_ACTUALLY_PROVEN',
            permission='NO_AUTOMATIC_CONTROL_FROM_PROBE_OR_EMPTY_LEDGER_EQUALITY',returns='NOT_CLAIMED'),
        g5=read('g5-five-day-original-20261017T1544','RECEIPT.json'),
        g3_g4='0319_INDEPENDENT_PACKAGE_NOT_REQUIRED_FOR_CORE_NO_DISPATCH_OR_TRADING_ACCEPTANCE',
        h1='F79_EDAC_ALREADY_ACCEPTED_SUNDAY_DAYTIME_ONLY_DONT_REPEAT_TESTS',
        operations=dict(team_deployed=False,final_head_full='PENDING_SOURCE_CONVERGENCE',vm_delta='PENDING',
            rollback='MUST_EQUAL_ACTUAL_POST_H1_PRODUCTION_HEAD_NOT_BARE_2B',package_sha='PENDING'),
        production_writes=0,model_calls=0)
    out.mkdir(parents=True);save(out/'MATRIX.json',result)
    print(json.dumps(dict(path=str(out/'MATRIX.json'),sha256=hashlib.sha256((out/'MATRIX.json').read_bytes()).hexdigest(),
        final_candidate=False,release_approved=False,control_legal_rate=None)));return 0

if __name__=='__main__':sys.exit(main())
