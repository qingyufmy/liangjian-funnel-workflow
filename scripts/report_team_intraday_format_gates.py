"""Summarize actual six isolated calls and limited original G5 evidence."""
import argparse,hashlib,json,subprocess,sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'scripts'))
from run_team_a5_frozen_dryrun import save


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    base=ROOT/'artifacts/team-v2-20261016';out=a.output.resolve()
    if out.exists() or not out.is_relative_to(base.resolve()):raise ValueError('NEW_OWNED_OUTPUT_REQUIRED')
    calls=[]
    for role in ('radar','pmi'):
        for day in ('1014','1015','1016'):
            directory=base/(role+'-author315-'+day+'-real');path=directory/'RESULT.json';raw=path.read_bytes();result=json.loads(raw)
            prepared=json.loads((directory/'PREPARED.json').read_bytes())
            if (result['status']!='PASS_EMPTY_NON_ON_FORMAT_LATENCY_ONLY' or result['model_calls']!=1
                    or not result['validation']['valid'] or result['latency_ms']>60000
                    or any(result[k] for k in ('orders','paper_ledger_writes','production_writes','positive_candidate_gate','live_trigger_acceptance'))):
                raise ValueError('INTRADAY_FORMAT_GATE_NOT_PASSED')
            calls.append(dict(role=result['role'],session=result['session'],source_day=result['source_day'],
                latency_ms=result['latency_ms'],status=result['status'],path=str(path.relative_to(ROOT)),
                result_sha256=hashlib.sha256(raw).hexdigest(),original_cache_sha256=prepared['original_source_sha256'],
                original_session='NO_ORIGINAL_SESSION',model_calls=1,emotion_state=prepared['replacements']['LIVE_MARKET_STATE']['emotion_state']))
    g5=base/'g5-five-day-original-20261017T1544/RECEIPT.json';raw=g5.read_bytes();original=json.loads(raw)
    body=dict(actual_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
        head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),calls=calls,
        format_gate='THREE_SOURCE_DAYS_PER_SESSION_PASSED_EMPTY_NON_ON_ONLY',
        original_g5=dict(path=str(g5.relative_to(ROOT)),sha256=hashlib.sha256(raw).hexdigest(),
            actual_started_at=original['actual_started_at'],actual_finished_at=original['actual_finished_at'],
            original_head=original['head'],two_pass_equal=original['two_pass_equal'],daily=original['daily'],
            original_emotion_proved_ticks=original['original_emotion_proved_ticks'],
            emotion_switch_history=original['emotion_switch_history'],status=original['status']),
        code='RESEARCH_SESSIONS_SCHEMA_AND_TRANSPORT_ONLY_NOT_LIVE_PIPELINE',
        replay='G5_ORIGINAL_SOURCE_LIMITED_NOT_FULL_GATE',operations='NOT_DEPLOYED',
        strategy='NO_POSITIVE_PICK_OR_FUNDED_ACCOUNT_OR_RETURNS_PROOF',release_approved=False,
        production_writes=0,orders=0)
    out.mkdir(parents=True);save(out/'GATES.json',body)
    print(json.dumps(dict(path=str(out/'GATES.json'),sha256=hashlib.sha256((out/'GATES.json').read_bytes()).hexdigest(),
        completed_model_calls=6,release_approved=False)));return 0

if __name__=='__main__':sys.exit(main())
