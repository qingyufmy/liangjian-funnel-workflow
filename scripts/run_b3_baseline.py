"""Run local-only streaming B3 V0 from an explicit frozen evidence copy."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from liangjian_funnel.evaluation.b3_baseline import run_baseline


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--state-db',required=True,type=Path)
    p.add_argument('--minute-db',type=Path)
    p.add_argument('--market-root',required=True,type=Path)
    p.add_argument('--output',required=True,type=Path)
    p.add_argument('--captured-at')
    a=p.parse_args()
    result=run_baseline(state_db=a.state_db,minute_db=a.minute_db,market_state_root=a.market_root,output=a.output,captured_at=a.captured_at)
    print(json.dumps({key:value for key,value in result.items() if key!='raw_diffs'},ensure_ascii=False))
    return 0 if not result['raw_diff_count'] and not result['gap_reasons'] else 2


if __name__=='__main__':raise SystemExit(main())
