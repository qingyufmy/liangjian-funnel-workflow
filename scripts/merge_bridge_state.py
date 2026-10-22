"""Only merge owned fields; never overwrite canonical bridge with a subobject."""
import argparse
import json
from pathlib import Path
from liangjian_funnel.runtime.shared_json import merge_shared_json

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--state',type=Path,required=True)
    parser.add_argument('--patch',type=Path,required=True);args=parser.parse_args()
    result=merge_shared_json(args.state,json.loads(args.patch.read_bytes()),allowed_prefix='codex_')
    print(json.dumps(dict(status='MERGED_VALIDATED',keys=len(result))))
