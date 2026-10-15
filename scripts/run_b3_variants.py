"""Run B3 variants from explicit local immutable evidence copies, not production."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from liangjian_funnel.evaluation.b3_variant_replay import run_variants


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-db', type=Path, required=True)
    parser.add_argument('--market-root', type=Path, required=True)
    parser.add_argument('--minute-db', type=Path)
    parser.add_argument('--daily-db', type=Path)
    parser.add_argument('--auction-evidence', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--captured-at', required=True)
    parser.add_argument('--variants', default='V0,V1,V2,V3,V4,V5')
    args = parser.parse_args()
    result = run_variants(state_db=args.state_db, minute_db=args.minute_db,
        market_state_root=args.market_root, daily_db=args.daily_db, auction_evidence=args.auction_evidence,
        output=args.output, captured_at=args.captured_at, variants=tuple(args.variants.split(',')))
    print(json.dumps({key: value for key, value in result.items() if key != 'V0_diffs'}, ensure_ascii=False))
    if result['V0_diff_count'] or result['V0_source_gap_count']:
        return 2
    return 0 if 'V0' not in result['variants'] or result['V0_evaluated_window_count'] else 3


if __name__ == '__main__':
    raise SystemExit(main())
