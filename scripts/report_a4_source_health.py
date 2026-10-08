"""Summarize immutable per-minute source health without touching runtime DB."""
import argparse
import json
from pathlib import Path

from liangjian_funnel.data.source_health import summarize_health
from liangjian_funnel.reporting import atomic_write_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--quality-dir', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit('REFUSE_OVERWRITE')
    records, invalid = [], []
    for path in sorted(args.quality_dir.glob('*.json')):
        try:
            record = json.loads(path.read_text(encoding='utf-8'))
            if not isinstance(record, dict):
                raise ValueError('not object')
            records.append(record)
        except (OSError, ValueError):
            invalid.append(path.name)
    result = summarize_health(records)
    result.update(input_files=len(records), unreadable_files=invalid,
                  source_health_complete=bool(records) and not invalid and result['legacy_rows_without_health'] == 0)
    atomic_write_json(args.output, result)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()
