"""Preview/apply audited A4 derived-field corrections; no orders or notifications."""
import argparse
import hashlib
import json
from pathlib import Path
from datetime import date

from liangjian_funnel.settings import Settings
from liangjian_funnel.runtime.state import RuntimeStore
from liangjian_funnel.reporting import atomic_write_json


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-date", required=True, type=date.fromisoformat)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit("REFUSE_OVERWRITE")
    settings = Settings.from_env(root=Path.cwd())
    store = RuntimeStore(settings.state_db_path)
    day = args.trade_date.isoformat()
    before_lives = store.list_a4_signal_lifecycles(trade_date=day, limit=1000)
    before_labels = store.list_outcome_labels(trade_date=day, stage="A4")
    fills_before = digest(store.list_fills())
    events_before = digest(store.list_monitor_events())
    preview = {
        "lifecycles": [store.rebuild_a4_exit_projection(row["lifecycle_id"]) for row in before_lives],
        "labels": [store.repair_a4_outcome_reasons(row["label_id"]) for row in before_labels],
    }
    # Evidence is persisted before the first correction; partial failures
    # retain the exact previous projections and the approved scope.
    before_path = args.output.with_name(args.output.stem + "-before.json")
    if before_path.exists():
        raise SystemExit("REFUSE_OVERWRITE")
    atomic_write_json(before_path, {"trade_date": day, "apply_requested": args.apply,
        "lifecycles": before_lives, "labels": before_labels, "preview": preview,
        "fills_sha256": fills_before, "events_sha256": events_before})
    result = preview
    if args.apply:
        result = {
            "lifecycles": [store.rebuild_a4_exit_projection(row["lifecycle_id"], apply=True) for row in before_lives],
            "labels": [store.repair_a4_outcome_reasons(row["label_id"], apply=True) for row in before_labels],
        }
    unchanged = fills_before == digest(store.list_fills()) and events_before == digest(store.list_monitor_events())
    result.update(trade_date=day, apply_requested=args.apply, raw_events_and_fills_unchanged=unchanged,
                  before_artifact=str(before_path))
    atomic_write_json(args.output, result)
    print(json.dumps(result, ensure_ascii=False))
    if not unchanged:
        raise SystemExit("RAW_EVIDENCE_CHANGED_DURING_REPAIR")


if __name__ == "__main__":
    main()
