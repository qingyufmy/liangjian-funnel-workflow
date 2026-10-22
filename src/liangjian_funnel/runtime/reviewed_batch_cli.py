"""Explicit business entry for isolated reviews and reviewed-only switching.

No research generation, direct SQL writes, auction activation or service control.
Production mutation still requires the release's separately approved execution.
"""
import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
from zoneinfo import ZoneInfo


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('phase', choices=('review-isolated', 'stage-reviewed', 'switch-reviewed', 'inspect'))
    parser.add_argument('--root', required=True, type=Path)
    parser.add_argument('--scope-id', required=True)
    parser.add_argument('--staging-root', type=Path)
    parser.add_argument('--baseline-team-db', type=Path)
    parser.add_argument('--research-run-id')
    parser.add_argument('--cas', type=Path)
    parser.add_argument('--allow-model-calls', action='store_true')
    parser.add_argument('--allow-reviewed-write', action='store_true')
    args = parser.parse_args(argv)
    # Permission gates precede Settings/Store construction and any file access.
    if args.phase == 'review-isolated' and not args.allow_model_calls:
        raise ValueError('REVIEWED_CLI_MODEL_CALL_AUTHORITY_REQUIRED')
    if args.phase in ('stage-reviewed', 'switch-reviewed') and not args.allow_reviewed_write:
        raise ValueError('REVIEWED_CLI_WRITE_AUTHORITY_REQUIRED')
    if re.fullmatch('[A-Za-z0-9_-]{1,40}', args.scope_id) is None:
        raise ValueError('REVIEWED_CLI_SCOPE_INVALID')
    from ..settings import Settings
    from .calendar import ExchangeTradingCalendar
    from .team_v2_morning import ACCOUNT
    from .team_v2_sources import read_planning_scope
    from .reviewed_batch_switch import load_active_reviewed_batch
    settings = Settings.from_env(root=args.root)
    calendar = ExchangeTradingCalendar()
    now = datetime.now(ZoneInfo('Asia/Shanghai'))
    day = now.date().isoformat()
    if args.phase == 'inspect':
        scope = load_active_reviewed_batch(settings.state_db_path, account_id=ACCOUNT, trade_date=day)
        result = dict(phase=args.phase, trade_date=day, production_writes=0, active_scope=None if scope is None else
            {key: scope[key] for key in ('scope_id', 'bundle_sha256', 'pointer_revision')})
    elif args.phase == 'review-isolated':
        if args.baseline_team_db is None:
            raise ValueError('REVIEWED_CLI_BASELINE_REQUIRED')
        from .reviewed_batch_morning import prepare_candidate_morning_engine, run_candidate_morning_reviews
        source = read_planning_scope(settings, now=now, calendar=calendar)
        prepare_candidate_morning_engine(settings=settings, source=source,
            baseline_team_db=args.baseline_team_db, scope_id=args.scope_id, calendar=calendar)
        actual = run_candidate_morning_reviews(settings=settings, scope_id=args.scope_id, calendar=calendar)
        result = {key: actual[key] for key in ('scope_id', 'trade_date', 'status', 'a6_meeting_id', 'a6_call_id',
            'pm_meeting_id', 'pm_call_id', 'pm_called', 'production_writes', 'publication_calls')}
        if 'reason_code' in actual:
            result['reason_code'] = actual['reason_code']
    elif args.phase == 'stage-reviewed':
        if args.staging_root is None or not args.research_run_id:
            raise ValueError('REVIEWED_CLI_STAGED_SOURCE_REQUIRED')
        if args.staging_root.resolve() == args.root.resolve():
            raise ValueError('REVIEWED_CLI_STAGING_EQUALS_DESTINATION')
        from .reviewed_batch_evidence import build_reviewed_batch_evidence
        staging = Settings.from_env(root=args.staging_root)
        if not staging.state_db_path.resolve().is_relative_to(args.staging_root.resolve()):
            raise ValueError('REVIEWED_CLI_STAGING_DATABASE_OUTSIDE_ROOT')
        source = read_planning_scope(staging, now=now, calendar=calendar)
        rows = [dict(plan_id=row['plan_id'], lane_id=row['lane_id'], symbol=row['symbol'], status=row['status'],
            expires_at=row['expires_at'], valid_from=row['valid_from'], plan_version=row['plan_version'],
            payload=json.loads(row['payload_json'])) for row in source['plan_rows']]
        bundle = build_reviewed_batch_evidence(isolated_root=args.staging_root.resolve(), scope_id=args.scope_id,
            account_id=ACCOUNT, trade_date=day, lane_id=staging.research_primary_lane_id, run_id=args.research_run_id,
            a6_meeting_id=args.scope_id + ':risk', pm_meeting_id=args.scope_id + ':pm', candidate_rows=rows)
        if settings.research_primary_lane_id != bundle['lane_id']:
            raise ValueError('REVIEWED_CLI_DESTINATION_LANE_MISMATCH')
        from .state import RuntimeStore
        result = dict(phase=args.phase, **RuntimeStore(settings.state_db_path).stage_reviewed_batch(bundle),
            active_pointer_changed=False, published_plans=0)
    else:
        if args.cas is None:
            raise ValueError('REVIEWED_CLI_CAS_REQUIRED')
        if args.cas.stat().st_size > 128 * 1024:
            raise ValueError('REVIEWED_CLI_CAS_TOO_LARGE')
        cas = json.loads(args.cas.read_bytes())
        if set(cas) != {'expected_old_rows_sha256', 'expected_old_plan_ids', 'expected_pointer_revision'}:
            raise ValueError('REVIEWED_CLI_CAS_FIELDS_INVALID')
        from .state import RuntimeStore
        result = dict(phase=args.phase, **RuntimeStore(settings.state_db_path).commit_reviewed_batch_switch(args.scope_id, **cas))
    print(json.dumps(result, ensure_ascii=True))
    return 2 if result.get('status') == 'BLOCKED' else 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (ValueError, OSError, KeyError, TypeError, RuntimeError) as exc:
        reason = str(exc)
        safe = reason if re.fullmatch('[A-Z0-9_]{1,160}', reason) else 'REVIEWED_CLI_INPUT_OR_SOURCE_ERROR'
        print(json.dumps(dict(status='BLOCKED', reason_code=safe,
            exception_message_sha256=hashlib.sha256(reason.encode()).hexdigest())))
        raise SystemExit(2)
