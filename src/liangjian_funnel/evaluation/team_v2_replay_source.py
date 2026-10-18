"""Read frozen evidence for 0277 correctness replay; never a live producer.

No model, broker, runtime-store constructor or provider calls. Missing events
and snapshots stay missing; later archive rows cannot repair earlier windows.
"""
from contextlib import closing
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import sqlite3
import zlib

from ..data.mootdx import MinuteBar
from ..runtime.team_v2_quote_evidence import REPLAY_BASIS, replay_quote_from_closed_bar
from ..runtime.team_v2_sources import stamp
from ..runtime.team_v2_store import canonical

SIX_DAYS = ('2026-10-08', '2026-10-09', '2026-10-12',
            '2026-10-13', '2026-10-14', '2026-10-15')


def sha(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def session_minutes(day):
    """Original scheduled decision minutes, no lunch or invented 15:01 tick."""
    base = stamp(day + 'T00:00:00+08:00')
    return tuple(base + timedelta(minutes=n)
                 for start, end in ((9*60+31, 11*60+30), (13*60+1, 15*60))
                 for n in range(start, end+1))


def activation_reason(frame, row, *, terminal_seen=False):
    """Never reclassify an observed terminal event as an active entry plan."""
    at=stamp(frame['decision_at'])
    if terminal_seen or frame['original_action'] in {'PLAN_INVALIDATED','PLAN_EXPIRED'}:
        return 'REPLAY_ORIGINAL_TERMINAL_PLAN_NOT_ACTIVE'
    if row.get('valid_from') is None:
        return 'REPLAY_ORIGINAL_ACTIVATION_UNPROVEN'
    start=stamp(row['valid_from']);end=stamp(row['expires_at'])
    if not start.date()==at.date() or not start<=at<end:
        return 'REPLAY_ORIGINAL_PLAN_OUTSIDE_ACTIVE_WINDOW'
    return None


def project_snapshot(row, *, symbol, decision_at, event_created_at):
    """Exact original payload + chronological closed window, not scalar quotes."""
    at = stamp(decision_at)
    cutoff = at.replace(second=0, microsecond=0) - timedelta(minutes=1)
    if (row['symbol'] != symbol or row['interval'] != '1m'
            or stamp(row['decision_as_of']) != at
            or stamp(row['captured_at']) > stamp(event_created_at)):
        raise ValueError('REPLAY_ORIGINAL_SNAPSHOT_BINDING_INVALID')
    raw = zlib.decompress(row['payload_zlib'])
    if hashlib.sha256(raw).hexdigest() != row['payload_sha256']:
        raise ValueError('REPLAY_ORIGINAL_SNAPSHOT_HASH_MISMATCH')
    body = json.loads(raw)
    if not isinstance(body, list) or not body:
        raise ValueError('REPLAY_ORIGINAL_MINUTE_EMPTY')
    bars = tuple(MinuteBar.model_validate(value) for value in body)
    ends = tuple(bar.bar_end for bar in bars)
    if (any(bar.symbol != symbol or bar.interval != '1m' or bar.evidence_kind != 'MARKET_BAR'
            or bar.bar_end > cutoff or bar.bar_end.date() != cutoff.date() for bar in bars)
            or tuple(sorted(set(ends))) != ends or ends[-1] != cutoff):
        raise ValueError('REPLAY_ORIGINAL_CLOSED_WINDOW_INVALID')
    quote = replay_quote_from_closed_bar(bars[-1], decision_as_of=at)
    return dict(original_snapshot_sha256=row['payload_sha256'],
        original_captured_at=row['captured_at'], execution_cutoff=cutoff.isoformat(),
        bar_count=len(bars), last_bar=body[-1],
        quote=dict(symbol=quote.symbol, last=str(quote.last), quote_time=quote.quote_time.isoformat(),
                   bid=None, ask=None, source_id=quote.source_id, quote_basis=quote.quote_basis,
                   original_bar_sha256=quote.original_bar_sha256),
        quote_basis=REPLAY_BASIS, tradability_status='ORIGINAL_RULE_PROOF_REQUIRED',
        emotion_state='DATA_BLOCK', leader_open=False,
        authority='REPLAY_ONLY_NOT_PRODUCTION_OR_RETURNS')


def frozen_frames(path, *, expected_sha256, days=SIX_DAYS):
    """Yield each observed plan/minute once, retaining duplicate/conflict facts.

    The archived final plan status is NOT historical activation authority. This
    preparer binds original events; the execution driver must decide historical
    eligibility from the event/window evidence and preserve missing activation.
    """
    path = Path(path)
    if not path.is_file() or path.is_symlink():
        raise ValueError('REPLAY_FROZEN_DATABASE_REQUIRED')
    with path.open('rb') as stream:
        before = hashlib.file_digest(stream, 'sha256').hexdigest()
    if before != expected_sha256:
        raise ValueError('REPLAY_FROZEN_DATABASE_HASH_MISMATCH')
    if tuple(sorted(set(days))) != tuple(days) or any(day not in SIX_DAYS for day in days):
        raise ValueError('REPLAY_DAY_SCOPE_INVALID')
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True, timeout=2)) as db:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON'); db.execute('BEGIN')
        plans = {row['plan_id']: dict(row) for row in db.execute('SELECT * FROM execution_plans')}
        for day in days:
            groups = {}; outside = []
            scheduled = {at.isoformat() for at in session_minutes(day)}
            for row in db.execute('SELECT * FROM monitor_events WHERE substr(minute_end,1,10)=? '
                                  'ORDER BY minute_end,lane_id,event_key,event_id', (day,)):
                event = dict(row); body = json.loads(event['payload_json'])
                pid = body.get('plan_id')
                if not pid:
                    outside.append(event); continue
                key = (event['minute_end'], event['lane_id'], pid)
                groups.setdefault(key, []).append((event, body))
            observed_plans = sorted({key[2] for key in groups})
            yield dict(kind='day', trade_date=day, observed_plan_ids=observed_plans,
                scheduled_minutes=sorted(scheduled), unbound_events=outside,
                unbound_events_sha256=sha(outside), original_event_count=sum(map(len, groups.values()))+len(outside),
                source_database_sha256=before, quote_basis=REPLAY_BASIS,
                status='NO_PLAN_BOUND_EVENTS' if not groups else 'OBSERVED_SOURCE_ONLY')
            for (minute, lane, pid), events in sorted(groups.items()):
                event, body = events[0]; plan = plans.get(pid)
                record = dict(kind='plan_minute', trade_date=day, decision_at=minute, lane_id=lane, plan_id=pid,
                    original_events=[e for e, _ in events], original_events_sha256=sha([e for e, _ in events]),
                    original_plan_row_sha256=sha(plan) if plan else None,
                    final_plan_status_not_historical_authority=plan['status'] if plan else None,
                    original_action=event['action'], original_reason_code=event['reason_code'],
                    quote_basis=REPLAY_BASIS, emotion_state='DATA_BLOCK', leader_open=False,
                    original_snapshot_id=body.get('minute_snapshot_id'), status='DATA_LIMITED', reason_codes=[])
                reasons = record['reason_codes']
                if minute not in scheduled: reasons.append('REPLAY_EVENT_OUTSIDE_SESSION')
                if plan is None or plan['symbol'] != body.get('symbol') or plan['lane_id'] != lane:
                    reasons.append('REPLAY_ORIGINAL_PLAN_BINDING_MISSING')
                # Repeated polls remain evidence, not additional model minutes.
                fingerprints = {sha([b, e['action'], e['reason_code'], e['effective']]) for e, b in events}
                if len(fingerprints) != 1: reasons.append('REPLAY_PLAN_MINUTE_EVENT_CONFLICT')
                snapshot_id = body.get('minute_snapshot_id')
                rows = db.execute("SELECT * FROM minute_decision_snapshots WHERE snapshot_id=? "
                    "AND symbol=? AND interval='1m'", (snapshot_id, body.get('symbol'))).fetchall()
                if len(rows) != 1: reasons.append('REPLAY_ORIGINAL_MINUTE_SNAPSHOT_MISSING_OR_AMBIGUOUS')
                elif not reasons:
                    try:
                        record['source'] = project_snapshot(dict(rows[0]), symbol=body['symbol'],
                            decision_at=minute, event_created_at=event['created_at'])
                        record['status'] = 'SOURCE_BOUND_RULES_PENDING'
                    except (ValueError, TypeError, KeyError, zlib.error) as exc:
                        own = {'REPLAY_ORIGINAL_SNAPSHOT_BINDING_INVALID', 'REPLAY_ORIGINAL_SNAPSHOT_HASH_MISMATCH',
                               'REPLAY_ORIGINAL_MINUTE_EMPTY', 'REPLAY_ORIGINAL_CLOSED_WINDOW_INVALID'}
                        reasons.append(str(exc) if str(exc) in own else 'REPLAY_ORIGINAL_MINUTE_INVALID')
                record['frame_sha256'] = sha(record)
                yield record
            for pid in observed_plans:
                seen = {key[0] for key in groups if key[2] == pid}
                yield dict(kind='coverage', trade_date=day, plan_id=pid,
                    observed_distinct_minutes=len(seen & scheduled), scheduled_minutes=240,
                    missing_original_event_minutes=sorted(scheduled-seen),
                    note='NO_LATER_BAR_OR_OTHER_PLAN_IMPUTATION', quote_basis=REPLAY_BASIS)
        db.rollback()
    with path.open('rb') as stream:
        if hashlib.file_digest(stream, 'sha256').hexdigest() != before:
            raise ValueError('REPLAY_FROZEN_DATABASE_CHANGED_DURING_READ')
