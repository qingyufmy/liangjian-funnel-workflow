"""One immutable reviewed batch per monitor tick; legacy meetings stay intact.

The authority is the workflow database's atomic active pointer and READY
bundle. This facade reads those frozen decisions, never recreates a review or
copies decisions into the independent Team database.
"""
from copy import deepcopy
import hashlib
import sqlite3
from contextlib import closing
from pathlib import Path

from .team_v2_store import TeamMeetingStore, canonical


class ReviewScopeError(ValueError):
    """A scope race/corruption blocks this tick, not the whole account."""


def read_scope_token(db, *, account_id, trade_date):
    """Called inside the same read transaction as execution-plan reads."""
    exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='active_reviewed_batch_scope'").fetchone()
    if not exists:
        return None
    row = db.execute('SELECT scope_id,bundle_sha256,revision FROM active_reviewed_batch_scope '
        'WHERE account_id=? AND trade_date=?', (account_id, trade_date)).fetchone()
    if row is None:
        return None
    return dict(scope_id=row[0], bundle_sha256=row[1], pointer_revision=row[2])


def token_of(scope):
    if scope is None:
        return None
    return {key: scope[key] for key in ('scope_id', 'bundle_sha256', 'pointer_revision')}


def assert_scope_token(db, expected, *, account_id, trade_date):
    if read_scope_token(db, account_id=account_id, trade_date=trade_date) != expected:
        raise ReviewScopeError('TEAM_REVIEW_SCOPE_CHANGED_RETRY')


class ReviewedScopeStore:
    def __init__(self, store, path, *, account_id, trade_date, scope):
        self._store = store
        self._workflow_path = Path(path)
        self._account = account_id
        self._day = trade_date
        self._scope = deepcopy(scope)
        self.review_scope_token = token_of(scope)
        self.pm_meeting_id = 'morning-pm:' + trade_date
        if scope is None:
            return
        bundle = scope['bundle']
        if (hashlib.sha256(canonical(bundle).encode()).hexdigest() != scope['bundle_sha256']
                or bundle.get('schema_version') != 'reviewed-batch/1'
                or bundle.get('scope_id') != scope['scope_id'] or bundle.get('status') != 'READY'
                or bundle.get('account_id') != account_id or bundle.get('trade_date') != trade_date
                or bundle.get('risk', {}).get('status') != 'READY'
                or bundle.get('pm', {}).get('status') != 'READY'
                or bundle.get('pm', {}).get('model_validated') is not True):
            raise ReviewScopeError('TEAM_REVIEW_SCOPE_BUNDLE_INVALID')
        engine = bundle.get('engine')
        source = engine.get('source') if isinstance(engine, dict) else None
        if (not isinstance(source, dict) or engine.get('status') != 'COMPLETE'
                or source.get('status') != 'COMPLETE' or source.get('trade_date') != trade_date
                or source.get('lane_id') != bundle.get('lane_id')
                or not isinstance(source.get('plan_rows'), list)
                or hashlib.sha256(canonical(source['plan_rows']).encode()).hexdigest() != source.get('plan_rowset_sha256')):
            raise ReviewScopeError('TEAM_REVIEW_SCOPE_ENGINE_INVALID')
        for role in ('a6', 'pm'):
            evidence = bundle.get('evidence', {}).get(role, {})
            if (evidence.get('validated') is not True
                    or any(not isinstance(evidence.get(key), str) or not evidence[key]
                        for key in ('meeting_id', 'input_sha256', 'call_id', 'raw_sha256', 'journal_ref'))):
                raise ReviewScopeError('TEAM_REVIEW_SCOPE_EVIDENCE_INVALID')
        if bundle['pm'].get('source_call_id') != bundle['evidence']['pm']['call_id']:
            raise ReviewScopeError('TEAM_REVIEW_SCOPE_PM_BINDING_INVALID')
        self.pm_meeting_id = bundle['evidence']['pm']['meeting_id']
        # Decode/compile with the exact existing consumer semantics now, so a
        # malformed READY bundle cannot make a half-old/half-new tick later.
        try:
            TeamMeetingStore._decode(canonical(bundle['risk']))
            if self._pm_allocation(bundle) is None:
                raise ValueError('PM_ALLOCATION_UNAVAILABLE')
        except (ValueError, TypeError, KeyError, ArithmeticError) as exc:
            raise ReviewScopeError('TEAM_REVIEW_SCOPE_DECISION_INVALID') from exc

    def __getattr__(self, name):
        return getattr(self._store, name)

    def ensure_current(self):
        path = self._workflow_path
        if not path.is_file() or path.is_symlink():
            raise ReviewScopeError('TEAM_REVIEW_SCOPE_DATABASE_UNAVAILABLE')
        try:
            with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=2)) as db:
                db.execute('PRAGMA query_only=ON')
                db.execute('BEGIN')
                assert_scope_token(db, self.review_scope_token, account_id=self._account, trade_date=self._day)
        except sqlite3.Error as exc:
            raise ReviewScopeError('TEAM_REVIEW_SCOPE_READ_UNAVAILABLE') from exc

    def _bind(self, account_id, trade_date):
        if (account_id, trade_date) != (self._account, self._day):
            raise ReviewScopeError('TEAM_REVIEW_SCOPE_ACCOUNT_DATE_MISMATCH')
        self.ensure_current()

    def load_morning_engine(self, account_id, trade_date):
        self._bind(account_id, trade_date)
        if self._scope is None:
            return self._store.load_morning_engine(account_id, trade_date)
        return deepcopy(self._scope['bundle']['engine'])

    def load_morning_risk(self, account_id, trade_date, *, source_call_id):
        self._bind(account_id, trade_date)
        if self._scope is None:
            return self._store.load_morning_risk(account_id, trade_date, source_call_id=source_call_id)
        allowed = {'A6:morning:' + trade_date, self._scope['bundle']['evidence']['a6']['call_id']}
        if source_call_id not in allowed:
            raise ReviewScopeError('TEAM_REVIEW_SCOPE_A6_BINDING_INVALID')
        return TeamMeetingStore._decode(canonical(self._scope['bundle']['risk']))

    def _pm_allocation(self, bundle):
        from .team_v2_allocation import compile_pm_allocation
        evidence = bundle['evidence']['pm']
        return compile_pm_allocation(bundle['pm'], account_id=self._account, trade_date=self._day,
            meeting_id=evidence['meeting_id'], input_sha256=evidence['input_sha256'])

    def _pm_bind(self, meeting_id, account_id, trade_date):
        self._bind(account_id, trade_date)
        if self._scope is not None and meeting_id != self.pm_meeting_id:
            raise ReviewScopeError('TEAM_REVIEW_SCOPE_PM_MEETING_MISMATCH')

    def load_pm_allocation(self, meeting_id, *, account_id, trade_date):
        self._pm_bind(meeting_id, account_id, trade_date)
        if self._scope is None:
            return self._store.load_pm_allocation(meeting_id, account_id=account_id, trade_date=trade_date)
        return self._pm_allocation(self._scope['bundle'])

    def load_pm_target_upper(self, meeting_id, *, account_id, trade_date):
        self._pm_bind(meeting_id, account_id, trade_date)
        if self._scope is None:
            return self._store.load_pm_target_upper(meeting_id, account_id=account_id, trade_date=trade_date)
        from ..pipeline.team_v2_risk_schema import number
        body = self._scope['bundle']['pm']
        if body.get('session') != 'PM_PREMARKET':
            return None
        bounds = (body.get('deployment_target') or {}).get('baseline_range')
        if not isinstance(bounds, list) or len(bounds) != 2:
            return None
        low, high = map(number, bounds)
        if low is None or high is None or not 0 <= low <= high <= 100:
            raise ReviewScopeError('PM_TARGET_UPPER_INVALID')
        return high


def freeze_review_scope(settings, store, *, account_id, trade_date):
    from .reviewed_batch_switch import load_active_reviewed_batch
    from .state import RuntimeStateError
    try:
        scope = load_active_reviewed_batch(settings.state_db_path, account_id=account_id, trade_date=trade_date)
        if scope is not None and scope['bundle']['lane_id'] != settings.research_primary_lane_id:
            raise ReviewScopeError('TEAM_REVIEW_SCOPE_LANE_MISMATCH')
        return ReviewedScopeStore(store, settings.state_db_path, account_id=account_id,
            trade_date=trade_date, scope=scope)
    except ReviewScopeError:
        raise
    except (ValueError, TypeError, KeyError, sqlite3.Error, RuntimeStateError) as exc:
        raise ReviewScopeError('TEAM_REVIEW_SCOPE_UNPROVEN') from exc
