"""Original frozen minute windows -> typed coordinator source, never permission.

One RO transaction, bounded decoded-window cache, original arrival/hash binding.
Missing minutes are not repaired from another plan, a later snapshot or history.
"""
from collections import OrderedDict
from dataclasses import asdict, replace
from datetime import timedelta
import hashlib
import json
from pathlib import Path
import sqlite3
import zlib

from liangjian_paper_broker.ledger import dump

from .team_v2_replay_source import project_snapshot, sha
from ..data.mootdx import MinuteBar
from ..runtime.team_v2_minute_projection import project_closed_minute
from ..runtime.team_v2_quote_evidence import replay_quote_from_closed_bar
from ..runtime.team_v2_sources import stamp
from ..runtime.team_v2_tradability import production_monitor_tradability


def _file_hash(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


class FrozenMinuteMaterializer:
    def __init__(self, database, *, expected_sha256, plans, settings, cache_size=64):
        self.path = Path(database).resolve()
        if (type(cache_size) is not int or not 1 <= cache_size <= 512
                or not self.path.is_file() or Path(database).is_symlink()
                or _file_hash(self.path) != expected_sha256):
            raise ValueError('REPLAY_MATERIALIZER_SOURCE_INVALID')
        self.pin = expected_sha256
        self.plans = {pid:dict(row) for pid,row in plans.items()}
        self.verified_plans = set()
        self.settings = settings
        self.cache_size = cache_size
        self.cache = OrderedDict()
        self.db = None
        self.snapshot_reads = 0

    def __enter__(self):
        if self.db is not None or _file_hash(self.path) != self.pin:
            raise ValueError('REPLAY_MATERIALIZER_SOURCE_CHANGED_BEFORE_READ')
        self.verified_plans.clear()
        self.db = sqlite3.connect(self.path.as_uri()+'?mode=ro', uri=True, timeout=2)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA query_only=ON')
        self.db.execute('BEGIN')
        return self

    def __exit__(self, kind, value, traceback):
        try:
            self.db.rollback()
        finally:
            self.db.close()
            self.db = None
            self.cache.clear()
        if _file_hash(self.path) != self.pin:
            raise ValueError('REPLAY_MATERIALIZER_SOURCE_CHANGED')

    def _window(self, frame, row):
        if row['plan_id'] not in self.verified_plans:
            original = self.db.execute('SELECT * FROM execution_plans WHERE plan_id=?', (row['plan_id'],)).fetchall()
            if len(original) != 1 or sha(dict(original[0])) != sha(row):
                raise ValueError('REPLAY_MATERIALIZER_DATABASE_PLAN_MISMATCH')
            self.verified_plans.add(row['plan_id'])
        key = (frame['original_snapshot_id'], row['symbol'], frame['decision_at'])
        if key in self.cache:
            snapshot, bars, proof = self.cache.pop(key)
            self.cache[key] = (snapshot, bars, proof)
        else:
            rows = self.db.execute("SELECT * FROM minute_decision_snapshots WHERE snapshot_id=? AND symbol=? AND interval='1m'",
                key[:2]).fetchall()
            self.snapshot_reads += 1
            if len(rows) != 1:
                raise ValueError('REPLAY_MATERIALIZER_SNAPSHOT_AMBIGUOUS')
            snapshot = dict(rows[0])
            proof = project_snapshot(snapshot, symbol=row['symbol'], decision_at=frame['decision_at'],
                event_created_at=min(stamp(e['created_at']) for e in frame['original_events']))
            bars = tuple(MinuteBar.model_validate(v) for v in json.loads(zlib.decompress(snapshot['payload_zlib'])))
            self.cache[key] = (snapshot, bars, proof)
            if len(self.cache) > self.cache_size:
                self.cache.popitem(last=False)
        # Another plan sharing this window still needs its own original arrival
        # and frame source proof. A cache hit must not borrow that permission.
        if (stamp(snapshot['captured_at']) > min(stamp(e['created_at']) for e in frame['original_events'])
                or sha(proof) != sha(frame['source'])):
            raise ValueError('REPLAY_MATERIALIZER_FRAME_SOURCE_MISMATCH')
        return bars, proof

    def materialize(self, batch):
        from ..workflow import _intraday_market_context
        if self.db is None:
            raise ValueError('REPLAY_MATERIALIZER_REQUIRES_RO_CONTEXT')
        at = stamp(batch['decision_at'])
        if (batch['trade_date'] != at.date().isoformat() or batch['quote_basis'] != 'REPLAY_BAR_DERIVED'
                or stamp(batch['closed_bar_end']) != at-timedelta(minutes=1)):
            raise ValueError('REPLAY_MATERIALIZER_BATCH_BINDING_INVALID')
        original_entry_ids = {f['plan_id'] for f in batch['entry_source_frames']}
        source_ids = set()
        projected = {}
        paper = {}
        limited = []
        conflicts = set()
        live = dict(status='DATA_BLOCKED', reason_code='REPLAY_HISTORICAL_LIVE_AUTHORITY_UNAVAILABLE',
            emotion_state='DATA_BLOCK', leader_open=False, quote_basis='REPLAY_BAR_DERIVED')
        for frame in batch['source_frames']:
            pid = frame['plan_id']
            if (pid in source_ids or frame['decision_at'] != batch['decision_at']
                    or sha({k:v for k,v in frame.items() if k != 'frame_sha256'}) != frame['frame_sha256']):
                raise ValueError('REPLAY_MATERIALIZER_FRAME_BINDING_INVALID')
            source_ids.add(pid)
            if frame['status'] != 'SOURCE_BOUND_RULES_PENDING':
                limited.append(dict(plan_id=pid, reason_codes=frame['reason_codes']))
                continue
            row = self.plans.get(pid)
            if row is None or sha(row) != frame['original_plan_row_sha256']:
                raise ValueError('REPLAY_MATERIALIZER_PLAN_HASH_MISMATCH')
            raw = json.loads(row['payload_json'])
            if raw.get('symbol') != row['symbol']:
                raise ValueError('REPLAY_MATERIALIZER_PLAN_SYMBOL_MISMATCH')
            bars, source = self._window(frame, row)
            minute = project_closed_minute(bars, (), observed_at=at)
            bar = minute.get('paper_bar')
            if bar is None:
                limited.append(dict(plan_id=pid, reason_codes=minute['reason_codes']))
                continue
            quote = replay_quote_from_closed_bar(bars[-1], decision_as_of=at)
            rule = production_monitor_tradability(self.settings, symbol=row['symbol'], now=at,
                bar=bar, quote=quote, plan=raw, minute_bars=bars)
            if rule.get('status') != 'KNOWN':
                limited.append(dict(plan_id=pid, reason_codes=[rule['reason_code']]))
                continue
            executable = replace(bar, tradability_known=True, halted=rule['halted'],
                limit_up=rule['limit_up'], limit_down=rule['limit_down'])
            if row['symbol'] in paper and paper[row['symbol']] != executable:
                conflicts.add(row['symbol'])
            paper[row['symbol']] = executable
            context = _intraday_market_context(row['symbol'], bars, (), current=at,
                execution_cutoff=at-timedelta(minutes=1), live_market_state=live)
            context.update(realtime_quote=source['quote'], original_tradability_contract=rule,
                market_authority='REPLAY_ONLY_NO_LIVE_AUTHORITY')
            event = json.loads(frame['original_events'][0]['payload_json'])
            strategy = event.get('strategy') or {}
            projected[pid] = dict(plan_row=dict(row), original_plan=raw, closed_minute_context=context,
                original_action=frame['original_action'], frame_sha256=frame['frame_sha256'],
                original_snapshot_sha256=source['original_snapshot_sha256'], paper_bar=executable,
                quote=quote, tradability=rule, entry_source_eligible=pid in original_entry_ids,
                tech_reference={**{k:'MET' for k in strategy.get('met_conditions',[])},
                    **{k:'UNMET' for k in strategy.get('unmet_conditions',[])}})
        if not original_entry_ids <= source_ids:
            raise ValueError('REPLAY_MATERIALIZER_ENTRY_SOURCE_MISSING')
        for symbol in conflicts:
            paper.pop(symbol)
            for pid, view in list(projected.items()):
                if view['plan_row']['symbol'] == symbol:
                    projected.pop(pid)
                    limited.append(dict(plan_id=pid, reason_codes=['REPLAY_SAME_SYMBOL_BAR_OR_RULE_CONFLICT']))
        return dict(status='SOURCE_MATERIALIZED_NOT_EXECUTION', trade_date=batch['trade_date'],
            decision_at=batch['decision_at'], projected=projected, closed_bars=paper,
            entry_plan_ids=sorted(original_entry_ids & set(projected)), limited=limited,
            absent_original_plan_ids=batch['absent_original_plan_ids'],
            historical_live_state=live, model_permission='PM_AND_A6_RESULTS_STILL_REQUIRED',
            quote_basis='REPLAY_BAR_DERIVED', source_requests=0, production_writes=0,
            source_digest=sha(json.loads(dump({pid:dict(frame_sha256=view['frame_sha256'],
                snapshot_sha256=view['original_snapshot_sha256'],paper_bar=asdict(view['paper_bar']))
                for pid,view in sorted(projected.items())}))))
