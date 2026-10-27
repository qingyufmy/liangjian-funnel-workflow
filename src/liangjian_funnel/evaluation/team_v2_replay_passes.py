"""Two independent executions sharing only immutable model captures.

The paper path is deliberately the same for both executions: it participates
in the live admission binding. First-pass databases are archived, not cleared.
This proves repeatability only, not source coverage, legal rate or returns.
"""
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3

from liangjian_paper_broker.ledger import dump
from ..pipeline.team_v2_judgments import ModelJudgmentJournal

TABLES=('paper_meta','paper_orders','paper_fills','paper_lots','paper_bars',
        'paper_marks','exec_ledger','daily_nav','paper_admissions','paper_risk_days',
        'order_cancellation_sessions','order_cancellations')


def file_hash(path):
    with Path(path).open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


class CapturesOnlyJournal(ModelJudgmentJournal):
    """Read originals, fail BEFORE HTTP if any request lacks the exact capture."""
    def __init__(self,path):
        self.path=Path(path).resolve()
        if not self.path.is_file():raise ValueError('REPLAY_ORIGINAL_MODEL_JOURNAL_REQUIRED')

    def _connect(self):
        db=sqlite3.connect(self.path.as_uri()+'?mode=ro',uri=True,timeout=2)
        db.execute('PRAGMA query_only=ON')
        return db

    def claim(self,*args,**kwargs):raise ValueError('REPLAY_SECOND_PASS_NEW_MODEL_CALL_FORBIDDEN')
    def freeze_transport_budget(self,call_id,*,source_sha256,timeout_seconds):
        original=self.load_transport_budget(call_id,source_sha256=source_sha256)
        if original is None:raise ValueError('REPLAY_ORIGINAL_TRANSPORT_BUDGET_REQUIRED')
        return original
    def capture(self,*args,**kwargs):raise ValueError('REPLAY_SECOND_PASS_CAPTURE_FORBIDDEN')
    def mark_failed(self,*args,**kwargs):raise ValueError('REPLAY_SECOND_PASS_CAPTURE_FORBIDDEN')

    def record_output_projection(self,call_id,*,original,context,projected,changes):
        """Revalidate the first pass view; the second pass cannot write it."""
        from ..pipeline.team_v2_judgments import _json,_hash
        original_sha=_hash(_json(original));context_sha=_hash(_json(context))
        body=dict(original_sha256=original_sha,context_sha256=context_sha,
            projected=projected,changes=changes)
        text=_json(body);expected=(call_id,original_sha,context_sha,text,_hash(text))
        with closing(self._connect()) as db:
            captured=db.execute('SELECT output_hash FROM model_calls WHERE call_id=?',(call_id,)).fetchone()
            row=db.execute('SELECT * FROM model_output_projections WHERE call_id=?',(call_id,)).fetchone()
        if captured is None or captured[0]!=original_sha:
            raise ValueError('MODEL_PROJECTION_ORIGINAL_BINDING_CONFLICT')
        if row!=expected:raise ValueError('REPLAY_ORIGINAL_PROJECTION_REQUIRED_OR_CONFLICT')
        return dict(call_id=call_id,original_sha256=original_sha,
            context_sha256=context_sha,body_sha256=expected[-1],changes=changes)


class NoModelRecallClient:
    def __init__(self):self.attempts=0

    def complete(self,*args,**kwargs):
        self.attempts+=1
        raise ValueError('REPLAY_SECOND_PASS_NETWORK_FORBIDDEN')


def paper_bytes(path):
    if not path.is_file() or path.is_symlink():raise ValueError('REPLAY_OWNED_PAPER_REQUIRED')
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=2)) as db:
        db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
        tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if not set(TABLES).issubset(tables):raise ValueError('REPLAY_PAPER_SCHEMA_INVALID')
        ledger={table:[dict(r) for r in db.execute('SELECT * FROM '+table+' ORDER BY 1,2')] for table in TABLES}
        curve=[dict(r) for r in db.execute('SELECT trade_date,payload_json FROM daily_nav ORDER BY trade_date')]
        db.rollback()
    return dump(ledger).encode(),dump(curve).encode(),dict(
        settlements=len(curve),fills=len(ledger['paper_fills']),
        buy_fills=sum(row['side']=='BUY' for row in ledger['paper_fills']),
        sell_fills=sum(row['side']=='SELL' for row in ledger['paper_fills']))


def _save(path,data):
    with path.open('xb') as stream:stream.write(data)


def _guard_path(path,root):
    resolved=path.resolve()
    if resolved!=path or not resolved.is_relative_to(root) or resolved==root:
        raise ValueError('REPLAY_NEW_OWNED_OUTPUT_REQUIRED')
    for parent in (path,*path.parents):
        if parent.is_symlink() or getattr(parent,'is_junction',lambda:False)():
            raise ValueError('REPLAY_PATH_REDIRECTION_FORBIDDEN')
        if parent==root:break


def run_two_passes(output,*,allowed_root,run_pass):
    """Callback(work, journal, client_override) executes original frozen inputs.

    First override=None permits the explicitly authorized caller's client.
    Second pass gets a RO journal and a rejecting client. Input/model/path
    binding must match; no old response is adapted to a changed request.
    """
    root=Path(allowed_root).resolve();output=Path(output).absolute()
    if not root.is_dir():raise ValueError('REPLAY_ARTIFACT_ROOT_REQUIRED')
    _guard_path(output,root)
    if output.exists():raise ValueError('REPLAY_NEW_OWNED_OUTPUT_REQUIRED')
    output.mkdir(parents=True);work=output/'working';work.mkdir()
    shared=output/'model.db';journal=ModelJudgmentJournal(shared)
    summaries=[];snapshots=[];no_recall=NoModelRecallClient()
    for ordinal in (1,2):
        if ordinal==2:work.mkdir()
        journal_before=file_hash(shared) if ordinal==2 else None
        run_pass(work,journal if ordinal==1 else CapturesOnlyJournal(shared),None if ordinal==1 else no_recall)
        ledger,curve,counts=paper_bytes(work/'paper.db')
        if ordinal==2 and (no_recall.attempts or file_hash(shared)!=journal_before):
            raise ValueError('REPLAY_SECOND_PASS_MODEL_CAPTURE_CHANGED')
        _save(output/f'pass-{ordinal}-ledger.json',ledger)
        _save(output/f'pass-{ordinal}-curve.json',curve)
        summaries.append(dict(pass_number=ordinal,**counts,ledger_sha256=hashlib.sha256(ledger).hexdigest(),
            curve_sha256=hashlib.sha256(curve).hexdigest(),paper_identity=str(work/'paper.db')))
        snapshots.append((ledger,curve))
        # Exact new owned directory, checked again before a recoverable move.
        destination=output/f'pass-{ordinal}-state'
        _guard_path(work,output);_guard_path(destination,output)
        if destination.exists():raise ValueError('REPLAY_ARCHIVE_ALREADY_EXISTS')
        if any(p.is_symlink() or getattr(p,'is_junction',lambda:False)() for p in work.rglob('*')):
            raise ValueError('REPLAY_PATH_REDIRECTION_FORBIDDEN')
        if any(work.rglob('*-wal')) or any(work.rglob('*-shm')) or any(work.rglob('*-journal')):
            raise ValueError('REPLAY_DATABASE_NOT_CLOSED_FOR_ARCHIVE')
        work.rename(destination)
    equal=snapshots[0]==snapshots[1]
    result=dict(status='IDENTICAL_EXECUTION_BYTES' if equal else 'EXECUTION_BYTES_DIFFER',passes=summaries,
        ledger_bytes_equal=snapshots[0][0]==snapshots[1][0],curve_bytes_equal=snapshots[0][1]==snapshots[1][1],
        second_pass_model_calls=0,original_model_journal_unchanged=True,
        has_funded_fill_path=bool(summaries[0]['buy_fills']),
        authority='REPEATABILITY_COMPONENT_ONLY_NOT_SIX_DAY_SOURCE_OR_LEGAL_RATE_OR_RETURNS',
        control_release_gate='NOT_EVALUATED',production_writes=0)
    _save(output/'REPEATABILITY.json',(json.dumps(result,ensure_ascii=False,indent=2)+'\n').encode())
    return result
