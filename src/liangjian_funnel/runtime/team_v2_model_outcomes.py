"""Natural outcomes tail: read existing daily cache, append Team labels only.

No provider/model/research/plan path. A Team failure is a separate data status,
not permission to change the legacy outcome job result or retry a closed label.
"""
from contextlib import closing
from datetime import datetime,time,timedelta,timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import time as timer

from ..pipeline.local_fact_cache import canonical_json_hash
from ..pipeline.team_v2_feedback import append_judgment_outcome,_reference_day,_advance,_stamp,_hash
from ..pipeline.team_v2_judgments import ModelJudgmentJournal
from .calendar import ExchangeTradingCalendar
from .team_v2_morning import _directory,TZ


def refresh_model_outcomes(settings,*,clock=None,calendar=None):
    now=_stamp((clock or (lambda:datetime.now(TZ)))());calendar=calendar or ExchangeTradingCalendar()
    result=dict(status='NOOP',reason_code='TEAM_MODEL_JOURNAL_NOT_INITIALIZED',written=0,
        missing_price=0,invalid_evidence=0,scope='MODEL_JUDGMENTS_NOT_TRADES',network_used=False,models_called=False,
        production_business_database_written=False)
    directory=_directory(settings);path=directory/'model.db';source=Path(settings.fact_cache_db_path)
    if not path.is_file():return result
    if calendar.is_trading_day(now.date()) is not True or now.time().replace(tzinfo=None)<time(15,30):
        return dict(result,reason_code='MODEL_LABEL_WINDOW_CLOSED')
    if source.is_symlink() or source.resolve()==path.resolve() or not source.is_file():
        return dict(result,status='DATA_LIMITED',reason_code='MODEL_LABEL_PRICE_SOURCE_UNAVAILABLE')
    start=now.date()
    for _ in range(19):start=calendar.previous_trading_day(start)
    try:
        with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=1)) as db:
            db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
            if db.execute('SELECT version FROM model_research_meta').fetchall()[0][0]!='model-research/1':
                return dict(result,status='DATA_LIMITED',reason_code='MODEL_LABEL_JOURNAL_SCOPE_INVALID')
            rows=db.execute('SELECT j.decision_id,j.symbol,j.judgment_path,c.role,c.source_trade_date,c.execution_trade_date '
                'FROM model_judgments j JOIN model_calls c USING(call_id) '
                'WHERE j.validated=1 AND c.validated=1 AND j.symbol IS NOT NULL AND c.role IN (\'A1\',\'A2\',\'A3\',\'A4\',\'A6\') '
                'AND c.execution_trade_date>=? AND c.source_trade_date<=? ORDER BY j.decision_id LIMIT 5001',
                (start.isoformat(),now.date().isoformat())).fetchall()
            if len(rows)>5000:return dict(result,status='DATA_LIMITED',reason_code='MODEL_LABEL_SCOPE_LIMIT_EXCEEDED')
            exists=db.execute("SELECT 1 FROM sqlite_master WHERE name='model_judgment_outcomes'").fetchone()
            prior={}
            if exists:
                old_rows=db.execute('SELECT o.* FROM model_judgment_outcomes o JOIN model_judgments j USING(decision_id) '
                    'JOIN model_calls c USING(call_id) WHERE c.execution_trade_date>=? AND c.source_trade_date<=? '
                    'LIMIT 10001',(start.isoformat(),now.date().isoformat())).fetchall()
                if len(old_rows)>10000:return dict(result,status='DATA_LIMITED',reason_code='MODEL_LABEL_SCOPE_LIMIT_EXCEEDED')
                for row in old_rows:
                    if hashlib.sha256(row['body_json'].encode()).hexdigest()!=row['body_sha256']:
                        return dict(result,status='DATA_LIMITED',reason_code='MODEL_LABEL_PRIOR_HASH_MISMATCH')
                    prior.setdefault(row['decision_id'],{})[row['horizon']]=json.loads(row['body_json'])
        if not rows:return dict(result,reason_code='MODEL_LABEL_NO_SYMBOL_JUDGMENTS')
        # Constructor initializes only the owned journal, after scope proof.
        journal=ModelJudgmentJournal(path);started=timer.monotonic();cache={}
        with closing(sqlite3.connect(source.resolve().as_uri()+'?mode=ro',uri=True,timeout=1)) as db:
            db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
            def price(symbol,day):
                key=(symbol,day)
                if key in cache:return cache[key]
                lo=datetime.combine(day,time(),TZ).astimezone(timezone.utc).isoformat()
                hi=datetime.combine(day+timedelta(days=1),time(),TZ).astimezone(timezone.utc).isoformat()
                values=db.execute("SELECT * FROM daily_bars WHERE symbol=? AND adjust='none' AND bar_timestamp>=? "
                    'AND bar_timestamp<? ORDER BY fetched_at DESC,content_hash DESC LIMIT 33',(symbol,lo,hi)).fetchall()
                if len(values)>32:cache[key]=None;return None
                known=[]
                for row in values:
                    fetched=_stamp(row['fetched_at'])
                    if fetched>now:continue
                    if _stamp(row['bar_timestamp']).date()!=day:continue
                    payload=json.loads(row['payload_json'])
                    if canonical_json_hash(payload)!=row['content_hash']:raise ValueError('MODEL_LABEL_CACHE_HASH_MISMATCH')
                    known.append((fetched,row,payload))
                if not known:cache[key]=None;return None
                latest=max(k[0] for k in known);selected=[k for k in known if k[0]==latest]
                if len({k[1]['content_hash'] for k in selected})!=1:raise ValueError('MODEL_LABEL_CACHE_REVISION_AMBIGUOUS')
                fetched,row,payload=selected[0]
                original={**payload,'symbol':symbol,'bar_timestamp':row['bar_timestamp'],'adjust':row['adjust'],
                    'cache_original_payload_sha256':row['content_hash']}
                evidence=dict(row=original,row_sha256=_hash(original),
                    source_ref='daily-cache:'+row['content_hash'],observed_at=fetched.isoformat())
                cache[key]=evidence;return evidence
            for row in rows:
                if timer.monotonic()-started>10:return dict(result,status='DATA_LIMITED',reason_code='MODEL_LABEL_LOCAL_BUDGET_EXCEEDED')
                base=_reference_day(row)
                if not start<=base<=now.date():continue
                previous=prior.get(row['decision_id'],{})
                reference=next(iter(previous.values()))['reference'] if previous else None
                for horizon in (1,3):
                    if horizon in previous:continue
                    due=_advance(calendar,base,horizon)
                    if due>now.date():continue
                    try:
                        reference=reference or price(row['symbol'],base);target=price(row['symbol'],due)
                        if reference is None or target is None:result['missing_price']+=1;continue
                        appended=append_judgment_outcome(journal,decision_id=row['decision_id'],horizon=horizon,
                            reference=reference,target=target,captured_at=now,calendar=calendar)
                        result['written']+=appended['status']=='RECORDED'
                    except (ValueError,TypeError,KeyError):result['invalid_evidence']+=1
        limited=bool(result['missing_price'] or result['invalid_evidence'])
        return dict(result,status='DATA_LIMITED' if limited else 'COMPLETED',reason_code='MODEL_LABEL_PARTIAL' if limited else 'OK')
    except (sqlite3.Error,ValueError,KeyError,IndexError):
        return dict(result,status='DATA_LIMITED',reason_code='MODEL_LABEL_SOURCE_UNAVAILABLE_OR_INVALID')
