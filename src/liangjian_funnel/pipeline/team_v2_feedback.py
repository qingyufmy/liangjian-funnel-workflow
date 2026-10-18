"""Research-only immutable close-return labels and as-of method aggregates.

No fills/trade performance, customer I/O, provider, RuntimeStore or mutable
legacy t1/t3 columns. Production outcomes must explicitly supply frozen price
evidence; old observations without availability/catalog proof remain unknown.
"""
from collections.abc import Mapping
from contextlib import closing
from datetime import date,datetime,time,timedelta
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import sqlite3
from zoneinfo import ZoneInfo

from ..runtime.calendar import ExchangeTradingCalendar
from ..runtime.team_v2_store import canonical

TZ=ZoneInfo('Asia/Shanghai')
BASIS='SOURCE_DAY_CLOSE_TO_CLOSE_RESEARCH_NOT_FILL'
MINIMUMS=dict(minimum_labeled_trade_dates=10,minimum_samples=15,minimum_distinct_symbols=5)


def _hash(value):return hashlib.sha256(canonical(value).encode()).hexdigest()


def _stamp(value):
    stamp=datetime.fromisoformat(value) if isinstance(value,str) else value
    if not isinstance(stamp,datetime) or stamp.utcoffset() is None:raise ValueError('METHOD_SOURCE_TIME_INVALID')
    return stamp.astimezone(TZ)


def _day(value):
    if not isinstance(value,str) or date.fromisoformat(value).isoformat()!=value:raise ValueError('METHOD_SOURCE_DATE_INVALID')
    return date.fromisoformat(value)


def _advance(calendar,day,n):
    for _ in range(n):day=calendar.next_trading_day(day)
    return day


def _reference_day(call):
    # This deliberately does not pretend a research close is the price paid
    # by a trader. Intraday execution-day judgments use that day's close.
    key='execution_trade_date' if call['role'] in {'A4','A6'} else 'source_trade_date'
    return _day(call[key])


def _original_judgment(row):
    """Stored classification must still be exactly a row of the raw response."""
    path=re.fullmatch(r'([A-Za-z_]+)\[(\d+)\]',row['judgment_path'])
    if not path:raise ValueError('METHOD_JUDGMENT_PATH_UNSUPPORTED')
    judgment=json.loads(row['judgment_json']);output=json.loads(row['output_json'])
    original=output[path[1]][int(path[2])]
    if {k:v for k,v in judgment.items() if k!='path'}!=original:
        raise ValueError('METHOD_JUDGMENT_ORIGINAL_MISMATCH')
    return judgment,path[1]


def _price(evidence,symbol,day,cutoff):
    from ..evaluation.outcome_labels import _observation
    if (not isinstance(evidence,Mapping) or set(evidence)!={'row','source_ref','observed_at','row_sha256'}
            or not isinstance(evidence['row'],Mapping) or _hash(evidence['row'])!=evidence['row_sha256']
            or not isinstance(evidence['source_ref'],str) or not evidence['source_ref']):
        raise ValueError('METHOD_PRICE_EVIDENCE_INVALID')
    observed=_stamp(evidence['observed_at']);closed=datetime.combine(day,time(15),TZ)
    if not closed<=observed<=cutoff:raise ValueError('METHOD_PRICE_NOT_AVAILABLE_ASOF')
    row=_observation(evidence['row'])
    if row.symbol!=symbol or row.trade_date!=day or not row.tradable:
        raise ValueError('METHOD_PRICE_DOMAIN_INVALID')
    return Decimal(str(row.close))*Decimal(str(row.adjust_factor))


def append_judgment_outcome(journal,*,decision_id,horizon,reference,target,captured_at,calendar=None):
    """Append evidence to the owned research journal, never edit a decision.

    A caller must use the actual observation clock, not backdate the label to
    its price day. Unknown/invalid/non-symbol model judgments cannot be labeled.
    """
    calendar=calendar or ExchangeTradingCalendar();captured=_stamp(captured_at)
    if type(horizon) is not int or horizon not in (1,3):raise ValueError('METHOD_HORIZON_INVALID')
    with closing(journal._connect()) as db,db:
        db.row_factory=sqlite3.Row;db.execute('BEGIN IMMEDIATE')
        row=db.execute('SELECT j.*,c.role,c.source_trade_date,c.execution_trade_date,c.captured_at call_captured_at,'
            'c.output_json,c.output_hash,c.validated call_validated FROM model_judgments j '
            'JOIN model_calls c USING(call_id) WHERE decision_id=?',(decision_id,)).fetchone()
        if (row is None or row['validated']!=1 or row['call_validated']!=1 or not row['symbol']
                or hashlib.sha256(row['judgment_json'].encode()).hexdigest()!=row['judgment_sha256']
                or hashlib.sha256(row['output_json'].encode()).hexdigest()!=row['output_hash']):
            raise ValueError('METHOD_JUDGMENT_NOT_PROVEN')
        call_time=_stamp(row['call_captured_at']);base=_reference_day(row)
        _original_judgment(row)
        if calendar.is_trading_day(base) is not True or captured<call_time:raise ValueError('METHOD_SOURCE_TIME_INVALID')
        due=_advance(calendar,base,horizon)
        first=_price(reference,row['symbol'],base,captured);last=_price(target,row['symbol'],due,captured)
        body=dict(schema_version='model-judgment-outcome/1',decision_id=decision_id,call_id=row['call_id'],
            judgment_sha256=row['judgment_sha256'],model_output_sha256=row['output_hash'],horizon=horizon,
            reference_trade_date=base.isoformat(),target_trade_date=due.isoformat(),basis=BASIS,
            reference=reference,target=target,forward_return=str(last/first-1),available_at=captured.isoformat())
        old=db.execute('SELECT body_json,body_sha256 FROM model_judgment_outcomes WHERE decision_id=? AND horizon=?',
            (decision_id,horizon)).fetchone()
        if old:
            # A retry's observation time can be later, not a reason to relabel.
            original=json.loads(old['body_json'])
            if (hashlib.sha256(old['body_json'].encode()).hexdigest()!=old['body_sha256']
                    or {k:v for k,v in original.items() if k!='available_at'}!={k:v for k,v in body.items() if k!='available_at'}):
                raise ValueError('METHOD_OUTCOME_BINDING_CONFLICT')
            return dict(status='REUSED',body_sha256=old['body_sha256'])
        sibling=db.execute('SELECT body_json,body_sha256 FROM model_judgment_outcomes WHERE decision_id=? LIMIT 1',
            (decision_id,)).fetchone()
        if sibling and (hashlib.sha256(sibling['body_json'].encode()).hexdigest()!=sibling['body_sha256']
                or json.loads(sibling['body_json'])['reference']!=reference):
            raise ValueError('METHOD_REFERENCE_REVISION_CONFLICT')
        text=canonical(body);sha=hashlib.sha256(text.encode()).hexdigest()
        db.execute('INSERT INTO model_judgment_outcomes VALUES (?,?,?,?,?)',
            (decision_id,horizon,captured.isoformat(),text,sha))
        return dict(status='RECORDED',body_sha256=sha)


def read_method_feedback(journal_path,*,recipient,as_of,calendar=None):
    """mode=ro only; no schema initialization/backfill or per-stock prose.

    Each method bucket needs ten mature trading dates, fifteen judgments and
    five distinct stocks. Only rates/counts/dates/proof hashes leave this reader.
    A method is a role/session/decision-family and its actual evidence-category
    set, not an untrusted model-written free-text strategy or a stock identity.
    """
    if recipient not in {'A1','A2','A3','A4','A6'}:raise ValueError('METHOD_RECIPIENT_INVALID')
    cutoff=_stamp(as_of);calendar=calendar or ExchangeTradingCalendar()
    def insufficient(code,**metadata):return dict(status='INSUFFICIENT',reason_code=code,
        recipient=recipient,as_of=cutoff.isoformat(),**MINIMUMS,methods=[],**metadata)
    path=Path(journal_path)
    if not path.is_file():return insufficient('METHOD_JOURNAL_NOT_AVAILABLE')
    try:
        with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=2)) as db:
            db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
            tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if not {'model_research_meta','model_calls','model_judgments','model_call_evidence','model_judgment_outcomes'}<=tables:
                return insufficient('METHOD_LABEL_AVAILABILITY_UNPROVEN')
            if db.execute('SELECT version FROM model_research_meta').fetchall()[0][0]!='model-research/1':
                return insufficient('METHOD_JOURNAL_SCOPE_INVALID')
            if db.execute('SELECT 1 FROM model_judgment_outcomes LIMIT 1').fetchone() is None:
                return insufficient('METHOD_TEN_MATURE_TRADING_DATES_NOT_MET')
            end=cutoff.date()
            if calendar.is_trading_day(end) is not True or cutoff.time().replace(tzinfo=None)<time(15):
                end=calendar.previous_trading_day(end)
            start=end
            for _ in range(19):start=calendar.previous_trading_day(start)
            rows=db.execute('SELECT j.*,c.role,c.session,c.source_trade_date,c.execution_trade_date,'
                'c.captured_at call_captured_at,c.output_json,c.output_hash,c.catalog_hash,c.validated call_validated,'
                'e.catalog_json,e.catalog_sha256,o.horizon,o.captured_at label_captured_at,o.body_json,o.body_sha256 '
                'FROM model_judgments j JOIN model_calls c USING(call_id) '
                'JOIN model_call_evidence e USING(call_id) JOIN model_judgment_outcomes o USING(decision_id) '
                'WHERE c.role=? AND c.execution_trade_date>=? AND c.source_trade_date<=? '
                'ORDER BY j.decision_id,o.horizon LIMIT 20001',(recipient,start.isoformat(),end.isoformat())).fetchall()
        if len(rows)>20000:return insufficient('METHOD_WINDOW_SOURCE_LIMIT_EXCEEDED')
        decisions={};excluded=0
        for row in rows:
            try:
                if (row['validated']!=1 or row['call_validated']!=1 or _stamp(row['call_captured_at'])>cutoff
                        or _stamp(row['label_captured_at'])>cutoff):continue
                for text,sha in [('judgment_json','judgment_sha256'),('output_json','output_hash'),
                        ('catalog_json','catalog_sha256'),('body_json','body_sha256')]:
                    if hashlib.sha256(row[text].encode()).hexdigest()!=row[sha]:raise ValueError('HASH')
                catalog=json.loads(row['catalog_json']);judgment,family=_original_judgment(row);body=json.loads(row['body_json'])
                if row['catalog_sha256']!=row['catalog_hash']:raise ValueError('CATALOG')
                base=_reference_day(row);due=_advance(calendar,base,row['horizon'])
                if not start<=base<=end:continue
                if (body['schema_version']!='model-judgment-outcome/1' or body['decision_id']!=row['decision_id'] or body['call_id']!=row['call_id']
                        or body['judgment_sha256']!=row['judgment_sha256'] or body['model_output_sha256']!=row['output_hash']
                        or body['horizon']!=row['horizon'] or body['reference_trade_date']!=base.isoformat()
                        or body['target_trade_date']!=due.isoformat() or body['basis']!=BASIS
                        or _stamp(body['available_at'])!=_stamp(row['label_captured_at'])):raise ValueError('BINDING')
                available=_stamp(row['label_captured_at'])
                first=_price(body['reference'],row['symbol'],base,available);last=_price(body['target'],row['symbol'],due,available)
                if Decimal(body['forward_return'])!=last/first-1:raise ValueError('RETURN')
                types={entry['id']:entry['category'] for entry in catalog['items']}
                refs=[*judgment.get('supporting_refs',judgment.get('supporting_source_refs',[])),
                    *judgment.get('opposing_refs',judgment.get('contradicting_source_refs',[]))]
                evidence=tuple(sorted({types[ref] for ref in refs if ref!='NO_OPPOSING_EVIDENCE_FOUND'}))
                if not evidence or any(re.fullmatch(r'[A-Z][A-Z0-9_]{0,31}',c) is None for c in evidence):
                    raise ValueError('METHOD')
                kind=next((judgment[k] for k in ('action','review_status','adjustment','decision','stance','status')
                    if isinstance(judgment.get(k),str)),None)
                if kind is not None and kind not in {'NO_ACTION','START_CONFIRMATION','BUY_SIGNAL','ADD_SIGNAL',
                        'SELL_SIGNAL','REDUCE_SIGNAL','CANCEL_SIGNAL','BLOCK','PASS','VETO','DATA_GAP','NO_FOCUS',
                        'KEEP','TIGHTEN_TO_PROBE','REDUCE_BATCHES','SUSPEND','REDUCE_HALF','EXIT','HALT_NEW_ENTRY',
                        'RESUME','HOLD','REDUCE_TOMORROW','EXIT_TOMORROW','ACTIVE','MONITOR','REJECT'}:raise ValueError('KIND')
                key=(row['session'],family+(':'+kind if kind is not None else ''),evidence)
                entry=decisions.setdefault(row['decision_id'],dict(day=base,key=key,symbol=row['symbol'],returns={},hashes=[]))
                if entry['key']!=key or entry['day']!=base or entry['symbol']!=row['symbol']:raise ValueError('METHOD')
                entry['returns'][row['horizon']]=Decimal(body['forward_return']);entry['hashes'].append(row['body_sha256'])
            except (ValueError,TypeError,KeyError,IndexError,ArithmeticError):excluded+=1
        groups={}
        for entry in decisions.values():
            if set(entry['returns'])=={1,3}:groups.setdefault(entry['key'],[]).append(entry)
        methods=[]
        for (session,family,evidence),items in sorted(groups.items()):
            days=sorted({i['day'] for i in items})
            if len(days)<10 or len(items)<15 or len({i['symbol'] for i in items})<5:continue
            bucket=dict(recipient=recipient,session=session,judgment_type=family,evidence_types=list(evidence))
            methods.append(dict(method_id=_hash(bucket),**bucket,sample_count=len(items),labeled_trade_dates=len(days),
                date_from=days[0].isoformat(),date_through=days[-1].isoformat(),
                positive_fraction_1d=str(Decimal(sum(i['returns'][1]>0 for i in items))/len(items)),
                positive_fraction_3d=str(Decimal(sum(i['returns'][3]>0 for i in items))/len(items)),
                outcome_evidence_sha256=_hash(sorted(h for i in items for h in i['hashes']))))
        if not methods:return insufficient('METHOD_BUCKET_MATURITY_NOT_MET',excluded_records=excluded)
        return dict(status='READY',recipient=recipient,as_of=cutoff.isoformat(),**MINIMUMS,
            window_trading_dates=20,methodology='MODEL_JUDGMENTS_NOT_EFFECTIVE_TRADES',basis=BASIS,
            methods=methods,excluded_records=excluded)
    except (sqlite3.Error,ValueError,IndexError):return insufficient('METHOD_SOURCE_READ_OR_CONTRACT_FAILED')


def read_a5_method_feedback(journal_path,team_path,*,recipient,as_of):
    """Inject only mature aggregate IDs actually selected by a validated A5.

    A5 free-form prose is retained in its original report, not copied into
    another model's prompt. A missing/new-format source is explicit, not READY.
    """
    cutoff=_stamp(as_of)
    if recipient not in {'A1','A2','A3','A4','A6'}:raise ValueError('METHOD_RECIPIENT_INVALID')
    def missing(code):return dict(status='INSUFFICIENT',reason_code=code,recipient=recipient,
        as_of=cutoff.isoformat(),**MINIMUMS,methods=[])
    if not Path(journal_path).is_file() or not Path(team_path).is_file():return missing('A5_METHOD_SOURCE_NOT_AVAILABLE')
    try:
        with closing(sqlite3.connect(Path(team_path).resolve().as_uri()+'?mode=ro',uri=True,timeout=2)) as db:
            db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
            tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if not {'team_a5_reviews','team_v2_meta','control_minute_context'}<=tables:return missing('A5_METHOD_SOURCE_NOT_AVAILABLE')
            if db.execute('SELECT version FROM team_v2_meta').fetchall()[0][0]!='team-runtime/1':return missing('A5_METHOD_SOURCE_SCOPE_INVALID')
            reviews=db.execute("SELECT r.*,c.source_sha256,c.body_json FROM team_a5_reviews r "
                "JOIN control_minute_context c ON c.decision_id='a5-review:'||r.review_id "
                "WHERE r.trade_date<=? AND r.review_kind='POST_CLOSE' ORDER BY r.trade_date DESC,r.review_id LIMIT 20",
                (cutoff.date().isoformat(),)).fetchall()
        with closing(sqlite3.connect(Path(journal_path).resolve().as_uri()+'?mode=ro',uri=True,timeout=2)) as db:
            db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
            if db.execute('SELECT version FROM model_research_meta').fetchall()[0][0]!='model-research/1':return missing('METHOD_JOURNAL_SCOPE_INVALID')
            for review in reviews:
                context=json.loads(review['body_json']);result=json.loads(review['result_json'])
                if (_hash(context)!=review['source_sha256'] or review['source_sha256']!=review['input_sha256']
                        or context['call_id']!=review['source_call_id'] or result.get('source_call_id')!=review['source_call_id']
                        or result.get('status')!='READY' or not isinstance(result.get('report'),dict)):continue
                call=db.execute("SELECT * FROM model_calls WHERE call_id=? AND role='A5' AND session='POST_CLOSE' AND validated=1",
                    (review['source_call_id'],)).fetchone()
                if call is None or _stamp(call['captured_at'])>cutoff:continue
                report=result['report'];facts=context['facts']
                if (hashlib.sha256(call['output_json'].encode()).hexdigest()!=call['output_hash']
                        or json.loads(call['output_json'])!=report or facts['input_hash']!=result['fact_sha256']
                        or _hash({k:v for k,v in facts.items() if k!='input_hash'})!=facts['input_hash']
                        or facts['trade_date']!=review['trade_date'] or _stamp(facts['observed_at'])>_stamp(call['captured_at'])):continue
                metrics=facts.get('method_calibration',{}).get(recipient,{})
                if (metrics.get('status')!='READY' or metrics.get('recipient')!=recipient
                        or _stamp(metrics['as_of'])!=_stamp(facts['observed_at'])):continue
                # A self-consistent READY blob is not proof. Rebuild the
                # exact historical aggregate from immutable label receipts.
                if read_method_feedback(journal_path,recipient=recipient,as_of=facts['observed_at'])!=metrics:continue
                catalog=facts['citation_catalog']
                if _hash(catalog)!=call['catalog_hash']:continue
                proof=next((r for r in catalog['items'] if r['id']=='A5:METHOD:'+recipient),None)
                if proof is None or proof.get('sha256')!=_hash(metrics):continue
                by_id={r['method_id']:r for r in metrics['methods'] if r.get('labeled_trade_dates',0)>=10}
                refs=report.get('method_feedback',{}).get(recipient,[])
                selected={}
                for row in refs:
                    if (not isinstance(row,dict) or row.get('stance') not in {'TIGHTEN','KEEP'}
                            or not isinstance(row.get('method_id'),str)
                            or row['method_id'] not in by_id
                            or 'A5:METHOD:'+recipient not in row.get('evidence_ids',[])):continue
                    selected.setdefault(row['method_id'],set()).add(row['stance'])
                # Conflicting duplicates are not resolved by array order.
                methods=[dict(by_id[i],stance=next(iter(stances))) for i,stances in sorted(selected.items()) if len(stances)==1]
                if methods:return dict(status='READY',recipient=recipient,as_of=cutoff.isoformat(),
                    source_as_of=facts['observed_at'],**MINIMUMS,basis=BASIS,
                    methods=methods,a5_review_id=review['review_id'],a5_model_output_sha256=call['output_hash'],
                    feedback_scope='A5_SELECTED_DETERMINISTIC_METHOD_AGGREGATES_NO_STOCK_PROSE')
        return missing('A5_MATURE_METHOD_SELECTION_NOT_AVAILABLE')
    except (sqlite3.Error,ValueError,KeyError,TypeError,IndexError):return missing('A5_METHOD_SOURCE_READ_OR_CONTRACT_FAILED')
