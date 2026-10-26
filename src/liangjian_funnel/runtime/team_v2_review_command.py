"""Independent A5 coach. Read source facts, never the customer report/prose.

The legacy customer model/publisher are untouched. Full sources are frozen in
owned Team state; a declared deterministic projection supplies the new coach.
Only prerequisite waits are retryable, before a model request is claimed.
"""
from collections import Counter
from contextlib import closing
from datetime import datetime, time
import hashlib
import json
import os
import re
import sqlite3

from ..pipeline.model_client import OpenAICompatibleModelClient
from ..pipeline.team_v2_judgments import ModelJudgmentJournal
from ..pipeline.team_v2_prompts import TeamPromptRepository
from .calendar import ExchangeTradingCalendar
from .team_v2_morning import ACCOUNT, TZ, _directory
from .team_v2_review import run_team_review
from .team_v2_review_facts import read_paper_review_evidence, build_team_review_facts
from .team_v2_sources import stamp
from .team_v2_store import TeamMeetingStore, canonical


def _hash(value):return hashlib.sha256(canonical(value).encode()).hexdigest()


def _frozen_legacy(settings, *, day, kind, now):
    """Select only input metadata/facts, never report_json or model responses."""
    database=settings.state_db_path
    identity=None
    if database.is_file():
        if database.is_symlink():raise ValueError('A5_LEGACY_SCOPE_INVALID')
        with closing(sqlite3.connect(database.resolve().as_uri()+'?mode=ro',uri=True,timeout=2)) as db:
            db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
            exists=db.execute("SELECT 1 FROM sqlite_master WHERE name='a5_daily_reviews' AND type='table'").fetchone()
            if exists:
                identity=db.execute('SELECT input_hash,length(CAST(fact_snapshot_json AS BLOB)),created_at '
                    'FROM a5_daily_reviews WHERE trade_date=? AND review_kind=? ORDER BY created_at DESC LIMIT 1',
                    (day,kind)).fetchone()
                if identity:
                    if identity[1]>64*1024*1024:raise ValueError('A5_LEGACY_FACT_TOO_LARGE')
                    if stamp(identity[2])>now:raise ValueError('A5_LEGACY_FUTURE_FACT')
                    text=db.execute('SELECT fact_snapshot_json FROM a5_daily_reviews WHERE trade_date=? '
                        'AND review_kind=? AND input_hash=? ORDER BY created_at DESC LIMIT 1',
                        (day,kind,identity[0])).fetchone()[0]
            db.rollback()
    if identity:
        facts=json.loads(text)
    else:
        # A failed customer model can still have a valid pre-call fact archive.
        # Multiple different sources are not resolved by filename or mtime.
        directory=settings.workflow_output_dir/'a5'/day
        if not directory.is_dir():return None
        if directory.is_symlink():raise ValueError('A5_LEGACY_SCOPE_INVALID')
        files=list(directory.glob(kind.lower().replace('_','-')+'-*-facts.json'))
        if len(files)>5:raise ValueError('A5_LEGACY_SOURCE_AMBIGUOUS')
        candidates={}
        for path in files:
            if path.is_symlink():raise ValueError('A5_LEGACY_SCOPE_INVALID')
            with path.open('rb') as stream:raw=stream.read(64*1024*1024+1)
            if len(raw)>64*1024*1024:raise ValueError('A5_LEGACY_FACT_TOO_LARGE')
            body=json.loads(raw);candidates[body.get('input_hash')]=body
        if not candidates:return None
        if len(candidates)!=1:raise ValueError('A5_LEGACY_SOURCE_AMBIGUOUS')
        facts=next(iter(candidates.values()))
    if (facts.get('input_hash')!=_hash({k:v for k,v in facts.items() if k!='input_hash'})
            or facts.get('trade_date')!=day or facts.get('review_kind')!=kind
            or (identity is not None and identity[0]!=facts['input_hash'])
            or stamp(facts['cutoff_at'])!=now.replace(hour=11 if kind=='MIDDAY' else 15,
                minute=30 if kind=='MIDDAY' else 0,second=0,microsecond=0)
            or stamp(facts.get('operational_cutoff_at',facts['cutoff_at']))>now):
        raise ValueError('A5_LEGACY_FACT_BINDING_INVALID')
    return facts


def review_events(facts):
    """Use existing journal decision IDs; a paper signal ID is not relabelled.

    A trader batch contains several judgments. Each relevant admission must
    join exactly one original captured judgment by source call, plan/action.
    Legacy events use their already-frozen evidence_id, explicitly identified.
    """
    domain={};events=[]
    def add(key,meta,body,basis):
        if not isinstance(key,str) or not key or key in domain:raise ValueError('A5_EVENT_IDENTITY_CONFLICT')
        domain[key]=meta;events.append(dict(decision_id=key,**meta,evidence=body,identity_basis=basis))
    legacy=facts['legacy_facts']
    for row in legacy.get('a4',{}).get('events',[]):
        if row.get('action')!='BUY_SIGNAL' and row.get('reason_code')!='TRADER_SIGNAL_BLOCKED_BY_GUARDRAIL':continue
        meta={k:str(row.get(k) or 'UNAVAILABLE') for k in ('symbol','strategy_profile','lifecycle_status')}
        if row.get('plan_id'):meta['plan_id']=row['plan_id']
        add(row.get('evidence_id'),meta,row,'ORIGINAL_LEGACY_EVENT_EVIDENCE_ID')
    calls=facts.get('model_judgments',{}).get('rows',{});judgments=calls.get('judgments',[])
    if not isinstance(judgments,list):judgments=[]
    sessions=facts.get('a6',{}).get('rows',{}).get('trader_sessions',[])
    if not isinstance(sessions,list):sessions=[]
    profiles={r.get('plan_id'):r.get('strategy_profile') for r in legacy.get('a3',{}).get('plans',[])}
    for session in sessions:
        result=json.loads(session['result_json'])
        for admission in result.get('admissions',[]):
            binding=admission['binding'];judgment=admission['judgment']
            action=binding.get('action')
            if action!='BUY_SIGNAL' and judgment.get('status')!='REJECTED':continue
            matching=[]
            for row in judgments:
                if row['call_id']!=admission.get('source_call_id',session['source_call_id']) or not row['validated']:continue
                body=json.loads(row['judgment_json'])
                if (body.get('action')==action and (body.get('plan_id')==binding['plan_id'] or
                        ('plan_id' not in body and body.get('symbol')==binding['symbol']))):matching.append(row)
            if len(matching)!=1:raise ValueError('A5_DECISION_JUDGMENT_BINDING_UNPROVEN')
            original=matching[0]
            meta=dict(symbol=binding['symbol'],plan_id=binding['plan_id'],
                strategy_profile=binding.get('strategy_profile') or profiles.get(binding['plan_id']) or 'UNAVAILABLE',
                lifecycle_status='REJECTED' if judgment.get('status')=='REJECTED' else 'UNAVAILABLE')
            add(original['decision_id'],meta,dict(admission=admission,model_judgment=json.loads(original['judgment_json'])),
                'ORIGINAL_MODEL_JUDGMENTS_DECISION_ID')
    return domain,events


def counterexample_stage_projection(rows):
    """0283: retain detailed original code beside the six-value role stage."""
    result=[]
    for source in rows:
        original=source.get('drop_stage','UNRESOLVED');layer=str(original).split('_',1)[0]
        result.append({**source,'original_drop_stage':original,
            'funnel_drop_stage':layer if layer in {'A1','A2','A3','A4','A6'} else 'UNRESOLVED'})
    return result


def counterexample_citations(rows):
    """Expose only original named facts, not invented model citation aliases."""
    result=[];seen=set()
    for i,row in enumerate(rows):
        sources=[(row,['counterexamples',i],'COUNTEREXAMPLE')]
        if isinstance(row.get('a3_candidate'),dict):
            sources.append((row['a3_candidate'],['counterexamples',i,'a3_candidate'],'A3_CANDIDATE'))
        for fact,path,category in sources:
            identity=fact.get('evidence_id')
            if identity is None:continue
            if not isinstance(identity,str) or not identity.strip() or identity in seen:
                raise ValueError('A5_COUNTEREXAMPLE_CITATION_CONFLICT')
            seen.add(identity)
            result.append(dict(id=identity,category=category,input_ref=path,sha256=_hash(fact)))
    return result


def coach_projection(full):
    """Declared aggregates, not silent truncation. Every signal stays explicit.

    Raw requests/responses stay in their original journals plus full frozen
    source. Aggregated waiting judgments are not counted as effective signals.
    """
    from ..review.daily import _model_fact_projection
    legacy=full['legacy_facts'];clean={k:v for k,v in legacy.items() if k!='review_history'}
    legacy_view=_model_fact_projection(clean);domain,events=review_events(full)
    projected={k:v for k,v in full.items() if k not in {'input_hash','legacy_facts','a2','a3','a4','a6','model_judgments','paper_broker'}}
    projected.update(a2=legacy_view.get('a2',{}),a3=legacy_view.get('a3',{}),
        a4=dict(legacy=legacy_view.get('a4',{})),signals_for_review=events,
        independent_verification=legacy_view.get('independent_verification',{'status':'UNAVAILABLE'}),
        counterexamples=legacy_view.get('independent_verification',{}).get('counterexamples',full.get('counterexamples',[])),
        legacy_operational_evidence={k:legacy_view[k] for k in
            ('operational_evidence','business_audit','execution_observability','data_quality') if k in legacy_view},
        full_team_fact_sha256=full['input_hash'],projection_schema='team-a5-coach-projection/1')
    projected['counterexamples']=counterexample_stage_projection(projected['counterexamples'])
    if 'counterexamples' in projected['independent_verification']:
        projected['independent_verification']={
            **{k:v for k,v in projected['independent_verification'].items() if k!='counterexamples'},
            'counterexamples_reference':dict(path='$.counterexamples',item_count=len(projected['counterexamples']),
                sha256=_hash(projected['counterexamples']))}
    tables=full.get('a6',{}).get('rows',{});team={}
    for table,rows in tables.items():
        if not isinstance(rows,list):team[table]=rows;continue
        if table=='trader_sessions':
            team[table]=[]
            for row in rows:
                result=json.loads(row['result_json']);admissions=result.get('admissions',[])
                team[table].append(dict(decision_id=row['decision_id'],source_call_id=row['source_call_id'],
                    status=result['status'],reason_codes=result.get('reason_codes',[]),
                    admission_status_counts=dict(Counter(a['judgment']['status'] for a in admissions)),
                    result_sha256=_hash(result)))
        elif table=='morning_engine':
            team[table]=[dict(account_id=r['account_id'],trade_date=r['trade_date'],
                frozen_result_sha256=_hash(json.loads(r['body_json']))) for r in rows]
        else:
            team[table]=[{k:(json.loads(v) if k=='result_json' else v) for k,v in r.items()
                if k not in {'input_json','body_json'}} for r in rows]
    projected['activation_decisions']=full.get('activation_decisions',{'status':'UNAVAILABLE','rows':[]})
    projected['a6']=dict(status=full['a6']['status'],rows=team)
    fallback=team.get('record_fallback_state',[])
    projected['record_fallback']=dict(status=('RECORD_FALLBACK_ACTIVE' if fallback else 'INACTIVE')
        if isinstance(fallback,list) else 'UNAVAILABLE',
        evidence=fallback)
    model=full.get('model_judgments',{});model_rows=model.get('rows',{});counts=Counter();sentinels=Counter();consequential=[]
    for row in model_rows.get('judgments',[]):
        body=json.loads(row['judgment_json']);classification=body.get('action') or body.get('verdict') or body.get('decision') or 'UNCLASSIFIED'
        counts[str(classification)]+=1
        if body.get('opposing_refs')==['NO_OPPOSING_EVIDENCE_FOUND']:sentinels[row['call_id']]+=1
        if body.get('action')=='BUY_SIGNAL':
            consequential.append(dict(decision_id=row['decision_id'],call_id=row['call_id'],judgment=body,
                validated=bool(row['validated']),t1_return=row['t1_return'],t3_return=row['t3_return']))
    projected['model_judgments']=dict(status=model['status'],
        calls=[{k:v for k,v in row.items() if k!='output_json'} for row in model_rows.get('calls',[])],
        judgment_classification_counts=dict(counts),opposing_sentinel_by_call=dict(sentinels),
        buy_judgments=consequential,full_source_sha256=model.get('content_sha256'))
    paper=full['paper_broker'];snapshot=paper.get('snapshot') or {}
    projected['paper_broker']=dict(snapshot={k:v for k,v in snapshot.items() if k not in {'rows','valuation_evidence'}},settlement=paper.get('settlement'),
        current_day_row_counts={k:len(v) for k,v in snapshot.get('rows',{}).items()})
    catalog=json.loads(canonical(full['citation_catalog']))
    catalog['categories'].append('SIGNAL')
    catalog['items'].append(dict(id='A5:ACTIVATION',category='SIGNAL',sha256=_hash(projected['activation_decisions'])))
    catalog['items'].extend(dict(id=e['decision_id'],category='SIGNAL',summary={k:v for k,v in e.items() if k!='evidence'},
        sha256=_hash(e)) for e in events)
    extra=counterexample_citations(projected['counterexamples'])
    if {row['id'] for row in extra}&{row['id'] for row in catalog['items']}:
        raise ValueError('A5_COUNTEREXAMPLE_CITATION_CONFLICT')
    catalog['categories']=list(dict.fromkeys([*catalog['categories'],*(row['category'] for row in extra)]))
    catalog['items'].extend(extra)
    projected['citation_catalog']=catalog
    from .team_v2_review_causal import plan_causal_projection
    causal=plan_causal_projection(full)
    projected['plan_causal_chains']=causal
    catalog['categories']=list(dict.fromkeys([*catalog['categories'],'CAUSAL']))
    catalog['items'].append(dict(id='A5:CAUSAL',category='CAUSAL',sha256=_hash(causal),input_ref=['plan_causal_chains']))
    projected['input_hash']=_hash(projected)
    return projected,domain


def export_hypotheses(settings, *, result):
    """Append-only research queue artifacts; never a production config input."""
    from .research_feature_policy import RESEARCH_FLAGS
    if not RESEARCH_FLAGS['a5_free_text_hypothesis_export']:
        return dict(status='JOURNAL_ADVISORY_ONLY_NOT_EXPORTED',count=0)
    if result.get('report') is None:return dict(status='NOT_APPROVED',count=0)
    directory=settings.root/'artifacts/replay-queue'/result['trade_date']
    if directory.resolve()!=settings.root.resolve()/'artifacts/replay-queue'/result['trade_date']:
        raise ValueError('A5_REPLAY_QUEUE_SCOPE_INVALID')
    directory.mkdir(parents=True,exist_ok=True)
    count=0
    for hypothesis in result['report']['hypotheses']:
        body=dict(schema_version='team-replay-hypothesis/1',status='PENDING_REPLAY',
            source_call_id=result['source_call_id'],fact_sha256=result['fact_sha256'],hypothesis=hypothesis,
            automatic_production_change=False)
        raw=canonical(body).encode();path=directory/(_hash([result['source_call_id'],hypothesis['hypothesis_id']])+'.json')
        if path.is_symlink():raise ValueError('A5_REPLAY_QUEUE_SCOPE_INVALID')
        try:fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        except FileExistsError:
            with path.open('rb') as source:prior=source.read(len(raw)+1)
            if prior!=raw:raise ValueError('A5_REPLAY_QUEUE_BINDING_CONFLICT')
        else:
            with os.fdopen(fd,'wb') as output:output.write(raw);output.flush();os.fsync(output.fileno())
        count+=1
    return dict(status='EXPORTED',count=count)


def run_review_stage(settings, review_kind, *, clock=None, calendar=None, client=None):
    clock=clock or (lambda:datetime.now(TZ));now=stamp(clock());day=now.date().isoformat()
    calendar=calendar or ExchangeTradingCalendar()
    if review_kind not in {'MIDDAY','POST_CLOSE'}:raise ValueError('A5_SESSION_INVALID')
    if calendar.is_trading_day(now.date()) is not True:return dict(status='NOOP',reason_code='NON_TRADING_DAY')
    if now.time().replace(tzinfo=None)<(time(11,35) if review_kind=='MIDDAY' else time(16)):
        return dict(status='NOOP',reason_code='TEAM_A5_WINDOW_NOT_OPEN')
    from .team_v2_review_policy import load_review_policy,review_wall_budget
    policy=load_review_policy(settings.root)
    wall_budget=review_wall_budget(policy,now=now,review_kind=review_kind)
    if wall_budget<=0:return dict(status='NOOP',reason_code='TEAM_A5_MIDDAY_WINDOW_CLOSED')
    directory=_directory(settings)
    if not all((directory/name).is_file() for name in ('paper.db','team.db')):
        return dict(status='NOOP',reason_code='TEAM_ACCOUNT_NOT_INITIALIZED')
    store=TeamMeetingStore(directory/'team.db');rid='team-a5:'+day+':'+review_kind
    with closing(store._connect()) as db:
        previous=db.execute('SELECT body_json FROM control_minute_context WHERE decision_id=?',('a5-command:'+rid,)).fetchone()
        eod=db.execute("SELECT 1 FROM eod_reviews WHERE json_extract(result_json,'$.account_id')=? "
            "AND json_extract(result_json,'$.trade_date')=? LIMIT 1",(ACCOUNT,day)).fetchone()
    if previous:
        frozen=json.loads(previous[0])
    else:
        if review_kind=='POST_CLOSE' and not eod and now.time().replace(tzinfo=None)<time(16,45):
            return dict(status='WAITING',reason_code='TEAM_A5_EOD_PREREQUISITE_PENDING')
        legacy=_frozen_legacy(settings,day=day,kind=review_kind,now=now)
        if legacy is None:return dict(status='WAITING',reason_code='TEAM_A5_FROZEN_FACTS_PENDING')
        # The customer fact archive contains past report history. It is not
        # input to the independent coach; retain its source hash, not prose.
        archive_hash=legacy['input_hash']
        legacy={k:v for k,v in legacy.items() if k not in {'review_history','input_hash'}}
        legacy['source_archive_input_sha256']=archive_hash;legacy['input_hash']=_hash(legacy)
        paper=read_paper_review_evidence(directory/'paper.db',trade_date=day,observed_at=now)
        journal=ModelJudgmentJournal(directory/'model.db')
        full=build_team_review_facts(legacy_facts=legacy,store=store,journal=journal,paper_snapshot=paper,
            settlement=paper.get('settlement'),account_id=ACCOUNT,trade_date=day,review_kind=review_kind,observed_at=now)
        store.freeze_control_context('a5-full-source:'+rid,source_sha256=full['input_hash'],body=full)
        facts,domain=coach_projection(full);repository=TeamPromptRepository(settings.root/'prompts/team_v2')
        prompt=repository.prepare('A5',dict(A5_FACT_SNAPSHOT=facts,EVIDENCE_CATALOG=facts['citation_catalog']))
        envelope=dict(schema_version='team-envelope/1',run_id=rid,stage_id='A5',source_trade_date=day,
            execution_trade_date=day,as_of=now.isoformat(),knowledge_cutoff=now.isoformat(),
            input_snapshot_ids=[facts['input_hash'],archive_hash],config_version='team-v2/1',
            config_hash=hashlib.sha256((settings.root/'config/risk_limits.yaml').read_bytes()).hexdigest(),
            prompt_version=prompt.role_version,model_name=settings.review_model,market_regime='DATA_BLOCK',
            status='FROZEN',reason_codes=[])
        frozen=dict(facts=facts,signal_domain=domain,required_envelope=envelope,model=settings.review_model,
            counterexample_domain=sorted({r['symbol'] for r in facts.get('counterexamples',[]) if r.get('symbol')}))
        store.freeze_control_context('a5-command:'+rid,source_sha256=_hash(frozen),body=frozen)
    wall_budget=min(wall_budget,review_wall_budget(policy,now=clock(),review_kind=review_kind))
    if wall_budget<=0:return dict(status='DEGRADED',reason_code='TEAM_A5_MIDDAY_WINDOW_CLOSED')
    result=run_team_review(repository=TeamPromptRepository(settings.root/'prompts/team_v2'),
        client=client or OpenAICompatibleModelClient(settings.model_copy(update={'model_timeout_seconds':policy['wall_clock_seconds']}),max_attempts=1),journal=ModelJudgmentJournal(directory/'model.db'),
        store=store,review_id=rid,call_id='A5:'+rid,account_id=ACCOUNT,run_id=rid,model=frozen['model'],
        trade_date=day,source_trade_date=day,review_kind=review_kind,clock=clock,
        required_envelope=frozen['required_envelope'],facts=frozen['facts'],signal_domain=frozen['signal_domain'],
        counterexample_domain=set(frozen['counterexample_domain']),timeout_seconds=wall_budget,max_output_tokens=policy['max_output_tokens'],allow_model_call=True)
    try:export=export_hypotheses(settings,result=result)
    except Exception:
        return dict(result,status='A5_REPLAY_QUEUE_EXPORT_FAILED',report_status=result['status'],
            reason_code='A5_REPLAY_QUEUE_EXPORT_FAILED')
    return dict(result,hypothesis_export=export)
