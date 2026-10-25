"""Append-only company research and cache-observation history, no trade rights."""
from contextlib import closing
from pathlib import Path
from collections.abc import Mapping
import json,sqlite3

from .feature_store import content_hash
from .company_value_research import FACETS,_time

SCHEMA='company-research-ledger/1'


def _json(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)


def source_identity(scope):
    """Retrieving unchanged PDF/facts again is not new economic evidence."""
    return {ref:sorted({item.get('semantic_fact_sha256',item['fact_sha256']) for item in originals})
        for ref,originals in sorted(scope['evidence'].items())}


class CompanyResearchLedger:
    def __init__(self,path):
        self.path=Path(path)
        self.path.parent.mkdir(parents=True,exist_ok=True)
        with closing(self._connect()) as db,db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS company_research_meta(version TEXT PRIMARY KEY);
                CREATE TABLE IF NOT EXISTS company_reviews(
                    review_id TEXT PRIMARY KEY,call_id TEXT NOT NULL,symbol TEXT NOT NULL,
                    source_as_of TEXT NOT NULL,observed_at TEXT NOT NULL,input_hash TEXT NOT NULL,
                    output_hash TEXT NOT NULL,scope_json TEXT NOT NULL,profile_json TEXT NOT NULL,
                    profile_hash TEXT NOT NULL,capture_origin TEXT NOT NULL,
                    UNIQUE(call_id,symbol));
                CREATE INDEX IF NOT EXISTS company_review_time ON company_reviews(symbol,source_as_of,observed_at);
                CREATE TABLE IF NOT EXISTS company_source_observations(
                    observation_id TEXT PRIMARY KEY,symbol TEXT NOT NULL,source_as_of TEXT NOT NULL,
                    observed_at TEXT NOT NULL,review_id TEXT NOT NULL,scope_json TEXT NOT NULL,
                    changes_json TEXT NOT NULL,status TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS company_request_baselines(
                    identity TEXT PRIMARY KEY,body_json TEXT NOT NULL,body_hash TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS company_model_captures(
                    call_id TEXT PRIMARY KEY,input_hash TEXT NOT NULL,response_json TEXT NOT NULL,response_hash TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS company_chain_audits(
                    call_id TEXT PRIMARY KEY,input_hash TEXT NOT NULL,observed_at TEXT NOT NULL,
                    output_json TEXT NOT NULL,output_hash TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS company_a5_advisories(
                    call_id TEXT PRIMARY KEY,input_hash TEXT NOT NULL,observed_at TEXT NOT NULL,
                    output_json TEXT NOT NULL,output_hash TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS company_followup_actions(
                    action_id TEXT PRIMARY KEY,audit_call_id TEXT NOT NULL,symbol TEXT NOT NULL,
                    owner TEXT NOT NULL,trigger_text TEXT NOT NULL,required_source TEXT NOT NULL,created_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS company_review_fact_audits(
                    audit_id TEXT PRIMARY KEY,review_id TEXT NOT NULL,observed_at TEXT NOT NULL,
                    body_json TEXT NOT NULL,body_hash TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS company_review_sources(
                    review_id TEXT PRIMARY KEY,dossier_json TEXT NOT NULL,dossier_hash TEXT NOT NULL);
            ''')
            db.execute('INSERT OR IGNORE INTO company_research_meta VALUES (?)',(SCHEMA,))

    def _connect(self):return sqlite3.connect(self.path,timeout=2)

    def record_review(self,*,call_id,input_hash,output_hash,output,scopes,source_as_of,observed_at,capture_origin,captured_output=None,numeric_audits=None,dossiers=None):
        if (content_hash(output)!=output_hash or _time(source_as_of) is None or _time(observed_at) is None
                or _time(source_as_of)>_time(observed_at) or capture_origin not in
                {'CAPTURED_CLIENT_RESPONSE_TRANSPORT_NOT_INDEPENDENTLY_VERIFIED','CONTROLLED_TEST'}):
            raise ValueError('COMPANY_REVIEW_ORIGINAL_BINDING_INVALID')
        from .company_value_research import validate_company_research
        if validate_company_research(output,scopes,required=True):raise ValueError('COMPANY_REVIEW_NOT_VALIDATED')
        source_records={}
        if dossiers is not None:
            from .company_dossier import validate_dossier
            from .company_value_research import company_evidence_scopes
            for symbol,dossier in dossiers.items():
                validate_dossier(dossier,symbol=symbol,knowledge_cutoff=source_as_of)
                if _time(dossier['observed_at'])>_time(observed_at):raise ValueError('COMPANY_REVIEW_SOURCE_FUTURE_RECEIPT')
                exact=company_evidence_scopes({'COMPANY_RESEARCH_DOSSIER':{symbol:dossier}},[symbol],knowledge_cutoff=source_as_of)[symbol]
                if any(value not in scopes.get(symbol,{}).get('evidence',{}).get(ref,[]) for ref,values in exact['evidence'].items() for value in values):
                    raise ValueError('COMPANY_REVIEW_SOURCE_SCOPE_MISMATCH')
                source_records[symbol]=dossier
        records=[]
        for pool in ('active_research_pool','monitor_pool'):
            for row in output.get(pool,[]):
                from .research.common import _first_symbol
                symbol=_first_symbol(row['symbol']);scope=scopes[symbol]
                profile=dict(symbol=symbol,original_pool=pool,company_value_research=row['company_value_research'],
                    financial_transmission=row['financial_transmission'],
                    company_followup_review=row.get('company_followup_review'),execution_authority=False)
                if numeric_audits is not None:profile['monetary_claim_audit']=numeric_audits[symbol]
                profile_hash=content_hash(profile)
                review_id=content_hash(dict(call_id=call_id,symbol=symbol,input_hash=input_hash))
                records.append((review_id,call_id,symbol,source_as_of,observed_at,input_hash,output_hash,
                    _json(scope),_json(profile),profile_hash,capture_origin))
        with closing(self._connect()) as db,db:
            if captured_output is not None:
                capture=(call_id,input_hash,_json(captured_output),content_hash(captured_output))
                old=db.execute('SELECT * FROM company_model_captures WHERE call_id=?',(call_id,)).fetchone()
                if old and old!=capture:raise ValueError('COMPANY_MODEL_CAPTURE_IMMUTABLE_CONFLICT')
                if not old:db.execute('INSERT INTO company_model_captures VALUES (?,?,?,?)',capture)
            for record in records:
                old=db.execute('SELECT * FROM company_reviews WHERE review_id=?',(record[0],)).fetchone()
                if old and old[:4]+old[5:]!=record[:4]+record[5:]:raise ValueError('COMPANY_REVIEW_IMMUTABLE_CONFLICT')
                if not old:db.execute('INSERT INTO company_reviews VALUES (?,?,?,?,?,?,?,?,?,?,?)',record)
                if record[2] in source_records:
                    source=source_records[record[2]];value=(record[0],_json(source),content_hash(source))
                    old_source=db.execute('SELECT * FROM company_review_sources WHERE review_id=?',(record[0],)).fetchone()
                    if old_source and old_source!=value:raise ValueError('COMPANY_REVIEW_SOURCE_IMMUTABLE_CONFLICT')
                    # An old judgment must not acquire new source content by
                    # later backfill. Such a review needs a new actual request.
                    if old and not old_source:raise ValueError('COMPANY_REVIEW_SOURCE_NOT_CAPTURED_AT_REVIEW')
                    if not old_source:db.execute('INSERT INTO company_review_sources VALUES (?,?,?)',value)
        return [r[0] for r in records]

    def baselines(self,symbols,*,source_as_of,observed_at):
        result={}
        if _time(source_as_of) is None or _time(observed_at) is None:raise ValueError('COMPANY_BASELINE_CLOCK_INVALID')
        with closing(self._connect()) as db:
            db.row_factory=sqlite3.Row
            for symbol in sorted(symbols):
                rows=db.execute('SELECT * FROM company_reviews WHERE symbol=? ORDER BY source_as_of DESC,observed_at DESC,review_id DESC',(symbol,)).fetchall()
                eligible=[r for r in rows if _time(r['source_as_of'])<=_time(source_as_of) and _time(r['observed_at'])<=_time(observed_at)]
                if not eligible:continue
                r=dict(max(eligible,key=lambda r:(_time(r['source_as_of']),_time(r['observed_at']),r['review_id'])))
                profile=json.loads(r.pop('profile_json'));scope=json.loads(r.pop('scope_json'))
                if content_hash(profile)!=r['profile_hash']:raise ValueError('COMPANY_LEDGER_PROFILE_HASH_MISMATCH')
                result[symbol]=dict(r,profile=profile,scope=scope)
                source=None
                if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='company_review_sources'").fetchone():
                    source=db.execute('SELECT dossier_json,dossier_hash FROM company_review_sources WHERE review_id=?',(r['review_id'],)).fetchone()
                result[symbol]['original_source_status']='CAPTURED_WITH_REVIEW' if source else 'ORIGINAL_SOURCE_NOT_CAPTURED_WITH_REVIEW'
                if source:
                    dossier=json.loads(source[0])
                    if content_hash(dossier)!=source[1]:raise ValueError('COMPANY_LEDGER_SOURCE_HASH_MISMATCH')
                    result[symbol]['original_dossier']=dossier
                if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='company_review_fact_audits'").fetchone():
                    result[symbol]['subsequent_fact_audits']=[json.loads(a[0]) for a in db.execute(
                        'SELECT body_json FROM company_review_fact_audits WHERE review_id=? AND julianday(observed_at)<=julianday(?) ORDER BY julianday(observed_at)',
                        (r['review_id'],observed_at))]
        return result

    def record_fact_audit(self,review_id,*,observed_at,audit):
        if _time(observed_at) is None or audit.get('schema')!='company-monetary-claim-audit/1' or audit.get('execution_authority') is not False:
            raise ValueError('COMPANY_FACT_AUDIT_INVALID')
        identity=content_hash(dict(review_id=review_id,observed_at=observed_at,audit=audit))
        with closing(self._connect()) as db,db:
            if not db.execute('SELECT 1 FROM company_reviews WHERE review_id=?',(review_id,)).fetchone():
                raise ValueError('COMPANY_FACT_AUDIT_UNKNOWN_REVIEW')
            db.execute('INSERT OR IGNORE INTO company_review_fact_audits VALUES (?,?,?,?,?)',
                (identity,review_id,observed_at,_json(audit),content_hash(audit)))
        return identity

    def freeze_baselines(self,identity,symbols,*,source_as_of,observed_at):
        with closing(self._connect()) as db:
            old=db.execute('SELECT body_json,body_hash FROM company_request_baselines WHERE identity=?',(identity,)).fetchone()
        if old:
            body=json.loads(old[0])
            if content_hash(body)!=old[1]:raise ValueError('COMPANY_BASELINE_ORIGINAL_HASH_MISMATCH')
            return body
        body=self.baselines(symbols,source_as_of=source_as_of,observed_at=observed_at)
        with closing(self._connect()) as db,db:
            db.execute('INSERT OR IGNORE INTO company_request_baselines VALUES (?,?,?)',(identity,_json(body),content_hash(body)))
        return self.freeze_baselines(identity,symbols,source_as_of=source_as_of,observed_at=observed_at)

    def observe(self,scopes,*,source_as_of,observed_at):
        baselines=self.baselines(scopes,source_as_of=source_as_of,observed_at=observed_at);result=[]
        for symbol,scope in sorted(scopes.items()):
            if symbol not in baselines:continue
            prior=baselines[symbol];before=source_identity(prior['scope']);after=source_identity(scope)
            changes=dict(added=sorted(set(after)-set(before)),removed=sorted(set(before)-set(after)),
                revised=sorted(ref for ref in set(before)&set(after) if before[ref]!=after[ref]))
            # A compact daily projection can omit old annual-report pages.
            # Their absence is a visibility gap, not a retraction or a new
            # adverse fact. Only new/revised actual evidence queues review.
            status=('PENDING_COMPANY_REVIEW' if changes['added'] or changes['revised'] else
                'SOURCE_PROJECTION_GAP' if changes['removed'] else 'NO_NEW_SOURCE_CONTENT')
            body=dict(symbol=symbol,review_id=prior['review_id'],source_as_of=source_as_of,
                observed_at=observed_at,scope=scope,changes=changes,
                status=status,
                automatic_investment_verdict=False,execution_authority=False)
            identity=content_hash(body)
            with closing(self._connect()) as db,db:
                db.execute('INSERT OR IGNORE INTO company_source_observations VALUES (?,?,?,?,?,?,?,?)',
                    (identity,symbol,source_as_of,observed_at,prior['review_id'],_json(scope),_json(changes),body['status']))
            result.append(dict(body,observation_id=identity))
        return result

    def record_a5_audit(self,*,call_id,input_hash,observed_at,output,context=None):
        if (output.get('schema')!='company-chain-audit/1' or _time(observed_at) is None
                or any(output.get(k) is not False for k in ('automatic_changes','entry_profit_proven','execution_authority'))):
            raise ValueError('COMPANY_A5_RESEARCH_AUTHORITY_INVALID')
        from .company_audit_policy import validate_company_a5_probe
        if (context is None or context['request'].input_hash!=input_hash
                or not context.get('fact_boundaries') or not context.get('followup_catalog')
                or not context.get('current_bindings') or set(context['current_bindings'])!=set(context['symbols'])
                or not validate_company_a5_probe(output,context).valid
                or _time(observed_at)<_time(output['observed_at'])):
            raise ValueError('COMPANY_A5_VALIDATED_CONTEXT_REQUIRED')
        with closing(self._connect()) as db:
            for symbol,binding in context['current_bindings'].items():
                original=db.execute('SELECT symbol FROM company_reviews WHERE review_id=?',(binding['a1_review_id'],)).fetchone()
                if original!=(symbol,):raise ValueError('COMPANY_A5_RECORDED_A1_BINDING_REQUIRED')
        # Model prose is advisory, never a fact row or an executable policy.
        # Persist only the exact server-bound enum/identity projection here.
        sealed=dict(schema='company-chain-facts/1',observed_at=output['observed_at'],
            source_trade_date=output['source_trade_date'],
            company_reviews=[dict(symbol=r['symbol'],current_state=r['current_state'],
                realization_fact=r['realization_fact']) for r in output['company_reviews']],
            followup_actions=[dict(action_id=a['action_id'],symbol=a['symbol'],owner=a['owner'])
                for a in output['followup_actions']],execution_authority=False)
        row=(call_id,input_hash,observed_at,_json(sealed),content_hash(sealed))
        with closing(self._connect()) as db,db:
            old=db.execute('SELECT * FROM company_chain_audits WHERE call_id=?',(call_id,)).fetchone()
            if old and old!=row:raise ValueError('COMPANY_A5_AUDIT_IMMUTABLE_CONFLICT')
            if not old:db.execute('INSERT INTO company_chain_audits VALUES (?,?,?,?,?)',row)
            known={r[0] for r in db.execute('SELECT DISTINCT symbol FROM company_reviews')}
            for action in output['followup_actions']:
                if action['symbol'] not in known or action['owner'] not in {'A1','A2','A3','DATA'}:
                    raise ValueError('COMPANY_FOLLOWUP_ACTION_OUTSIDE_RESEARCH')
                identity=content_hash(dict(call_id=call_id,action=action))
                db.execute('INSERT OR IGNORE INTO company_followup_actions VALUES (?,?,?,?,?,?,?)',
                    (identity,call_id,action['symbol'],action['owner'],action['trigger'],action['required_source'],observed_at))

    def record_a5_advisory(self,*,call_id,input_hash,observed_at,output):
        """Immutable untrusted original; no reader in any production projection."""
        if _time(observed_at) is None:raise ValueError('COMPANY_A5_CLOCK_INVALID')
        row=(call_id,input_hash,observed_at,_json(output),content_hash(output))
        with closing(self._connect()) as db,db:
            old=db.execute('SELECT * FROM company_a5_advisories WHERE call_id=?',(call_id,)).fetchone()
            if old and old!=row:raise ValueError('COMPANY_A5_ADVISORY_IMMUTABLE_CONFLICT')
            if not old:db.execute('INSERT INTO company_a5_advisories VALUES (?,?,?,?,?)',row)


def read_company_tracking(path,*,observed_at):
    """A5 read-only path: absence never initializes or migrates a database."""
    path=Path(path)
    if not path.exists():return dict(status='NOT_INITIALIZED',reviews=[],observations=[],followup_actions=[],audits=[],execution_authority=False)
    if _time(observed_at) is None:raise ValueError('COMPANY_A5_CLOCK_INVALID')
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=.5)) as db:
        db.row_factory=sqlite3.Row;db.execute('pragma query_only=ON')
        if [r[0] for r in db.execute('SELECT version FROM company_research_meta')]!=[SCHEMA]:
            raise ValueError('COMPANY_LEDGER_VERSION_INVALID')
        reviews=[];observations=[]
        total_reviews=db.execute('SELECT count(*) FROM company_reviews WHERE julianday(observed_at)<=julianday(?)',(observed_at,)).fetchone()[0]
        total_observations=db.execute('SELECT count(*) FROM company_source_observations WHERE julianday(observed_at)<=julianday(?)',(observed_at,)).fetchone()[0]
        for r in db.execute('SELECT * FROM company_reviews WHERE julianday(observed_at)<=julianday(?) ORDER BY julianday(observed_at) DESC,review_id LIMIT 200',(observed_at,)):
            body=dict(r);profile=json.loads(body.pop('profile_json'))
            if content_hash(profile)!=body['profile_hash']:raise ValueError('COMPANY_A5_ORIGINAL_HASH_MISMATCH')
            body['profile']=profile;body['scope']=json.loads(body.pop('scope_json'))
            original=None
            if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='company_review_sources'").fetchone():
                original=db.execute('SELECT dossier_json,dossier_hash FROM company_review_sources WHERE review_id=?',(body['review_id'],)).fetchone()
            body['original_source_status']='CAPTURED_WITH_REVIEW' if original else 'ORIGINAL_SOURCE_NOT_CAPTURED_WITH_REVIEW'
            if original:
                value=json.loads(original[0])
                if content_hash(value)!=original[1]:raise ValueError('COMPANY_LEDGER_SOURCE_HASH_MISMATCH')
                body['original_dossier']=value
            reviews.append(body)
        for r in db.execute('SELECT * FROM company_source_observations WHERE julianday(observed_at)<=julianday(?) ORDER BY julianday(observed_at) DESC,observation_id LIMIT 200',(observed_at,)):
            body=dict(r);body['changes']=json.loads(body.pop('changes_json'))
            expected_sources=source_identity(json.loads(body.pop('scope_json')))
            # Derive resolution from an explicit later A1 five-facet review;
            # retain the original immutable observation/status and chronology.
            followups=[v for v in reviews if v['symbol']==body['symbol']
                and _time(v['observed_at'])>=_time(body['observed_at'])
                and (v['profile'].get('company_followup_review') or {}).get('prior_review_id')==body['review_id']
                and all(source_identity(v['scope']).get(ref)==expected_sources[ref]
                    for ref in body['changes']['added']+body['changes']['revised'])]
            body['review_resolution']=('FOLLOWUP_RECORDED_NOT_INVESTMENT_APPROVED' if followups else 'UNREVIEWED')
            body['resolution_review_ids']=[v['review_id'] for v in followups]
            observations.append(body)
        actions=[];audits=[];fact_audits=[]
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='company_review_fact_audits'").fetchone():
            for row in db.execute('SELECT * FROM company_review_fact_audits WHERE julianday(observed_at)<=julianday(?) ORDER BY julianday(observed_at) DESC LIMIT 100',(observed_at,)):
                item=dict(row);body=json.loads(item.pop('body_json'))
                if content_hash(body)!=item['body_hash']:raise ValueError('COMPANY_FACT_AUDIT_HASH_MISMATCH')
                fact_audits.append(dict(item,audit=body))
        for r in db.execute('SELECT * FROM company_followup_actions WHERE julianday(created_at)<=julianday(?) ORDER BY julianday(created_at) DESC,action_id LIMIT 200',(observed_at,)):
            action=dict(r)
            action['state']='SOURCE_CHANGE_AVAILABLE_REVIEW_PENDING' if any(
                o['symbol']==r['symbol'] and _time(o['observed_at'])>=_time(r['created_at']) and o['status']=='PENDING_COMPANY_REVIEW'
                and o['review_resolution']=='UNREVIEWED'
                for o in observations) else 'WAITING_FOR_VERIFIED_SOURCE_OR_REVIEW_TRIGGER'
            action['automatically_completed']=False;actions.append(action)
        for r in db.execute('SELECT * FROM company_chain_audits WHERE julianday(observed_at)<=julianday(?) ORDER BY julianday(observed_at) DESC,call_id LIMIT 50',(observed_at,)):
            audit=dict(r);output=json.loads(audit.pop('output_json'))
            if content_hash(output)!=audit['output_hash']:raise ValueError('COMPANY_A5_AUDIT_HASH_MISMATCH')
            audit['output']=output;audits.append(audit)
    return dict(status='AVAILABLE',schema=SCHEMA,reviews=reviews,observations=observations,
        followup_actions=actions,audits=audits,fact_audits=fact_audits,total_review_rows=total_reviews,total_observation_rows=total_observations,
        bounded_view_truncated=total_reviews>len(reviews) or total_observations>len(observations),
        economic_truth_automatically_verified=False,execution_authority=False)


def validate_followups(output,baselines,scopes):
    issues=[]
    for pool in ('active_research_pool','monitor_pool'):
        for row in output.get(pool,[]):
            from .research.common import _first_symbol
            symbol=_first_symbol(row.get('symbol'));prior=baselines.get(symbol)
            if prior is None:continue
            follow=row.get('company_followup_review')
            if (not isinstance(follow,Mapping) or set(follow)!={'prior_review_id','facets'}
                    or follow['prior_review_id']!=prior['review_id'] or not isinstance(follow['facets'],Mapping)
                    or set(follow['facets'])!=set(FACETS)):
                issues.append('A1_COMPANY_FOLLOWUP_DOMAIN_INVALID:'+symbol+':EXPECTED_PRIOR_REVIEW_ID='+prior['review_id']);continue
            for facet,value in follow['facets'].items():
                if (not isinstance(value,Mapping) or set(value)!={'status','reason','source_refs'}
                        or not isinstance(value['status'],str) or value['status'] not in {'MAINTAIN','WEAKEN','INVALIDATE','UNRESOLVED'}
                        or not isinstance(value['reason'],str) or not 0<len(value['reason'].strip())<=600
                        or not isinstance(value['source_refs'],list) or len(value['source_refs'])>3
                        or any(not isinstance(r,str) or r not in scopes[symbol]['evidence'] for r in value['source_refs'])
                        or (value['status']!='UNRESOLVED' and not value['source_refs'])):
                    issues.append('A1_COMPANY_FOLLOWUP_EVIDENCE_INVALID:'+symbol+':'+facet)
    return issues
