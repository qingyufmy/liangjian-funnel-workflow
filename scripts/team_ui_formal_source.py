"""Whitelist-only formal close status; stdlib RO, no runtime constructors."""
import hashlib,json,sqlite3


def formal_source(root,day,connect):
    db=connect(root,'state/workflow.sqlite3')
    try:
        if not db.execute("SELECT 1 FROM sqlite_master WHERE name='natural_review_candidate_head' AND type='table'").fetchone():
            return dict(status='NOT_GENERATED',reason_code='FORMAL_SOURCE_NOT_RECORDED')
        rows=db.execute('SELECT h.scope_id,h.body_sha256,c.body_json,c.body_sha256 AS stored_sha,c.state FROM natural_review_candidate_head h '
            'LEFT JOIN natural_review_candidates c ON c.scope_id=h.scope_id '
            'WHERE h.account_id=? AND h.trade_date=? LIMIT 2',('local-paper',day)).fetchall()
        if not rows:return dict(status='NOT_GENERATED',reason_code='FORMAL_SOURCE_NOT_RECORDED')
        if len(rows)!=1:raise ValueError('FORMAL_SOURCE_AMBIGUOUS')
        row=rows[0];raw=row['body_json']
        if not isinstance(raw,str) or row['stored_sha']!=row['body_sha256']:
            raise ValueError('FORMAL_SOURCE_HEAD_UNPROVEN')
        # Full 36-stock originals exceed 1 MiB; retain all lineage in storage
        # while returning only the same small status projection to the UI.
        if len(raw.encode())>8*1024**2 or hashlib.sha256(raw.encode()).hexdigest()!=row['body_sha256']:
            raise ValueError('FORMAL_SOURCE_HASH_INVALID')
        body=json.loads(raw)
        if body.get('trade_date')!=day or body.get('scope_id')!=row['scope_id']:
            raise ValueError('FORMAL_SOURCE_SCOPE_INVALID')
        if row['state']!='EMPTY':return dict(status='CANDIDATE_PENDING_REVIEW' if row['state']=='PENDING' else row['state'],
            execution_authority=False,source_sha256=row['body_sha256'])
        if body.get('schema_version')!='natural-review-empty/1' or body.get('plan_rows')!=[] or body.get('execution_authority') is not False:
            raise ValueError('FORMAL_EMPTY_INVALID')
        import re
        reasons=[]
        for r in body.get('blocked',[])[:200]:
            if isinstance(r,dict) and re.fullmatch('[A-Z0-9_:.+-]{1,120}',str(r.get('reason',''))):
                symbol=r.get('symbol')
                if isinstance(symbol,str) and re.fullmatch(r'\d{6}\.(?:SZ|SH|BJ)',symbol):
                    reasons.append(dict(symbol=symbol,reason_code=r['reason']))
        return dict(status='FORMAL_EMPTY',reason_code='NATURAL_ZERO_PLAN_SOURCE',candidate_count=0,
            source_trade_date=body['source_trade_date'],target_trade_date=day,source_sha256=row['body_sha256'],
            blocked=reasons,execution_authority=False)
    finally:db.close()
