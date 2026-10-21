"""Owned A5 role, replay hypotheses and internal-only close-card outbox.

No customer publisher or execution-plan dependency. A committed review, method
feedback, replay queue, and card are atomic; a model capture can resume without
another HTTP request after a local commit failure. An uncertain card delivery
is never retried automatically (remote webhooks have no exactly-once contract).
"""
from contextlib import closing
from datetime import datetime,time
import hashlib
import json
from zoneinfo import ZoneInfo

from ..pipeline.team_v2_review_schema import validate_daily_review
from ..pipeline.numeric_fact_domain import numeric_fact_domain,numeric_domain_hash
from .team_v2_eod import _response
from .team_v2_store import canonical


def _hash(value):return hashlib.sha256(canonical(value).encode()).hexdigest()


class TeamReviewRuntime:
    def __init__(self,store):
        self.store=store
        with closing(store._connect()) as db,db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS team_a5_reviews (
                    review_id TEXT PRIMARY KEY,input_sha256 TEXT NOT NULL,account_id TEXT NOT NULL,
                    trade_date TEXT NOT NULL,review_kind TEXT NOT NULL,source_call_id TEXT NOT NULL,result_json TEXT NOT NULL,
                    UNIQUE(account_id,trade_date,review_kind));
                CREATE TABLE IF NOT EXISTS team_hypothesis_queue (
                    queue_id TEXT PRIMARY KEY,review_id TEXT NOT NULL,hypothesis_id TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'PENDING_REPLAY',body_json TEXT NOT NULL,
                    UNIQUE(review_id,hypothesis_id));
                CREATE TABLE IF NOT EXISTS team_method_feedback (
                    review_id TEXT NOT NULL,recipient TEXT NOT NULL,body_json TEXT NOT NULL,
                    PRIMARY KEY(review_id,recipient));
                CREATE TABLE IF NOT EXISTS team_internal_cards (
                    card_id TEXT PRIMARY KEY,review_id TEXT NOT NULL,body_json TEXT NOT NULL,
                    body_sha256 TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'PENDING',result_code TEXT);
            ''')

    def load(self,review_id,*,input_sha256):
        with closing(self.store._connect()) as db:
            row=db.execute('SELECT input_sha256,result_json FROM team_a5_reviews WHERE review_id=?',(review_id,)).fetchone()
        if row is None:return None
        if row[0]!=input_sha256:raise ValueError('A5_REVIEW_BINDING_CONFLICT')
        return json.loads(row[1])

    def record(self,*,review_id,input_sha256,account_id,trade_date,review_kind,source_call_id,result,card):
        text=canonical(result)
        with closing(self.store._connect()) as db,db:
            db.execute('BEGIN IMMEDIATE')
            old=db.execute('SELECT review_id,input_sha256,result_json FROM team_a5_reviews WHERE '
                'review_id=? OR (account_id=? AND trade_date=? AND review_kind=?)',
                (review_id,account_id,trade_date,review_kind)).fetchall()
            if old:
                if old!=[(review_id,input_sha256,text)]:raise ValueError('A5_REVIEW_BINDING_CONFLICT')
                return json.loads(old[0][2])
            db.execute('INSERT INTO team_a5_reviews VALUES (?,?,?,?,?,?,?)',
                (review_id,input_sha256,account_id,trade_date,review_kind,source_call_id,text))
            report=result.get('report')
            if report is not None:
                for row in report['hypotheses']:
                    key=_hash([review_id,row['hypothesis_id']])
                    db.execute('INSERT INTO team_hypothesis_queue(queue_id,review_id,hypothesis_id,body_json) '
                        'VALUES (?,?,?,?)',(key,review_id,row['hypothesis_id'],canonical(row)))
                for recipient,rows in report['method_feedback'].items():
                    db.execute('INSERT INTO team_method_feedback VALUES (?,?,?)',(review_id,recipient,canonical(rows)))
            prefix='team-close:' if review_kind=='POST_CLOSE' else 'team-midday:'
            db.execute('INSERT INTO team_internal_cards(card_id,review_id,body_json,body_sha256) VALUES (?,?,?,?)',
                (prefix+account_id+':'+trade_date,review_id,canonical(card),_hash(card)))
        return json.loads(text)

    def deliver_internal(self,card_id,*,send_internal):
        """Only a dedicated internal sender may be injected; no customer API."""
        with closing(self.store._connect()) as db,db:
            db.execute('BEGIN IMMEDIATE')
            row=db.execute('SELECT body_json,body_sha256,status FROM team_internal_cards WHERE card_id=?',(card_id,)).fetchone()
            if row is None:return dict(status='NO_REPORT_TODAY',card_id=card_id)
            if hashlib.sha256(row[0].encode()).hexdigest()!=row[1]:raise ValueError('INTERNAL_CARD_HASH_MISMATCH')
            if row[2]!='PENDING':return dict(status=row[2],card_id=card_id)
            db.execute("UPDATE team_internal_cards SET status='SENDING' WHERE card_id=?",(card_id,))
        try:
            acknowledged=send_internal(json.loads(row[0])) is True
            status='SENT' if acknowledged else 'UNKNOWN'
            code='INTERNAL_ACKNOWLEDGED' if acknowledged else 'INTERNAL_DELIVERY_UNCONFIRMED'
        except Exception:
            status='UNKNOWN';code='INTERNAL_DELIVERY_UNCONFIRMED'
        with closing(self.store._connect()) as db,db:
            db.execute('UPDATE team_internal_cards SET status=?,result_code=? WHERE card_id=? AND status=\'SENDING\'',
                (status,code,card_id))
        return dict(status=status,card_id=card_id)


def close_card(facts,*,review_status):
    """Server facts only; never copy model prose or provider exception text."""
    metrics=facts['metrics'].get('team_v2',{});settlement=facts['paper_broker'].get('settlement')
    def show(value):return '资料不足' if value is None else str(value)
    return dict(msg_type='text',content={'text': '\n'.join([
        '量见团队模拟盘'+('午间' if facts['review_kind']=='MIDDAY' else '收盘')+'｜'+facts['trade_date'],
        '复盘：'+('已完成' if review_status=='READY' else '未完成，事实仍保留'),
        '模拟账户净值：'+show(metrics.get('nav')),
        '使用率：'+show(metrics.get('utilization_pct')),
        '结算：'+('缺少结算回执' if settlement is None else str(settlement.get('status','UNKNOWN'))),
        '模型判断调用数：'+show(metrics.get('model_calls')),
        '成交口径：同一模拟账户；供应商成交量加权价与估算成交分列，不能当作两套独立收益曲线。',
        '严格收益与对照校准：未成熟或无证据时不评价。'])})


def run_team_review(*,repository,client,journal,store,review_id,call_id,account_id,run_id,model,
        trade_date,source_trade_date,review_kind,clock,required_envelope,facts,
        signal_domain,counterexample_domain,confirmed_defect_symbols=frozenset(),
        timeout_seconds=180,max_output_tokens=16384,allow_model_call=False):
    if not allow_model_call:raise ValueError('TEAM_MODEL_CALL_NOT_AUTHORIZED')
    now=clock()
    if (not isinstance(now,datetime) or now.utcoffset() is None or review_kind not in {'MIDDAY','POST_CLOSE'}
            or now.astimezone(ZoneInfo('Asia/Shanghai')).date().isoformat()!=trade_date
            or now.astimezone(ZoneInfo('Asia/Shanghai')).time().replace(tzinfo=None)<
                (time(11,30) if review_kind=='MIDDAY' else time(15,30))):raise ValueError('A5_REVIEW_WINDOW_INVALID')
    if (facts.get('input_hash')!=_hash({k:v for k,v in facts.items() if k!='input_hash'})
            or any(facts.get(k)!=v for k,v in dict(account_id=account_id,trade_date=trade_date,review_kind=review_kind).items())):
        raise ValueError('A5_FACT_BINDING_INVALID')
    catalog=facts['citation_catalog'];session=review_kind
    context=dict(facts=facts,account_id=account_id,run_id=run_id,model=model,call_id=call_id,
        source_trade_date=source_trade_date,required_envelope=required_envelope,
        signal_domain=dict(sorted(signal_domain.items())),counterexample_domain=sorted(counterexample_domain),
        confirmed_defect_symbols=sorted(confirmed_defect_symbols),timeout_seconds=timeout_seconds,
        max_output_tokens=max_output_tokens,prompt_hashes=dict(repository.prepare('A5',
            dict(A5_FACT_SNAPSHOT=facts,EVIDENCE_CATALOG=catalog)).source_hashes))
    binding=_hash(context)
    store.freeze_control_context('a5-review:'+review_id,source_sha256=binding,body=context)
    runtime=TeamReviewRuntime(store);previous=runtime.load(review_id,input_sha256=binding)
    if previous is not None:return previous
    number_domain=numeric_fact_domain(facts)
    args=dict(required_envelope=required_envelope,trade_date=trade_date,review_kind=review_kind,
        signal_domain=signal_domain,counterexample_domain=counterexample_domain,
        confirmed_defect_symbols=confirmed_defect_symbols,metrics=facts['metrics'],numeric_domain=number_domain)
    request=repository.prepare_messages('A5',dict(A5_FACT_SNAPSHOT=facts,EVIDENCE_CATALOG=catalog),
        required_envelope=required_envelope,session=session)
    from ..pipeline.team_v2_prompts import with_runtime_contract
    request=with_runtime_contract(request,dict(session=session,
        schema='a5-daily-review/2.0.0',preserve_all_root_fields=True,
        style='紧凑JSON：摘要和每层summary各不超过100字；每条strength/defect/limitation不超过60字；'
            '只列有直接原件支持的重点，每层每类至多2条；hypotheses至多1条；不重复叙述相同计数。'
            '未知明确UNAVAILABLE，不将模型raw校验失败冒充服务端最终阶段失败。',
        scorecard_numbers='只引用A5_FACT_SNAPSHOT中原有数值，不自行推算新数字；每项原数需能回溯路径。'))
    # No silent truncation to fit provider limits, and no model repair recall.
    if sum(len(m['content']) for m in request.messages)>250000:raise ValueError('A5_MODEL_CONTEXT_TOO_LARGE')
    output,valid,issues=_response(request=request,client=client,journal=journal,call_id=call_id,
        run_id=run_id,model=model,source_trade_date=source_trade_date,trade_date=trade_date,
        timeout_seconds=timeout_seconds,max_output_tokens=max_output_tokens,
        validator=lambda raw:validate_daily_review(raw,catalog,**args),catalog=catalog,clock=clock,session=session)
    result=dict(status='READY' if valid else 'A5_DEGRADED_NO_APPROVED_REPORT',source_call_id=call_id,
        trade_date=trade_date,review_kind=review_kind,account_id=account_id,
        fact_sha256=facts['input_hash'],report=output if valid else None,reason_codes=list(issues),
        numeric_claim_audit=dict(source_fact_sha256=facts['input_hash'],
            domain_sha256=numeric_domain_hash(number_domain),source_paths=number_domain),
        strategy_change_authorized=False)
    return runtime.record(review_id=review_id,input_sha256=binding,account_id=account_id,trade_date=trade_date,
        review_kind=review_kind,source_call_id=call_id,result=result,card=close_card(facts,review_status=result['status']))
