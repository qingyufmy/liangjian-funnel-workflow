"""Explicit 0247 finite renewal; historical shadow APIs/constants stay closed.

Only the same three approved original bodies, parser and cutoff are reused.
PASS/DATA_LIMITED are annotations. A detected changed body is BLOCK, never an
automatic new rule version. All writes remain in the caller's owned producer.
"""
import base64
from datetime import date,time
import hashlib
import json
from pathlib import Path
import time as timer

import yaml

from . import shadow_rule_preflight as old
from .bounded_work import BoundedWorkGate
from .calendar import ExchangeTradingCalendar
from .shadow_pit_sources import record_raw_response
from .team_v2_sources import stamp

_FETCH=BoundedWorkGate(1)


def load_plan(path):
    raw=Path(path).read_bytes();value=yaml.safe_load(raw)
    # This authorization is finite, not an operator-editable perpetual renewal.
    if value!=dict(plan_window={'from':date(2026,10,19),'to':date(2026,10,30)},
            approved_by='claude',approved_at='2026-10-17T01:25+08:00',
            scope='same-day refetch of the 3 approved URLs only'):
        raise ValueError('RULE_RENEWAL_AUTHORIZATION_INVALID')
    return dict(value,plan_sha256=hashlib.sha256(raw).hexdigest())


def _clock(plan,day,known):
    now=stamp(known);day=date.fromisoformat(str(day));window=plan['plan_window']
    if (not window['from']<=day<=window['to'] or day!=now.date()
            or now.time().replace(tzinfo=None)>=time(9,26)
            or not ExchangeTradingCalendar().is_trading_day(day)):
        raise ValueError('RULE_RENEWAL_DATE_OR_CUTOFF_UNPROVEN')
    return now


def confirm_plan(approved,records,*,plan,day,known):
    result=old._initial(day)
    result.update(schema_version='team-rule-renewal/1',renewal_plan_sha256=plan['plan_sha256'],
        renewal_window={k:str(v) for k,v in plan['plan_window'].items()},
        annotation_only=True,blocks_new_entries=False)
    try:
        now=_clock(plan,day,known);old._approved(approved);result['known_at']=now.isoformat()
        if not isinstance(records,(list,tuple)) or not 1<=len(records)<=3:raise ValueError('RULE_THREE_SOURCES_REQUIRED')
        indexed={r.endpoint:r for r in records}
        if len(indexed)!=len(records):raise ValueError('RULE_DUPLICATE_OR_UNBOUND_SOURCE')
        if set(indexed)-{d.url for d in approved.documents}:raise ValueError('RULE_SOURCE_SCOPE_MISMATCH')
        for doc in approved.documents:
            record=indexed.get(doc.url)
            if record is None:continue
            if not isinstance(record,old.RawResponseReceipt):raise ValueError('RULE_SOURCE_SCOPE_MISMATCH')
            rebuilt=record_raw_response(record.raw_response,source_ref=record.source_ref,endpoint=record.endpoint,
                request_parameters=record.request_parameters,request_options=record.request_options,
                request_started_at=record.request_started_at,response_received_at=record.response_received_at,
                http_status=record.http_status,complete=record.complete,byte_kind=record.byte_kind,clock_basis=record.clock_basis)
            if record.raw_sha256!=rebuilt.raw_sha256 or record.receipt_sha256!=rebuilt.receipt_sha256:
                raise ValueError('RULE_RESPONSE_BINDING_MISMATCH')
            audit=dict(document_id=doc.document_id,url=doc.url,approved_raw_sha256=doc.raw_sha256,
                **record.metadata(),source_receipt_sha256=record.receipt_sha256,binding_verified=True,
                raw_response_base64=base64.b64encode(record.raw_response).decode('ascii'),validation_status='DATA_LIMITED')
            result['rows'].append(audit)
            start=stamp(record.request_started_at);received=stamp(record.response_received_at)
            if (record.request_parameters or record.byte_kind!='HTTP_RESPONSE_CONTENT_BYTES'
                    or record.complete is not True or record.http_status!=200 or start.date()!=now.date()
                    or received.date()!=now.date() or not start<=received<=now
                    or received.time().replace(tzinfo=None)>=time(9,26)):
                raise ValueError('RULE_HTTP_OR_ARRIVAL_UNPROVEN')
            if record.raw_sha256!=doc.raw_sha256:
                result.update(status='BLOCK',annotation_only=False,blocks_new_entries=True)
                raise ValueError('RULE_VERSION_CHANGED_NO_AUTO_ACCEPT')
            audit.update(old._parse_document(doc.document_id,record.raw_response),validation_status='RULE_VERSION_CONFIRMED')
        if len(records)!=3:raise ValueError('RULE_THREE_SOURCES_REQUIRED')
        result.update(status='RULE_VERSION_CONFIRMED',confirmed_trade_date=str(day))
    except Exception as exc:
        code=str(exc) if isinstance(exc,(ValueError,old.RulePreflightError)) and str(exc).startswith('RULE_') else 'RULE_RESPONSE_EVIDENCE_UNPROVEN'
        result['reason_codes']=[code]
    return old._sealed(result)


def fetch_plan(approved,*,plan,day,clock,fetch_response,budget_seconds):
    started=timer.monotonic();count=[0]
    def limited(code):
        result=confirm_plan(approved,[],plan=plan,day=day,known=clock())
        result.update(reason_codes=[code],source_requests_started=count[0]);return old._sealed(result)
    try:
        _clock(plan,day,clock());old._approved(approved)
        if isinstance(budget_seconds,bool) or not 0<float(budget_seconds)<=30:
            raise ValueError('RULE_PREFLIGHT_BUDGET_INVALID')
    except Exception as exc:
        return limited(str(exc) if str(exc).startswith('RULE_') else 'RULE_RENEWAL_UNPROVEN')
    deadline=started+budget_seconds
    def worker():
        records=[]
        for doc in approved.documents:
            at=stamp(clock());remaining=deadline-timer.monotonic()
            if remaining<=0:raise ValueError('RULE_PREFLIGHT_BUDGET_EXCEEDED')
            count[0]+=1;response=fetch_response(doc.url,remaining);received=stamp(clock())
            if timer.monotonic()>=deadline:raise ValueError('RULE_PREFLIGHT_BUDGET_EXCEEDED')
            if str(response.url)!=doc.url:raise ValueError('RULE_HTTP_REDIRECT_UNPROVEN')
            records.append(record_raw_response(response.content,source_ref='official-rule:'+doc.document_id,
                endpoint=doc.url,request_parameters={},request_options={'timeout_argument':remaining},
                request_started_at=at,response_received_at=received,http_status=response.status_code,
                byte_kind='HTTP_RESPONSE_CONTENT_BYTES',clock_basis='INJECTED_CLOCK_NOT_AUTHENTICATED'))
            # Do not hide a proved changed body behind a later source timeout.
            if response.status_code==200 and records[-1].raw_sha256!=doc.raw_sha256:
                return confirm_plan(approved,records,plan=plan,day=day,known=received)
        return confirm_plan(approved,records,plan=plan,day=day,known=clock())
    work=_FETCH.call(worker,deadline=deadline,name='team-rule-renewal')
    if work.status!='READY':return limited('RULE_PREFLIGHT_BUDGET_OR_HTTP_FAILED')
    result=work.value;result['source_requests_started']=count[0];return old._sealed(result)


def validate_plan_receipt(approved,receipt,*,plan,at):
    try:
        if hashlib.sha256(old.canonical_receipt_bytes(receipt)).hexdigest()!=receipt['receipt_sha256']:
            raise ValueError
        now=stamp(at);known=stamp(receipt['known_at'])
        if known>now or now.date()!=known.date():raise ValueError
        records=[]
        for row in receipt['rows']:
            raw=base64.b64decode(row['raw_response_base64'],validate=True)
            record=record_raw_response(raw,**{k:row[k] for k in ('source_ref','endpoint','request_parameters',
                'request_options','request_started_at','response_received_at','http_status','complete','byte_kind','clock_basis')})
            if row['raw_sha256']!=record.raw_sha256 or row['source_receipt_sha256']!=record.receipt_sha256:raise ValueError
            records.append(record)
        result=confirm_plan(approved,records,plan=plan,day=str(now.date()),known=known)
        count=receipt['source_requests_started']
        if type(count) is not int or count!=3:raise ValueError
        result['source_requests_started']=count
        if old.canonical_receipt_bytes(result)!=old.canonical_receipt_bytes(receipt):raise ValueError
        return dict(status=result['status'],reason_codes=result['reason_codes'],blocks_new_entries=result['blocks_new_entries'])
    except Exception:return dict(status='DATA_LIMITED',reason_codes=['RULE_RENEWAL_RECEIPT_UNPROVEN'],blocks_new_entries=False)
