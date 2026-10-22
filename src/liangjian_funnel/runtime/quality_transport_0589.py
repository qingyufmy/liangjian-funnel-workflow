"""Process-local 0591 quality transport instrumentation, never a launch grant.

API::

    guard = QualityTransportGuard(binding=..., directory=NEW_OWNED_DIRECTORY,
        deadline=AWARE_ACTUAL_DEADLINE)
    with guard.install():
        # Existing real Team entry points / ModelClient; no synthetic claims.
        ...
    ledger = guard.snapshot()

Common binding is model_change_gate v2's head/source_trade_date/
execution_trade_date/quality_run_id/provider_base_url/models/role_run_ids.
The four maps retain A2/A3/A6/PM. Additional mandatory a4_model/a4_run_id bind
the fifth role without relabelling PM's original A3/PM_PREMARKET journal.
All five roles share ONE <=24-dispatch ledger; projections may not reset it.
The caller must separately verify approval, own-full, isolation and actual
calendar deadline. This helper neither creates nor deletes business claims,
calls the model, changes Settings/env, nor fabricates validation originals.

Existing model_call_claims are resolved read-only from the exact Team binding.
Schema attempts must be 1..3, and a retry requires a persisted raw failed
model_calls original. Only received, fully persisted+closed HTTP429 permits
same-call byte-identical transport retry (60/120/240/480; Retry-After can
lengthen). Every dispatch, including timeout/5xx/stream interruption, reserves
an O_EXCL original BEFORE HTTP. 5xx/unknown results are never retried.
ModelClient max_attempts is forced to 1; capacity/thinking/JSON hidden second
dispatch is rejected, not passed off as a schema correction. Provider TTFT/
usage telemetry remains the unmodified client's evidence, never invented here.

Files: immutable BINDING.json, dispatch-NNN-reserved.json and one final event
per actual dispatch; model-quality-http-ledger/0589-v2 is atomically replaced
in place. Unknown/unclosed streams stay visible as incomplete and terminal.
install() restores original class methods in finally. Tests inject mock
transport/sleep/RO outcome readers only; normal defaults are real sources.
"""
from __future__ import annotations

from contextlib import contextmanager, closing, nullcontext
from contextvars import ContextVar
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import sys
from threading import RLock
import time
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import httpx

ACTIVE = ContextVar('quality_transport_0589_call', default=None)
DELAYS = (60, 120, 240, 480)
ROLES = {'A2': ('A2','CLOSE_REVIEW'), 'A3': ('A3','PLAN_REVIEW'),
         'A6': ('A6','PREMARKET'), 'PM': ('A3','PM_PREMARKET'), 'A4': ('A4','INTRADAY')}
COMMON = ('head','source_trade_date','execution_trade_date','quality_run_id',
          'provider_base_url','models','role_run_ids')


class QualityTransportStop(SystemExit):
    """Non-fallback stop: must not become a role's fake NO_ACTION."""


def _json(value):
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)


def _hash(value):
    return hashlib.sha256(value).hexdigest()


def _sha(value):
    return isinstance(value,str) and re.fullmatch('[0-9a-f]{64}',value) is not None


def _now(): return datetime.now(ZoneInfo('Asia/Shanghai'))


def retry_after_seconds(value, at):
    try:
        seconds=float(value)
        if math.isfinite(seconds) and seconds>=0:return seconds
    except (TypeError,ValueError,OverflowError):pass
    try:
        parsed=parsedate_to_datetime(value)
        if parsed.utcoffset() is None:parsed=parsed.replace(tzinfo=timezone.utc)
        return max(0.,(parsed-at).total_seconds())
    except (TypeError,ValueError,OverflowError):return 0.


def _exclusive(path, body):
    raw=_json(body).encode()
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'wb') as stream:
        stream.write(raw);stream.flush();os.fsync(stream.fileno())
    _sync_directory(Path(path).parent)
    return dict(path=str(path),sha256=_hash(raw))


def _sync_directory(directory):
    if hasattr(os,'O_DIRECTORY'):
        fd=os.open(directory,os.O_RDONLY|os.O_DIRECTORY)
        try:os.fsync(fd)
        finally:os.close(fd)


def _read_claim(settings, model, runtime, metadata):
    """Exact current CLAIMED row, no generated ID and zero business writes."""
    envelope=runtime['required_envelope']
    request_binding=_hash(_json(dict(run_id=envelope['run_id'],session=metadata['team_session'],
        role=metadata['stage'],input_hash=metadata['input_hash'],model=model,
        timeout_seconds=metadata['timeout_seconds'],max_output_tokens=metadata['max_output_tokens'],
        catalog_hash=runtime['catalog_sha256'])).encode())
    path=Path(settings.root)/'state/team_v2/model.db'
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=1)) as db:
        db.execute('PRAGMA query_only=ON')
        rows=db.execute("SELECT call_id FROM model_call_claims WHERE request_binding=? AND state='CLAIMED' LIMIT 2",(request_binding,)).fetchall()
    if len(rows)!=1:raise ValueError('EXACT_EXISTING_CLAIM_REQUIRED')
    return rows[0][0]


def _read_outcome(settings, call_id):
    path=Path(settings.root)/'state/team_v2/model.db'
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=1)) as db:
        db.execute('PRAGMA query_only=ON')
        rows=db.execute('SELECT validated,issues_json FROM model_calls WHERE call_id=? LIMIT 2',(call_id,)).fetchall()
    if len(rows)!=1:return None
    return dict(validated=rows[0][0],issues=json.loads(rows[0][1]))


class QualityTransportGuard:
    def __init__(self, *, binding, directory, deadline, resolve_call=None, outcome_reader=None,
                 now=_now, sleep=time.sleep, transport_factory=None, persist_rejection=None,
                 wire_context=nullcontext):
        if not isinstance(deadline,datetime) or deadline.utcoffset() is None:
            raise ValueError('QUALITY_AWARE_DEADLINE_REQUIRED')
        if not re.fullmatch('[0-9a-f]{40}',str(binding.get('head',''))):raise ValueError('QUALITY_HEAD_REQUIRED')
        for key in ('source_trade_date','execution_trade_date'):
            if date.fromisoformat(binding[key]).isoformat()!=binding[key]:raise ValueError('QUALITY_DATE_BINDING')
        if binding['execution_trade_date']<=binding['source_trade_date']:raise ValueError('QUALITY_DATE_ORDER')
        for name in ('models','role_run_ids'):
            if set(binding[name])!={'A2','A3','A6','PM'} or not all(isinstance(v,str) and v for v in binding[name].values()):
                raise ValueError('QUALITY_FOUR_ROLE_BINDING_REQUIRED')
        for key in ('quality_run_id','a4_model','a4_run_id'):
            if not isinstance(binding.get(key),str) or not binding[key] or len(binding[key])>256:
                raise ValueError('QUALITY_CHAIN_AND_A4_BINDING_REQUIRED')
        url=urlsplit(binding['provider_base_url'])
        if url.scheme!='https' or not url.hostname or url.username or url.password or url.query or url.fragment:
            raise ValueError('QUALITY_PROVIDER_ENDPOINT_INVALID')
        self.binding=json.loads(_json({key:binding[key] for key in (*COMMON,'a4_model','a4_run_id')}))
        self.endpoint=binding['provider_base_url'].rstrip('/')+'/chat/completions'
        self.deadline,self.now,self.sleep=deadline,now,sleep
        self.resolve_call=resolve_call or _read_claim
        self.outcome_reader=outcome_reader or _read_outcome
        self.transport_factory=transport_factory or (lambda:httpx.HTTPTransport(retries=0))
        # Caller firewall owns this narrow thread-local permission. It is
        # entered only after exact URL/model/request validation, around actual
        # provider transport IO, rejection read, and each success stream next/
        # close -- never around source acquisition, business decode, or sleep.
        self.wire_context=wire_context
        if persist_rejection is None:
            from ..pipeline.provider_rejection_evidence import persist_provider_rejection
            persist_rejection=persist_provider_rejection
        self.persist_rejection=persist_rejection
        self.directory=Path(directory).resolve()
        if self.directory.exists():raise ValueError('QUALITY_NEW_EXCLUSIVE_DIRECTORY_REQUIRED')
        self.directory.mkdir(parents=False,mode=0o700)
        self._lock=RLock();self.calls=[];self.dispatches=[];self.terminal=None;self._installed=False
        _exclusive(self.directory/'BINDING.json',dict(self.binding,deadline=deadline.isoformat()))
        self._save()

    def _save(self):
        state=dict(self.binding,schema_version='model-quality-http-ledger/0589-v2',
            capture_kind='LIVE_PROVIDER_DISPATCH',max_client_attempts=1,actual_http=len(self.dispatches),
            rate_limit_retries=sum(row['is_429_retry'] for row in self.dispatches),
            dispatches=self.dispatches,terminal_stop=self.terminal)
        raw=_json(state).encode();tmp=self.directory/'LEDGER.tmp'
        with tmp.open('wb') as stream:
            stream.write(raw);stream.flush();os.fsync(stream.fileno())
        os.replace(tmp,self.directory/'LEDGER.json')
        _sync_directory(self.directory)

    def snapshot(self):
        return json.loads((self.directory/'LEDGER.json').read_bytes())

    def stop(self,reason):
        self.mark_terminal(reason)
        raise QualityTransportStop('QUALITY_'+reason)

    def mark_terminal(self,reason):
        with self._lock:
            if self.terminal is None:
                self.terminal=dict(reason=reason,actual_wall=self.now().isoformat());self._save()

    def check_deadline(self, call=None, delay=0):
        at=self.now()
        if not isinstance(at,datetime) or at.utcoffset() is None:self.stop('ACTUAL_AWARE_WALL_REQUIRED')
        deadline=min(self.deadline,call['deadline']) if call else self.deadline
        if at+timedelta(seconds=delay)>=deadline:self.stop('ACTUAL_DEADLINE_NO_WAIT_OR_DISPATCH')

    def identity(self,settings,model,messages,metadata):
        self.check_deadline()
        if str(settings.model_base_url)!=self.binding['provider_base_url']:self.stop('SETTINGS_PROVIDER_MISMATCH')
        try:
            if len(messages)!=2 or messages[-1]['role']!='user' or not messages[-1]['content'].startswith('RUNTIME_INPUT\n'):
                raise ValueError('TEAM_MESSAGES_REQUIRED')
            runtime=json.loads(messages[-1]['content'].removeprefix('RUNTIME_INPUT\n'))
            envelope=runtime['required_envelope']
            pair=(metadata['stage'],metadata['team_session'])
            role=next(role for role,allowed in ROLES.items() if pair==allowed)
            expected_model=self.binding['a4_model'] if role=='A4' else self.binding['models'][role]
            expected_run=self.binding['a4_run_id'] if role=='A4' else self.binding['role_run_ids'][role]
            if (model!=expected_model or envelope['stage_id']!=pair[0] or runtime['session']!=pair[1]
                    or envelope['run_id']!=expected_run or envelope['model_name']!=model
                    or any(envelope[k]!=self.binding[k] for k in ('source_trade_date','execution_trade_date'))):
                raise ValueError('IDENTITY_BINDING_MISMATCH')
            if not all(_sha(value) for value in (metadata['input_hash'],metadata['prompt_hash'],runtime['catalog_sha256'])):
                raise ValueError('HASH_BINDING_REQUIRED')
            if (_hash(_json([dict(message) for message in messages]).encode())!=metadata['input_hash'] or
                    _hash(messages[0]['content'].encode())!=metadata['prompt_hash']):
                raise ValueError('ORIGINAL_MESSAGES_HASH_MISMATCH')
            attempt=runtime.get('correction_attempt',1)
            if type(attempt) is not int or not 1<=attempt<=3:raise ValueError('SCHEMA_ATTEMPT_REQUIRED')
            if attempt>1 and not runtime.get('validation_feedback',{}).get('validation_errors'):
                raise ValueError('SCHEMA_FEEDBACK_REQUIRED')
            call_id=self.resolve_call(settings,model,runtime,metadata)
            if not isinstance(call_id,str) or not call_id or len(call_id)>256:raise ValueError('EXISTING_CLAIM_REQUIRED')
            unit=_hash(_json(dict(quality_role=role,role=pair[0],session=pair[1],run_id=expected_run,
                source_trade_date=envelope['source_trade_date'],execution_trade_date=envelope['execution_trade_date'],
                catalog_hash=runtime['catalog_sha256'])).encode())
            timeout=min(float(settings.model_timeout_seconds),float(metadata['timeout_seconds']))
            if not math.isfinite(timeout) or timeout<=0:raise ValueError('TIMEOUT_REQUIRED')
        except (ValueError,KeyError,TypeError,StopIteration,sqlite3.Error):self.stop('EXACT_ORIGINAL_CALL_BINDING_REQUIRED')
        return dict(quality_role=role,role=pair[0],session=pair[1],run_id=expected_run,model=model,
            call_id=call_id,logical_batch_id=unit,schema_correction_attempt=attempt,
            input_hash=metadata['input_hash'],prompt_hash=metadata['prompt_hash'],catalog_hash=runtime['catalog_sha256'],
            deadline=self.now()+timedelta(seconds=timeout),settings=settings,dispatched=False)

    def reserve(self,call,request,*,retry):
        self.check_deadline(call)
        raw=request.read()
        body_sha=_hash(raw)
        identity=_hash(_json(dict(method=request.method,url=str(request.url),body_sha256=body_sha,
            headers_sha256=_hash(repr(list(request.headers.raw)).encode()))).encode())
        if request.method!='POST' or str(request.url)!=self.endpoint:self.stop('EXACT_PROVIDER_ENDPOINT_REQUIRED')
        try:
            if json.loads(raw)['model']!=call['model']:self.stop('WIRE_MODEL_MISMATCH')
        except (ValueError,KeyError,TypeError):self.stop('WIRE_MODEL_MISSING')
        with self._lock:
            if self.terminal:raise QualityTransportStop('QUALITY_'+self.terminal['reason'])
            if len(self.dispatches)>=24:self.stop('ACTUAL_HTTP24_STOP')
            same=[row for row in self.dispatches if row['call_id']==call['call_id']]
            if retry:
                if (not same or same[-1]['response_status']!=429 or not same[-1].get('response_complete')
                        or not same[-1].get('rejection_evidence') or same[-1].get('close_failure')):
                    self.stop('ONLY_FULL_CAPTURED_CLOSED_429_RETRY_ALLOWED')
                if len(same)>=5 or sum(row['is_429_retry'] for row in self.dispatches)>=12:self.stop('RETRY429_BUDGET_STOP')
                if body_sha!=same[0]['request_sha256'] or identity!=same[0]['request_identity_sha256']:
                    self.stop('EXACT_SAME_REQUEST_BYTES_REQUIRED')
            else:
                if call['dispatched'] or same or any(c['call_id']==call['call_id'] for c in self.calls):self.stop('HIDDEN_CLIENT_RETRY_OR_DUPLICATE_CALL')
                unit=[c for c in self.calls if c['logical_batch_id']==call['logical_batch_id']]
                role_calls=[c for c in self.calls if c['quality_role']==call['quality_role']]
                if call['quality_role']!='A3' and len(role_calls)>=3:self.stop('ROLE_TOTAL3_STOP')
                if len(unit)>=3 or call['schema_correction_attempt']!=len(unit)+1:self.stop('SCHEMA_PER_BATCH3_OR_ORDER_STOP')
                if unit:
                    try:outcome=self.outcome_reader(call['settings'],unit[-1]['call_id'])
                    except Exception:self.stop('PREVIOUS_SCHEMA_OUTCOME_UNKNOWN')
                    if not isinstance(outcome,dict) or outcome.get('validated')!=0 or not outcome.get('issues'):
                        self.stop('RETRY_REQUIRES_PERSISTED_RAW_SCHEMA_FAILURE')
                self.calls.append({key:call[key] for key in ('call_id','quality_role','logical_batch_id','schema_correction_attempt')})
                call['dispatched']=True
            row={key:call[key] for key in ('quality_role','role','session','run_id','model','call_id','logical_batch_id',
                'schema_correction_attempt','input_hash','prompt_hash','catalog_hash')}
            row.update(ordinal=len(self.dispatches)+1,request_sha256=body_sha,request_identity_sha256=identity,
                is_429_retry=retry,dispatch_reserved=True,client_attempt_index=1,http_attempt_index=len(same)+1,
                actual_wall=self.now().isoformat(),response_status=None,response_complete=False)
            # This immutable reservation is fsynced before any network dispatch.
            ref=_exclusive(self.directory/f"dispatch-{row['ordinal']:03d}-reserved.json",dict(self.binding,**row))
            row['reservation_original']=ref
            self.dispatches.append(row);self._save()
            return row['ordinal']

    def received(self,ordinal,status):
        with self._lock:
            self.dispatches[ordinal-1]['response_status']=status;self._save()

    def finalize(self,ordinal,**fields):
        with self._lock:
            row=self.dispatches[ordinal-1]
            if 'completion_original' in row:self.stop('DUPLICATE_DISPATCH_FINALIZATION')
            row.update(fields)
            row['completion_original']=_exclusive(self.directory/f'dispatch-{ordinal:03d}-terminal.json',dict(self.binding,**row))
            self._save()

    @contextmanager
    def install(self):
        from ..pipeline.model_client import OpenAICompatibleModelClient as Client
        if self._installed:raise ValueError('QUALITY_GUARD_ALREADY_INSTALLED')
        original_init,original_complete=Client.__init__,Client.complete
        if (original_init.__module__!='liangjian_funnel.pipeline.model_client' or
                original_complete.__module__!='liangjian_funnel.pipeline.model_client'):
            self.stop('PREEXISTING_CLIENT_INSTRUMENTATION_CONFLICT')
        guard=self
        def guarded_init(client,settings,**kwargs):
            if kwargs.get('transport') is not None:guard.stop('PREEXISTING_TRANSPORT_FORBIDDEN')
            kwargs.update(transport=_Transport(guard,settings),max_attempts=1)
            original_init(client,settings,**kwargs)
        def guarded_complete(client,model,messages,**metadata):
            if (not isinstance(client.transport,_Transport) or client.transport.guard is not guard or client.max_attempts!=1):
                guard.stop('PREEXISTING_OR_MUTATED_CLIENT_FORBIDDEN')
            call=guard.identity(client.settings,model,messages,metadata)
            token=ACTIVE.set(call)
            try:
                result=original_complete(client,model,messages,**metadata)
                guard.check_deadline(call)
                return result
            except BaseException:
                # Original client errors/TTFT/usage remain unmodified. No role
                # may dispatch another HTTP after a transport/JSON failure.
                if call['dispatched'] and guard.terminal is None:
                    with guard._lock:
                        guard.terminal=dict(reason='MODEL_OR_STREAM_CALL_FAILED_NO_RETRY',actual_wall=guard.now().isoformat())
                        guard._save()
                raise
            finally:ACTIVE.reset(token)
        Client.__init__,Client.complete=guarded_init,guarded_complete;self._installed=True
        try:yield self
        finally:
            Client.__init__,Client.complete=original_init,original_complete;self._installed=False


class _Stream(httpx.SyncByteStream):
    def __init__(self,guard,call,ordinal,stream):
        self.guard,self.call,self.ordinal,self.stream=guard,call,ordinal,stream
        self.done=False;self.finalized=False
    def __iter__(self):
        try:
            iterator=iter(self.stream)
            while True:
                self.guard.check_deadline(self.call)
                with self.guard.wire_context():
                    try:chunk=next(iterator)
                    except StopIteration:break
                self.guard.check_deadline(self.call)
                yield chunk
            self.done=True
        except BaseException:
            self._finish(response_complete=False,transport_failure='STREAM_INTERRUPTED_NO_RETRY')
            raise
    def _finish(self,**fields):
        if not self.finalized:
            self.guard.finalize(self.ordinal,**fields);self.finalized=True
    def close(self):
        prior_exception=sys.exc_info()[0] is not None
        try:
            with self.guard.wire_context():self.stream.close()
        except BaseException:
            self._finish(response_complete=False,close_failure='PROVIDER_STREAM_CLOSE_FAILED')
            self.guard.mark_terminal('PROVIDER_STREAM_CLOSE_FAILED')
            from ..pipeline.model_client import ModelClientError
            raise ModelClientError('PROVIDER_STREAM_CLOSE_FAILED',diagnostics={
                'provider_close_failure':['PROVIDER_STREAM_CLOSE_FAILED']}) from None
        if not self.done:
            self._finish(response_complete=False,transport_failure='STREAM_NOT_FULLY_CONSUMED')
            self.guard.mark_terminal('STREAM_NOT_FULLY_CONSUMED')
            # Preserve an already propagating original decoder/network error
            # so its unmodified ModelClient telemetry/classification persists.
            if not prior_exception:self.guard.stop('STREAM_NOT_FULLY_CONSUMED')
            return
        self._finish(response_complete=True)


class _Transport(httpx.BaseTransport):
    def __init__(self,guard,settings):self.guard,self.settings,self.inner=guard,settings,None
    def _close(self,response):
        errors=[]
        try:
            with self.guard.wire_context():response.close()
        except Exception:errors.append('PROVIDER_RESPONSE_CLOSE_FAILED')
        inner,self.inner=self.inner,None
        if inner is not None:
            try:
                with self.guard.wire_context():inner.close()
            except Exception:errors.append('PROVIDER_TRANSPORT_CLOSE_FAILED')
        return errors
    def handle_request(self,request):
        from ..pipeline.model_client import ModelHTTPError
        guard=self.guard;call=ACTIVE.get()
        if call is None:guard.stop('UNBOUND_HTTP_FORBIDDEN')
        for retry in range(5):
            ordinal=guard.reserve(call,request,retry=retry>0)
            remaining=max(.001,(min(guard.deadline,call['deadline'])-guard.now()).total_seconds())
            # Bound existing httpcore socket budgets without changing any
            # original messages/bytes/model/role kwargs or extending timeout.
            request.extensions['timeout']={key:min(float(value),remaining) if value is not None else remaining
                for key,value in request.extensions.get('timeout',dict(connect=remaining,read=remaining,write=remaining,pool=remaining)).items()}
            try:
                with guard.wire_context():
                    self.inner=guard.transport_factory()
                    response=self.inner.handle_request(request)
            except BaseException:
                guard.finalize(ordinal,response_complete=False,transport_failure='UNKNOWN_TRANSPORT_RESULT_NO_RETRY')
                guard.mark_terminal('UNKNOWN_TRANSPORT_RESULT_NO_RETRY')
                raise
            status=response.status_code
            guard.received(ordinal,status)
            if status!=429 and not 500<=status<=599:
                response.stream=_Stream(guard,call,ordinal,response.stream)
                return response
            receipt=None;failure=None
            try:
                with guard.wire_context():
                    evidence=guard.persist_rejection(response,settings=self.settings,model=call['model'],stage=call['role'],
                        session=call['session'],input_hash=call['input_hash'],prompt_hash=call['prompt_hash'],
                        attempt=len([r for r in guard.dispatches if r['call_id']==call['call_id']]),captured_at=guard.now(),all_headers=True,
                        deadline=time.monotonic()+max(0.,(min(guard.deadline,call['deadline'])-guard.now()).total_seconds()))
                receipt={key:evidence[key] for key in ('receipt_path','receipt_sha256','body_sha256','status_code')}
            except Exception:
                failure='PROVIDER_REJECTION_EVIDENCE_FAILURE'
            close_failure=self._close(response)
            guard.finalize(ordinal,response_complete=failure is None and not close_failure,
                rejection_evidence=receipt,evidence_failure=failure,close_failure=close_failure or None)
            reason='RATE_LIMIT_RETRY_EXHAUSTED' if status==429 else 'UPSTREAM_5XX_RETRY_EXHAUSTED'
            if failure or close_failure or status!=429 or retry==4:
                with guard._lock:
                    terminal_reason=failure or ('PROVIDER_CLOSE_FAILURE' if close_failure else reason)
                    guard.terminal=dict(reason=terminal_reason,actual_wall=guard.now().isoformat());guard._save()
                raise ModelHTTPError(reason,status_code=status,attempts=1,diagnostics={
                    'provider_rejection_evidence':[r['rejection_evidence'] for r in guard.dispatches
                        if r['call_id']==call['call_id'] and r.get('rejection_evidence')], 'provider_evidence_failure':failure,
                    'provider_close_failure':close_failure or None,'quality_actual_http':len(guard.dispatches)}) from None
            if len(guard.dispatches)>=24 or sum(row['is_429_retry'] for row in guard.dispatches)>=12:
                guard.stop('RETRY429_OR_HTTP24_BUDGET_STOP')
            delay=max(float(DELAYS[retry]),*(retry_after_seconds(value,guard.now()) for value in response.headers.get_list('retry-after')),0.)
            guard.check_deadline(call,delay)
            guard.sleep(delay)
            guard.check_deadline(call)
        raise AssertionError('UNREACHABLE')
    def close(self):
        inner,self.inner=self.inner,None
        if inner is not None:
            try:
                with self.guard.wire_context():inner.close()
            except Exception:
                self.guard.mark_terminal('PROVIDER_TRANSPORT_CLOSE_FAILED')
                from ..pipeline.model_client import ModelClientError
                raise ModelClientError('PROVIDER_TRANSPORT_CLOSE_FAILED',diagnostics={
                    'provider_close_failure':['PROVIDER_TRANSPORT_CLOSE_FAILED']}) from None
