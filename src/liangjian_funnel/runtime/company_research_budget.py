"""One approved research-only HTTP budget across A1/A2/A3/A5 and restarts."""
from contextlib import contextmanager
from datetime import datetime,timedelta
from pathlib import Path
from threading import RLock
import hashlib,json,os,time

import httpx


class CompanyBudgetStop(RuntimeError):pass


class CompanyResearchBudget(httpx.BaseTransport):
    def __init__(self,directory,*,binding,transport=None,now=None,sleep=time.sleep,amendment_path=None):
        self.directory=Path(directory);self.directory.mkdir(parents=True,exist_ok=True)
        self.path=self.directory/'HTTP_LEDGER.json';self.binding=dict(binding)
        self.now=now or (lambda:datetime.now().astimezone());self.sleep_impl=sleep
        self.transport=transport or httpx.HTTPTransport(retries=0);self.lock=RLock();self.role_name=None;self.logical=None
        self.deadline=datetime.fromisoformat(binding['deadline'])
        if self.deadline.utcoffset() is None or binding['approved'] is not True:
            raise ValueError('COMPANY_BUDGET_EXPLICIT_APPROVAL_REQUIRED')
        if binding['caps']!={'A1':4,'A2_A3':6,'A5':2,'TOTAL':12}:
            raise ValueError('COMPANY_BUDGET_SCOPE_MISMATCH')
        if self.path.exists():
            self.state=json.loads(self.path.read_bytes())
            if self.state['binding']!=self.binding:raise ValueError('COMPANY_BUDGET_BINDING_CHANGED')
            if any(row.get('status') is None or (row.get('status')==200 and not row.get('stream_exhausted'))
                    for row in self.state['dispatches']):
                raise CompanyBudgetStop('COMPANY_UNKNOWN_PRIOR_DISPATCH_NO_RETRY')
        else:
            self.state=dict(schema='company-research-http-budget/1',binding=self.binding,dispatches=[],terminal_stop=None)
            self._save()
        self.effective_caps=dict(self.binding['caps'])
        amendments=self.state.get('budget_amendments',[])
        self.only_role=None
        if not isinstance(amendments,list) or len(amendments)>2:raise ValueError('COMPANY_AMENDMENT_HISTORY_INVALID')
        for saved in amendments:
            path=self.directory/saved['file'];value,digest=self._read_amendment(path)
            if digest!=saved['sha256']:raise ValueError('COMPANY_AMENDMENT_ORIGINAL_CHANGED')
            self.effective_caps=dict(value['caps'])
            if value.get('request')=='COMPANY_A5_FINAL_SEMANTIC_REQUEST_20261025.json':
                if len(amendments)!=2 or saved is not amendments[-1] or value['parent_amendment_sha256']!=amendments[0]['sha256']:
                    raise ValueError('COMPANY_FINAL_A5_AMENDMENT_CHAIN_INVALID')
                self.only_role='A5';self.deadline=datetime.fromisoformat(value['deadline'])
        if amendment_path is not None:
            path=Path(amendment_path);value,digest=self._read_amendment(path)
            known=any(saved['sha256']==digest for saved in amendments)
            final_a5=value.get('request')=='COMPANY_A5_FINAL_SEMANTIC_REQUEST_20261025.json'
            if known and amendments[-1]['sha256']!=digest:raise ValueError('COMPANY_AMENDMENT_ALREADY_SUPERSEDED')
            if not known:
                if amendments and not (final_a5 and len(amendments)==1 and value['parent_amendment_sha256']==amendments[0]['sha256']):
                    raise ValueError('COMPANY_AMENDMENT_ALREADY_FROZEN')
                if final_a5:
                    counts={role:sum(r['role']==role for r in self.state['dispatches']) for role in ('A1','A2','A3','A5')}
                    parent_hash=hashlib.sha256(self.path.read_bytes()).hexdigest()
                    if (len(amendments)!=1 or self.effective_caps!=dict(A1=4,A2_A3=6,A5=3,TOTAL=12)
                            or counts!=dict(A1=4,A2=5,A3=0,A5=3) or len(self.state['dispatches'])!=12
                            or self.state['terminal_stop'] is not None or value['parent_ledger_sha256']!=parent_hash
                            or any(r.get('status')!=200 or not r.get('stream_exhausted') for r in self.state['dispatches'])):
                        raise ValueError('COMPANY_FINAL_A5_EXACT_TWELVE_COMPLETED_REQUIRED')
                if value.get('request')=='COMPANY_CURRENT_AUDIT_REALLOCATION_REQUEST_20261025.json':
                    counts={role:sum(r['role']==role for r in self.state['dispatches']) for role in ('A1','A2','A5')}
                    if len(self.state['dispatches'])!=10 or counts!=dict(A1=4,A2=4,A5=2):
                        raise ValueError('COMPANY_CURRENT_AUDIT_EXPECTED_TEN_CALLS_REQUIRED')
                self.state['budget_amendments']=[*amendments,dict(file=path.name,sha256=digest,
                    applied_at=self.now().isoformat(),dispatches_already_used=len(self.state['dispatches']))]
                self._save()
            self.effective_caps=dict(value['caps'])
            if final_a5:self.only_role='A5';self.deadline=datetime.fromisoformat(value['deadline'])

    def _read_amendment(self,path):
        if path.resolve().parent!=self.directory.resolve():raise ValueError('COMPANY_AMENDMENT_OWNED_ORIGINAL_REQUIRED')
        raw=path.read_bytes();value=json.loads(raw)
        original_a1=(value.get('request')=='ROBOT_COMPANY_A1_BUDGET_REALLOCATION_REQUEST_20261025.json'
            and value.get('caps')=={'A1':8,'A2_A3':6,'A5':2,'TOTAL':12})
        current_audit=(value.get('request')=='COMPANY_CURRENT_AUDIT_REALLOCATION_REQUEST_20261025.json'
            and value.get('caps')=={'A1':4,'A2_A3':6,'A5':3,'TOTAL':12}
            and value.get('original_inputs_sha256')==self.binding.get('inputs_sha256')
            and bool(self.binding.get('inputs_sha256')) and value.get('dispatches_already_used')==10)
        final_a5=(value.get('request')=='COMPANY_A5_FINAL_SEMANTIC_REQUEST_20261025.json'
            and value.get('caps')=={'A1':4,'A2_A3':6,'A5':4,'TOTAL':13}
            and value.get('original_inputs_sha256')==self.binding.get('inputs_sha256') and bool(self.binding.get('inputs_sha256'))
            and value.get('dispatches_already_used')==12 and value.get('exact_role')=='A5' and value.get('additional_http')==1
            and value.get('no_retry') is True and value.get('original_deadline')==self.binding['deadline']
            and all(isinstance(value.get(k),str) and len(value[k])==64 and set(value[k])<=set('0123456789abcdef')
                for k in ('parent_amendment_sha256','parent_ledger_sha256','request_sha256')))
        deadline_same=value.get('deadline')==self.binding['deadline']
        deadline_renewed=False
        if final_a5 and not deadline_same:
            try:
                renewed=datetime.fromisoformat(value['deadline'])
                deadline_renewed=(value.get('deadline_extension_approved') is True and
                    isinstance(value.get('deadline_human_answer'),str) and bool(value['deadline_human_answer'].strip()) and
                    renewed.utcoffset() is not None and renewed>datetime.fromisoformat(self.binding['deadline']) and
                    renewed.date()==datetime.fromisoformat(self.binding['deadline']).date())
            except (ValueError,TypeError,KeyError):pass
        if (value.get('schema')!='company-research-budget-amendment/1' or value.get('approved') is not True
                or value.get('source')!='ACTUAL_HUMAN_REPLY_IN_THIS_THREAD'
                or not isinstance(value.get('human_answer'),str) or not value['human_answer'].strip()
                or not (original_a1 or current_audit or final_a5)
                or value.get('original_approval_sha256')!=self.binding.get('approval_sha256')
                or not self.binding.get('approval_sha256') or not (deadline_same or final_a5 and deadline_renewed)
                or value.get('symbols')!=['003021.SZ','300296.SZ']):
            raise ValueError('COMPANY_AMENDMENT_EXPLICIT_SCOPE_REQUIRED')
        return value,hashlib.sha256(raw).hexdigest()

    def _save(self):
        raw=json.dumps(self.state,ensure_ascii=False,sort_keys=True,indent=2).encode()
        temp=self.path.with_suffix('.tmp')
        with temp.open('wb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
        os.replace(temp,self.path)

    def check(self,*,delay=0):
        if self.state['terminal_stop']:raise CompanyBudgetStop(self.state['terminal_stop'])
        if self.now()+timedelta(seconds=delay)>=self.deadline:self.stop('COMPANY_DEADLINE_STOP')

    def stop(self,reason):
        self.state['terminal_stop']=self.state['terminal_stop'] or reason;self._save()
        raise CompanyBudgetStop(self.state['terminal_stop'])

    @contextmanager
    def phase(self,role):
        if role not in {'A1','A2','A3','A5'} or self.role_name is not None:raise ValueError('COMPANY_HTTP_ROLE_INVALID')
        if self.only_role is not None and role!=self.only_role:raise ValueError('COMPANY_FINAL_A5_ONLY')
        self.check();self.role_name=role
        try:yield self
        finally:self.role_name=None

    @contextmanager
    def logical_call(self):
        if self.logical is not None:raise ValueError('COMPANY_LOGICAL_CALL_NESTED')
        self.check();self.logical=len(self.state['dispatches'])+1
        try:yield
        except (httpx.HTTPError,):self.stop('COMPANY_TRANSPORT_OR_STREAM_FAILURE')
        finally:self.logical=None

    def backoff_sleep(self,requested):
        with self.lock:
            self.check()
            if self.only_role=='A5' and len(self.state['dispatches'])>=13:self.stop('COMPANY_FINAL_A5_NO_RETRY')
            current=[r for r in self.state['dispatches'] if r['logical_call']==self.logical]
            count=sum(r.get('status')==429 for r in current)
            delay=(60,120,240,480)[min(count-1,3)] if count else requested
            self.check(delay=delay)
        self.sleep_impl(delay)
        self.check()

    def handle_request(self,request):
        with self.lock:
            self.check()
            if self.role_name not in {'A1','A2','A3','A5'} or self.logical is None:
                self.stop('COMPANY_UNSCOPED_HTTP_FORBIDDEN')
            body=json.loads(request.read())
            if body.get('model')!=self.binding['model'] or str(request.url)!=self.binding['chat_url']:
                self.stop('COMPANY_MODEL_OR_PROVIDER_CHANGED')
            rows=self.state['dispatches'];group='A2_A3' if self.role_name in {'A2','A3'} else self.role_name
            count=sum(r['budget_group']==group for r in rows)
            if len(rows)>=self.effective_caps['TOTAL'] or count>=self.effective_caps[group]:
                self.stop('COMPANY_APPROVED_HTTP_CAP_REACHED')
            if sum(r['logical_call']==self.logical for r in rows)>=5:self.stop('COMPANY_PER_CALL_RETRY_CAP')
            row=dict(ordinal=len(rows)+1,role=self.role_name,budget_group=group,logical_call=self.logical,
                at=self.now().isoformat(),request_sha256=hashlib.sha256(request.content).hexdigest(),status=None)
            rows.append(row);self._save()
            with (self.directory/('request-%03d.json'%row['ordinal'])).open('xb') as f:f.write(request.content)
        try:response=self.transport.handle_request(request)
        except Exception:
            with self.lock:self.state['terminal_stop']='COMPANY_TRANSPORT_FAILURE';self._save()
            raise
        with self.lock:
            row['status']=response.status_code
            if response.status_code==403 or response.status_code>=500:
                self.state['terminal_stop']='COMPANY_HTTP_'+str(response.status_code)+'_STOP'
            if sum(r.get('status')==429 for r in rows)>=12:
                self.state['terminal_stop']='COMPANY_TWELFTH_429_STOP'
            self._save()
        owner=self;stream=response.stream
        class RecordedStream(httpx.SyncByteStream):
            def __iter__(self):
                digest=hashlib.sha256();size=0
                original=(owner.directory/('response-%03d.bin'%row['ordinal'])).open('xb')
                try:
                    for chunk in stream:
                        # An error header sets terminal_stop immediately,
                        # but its original response body must still be read.
                        if owner.now()>=owner.deadline:owner.stop('COMPANY_DEADLINE_STOP')
                        original.write(chunk);digest.update(chunk);size+=len(chunk);yield chunk
                    row['stream_exhausted']=True
                except Exception:
                    owner.state['terminal_stop']='COMPANY_STREAM_INTERRUPTED';raise
                finally:
                    original.flush();os.fsync(original.fileno());original.close()
                    with owner.lock:
                        row['response_bytes']=size;row['response_bytes_sha256']=digest.hexdigest();owner._save()
            def close(self):stream.close()
        return httpx.Response(response.status_code,headers=response.headers,stream=RecordedStream(),request=request,
            extensions=response.extensions)

    def close(self):
        # ModelClient borrows this transport for successive logical calls.
        # Closing one httpx.Client must not close the shared budget/pool.
        pass

    def finish(self):self.transport.close()


class BudgetedCompanyModelClient:
    def __init__(self,client,budget):self.client=client;self.budget=budget
    def complete(self,*args,**kwargs):
        from ..pipeline.model_client import ModelClientError,ModelNetworkError
        with self.budget.logical_call():
            remaining=(self.budget.deadline-self.budget.now()).total_seconds()
            kwargs['timeout_seconds']=min(float(kwargs.get('timeout_seconds') or remaining),remaining)
            try:
                result=self.client.complete(*args,**kwargs)
                # Preserve parsed responses before semantic validation, even
                # when the normal pipeline rejects their schema or contents.
                path=self.budget.directory/('model-result-%03d.json'%self.budget.logical)
                with path.open('x',encoding='utf-8') as f:json.dump(dict(model=result.model,
                    input_hash=result.input_hash,prompt_hash=result.prompt_hash,output=result.output),f,
                    ensure_ascii=False,indent=2,allow_nan=False)
                return result
            except ModelClientError as exc:
                # "STREAM" also occurs in local telemetry persistence errors.
                # Preserve that local failure without claiming the completed
                # provider stream broke, or erasing its stable reason code.
                with self.budget.lock:
                    self.budget.state.setdefault('client_failures',[]).append(dict(
                        logical_call=self.budget.logical,reason_code=exc.reason_code,
                        exception_class=type(exc).__name__,at=self.budget.now().isoformat()))
                    self.budget._save()
                protocol=exc.reason_code.startswith('STREAM_') or exc.reason_code=='FINPOINTS_STREAM_REQUIRED'
                if isinstance(exc,ModelNetworkError) or protocol or any(
                        word in exc.reason_code for word in ('NETWORK','TIMEOUT','DEADLINE')):
                    self.budget.stop('COMPANY_CLIENT_TRANSPORT_FAILURE')
                raise
