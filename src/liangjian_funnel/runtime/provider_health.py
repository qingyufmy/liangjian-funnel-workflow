"""Per-role HTTP availability; internal queue only, no inline alert requests."""
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
from zoneinfo import ZoneInfo
from .shared_json import exclusive_file, atomic_json


def queue_internal(settings, *, kind, source_id, text, now):
    from .team_v2_internal import InternalOutbox
    from .team_v2_store import TeamMeetingStore
    store=TeamMeetingStore(Path(settings.root)/'state/team_v2/team.db')
    return InternalOutbox(store).enqueue(account_id='local-paper',trade_date=now.date().isoformat(),
        kind=kind,source_id=source_id,observed_at=now,priority=5,
        body={'msg_type':'text','content':{'text':text}})


def observe_http(settings, *, model, role, status, now=None):
    now=now or datetime.now(ZoneInfo('Asia/Shanghai'));role=str(role or 'UNSPECIFIED')[:80]
    binding=hashlib.sha256((settings.model_base_url+'|'+model+'|'+role).encode()).hexdigest()
    path=Path(settings.root)/'state/provider-health.json';path.parent.mkdir(parents=True,exist_ok=True)
    with exclusive_file(path.with_suffix('.lock')):
        state=json.loads(path.read_bytes()) if path.exists() else {}
        row=state.get(binding,dict(consecutive_failures=0,episode=0,alerted=False))
        failed=400<=status<=599
        if failed and not row['consecutive_failures']:row.update(episode=row['episode']+1,alerted=False)
        row.update(consecutive_failures=row['consecutive_failures']+1 if failed else 0,
            role=role,model=model,status_code=status,observed_at=now.isoformat())
        if failed and (status==429 or row['consecutive_failures']>=2) and not row['alerted']:
            queue_internal(settings,kind='PROVIDER_UNAVAILABLE',source_id=binding+':'+str(row['episode']),
                text='模型服务不可用：角色 '+role+'；状态异常；请检查供应商可用性。',now=now)
            row['alerted']=True
        state[binding]=row;atomic_json(path,state)
        return row


def run_daily_availability(settings, *, client=None, now=None):
    """Once/day/model, durable attempt before HTTP. No retries or model changes."""
    from ..pipeline.model_client import OpenAICompatibleModelClient,ModelClientError
    from .integrity import verify_integrity
    now=now or datetime.now(ZoneInfo('Asia/Shanghai'))
    if (now.hour,now.minute)!=(8,0):raise ValueError('PROVIDER_PROBE_OUTSIDE_0800')
    integrity=verify_integrity(settings.state_db_path)
    root=Path(settings.root)/'state/provider-probes'/now.date().isoformat();root.mkdir(parents=True,exist_ok=True)
    results=[]
    models=dict.fromkeys((*settings.research_models,settings.review_model,settings.monitor_model,settings.team_intraday_model))
    api=client or OpenAICompatibleModelClient(settings,max_attempts=1)
    for model in models:
        pin=hashlib.sha256((settings.model_base_url+'|'+model).encode()).hexdigest()
        try:
            with (root/(pin+'.attempt')).open('x') as stream:
                stream.write(now.isoformat());stream.flush();os.fsync(stream.fileno())
        except FileExistsError:
            receipt=root/(pin+'.json')
            row=json.loads(receipt.read_bytes()) if receipt.exists() else dict(model=model,status='UNKNOWN_INTERRUPTED')
            results.append(dict(row,reused=True));continue
        try:
            result=api.complete(model,[{'role':'user','content':'Return only JSON: {"ok":true}'}],
                stage='AVAILABILITY',timeout_seconds=20,max_output_tokens=128)
            row=dict(model=model,status='PASS' if result.output=={'ok':True} else 'INVALID_RESPONSE',
                latency_ms=result.latency_ms,stream=result.provider_stream_telemetry)
        except ModelClientError as exc:
            row=dict(model=model,status='FAILED',reason_code=exc.reason_code,status_code=exc.status_code)
        atomic_json(root/(pin+'.json'),row);results.append(row)
    return dict(status='READY' if all(r['status']=='PASS' for r in results) else 'DEGRADED',
        integrity=integrity,models=results)
