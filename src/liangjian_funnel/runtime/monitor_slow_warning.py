"""Consume the parent's first real 20s warning through the existing outbox."""
from datetime import datetime
import json
from pathlib import Path
import re


def project_slow_warning(settings, *, now):
    path=Path(settings.root)/'outputs/node/slow-monitor'/(now.date().isoformat()+'.json')
    if not path.is_file():return dict(status='NO_WARNING')
    if path.is_symlink() or path.stat().st_size>4096:raise ValueError('MONITOR_WARNING_SCOPE_INVALID')
    body=json.loads(path.read_bytes())
    observed=datetime.fromisoformat(body['observed_at'])
    started=datetime.fromisoformat(body['started_at'])
    if (body.get('schema')!='monitor-slow-warning/1' or observed.utcoffset() is None
            or started.utcoffset() is None or observed>now or observed.astimezone(now.tzinfo).date()!=now.date()
            or body.get('threshold_ms')!=20000 or (observed-started).total_seconds()<20
            or not isinstance(body.get('run_id'),str) or not re.fullmatch(r'monitor-[A-Za-z0-9:-]{1,100}',body['run_id'])):
        raise ValueError('MONITOR_WARNING_BINDING_INVALID')
    from .provider_health import queue_internal
    alert=queue_internal(settings,kind='MONITOR_ROUND_OVER_20S',source_id=now.date().isoformat(),
        text='A4单轮运行超过20秒；任务尚在运行，此告警不等于超时终止。运行='+body['run_id']+
            '；请核原始逐段计时，保留原50秒快照和58秒父截止。',now=now)
    return dict(status='QUEUED_DEDUPLICATED',run_id=body['run_id'],internal_alert=alert)
