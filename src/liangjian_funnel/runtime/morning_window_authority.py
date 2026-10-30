"""1197 scoped event-driven morning grant; default natural windows unchanged."""
from collections.abc import Mapping
from datetime import datetime
import re
from zoneinfo import ZoneInfo
TZ=ZoneInfo('Asia/Shanghai')
FIELDS={'authority','capture_authority','source_trade_date','trade_date','deadline_at','candidate_scope_id','candidate_sha256','candidate_revision'}
def morning_deadline(day,source_day,*,authorization=None,source=None,now=None):
    ordinary=datetime.fromisoformat(day+'T08:45:00+08:00')
    if authorization is None:return ordinary
    a=authorization
    if (not isinstance(a,Mapping)or set(a)!=FIELDS or a.get('authority')!='Claude1197'
        or a.get('capture_authority')!='Claude1195' or a.get('trade_date')!=day
        or day!='2026-10-30' or source_day!='2026-10-29' or a.get('source_trade_date')!=source_day
        or a.get('deadline_at')!='2026-10-30T14:00:00+08:00'
        or not isinstance(a.get('candidate_scope_id'),str)or re.fullmatch(r'[A-Za-z0-9_-]{1,40}',a['candidate_scope_id'])is None
        or not isinstance(a.get('candidate_sha256'),str)or re.fullmatch('[0-9a-f]{64}',a['candidate_sha256'])is None
        or type(a.get('candidate_revision'))is not int or a['candidate_revision']<1):
        raise ValueError('MORNING1197_AUTHORITY_INVALID')
    if source is not None and (source.get('source_namespace')!='NATURAL_MORNING_CANDIDATE'
        or source.get('source_trade_date')!=source_day or source.get('trade_date')!=day
        or any(source.get(k)!=a[k] for k in ('candidate_scope_id','candidate_sha256','candidate_revision'))):
        raise ValueError('MORNING1197_CANDIDATE_BINDING_INVALID')
    deadline=datetime.fromisoformat(a['deadline_at'])
    if now is not None and (not isinstance(now,datetime)or now.utcoffset()is None
        or now.astimezone(TZ).date().isoformat()!=day or now>=deadline):
        raise ValueError('MORNING1197_WINDOW_CLOSED')
    return deadline
