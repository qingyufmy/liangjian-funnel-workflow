"""Once-per-day formal rule preflight, written only to owned Team state.

Original reviewed bytes are approval pins, not today's observations. The live
edge fetches the same three formal documents before 09:26; changed versions
remain blocked. A claimed/interrupted attempt is never silently re-requested.
"""
import json
import os
from pathlib import Path
from datetime import datetime,time
from zoneinfo import ZoneInfo

from .shadow_rule_preflight import load_approved_rule_archive,fetch_rule_preflight
from .shadow_price_limits import validate_shadow_rule_receipt
from .team_v2_sources import stamp
from .team_v2_store import canonical


def _write_once(path,body):
    path.parent.mkdir(parents=True,exist_ok=True)
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'w',encoding='utf-8',newline='\n') as output:
        output.write(canonical(body));output.flush();os.fsync(output.fileno())


def prepare_daily_rule_source(settings,*,clock=None,calendar,fetch_response=None,budget_seconds=10):
    clock=clock or (lambda:datetime.now(ZoneInfo('Asia/Shanghai')));now=stamp(clock());day=now.date().isoformat()
    def limited(reason,**more):return dict(status='DATA_LIMITED',reason_code=reason,trade_date=day,
        source_requests_started=0,**more)
    if calendar.is_trading_day(now.date()) is not True:return limited('NON_TRADING_DAY')
    if now.time().replace(tzinfo=None)>=time(9,26):return limited('RULE_PREFLIGHT_WINDOW_CLOSED')
    root=Path(settings.root).resolve();directory=Path(settings.root)/'state/team_v2/rules'
    if directory.resolve()!=root/'state/team_v2/rules':raise ValueError('TEAM_RULE_DIRECTORY_INVALID')
    target=directory/(day+'.json');attempt=directory/(day+'.attempt.json');claim=directory/(day+'.claim.json')
    if any(path.is_symlink() for path in (target,attempt,claim)):raise ValueError('TEAM_RULE_FILE_SCOPE_INVALID')
    try:approved=load_approved_rule_archive(root/'artifacts/wp5-20261010/official-rules-20261010')
    except Exception:return limited('RULE_APPROVED_ARCHIVE_UNAVAILABLE')
    def existing():
        try:
            if target.stat().st_size>16*1024*1024:raise ValueError
            receipt=json.loads(target.read_text(encoding='utf-8'))
            checked=validate_shadow_rule_receipt(approved,receipt,at=now)
            if checked['status']!='RULE_VERSION_CONFIRMED':raise ValueError
            return dict(status='RULE_VERSION_CONFIRMED',trade_date=day,source_requests_started=0,
                reused=True,receipt_path=str(target),receipt_sha256=receipt['receipt_sha256'])
        except Exception:return limited('RULE_EXISTING_RECEIPT_UNPROVEN')
    if target.exists():return existing()
    if attempt.exists():
        try:return {**json.loads(attempt.read_text(encoding='utf-8')),'reused':True,'source_requests_started':0}
        except Exception:return limited('RULE_EXISTING_ATTEMPT_UNPROVEN')
    try:_write_once(claim,dict(trade_date=day,started_at=now.isoformat()))
    except FileExistsError:return limited('RULE_PREFLIGHT_ALREADY_CLAIMED')
    # This is the only network edge. The validator still checks original URLs,
    # bytes, declared arrivals, exact approved versions and its own hard budget.
    def fetch(url,remaining):
        import httpx
        with httpx.Client(trust_env=False,follow_redirects=False,timeout=remaining) as client:
            return client.get(url)
    receipt=fetch_rule_preflight(approved,trade_date=day,fetch_response=fetch_response or fetch,
        clock=clock,budget_seconds=budget_seconds)
    count=receipt['source_requests_started']
    if receipt['status']=='RULE_VERSION_CONFIRMED':
        # Revalidate using completion time; never substitute today's clock for
        # a stored old receipt or synthesize the approved archive as an arrival.
        completed=stamp(clock())
        checked=validate_shadow_rule_receipt(approved,receipt,at=completed)
        if checked['status']=='RULE_VERSION_CONFIRMED':
            _write_once(target,receipt)
            result=dict(status='RULE_VERSION_CONFIRMED',trade_date=day,source_requests_started=count,
                reused=False,receipt_path=str(target),receipt_sha256=receipt['receipt_sha256'])
        else:result=dict(status='DATA_LIMITED',reason_code='RULE_COMPLETED_RECEIPT_UNPROVEN',
            trade_date=day,source_requests_started=count)
    else:result=dict(status='DATA_LIMITED',reason_code='RULE_PREFLIGHT_FAILED',
        reason_codes=receipt['reason_codes'],trade_date=day,source_requests_started=count)
    _write_once(attempt,result)
    return result
