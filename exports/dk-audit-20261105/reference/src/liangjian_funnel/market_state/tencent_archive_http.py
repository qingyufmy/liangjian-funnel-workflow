"""M158 daily close archive budget; DK1 probe accounting stays immutable.

Both daily and minute source reservations share MKT30/day. Prior canonical
close captures are inherited for same-day counting and duplicate prevention;
the old 20-probe cap is retained only as a retired audit number.
"""
from datetime import datetime
import hashlib
from pathlib import Path
import time

from .bars15m import TZ
from .research_http import ProbeLedger


class DailyArchiveLedger(ProbeLedger):
    def __init__(self,path,*,parent_path,clock=lambda:datetime.now(TZ),sleep=time.sleep):
        super().__init__(Path(path),clock=clock,sleep=sleep)
        self.parent_path=Path(parent_path)
        if any(p.is_symlink() for p in (self.parent_path,*self.parent_path.parents)):
            raise ValueError('M158_PARENT_SCOPE_INVALID')
        parent=ProbeLedger(self.parent_path).read()
        if not parent or parent[0]['event'].get('kind')!='M28_ORIGIN':raise ValueError('M158_CANONICAL_PARENT_REQUIRED')
        attempts=[r['event'] for r in parent if r['event']['kind']=='RESERVED']
        inherited=[r for r in attempts if r['source'] in {'MKT_TENCENT','DK_TENCENT'}]
        invoked=[r['event'] for r in parent if r['event']['kind']=='TRANSPORT_INVOKED']
        if invoked and not any(r['event'].get('reservation')==invoked[-1]['reservation']
                and r['event']['kind'] in {'STREAM_CLOSED','TRANSPORT_EXCEPTION','REDIRECT_REJECTED'} for r in parent):
            raise ValueError('M158_RETIRED_PARENT_STILL_STREAMING')
        terminal=[r['event']['at'] for r in parent if r['event']['kind'] in {'STREAM_CLOSED','TRANSPORT_EXCEPTION','REDIRECT_REJECTED'}]
        origin=dict(kind='M158_ORIGIN',decision='liangjian#M158',mkt_daily_limit=30,
            parent_sha256=hashlib.sha256(self.parent_path.read_bytes()).hexdigest(),
            retired_dk_probe_count=parent[0]['event']['prior_dk_tencent']+sum(r['source']=='DK_TENCENT' for r in attempts),
            em_retired=parent[0]['event']['prior_em_retired'],
            last_parent_terminal_at=terminal[-1] if terminal else None,
            inherited_close_captures=[dict(source=r['source'],request_key=r['request_key'],at=r['at']) for r in inherited])
        with self.locked():
            rows=self.read()
            if not rows:self._append(origin,rows)
            elif rows[0]['event']!=origin:raise ValueError('M158_RETIRED_PARENT_CHANGED')

    def summary(self):
        rows=self.read();origin=rows[0]['event'];day=self.clock().astimezone(TZ).date().isoformat()
        calls=origin['inherited_close_captures']+[r['event'] for r in rows if r['event']['kind']=='RESERVED']
        return dict(mkt_today=sum(r['source'] in {'MKT_TENCENT','DK_TENCENT'} and datetime.fromisoformat(r['at']).astimezone(TZ).date().isoformat()==day for r in calls),
            retired_dk_probe_count=origin['retired_dk_probe_count'],em_retired=origin['em_retired'],
            controlled_model=sum(r['source']=='CONTROLLED_MODEL' for r in calls))

    def reserve(self,source,*,request_key=None):
        if source not in {'MKT_TENCENT','DK_TENCENT','CONTROLLED_MODEL'}:raise ValueError('M158_TENCENT_ARCHIVE_ONLY')
        with self.locked():
            rows=self.read();origin=rows[0]['event']
            if any(r['event']['kind'] in {'SOURCE_HTTP_FAILED','SOURCE_TRANSPORT_FAILED','SOURCE_BUSINESS_FAILED'} for r in rows):
                raise ValueError('M158_FIRST_ERROR_STOPPED')
            own=[r['event'] for r in rows if r['event']['kind']=='RESERVED']
            previous=origin['inherited_close_captures']+own
            if request_key is None or any(r['request_key']==request_key for r in previous):
                raise ValueError('M158_DUPLICATE_REQUEST_NO_RETRY')
            counts=self.summary()
            if (source!='CONTROLLED_MODEL' and counts['mkt_today']>=30 or source=='CONTROLLED_MODEL' and counts['controlled_model']>=1):
                raise ValueError('M158_DAILY_BUDGET_EXHAUSTED')
            now=self.clock()
            if now.utcoffset() is None:raise ValueError('M158_AWARE_CLOCK_REQUIRED')
            invoked=[r['event'] for r in rows if r['event']['kind']=='TRANSPORT_INVOKED']
            reference=None
            if invoked:
                terminal=[r['event'] for r in rows if r['event'].get('reservation')==invoked[-1]['reservation']
                    and r['event']['kind'] in {'STREAM_CLOSED','TRANSPORT_EXCEPTION','REDIRECT_REJECTED'}]
                if not terminal:raise ValueError('M158_PREVIOUS_STREAM_NOT_CLOSED')
                reference=datetime.fromisoformat(terminal[-1]['at'])
            elif origin['last_parent_terminal_at']:reference=datetime.fromisoformat(origin['last_parent_terminal_at'])
            elif previous:reference=max(datetime.fromisoformat(r['at']) for r in previous)
            if reference is not None:
                gap=(now-reference).total_seconds()
                if gap<0:raise ValueError('M158_CLOCK_MOVED_BACKWARDS')
                if gap<1:
                    self.sleep(1-gap);now=self.clock()
                    if (now-reference).total_seconds()<1:raise ValueError('M158_TERMINAL_SPACING_FAILED')
            return self._append(dict(kind='RESERVED',source=source,request_key=request_key,at=now.isoformat()),rows)['seq']
