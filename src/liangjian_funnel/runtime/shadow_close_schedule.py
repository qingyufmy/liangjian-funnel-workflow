"""Independent W3 timing contract. No A5 scheduler/store/model/notification calls.

The owner supplies read-only observations and independent freeze/report writers.
Source references and SHA are bindings, not proof of authentic A5 acquisition.
This coordinator is not automatically wired to a production timer.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, time
import re
from zoneinfo import ZoneInfo

_TZ = ZoneInfo('Asia/Shanghai')
_SHA = re.compile('[0-9a-f]{64}')


class ShadowCloseCoordinator:
    def __init__(self, trade_date, *, freeze, build_final):
        self.day = date.fromisoformat(trade_date)
        self.freeze, self.build_final = freeze, build_final
        self.cutoff = datetime.combine(self.day,time(15),_TZ)
        self.draft_due = datetime.combine(self.day,time(15,30),_TZ)
        self.deadline = datetime.combine(self.day,time(16,45),_TZ)
        self.draft = None
        self.draft_status = None
        self.last_clock = None
        self.terminal = None

    def _a5_ready(self, value, now):
        if not isinstance(value,dict):return False
        if (value.get('trade_date')!=self.day.isoformat()
                or value.get('slot')!='A5_POST_CLOSE_1600'
                or value.get('status')!='SUCCEEDED'
                or not value.get('source_ref')
                or not isinstance(value.get('source_sha256'),str)
                or _SHA.fullmatch(value['source_sha256']) is None):return False
        try:
            end=datetime.fromisoformat(value['completed_at'])
            return (end.tzinfo is not None
                and datetime.combine(self.day,time(16),_TZ)<=end<=now)
        except (KeyError,ValueError,TypeError):return False

    def poll(self, observed_at, *, a5=None):
        if not isinstance(observed_at,datetime) or observed_at.tzinfo is None:
            raise ValueError('AWARE_SHADOW_CLOSE_CLOCK_REQUIRED')
        now=observed_at.astimezone(_TZ)
        if now.date()!=self.day:raise ValueError('SHADOW_CLOSE_DAY_MISMATCH')
        if self.last_clock is not None and now<self.last_clock:
            raise ValueError('SHADOW_CLOSE_CLOCK_REGRESSED')
        self.last_clock=now
        if self.terminal is not None:return deepcopy(self.terminal)
        if now<self.draft_due:return {'status':'WAIT_DRAFT'}
        if self.draft is None:
            try:
                frozen=self.freeze(trade_date=self.day.isoformat(),as_of=now,market_cutoff=self.cutoff)
                if (not isinstance(frozen,dict) or frozen.get('trade_date')!=self.day.isoformat()
                        or frozen.get('market_cutoff')!=self.cutoff.isoformat()
                        or not frozen.get('source_ref') or not isinstance(frozen.get('sha256'),str)
                        or _SHA.fullmatch(frozen['sha256']) is None):
                    raise ValueError('SHADOW_DRAFT_BINDING_INVALID')
                self.draft=deepcopy(frozen)
                self.draft_status='ON_TIME_DRAFT' if now==self.draft_due else 'LATE_DRAFT'
            except Exception as exc:
                self.terminal={'status':'SHADOW_DRAFT_FAILED','failure_type':type(exc).__name__,
                    'observed_at':now.isoformat(),'production_mutation':False}
                return deepcopy(self.terminal)
        # A later read cannot establish that A5 was observable at the fence.
        ready=now<=self.deadline and self._a5_ready(a5,now)
        if not ready and now<self.deadline:return {'status':'WAIT_A5','draft':deepcopy(self.draft)}
        a5_status='COMPLETE' if ready else 'A5_NOT_COMPLETE'
        try:
            report=self.build_final(draft=deepcopy(self.draft),draft_status=self.draft_status,
                a5_status=a5_status,a5=deepcopy(a5) if ready else None,as_of=now)
            if (not isinstance(report,dict) or not report.get('source_ref')
                    or not isinstance(report.get('sha256'),str)
                    or _SHA.fullmatch(report['sha256']) is None):
                raise ValueError('SHADOW_REPORT_BINDING_INVALID')
            self.terminal={'status':'FORMAL_WRITTEN','report':deepcopy(report),
                'a5_status':a5_status,'draft_status':self.draft_status,'observed_at':now.isoformat(),
                'deadline_missed':now>self.deadline,
                'production_mutation':False,'customer_notifications':0}
        except Exception as exc:
            self.terminal={'status':'SHADOW_REPORT_FAILED','failure_type':type(exc).__name__,
                'observed_at':now.isoformat(),'production_mutation':False}
        return deepcopy(self.terminal)
