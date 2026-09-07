"""Independent, read-only runtime watchdog with a durable local alert outbox.

Runs outside the VM: no trade/research/restart methods, no production writes.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

TZ = ZoneInfo('Asia/Shanghai')


def completed_minutes_from_log(text: str) -> list[str]:
    """Accept successful business dispatch, not an exit-zero scheduler NOOP."""
    runs = {}
    for line in text.splitlines():
        try:
            row = json.loads(line.lstrip('\x00'))
        except ValueError:
            continue
        if row.get('job') != 'monitor' or not row.get('runId'):
            continue
        item = runs.setdefault(row['runId'], {})
        message = str(row.get('message') or '')
        if row.get('stream') == 'stdout':
            if '"status": "DISPATCHED"' in message:
                item['dispatched'] = True
            if '"time":' in message:
                try:
                    value = json.loads('{'+message.strip().rstrip(',')+'}')['time']
                    item['minute'] = datetime.fromisoformat(value).replace(second=0,microsecond=0).isoformat()
                except (ValueError,KeyError):
                    pass
        if row.get('stream') == 'node' and '任务结束 run-monitor status=succeeded ' in message:
            item['finished'] = True
    return sorted({r['minute'] for r in runs.values() if r.get('dispatched') and r.get('finished') and r.get('minute')})


def expected_minutes(now: datetime) -> list[str]:
    """Only minutes whose completion grace (90s) has elapsed, lunch excluded."""
    if now.tzinfo is None:
        raise ValueError('AWARE_TIME_REQUIRED')
    now = now.astimezone(TZ)
    return [at.isoformat() for h,m in ((9,31),(13,1)) for i in range(120)
            if (at := now.replace(hour=h,minute=m,second=0,microsecond=0)+timedelta(minutes=i))
            <= now-timedelta(seconds=90)]


def gap_ranges(missing: list[str]) -> list[dict[str, Any]]:
    result = []
    for stamp in sorted(set(missing)):
        at = datetime.fromisoformat(stamp)
        if result and at-datetime.fromisoformat(result[-1]['end'])==timedelta(minutes=1):
            result[-1]['end']=stamp;result[-1]['minutes']+=1
        else:
            result.append({'start':stamp,'end':stamp,'minutes':1})
    return result


def assess(now: datetime, snapshot: dict, *, trading_day: bool) -> dict:
    now = now.astimezone(TZ)
    result: dict[str, Any] = {'date':now.date().isoformat(),'problems':{},'gaps':[],
                              'reachable':bool(snapshot.get('reachable')),'checked_at':now.isoformat()}
    if not trading_day or not (8*60+20 <= now.hour*60+now.minute <= 20*60+40):
        result['skipped']=True
        return result
    if not snapshot.get('reachable'):
        result['problems']['HOST_UNREACHABLE']='远端只读探测失败，无法核验主机连接、服务或A4执行账本。'
        return result
    if not snapshot.get('app_healthy'):
        result['problems']['APP_UNHEALTHY']='虚拟机可连接，但应用健康检查失败。'
    expected = expected_minutes(now)
    observed=set(snapshot.get('minutes') or [])
    result['gaps']=gap_ranges([s for s in expected if s not in observed])
    latest=snapshot.get('last_completed_minute') or ''
    result['last_completed_minute']=latest
    if expected and latest < expected[-1]:
        result['problems']['MONITOR_STALLED']=f"A4未完成应执行分钟；最近完成：{latest or '无'}；应至少完成：{expected[-1]}。"
    leases={r['lease_name']:r for r in snapshot.get('leases',[])}
    deadlines=(('premarket_0830',9,20,'盘前分析'),('morning_0925',9,40,'早盘复核'),
               ('a5_midday_1135',12,45,'午间复盘'),('a5_post_close_1600',17,0,'收盘复盘'),
               ('close_1510',20,30,'盘后研究'))
    for name,h,m,label in deadlines:
        if now <= now.replace(hour=h,minute=m,second=0,microsecond=0):continue
        lease=leases.get('scheduler:'+name,{})
        if lease.get('state')!='COMPLETED' or result['date'] not in str(lease.get('last_dispatch_key','')):
            result['problems']['MISSED_'+name]=f'{label}超过本日恢复窗口，尚无完成记录。'
    return result


class WatchdogLedger:
    """One process-wide local transaction serializes transitions and delivery.

    Delivery is at-least-once: a crash after HTTP success but before local
    commit can repeat a card. Never claim exactly-once external delivery.
    """

    def __init__(self,path: Path):
        path.parent.mkdir(parents=True,exist_ok=True)
        self.db=sqlite3.connect(path,timeout=2)
        self.db.row_factory=sqlite3.Row
        self.db.executescript('''
          CREATE TABLE IF NOT EXISTS incident(
            key TEXT PRIMARY KEY, code TEXT, day TEXT, active INTEGER, episode INTEGER,
            detail TEXT, started_at TEXT, ended_at TEXT);
          CREATE TABLE IF NOT EXISTS outbox(
            id TEXT PRIMARY KEY, title TEXT, body TEXT, color TEXT, attempts INTEGER DEFAULT 0,
            sent_at TEXT, next_attempt TEXT, last_reason TEXT);
          CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT);
        ''')

    def _queue(self,key,title,body,color='orange'):
        self.db.execute('INSERT OR IGNORE INTO outbox(id,title,body,color) VALUES(?,?,?,?)',(key,title,body,color))

    def process(self,assessment: dict,notifier,now: datetime) -> dict:
        stamp=now.astimezone(TZ).isoformat();day=assessment['date'];sent=[];failed=[]
        self.db.execute('BEGIN IMMEDIATE')
        try:
            if not assessment.get('skipped'):
                problems=assessment['problems']
                for code,detail in problems.items():
                    key=day+':'+code
                    row=self.db.execute('SELECT * FROM incident WHERE key=?',(key,)).fetchone()
                    if row is None or not row['active']:
                        episode=(row['episode'] if row else 0)+1
                        self.db.execute('INSERT OR REPLACE INTO incident VALUES(?,?,?,?,?,?,?,?)',
                                        (key,code,day,1,episode,detail,stamp,None))
                        self._queue(key+f':{episode}:blocked',f'A4运行告警｜{day}',
                                    f'时间：{stamp}\n{detail}\n仅报警，不自动重启、不补发历史交易信号。')
                # An unreachable host cannot prove monitor/job recovery.
                if assessment.get('reachable'):
                    for row in self.db.execute('SELECT * FROM incident WHERE active=1 AND day=?',(day,)).fetchall():
                        code=row['code']
                        if code in problems:continue
                        if code=='MONITOR_STALLED' and 'APP_UNHEALTHY' in problems:continue
                        self.db.execute('UPDATE incident SET active=0,ended_at=? WHERE key=?',(stamp,row['key']))
                        self._queue(row['key']+f":{row['episode']}:recovery",f'A4运行恢复｜{day}',
                                    f"恢复时间：{stamp}\n原问题：{row['detail']}\n已重新核验。中断区间只进入复盘，不补发历史交易信号。",'green')
                # Surface a gap first discovered after a restart, even if
                # the watchdog itself was not running during the outage.
                if assessment.get('reachable'):
                    for gap in assessment['gaps']:
                        if gap['end'] >= assessment.get('last_completed_minute',''):
                            continue  # Still-open gap is covered by the stall incident.
                        key=day+':gap:'+gap['start']
                        self._queue(key,f'A4执行缺口核对｜{day}',
                                    f"发现未执行区间：{gap['start']}—{gap['end']}，共{gap['minutes']}个交易分钟。\n当前监控已继续，缺口需要离线复盘。")
            # Stop repeated historical reminders from replacing current
            # incidents: pending transport failures retry with bounded backoff.
            for row in self.db.execute('SELECT * FROM outbox WHERE sent_at IS NULL ORDER BY rowid LIMIT 3').fetchall():
                if row['next_attempt'] and row['next_attempt']>stamp:
                    break
                try:
                    delivery=notifier.send(row['title'],row['body'],row['color'])
                    ok=bool(delivery.ok);reason=str(delivery.reason_code)
                except Exception:
                    ok=False;reason='WATCHDOG_DELIVERY_EXCEPTION'
                attempts=row['attempts']+1
                retry=(now+timedelta(seconds=min(900,60*2**min(attempts-1,4)))).astimezone(TZ).isoformat()
                self.db.execute('UPDATE outbox SET attempts=?,sent_at=?,next_attempt=?,last_reason=? WHERE id=?',
                                (attempts,stamp if ok else None,retry,reason,row['id']))
                (sent if ok else failed).append(row['id'])
                if not ok:break  # Preserve fault-before-recovery delivery order.
            self.db.execute('INSERT OR REPLACE INTO meta VALUES(?,?)',('last_check',json.dumps(assessment,ensure_ascii=False)))
            self.db.commit()
        except Exception:
            self.db.rollback();raise
        return {'sent':sent,'failed':failed,'problems':list(assessment['problems']),
                'gap_minutes':sum(g['minutes'] for g in assessment['gaps']),'checked_at':stamp}

    def queue_test(self,now: datetime):
        key='installation-test:'+now.astimezone(TZ).date().isoformat()
        self._queue(key,'A4独立告警验收','这是一条明确标记的安装测试消息，不代表系统故障。\n探针运行在虚拟机外部，覆盖停机、任务停滞、漏执行与恢复提醒。','blue')
        self.db.commit()
