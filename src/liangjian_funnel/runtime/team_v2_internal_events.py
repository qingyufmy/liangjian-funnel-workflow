"""Bounded projection of committed owned facts, outside the trading process.

Paper reads are query-only, one snapshot per pass. Incremental cursors retain
their original row hash so replacing/reusing a source ledger is not silently
accepted. A crash between enqueue and cursor commit reuses the event binding.
No model, production-plan store, broker constructor, or source fetch is used.
"""
from contextlib import closing
from datetime import datetime, time
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
import sqlite3

from .team_v2_store import canonical


def _sha(text):
    return hashlib.sha256(text.encode()).hexdigest()


def _number(value):
    if value is None or isinstance(value, bool):
        return '资料不足'
    try:
        return str(value) if Decimal(str(value)).is_finite() else '资料不足'
    except InvalidOperation:
        return '资料不足'


def _code(value):
    return value if isinstance(value, str) and re.fullmatch('[A-Z][A-Z0-9_:.+-]{0,79}', value) else 'UNAVAILABLE'


def _card(*lines):
    return dict(msg_type='text', content={'text': '\n'.join(lines)})


class InternalEventProjector:
    def __init__(self, outbox):
        self.outbox = outbox
        self.store = outbox.store
        with closing(self.store._connect()) as db, db:
            db.execute('CREATE TABLE IF NOT EXISTS team_notification_cursors ('
                'account_id TEXT NOT NULL,trade_date TEXT NOT NULL,stream TEXT NOT NULL,'
                'last_row INTEGER NOT NULL,source_sha256 TEXT NOT NULL,'
                'PRIMARY KEY(account_id,trade_date,stream))')

    def _batch(self, source, *, account, day, table, columns, predicate, params):
        # All SQL identifiers/clauses are literal call-site constants below.
        with closing(self.store._connect()) as db:
            cursor = db.execute('SELECT last_row,source_sha256 FROM team_notification_cursors '
                'WHERE account_id=? AND trade_date=? AND stream=?', (account, day, table)).fetchone()
        if cursor:
            anchor = source.execute('SELECT '+columns+' FROM '+table+' WHERE rowid=?', (cursor[0],)).fetchone()
            if anchor is None or _sha(canonical(list(anchor))) != cursor[1]:
                raise ValueError('INTERNAL_SOURCE_CURSOR_CONFLICT')
        rows = source.execute('SELECT rowid,'+columns+' FROM '+table+' WHERE '+predicate+
            ' AND rowid>? ORDER BY rowid LIMIT 100', (*params, cursor[0] if cursor else 0)).fetchall()
        if not rows:
            # Empty current day must not rescan all prior-day rows every poll.
            # Keep the actual last immutable row as an anchor, not a made-up
            # current-day event. Later legitimate inserts have higher rowids.
            last = source.execute('SELECT rowid,'+columns+' FROM '+table+' ORDER BY rowid DESC LIMIT 1').fetchone()
            if last is not None and (cursor is None or last[0]>cursor[0]):
                self._advance(account,day,table,last)
        return rows

    def _advance(self, account, day, table, row):
        with closing(self.store._connect()) as db, db:
            db.execute('INSERT INTO team_notification_cursors VALUES (?,?,?,?,?) '
                'ON CONFLICT(account_id,trade_date,stream) DO UPDATE SET '
                'last_row=excluded.last_row,source_sha256=excluded.source_sha256 '
                'WHERE excluded.last_row>team_notification_cursors.last_row',
                (account, day, table, row[0], _sha(canonical(list(row[1:])))))

    def _emit(self, account, day, kind, source, body, now, priority=50):
        return self.outbox.enqueue(account_id=account, trade_date=day, kind=kind, source_id=source,
            body=body, observed_at=now, priority=priority)

    def _paper_source(self, path, account, day):
        with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True, timeout=.25)) as source:
            source.execute('PRAGMA query_only=ON'); source.execute('BEGIN')
            marker = source.execute("SELECT value FROM paper_meta WHERE key='initial_capital'").fetchone()
            if marker is None:
                raise ValueError('INTERNAL_PAPER_SCOPE_INVALID')
            admissions = self._batch(source, account=account, day=day, table='paper_admissions',
                columns='signal_id,result_json', predicate='trade_date=?', params=(day,))
            fills = self._batch(source, account=account, day=day, table='paper_fills',
                columns='fill_id,symbol,side,qty,price,minute_end,fill_basis',
                predicate='minute_end>=? AND minute_end<?', params=(day, day+'T99'))
            halt = source.execute('SELECT halt_reasons_json,body_json FROM paper_risk_days WHERE trade_date=?', (day,)).fetchone()
            global_halt = source.execute("SELECT value FROM paper_meta WHERE key='risk_halt_reason'").fetchone()
            nav = source.execute('SELECT payload_json FROM daily_nav WHERE trade_date=?', (day,)).fetchone()
            source.rollback()
        return admissions, fills, halt, global_halt, nav

    def _paper(self, path, account, day, now):
        admissions, fills, halt, global_halt, nav = self._paper_source(path, account, day)
        # No paper read lock survives into owned notification writes.
        for row in admissions:
            raw = json.loads(row[2]); judgment = raw['judgment']; binding = raw['binding']
            if datetime.fromisoformat(raw['observed_at']) > now:
                raise ValueError('INTERNAL_FUTURE_ADMISSION')
            status = judgment.get('status')
            if status in {'APPROVED', 'REDUCED', 'REJECTED'}:
                symbol = binding.get('symbol')
                symbol = symbol if isinstance(symbol, str) and re.fullmatch(r'\d{6}\.(SH|SZ|BJ)', symbol) else 'UNKNOWN'
                codes = judgment.get('reason_codes') or ['UNAVAILABLE']
                self._emit(account, day, 'DECISION', row[1], _card(
                    '模拟盘｜引擎裁决', '股票：'+symbol+'；状态：'+status,
                    '原裁决时间：'+datetime.fromisoformat(raw['observed_at']).isoformat(),
                    '数量：'+_number(judgment.get('shares'))+'；模式：'+_code(str(raw.get('mode', '')).upper()),
                    '来源：'+_code(binding.get('plan_origin')),
                    '原因：'+','.join(_code(code) for code in codes[:12]),
                    '裁决引用：'+_sha(row[1])[:16], '裁决不等于成交'), now)
                if status == 'REJECTED' and str(raw.get('mode', '')).upper() == 'CONTROL':
                    count = self.outbox.record_guard_rejection(account, day, row[1])
                    if count > 5:
                        # Freeze the first threshold crossing, not an ever-
                        # changing count under the same message identity.
                        self._emit(account, day, 'ALERT', 'guard-blocks-over-5', _card(
                            '模拟盘｜内部预警', 'TRADER_SIGNAL_BLOCKED_BY_GUARDRAIL',
                            '当日去重裁决拒绝数已超过5；不改变交易规则'), now, 0)
            self._advance(account, day, 'paper_admissions', row)
        for row in fills:
            if datetime.fromisoformat(row[6]) > now:
                raise ValueError('INTERNAL_FUTURE_FILL')
            symbol = row[2] if re.fullmatch(r'\d{6}\.(SH|SZ|BJ)', row[2]) else 'UNKNOWN'
            self._emit(account, day, 'FILL', row[1], _card('模拟盘｜撮合成交',
                '股票：'+symbol+'；方向：'+_code(row[3]),
                '原成交分钟：'+datetime.fromisoformat(row[6]).isoformat(),
                '数量：'+_number(row[4])+'；价格：'+_number(row[5]),
                '撮合依据：'+_code(row[7]), '成交引用：'+_sha(row[1])[:16]), now, 30)
            self._advance(account, day, 'paper_fills', row)
        codes = json.loads(halt[0]) if halt else []
        if global_halt and global_halt[0]:
            codes.append(global_halt[0])
        for code in set(codes):
            self._emit(account, day, 'ALERT', 'engine:'+_code(code), _card(
                '模拟盘｜停止开仓', _code(code), '保护性退出不受内部通知状态影响'), now, 0)
        if nav is not None:
            result = json.loads(nav[0]); values = result['account']
            opening = json.loads(halt[1]).get('day_open_nav') if halt else None
            daily_pnl = None
            if _number(opening) != '资料不足' and _number(values.get('nav')) != '资料不足':
                daily_pnl = str(Decimal(str(values['nav']))-Decimal(str(opening)))
            self._emit(account, day, 'SETTLEMENT', day, _card('模拟盘｜15:30结算',
                '状态：'+_code(result.get('status')), '净值：'+_number(values.get('nav')),
                '现金：'+_number(values.get('cash'))+'；持仓数：'+str(len(result['positions'])),
                '当日净值变动：'+_number(daily_pnl),
                '已实现累计损益：'+_number(values.get('pnl_realized')),
                '未实现损益：'+_number(values.get('pnl_unrealized')),
                '持仓：'+(';'.join((r['symbol'] if re.fullmatch(r'\d{6}\.(SH|SZ|BJ)',r['symbol']) else 'UNKNOWN')+
                    ' '+_number(r.get('qty'))+'股' for r in result['positions'][:30]) or '无'),
                '仅本地模拟；非真实账户'), now, 20)
            if result['reconcile']['status'] != 'MATCH':
                self._emit(account, day, 'ALERT', 'BROKER_RECONCILE_MISMATCH', _card(
                    '模拟盘｜内部预警', 'BROKER_RECONCILE_MISMATCH'), now, 0)

    def _team(self, account, day, now):
        with closing(self.store._connect()) as source:
            source.execute('PRAGMA query_only=ON')
            if source.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='control_record_latch'").fetchone():
                latch=source.execute('SELECT body_json,body_sha256 FROM control_record_latch WHERE account_id=? AND trade_date=?',
                    (account,day)).fetchone()
                if latch is not None:
                    if _sha(latch[0])!=latch[1]:raise ValueError('INTERNAL_CONTROL_LATCH_HASH_INVALID')
                    from .team_v2_first_day import record_alert_body
                    self._emit(account,day,'ALERT','CONTROL_FIRST_DAY_RECORD',record_alert_body(json.loads(latch[0])),now,0)
            rows = self._batch(source, account=account, day=day, table='regime_transitions',
                columns='transition_id,evidence_json', predicate='trade_date=?', params=(day,))
            for row in rows:
                evidence = json.loads(row[2])
                if datetime.fromisoformat(evidence['tick']) > now:
                    raise ValueError('INTERNAL_FUTURE_TRANSITION')
                self._emit(account, day, 'REGIME', row[1], _card('模拟盘｜市场状态切换',
                    '类型：'+_code(str(evidence['kind']).upper()),
                    '原确认时间：'+datetime.fromisoformat(evidence['tick']).isoformat(),
                    '原状态：'+_code(evidence.get('previous'))+'；新状态：'+_code(evidence['current']),
                    '两根5分钟确认；已有持仓退出规则不变'), now, 10)
                self._advance(account, day, 'regime_transitions', row)
            risk = source.execute('SELECT consecutive_failures,halted FROM risk_day_state WHERE account_id=? AND trade_date=?',
                (account, day)).fetchone()
            if risk and risk[0] >= 2 and risk[1]:
                self._emit(account, day, 'ALERT', 'A6_DEGRADED_HALT', _card(
                    '模拟盘｜停止开仓', 'A6_DEGRADED_HALT', '连续两次风控失败；重启不解除'), now, 0)
            # Fixed first morning summary. Missing predecessors are displayed,
            # not reconstructed by this notification process.
            if now.time().replace(tzinfo=None) >= time(8, 45):
                with closing(self.store._connect()) as own:
                    exists = own.execute("SELECT 1 FROM team_notifications WHERE account_id=? AND trade_date=? AND kind='MORNING'",
                        (account, day)).fetchone()
                if not exists:
                    engine = source.execute('SELECT body_json FROM morning_engine WHERE account_id=? AND trade_date=?', (account, day)).fetchone()
                    risk_row = source.execute('SELECT result_json FROM risk_meetings WHERE account_id=? AND trade_date=? ORDER BY rowid DESC LIMIT 1',
                        (account, day)).fetchone()
                    pm = source.execute('SELECT result_json FROM pm_decisions WHERE account_id=? AND trade_date=? ORDER BY rowid DESC LIMIT 1',
                        (account, day)).fetchone()
                    pm_result = json.loads(pm[0]) if pm else {}
                    risk_result = json.loads(risk_row[0]) if risk_row else {}
                    target = pm_result.get('deployment_target') or {}
                    original = {r['plan_id']:r for r in json.loads(engine[0]).get('envelopes', [])} if engine else {}
                    changes = []
                    for item in risk_result.get('envelopes', [])[:30]:
                        prior = original.get(item['plan_id']) or {}
                        if prior.get('risk_budget') != item.get('risk_budget') or prior.get('blocked_reasons') != item.get('blocked_reasons'):
                            changes.append('封套'+_sha(item['plan_id'])[:12]+'：'+_number(prior.get('risk_budget'))+'→'+
                                _number(item.get('risk_budget'))+'；'+','.join(_code(v) for v in item.get('blocked_reasons', [])[:8]))
                    self._emit(account, day, 'MORNING', day, _card('模拟盘｜08:45晨会摘要',
                        '引擎：'+('已有冻结结果' if engine else '缺失'),
                        'A6：'+_code(risk_result.get('status'))+'；A3：'+_code(pm_result.get('status')),
                        '目标使用率：'+_number(target.get('target_pct')),
                        '槽位数：'+str(len(pm_result.get('slot_plan', []))),
                        '风险封套数：'+str(len(risk_result.get('envelopes', []))),
                        '收紧项：'+('\n'.join(changes) if changes else '未记录新增收紧；缺失前序时不作无收紧证明'),
                        '仅通知已存事实，不补做晨会或授予交易权限'), now, 15)
            tables = {r[0] for r in source.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if {'team_internal_cards', 'team_a5_reviews'} <= tables:
                cards = source.execute("SELECT c.card_id,c.body_json FROM team_internal_cards c JOIN team_a5_reviews r "
                    "ON r.review_id=c.review_id WHERE r.account_id=? AND r.trade_date=? AND c.status='PENDING' LIMIT 10",
                    (account, day)).fetchall()
                for card_id, body in cards:
                    self._emit(account, day, 'A5_MIDDAY' if card_id.startswith('team-midday:') else 'A5_CLOSE',
                        card_id, json.loads(body), now, 20)
            if now.time().replace(tzinfo=None)>=time(17,10):
                review = (source.execute("SELECT 1 FROM team_a5_reviews WHERE account_id=? AND trade_date=? "
                    "AND review_kind='POST_CLOSE' LIMIT 1",(account,day)).fetchone()
                    if 'team_a5_reviews' in tables else None)
                if review is None:
                    self._emit(account,day,'ALERT','NO_REPORT_TODAY',_card(
                        '模拟盘｜内部预警','NO_REPORT_TODAY',
                        '17:10起核验未见当日Team V2盘后报告；不冒用旧版报告、不补跑模型'),now,0)
            source.rollback()

    def _rule(self, path, account, day, now):
        if path.is_symlink() or path.parent.is_symlink():
            raise ValueError('INTERNAL_RULE_SCOPE_INVALID')
        if not path.is_file():
            return
        with path.open('rb') as stream:
            raw = stream.read(16385)
        if len(raw)>16384:
            raise ValueError('INTERNAL_RULE_RECEIPT_TOO_LARGE')
        result=json.loads(raw)
        if (result.get('trade_date')!=day or result.get('audience')!='INTERNAL_ONLY'
                or result.get('reason_code')!='RULE_VERSION_CHANGED_NO_AUTO_ACCEPT'
                or result.get('blocks_new_entries') is not True
                or not re.fullmatch('[0-9a-f]{64}',result.get('receipt_sha256',''))):
            raise ValueError('INTERNAL_RULE_RECEIPT_INVALID')
        self._emit(account,day,'ALERT','rule:'+result['receipt_sha256'],_card(
            '模拟盘｜内部预警','RULE_VERSION_CHANGED_NO_AUTO_ACCEPT',
            '原文件变化；保持原有新开仓阻断，不自动接受新版本'),now,0)

    def project(self, *, paper_path, account_id, now, rule_alert_path=None):
        day = now.date().isoformat(); issues = []
        for name, action in (('PAPER', lambda:self._paper(paper_path, account_id, day, now)),
                             ('TEAM', lambda:self._team(account_id, day, now))):
            try:
                action()
            except Exception:
                # A broken projection is visible, never a trading halt and
                # never an arbitrary provider/DB exception in notification logs.
                issues.append('INTERNAL_'+name+'_PROJECTION_FAILED')
        if rule_alert_path is not None:
            try:self._rule(rule_alert_path,account_id,day,now)
            except Exception:issues.append('INTERNAL_RULE_PROJECTION_FAILED')
        return dict(status='READY' if not issues else 'DATA_LIMITED', reason_codes=issues)

    def sync_acknowledgments(self, *, account_id, trade_date):
        with closing(self.store._connect()) as db, db:
            db.execute("UPDATE regime_transitions SET notification_status=(SELECT n.status FROM team_notifications n "
                "WHERE n.kind='REGIME' AND n.source_id=regime_transitions.transition_id AND n.account_id=? AND n.trade_date=?) "
                "WHERE trade_date=? AND EXISTS(SELECT 1 FROM team_notifications n WHERE n.kind='REGIME' "
                "AND n.source_id=regime_transitions.transition_id AND n.account_id=? AND n.trade_date=?)",
                (account_id, trade_date, trade_date, account_id, trade_date))
            tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if 'team_internal_cards' in tables:
                db.execute("UPDATE team_internal_cards SET status=(SELECT n.status FROM team_notifications n "
                    "WHERE n.kind IN ('A5_CLOSE','A5_MIDDAY') AND n.source_id=team_internal_cards.card_id AND n.account_id=? AND n.trade_date=?) "
                    "WHERE EXISTS(SELECT 1 FROM team_notifications n WHERE n.kind IN ('A5_CLOSE','A5_MIDDAY') "
                    "AND n.source_id=team_internal_cards.card_id AND n.account_id=? AND n.trade_date=?)",
                    (account_id, trade_date, account_id, trade_date))
