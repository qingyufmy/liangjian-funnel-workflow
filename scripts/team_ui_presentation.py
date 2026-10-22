"""Display-only identities and source-scoped facts; never trading authority."""
import hashlib
import json
import re
import sqlite3
from datetime import datetime

CONDITIONS = {'A_SUPPORT':'关键支撑','A_PULLBACK':'回踩幅度','A_CONTRACTION':'缩量',
 'A_STABILIZED':'止跌企稳','A_15M_SUPPORT':'15分钟支撑','ENTRY_ZONE':'入场区间','ENTRY_RR':'盈亏比',
 'B_KEY_LEVEL':'突破关键位','B_KEY_BELOW_NO_CHASE':'未超过追涨界限','B_ATTEMPT_PREFIX':'开盘尝试前缀',
 'B_EFFECTIVE_BREAKOUT':'有效突破','B_RETEST':'回踩确认','B_RETEST_VOLUME':'回踩量能',
 'B_ACCEPTED':'突破获接受','B_STOP_ONLY_RAISED':'止损只升不降','B_NO_CHASE':'不追涨'}
STATUSES = {'APPROVED':'通过','REDUCED':'降额通过','REJECTED':'拒绝','BLOCKED':'拦截',
 'RECORD_ONLY':'只记录','FILLED':'已成交','PARTIAL':'部分成交','OPEN':'待成交','CANCELLED':'已撤销'}


def digest(value):
    return hashlib.sha256(value if isinstance(value, bytes) else json.dumps(value, ensure_ascii=False,
        sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def source(table, key, raw):
    return dict(table=table, id=key, sha256=digest(raw.encode()))


def receipt(root, day):
    path = root / 'state/team_v2/ui_presentation_source.json'
    if not path.is_file(): return None
    if path.is_symlink() or not path.resolve().is_relative_to(root) or path.stat().st_size > 16384:
        raise ValueError('PRESENTATION_RECEIPT_SCOPE')
    outer = json.loads(path.read_bytes()); body = outer.get('body')
    if (not isinstance(body, dict) or outer.get('body_sha256') != digest(body)
            or body.get('schema') != 'team-ui-presentation-source/0619-v1'
            or body.get('producer') != 'PINNED_READ_ONLY_OPS' or body.get('trade_date') != day):
        raise ValueError('PRESENTATION_RECEIPT_BINDING')
    return body


def valid_source(value):
    return (isinstance(value, dict) and isinstance(value.get('id'), str)
        and bool(re.fullmatch(r'[A-Za-z0-9_.:+-]{1,240}', value['id']))
        and isinstance(value.get('sha256'), str) and bool(re.fullmatch('[a-f0-9]{64}', value['sha256'])))


def identity(root, plan_id, symbol, connect):
    """A historical plan supplies identity only, never current eligibility."""
    if not isinstance(plan_id, str) or len(plan_id) > 240: return None
    db = connect(root, 'state/workflow.sqlite3')
    try:
        row = db.execute('SELECT symbol,payload_json FROM execution_plans WHERE plan_id=? LIMIT 2', (plan_id,)).fetchall()
        if len(row) != 1 or row[0]['symbol'] != symbol or len(row[0]['payload_json']) > 262144: return None
        body = json.loads(row[0]['payload_json']); name = body.get('company_name') or body.get('name')
        if not isinstance(name, str) or not name.strip() or len(name) > 64: return None
        return dict(name=name, source=source('execution_plans', plan_id, row[0]['payload_json']), identity_only=True)
    finally: db.close()


def decorate_names(root, rows, connect, known=None):
    known = known if isinstance(known, dict) else {}
    for row in rows or ():
        symbol = row.get('symbol') or row.get('code'); plan_id = row.get('plan_id') or row.get('id')
        if not isinstance(symbol, str): continue
        found = None
        if row.get('name') and valid_source(row.get('name_source') or row.get('source')):
            found = dict(name=row['name'], source=row.get('name_source') or row['source'], identity_only=True)
        prior = known.get(symbol)
        if found is None and isinstance(prior, dict) and prior.get('source', {}).get('id') == plan_id:
            found = prior
        if found is None:
            try: found = identity(root, plan_id, symbol, connect)
            except (OSError, ValueError, KeyError, sqlite3.Error): pass
        if found is None: found = known.get(symbol)
        if isinstance(found, dict) and isinstance(found.get('name'), str) and valid_source(found.get('source')):
            row.update(name=found['name'], name_source=found['source']); known[symbol] = found
        else: row.update(name=None, name_missing_reason='NO_BOUND_PLAN_IDENTITY')
    return known


def condition_basis(root, day, plan, connect):
    """Explain the exact displayed last condition row, not the day's maximum."""
    conditions = plan.get('conditions') or []
    key = next((c.get('source_id') for c in conditions if c.get('source_id')), None)
    if key is None: return
    db = connect(root, 'state/workflow.sqlite3')
    try:
        row = db.execute('SELECT event_id,minute_end,payload_json FROM monitor_events WHERE event_id=? LIMIT 2', (key,)).fetchall()
        if len(row) != 1 or not row[0]['minute_end'].startswith(day) or len(row[0]['payload_json']) > 262144: return
        raw = row[0]['payload_json']; body = json.loads(raw)
        if body.get('plan_id') != plan['id']: return
        strategy = body.get('strategy') or {}; entry = strategy.get('entry_confirmation') or {}
        required = entry.get('required_conditions')
        if not isinstance(required, list): return
        facts = entry.get('conditions') or {}; met = [k for k in required if facts.get(k, {}).get('status') == 'MET']
        proof = source('monitor_events', key, raw)
        plan['confirmation_basis'] = dict(kind='LATEST_RECORDED_REQUIRED_CONDITIONS_NOT_DAY_BEST',
            decision_at=row[0]['minute_end'], strategy_as_of=strategy.get('as_of'),
            closed_5m_end=strategy.get('closed_5m_end'), observed_at=strategy.get('closed_5m_end'),
            label='最新已记录闭合5分钟条件，非全天最高进度', required_count=len(required), met_count=len(met),
            required_conditions=required, confirmed=entry.get('confirmed') if isinstance(entry.get('confirmed'),bool) else None,
            original_status=entry.get('status'), source=proof)
        rr = facts.get('ENTRY_RR') or {}; threshold = rr.get('threshold') or {}
        plan['rr_basis'] = dict(kind='ORIGINAL_ENTRY_RR_AT_CLOSED_BAR_NOT_PLAN_OR_DAY_BEST',
            value=rr.get('value'), price=entry.get('current_entry_close'), stop=threshold.get('stop'),
            target=threshold.get('target'), minimum=threshold.get('minimum'), observed_at=rr.get('source_as_of'),
            closed_bar_end=strategy.get('closed_5m_end'), label='原ENTRY_RR；按该闭合价距止损计算，非计划盈亏比',
            decision_at=row[0]['minute_end'], formula='(target-price)/(price-stop)', source=proof)
    finally: db.close()


def apply(root, day, result, connect):
    frozen = None
    try: frozen = receipt(root, day)
    except (OSError, ValueError, KeyError, TypeError): pass
    known = {}
    for symbol, value in (frozen or {}).get('security_names', {}).items():
        if (re.fullmatch(r'\d{6}\.(SH|SZ|BJ)', symbol) and isinstance(value, dict)
                and isinstance(value.get('name'), str) and 0 < len(value['name']) <= 64 and valid_source(value.get('source'))):
            known[symbol] = value
    sections = result.get('sections') or {}
    for section in sections.values(): decorate_names(root, section.get('rows'), connect, known)
    decorate_names(root, result.get('admissions'), connect, known)
    result['security_names'] = known
    plans = sections.get('plans', {}).get('rows') or []
    records = []
    for plan in plans:
        try: condition_basis(root, day, plan, connect)
        except (OSError, ValueError, KeyError, TypeError, sqlite3.Error): pass
        state = (frozen or {}).get('plan_data_status', {}).get(plan.get('code'))
        if isinstance(state, dict) and state.get('status') in {'AVAILABLE', 'PARTIAL', 'UNPROVEN'} and valid_source(state.get('source')):
            plan['data_status'] = state
        else:
            plan['data_status'] = dict(status='UNPROVEN', label='完整性缺少独立来源', scope='NOT_INFERRED_FROM_ACTIVE_STATUS', source=None)
        basis = plan.get('confirmation_basis') or {}
        unmet = [c['code'] for c in plan.get('conditions', []) if isinstance(c,dict)
                 and isinstance(c.get('code'),str) and c.get('status') != 'MET']
        if valid_source(basis.get('source')):
            unproven = [c['code'] for c in plan.get('conditions', []) if isinstance(c,dict)
                        and isinstance(c.get('code'),str) and c.get('status')=='UNPROVEN']
            title = '形态条件已确认' if basis.get('confirmed') is True else '形态条件待证' if unproven else '等待形态条件'
            if plan.get('status')=='EXPIRED': title='已到期计划的末次条件'
            labels=[CONDITIONS.get(code,'未解释条件') for code in unmet]
            text=f"{basis.get('met_count')} / {basis.get('required_count')} 项满足"
            if labels:text+='；待满足或待证：'+'、'.join(labels)
            records.append(dict(id='condition-' + basis['source']['id'], at=basis.get('decision_at'), kind='PLAN_CONDITIONS',role='A4',
                title=title, text=text,unmet_conditions=unmet,unproven_conditions=unproven,
                symbol=plan.get('code'), name=plan.get('name'), source=basis['source']))
    risk = result.get('risk') or {}
    limits = (frozen or {}).get('risk_limits')
    if isinstance(limits, dict) and valid_source(limits.get('source')):
        values = limits.get('values') or {}
        for target, original, scale in [('max_open','max_new_entries',1),('max_cluster','max_cluster_signals',1),
                ('min_cash_pct','min_cash_reserve_pct',100),('max_risk_pct','max_risk_pct',100),('standard_risk_pct','standard_risk_pct',100)]:
            if isinstance(values.get(original), bool): continue
            try: value = float(values[original]) * scale
            except (KeyError, ValueError, TypeError): continue
            if 0 <= value <= 100: risk[target] = int(value) if value.is_integer() else value
        risk.update(limits_basis='LAST_PERSISTED_ADMISSION_NOT_CURRENT_CONFIG', source=limits['source'],
                    limits_observed_at=limits.get('observed_at'))
    risk['missing_reasons'] = {key:'NO_BOUND_CURRENT_OR_LAST_ADMISSION_LIMIT' for key in
        ('max_open','max_cluster','min_cash_pct','max_risk_pct') if risk.get(key) is None}
    result['risk'] = risk
    market = (frozen or {}).get('market')
    result['market'] = market if isinstance(market, dict) else dict(status='UNPROVEN', emotion=None,
        indices=[dict(key=k,label=label,value=None,status='NOT_INTEGRATED',missing_reason='NO_BOUND_INDEX_QUOTE')
                 for k,label in [('SSE','上证指数'),('SZSE','深证成指'),('CHINEXT','创业板指')]])
    for item in (frozen or {}).get('research_records', []):
        if isinstance(item, dict) and valid_source(item.get('source')): records.append(item)
    for admission in result.get('admissions') or []:
        if valid_source(admission.get('source')):
            records.append(dict(id='admission-' + str(admission.get('id')), at=admission.get('at'), kind='EXECUTION_ADMISSION',role='SYSTEM',
                title='执行准入'+STATUSES.get(admission.get('status'),'状态待解释'),
                text='准入数量：'+str(admission.get('shares'))+' 股',status=admission.get('status'),shares=admission.get('shares'),risk_unit=admission.get('risk_unit'),
                symbol=admission.get('code'), name=admission.get('name'), source=admission['source']))
    for fill in sections.get('fills',{}).get('rows') or []:
        if valid_source(fill.get('source')):
            side={'BUY':'买入','SELL':'卖出'}.get(fill.get('side'),'方向未记录')
            records.append(dict(id='fill-'+str(fill.get('fill_id')),at=fill.get('minute_end'),kind='EXECUTION_FILL',role='SYSTEM',
                title='实际'+side+'成交',text=f"{fill.get('qty')} 股，成交价 {fill.get('price')}",
                symbol=fill.get('symbol'),name=fill.get('name'),qty=fill.get('qty'),price=fill.get('price'),source=fill['source']))
    def timed(row):
        try:return datetime.fromisoformat(row['at']).utcoffset() is not None
        except (ValueError,TypeError,KeyError):return False
    known_time=sorted((r for r in records if timed(r)),key=lambda r:r['at'])[-60:]
    unknown=[dict(r,at=None,time_missing_reason='SOURCE_TIMESTAMP_UNAVAILABLE') for r in records if not timed(r)]
    result['trade_records'] = known_time+unknown[:max(0,min(10,60-len(known_time)))]
    result['trade_records_coverage']=dict(known_time_rows=len(known_time),unknown_time_total=len(unknown),limit=60,
        ordering='KNOWN_TIME_THEN_UNKNOWN_NOT_DAY_BEST_OR_EXECUTION_PERMISSION')
    result['presentation_source_status'] = 'PINNED_OBSERVATION_NOT_LIVE_COLLECTION' if frozen else 'MISSING_SOURCE'
    return result
