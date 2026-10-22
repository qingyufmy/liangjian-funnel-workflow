"""Bounded, display-only dashboard. No business imports, writes or network calls.

Each section carries its own source/coverage. A successful HTTP read is never
promoted to a healthy strategy, a live mark, or a full-day signal count.
"""
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
import json
import sqlite3

def quantity_audit(value,number,scalar):
    if not isinstance(value,dict):return None
    keys=('nav','price','stop_price','risk_budget','symbol_value','position_pct','raw_risk_qty','risk_round_lot_qty','cap20_qty','probe10_qty','final_qty','daily_atr14','stop_distance_atr')
    result={key:number(value.get(key)) for key in keys if key in value}
    result['raw_decimal_strings']={key:value[key] for key in keys if isinstance(value.get(key),str) and len(value[key])<=128 and number(value[key]) is not None}
    result['binding_limits']=[scalar(x,identifier=True) for x in value.get('binding_limits',[])][:24]
    return result if any(result.get(k) is not None for k in ('raw_risk_qty','final_qty')) else None


def project_dashboard(root,query,out,*,project,connect,source,number,scalar):
    day=query['trade_date'];end=(date.fromisoformat(day)+timedelta(days=1)).isoformat()
    result=dict(out,status='AVAILABLE',missing_reason=None,sections={})
    sections=result['sections']
    for key,kind in [('plans','plans'),('calls','tasks')]:
        try:
            sections[key]=project(root,dict(query,kind=kind,limit=500,offset=0))
        except (OSError,ValueError,KeyError,sqlite3.Error):
            sections[key]=dict(status='MISSING_SOURCE',rows=None,observed_at=out['observed_at'],missing_reason='SOURCE_UNAVAILABLE')
    plans=sections['plans']
    if plans.get('rows') is not None and len(plans['rows'])<=20:
        try:
            from team_ui_plan_detail import project_detail
            db=connect(root,'state/workflow.sqlite3')
            try:
                for plan in plans['rows']:
                    plan.update(project_detail(db,day,plan['id'],root=root,connect=connect))
                    price=plan.get('price');low,high=plan['zone']
                    if price is not None and low is not None and high is not None:
                        plan['in_zone']=low<=price<=high
                        plan['distance_pct']=0 if plan['in_zone'] else (low-price)/price*100 if price<low else (price-high)/price*100
                        plan['distance_side']='BELOW' if price<low else 'ABOVE'
            finally:db.close()
        except (OSError,ValueError,KeyError,sqlite3.Error):
            plans['detail_missing_reason']='BOUNDED_DETAIL_SOURCE_UNAVAILABLE'
    account={'nav':None,'cash':None,'realized':None,'float':None,'position_pct':None,'at':None,'missing_reason':'ACCOUNT_SOURCE_UNAVAILABLE'}
    try:
        db=connect(root,'state/team_v2/paper.db')
        try:
            db.execute('BEGIN')
            from team_ui_account import project_account,quantity
            for key,kind in [('positions','account/positions'),('orders','account/orders'),('fills','account/fills')]:
                try:sections[key]=project_account(root,dict(query,kind=kind,limit=500,offset=0),out,connect=connect,source=source,scalar=scalar,number=number,connection=db)
                except (OSError,ValueError,KeyError,sqlite3.Error):sections[key]=dict(status='MISSING_SOURCE',rows=None,observed_at=out['observed_at'],missing_reason='SOURCE_UNAVAILABLE')
            meta=dict(db.execute("SELECT key,value FROM paper_meta WHERE key IN ('cash','realized')"))
            all_lots=[dict(x) for x in db.execute('SELECT symbol,qty,cost FROM paper_lots LIMIT 501')]
            if len(all_lots)>500:raise ValueError('ROW_BUDGET')
            lots=[x for x in all_lots if quantity(x['qty'])>0]
            marks={x['symbol']:dict(x) for x in db.execute('SELECT symbol,price,trade_date,minute_end FROM paper_marks LIMIT 501')}
            if len(lots)>500 or len(marks)>500:raise ValueError('ROW_BUDGET')
            cash=Decimal(meta['cash']);realized=Decimal(meta['realized'])
            if not cash.is_finite() or not realized.is_finite():raise ValueError('ACCOUNT_VALUE')
            needed=[marks.get(x['symbol']) for x in lots]
            mark_times={m['minute_end'] for m in needed if m and m['trade_date']==day}
            complete=all(m and m['trade_date']==day for m in needed) and len(mark_times)<=1
            market=sum((Decimal(m['price'])*x['qty'] for x,m in zip(lots,needed)),Decimal(0)) if complete else None
            cost=sum((Decimal(x['cost']) for x in lots),Decimal(0))
            if market is not None and not market.is_finite():raise ValueError('MARK_VALUE')
            nav=cash+market if market is not None else None
            proof=json.dumps(dict(meta=meta,lots=lots,marks=[m for m in needed]),sort_keys=True)
            account=dict(nav=number(str(nav)) if nav is not None else None,cash=number(str(cash)),realized=number(str(realized)),
                float=number(str(market-cost)) if market is not None else None,
                position_pct=number(format(market/nav*100,'f')) if nav is not None and nav>0 else None,
                at=next(iter(mark_times),None),missing_reason=None if complete else 'INCOMPLETE_OR_ASYNCHRONOUS_MARKS',
                provenance=source('paper_meta+paper_lots+paper_marks','current-account',proof),valuation_basis='LATEST_PERSISTED_SAME_TIME_MARKS_NOT_LIVE_QUOTES')
        finally:db.rollback();db.close()
    except (OSError,ValueError,KeyError,sqlite3.Error,InvalidOperation):pass
    for key in ('positions','orders','fills'):
        if key not in sections:sections[key]=dict(status='MISSING_SOURCE',rows=None,observed_at=out['observed_at'],missing_reason='SOURCE_UNAVAILABLE')
    result['account_consistency']='ONE_PAPER_DATABASE_RO_TRANSACTION_NOT_CROSS_DATABASE_ATOMIC'
    result['account']=account
    calls=sections['calls'];rows=calls.get('rows');complete=rows is not None and calls.get('next_offset') is None
    latencies=sorted(x['latency_ms'] for x in (rows or []) if isinstance(x.get('latency_ms'),(int,float)))
    result['metrics']={'model_calls':len(rows) if complete else None,'model_latency_p50_ms':latencies[(len(latencies)-1)//2] if latencies and complete else None,
        'model_latency_max_ms':max(latencies) if latencies and complete else None,'signals':None,'confirmations':None,
        'missing_reason':'SIGNAL_AND_CONFIRMATION_JOURNALS_NOT_BOUND'}
    result['health']={'mode':None,'record_latch':None,'model_roles':None,'last_model_result':None,'missing_reason':'ACTUAL_SETTINGS_AND_LATCH_NOT_BOUND'}
    result['risk']={'open':None,'max_open':None,'cluster_open':None,'max_cluster':None,'cash_pct':account['cash']/account['nav']*100 if account.get('nav') else None,'min_cash_pct':None,'risk_pct':None,'max_risk_pct':None,'missing_reason':'ACTUAL_EXECUTION_LIMITS_NOT_BOUND'}
    order_rows=sections['orders'].get('rows');fill_rows=sections['fills'].get('rows');position_rows=sections['positions'].get('rows')
    if order_rows is not None and fill_rows is not None and sections['orders'].get('next_offset') is None and sections['fills'].get('next_offset') is None:
        if not any(r.get('side')=='BUY' for r in order_rows+fill_rows):result['risk'].update(open=0,cluster_open=0)
        if position_rows==[] and not any(r.get('side')=='BUY' and r.get('status') in ('OPEN','PARTIAL') for r in order_rows):result['risk']['risk_pct']=0
    result['admissions']=[]
    try:
        db=connect(root,'state/team_v2/paper.db')
        try:
            admitted=[dict(x) for x in db.execute('SELECT signal_id,binding_hash,result_json FROM paper_admissions WHERE trade_date=? ORDER BY rowid LIMIT 501',(day,))]
            if len(admitted)>500:raise ValueError('ADMISSION_ROW_BUDGET')
            for row in admitted:
                body=json.loads(row['result_json']);binding=body.get('binding') or {};judgment=body.get('judgment') or {}
                if binding.get('trade_date')!=day:raise ValueError('ADMISSION_DATE')
                result['admissions'].append(dict(id=scalar(row['signal_id'],identifier=True),plan_id=scalar(binding.get('plan_id'),identifier=True),code=scalar(binding.get('symbol'),identifier=True),
                    action=scalar(binding.get('action'),identifier=True),at=scalar(body.get('observed_at'),identifier=True),mode=scalar(body.get('mode'),identifier=True),
                    shares=number(judgment.get('shares')),status=scalar(judgment.get('status'),identifier=True),risk_unit=scalar(judgment.get('risk_unit'),identifier=True),reason_codes=[scalar(x,identifier=True) for x in judgment.get('reason_codes',[])][:24],
                    quantity_audit=quantity_audit(judgment.get('quantity_audit'),number,scalar),
                    dispatched=body.get('dispatched') if isinstance(body.get('dispatched'),bool) else None,source=source('paper_admissions',row['signal_id'],row['result_json'])))
            if admitted:
                latest=json.loads(admitted[-1]['result_json']);binding=latest.get('binding') or {};limits=binding.get('limits') or {}
                result['risk'].update(last_admission_limits=dict(max_new_entries=number(limits.get('max_new_entries')),max_cluster_signals=number(limits.get('max_cluster_signals')),
                    min_cash_reserve_pct=number(limits.get('min_cash_reserve_pct'))),
                    at=scalar(latest.get('observed_at'),identifier=True),source=source('paper_admissions',admitted[-1]['signal_id'],admitted[-1]['result_json']),
                    missing_reason='CURRENT_RISK_OCCUPANCY_NOT_PERSISTED',limits_basis='LAST_PERSISTED_ADMISSION_NOT_CURRENT_CONFIG')
                result['health']['last_admission_mode']=scalar(latest.get('mode'),identifier=True)
            result['metrics']['admission_signals']=None if any(a['action'] is None for a in result['admissions']) else len([a for a in result['admissions'] if a['action'] in {'BUY_SIGNAL','ADD_SIGNAL','SELL_SIGNAL','REDUCE_SIGNAL','CANCEL_SIGNAL'}])
        finally:db.close()
    except (OSError,ValueError,KeyError,sqlite3.Error):
        result['metrics']['admission_signals']=None
        result['admissions']=None
    try:
        import hashlib
        db=connect(root,'state/team_v2/team.db')
        try:
            tables={x[0] for x in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('control_record_latch','risk_day_state')")}
            if 'control_record_latch' in tables:
                latch=db.execute('SELECT body_json,body_sha256 FROM control_record_latch WHERE account_id=? AND trade_date=?',('local-paper',day)).fetchone()
                if latch:
                    if hashlib.sha256(latch['body_json'].encode()).hexdigest()!=latch['body_sha256']:raise ValueError('LATCH_HASH')
                    body=json.loads(latch['body_json'])
                    if body.get('account_id')!='local-paper' or body.get('trade_date')!=day or body.get('mode')!='record':raise ValueError('LATCH_SCOPE')
                    result['health'].update(mode='RECORD',record_latch=True,latch_reason=scalar(body.get('reason_code'),identifier=True),latch_source=source('control_record_latch',day,latch['body_json']))
                else:result['health']['record_latch']=False
            if 'risk_day_state' in tables:
                risk=db.execute('SELECT consecutive_failures,halted,last_observed_at FROM risk_day_state WHERE account_id=? AND trade_date=?',('local-paper',day)).fetchone()
                if risk:
                    result['health']['a6_day_state']=dict(risk)
                    result['health']['a6_day_source']=source('risk_day_state',day,json.dumps(dict(risk),sort_keys=True))
        finally:db.close()
    except (OSError,ValueError,KeyError,sqlite3.Error):
        result['health']['missing_reason']='LATCH_SOURCE_UNPROVEN'
    if rows:
        latest=rows[-1];result['health']['last_model_result']={k:latest.get(k) for k in ('role','session','captured_at','validated','latency_ms','source')}
    result['agents']=[]
    for agent in ('A1','A2','A3','A4','A5','A6','PM'):
        matching=[x for x in (rows or []) if ('PM' if 'PM' in (x.get('session') or '') else x.get('role'))==agent]
        last=matching[-1] if matching else None
        result['agents'].append(dict(agent=agent,calls=len(matching) if complete else None,last=last,
            state=('已交付' if last['validated'] else '校验失败') if last else '未见本日调用原件'))
    result['alerts']=[]
    for key,section in sections.items():
        if section['status']!='AVAILABLE':result['alerts'].append(dict(id='missing-'+key,severity='warning',text={'plans':'计划','positions':'持仓','orders':'委托','fills':'成交','calls':'模型调用'}[key]+'数据缺失',at=out['observed_at']))
    from team_ui_runtime import read_runtime_receipt
    runtime=read_runtime_receipt(root,query)
    result['runtime_receipt']=runtime
    if runtime['status']=='VERIFIED_PROCESS_AND_CONFIG_BOUND':
        settings=runtime['settings'];limits=runtime['configured_limits']
        result['health'].update(mode='RECORD' if result['health']['record_latch'] else settings['a4_trader_mode'].upper() if result['health']['record_latch'] is False else None,settings_mode=settings['a4_trader_mode'],observed_at=runtime['observed_at'],
            production_pid_at_capture=runtime['pid'],head_at_capture=runtime['head'],runtime_source=runtime['source'],missing_reason=None,
            model_roles=dict(research=','.join(settings['research_models']),review=settings['review_model'],monitor=settings['monitor_model'],team_intraday=settings['team_intraday_model'],basis=f"{runtime['observed_at'][11:19]}已核同一Node进程/HEAD/配置SHA"))
        result['risk'].update(max_open=limits.get('max_new_entries'),max_cluster=limits.get('max_cluster_signals'),min_cash_pct=limits.get('min_cash_reserve_pct')*100 if 'min_cash_reserve_pct' in limits else None,
            configured_probe_position_pct=limits.get('probe_max_position_pct')*100 if 'probe_max_position_pct' in limits else None,configured_no_new_entry_after=limits.get('no_new_entry_after'),limits_basis=runtime['limits_basis'],source=runtime['source'])
    marker=root/'state/team_ui_frozen_capture.json'
    if marker.is_file():
        if marker.is_symlink() or marker.stat().st_size>16384:raise ValueError('FROZEN_MARKER_SCOPE')
        frozen=json.loads(marker.read_text(encoding='utf-8'))
        if frozen.get('schema')!='team-ui-frozen-capture/1':raise ValueError('FROZEN_MARKER_SCHEMA')
        result.update(data_mode='FROZEN_CAPTURE_NOT_REALTIME',observed_at=frozen['captured_at'],coverage_kind=frozen['coverage'])
        for section in sections.values():section.update(observed_at=frozen['captured_at'],data_mode='FROZEN_CAPTURE_NOT_REALTIME')
        if frozen.get('model_calls_complete') is not True:
            result['metrics'].update(model_calls=None,model_latency_p50_ms=None,model_latency_max_ms=None,missing_reason='CAPTURED_MODEL_ROWS_ARE_NOT_FULL_DAY')
            for agent in result['agents']:agent['calls']=None
        result['health'].update(production_pid_at_capture=frozen.get('node_pid_at_capture'),head_at_capture=frozen.get('head_at_capture'),model_roles=frozen.get('roles'),runtime_basis='FROZEN_CAPTURE_NOT_CURRENT_NODE')
        supplement=frozen.get('supplement')
        if supplement:
            node=supplement['nodes'][0];settings=node['settings'];at=supplement['captured_at']
            result['health'].update(mode='RECORD' if result['health'].get('record_latch') else settings['a4_trader_mode'].upper(),settings_mode=settings['a4_trader_mode'],
                observed_at=at,production_pid_at_capture=node['pid'],head_at_capture=supplement['head'],
                model_roles=dict(research=','.join(settings['research_models']),review=settings['review_model'],monitor=settings['monitor_model'],team_intraday=settings['team_intraday_model'],basis=f'{at[11:19]} 独立核对实际Node Settings；这是冻结观测，不代表现在'),
                runtime_source={'id':'actual-node-settings','sha256':frozen['supplement_sha256']})
            sections['calls']['observed_at']=at
            result['admissions']=[]
            for row in supplement['admissions']:
                if row['trade_date']!=day:continue
                body=row['result'];binding=body['binding'];judgment=body['judgment']
                result['admissions'].append(dict(id=row['signal_id'],plan_id=binding['plan_id'],code=binding['symbol'],action=scalar(binding.get('action'),identifier=True),at=body['observed_at'],mode=body['mode'],shares=number(judgment['shares']),risk_unit=judgment['risk_unit'],reason_codes=judgment['reason_codes'],status=judgment['status'],
                    source={'table':'paper_admissions','id':row['signal_id'],'sha256':row['result_sha256'],'hash_basis':'ORIGINAL_RESULT_HASH_APPROVED_WHITELIST_NOT_FULL_RESULT_REHASH'}))
            result['metrics']['approved_admissions']=sum(a['shares'] is not None and a['shares']>0 and a['status'] in ('APPROVED','REDUCED') for a in result['admissions'])
            if frozen['model_calls_complete']:
                result['metrics'].update(model_calls=len(rows or []),model_latency_p50_ms=latencies[(len(latencies)-1)//2] if latencies else None,model_latency_max_ms=max(latencies) if latencies else None)
                for agent in result['agents']:agent['calls']=sum(('PM' if 'PM' in (x.get('session') or '') else x.get('role'))==agent['agent'] for x in (rows or []))
        result['known_incidents']=frozen.get('known_incidents')
    from team_ui_health import project_health
    history=project_health(root,day,connect)
    result['health_history']=history
    admissions=result['admissions']
    result['metrics']['team_effective_admissions']=len({a['id'] for a in admissions if a.get('action') in {'BUY_SIGNAL','ADD_SIGNAL','SELL_SIGNAL','REDUCE_SIGNAL','FORCED_RISK_EXIT'} and a.get('status') in {'APPROVED','REDUCED'} and (a.get('shares') or 0)>0}) if admissions is not None and all(a.get('action') is not None for a in admissions) else None
    monitor=history['monitor']
    if marker.is_file() and frozen.get('monitor_complete') is not True:
        monitor.update(signals=None,confirmations=None,complete=False,reason='FROZEN_MONITOR_ROWS_NOT_FULL_DAY')
    result['metrics'].update(signals=monitor.get('signals'),confirmations=monitor.get('confirmations'),signal_basis=monitor.get('basis'),signal_coverage=monitor.get('coverage_basis'),signal_missing_reason=monitor.get('reason'),confirmation_missing_reason=monitor.get('confirmation_reason'))
    result['metrics']['admission_signals_basis']='ADMISSION_REQUESTS_INCLUDING_REJECTED_NOT_EFFECTIVE_SIGNALS'
    if monitor.get('signals') is not None and monitor.get('confirmations') is not None:result['metrics']['missing_reason']=None
    for event in history['events']:
        if event['kind'] in ('PROVIDER_HTTP_ERROR','JOB_FAILED','JOB_TERMINATED'):
            result['alerts'].append(dict(id='history-'+event['source']['sha256'][:16],severity='warning',text='已有模型/调度失败原记录；仅表示当时异常，不据此断言当前仍失败。',at=event.get('at'),provenance=event['source']))
    for delivery in history['notifications'].get('rows',[]):
        if delivery.get('status')=='FAILED':result['alerts'].append(dict(id='delivery-'+str(delivery.get('id')),severity='warning',text='通知表保留一条失败记录；是否有后续送达需核原件，本页不补发。',at=delivery.get('at'),provenance=delivery.get('source')))
    health=result['health']
    if health.get('mode')=='RECORD' or health.get('record_latch') is True:
        result['alerts'].append(dict(id='record-mode',severity='critical',text='Team处于只记录保护；本页不能解锁或恢复受控执行。',at=health.get('observed_at') or out['observed_at'],provenance=health.get('latch_source') or health.get('runtime_source')))
    if (health.get('a6_day_state') or {}).get('consecutive_failures',0)>0:
        result['alerts'].append(dict(id='a6-day-failure',severity='warning',text='A6当日原表保留了失败记录；是否已恢复需核对后续原件，不能由页面连通推断。',at=health['a6_day_state']['last_observed_at'],provenance=health.get('a6_day_source')))
    for plan in sections['plans'].get('rows') or []:
        matching=[a for a in (result['admissions'] or []) if a['plan_id']==plan['id'] and a.get('quantity_audit')]
        if matching:
            latest=max(matching,key=lambda a:a.get('at') or '')
            plan['sizing']={**latest['quantity_audit'],'at':latest['at'],'source':latest['source']}
    return result
