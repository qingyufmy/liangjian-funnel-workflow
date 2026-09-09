"""Read-only acceptance of one published A2/A3 batch and its next-day plans."""
import argparse
from collections import Counter
from datetime import date, datetime
import hashlib
import importlib
import json
from pathlib import Path
import sqlite3
import subprocess
from zoneinfo import ZoneInfo

from liangjian_funnel.settings import Settings
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar


def audit_520_inputs(plans, settings, target_date):
    """Read the same archived history as A4; do not construct a writable store."""
    from liangjian_funnel.data.cache import _from_row
    from liangjian_funnel.runtime.strategies import _Bar, _historical_fifteen, _macd_observation
    targets = [p for p in plans if p['payload'].get('strategy_profile') == 'MA520_SWING']
    if not targets:
        return []
    cutoff = datetime.combine(date.fromisoformat(target_date), datetime.min.time(), ZoneInfo('Asia/Shanghai'))
    checks = []
    with sqlite3.connect((settings.minute_cache_dir/'minute_bars.sqlite3').as_uri()+'?mode=ro',uri=True) as db:
        for plan in targets:
            rows = db.execute('''select symbol,interval,bar_end,open_value,high_value,low_value,
                close_value,volume_value,amount_value,source_id,adjust_mode from minute_bars
                where symbol=? and interval='5m' and bar_end<? order by bar_end desc limit 360''',
                (plan['symbol'],cutoff.isoformat())).fetchall()
            history = [_from_row(row).model_dump(mode='json') for row in reversed(rows)]
            # The marker supplies only target date/symbol to the history selector.
            # It is never included in indicator inputs or an A4 decision.
            marker = _Bar(symbol=plan['symbol'],end=cutoff,open=1,high=1,low=1,close=1,volume=0,amount=0)
            bars = _historical_fifteen({'market_context':{'historical_5m':history}},[marker])
            macd = _macd_observation(bars)
            daily = plan['payload'].get('daily_macd') or (plan['payload'].get('strategy_facts') or {}).get('daily_macd') or {}
            checks.append({'symbol':plan['symbol'],'daily_macd_available':all(isinstance(daily.get(k),(int,float)) for k in ('dif','dea','hist')),
                'closed_prior_m15_count':len(bars),'m15_macd_warmup_complete':macd.get('warmup_complete') is True,
                'last_prior_bar':bars[-1].end.isoformat() if bars else None})
    return checks


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--target-date', required=True)
    args = parser.parse_args()
    if not all(c.isalnum() or c in '-_.' for c in args.run_id):
        raise SystemExit('INVALID_RUN_ID')
    root = Path.cwd()
    settings = Settings.from_env(root=root)
    audit = json.loads((settings.workflow_output_dir/'research'/f'research_{args.run_id}_lane_1.json').read_text())
    stages = {row['stage']:row for row in audit['stages']}
    a1,a2,a3 = (stages[key]['output'] for key in ('A1','A2','A3'))
    symbols = lambda rows: {row['symbol'] for row in rows}
    a1_set = symbols(a1.get('active_research_pool', []))
    a2_effective = symbols(a2.get('focus_pool', []) + a2.get('watch_only_pool', []))
    a2_all = set().union(*(symbols(a2.get(key, [])) for key in (
        'focus_pool','watch_only_pool','outside_rotation_pool','crowded_pool','low_identity_pool','rejected_candidates')))
    core,secondary = a3.get('core_watch_pool',[]),a3.get('secondary_watch_pool',[])
    db=sqlite3.connect(settings.state_db_path.as_uri()+'?mode=ro',uri=True); db.row_factory=sqlite3.Row
    plans=[]
    for row in db.execute('select plan_id,symbol,status,valid_from,expires_at,payload_json from execution_plans where lane_id=? and expires_at>=? and expires_at<?',('lane_1',args.target_date,args.target_date+'T23:59')):
        item=dict(row); payload=json.loads(item.pop('payload_json'))
        if payload.get('source_run_id')==args.run_id:
            plans.append({**item,'payload':payload})
    pending_other=db.execute("select count(*) from execution_plans where lane_id='lane_1' and status='PENDING_MORNING_REVIEW' and substr(expires_at,1,10)=?",(args.target_date,)).fetchone()[0]-len(plans)
    columns={row[1] for row in db.execute('pragma table_info(astock_outcome_labels)')}
    db.close()
    indicator_checks=audit_520_inputs(plans,settings,args.target_date)
    module_checks={}
    for name in ('pipeline.deterministic','pipeline.research','runtime.strategies','runtime.entry_contract','runtime.simulation','runtime.indicator_history','review.context','review.daily','workflow'):
        module=importlib.import_module('liangjian_funnel.'+name)
        local=(root/'src/liangjian_funnel'/Path(*name.split('.'))).with_suffix('.py')
        module_checks[name]=hashlib.sha256(local.read_bytes()).hexdigest()==hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
    assertions={
        'target_is_trading_day':ExchangeTradingCalendar().is_trading_day(date.fromisoformat(args.target_date)),
        'a2_all_subset_a1':a2_all<=a1_set,
        'a3_research_subset_a2':symbols(core+secondary)<=a2_effective,
        'published_equals_core':symbols(plans)==symbols(core) and len(plans)==len(core),
        'nonempty_plans':bool(plans),
        'pending_not_activated':all(p['status']=='PENDING_MORNING_REVIEW' and not p['valid_from'] for p in plans),
        'single_pending_batch':pending_other==0,
        'all_expire_target_close':all(p['expires_at'].startswith(args.target_date+'T15:00:00') for p in plans),
        'no_reserve_executable':all(p['payload'].get('rotation_reserve_scope')!='RESEARCH_ONLY_NO_AUTOMATIC_ENTRY' for p in plans),
        'all_qualified_model_pass':all(p['payload'].get('eligibility')=='QUALIFIED' and p['payload'].get('review_status')=='PASS' for p in plans),
        'new_trend_rule_frozen':all(p['payload'].get('strategy_profile')!='TREND_MA5' or p['payload'].get('trend_entry_rule_version')=='trend-ma5/2' for p in plans),
        'tn_schema_ready':{f'signal_return_{n}d' for n in (1,3,5,10)}<=columns,
        'installed_modules_match_source':all(module_checks.values()),
        '520_daily_and_prior_macd_ready':all(r['daily_macd_available'] and r['m15_macd_warmup_complete'] for r in indicator_checks),
    }
    history_path=settings.workflow_output_dir/'runs'/f'{args.run_id}-indicator-preparation.json'
    history=json.loads(history_path.read_text()) if history_path.exists() else {'status':'MISSING'}
    result={'run_id':args.run_id,'target_trade_date':args.target_date,'commit':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
        'assertions':assertions,'module_checks':module_checks,'a1_count':len(a1_set),'a2_counts':{key:len(a2.get(key,[])) for key in ('focus_pool','watch_only_pool','outside_rotation_pool','rejected_candidates')},
        'a2_quant':a2.get('local_screen_summary'),'a2_themes':a2.get('active_themes'),'a3_core':len(core),'a3_research_reserve':sum(r.get('rotation_reserve_eligible') is True for r in secondary),
        'a3_strategy_counts':dict(Counter(p['payload'].get('strategy_profile') for p in plans)),
        'plans':[{key:value for key,value in p.items() if key!='payload'}|{'strategy':p['payload'].get('strategy_profile'),'name':p['payload'].get('name')} for p in plans],
        'indicator_preparation':history,'520_archived_input_checks':indicator_checks,'status':'READY' if all(assertions.values()) and history.get('status')=='READY' else 'NEEDS_ATTENTION'}
    print(json.dumps(result,ensure_ascii=False))
    if not all(assertions.values()):
        raise SystemExit(2)


if __name__=='__main__':
    main()
