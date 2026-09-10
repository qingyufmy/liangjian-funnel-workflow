"""Read-only production inputs and isolated public-source recovery acceptance (no orders or plan publication)."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sqlite3
import yaml

from liangjian_funnel.data.cache import MinuteBarStore
from liangjian_funnel.data.mootdx import MootdxAdapter
from liangjian_funnel.data.tencent_minute import TencentIntradayAdapter
from liangjian_funnel.data.rotation_theme import _default_tencent_quote_batch_fetch, _default_tencent_quote_fetch, _normalize_symbol
from liangjian_funnel.data.board_history import enrich_board_momentum, enrich_reported_five_day_momentum
from liangjian_funnel.pipeline.local_fact_cache import LocalFactCache
from liangjian_funnel.pipeline.emotion_theme import bind_emotion_themes
from liangjian_funnel.pipeline.statement_metrics import derive_statement_metrics
from liangjian_funnel.review.verification import A5IndependentVerifier
from liangjian_funnel.reporting import atomic_write_json
from liangjian_funnel.settings import Settings


class ReadOnlyFacts(LocalFactCache):
    def __init__(self,path): self.path=path.resolve()
    def _connect(self):
        db=sqlite3.connect(self.path.as_uri()+'?mode=ro',uri=True);db.row_factory=sqlite3.Row;return db


class ReadOnlyMinutes(MinuteBarStore):
    def __init__(self,directory): self.directory=directory.resolve();self.path=self.directory/'minute_bars.sqlite3'
    def _connect(self): return sqlite3.connect(self.path.as_uri()+'?mode=ro',uri=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot',type=Path,required=True)
    parser.add_argument('--a5-facts',type=Path,required=True)
    parser.add_argument('--output-dir',type=Path,required=True)
    parser.add_argument('--skip-quotes',action='store_true')
    parser.add_argument('--shadow-registry-config',type=Path)
    args=parser.parse_args(); s=Settings.from_env(root=Path.cwd())
    frozen=json.loads(args.snapshot.read_text());data=frozen.get('data',frozen)
    a5=json.loads(args.a5_facts.read_text());cutoff=datetime.fromisoformat(a5['cutoff_at'])
    output={'schema_version':'free-data-recovery-acceptance/1.0','production_mutation':False,
            'frozen_input_sha256':hashlib.sha256(args.snapshot.read_bytes()).hexdigest(),
            'original_a5_sha256':hashlib.sha256(args.a5_facts.read_bytes()).hexdigest()}
    symbols={'000980.SZ','000988.SZ','002046.SZ','002354.SZ','600172.SH','600519.SH','603900.SH','688836.SH'}
    output['emotion_theme_bindings']=bind_emotion_themes({},data,symbols)
    if args.shadow_registry_config:
        configuration=yaml.safe_load(args.shadow_registry_config.read_text(encoding='utf-8'))
        registry=configuration['agent_1']['mature_theme_registry']
        output['shadow_registry_bindings']={'scope':'NEW_MAPPING_POLICY_REPLAY_NOT_HISTORICAL_INPUT',
            'registry_version':registry['version'],
            'config_sha256':hashlib.sha256(args.shadow_registry_config.read_bytes()).hexdigest(),
            'bindings':bind_emotion_themes({}, {**data,'A1_MATURE_THEME_REGISTRY':registry},symbols)}
    facts=ReadOnlyFacts(s.fact_cache_db_path)
    financial=[]
    for dataset in ('INCOME','BALANCE','CASH_FLOW'):
        financial.extend({'_dataset':dataset,**r['payload']} for r in facts.query_financial_facts('301699.SZ',dataset=dataset))
    output['301699_derived_statements']=derive_statement_metrics(financial)
    atomic_write_json(args.output_dir/'acceptance.json',output)
    print('Frozen identities and financial derivation complete',flush=True)
    boards=[dict(r) for r in data['SELECTED_BOARD_SNAPSHOT']['boards']]
    memberships={}
    for path in (s.fact_store_dir/'rotation_theme'/'memberships').glob('*.json'):
        row=json.loads(path.read_text());theme=row.get('theme_id')
        if row.get('captured_at','') <= data['snapshot_manifest']['as_of'] and row.get('captured_at','') > memberships.get(theme,{}).get('captured_at',''):
            memberships[theme]=row
    for row in boards:
        member=memberships.get(row['theme_id'],{})
        row['component_board_codes']=sorted({p['board_code'] for p in member.get('pagination_evidence',{}).get('pages',[]) if p.get('board_code')})
        row['historical_component_basis']='FROZEN_MEMBERSHIP_PROVIDER_CODES_RECOVERY'
    enrich_board_momentum(boards,args.output_dir/'index_history',cutoff.date())
    periods=[v.get('5d',{}) for v in data.get('BOARD_CAPITAL_FLOW_SNAPSHOT',{}).get('by_taxonomy',{}).values()]
    enrich_reported_five_day_momentum(boards,periods,cutoff.date())
    output['board_momentum']=boards
    print('Board five-day momentum covered',sum(r.get('momentum_5d_pct') is not None for r in boards),'/',len(boards),flush=True)
    if not args.skip_quotes:
        all_symbols=sorted({_normalize_symbol(v) for row in boards for v in row['constituents']})
        quotes = _default_tencent_quote_batch_fetch(all_symbols)
        output['board_quote_coverage']=[]
        for row in boards:
            members=sorted({_normalize_symbol(v) for v in row['constituents']})
            missing=[v for v in members if quotes.get(v,{}).get('trade_date') != cutoff.date() or quotes.get(v,{}).get('turnover_cny') is None]
            output['board_quote_coverage'].append({'theme_id':row['theme_id'],'members':len(members),'missing':missing,
                'turnover_denominator_cny':None if missing else sum(quotes[v]['turnover_cny'] for v in members)})
        print('Board quotes checked',len(quotes),flush=True)
    atomic_write_json(args.output_dir/'acceptance.json',output)
    with sqlite3.connect(s.state_db_path.resolve().as_uri()+'?mode=ro',uri=True) as db:
        db.row_factory=sqlite3.Row
        wanted={r['plan_id'] for r in a5['a3']['plans']}
        plans=[dict(r) for r in db.execute('SELECT * FROM execution_plans') if r['plan_id'] in wanted]
        day=cutoff.date().isoformat()
        events=[dict(r) for r in db.execute('SELECT * FROM monitor_events WHERE minute_end>=? AND minute_end<=? ORDER BY minute_end',(day,cutoff.isoformat()))]
    verifier=A5IndependentVerifier(daily_cache=facts,minute_store=ReadOnlyMinutes(s.minute_cache_dir),
        tencent=TencentIntradayAdapter(timeout_seconds=6),mootdx=MootdxAdapter(nodes=s.mootdx_servers[:2],timeout_seconds=4,max_pages=3),workers=6,quote_fetch=_default_tencent_quote_fetch,evidence_dir=args.output_dir/'market_evidence')
    result=verifier.verify(a2=a5['a2'],market_universe=a5['a2']['candidates'],plan_rows=plans,event_rows=events,cutoff_at=cutoff)
    atomic_write_json(args.output_dir/'a5-independent-recovery.json',result)
    output['a5_summary']={'original_universe':len(a5['a2']['candidates']),
        'remaining_missing':result['a2']['market_cross_section_missing_symbols'],
        'recovered_count':len(result['a2']['market_cross_section_recovery']),
        'a3_macd_covered':result['a3']['macd_formula_covered_count'],'a3_macd_mismatch':result['a3']['macd_mismatch_count'],
        'a4_discrepancy_classes':dict(Counter(r['discrepancy_class'] for r in result['a4']['plans']))}
    atomic_write_json(args.output_dir/'acceptance.json',output)
    print(json.dumps(output['a5_summary'],ensure_ascii=False),flush=True)


if __name__=='__main__': main()
