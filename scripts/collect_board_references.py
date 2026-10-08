"""Collect independent free board references into an isolated SHADOW ledger."""
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from liangjian_funnel.data.board_reference import (
    BoardReferenceClient, collect_sina_catalog, collect_sina_members,
    collect_ths_catalog, collect_ths_members, import_tdx_blocks, import_tdx_industries,
    load_reference, write_reference, audit_theme_bindings, digest,
    collect_em_f10_tags, invert_f10_universe,
)
from liangjian_funnel.data.rotation_theme import load_rotation_theme_config
from liangjian_funnel.reporting import atomic_write_json


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source',choices=['sina','ths','tdx','em-f10'],required=True)
    p.add_argument('--category',choices=['concept','industry'],default='concept')
    scope=p.add_mutually_exclusive_group()
    scope.add_argument('--catalog-only',action='store_true')
    scope.add_argument('--board',action='append')
    scope.add_argument('--all',action='store_true')
    p.add_argument('--tdx-file');p.add_argument('--tdx-stock-file');p.add_argument('--source-updated-at')
    p.add_argument('--universe-json',help='JSON list of qualified stock symbols; never assumed full market')
    p.add_argument('--batch-limit',type=int,default=200,help='Maximum newly fetched F10 stocks; rerun resumes successful versions')
    p.add_argument('--registry');p.add_argument('--output-dir',required=True)
    p.add_argument('--deadline-seconds',type=float,default=300)
    p.add_argument('--max-requests',type=int,default=1000)
    args=p.parse_args()
    if not 1<=args.deadline_seconds<=900 or not 1<=args.max_requests<=2000 or not 1<=args.batch_limit<=2000:p.error('bounded deadline/request/batch limits required')
    if args.source=='tdx' and (not args.tdx_file or not args.source_updated_at):p.error('TDX requires a file and explicitly verified supplier update time')
    if args.source not in {'tdx','em-f10'} and not (args.catalog_only or args.board or args.all):p.error('choose --catalog-only, --board or --all')
    if args.source=='em-f10' and not args.universe_json:p.error('F10 reverse mapping needs an explicit universe')
    if args.source=='ths' and args.category!='concept':p.error('THS web industry is not implemented')
    root=Path(args.output_dir);now=datetime.now(ZoneInfo('Asia/Shanghai'))
    c=BoardReferenceClient(max_requests=args.max_requests,deadline_seconds=args.deadline_seconds)
    stock_refresh={'fresh_attempts':0,'fresh_failures':0,'cache_reused':0,'deferred':0}
    if args.source=='sina':catalog=collect_sina_catalog(c,args.category)
    elif args.source=='ths':catalog=collect_ths_catalog(c)
    elif args.source=='em-f10':
        universe=json.loads(Path(args.universe_json).read_text(encoding='utf-8'))
        if not isinstance(universe,list) or any(not isinstance(s,str) for s in universe) or len(universe)!=len(set(universe)):p.error('universe must be a unique list of qualified symbols')
        if not universe or any(not re.fullmatch(r'\d{6}\.(SH|SZ|BJ)',s) for s in universe):p.error('nonempty qualified symbol universe required')
        def iter_tags():
            for symbol in universe:
                stock_root=root/'stocks'/symbol
                loaded=load_reference(stock_root,'EM_F10',symbol,now=datetime.now(ZoneInfo('Asia/Shanghai')),kind='stock_tags')
                if loaded.get('available') and loaded['age_days']<7:
                    stock_refresh['cache_reused']+=1;yield loaded;continue
                if stock_refresh['fresh_attempts']>=args.batch_limit:
                    stock_refresh['deferred']+=1
                    if loaded.get('available'):yield loaded
                    continue
                stock_refresh['fresh_attempts']+=1
                attempt=collect_em_f10_tags(c,symbol);write_reference(stock_root,attempt)
                if attempt['available']:yield attempt
                else:
                    stock_refresh['fresh_failures']+=1
                    if loaded.get('available'):yield loaded
        catalog=invert_f10_universe(universe,iter_tags(),now_provider=lambda:datetime.now(ZoneInfo('Asia/Shanghai')))
    elif args.category=='industry':
        if not args.tdx_stock_file:p.error('TDX industry needs --tdx-stock-file tdxhy.cfg')
        catalog=import_tdx_industries(args.tdx_file,args.tdx_stock_file,now=now,source_updated_at=datetime.fromisoformat(args.source_updated_at))
    else:catalog=import_tdx_blocks(args.tdx_file,now=now,source_updated_at=datetime.fromisoformat(args.source_updated_at))
    path=write_reference(root/'versions',catalog);original_catalog=catalog
    if not catalog['available'] and args.source in {'sina','ths'}:
        catalog=load_reference(root/'versions',catalog['source_id'],catalog['board_id'],now=datetime.now(ZoneInfo('Asia/Shanghai')),kind='catalog',update_failed=True)
    results=[]
    if catalog['available'] and args.source in {'sina','ths'} and not args.catalog_only:
        names={r['board_id']:r['name'] for r in catalog['records']}
        codes=list(names) if args.all else args.board
        if any(code not in names for code in codes):p.error('board code is absent from the verified catalog')
        for code in codes:
            result=collect_sina_members(c,code,names[code]) if args.source=='sina' else collect_ths_members(c,code,names[code])
            member_path=write_reference(root/'versions',result)
            fallback=None
            if not result['available']:
                fallback=load_reference(root/'versions',catalog['source_id'],code,now=datetime.now(ZoneInfo('Asia/Shanghai')),update_failed=True)
            effective=result if result['available'] else fallback
            results.append({'board_id':code,'attempt_available':result['available'],'reason_code':result['reason_code'],
                            'attempt_hash':result['content_hash'],'path':str(member_path),'effective_available':bool(effective and effective.get('available')),
                            'effective_hash':effective.get('content_hash') if effective else None,
                            'observed_at':effective.get('observed_at') if effective else None,
                            'member_count':len(effective.get('records',[])) if effective else 0,
                            'fallback_reused':bool(effective and effective.get('fallback_reused'))})
    themes=load_rotation_theme_config(args.registry).themes
    audit=audit_theme_bindings([t.as_dict() for t in themes],[catalog])
    report={'schema_version':'liangjian-reference-refresh-audit/1.0.0','observed_at':datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
            'execution_scope':'SHADOW','production_publish_forbidden':True,'source':args.source,
            'catalog_attempt_available':original_catalog['available'],'catalog_available':catalog['available'],
            'catalog_hash':catalog.get('content_hash'),'catalog_path':str(path),'catalog_count':len(catalog.get('records',[])),
            'catalog_fallback_reused':bool(catalog.get('fallback_reused')),'requested_scope':'ALL_DISCOVERED_SOURCE_DIRECTORY' if args.all else 'SELECTED_BOARDS' if args.board else 'CATALOG_ONLY',
            'market_mapping_complete':False,
            'graph_complete':bool(args.all and catalog['available'] and results and all(r['effective_available'] for r in results)),
            'requested_board_count':len(results),'memberships':results,'theme_binding_audit':audit}
    if args.source=='em-f10':
        report['universe_audit']={k:catalog[k] for k in ('input_universe_count','covered_stock_count','missing_symbols')}
        report['incremental_refresh']=stock_refresh
    report['content_hash']=digest(report)
    report_path=root/f"reference-audit-{report['content_hash']}.json";atomic_write_json(report_path,report)
    print(json.dumps({k:report[k] for k in ['source','catalog_available','catalog_count','requested_board_count','graph_complete']},ensure_ascii=False))
    print(json.dumps({'report':str(report_path),'matched_themes':audit['matched_theme_count'],'theme_count':audit['theme_count'],'scope':'SHADOW'},ensure_ascii=False))
    # A recovered cache is visible, but a failed fresh attempt is not exit-0 acceptance.
    return 0 if original_catalog['available'] and all(r['attempt_available'] for r in results) and not stock_refresh['fresh_failures'] else 2


if __name__=='__main__':raise SystemExit(main())
