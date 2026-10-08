"""Offline theme/source coverage and explicit failover audit. Never publishes plans."""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from liangjian_funnel.data.board_reference import (
    audit_theme_bindings, digest, resolve_theme_reference, tdx_board_members,
    ReferenceError,
)
from liangjian_funnel.data.rotation_theme import load_rotation_theme_config
from liangjian_funnel.reporting import atomic_write_json


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--references',required=True)
    parser.add_argument('--registry')
    parser.add_argument('--bindings',help='Explicit JSON list with approved/priority/source/board/name')
    parser.add_argument('--output',required=True)
    args=parser.parse_args()
    now=datetime.now(ZoneInfo('Asia/Shanghai'))
    themes=[theme.as_dict() for theme in load_rotation_theme_config(args.registry).themes]
    catalogs=[];members=[];invalid=[]
    for path in Path(args.references).rglob('board-reference-*.json'):
        try:
            reference=json.loads(path.read_text(encoding='utf-8'))
        except (OSError,ValueError):invalid.append(str(path));continue
        if reference.get('kind') in {'catalog','catalog_with_members'}:catalogs.append(reference)
        elif reference.get('kind')=='members':members.append(reference)
        if reference.get('source_id')=='TDX_LOCAL' and reference.get('kind')=='catalog_with_members' and reference.get('available'):
            for board in reference.get('records',[]):
                try:members.append(tdx_board_members(reference,board['board_id'],now=now))
                except (ReferenceError,KeyError):continue
    report=audit_theme_bindings(themes,catalogs)
    report.update(observed_at=now.isoformat(),invalid_files=invalid,catalog_versions=len(catalogs),member_versions=len(members),
                  note='Exact-name suggestions are not approved mappings; input-universe F10 graphs are not full provider memberships.')
    if args.bindings:
        bindings=json.loads(Path(args.bindings).read_text(encoding='utf-8'))
        if not isinstance(bindings,list) or any(not isinstance(b,dict) for b in bindings):parser.error('bindings must be a JSON object list')
        known={t['theme_id'] for t in themes}
        if any(b.get('theme_id') not in known for b in bindings):parser.error('unknown strategy theme binding')
        projections={}
        for theme in themes:
            try:
                projections[theme['theme_id']]=resolve_theme_reference(theme['theme_id'],bindings,catalogs,members,now=now)
            except ReferenceError as exc:
                projections[theme['theme_id']]={'available':False,'reason_code':str(exc)}
        report['projections']=projections
        report['qualified_theme_count']=sum(p.get('available',False) for p in projections.values())
    report['content_hash']=digest(report)
    path=Path(args.output)
    if path.exists():parser.error('use a new output path; do not overwrite audit evidence')
    atomic_write_json(path,report)
    print(json.dumps({'output':str(path),'matched_theme_count':report['matched_theme_count'],'theme_count':report['theme_count'],
                      'qualified_theme_count':report.get('qualified_theme_count'),'execution_scope':'SHADOW'},ensure_ascii=False))
    return 0


if __name__=='__main__':raise SystemExit(main())
