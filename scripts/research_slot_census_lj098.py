"""Static domain census; never imports business code or opens a database."""
import ast
import hashlib
import json
from pathlib import Path
import re

DEST='docs/RESEARCH_SLOT_ALL_LANGUAGES_LJ098_20261028.json'
NUMERIC={'team_v2_pm_schema.py','team_v2_allocation.py','team_v2_a2_research.py','a3_attention_research.py'}


def domain(path,expression):
    name=Path(path).name
    if name in NUMERIC:return 'PORTFOLIO_NUMERIC_SLOT_SEPARATE'
    if name in {'scheduler.py','contracts.py','shadow_close_schedule.py','shadow_a5_observer.py'}:
        return 'SCHEDULE_OR_DECLARED_EVENT_IDENTITY_SEPARATE'
    if path in {'server/runner.ts','server/monitor-health.ts','server/scheduler.ts'}:
        return 'PROCESS_CONCURRENCY_OR_CLOCK_BUCKET_SEPARATE'
    if path.startswith('server/') or path=='web/src/types.ts':return 'ORIGINAL_AUDIT_FIELD_COPY_NO_AUTHORITY'
    if path.startswith('web/'):return 'PRESENTATION_ONLY_NO_PUBLICATION_AUTHORITY'
    if path.startswith('config/') or path.startswith('.github/'):return 'DECLARED_SCHEDULE_OR_CI_CONFIGURATION'
    if path.startswith('scripts/'):return 'OFFLINE_DIAGNOSTIC_OR_REPORT_CONSUMER'
    if 'require_research_slot' in expression or 'research_slot(' in expression:return 'CENTRAL_RESEARCH_SLOT_SEMANTICS'
    if name in {'common.py','state.py','reporting.py','progress.py'}:
        return 'ORIGINAL_RESEARCH_OR_AUDIT_WRITER_PRESERVES_LABEL'
    return 'RESEARCH_FLOW_BOUNDARY_REGISTERED_SOURCE'


def census(root):
    rows=set();sensitive=[]
    roots=('src','scripts','server','web/src','config','.github')
    for top in roots:
        base=root/top
        if not base.exists():continue
        for path in base.rglob('*'):
            if not path.is_file() or path.suffix not in {'.py','.sql','.ts','.js','.mjs','.yaml','.yml','.json'}:continue
            rel=path.relative_to(root).as_posix()
            if rel=='scripts/research_slot_census_lj098.py' or '__pycache__' in path.parts:continue
            text=path.read_text('utf-8-sig')
            if not re.search(r'\bslot\b|research_slots|review_slots',text):continue
            if path.suffix=='.py':
                tree=ast.parse(text)
                for node in ast.walk(tree):
                    hit=(isinstance(node,ast.Attribute) and node.attr=='slot'
                        or isinstance(node,ast.keyword) and node.arg=='slot'
                        or isinstance(node,(ast.Subscript,ast.Compare,ast.Call))
                           and re.search(r"\bslot\b",ast.unparse(node)))
                    if hit:
                        expression=ast.unparse(node)
                        rows.add((rel,domain(rel,expression),expression))
                    if isinstance(node,ast.Constant) and isinstance(node.value,str):
                        sql=node.value
                        if re.search(r'\b(?:SELECT|UPDATE|DELETE)\b',sql,re.I) and re.search(r'\bslot\b',sql):
                            expression='SQL:'+sql
                            rows.add((rel,'SQL_ORIGINAL_FIELD_OR_SEMANTIC_FILTER',expression))
                            if re.search(r"(?i)\bslot\s*=\s*['\"](?:close|morning)['\"]",sql):
                                sensitive.append(dict(path=rel,sql=sql))
            else:
                for line in text.splitlines():
                    line=line.strip()
                    if line.startswith(('//','#','*')):continue
                    if re.search(r'\bslot\b|research_slots|review_slots',line):
                        rows.add((rel,domain(rel,line),line))
    return dict(schema_version='research-slot-domain-census/2',authority='CLAUDE_0937',
        domains_separate=True,publication_slots=['close','morning'],
        premarket_is_reporting_identity_not_new_publication_alias=True,
        sensitive_sql_filters=sensitive,
        sites=[dict(path=p,domain=d,expression=e) for p,d,e in sorted(rows)])


if __name__=='__main__':
    root=Path(__file__).resolve().parents[1]
    value=census(root)
    if value['sensitive_sql_filters']:raise ValueError('CASE_SENSITIVE_RESEARCH_SQL_FILTER')
    (root/DEST).write_text(json.dumps(value,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(dict(sites=len(value['sites']),manifest=DEST)))
