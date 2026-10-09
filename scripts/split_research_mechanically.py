"""WP0-only mechanical relocation; verify every original callable afterwards."""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
from pathlib import Path
import re
import subprocess
import textwrap

from verify_research_equivalence import definitions, external_imports, verify

METHODS = {'_run_v2_a1_review':'a1', '_run_a1_batched':'a1',
           '_run_a2_batched':'a2', '_run_a3_batched':'a3'}


def stage_for(name):
    for stage in ('a1','a2','a3'):
        if re.search(r'(?:^|_)'+stage+r'(?:_|$)', name):
            return stage
    return None


def rebase(source):
    lines = source.splitlines(keepends=True)
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ImportFrom) and node.level:
            index = node.lineno-1
            lines[index] = re.sub(r'^(\s*from\s+)(\.+)', r'\1.\2', lines[index], count=1)
    return ''.join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    branch = subprocess.check_output(['git','branch','--show-current'], cwd=root, text=True).strip()
    if not branch.startswith('codex/wp0-'):
        raise SystemExit('ISOLATED_WP0_BRANCH_REQUIRED')
    old = root/'src/liangjian_funnel/pipeline/research.py'
    target = old.with_suffix('')
    if target.exists():
        raise SystemExit('RESEARCH_PACKAGE_ALREADY_EXISTS')
    source = old.read_text(encoding='utf-8')
    module = ast.parse(source)
    lines = source.splitlines(keepends=True)
    moves = {stage:[] for stage in ('a1','a2','a3')}
    ranges = []
    method_bindings = {stage:{} for stage in moves}
    for node in module.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            stage = stage_for(node.name)
            if stage:
                moves[stage].append((node.name, node))
                ranges.append((min([node.lineno]+[x.lineno for x in node.decorator_list])-1, node.end_lineno))
        elif isinstance(node, ast.ClassDef) and node.name == 'ResearchPipeline':
            for method in node.body:
                if isinstance(method, ast.FunctionDef) and method.name in METHODS:
                    if any(isinstance(item, ast.Name) and item.id in ('super','__class__') for item in ast.walk(method)):
                        raise ValueError('CLASS_CLOSURE_CANNOT_BE_MOVED:'+method.name)
                    stage = METHODS[method.name]
                    moves[stage].append((method.name, method))
                    method_bindings[stage][method.name] = 'ResearchPipeline.'+method.name
                    ranges.append((min([method.lineno]+[x.lineno for x in method.decorator_list])-1, method.end_lineno))
    manifest = {'schema_version':'research-ast-relocation/1',
        'source_sha256': hashlib.sha256(source.encode()).hexdigest(),
        'baseline_head':subprocess.check_output(['git','rev-parse','HEAD'], cwd=root,text=True).strip(),
        'definitions':definitions(source,'liangjian_funnel.pipeline'),
        'imports':sorted(external_imports(source,'liangjian_funnel.pipeline')),
        'method_bindings':method_bindings,
        'stages':{stage:[name for name,_ in items] for stage,items in moves.items()}}
    if not args.execute:
        print(json.dumps({stage:len(items) for stage,items in moves.items()}))
        return
    # This is the explicitly scoped bulk mechanical rewrite; no business
    # expression, branch, threshold, signature or function body is edited.
    target.mkdir()
    keep = list(lines)
    for start,end in ranges:
        keep[start:end] = ['']*(end-start)
    common = rebase(''.join(keep))
    common += '\n\n__all__ = [name for name in globals() if not name.startswith("__")]\n'
    (target/'common.py').write_text(common,encoding='utf-8')
    for stage,items in moves.items():
        segments = []
        for name,node in items:
            start = min([node.lineno]+[x.lineno for x in node.decorator_list])-1
            segments.append(textwrap.dedent(''.join(lines[start:node.end_lineno])))
        text = 'from __future__ import annotations\nfrom .common import *\n\n'+rebase('\n\n'.join(segments))
        text += '\n\n_MOVED_NAMES = '+repr(tuple(name for name,_ in items))+'\n'
        text += '_METHOD_BINDINGS = '+repr(method_bindings[stage])+'\n'
        (target/(stage+'.py')).write_text(text,encoding='utf-8')
    (target/'__init__.py').write_text((root/'scripts/research_facade.py.template').read_text(encoding='utf-8'),encoding='utf-8')
    (root/'artifacts/wp0-20261009/research-relocation-manifest.json').write_text(
        json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(verify(root,manifest)))
    # Removing the original tracked file is a relocation, not a repository
    # cleanup. Its original bytes remain recoverable in the baseline commit.
    old.unlink()


if __name__ == '__main__':
    main()
