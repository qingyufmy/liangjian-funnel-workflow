"""Compare research function ASTs, signatures and resolved external imports."""
from __future__ import annotations

import argparse
import ast
import copy
import hashlib
import importlib.util
import json
from pathlib import Path


def canonical(node, package):
    node = copy.deepcopy(node)
    for item in ast.walk(node):
        if isinstance(item, ast.ImportFrom) and item.level:
            item.module = importlib.util.resolve_name('.'*item.level+(item.module or ''), package)
            item.level = 0
    return ast.dump(node, include_attributes=False)


def definitions(source: str, package: str, method_bindings=None):
    module = ast.parse(source)
    bindings = method_bindings or {}
    result = {}
    for node in module.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            key = bindings.get(node.name, node.name)
            result[key] = canonical(node, package)
        elif isinstance(node, ast.ClassDef):
            for method in node.body:
                if isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    result[node.name+'.'+method.name] = canonical(method, package)
    return result


def external_imports(source: str, package: str):
    found = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            found.update(('import', alias.name, alias.asname or '') for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            module = importlib.util.resolve_name('.'*node.level+(node.module or ''), package) if node.level else node.module
            if module == 'liangjian_funnel.pipeline.research.common':
                continue
            found.update((module or '', alias.name, alias.asname or '') for alias in node.names)
    return found


def verify(root: Path, manifest: dict):
    target = root/'src/liangjian_funnel/pipeline/research'
    actual, imports = {}, set()
    for name in ('common', 'a1', 'a2', 'a3'):
        source = (target/(name+'.py')).read_text(encoding='utf-8')
        mapping = manifest['method_bindings'].get(name, {})
        functions = definitions(source, 'liangjian_funnel.pipeline.research', mapping)
        for key in functions:
            if key in actual:
                raise ValueError('DUPLICATE_RESEARCH_DEFINITION:'+key)
        actual.update(functions)
        imports |= external_imports(source, 'liangjian_funnel.pipeline.research')
    expected = manifest['definitions']
    missing, extra = sorted(set(expected)-set(actual)), sorted(set(actual)-set(expected))
    changed = sorted(name for name in expected.keys() & actual.keys() if expected[name] != actual[name])
    import_diff = sorted(imports.symmetric_difference(tuple(x) for x in manifest['imports']))
    if missing or extra or changed or import_diff:
        raise ValueError(json.dumps({'missing':missing,'extra':extra,'changed':changed,'import_diff':import_diff}))
    return {'definitions': len(actual), 'missing':0, 'extra':0, 'changed':0,
            'external_import_edges':len(imports), 'import_diff':0,
            'baseline_sha256':manifest['source_sha256'],
            'scope':'AST_SIGNATURE_AND_RESOLVED_IMPORT_EQUIVALENCE_NOT_PRODUCTION_ACCEPTANCE'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    result = verify(Path(__file__).resolve().parents[1], json.loads(args.manifest.read_text(encoding='utf-8')))
    if args.output:
        with args.output.open('x', encoding='utf-8') as f:
            json.dump(result, f, indent=2)
    print(json.dumps(result))


if __name__ == '__main__':
    try:
        main()
    except ValueError as exc:
        print(str(exc))
        raise SystemExit(1)
