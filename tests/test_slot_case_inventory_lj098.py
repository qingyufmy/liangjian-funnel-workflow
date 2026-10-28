"""Every source close/morning slot comparison has an explicit producer contract."""
import ast,json
from pathlib import Path

ROOT=Path(__file__).parents[1]

def test_whole_source_slot_comparison_inventory_is_complete_and_reviewed():
    inventory=json.loads((ROOT/'docs/SLOT_CASE_COMPARISON_INVENTORY_LJ098_20261028.json').read_bytes())
    assert not inventory['unreviewed']
    expected={(r['path'],r['comparison']) for r in inventory['comparisons']}
    actual=set()
    for path in (ROOT/'src').rglob('*.py'):
        for node in ast.walk(ast.parse(path.read_bytes())):
            if not isinstance(node,ast.Compare):continue
            values={x.value for x in ast.walk(node) if isinstance(x,ast.Constant) and isinstance(x.value,str)}
            expression=ast.unparse(node)
            if values & {'close','CLOSE','morning','MORNING'} and 'slot' in expression:
                actual.add((path.relative_to(ROOT).as_posix(),expression))
    assert actual==expected
    for row in inventory['comparisons']:
        assert row['contract']!='REQUIRES_REVIEW'
        if row['contract']=='HELPER_NORMALIZES_AT_ENTRY':
            tree=ast.parse((ROOT/row['path']).read_bytes())
            function=next(n for n in ast.walk(tree) if isinstance(n,ast.FunctionDef) and n.name==row['function'])
            assert any(isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='slot' for t in n.targets)
                and ast.unparse(n.value)=='str(slot).lower()' for n in function.body)
