"""CI census of literal A4 condition/reason producers plus frozen live enums."""
from pathlib import Path
import ast,re

FIELDS={'met','unmet','veto','reasons','reason_codes','met_conditions','unmet_conditions','veto_conditions'}
CODE=re.compile(r'^[A-Z][A-Z0-9_]{2,}$')

def producer_codes(source):
    tree=ast.parse(source);result=set()
    def codes(node):
        return {n.value for n in ast.walk(node) if isinstance(n,ast.Constant) and isinstance(n.value,str) and CODE.fullmatch(n.value)}
    for node in ast.walk(tree):
        if isinstance(node,ast.Call):
            for key in node.keywords:
                if key.arg in FIELDS:result|=codes(key.value)
            if isinstance(node.func,ast.Attribute) and node.func.attr in {'append','extend'}:
                owner=node.func.value
                if isinstance(owner,ast.Name) and owner.id in FIELDS:
                    for value in node.args:result|=codes(value)
        if isinstance(node,ast.Assign):
            if any(isinstance(n,ast.Name) and n.id in FIELDS for target in node.targets for n in ast.walk(target)):
                result|=codes(node.value)
            if any(isinstance(target,ast.Subscript) and isinstance(target.slice,ast.Constant)
                   and target.slice.value in FIELDS for target in node.targets):result|=codes(node.value)
        if isinstance(node,ast.Dict):
            for key,value in zip(node.keys,node.values):
                if isinstance(key,ast.Constant) and key.value in FIELDS:result|=codes(value)
    return result
