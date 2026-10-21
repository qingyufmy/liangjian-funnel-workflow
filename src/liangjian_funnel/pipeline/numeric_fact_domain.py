"""Numeric claims must resolve to the exact fact projection sent to a model."""
from collections.abc import Mapping
from decimal import Decimal,InvalidOperation
import hashlib,json,re

NUMERIC=re.compile(r'[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?')


def numeric_fact_domain(facts):
    """Return canonical value -> source paths, without mining IDs/date tokens.

    Full numeric string values and finite JSON numbers are facts. JSON encoded
    source rows are traversed structurally, not treated as narrative text.
    """
    found={}
    def walk(value,path):
        if isinstance(value,Mapping):
            for key,child in value.items():walk(child,path+'.'+str(key))
        elif isinstance(value,(tuple,list)):
            for index,child in enumerate(value):walk(child,path+'['+str(index)+']')
        elif value is None or isinstance(value,bool):return
        elif isinstance(value,(int,float,Decimal)) or isinstance(value,str) and NUMERIC.fullmatch(value):
            try:number=Decimal(str(value))
            except InvalidOperation:return
            if number.is_finite():found.setdefault(str(number.normalize()),[]).append(path)
        elif isinstance(value,str) and value[:1] in ('{','['):
            try:body=json.loads(value)
            except (ValueError,TypeError):return
            walk(body,path+'::json')
    walk(facts,'$')
    return found


def numeric_domain_hash(domain):
    return hashlib.sha256(json.dumps(domain,sort_keys=True,ensure_ascii=False,
        separators=(',',':'),allow_nan=False).encode()).hexdigest()
