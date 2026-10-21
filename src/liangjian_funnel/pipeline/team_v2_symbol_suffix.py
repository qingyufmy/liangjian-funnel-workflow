"""0498 identity spelling only; frozen directory owns unique six-digit IDs."""
from collections.abc import Mapping
from copy import deepcopy
import re

SYMBOL=re.compile(r'^(\d{6})\.(SH|SZ|BJ)$')

def canonical_symbol(value,directory):
    if not isinstance(value,str) or not SYMBOL.fullmatch(value):return value
    same={s for s in directory if isinstance(s,str) and SYMBOL.fullmatch(s) and s[:6]==value[:6]}
    return next(iter(same)) if len(same)==1 else value

def canonicalize_a2_symbols(raw,*,candidate_ids,nomination_scope,direction_symbols,catalog):
    if not isinstance(raw,Mapping):return raw,[]
    directory=set(candidate_ids)|set(nomination_scope)
    for members in direction_symbols.values():directory.update(members)
    result=deepcopy(dict(raw));actions=[]
    refs={x['id'] for x in catalog['items']}
    def replace(value,path,allowed):
        canonical=canonical_symbol(value,directory)
        if canonical!=value and canonical in allowed:
            actions.append(dict(rule_id='SYMBOL_SUFFIX_CANONICAL',field=path,raw=value,canonical=canonical))
            return canonical
        return value
    for pool in ('focus_decisions','reject_decisions','model_nominations','rotation_reviews'):
        rows=result.get(pool)
        if not isinstance(rows,list):continue
        for i,row in enumerate(rows):
            if not isinstance(row,dict):continue
            if pool!='rotation_reviews' and 'symbol' in row:
                row['symbol']=replace(row['symbol'],f'{pool}[{i}].symbol',nomination_scope if pool=='model_nominations' else candidate_ids)
            if pool=='rotation_reviews' and isinstance(row.get('reviewed_symbols'),list):
                allowed=direction_symbols.get(row.get('rotation_direction_id'),set())
                row['reviewed_symbols']=[replace(s,f'{pool}[{i}].reviewed_symbols[{j}]',allowed) for j,s in enumerate(row['reviewed_symbols'])]
    def visit(value,path=''):
        if isinstance(value,dict):
            for key,node in value.items():
                if key in ('supporting_refs','opposing_refs') and isinstance(node,list):
                    for i,ref in enumerate(node):
                        if not isinstance(ref,str) or ':' not in ref:continue
                        prefix,_,symbol=ref.rpartition(':');canonical=canonical_symbol(symbol,directory)
                        proposed=prefix+':'+canonical
                        if proposed!=ref and proposed in refs:
                            actions.append(dict(rule_id='SYMBOL_SUFFIX_CANONICAL',field=path+key+f'[{i}]',raw=ref,canonical=proposed))
                            node[i]=proposed
                else:visit(node,path+key+'.')
        elif isinstance(value,list):
            for i,node in enumerate(value):visit(node,path+str(i)+'.')
    visit(result)
    return result,actions
