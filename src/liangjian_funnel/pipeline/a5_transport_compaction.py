"""Reversible, readable dictionaries; no business field or value is removed."""
from collections import Counter
from copy import deepcopy
import json

SCHEMA='a5-extra-lossless/1'
KEY='_a5_extra_lossless'

def text(v):return json.dumps(v,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)

def _strings(v):
    if isinstance(v,str):yield v
    elif isinstance(v,list):
        for x in v:yield from _strings(x)
    elif isinstance(v,dict):
        for x in v.values():yield from _strings(x)

def pack_prefixes(value):
    counts=Counter(_strings(value));prefixes=Counter()
    for s,n in counts.items():
        for size in (8,12,16,24,32,48,64,96,128,192,256):
            if len(s)>size:prefixes[s[:size]]+=n
    candidates=sorted((p for p,n in prefixes.items() if n>=3 and n*(len(p)-8)-len(p)-20>0),
        key=lambda p:(-(prefixes[p]*(len(p)-8)-len(p)-20),p))[:512]
    chosen={s:max((p for p in candidates if s.startswith(p)),key=len,default=None) for s in counts}
    used=sorted(set(p for p in chosen.values() if p));indices={p:i for i,p in enumerate(used)}
    def encode(v):
        if isinstance(v,str):
            p=chosen[v]
            return '^'+str(indices[p])+':'+v[len(p):] if p else '^'+v if v.startswith('^') else v
        if isinstance(v,list):return [encode(x) for x in v]
        if isinstance(v,dict):return {k:encode(x) for k,x in v.items()}
        return v
    return dict(encoding='a5-prefix-dictionary/1',prefixes=used,
        decoding='In data values, ^N:tail means prefixes[N]+tail; ^^ loses one ^; dictionary literals and object keys are not references.',data=encode(value))


def pack_object_schemas(value):
    """Named columns, literal common cell values and explicit exceptions."""
    schemas=[];indices={};groups={}
    def collect(node):
        if isinstance(node,dict):
            columns=tuple(sorted(node));groups.setdefault(columns,[]).append(node)
            for x in node.values():collect(x)
        elif isinstance(node,list):
            for x in node:collect(x)
    collect(value);defaults=[]
    for columns,rows in groups.items():
        indices[columns]=len(schemas);schemas.append(list(columns));common={}
        for i,key in enumerate(columns):
            counts=Counter(text(row[key]) for row in rows if not isinstance(row[key],(dict,list)))
            if counts:
                literal,n=counts.most_common(1)[0]
                if n>=3:common[i]=json.loads(literal)
        defaults.append(common)
    def encode(node):
        if isinstance(node,dict):
            columns=tuple(sorted(node));index=indices[columns]
            full={'#'+str(index):[encode(node[k]) for k in columns]}
            sparse={'%'+str(index):[[i,encode(node[k])] for i,k in enumerate(columns)
                if i not in defaults[index] or text(node[k])!=text(defaults[index][i])]}
            return sparse if len(text(sparse))<len(text(full)) else full
        if isinstance(node,list):return [encode(x) for x in node]
        return node
    data=encode(value)
    return dict(encoding='a5-object-schema/1',schemas=schemas,defaults=[[[i,v] for i,v in common.items()] for common in defaults],
        decoding='Every data object is {"#N":[all values in schemas[N] key order]} or {"%N":[[column index,exception value],...]}. For %N start from literal defaults[N], then apply all exceptions; every schema column must be present. Arrays remain arrays. Recursively restore before other encodings. Defaults are exact literal source values, never assumptions. No source field or value is removed.',data=data)


def compact(value):
    from ..review.context import pack_compact_strings,pack_key_dictionary,pack_structure_dictionary
    original=value;layers=[]
    for pack in (pack_structure_dictionary,pack_object_schemas,pack_compact_strings,pack_prefixes,pack_key_dictionary):
        candidate=pack(value)
        if len(text(candidate))<len(text(value)):value=candidate;layers.append(candidate['encoding'])
    if not layers:return original
    result={KEY:dict(schema=SCHEMA,layers=layers,data=value,
        decoding='Decode outer data using its declared dictionary first, then each nested dictionary in reverse layers order. Only after this expand the original _a5_transport. Every original fact is retained.')}
    if expand(result)!=original:raise ValueError('A5_EXTRA_INVERSE_FAILED')
    return result if len(text(result))<len(text(original)) else original

def expand(value):
    if not isinstance(value,dict) or set(value)!={KEY}:return value
    meta=value[KEY]
    if set(meta)!={'schema','layers','data','decoding'} or meta['schema']!=SCHEMA:raise ValueError('A5_EXTRA_SCHEMA_INVALID')
    result=deepcopy(meta['data'])
    for layer in reversed(meta['layers']):
        if not isinstance(result,dict) or result.get('encoding')!=layer:raise ValueError('A5_EXTRA_LAYER_INVALID')
        if layer=='a5-value-dictionary/1':
            dictionary=result['dictionary']
            def decode(v):
                if isinstance(v,dict) and set(v)=={'$a5_value'}:
                    i=v['$a5_value']
                    if type(i) is not int or not 0<=i<len(dictionary):raise ValueError('A5_EXTRA_VALUE_INDEX_INVALID')
                    return deepcopy(dictionary[i])
                if isinstance(v,dict) and set(v)=={'$a5_value_object'}:
                    return {k:decode(x) for k,x in v['$a5_value_object'].items()}
                if isinstance(v,dict):return {k:decode(x) for k,x in v.items()}
                if isinstance(v,list):return [decode(x) for x in v]
                return v
        elif layer=='a5-object-schema/1':
            schemas=result['schemas'];defaults=result['defaults']
            if len(defaults)!=len(schemas):raise ValueError('A5_EXTRA_DEFAULTS_INVALID')
            if any(not isinstance(keys,list) or any(not isinstance(k,str) for k in keys) or len(keys)!=len(set(keys)) for keys in schemas):raise ValueError('A5_EXTRA_SCHEMA_KEYS_INVALID')
            def decode(v):
                if isinstance(v,list):return [decode(x) for x in v]
                if isinstance(v,dict):
                    if len(v)!=1:raise ValueError('A5_EXTRA_OBJECT_INVALID')
                    marker,values=next(iter(v.items()))
                    if marker[:1] not in ('#','%') or not marker[1:].isdigit() or not 0<=int(marker[1:])<len(schemas):raise ValueError('A5_EXTRA_OBJECT_SCHEMA_INVALID')
                    index=int(marker[1:]);keys=schemas[index]
                    if marker.startswith('%'):
                        row={}
                        for pairs,original in ((defaults[index],True),(values,False)):
                            seen=set()
                            if not isinstance(pairs,list):raise ValueError('A5_EXTRA_DEFAULTS_INVALID')
                            for pair in pairs:
                                if not isinstance(pair,list) or len(pair)!=2:raise ValueError('A5_EXTRA_DEFAULTS_INVALID')
                                i,x=pair
                                if type(i) is not int or not 0<=i<len(keys) or i in seen:raise ValueError('A5_EXTRA_DEFAULTS_INVALID')
                                seen.add(i);row[i]=deepcopy(x) if original else decode(x)
                        if set(row)!=set(range(len(keys))):raise ValueError('A5_EXTRA_OBJECT_WIDTH_INVALID')
                        return {key:row[i] for i,key in enumerate(keys)}
                    if not isinstance(values,list) or len(values)!=len(keys):raise ValueError('A5_EXTRA_OBJECT_WIDTH_INVALID')
                    return {key:decode(x) for key,x in zip(keys,values)}
                return v
        elif layer=='a5-key-dictionary/1':
            keys=result['keys']
            if any(not isinstance(k,str) for k in keys) or len(set(keys))!=len(keys):raise ValueError('A5_EXTRA_KEYS_INVALID')
            def decode(v):
                if isinstance(v,list):return [decode(x) for x in v]
                if isinstance(v,dict):
                    out={}
                    for k,x in v.items():
                        if not k.startswith('k') or not k[1:].isdigit() or not 0<=int(k[1:])<len(keys):raise ValueError('A5_EXTRA_KEY_INVALID')
                        key=keys[int(k[1:])]
                        if key in out:raise ValueError('A5_EXTRA_KEY_COLLISION')
                        out[key]=decode(x)
                    return out
                return v
        elif layer in {'a5-string-dictionary/2','a5-prefix-dictionary/1'}:
            prefix=layer=='a5-prefix-dictionary/1';dictionary=result['prefixes' if prefix else 'dictionary'];marker='^' if prefix else '~'
            if any(not isinstance(s,str) for s in dictionary):raise ValueError('A5_EXTRA_STRING_INVALID')
            def decode(v):
                if isinstance(v,str) and v.startswith(marker*2):return v[1:]
                if isinstance(v,str) and v.startswith(marker):
                    index,sep,tail=v[1:].partition(':') if prefix else (v[1:],'','')
                    if not index.isdigit() or not 0<=int(index)<len(dictionary) or prefix and not sep:raise ValueError('A5_EXTRA_REFERENCE_INVALID')
                    return dictionary[int(index)]+tail
                if isinstance(v,list):return [decode(x) for x in v]
                if isinstance(v,dict):return {k:decode(x) for k,x in v.items()}
                return v
        else:raise ValueError('A5_EXTRA_LAYER_INVALID')
        result=decode(result['data'])
    return result
