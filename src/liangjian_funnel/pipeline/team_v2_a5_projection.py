"""Lossless A5 transport: JSON row strings decoded, repeated facts once.

The validator still consumes the complete original fact object. This does not
drop fields, derive numbers, change evidence IDs, or summarize decision prose.
"""
from collections import Counter
from copy import deepcopy
import hashlib
import json

SCHEMA='a5-lossless-facts/1'
REF='$a5_fact_ref'
JSON_ROW='$a5_canonical_json'
META='_a5_transport'
TABLE='$a5_rows'


def _text(value):
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)


def _sha(value):return hashlib.sha256(_text(value).encode()).hexdigest()


def project_review_facts(facts):
    counts=Counter();objects={}
    def decode(value):
        if isinstance(value,dict):
            if set(value).intersection((REF,JSON_ROW,META,TABLE)):
                raise ValueError('A5_TRANSPORT_RESERVED_KEY')
            return {k:decode(v) for k,v in value.items()}
        if isinstance(value,list):return [decode(v) for v in value]
        if isinstance(value,str) and value[:1] in ('{','['):
            try:parsed=json.loads(value)
            except ValueError:return value
            if isinstance(parsed,(dict,list)) and _text(parsed)==value:
                return {JSON_ROW:decode(parsed)}
        return value
    decoded=decode(facts)
    def scan(value):
        if isinstance(value,(dict,list,str)):
            text=_text(value)
            if len(text)>=120:counts[text]+=1;objects[text]=value
        if isinstance(value,dict):
            for v in value.values():scan(v)
        elif isinstance(value,list):
            for v in value:scan(v)
    scan(decoded)
    repeated=sorted(s for s,n in counts.items() if n>=2 and (n-1)*len(s)-n*40-100>0)
    ids={s:'A5:FACT:'+str(i) for i,s in enumerate(repeated)}
    def project(value,reference=True):
        if reference and isinstance(value,(dict,list,str)) and _text(value) in ids:
            return {REF:ids[_text(value)]}
        if isinstance(value,dict):return {k:project(v) for k,v in value.items()}
        if isinstance(value,list):
            rows=[project(v) for v in value]
            # Explicit column names preserve model readability. Presence masks
            # distinguish an absent field from an explicit null. This is a
            # reversible transport, never a summary or dropped observation.
            if len(rows)>=3 and all(isinstance(v,dict) and not set(v).intersection((REF,JSON_ROW,TABLE)) for v in rows):
                columns=sorted(set().union(*(v.keys() for v in rows)))
                table={TABLE:dict(columns=columns,rows=[[v.get(k) for k in columns] for v in rows])}
                if any(set(v)!=set(columns) for v in rows):
                    table[TABLE]['present']=[[i for i,k in enumerate(columns) if k in v] for v in rows]
                if len(_text(table))<len(_text(rows)):return table
            return rows
        return value
    output=project(decoded)
    output[META]=dict(schema=SCHEMA,source_sha256=_sha(facts),
        instructions='以共享事实引用读取同一原值；$a5_canonical_json为数据库原JSON字符串无损解码；$a5_rows按columns逐列读取rows，present为实际存在字段下标，省略时全字段存在；不是新增事实；证据目录ID保持原样。',
        shared_facts={ids[s]:project(objects[s],False) for s in repeated})
    if expand_review_facts(output)!=facts:raise ValueError('A5_TRANSPORT_INVERSE_FAILED')
    return output,dict(schema=SCHEMA,source_sha256=_sha(facts),transport_sha256=_sha(output),
        source_chars=len(_text(facts)),transport_chars=len(_text(output)),
        shared_fact_count=len(repeated),lossless_inverse_verified=True)


def expand_review_facts(projected):
    body=deepcopy(projected);meta=body.pop(META)
    if meta['schema']!=SCHEMA:raise ValueError('A5_TRANSPORT_SCHEMA_INVALID')
    visiting=set();catalog=meta['shared_facts']
    def expand(v):
        if isinstance(v,dict) and set(v)=={REF}:
            key=v[REF]
            if key not in catalog or key in visiting:raise ValueError('A5_TRANSPORT_REF_INVALID')
            visiting.add(key);value=expand(catalog[key]);visiting.remove(key);return value
        if isinstance(v,dict) and set(v)=={JSON_ROW}:return _text(expand(v[JSON_ROW]))
        if isinstance(v,dict) and set(v)=={TABLE}:
            t=v[TABLE];columns=t['columns'];rows=t['rows'];present=t.get('present')
            if len(set(columns))!=len(columns) or any(len(r)!=len(columns) for r in rows):raise ValueError('A5_TRANSPORT_TABLE_INVALID')
            if present is not None and (len(present)!=len(rows) or any(len(set(p))!=len(p) or any(type(i) is not int or i<0 or i>=len(columns) for i in p) for p in present)):
                raise ValueError('A5_TRANSPORT_TABLE_INVALID')
            return [{k:expand(row[i]) for i,k in enumerate(columns) if present is None or i in present[n]} for n,row in enumerate(rows)]
        if isinstance(v,dict):return {k:expand(x) for k,x in v.items()}
        if isinstance(v,list):return [expand(x) for x in v]
        return v
    result=expand(body)
    if _sha(result)!=meta['source_sha256']:raise ValueError('A5_TRANSPORT_FACT_HASH_MISMATCH')
    return result
