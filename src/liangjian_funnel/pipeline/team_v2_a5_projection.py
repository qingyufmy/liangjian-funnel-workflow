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


def _text(value):
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)


def _sha(value):return hashlib.sha256(_text(value).encode()).hexdigest()


def project_review_facts(facts):
    counts=Counter();objects={}
    def decode(value):
        if isinstance(value,dict):
            if set(value).intersection((REF,JSON_ROW,META)):
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
        if isinstance(value,list):return [project(v) for v in value]
        return value
    output=project(decoded)
    output[META]=dict(schema=SCHEMA,source_sha256=_sha(facts),
        instructions='以共享事实引用读取同一原值；$a5_canonical_json为数据库原JSON字符串的无损解码，不是新增事实；证据目录ID保持原样。',
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
        if isinstance(v,dict):return {k:expand(x) for k,x in v.items()}
        if isinstance(v,list):return [expand(x) for x in v]
        return v
    result=expand(body)
    if _sha(result)!=meta['source_sha256']:raise ValueError('A5_TRANSPORT_FACT_HASH_MISMATCH')
    return result
