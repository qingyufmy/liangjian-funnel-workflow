"""Lossless A2 model transport: shared facts once, explicit named columns.

The server still validates the original frozen facts. This module neither
filters a candidate nor changes a value; expansion is an exact audit inverse.
"""
from collections import Counter
from collections.abc import Mapping
from copy import deepcopy
import hashlib
import json

SCHEMA='a2-lossless-model-input/1'
_MISSING={'$a2_missing':True}
_KEYS=('UPSTREAM_ACTIVE_POOL','A2_BOTTLENECK_CONTEXT','A2_QUANT_FILTERED_SAMPLE')


def _text(value):
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)


def _sha(value):
    return hashlib.sha256(_text(value).encode()).hexdigest()


def prepare_a2_transport_values(values):
    """Return an independently bound model projection and small audit metadata."""
    if (not isinstance(values,Mapping) or not all(k in values for k in _KEYS)
            or not isinstance(values['A2_BOTTLENECK_CONTEXT'],Mapping)
            or not isinstance(values['UPSTREAM_ACTIVE_POOL'],Mapping)
            or not isinstance(values['UPSTREAM_ACTIVE_POOL'].get('active_research_pool'),list)
            or not isinstance(values['A2_QUANT_FILTERED_SAMPLE'],list)):
        raise ValueError('TEAM_A2_TRANSPORT_SOURCE_INVALID')
    source=deepcopy(dict(values));counts=Counter();objects={}
    def scan(value):
        if isinstance(value,Mapping):
            if value==_MISSING or set(value)=={'catalog_ref'}:
                raise ValueError('TEAM_A2_TRANSPORT_RESERVED_VALUE')
        if isinstance(value,(dict,list,str)):
            encoded=_text(value)
            if len(encoded)>=100:counts[encoded]+=1;objects[encoded]=value
        if isinstance(value,dict):
            for child in value.values():scan(child)
        elif isinstance(value,list):
            for child in value:scan(child)
    for key in _KEYS:scan(source[key])
    selected=[encoded for encoded,n in counts.items()
        if n>=2 and (n-1)*len(encoded)-n*38-100>0]
    selected.sort(key=lambda encoded:hashlib.sha256(encoded.encode()).hexdigest())
    ids={encoded:'A2:FACT:'+str(index).zfill(4) for index,encoded in enumerate(selected,1)}
    def project(value,allow_ref=True):
        if isinstance(value,(dict,list,str)) and allow_ref:
            encoded=_text(value)
            if encoded in ids:return {'catalog_ref':ids[encoded]}
        if isinstance(value,dict):return {k:project(child) for k,child in value.items()}
        if isinstance(value,list):return [project(child) for child in value]
        return value
    def table(rows):
        if any(not isinstance(row,Mapping) for row in rows):
            raise ValueError('TEAM_A2_TRANSPORT_ROW_INVALID')
        columns=sorted({key for row in rows for key in row})
        return {'columns':columns,'rows':[[project(row[key]) if key in row else dict(_MISSING)
            for key in columns] for row in rows]}
    quant=source['A2_BOTTLENECK_CONTEXT']
    if '_transport' in quant or '_stock_table' in quant:
        raise ValueError('TEAM_A2_TRANSPORT_ALREADY_COMPACT')
    stock_rows=[]
    for symbol,row in quant.items():
        if symbol.startswith('_'):continue
        if not isinstance(row,Mapping) or 'symbol' in row:
            raise ValueError('TEAM_A2_TRANSPORT_ROW_INVALID')
        stock_rows.append({'symbol':symbol,**row})
    transport=deepcopy(source)
    transport['UPSTREAM_ACTIVE_POOL']={k:project(v) for k,v in source['UPSTREAM_ACTIVE_POOL'].items()
        if k!='active_research_pool'}
    transport['UPSTREAM_ACTIVE_POOL']['active_research_pool']=table(source['UPSTREAM_ACTIVE_POOL']['active_research_pool'])
    transport['A2_QUANT_FILTERED_SAMPLE']=table(source['A2_QUANT_FILTERED_SAMPLE'])
    transport['A2_BOTTLENECK_CONTEXT']={k:deepcopy(v) for k,v in quant.items() if k.startswith('_')}
    transport['A2_BOTTLENECK_CONTEXT']['_stock_table']=table(stock_rows)
    catalog={ids[encoded]:{'sha256':hashlib.sha256(encoded.encode()).hexdigest(),
        'value':project(objects[encoded],allow_ref=False)} for encoded in selected}
    transport['A2_BOTTLENECK_CONTEXT']['_transport']={
        'schema':SCHEMA,'missing_value':dict(_MISSING),'catalog':catalog}
    if expand_a2_transport_values(transport)!=source:
        raise ValueError('TEAM_A2_TRANSPORT_LOSSLESS_CHECK_FAILED')
    return transport,dict(schema=SCHEMA,source_values_sha256=_sha(source),
        transport_values_sha256=_sha(transport),shared_fact_count=len(catalog),
        source_characters=len(_text(source)),transport_characters=len(_text(transport)),
        lossless_inverse_verified=True,original_evidence_catalog_unchanged=(
            source.get('EVIDENCE_CATALOG')==transport.get('EVIDENCE_CATALOG')))


def expand_a2_transport_values(values):
    """Audit-only inverse. No provider/DB/settings/permission side effects."""
    source=deepcopy(dict(values));quant=source.get('A2_BOTTLENECK_CONTEXT')
    protocol=quant.get('_transport') if isinstance(quant,Mapping) else None
    if (not isinstance(protocol,Mapping) or protocol.get('schema')!=SCHEMA
            or protocol.get('missing_value')!=_MISSING):
        raise ValueError('TEAM_A2_TRANSPORT_SCHEMA_INVALID')
    catalog=protocol.get('catalog');cache={};visiting=set()
    if not isinstance(catalog,Mapping):raise ValueError('TEAM_A2_TRANSPORT_CATALOG_INVALID')
    def resolve(value):
        if isinstance(value,dict) and set(value)=={'catalog_ref'}:
            identity=value['catalog_ref']
            if identity in cache:return deepcopy(cache[identity])
            if identity in visiting or identity not in catalog:
                raise ValueError('TEAM_A2_TRANSPORT_REFERENCE_INVALID')
            visiting.add(identity);entry=catalog[identity];expanded=resolve(entry['value'])
            if _sha(expanded)!=entry['sha256']:raise ValueError('TEAM_A2_TRANSPORT_FACT_HASH_MISMATCH')
            visiting.remove(identity);cache[identity]=expanded
            return deepcopy(expanded)
        if isinstance(value,dict):return {k:resolve(child) for k,child in value.items()}
        if isinstance(value,list):return [resolve(child) for child in value]
        return value
    def rows(table):
        columns=table['columns'];result=[]
        if len(columns)!=len(set(columns)):raise ValueError('TEAM_A2_TRANSPORT_COLUMNS_INVALID')
        for raw in table['rows']:
            if len(raw)!=len(columns):raise ValueError('TEAM_A2_TRANSPORT_COLUMNS_INVALID')
            result.append({key:resolve(value) for key,value in zip(columns,raw,strict=True) if value!=_MISSING})
        return result
    stock_rows=rows(quant['_stock_table'])
    rebuilt={}
    for row in stock_rows:
        symbol=row.pop('symbol')
        if symbol in rebuilt:raise ValueError('TEAM_A2_TRANSPORT_DUPLICATE_SYMBOL')
        rebuilt[symbol]=row
    source['A2_BOTTLENECK_CONTEXT']={**rebuilt,**{k:deepcopy(v) for k,v in quant.items()
        if k not in ('_stock_table','_transport')}}
    upstream=source['UPSTREAM_ACTIVE_POOL']
    source['UPSTREAM_ACTIVE_POOL']={k:resolve(v) for k,v in upstream.items() if k!='active_research_pool'}
    source['UPSTREAM_ACTIVE_POOL']['active_research_pool']=rows(upstream['active_research_pool'])
    source['A2_QUANT_FILTERED_SAMPLE']=rows(source['A2_QUANT_FILTERED_SAMPLE'])
    return source
