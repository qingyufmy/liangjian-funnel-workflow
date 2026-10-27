"""Lossless A5 transport: JSON row strings decoded, repeated facts once.

The validator still consumes the complete original fact object. This does not
drop fields, derive numbers, change evidence IDs, or summarize decision prose.
"""
from collections import Counter
from copy import deepcopy
import hashlib
import json

SCHEMA='a5-lossless-facts/2'
REF='$a5_fact_ref'
JSON_ROW='$a5_canonical_json'
META='_a5_transport'
TABLE='$a5_rows'
INHERIT='$a5_inherit'
EXTRA='_a5_extra_lossless'


def _text(value):
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)


def _sha(value):return hashlib.sha256(_text(value).encode()).hexdigest()


def project_review_facts(facts):
    counts=Counter();objects={}
    def decode(value):
        if isinstance(value,dict):
            if set(value).intersection((REF,JSON_ROW,META,TABLE,INHERIT,EXTRA)):
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
            if len(text)>=200:counts[text]+=1;objects[text]=value
        if isinstance(value,dict):
            for v in value.values():scan(v)
        elif isinstance(value,list):
            for v in value:scan(v)
    scan(decoded)
    # Near-identical strategy records differ in only a few observations. Define
    # their exact common fields once; keep all varying fields named in place.
    groups={}
    for text,value in objects.items():
        if isinstance(value,dict) and len(value)>=4:
            groups.setdefault(tuple(sorted(value)),{})[text]=value
    bases={};object_bases={}
    for _,members in sorted(groups.items()):
        if len(members)<2:continue
        values=list(members.values());first=values[0]
        common={k:v for k,v in first.items() if all(_text(other[k])==_text(v) for other in values[1:])}
        if len(common)<2 or len(_text(common))<60 or (len(values)-1)*len(_text(common))-len(values)*40<120:continue
        identity='A5:COMMON:'+str(len(bases));bases[identity]=common
        for text in members:object_bases[text]=identity
        # A global intersection can miss large shared variants (for example
        # two patterns with the same field set). Factor exact variant fields
        # under their common parent; this preserves every exception and row.
        variants=[]
        for key in sorted(set.intersection(*(set(v) for v in values))):
            if key in common:continue
            partitions={};valid=True
            for encoded,value in members.items():
                scalar=value[key]
                if not isinstance(scalar,(str,int,float,bool,type(None))) or len(_text(scalar))>96:
                    valid=False;break
                partitions.setdefault(_text(scalar),{})[encoded]=value
            if not valid or not 2<=len(partitions)<=min(8,len(values)//2):continue
            children=[];gain=0
            for _,child in sorted(partitions.items()):
                if len(child)<2:continue
                rows=list(child.values());first_child=rows[0]
                extra={k:v for k,v in first_child.items() if k not in common
                    and all(k in other and _text(other[k])==_text(v) for other in rows[1:])}
                saving=(len(rows)-1)*len(_text(extra))-len(rows)*40-60
                if saving>120:children.append((child,extra));gain+=saving
            if gain>0:variants.append((gain,key,children))
        if variants:
            _,_,children=max(variants,key=lambda item:(item[0],item[1]))
            for child,extra in children:
                child_id='A5:COMMON:'+str(len(bases));bases[child_id]={INHERIT:identity,**extra}
                for encoded in child:object_bases[encoded]=child_id
    repeated=sorted(s for s,n in counts.items() if n>=2 and (n-1)*len(s)-n*25-40>0)
    ids={s:'A5:FACT:'+str(i) for i,s in enumerate(repeated)}
    def inherited_fields(identity):
        fields=set(bases[identity])-{INHERIT}
        if INHERIT in bases[identity]:fields|=inherited_fields(bases[identity][INHERIT])
        return fields
    def project(value,reference=True,inherit=True):
        if reference and isinstance(value,(dict,list,str)) and _text(value) in ids:
            return {REF:ids[_text(value)]}
        if isinstance(value,dict):
            common_id=object_bases.get(_text(value)) if inherit else None
            if common_id is not None:
                fields=inherited_fields(common_id)
                return {INHERIT:common_id,**{k:project(v) for k,v in value.items() if k not in fields}}
            return {k:project(v) for k,v in value.items()}
        if isinstance(value,list):
            rows=[project(v) for v in value]
            # Explicit column names preserve model readability. Presence masks
            # distinguish an absent field from an explicit null. This is a
            # reversible transport, never a summary or dropped observation.
            if len(rows)>=3 and all(isinstance(v,dict) and not set(v).intersection((REF,JSON_ROW,TABLE,INHERIT)) for v in rows):
                columns=sorted(set().union(*(v.keys() for v in rows)))
                table={TABLE:dict(columns=columns,rows=[[v.get(k) for k in columns] for v in rows])}
                if any(set(v)!=set(columns) for v in rows):
                    table[TABLE]['present']=[[i for i,k in enumerate(columns) if k in v] for v in rows]
                if len(_text(table))<len(_text(rows)):return table
            return rows
        return value
    output=project(decoded)
    output[META]=dict(schema=SCHEMA,source_sha256=_sha(facts),
        instructions='共享事实均为原值；$a5_inherit合并common_definitions对应公共字段与本记录显式字段（两者无重叠）；$a5_canonical_json为原JSON字符串无损解码；$a5_rows按columns读取rows，present为实际存在字段下标，省略时全字段存在；不是新增事实；证据ID不变。',
        shared_facts={ids[s]:project(objects[s],False) for s in repeated},
        common_definitions={identity:project(value,inherit=False) for identity,value in bases.items()})
    # The initial catalogue includes candidates hidden inside an interned
    # ancestor. Retain only definitions reachable from the complete fact body;
    # removing an unused encoding definition removes no source fact.
    metadata=output[META];used_facts=set();used_common=set()
    def reach(node):
        if isinstance(node,dict):
            if set(node)=={REF}:
                identity=node[REF]
                if identity not in used_facts:
                    used_facts.add(identity);reach(metadata['shared_facts'][identity])
                return
            if INHERIT in node:
                identity=node[INHERIT]
                if identity not in used_common:
                    used_common.add(identity);reach(metadata['common_definitions'][identity])
            for key,value in node.items():
                if key!=INHERIT:reach(value)
        elif isinstance(node,list):
            for value in node:reach(value)
    reach({k:v for k,v in output.items() if k!=META})
    metadata['shared_facts']={k:v for k,v in metadata['shared_facts'].items() if k in used_facts}
    metadata['common_definitions']={k:v for k,v in metadata['common_definitions'].items() if k in used_common}
    # Transport-only indices are numbers, never fabricated business IDs.
    fact_indices={key:i for i,key in enumerate(metadata['shared_facts'])}
    common_indices={key:i for i,key in enumerate(metadata['common_definitions'])}
    def number_refs(node):
        if isinstance(node,dict):
            return {k:(fact_indices[v] if k==REF else common_indices[v] if k==INHERIT else number_refs(v)) for k,v in node.items()}
        if isinstance(node,list):return [number_refs(v) for v in node]
        return node
    body=number_refs({k:v for k,v in output.items() if k!=META})
    metadata['shared_facts']=[number_refs(v) for v in metadata['shared_facts'].values()]
    metadata['common_definitions']=[number_refs(v) for v in metadata['common_definitions'].values()]
    metadata['instructions']+=' 共享定义以数组下标引用，$a5_fact_ref和$a5_inherit的数值仅为传输字典下标，不是事实数值或证据ID。'
    output={**body,META:metadata}
    if len(_text(output))>180000:
        from .a5_transport_compaction import compact
        output=compact(output)
    if expand_review_facts(output)!=facts:raise ValueError('A5_TRANSPORT_INVERSE_FAILED')
    return output,dict(schema=SCHEMA,source_sha256=_sha(facts),transport_sha256=_sha(output),
        source_chars=len(_text(facts)),transport_chars=len(_text(output)),
        shared_fact_count=len(repeated),common_definition_count=len(bases),lossless_inverse_verified=True)


def expand_review_facts(projected):
    from .a5_transport_compaction import expand
    projected=expand(projected)
    body=deepcopy(projected);meta=body.pop(META)
    if meta['schema'] not in (SCHEMA,'a5-lossless-facts/1'):raise ValueError('A5_TRANSPORT_SCHEMA_INVALID')
    visiting=set();catalog=meta['shared_facts']
    def valid_index(container,key):
        if isinstance(container,list):return type(key) is int and 0<=key<len(container)
        return isinstance(container,dict) and isinstance(key,str) and key in container
    def expand(v):
        if isinstance(v,dict) and INHERIT in v:
            identity=v[INHERIT];definitions=meta.get('common_definitions',{})
            if (not valid_index(definitions,identity) or ('common',identity) in visiting):raise ValueError('A5_TRANSPORT_INHERIT_INVALID')
            visiting.add(('common',identity));base=expand(definitions[identity]);visiting.remove(('common',identity))
            fields={k:expand(x) for k,x in v.items() if k!=INHERIT}
            if not isinstance(base,dict) or set(base)&set(fields):raise ValueError('A5_TRANSPORT_INHERIT_INVALID')
            return {**base,**fields}
        if isinstance(v,dict) and set(v)=={REF}:
            key=v[REF]
            if (not valid_index(catalog,key) or ('fact',key) in visiting):raise ValueError('A5_TRANSPORT_REF_INVALID')
            visiting.add(('fact',key));value=expand(catalog[key]);visiting.remove(('fact',key));return value
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
