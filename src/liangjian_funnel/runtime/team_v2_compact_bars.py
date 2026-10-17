"""Lossless model-only column encoding; no rounding or indicator synthesis."""
from collections.abc import Mapping
from copy import deepcopy
import re

ALIASES={'bar_end':'t','open':'o','high':'h','low':'l','close':'c','volume':'v','amount':'a','provider_bar_end':'pt'}


def encode_bars(rows):
    if not isinstance(rows,(list,tuple)) or any(not isinstance(r,Mapping) for r in rows):
        raise ValueError('TRADER_BAR_ENCODING_INVALID')
    keys=sorted(set().union(*(set(r) for r in rows))) if rows else []
    result={'encoding':'LOSSLESS_COLUMNS_V1','count':len(rows),'constants':{},'fields':{},'absent':{}}
    for key in keys:
        name=ALIASES.get(key,key);values=[deepcopy(r.get(key)) for r in rows]
        missing=[i for i,r in enumerate(rows) if key not in r]
        if values and not missing and all(v==values[0] for v in values):
            result['constants'][key]=values[0]
        else:
            result['fields'][name]=key;result[name]=values
            if missing:result['absent'][name]=missing
    result['column_aliases']={}
    for name in result['fields']:
        if name in result['absent']:continue
        earlier=next((other for other in result['fields'] if other!=name and other in result
            and other not in result['absent'] and list(result['fields']).index(other)<list(result['fields']).index(name)
            and result[other]==result[name]),None)
        if earlier:
            result['column_aliases'][name]=earlier;result.pop(name)
    timestamps=result.get('t',[])
    if (timestamps and all(isinstance(t,str) and re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\+08:00',t) for t in timestamps)
            and len({t[:10] for t in timestamps})==1):
        result['time_encoding']={'date':timestamps[0][:10],'suffix':'+08:00','format':'HH:mm:ss'}
        result['t']=[t[11:19] for t in timestamps]
    return result


def decode_bars(encoded):
    """Audit inverse, including original missing-vs-null and metadata fields."""
    if encoded.get('encoding')!='LOSSLESS_COLUMNS_V1':raise ValueError('TRADER_BAR_ENCODING_INVALID')
    rows=[deepcopy(encoded['constants']) for _ in range(encoded['count'])]
    for column,key in encoded['fields'].items():
        values=deepcopy(encoded[encoded.get('column_aliases',{}).get(column,column)])
        actual=encoded.get('column_aliases',{}).get(column,column)
        if actual=='t' and 'time_encoding' in encoded:
            base=encoded['time_encoding'];values=[base['date']+'T'+v+base['suffix'] for v in values]
        if len(values)!=len(rows):raise ValueError('TRADER_BAR_ENCODING_INVALID')
        for i,value in enumerate(values):
            if i not in encoded['absent'].get(column,[]):rows[i][key]=deepcopy(value)
    return rows
