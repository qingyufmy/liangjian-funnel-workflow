"""Seal pinned local PIT inputs into a NEW independent shadow evidence directory.

No source discovery, Settings, network, RuntimeStore or execution publication.
This CLI reuses caller-frozen bytes; it does not authenticate a historical PIT.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from liangjian_funnel.runtime.shadow_evidence import ShadowEvidenceLedger
from liangjian_funnel.runtime.shadow_pit_capture import (
    PITSourceReceipt, capture_shadow_pit, serializable_capture_report,
)


class InputError(ValueError):
    pass


def digest(body):
    return hashlib.sha256(body).hexdigest()


def read_bytes(path):
    if path.stat().st_size > 16*1024*1024:
        raise InputError('LOCAL_INPUT_SIZE_LIMIT')
    return path.read_bytes()


def load_json(body):
    def pairs(items):
        obj={}
        for key,value in items:
            if key in obj: raise InputError('LOCAL_JSON_DUPLICATE_KEY')
            obj[key]=value
        return obj
    try:
        return json.loads(body,object_pairs_hook=pairs,
            parse_constant=lambda _: (_ for _ in ()).throw(InputError('LOCAL_JSON_NONFINITE')))
    except (json.JSONDecodeError,UnicodeError):
        raise InputError('LOCAL_JSON_INVALID') from None


def pinned_file(root,item):
    if not isinstance(item,dict): raise InputError('LOCAL_FILE_RECEIPT_INVALID')
    name=item.get('path'); sha=item.get('sha256')
    if not isinstance(name,str) or not name or Path(name).is_absolute() or '..' in Path(name).parts:
        raise InputError('LOCAL_RELATIVE_PATH_REQUIRED')
    path=(root/name).resolve(strict=True)
    if not path.is_relative_to(root) or not path.is_file():
        raise InputError('LOCAL_PATH_ESCAPE')
    body=read_bytes(path)
    if not isinstance(sha,str) or not re.fullmatch('[0-9a-f]{64}',sha) or digest(body)!=sha:
        raise InputError('LOCAL_FILE_HASH_MISMATCH')
    return body,{'path':name,'sha256':sha,'size_bytes':len(body)}


def write_json(path,value):
    with path.open('x',encoding='utf-8',newline='\n') as output:
        output.write(json.dumps(value,ensure_ascii=False,sort_keys=True,allow_nan=False,indent=2)+'\n')


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input',type=Path,required=True)
    parser.add_argument('--input-sha256',required=True)
    parser.add_argument('--observed-at',required=True,help='Explicit aware knowledge time, not inferred from file mtime')
    parser.add_argument('--output-dir',type=Path,required=True,help='NEW independent directory; existing paths rejected')
    args=parser.parse_args(argv)
    try:
        input_path=args.input.resolve(strict=True); root=input_path.parent
        manifest_body=read_bytes(input_path)
        if not re.fullmatch('[0-9a-f]{64}',args.input_sha256) or digest(manifest_body)!=args.input_sha256:
            raise InputError('LOCAL_MANIFEST_HASH_MISMATCH')
        source=load_json(manifest_body)
        if not isinstance(source,dict) or source.get('schema_version')!='shadow-pit-local-input/1':
            raise InputError('LOCAL_MANIFEST_SCHEMA_INVALID')
        plans_body,plans_binding=pinned_file(root,source.get('plans'))
        plans=load_json(plans_body)
        if not isinstance(plans,list): raise InputError('LOCAL_PLAN_ARRAY_REQUIRED')
        quotes=source.get('quotes')
        if not isinstance(quotes,dict): raise InputError('LOCAL_QUOTES_MAPPING_REQUIRED')
        receipts={}; quote_bindings={}
        for symbol,item in quotes.items():
            if not isinstance(symbol,str) or not re.fullmatch(r'[0-9]{6}\.(SH|SZ|BJ)',symbol):
                raise InputError('LOCAL_QUOTE_SYMBOL_INVALID')
            body,binding=pinned_file(root,item)
            fmt=item.get('format','EXPLICIT_JSON_FIELDS')
            receipts[symbol]=PITSourceReceipt(source_ref=item.get('source_ref'),
                captured_at=item.get('captured_at'),complete=item.get('complete'),format=fmt,
                raw_response=body if fmt!='NORMALIZED_QUOTE' else None,
                normalized=load_json(body) if fmt=='NORMALIZED_QUOTE' else None,
                byte_kind=item.get('byte_kind','CONSUMER_INPUT_BYTES'),field_map=item.get('field_map'))
            quote_bindings[symbol]=binding
        output=args.output_dir.resolve()
        if output.exists() or root==output or root.is_relative_to(output):
            raise InputError('NEW_INDEPENDENT_OUTPUT_REQUIRED')
        # Pure validation before constructing any writable ledger. A missed
        # window still gets a honest independent limited receipt, never a fetch.
        report=capture_shadow_pit(plans,observed_at=args.observed_at,source_receipts=receipts)
        output.mkdir(parents=True,exist_ok=False)
        ledger=ShadowEvidenceLedger(output/'shadow.sqlite3',output/'price-limit-evidence.jsonl')
        report=capture_shadow_pit(plans,observed_at=args.observed_at,source_receipts=receipts,ledger=ledger)
        report=serializable_capture_report(report)
        report.update(capture_mode='LOCAL_PINNED_PIT_INPUT',
                      observed_at_basis='CALLER_SUPPLIED_NOT_FILE_MTIME',
                      historical_pit_authenticated=False,actual_execution_authorized=False)
        code=0 if report['status']=='COMPLETE' else 2
        write_json(output/'capture-report.json',report)
        output_binding={}
        for name in ('capture-report.json','shadow.sqlite3','price-limit-evidence.jsonl'):
            path=output/name
            output_binding[name]={'sha256':digest(path.read_bytes()),'size_bytes':path.stat().st_size} if path.exists() else None
        write_json(output/'manifest.json',{'schema_version':'shadow-pit-local-output/1',
            'input_manifest_sha256':digest(manifest_body),'plans':plans_binding,'quotes':quote_bindings,
            'observed_at':report['observed_at'],'outputs':output_binding,'native_exit_code':code,
            'source_authenticated':False,'historical_pit_authenticated':False,
            'implementation_sha256':{p.name:digest(p.read_bytes()) for p in (
                Path(__file__).resolve(),Path(__file__).resolve().parents[1]/'src/liangjian_funnel/runtime/shadow_pit_capture.py',
                Path(__file__).resolve().parents[1]/'src/liangjian_funnel/runtime/shadow_evidence.py',
                Path(__file__).resolve().parents[1]/'src/liangjian_funnel/evaluation/ablation/price_limits.py')}})
        print(json.dumps({'status':report['status'],'selected_plan_count':report['selected_plan_count'],
                          'native_exit_code':code,'actual_execution_authorized':False}))
        return code
    except InputError as exc:
        print(json.dumps({'status':'DATA_LIMITED','error_code':str(exc),'actual_execution_authorized':False}))
        return 2
    except Exception:
        print(json.dumps({'status':'DATA_LIMITED','error_code':'LOCAL_CAPTURE_SETUP_OR_WRITE_FAILED',
                          'actual_execution_authorized':False}))
        return 2


if __name__=='__main__':
    raise SystemExit(main())
