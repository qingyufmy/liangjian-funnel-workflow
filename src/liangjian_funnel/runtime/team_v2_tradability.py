"""Team execution projection of the unchanged strict same-day rule core.

Claude 0242 permits reuse, not bypassing identity/original-body/day gates.
Caller-provided hashes still do not authenticate an HTTP source. Missing
producer material stays RULE_UNCONFIRMED; no code-prefix rate table is added.
"""
import base64
import json
from decimal import Decimal
from pathlib import Path

from .shadow_pit_capture import PITSourceReceipt
from .shadow_price_limits import derive_shadow_price_limits
from .shadow_rule_preflight import load_approved_rule_archive
from .team_v2_sources import stamp


def current_tradability(evidence, source, *, symbol, now, rule_receipt,approved,bar,quote):
    result=derive_shadow_price_limits(evidence,symbol=symbol,at=now,
        rule_receipt=rule_receipt,approved=approved,source=source)
    if result['status']!='KNOWN':return dict(status='RULE_UNCONFIRMED',rule_proof=result)
    if (bar.symbol!=symbol or not bar.closed or (quote is not None and (quote.symbol!=symbol
            or quote.quote_time>now or (now-quote.quote_time).total_seconds()>90))):
        return dict(status='DATA_LIMITED',reason_code='CONTROL_RULE_QUOTE_BAR_BINDING_UNPROVEN',rule_proof=result)
    return dict(status='KNOWN',rule_proof=result,tradability_known=True,
        halted=bar.volume_shares==0,limit_up=bar.close>=Decimal(str(result['upper'])),limit_down=bar.close<=Decimal(str(result['lower'])),
        entry_tradable=quote is not None and bar.volume_shares>0
            and Decimal(str(result['lower']))<Decimal(str(quote.price))<Decimal(str(result['upper'])))


def load_current_tradability(settings, *, symbol, now,bar,quote):
    root=Path(settings.root).resolve();base=root/'state/team_v2';day=stamp(now).date().isoformat()
    paths=(base/'rules'/(day+'.json'),base/'pit'/day/(symbol+'.json'))
    try:
        for path in paths:
            if path.is_symlink() or path.resolve()!=path or not path.is_file() or path.stat().st_size>16*1024*1024:
                return dict(status='RULE_UNCONFIRMED',reason_code='CONTROL_CURRENT_RULE_MATERIAL_MISSING')
        receipt,material=(json.loads(path.read_text(encoding='utf-8')) for path in paths)
        original=base64.b64decode(material['raw_response_base64'],validate=True)
        source=PITSourceReceipt(material['source_ref'],material['captured_at'],material['complete'],
            format=material['format'],raw_response=original,byte_kind=material['byte_kind'],field_map=material.get('field_map'))
        approved=load_approved_rule_archive(root/'artifacts/wp5-20261010/official-rules-20261010')
        return current_tradability(material['evidence'],source,symbol=symbol,now=now,rule_receipt=receipt,
            approved=approved,bar=bar,quote=quote)
    except Exception:
        return dict(status='RULE_UNCONFIRMED',reason_code='CONTROL_CURRENT_RULE_MATERIAL_UNPROVEN')
