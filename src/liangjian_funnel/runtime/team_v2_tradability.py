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


def production_monitor_tradability(settings,*,symbol,now,bar,quote,plan,minute_bars=()):
    """Adapter of the existing published-plan/current-minute monitor contract.

    Publication already uses TRADABILITY_FLAGS (workflow._publish_plans).
    MonitorEngine.process_minute checks current 1m identity and evaluate_strategy's
    _locked_limit_up; simulation.PaperBroker.apply checks executable bar/quantity/T1.
    No emotion identity, shadow PIT or research rule-date gate is added here.
    """
    from collections.abc import Mapping
    from types import SimpleNamespace
    from .strategies import _locked_limit_up
    from ..evaluation.ablation.price_limits import _explicit
    from .stock_trading_rules import stock_trading_rules
    result=dict(status='DATA_LIMITED',reason_code='CONTROL_PRODUCTION_CONTRACT_UNPROVEN',
        basis='PRODUCTION_MONITOR_CONTRACT',shadow_pit_required=False)
    try:
        now=stamp(now);stock_trading_rules(symbol)
        if (not isinstance(plan,Mapping) or plan.get('symbol')!=symbol or bar.symbol!=symbol
                or not bar.closed or bar.minute_end.date()!=now.date() or bar.minute_end>=now
                or (now-bar.minute_end).total_seconds()>120):return result
        # Same optional explicit limit aliases as the original strategy. No
        # identity/rate estimate is introduced when that original field is absent.
        latest=tuple(minute_bars)[-5:]
        five=SimpleNamespace(low=min(b.low for b in latest) if latest else float(bar.low))
        locked,upper=_locked_limit_up(plan,bar,five)
        explicit,bad,conflict=_explicit(plan)
        if bad or conflict:return {**result,'reason_code':'CONTROL_EXPLICIT_LIMIT_CONFLICT'}
        lower=explicit[1]
        halted=bar.volume_shares<=0 or plan.get('suspended') is True or plan.get('halted') is True
        quote_valid=bool(quote is not None and quote.symbol==symbol and quote.quote_time.date()==now.date()
            and quote.quote_time<=now and (now-quote.quote_time).total_seconds()<=90)
        at_up=bool(locked or upper is not None and bar.close>=Decimal(str(upper)))
        at_down=bool(lower is not None and bar.close<=lower)
        quote_at_limit=bool(quote_valid and (upper is not None and Decimal(str(quote.price))>=Decimal(str(upper))
            or lower is not None and Decimal(str(quote.price))<=lower))
        return dict(status='KNOWN',reason_code=None,basis=result['basis'],shadow_pit_required=False,
            tradability_known=True,halted=halted,limit_up=at_up,limit_down=at_down,
            entry_tradable=quote_valid and not halted and not at_up and not at_down and not quote_at_limit and bar.high>bar.low)
    except (ValueError,TypeError,AttributeError):return result


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
