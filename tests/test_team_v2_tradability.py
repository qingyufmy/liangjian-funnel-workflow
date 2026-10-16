from datetime import datetime
from decimal import Decimal as D
from types import SimpleNamespace

from test_team_v2_paper_ledger import bar
from liangjian_funnel.runtime import team_v2_tradability as module


def test_rule_core_unconfirmed_never_becomes_permission(monkeypatch):
    monkeypatch.setattr(module,'derive_shadow_price_limits',lambda *a,**k:{'status':'DATA_LIMITED','reason_codes':['RULE_DATE_UNPROVEN']})
    result=module.current_tradability({},None,symbol='000001.SZ',now=datetime.fromisoformat('2026-10-19T10:01:00+08:00'),
        rule_receipt=None,approved=None,bar=bar(),quote=None)
    assert result['status']=='RULE_UNCONFIRMED' and 'tradability_known' not in result


def test_known_core_preserves_decimal_boundary_and_no_quote_only_blocks_entry(monkeypatch):
    monkeypatch.setattr(module,'derive_shadow_price_limits',lambda *a,**k:{'status':'KNOWN','upper':11.1,'lower':9.1})
    at=datetime.fromisoformat('2026-10-19T10:01:00+08:00')
    sealed=bar(close=D('11.1'),high=D('11.1'),low=D('10'),vwap=D('10.5'))
    result=module.current_tradability({},None,symbol='000001.SZ',now=at,rule_receipt=None,approved=None,bar=sealed,quote=None)
    assert result['status']=='KNOWN' and result['limit_up'] is True and result['entry_tradable'] is False
    quote=SimpleNamespace(symbol='000001.SZ',quote_time=at,price=11.1)
    assert not module.current_tradability({},None,symbol='000001.SZ',now=at,rule_receipt=None,approved=None,
        bar=sealed,quote=quote)['entry_tradable']
