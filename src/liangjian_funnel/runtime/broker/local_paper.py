"""Execution adapter for the independent local paper ledger, never a live SDK."""
from dataclasses import fields
from pathlib import Path
import yaml

from liangjian_paper_broker.ledger import LocalPaperBroker
from liangjian_paper_broker.matching import PaperRules, local_time, number
from ..risk_engine import RiskLimits


class LocalPaperAdapter:
    def __init__(self,database,*,paper_config,risk_config,clock,calendar,run_id):
        config=yaml.safe_load(Path(paper_config).read_text(encoding='utf-8'))
        keys={f.name for f in fields(PaperRules)}-{'trade_date','trading_day'}
        if not isinstance(config,dict) or set(config)!=keys|{'initial_capital'}:
            raise ValueError('PAPER_CONFIG_KEYS_INVALID')
        limits=RiskLimits.from_yaml(risk_config)
        if number(config['initial_capital'])!=limits.initial_capital:
            raise ValueError('PAPER_INITIAL_CAPITAL_CONFLICT')
        self.policy={k:config[k] for k in keys};self.clock=clock;self.calendar=calendar
        rules=PaperRules(local_time(clock()).date().isoformat(),**self.policy)
        self.ledger=LocalPaperBroker(database,initial_capital=limits.initial_capital,clock=clock,run_id=run_id,policy=rules)

    def get_account(self):return self.ledger.get_account()
    def get_positions(self):return self.ledger.get_positions()
    def planning_snapshot(self):return self.ledger.planning_snapshot()
    def get_orders(self,trade_date):return self.ledger.get_orders(trade_date)
    def get_fills(self,trade_date):return self.ledger.get_fills(trade_date)
    def get_admissions(self,trade_date):return self.ledger.get_admissions(trade_date)
    def open_risk_day(self,trade_date):
        if not self.calendar.is_trading_day(local_time(self.clock()).date()):
            raise ValueError('PAPER_NON_TRADING_DAY')
        return self.ledger.open_risk_day(trade_date)
    def cancel_order(self,order_id):return self.ledger.cancel_order(order_id)
    def reconcile(self):return self.ledger.reconcile()
    def settle(self,trade_date):return self.ledger.settle(trade_date)

    def place_order(self,*args,**kwargs):
        if not self.calendar.is_trading_day(local_time(self.clock()).date()):
            raise ValueError('PAPER_NON_TRADING_DAY')
        return self.ledger.place_order(*args,**kwargs)

    def admit_signal(self,*args,**kwargs):
        if not self.calendar.is_trading_day(local_time(self.clock()).date()):
            raise ValueError('PAPER_NON_TRADING_DAY')
        return self.ledger.admit_signal(*args,**kwargs)

    def on_bar(self,minute_bar):
        observed=local_time(self.clock());day=local_time(minute_bar.minute_end).date()
        if not self.calendar.is_trading_day(day):raise ValueError('PAPER_NON_TRADING_DAY')
        return self.ledger.on_bar(minute_bar,PaperRules(day.isoformat(),**self.policy),observed_at=observed)


def create_broker(kind,**kwargs):
    if kind!='local_paper':
        # No gm_sim/gm_live imports, credential lookups or fallback to another account.
        raise ValueError('BROKER_ADAPTER_NOT_ENABLED')
    return LocalPaperAdapter(**kwargs)
