from dataclasses import replace
from datetime import datetime
from decimal import Decimal as D
from pathlib import Path
from types import SimpleNamespace
import pytest
from liangjian_funnel.runtime.broker.local_paper import create_broker
from liangjian_paper_broker.matching import MinuteBar


def make(tmp_path,*,calendar=None):
    root=Path(__file__).resolve().parents[1]
    clock=SimpleNamespace(now=datetime.fromisoformat('2026-10-19T10:00:00+08:00'))
    b=create_broker('local_paper',database=tmp_path/'paper.db',paper_config=root/'config/paper_broker.yaml',
                    risk_config=root/'config/risk_limits.yaml',clock=lambda:clock.now,
                    calendar=calendar or SimpleNamespace(is_trading_day=lambda _:True),run_id='isolated-test')
    return b,clock


@pytest.mark.parametrize('kind',['gm_live','gm_sim','qmt','unknown',''])
def test_unimplemented_brokers_do_not_silently_acquire_real_authority(kind):
    with pytest.raises(ValueError,match='NOT_ENABLED'):create_broker(kind)


def test_adapter_uses_yaml_fees_and_actual_arrival_clock(tmp_path):
    b,clock=make(tmp_path)
    b.place_order('000001.SZ','BUY',1000,order_type='LIMIT',limit_price='10',client_order_id='c1',
                  plan_id='p1',signal_id='s1',no_chase_price='10.2',tradable=True)
    end=datetime.fromisoformat('2026-10-19T10:01:00+08:00')
    bar=MinuteBar('000001.SZ',end,D('9.9'),D('10.1'),D('10'),100000,
                  closed=True,tradability_known=True,close=D('10'))
    with pytest.raises(ValueError,match='FUTURE_BAR'):b.on_bar(bar)
    clock.now=end
    fills=b.on_bar(bar)
    assert len(fills)==1 and fills[0]['fee']=='5.00'
    assert b.reconcile()['status']=='MATCH'


def test_non_trading_day_rejects_before_accepting_order(tmp_path):
    b,_=make(tmp_path,calendar=SimpleNamespace(is_trading_day=lambda _:False))
    with pytest.raises(ValueError,match='NON_TRADING_DAY'):
        b.place_order('000001.SZ','BUY',1000,order_type='LIMIT',limit_price='10',client_order_id='c1',
                      plan_id='p1',signal_id='s1',no_chase_price='10.2',tradable=True)
    assert b.get_orders('2026-10-19')==()
