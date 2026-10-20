from copy import deepcopy
from datetime import datetime

import pytest

from test_team_v2_minute_projection import source
from liangjian_funnel.runtime.team_v2_monitor_health import (
    assess_monitor_packet, load_monitor_health_policy, project_packet_sources)

NOW = datetime.fromisoformat('2026-10-19T09:32:00+08:00')
POLICY = dict(schema_version='team-monitor-health/1', max_invalid_symbol_ratio='0.50')


def packet(count=3):
    symbols = [f'{n:06}.SZ' for n in range(1, count+1)]
    return dict(binding=dict(symbols=symbols), captured_at=NOW.isoformat(),
        execution_cutoff=NOW.replace(minute=31).isoformat(), reason_codes=[],
        sources={s:dict(minute_complete=True, quote=dict(complete=True),
            bars=[source(symbol=s).model_dump(mode='json')]) for s in symbols})


def health(value, **live):
    return assess_monitor_packet(value, live=dict(status='READY', **live), policy=POLICY)


def test_one_no_trade_symbol_is_not_data_failure_and_has_no_paper_bar():
    value = packet(33)
    value['sources']['000001.SZ']['bars'] = [source(open=10, high=10, low=10, close=10,
        volume=0, amount=0).model_dump(mode='json')]
    original = deepcopy(value)
    projected = project_packet_sources(value)
    result = health(value)
    assert len(result['ready_symbols']) == 32
    assert result['no_trade_symbols'] == ['000001.SZ'] and result['invalid_symbols'] == []
    assert result['account_record_required'] is False
    assert projected['000001.SZ']['status'] == 'NO_TRADES'
    assert projected['000001.SZ']['paper_bar'] is None
    assert projected['000001.SZ']['last_price'] == '10.0'
    assert value == original


def test_zero_volume_nonflat_or_positive_turnover_is_invalid_not_no_trade():
    for change in (dict(volume=0, amount=0), dict(open=10,high=10,low=10,close=10,volume=0,amount=1)):
        value = packet()
        value['sources']['000001.SZ']['bars'] = [source(**change).model_dump(mode='json')]
        result = health(value)
        assert result['invalid_symbols'] == ['000001.SZ'] and not result['no_trade_symbols']
        assert result['account_record_required'] is False


def test_missing_symbol_is_local_and_exact_half_does_not_trigger_global_lock():
    value = packet(2)
    value['sources'].pop('000001.SZ')
    result = health(value)
    assert result['status'] == 'SYMBOL_ISOLATED' and not result['account_record_required']
    assert result['invalid_symbol_ratio'] == '0.5'


def test_failure_above_threshold_and_total_outage_require_account_record():
    value = packet()
    value['sources'].pop('000001.SZ'); value['sources'].pop('000002.SZ')
    result = health(value)
    assert result['account_record_required']
    assert 'CONTROL_INVALID_SYMBOL_RATIO_EXCEEDED' in result['global_reason_codes']
    value['sources'].clear()
    assert 'CONTROL_ALL_SYMBOL_SOURCES_UNAVAILABLE' in health(value)['global_reason_codes']


def test_complete_no_trade_scope_is_market_fact_not_global_source_outage():
    value = packet()
    for symbol in value['sources']:
        value['sources'][symbol]['bars'] = [source(symbol=symbol,open=10,high=10,low=10,close=10,
            volume=0,amount=0).model_dump(mode='json')]
    result = health(value)
    assert not result['account_record_required'] and not result['invalid_symbols']
    assert len(result['no_trade_symbols']) == 3


@pytest.mark.parametrize('live,reason',[
    ({'status':'DATA_BLOCKED'}, 'CONTROL_MARKET_SOURCE_UNAVAILABLE'),
    ({'status':'READY','benchmark_status':'MISSING'}, 'CONTROL_BENCHMARK_UNAVAILABLE')])
def test_market_or_explicit_benchmark_failure_is_global_even_with_good_symbols(live,reason):
    result = assess_monitor_packet(packet(), live=live, policy=POLICY)
    assert result['account_record_required'] and reason in result['global_reason_codes']


def test_unknown_optional_benchmark_diagnostic_is_not_fabricated_as_verified():
    assert health(packet())['benchmark_evidence'] == 'EXISTING_LIVE_SOURCE_CONTRACT_ONLY'


def test_single_symbol_rule_adapter_failure_isolated_but_broad_failure_is_global():
    value=packet(3)
    failures={'000001.SZ':['CONTROL_SYMBOL_TRADABILITY_FAILED']}
    result=assess_monitor_packet(value,live={'status':'READY'},policy=POLICY,symbol_failures=failures)
    assert not result['account_record_required'] and result['invalid_symbols']==['000001.SZ']
    failures['000002.SZ']=['CONTROL_PRODUCTION_CONTRACT_UNPROVEN']
    assert assess_monitor_packet(value,live={'status':'READY'},policy=POLICY,
        symbol_failures=failures)['account_record_required']


def test_policy_loader_same_config_default_and_rejects_invalid(tmp_path):
    target = tmp_path/'health.yaml'
    assert load_monitor_health_policy(target)['max_invalid_symbol_ratio'] == '0.50'
    target.write_text('schema_version: team-monitor-health/1\nmax_invalid_symbol_ratio: 1\n')
    with pytest.raises(ValueError,match='POLICY_INVALID'):
        load_monitor_health_policy(target)


def test_dispatch_isolated_symbol_preserves_customer_and_never_latches(tmp_path,monkeypatch):
    from test_team_v2_dual_monitor import app_fixture,NOW as tick
    from liangjian_funnel.runtime.team_v2_dual_monitor import run_dual_monitor
    from liangjian_funnel.runtime.team_v2_first_day import forced_record
    from liangjian_funnel.runtime.team_v2_morning import ACCOUNT
    import liangjian_funnel.runtime.team_v2_monitor as monitor
    app=app_fixture(tmp_path); app.monitor_once=lambda **kwargs:{'notifications':['customer-original']}
    monkeypatch.setattr(monitor,'run_monitor_control',lambda *a,**kw:{'status':'SYMBOL_ISOLATED',
        'source_health':health(packet())})
    result=run_dual_monitor(app,now=tick)
    assert result['notifications']==['customer-original'] and result['mode']=='control'
    assert forced_record(app.settings,account_id=ACCOUNT,now=tick) is None


def test_actual_monitor_no_trade_round_skips_entry_without_global_degradation(tmp_path,monkeypatch):
    from test_team_v2_monitor_dispatch import prepared
    from liangjian_funnel.runtime.team_v2_monitor import run_monitor_control
    from liangjian_funnel.runtime.team_v2_dual_monitor import run_dual_monitor
    from liangjian_funnel.runtime.team_v2_first_day import forced_record
    from liangjian_funnel.runtime.team_v2_morning import ACCOUNT
    import liangjian_funnel.runtime.team_v2_monitor as monitor
    app, clock, seen, api, proof = prepared(tmp_path,monkeypatch)
    original = app._fetch_live_bars
    def bars(*args, **kwargs):
        result=original(*args, **kwargs); rows=list(result.bars)
        rows[-1]=rows[-1].model_copy(update=dict(open=10,high=10,low=10,close=10,volume=0,amount=0))
        return result.model_copy(update={'bars':tuple(rows)})
    app._fetch_live_bars=bars
    app.monitor_once=lambda **kwargs:{'notifications':['customer-original'],'live_market_state':{
        'status':'READY','as_of':clock.now.isoformat(),'entry_permission':'ALLOW'}}
    monkeypatch.setattr(monitor,'run_monitor_control',lambda owner,**kw:run_monitor_control(owner,
        clock=lambda:clock.now,client=api,tradability_loader=proof,**kw))
    dispatched=run_dual_monitor(app,now=clock.now)
    result=dispatched['team']
    assert result['status']=='SYMBOL_ISOLATED'
    assert dispatched['mode']=='control' and dispatched['notifications']==['customer-original']
    assert forced_record(app.settings,account_id=ACCOUNT,now=clock.now) is None
    assert not result['source_health']['account_record_required']
    assert result['source_health']['no_trade_symbols']==['000001.SZ']
    assert not seen and not result['result']['trader']


def test_actual_monitor_global_source_outage_blocks_valid_stock_before_account_latch(tmp_path,monkeypatch):
    from test_team_v2_monitor_dispatch import prepared
    from liangjian_funnel.runtime.team_v2_monitor import run_monitor_control
    import liangjian_funnel.runtime.live_market as live
    app,clock,seen,api,proof=prepared(tmp_path,monkeypatch)
    monkeypatch.setattr(live,'load_or_refresh_live_market_state',lambda *a,**kw:{'status':'DATA_BLOCKED'})
    result=run_monitor_control(app,now=clock.now,clock=lambda:clock.now,client=api,tradability_loader=proof)
    assert result['status']=='DEGRADED' and result['source_health']['account_record_required']
    assert not seen and not result['result']['trader']['admissions']
