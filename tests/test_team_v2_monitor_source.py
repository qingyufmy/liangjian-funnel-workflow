from copy import deepcopy
from datetime import timedelta
import json
import time

import pytest

from test_team_v2_execution_sources import setup,read
from test_team_v2_minute_projection import source
from liangjian_funnel.data.mootdx import FetchResult
from liangjian_funnel.data.tencent_minute import MarketQuote,QuoteResult
from liangjian_funnel.runtime.team_v2_monitor_source import collect_monitor_packet
from liangjian_funnel.runtime.bounded_work import BoundedWorkGate


def prepared(tmp_path):
    settings,clock,frozen=setup(tmp_path);clock.now=clock.now.replace(hour=9,minute=32)
    scope=read(settings,clock,frozen);calls=[]
    def minute(symbol,cutoff,count,deadline):
        calls.append(('minute',symbol,cutoff,count))
        return FetchResult(symbol=symbol,interval='1m',requested_bars=count,returned_bars=1,
            bars=(source(),),reason_code='OK',complete=True)
    def quote(symbol,at,deadline):
        calls.append(('quote',symbol))
        return QuoteResult(symbol=symbol,complete=True,reason_code='OK',quote=MarketQuote(symbol=symbol,
            quote_time=clock.now,price=10,open=10,previous_close=10,volume=10000,amount=100000))
    args=dict(clock=lambda:clock.now,deadline=time.monotonic()+3,fetch_bars=minute,fetch_quote=quote)
    return settings,clock,scope,calls,args


def test_existing_source_and_publication_contract_freeze_once_without_model_authority(tmp_path):
    settings,clock,scope,calls,args=prepared(tmp_path)
    result=collect_monitor_packet(settings,scope,**args)
    assert result['status']=='CAPTURED' and len(calls)==2
    assert result['sources']['000001.SZ']['minute_complete']
    assert result['no_model_or_order_authority']
    assert result['sources']['000001.SZ']['tradability_authority']=='UNPROVEN_RULE_ADAPTER_PENDING'
    assert collect_monitor_packet(settings,scope,**args)==result and len(calls)==2


def test_tampered_packet_or_changed_scope_fails_without_refetch(tmp_path):
    settings,clock,scope,calls,args=prepared(tmp_path)
    result=collect_monitor_packet(settings,scope,**args)
    changed=deepcopy(scope);changed['morning_rowset_sha256']='f'*64
    with pytest.raises(ValueError,match='CONTROL_PACKET_SCOPE_CHANGED'):collect_monitor_packet(settings,changed,**args)
    path=next((settings.root/'state/team_v2/minutes').glob('*.json'))
    body=json.loads(path.read_text());body['captured_at']='ALTERED';path.write_text(json.dumps(body))
    with pytest.raises(ValueError,match='CONTROL_PACKET_HASH_MISMATCH'):collect_monitor_packet(settings,scope,**args)
    assert len(calls)==2


def test_future_quote_cannot_use_existing_provider_tolerance_as_authority(tmp_path):
    settings,clock,scope,calls,args=prepared(tmp_path);old=args['fetch_quote']
    def future(*args):
        value=old(*args);return value.model_copy(update={'quote':value.quote.model_copy(
            update={'quote_time':clock.now+timedelta(seconds=2)})})
    result=collect_monitor_packet(settings,scope,**{**args,'fetch_quote':future})
    assert result['status']=='DATA_LIMITED'
    assert result['sources']['000001.SZ']['quote']['reason_code']=='CONTROL_QUOTE_FUTURE'


def test_one_symbol_failure_and_deadline_do_not_fabricate_empty_success(tmp_path):
    settings,clock,scope,calls,args=prepared(tmp_path)
    def broken(*args):raise RuntimeError('DO_NOT_LOG_PROVIDER_PAYLOAD')
    result=collect_monitor_packet(settings,scope,**{**args,'fetch_bars':broken})
    assert result['status']=='DATA_LIMITED' and result['sources']['000001.SZ']['bars']==[]
    assert 'DO_NOT_LOG' not in json.dumps(result)


def test_quote_failure_does_not_discard_valid_closed_bars_for_hard_risk(tmp_path):
    settings,clock,scope,calls,args=prepared(tmp_path)
    def broken(*args):raise RuntimeError('DO_NOT_LOG_PRIVATE_QUOTE_ERROR')
    result=collect_monitor_packet(settings,scope,**{**args,'fetch_quote':broken})
    assert result['status']=='DATA_LIMITED' and result['sources']['000001.SZ']['minute_complete']
    assert result['sources']['000001.SZ']['bars'] and 'DO_NOT_LOG' not in json.dumps(result)


def test_current_packet_capture_crossing_minute_is_not_current_permission(tmp_path):
    settings,clock,scope,calls,args=prepared(tmp_path);old=args['fetch_quote']
    def crossed(*args):
        result=old(*args);clock.now+=timedelta(minutes=1);return result
    result=collect_monitor_packet(settings,scope,**{**args,'fetch_quote':crossed})
    assert result['status']=='DATA_LIMITED'
    assert {'reason_code':'CONTROL_CAPTURE_MINUTE_MOVED'} in result['reason_codes']


def test_non_current_lunch_window_never_fetches_or_fabricates_bar(tmp_path):
    settings,clock,scope,calls,args=prepared(tmp_path);clock.now=clock.now.replace(hour=13,minute=0)
    result=collect_monitor_packet(settings,scope,**args)
    assert result['status']=='NOOP' and not calls


def test_past_global_deadline_makes_no_source_call(tmp_path):
    settings,clock,scope,calls,args=prepared(tmp_path)
    result=collect_monitor_packet(settings,scope,**{**args,'deadline':time.monotonic()-1})
    assert result['status']=='DATA_LIMITED' and not calls
