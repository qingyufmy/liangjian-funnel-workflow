from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
import pytest

from liangjian_funnel.runtime.team_v2_trader_inputs import transition_model_scope
from liangjian_funnel.runtime.team_v2_compact_bars import encode_bars,decode_bars
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
from test_team_v2_risk_engine import plan
from liangjian_funnel.runtime.team_v2_sources import stamp


def test_real_load_shape_43_transitions_only24_sent_rank_and_held_first_19_deferred():
    plans={f'p{i:02d}':replace(plan(),plan_id=f'p{i:02d}') for i in range(43)}
    result=transition_model_scope(plans,active_plan_ids=set(plans),held_plan_ids={'held':'p42'},
        evaluations={pid:{'action':'START_CONFIRMATION'} for pid in plans},
        at=stamp('2026-10-13T10:01:00+08:00'),previous={},ranks={'p41':1})
    assert result['plan_ids'][:2]==['p42','p41'] and len(result['plan_ids'])==24
    assert sum(r['reason_code']=='DEFERRED_CAPACITY' for r in result['decisions'])==19
    assert sum(r['selected_for_model'] for r in result['decisions'])==24
    assert not any(r['model_judgment_denominator'] for r in result['decisions'])


def test_restart_carry_wait_heartbeat_transition_and_unsent_retry_are_not_fake_model_votes(tmp_path):
    store=TeamMeetingStore(tmp_path/'team.db');plans={'p1':plan()};at=stamp('2026-10-19T10:01:00+08:00')
    def select(tick,previous,action='START_CONFIRMATION'):
        return transition_model_scope(plans,active_plan_ids={'p1'},held_plan_ids={},
            evaluations={'p1':{'action':action}},at=tick,previous=previous)
    def freeze(n,action='START_CONFIRMATION'):
        tick=at+timedelta(minutes=n)
        return store.freeze_trader_model_scope(account_id='a',trade_date='2026-10-19',decision_id=str(n),
            observed_at=tick,source_sha256=(str(n)+'a'*64)[:64],selector=lambda previous:select(tick,previous,action))
    assert freeze(0)['plan_ids']==['p1']
    # Rendering/source failures never mark the request as sent.
    assert freeze(5)['decisions'][0]['reason_code']=='DEFERRED_CAPACITY_RETRY'
    store.record_trader_model_sent(account_id='a',trade_date='2026-10-19',plan_ids={'p1'},observed_at=at+timedelta(minutes=5))
    store=TeamMeetingStore(tmp_path/'team.db')
    carried=freeze(10)
    assert carried['plan_ids']==[] and carried['decisions'][0]['reason_code']=='CARRIED_WAIT'
    assert not carried['decisions'][0]['model_judgment_denominator']
    assert freeze(20)['decisions'][0]['reason_code']=='OBSERVATION_HEARTBEAT_15M'
    assert freeze(25,'BUY_SIGNAL')['decisions'][0]['reason_code']=='ENGINE_STATE_TRANSITION'
    assert freeze(25,'BUY_SIGNAL')==freeze(25,'BUY_SIGNAL')


def test_column_encoding_exact_inverse_preserves_amount_estimates_provider_identity_and_missing():
    rows=[{'bar_end':'2026-10-13T09:35:00+08:00','open':10.1,'high':11,'low':10,'close':10.5,
        'volume':5000,'amount':52500.00000000001,'symbol':'p','source_id':'original','evidence_kind':'MARKET_BAR'},
        {'bar_end':'2026-10-13T09:36:00+08:00','open':10.5,'high':11,'low':10,'close':10.6,
        'volume':6000,'amount':None,'symbol':'p','source_id':'original','amount_kind':'ohlc_estimate'}]
    packed=encode_bars(rows)
    assert decode_bars(packed)==rows and 'vwap' not in packed
    assert packed['constants']['source_id']=='original' and packed['count']==2


def test_never_sent_out_of_window_is_not_mislabeled_deferred_but_gets_15m_heartbeat():
    at=stamp('2026-10-19T10:01:00+08:00')
    plans={'p1':replace(plan(),price=Decimal(15),entry_low=Decimal(10),entry_high=Decimal(11),invalidation_price=Decimal(9))}
    kwargs=dict(plans=plans,active_plan_ids={'p1'},held_plan_ids={},evaluations={'p1':{'action':'NO_ACTION'}},atr5m={'p1':Decimal('.1')})
    first=transition_model_scope(**kwargs,at=at,previous={})
    previous={'p1':dict(state=first['observation_states']['p1'],observed_at=at.isoformat(),last_sent_at=None)}
    assert transition_model_scope(**kwargs,at=at+timedelta(minutes=5),previous=previous)['decisions'][0]['reason_code']=='CARRIED_WAIT'
    assert transition_model_scope(**kwargs,at=at+timedelta(minutes=15),previous=previous)['decisions'][0]['reason_code']=='OBSERVATION_HEARTBEAT_15M'
