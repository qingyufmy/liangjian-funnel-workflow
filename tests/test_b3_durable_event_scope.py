"""A current-round publisher must not load unrelated effective history."""
from copy import deepcopy
import json

import pytest

from liangjian_funnel.workflow import _effective_events_for_identities


def row(identity, *, event_id='e', effective=1):
    lane, plan, action, minute = identity
    return {'event_id':event_id,'lane_id':lane,'payload_json':json.dumps({'plan_id':plan}),
            'action':action,'minute_end':minute,'effective':effective}


class Store:
    def __init__(self, rows):
        self.rows, self.calls = rows, []

    def list_monitor_events(self, **kwargs):
        self.calls.append(kwargs)
        assert kwargs.get('effective_only') is True
        assert kwargs.get('from_time') is not None and kwargs.get('to_time') is not None
        return tuple(item for item in self.rows if item['lane_id']==kwargs['lane_id']
            and item['effective'] and kwargs['from_time']<=item['minute_end']<=kwargs['to_time'])


def legacy_filter(rows, identities):
    return [item for item in rows if (item['lane_id'],json.loads(item['payload_json']).get('plan_id') or '',
                                      item['action'],item['minute_end']) in identities]


def test_no_effective_events_means_no_history_read():
    store=Store([])
    assert _effective_events_for_identities(store,lane_id='lane_1',identities=set())==[]
    assert store.calls==[]


def test_only_identity_minute_span_is_read_and_exact_old_filter_is_retained():
    a=('lane_1','p','BUY_SIGNAL','2026-10-15T10:01:00+08:00')
    b=('lane_1','q','PLAN_INVALIDATED','2026-10-15T10:03:00+08:00')
    identities={a,b}
    rows=[row(('lane_1','old','BUY_SIGNAL','2026-09-18T10:01:00+08:00')),
          row(a,event_id='a'),row(('lane_1','other','BUY_SIGNAL',a[3])),
          row(b,event_id='b'),row(('lane_1','future','BUY_SIGNAL','2026-10-16T10:01:00+08:00'))]
    original=deepcopy(rows);store=Store(rows)
    actual=_effective_events_for_identities(store,lane_id='lane_1',identities=identities)
    assert actual==legacy_filter(rows,identities)
    assert store.calls==[{'lane_id':'lane_1','effective_only':True,'from_time':a[3],'to_time':b[3]}]
    assert rows==original


def test_lanes_actions_and_plan_ids_are_not_replaced_by_symbols_or_minute_only():
    identity=('lane_1','p','BUY_SIGNAL','2026-10-15T10:01:00+08:00')
    rows=[row(identity,event_id='wanted'),row(('lane_1','q','BUY_SIGNAL',identity[3])),
          row(('lane_1','p','LLM_VETO',identity[3])),row(('lane_2','p','BUY_SIGNAL',identity[3]))]
    result=_effective_events_for_identities(Store(rows),lane_id='lane_1',identities={identity})
    assert [item['event_id'] for item in result]==['wanted']


def test_duplicate_durable_rows_are_not_silently_deduplicated_or_filled():
    identity=('lane_1','p','BUY_SIGNAL','2026-10-15T10:01:00+08:00')
    rows=[row(identity,event_id='a'),row(identity,event_id='b')]
    assert _effective_events_for_identities(Store(rows),lane_id='lane_1',identities={identity})==rows
    assert _effective_events_for_identities(Store([]),lane_id='lane_1',identities={identity})==[]


def test_malformed_current_evidence_is_not_silently_treated_as_valid():
    identity=('lane_1','p','BUY_SIGNAL','2026-10-15T10:01:00+08:00')
    item=row(identity);item['payload_json']='not-json'
    with pytest.raises(ValueError):
        _effective_events_for_identities(Store([item]),lane_id='lane_1',identities={identity})
