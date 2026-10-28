"""0995 authorized projections on actual model failures; structural gates remain."""
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
import json
import pytest

from liangjian_funnel.pipeline.team_v2_pm_schema import project_pm_compact,project_pm_notes
from liangjian_funnel.pipeline.team_v2_trader_schema import project_trader_references,validate_trader_output

SOURCE=Path(__file__).parent/'fixtures/postclose_20261027/quality_failures_0995_original.json'

def original(case):return json.loads(SOURCE.read_bytes())['cases'][case]

def trader_context(x):
    raw=x['raw']
    return dict(required_envelope=x['envelope'],decision_id=raw['decision_id'],
        minute_snapshot_id=raw['minute_snapshot_id'],plans={p['plan_id']:p['symbol'] for p in x['plans']},
        held_symbols=set(),tech_reference=x['tech_reference'],
        candidate_actions={p['plan_id']:'BUY_SIGNAL' for p in x['plans']},position_candidate_actions={})

def pm_context(x):
    from test_effective_a6_lj099 import _envelopes
    items=[i for i in x['catalog']['items'] if i['category']=='PLAN']
    return dict(plan_refs={i['id']:i['value']['plan_id'] for i in items},required_envelope=x['envelope'],
        envelopes=_envelopes(x['envelopes']),plan_clusters={i['value']['plan_id']:i['value']['cluster'] for i in items},
        free_slots=x['baseline']['free_slots'],baseline_range=tuple(Decimal(v) for v in x['baseline']['baseline_range']),
        utilization_cap=Decimal(x['baseline']['utilization_cap']),high_conviction_ids=set())

def test_real_1406_unknown_reference_dropped_audited_action_and_raw_unchanged():
    x=original('A4_1406');raw=x['raw'];before=deepcopy(raw)
    projected,audit=project_trader_references(raw,x['tech_reference'])
    checked=validate_trader_output(raw,x['catalog'],**trader_context(x))
    assert checked.valid,checked.issues
    dropped=[c for c in audit if c.get('code')=='UNKNOWN_REFERENCE_ITEM_DROPPED']
    assert len(dropped)==1 and dropped[0]['item']=='TREND_15M_PRESSURE_EASING'
    assert dropped[0]['original']['my_read']==dropped[0]['my_read']
    assert raw==before and projected['signals'][0]['action']==raw['signals'][0]['action']=='NO_ACTION'

@pytest.mark.parametrize('kind',['required_item','identity','unknown_fields','malformed_read','invalid_status','action'])
def test_unknown_reference_projection_never_repairs_missing_required_item_or_identity(kind):
    x=original('A4_1406');raw=x['raw'];row=raw['signals'][0]
    if kind=='required_item':
        # The real 14:06 source happens to have no UNMET item. Controlled
        # negative: one existing authoritative item becomes required UNMET.
        pid=row['plan_id'];required=next(iter(x['tech_reference'][pid]))
        x['tech_reference'][pid][required]='UNMET'
        row['tech_reference_review']=[r for r in row['tech_reference_review']
            if x['tech_reference'][row['plan_id']].get(r['item'])!='UNMET']
    elif kind=='identity':raw['envelope']['run_id']='foreign'
    elif kind=='unknown_fields':row['tech_reference_review'].append(dict(item='UNKNOWN',my_read='read',shares=100))
    elif kind=='malformed_read':row['tech_reference_review'].append(dict(item='UNKNOWN',my_read=[]))
    elif kind=='invalid_status':row['tech_reference_review'].append(dict(item='UNKNOWN',my_read='read',status='READY'))
    elif kind=='action':row['action']='BUY_NOW'
    assert not validate_trader_output(raw,x['catalog'],**trader_context(x)).valid

def test_real_pm_note_truncation_keeps_originals_and_decision_structure():
    x=original('PM');before=deepcopy(x['raw'])
    projected,checked=project_pm_compact(x['raw'],x['catalog'],**pm_context(x))
    assert checked.valid,checked.issues
    changes=projected['note_normalization']
    assert [v['original_chars'] for v in changes]==[50,43]
    assert all(v['code']=='NOTE_TRUNCATED' and len(v['projected'])==40 for v in changes)
    normalized,_=project_pm_notes(x['raw'])
    for raw,row in zip(before['plan_decisions'],normalized['plan_decisions']):
        assert {k:v for k,v in raw.items() if k!='note'}=={k:v for k,v in row.items() if k!='note'}
    assert x['raw']==before

@pytest.mark.parametrize('field,value',[('decision','BUY'),('size_tier','ALL_IN'),('plan_ref','FOREIGN'),
    ('reason_code','ARBITRARY'),('note',None),('note',[]),('note','')])
def test_note_projection_never_repairs_structural_errors(field,value):
    x=original('PM');x['raw']['plan_decisions'][0][field]=value
    assert not project_pm_compact(x['raw'],x['catalog'],**pm_context(x))[1].valid

def test_native_pm_journal_preserves_raw_note_and_has_truncation_audit(tmp_path):
    from test_team_v2_pm_session import fixture
    from liangjian_funnel.runtime.team_v2_pm_session import run_pm_premarket
    import sqlite3
    # Fixture transport already uses the real native stream client.
    import test_team_v2_pm_session as fixtures
    original_response=fixtures.response
    from unittest.mock import patch
    def long_note():
        out=original_response();out['plan_decisions'][0]['note']='原始备注'*20;return out
    with patch.object(fixtures,'response',long_note):args,seen=fixture(tmp_path)
    result=run_pm_premarket(**args)
    assert result['model_validated'] and len(seen)==1
    assert result['server_projection']['note_normalization'][0]['original']=='原始备注'*20
    with sqlite3.connect(tmp_path/'model.sqlite3') as db:
        raw=json.loads(db.execute('SELECT output_json FROM model_calls').fetchone()[0])
    assert raw['plan_decisions'][0]['note']=='原始备注'*20

def test_a6_quota_uses_original_messages_without_extra_lj099_instruction(tmp_path,monkeypatch):
    from test_team_v2_risk_session import quota_fixture
    from liangjian_funnel.runtime.team_v2_risk_session import run_risk_premarket
    args,seen,_=quota_fixture(tmp_path);repo=args['repository'];prepared=[];prepare=repo.prepare_messages
    def capture(*a,**kw):
        request=prepare(*a,**kw);prepared.append(request);return request
    monkeypatch.setattr(repo,'prepare_messages',capture)
    run_risk_premarket(**args)
    assert len(seen)==1 and seen[0]['messages']==list(prepared[-1].messages)
    assert 'CLAUDE_0937_LJ099' not in json.dumps(seen[0]['messages'])
