"""A2 transport is reversible representation, not an information filter."""
from copy import deepcopy
import hashlib,json
from pathlib import Path
import pytest

from liangjian_funnel.pipeline.team_v2_a2_projection import prepare_a2_transport_values,expand_a2_transport_values


def values():
    fact={'description':'Frozen factual source, not an execution instruction. '*8,
        'available':True,'source_refs':['original:fact:'+str(i) for i in range(8)]}
    rows={str(i):{'score':i,'facts':deepcopy(fact),'nullable':None,**({'optional':False} if i%2 else {})}
        for i in range(10)}
    return {'A2_BOTTLENECK_CONTEXT':{**rows,'_rotation_review_scope':{'D':list(rows)},
        '_rotation_review_contract':{'allowed_direction_ids':['D']},'_theme_review_scope':{'T':list(rows)}},
        'UPSTREAM_ACTIVE_POOL':{'active_research_pool':[{'symbol':s,'facts':deepcopy(fact)} for s in rows]},
        'A2_QUANT_FILTERED_SAMPLE':[{'symbol':'filtered','quant_filter_reason':'ORIGINAL','facts':deepcopy(fact)}],
        'EVIDENCE_CATALOG':{'categories':['QUANT'],'items':[{'id':'A2:QUANT:'+s,'sha256':'original:'+s} for s in rows]},
        'MARKET_EMOTION_SNAPSHOT':{'emotion_cycle_stage':'DIVERGENCE','new_long_permission':'ALLOW_TREND_ONLY'}}


def test_lossless_all_columns_facts_hashes_and_permissions_no_missing_null_conflation():
    original=values();before=deepcopy(original)
    projected,audit=prepare_a2_transport_values(original)
    assert audit['lossless_inverse_verified'] and audit['shared_fact_count']>0
    assert expand_a2_transport_values(projected)==original==before
    assert projected['EVIDENCE_CATALOG']==original['EVIDENCE_CATALOG']
    assert projected['MARKET_EMOTION_SNAPSHOT']==original['MARKET_EMOTION_SNAPSHOT']
    rows=expand_a2_transport_values(projected)['A2_BOTTLENECK_CONTEXT']
    assert 'optional' not in rows['0'] and rows['0']['nullable'] is None and rows['1']['optional'] is False
    assert prepare_a2_transport_values(original)==(projected,audit)


@pytest.mark.parametrize('collision',[{'catalog_ref':'A2:FACT:0001'},{'$a2_missing':True}])
def test_original_reserved_values_fail_before_model_instead_of_ambiguous_decode(collision):
    source=values();source['A2_BOTTLENECK_CONTEXT']['0']['facts']=collision
    with pytest.raises(ValueError,match='RESERVED_VALUE'):prepare_a2_transport_values(source)


@pytest.mark.parametrize('mutation',['cycle','unknown','sha','short','duplicate_columns'])
def test_corrupt_transports_never_silently_change_original_facts(mutation):
    projected,_=prepare_a2_transport_values(values())
    quant=projected['A2_BOTTLENECK_CONTEXT'];catalog=quant['_transport']['catalog'];identity=next(iter(catalog))
    if mutation=='cycle':catalog[identity]['value']={'catalog_ref':identity}
    elif mutation=='unknown':catalog[identity]['value']={'catalog_ref':'A2:FACT:UNKNOWN'}
    elif mutation=='sha':catalog[identity]['sha256']='0'*64
    elif mutation=='short':quant['_stock_table']['rows'][0].pop()
    else:quant['_stock_table']['columns'].append(quant['_stock_table']['columns'][0])
    with pytest.raises(ValueError):expand_a2_transport_values(projected)


def test_source_stock_symbol_field_cannot_be_shadowed_by_table_identity():
    source=values();source['A2_BOTTLENECK_CONTEXT']['0']['symbol']='other'
    with pytest.raises(ValueError,match='ROW_INVALID'):prepare_a2_transport_values(source)


def test_actual_a2_call_keeps_original_facts_and_records_exact_transport_binding(tmp_path):
    from test_team_v2_a2_research import fixture
    app,args,seen=fixture(tmp_path)
    before=deepcopy(dict(args['snapshot'].data))
    result=app._run_stage(**args)
    assert result.status=='VALIDATED',result.reason_codes
    assert len(seen)==1 and 'a2-lossless-model-input/1' in seen[0]['messages'][0]['content']
    from liangjian_funnel.pipeline.team_v2_prompts import _VERSIONS
    assert result.output['envelope']['prompt_version']==_VERSIONS['A2']
    assert dict(args['snapshot'].data)==before
    audit=result.diagnostics['input_projections'][0]
    assert audit['lossless_inverse_verified'] and audit['original_evidence_catalog_unchanged']
    import sqlite3
    with sqlite3.connect(app.team_research_adapter.store.path) as db:
        source=json.loads(db.execute("SELECT body_json FROM control_minute_context WHERE decision_id LIKE 'a2-research-source:%'").fetchone()[0])
    assert '_stock_table' not in source['values']['A2_BOTTLENECK_CONTEXT']
    assert source['envelope']['prompt_version']==_VERSIONS['A2']
    compact,projection=prepare_a2_transport_values(source['values'])
    replay=app.team_research_adapter.repository.prepare_messages('A2',compact,
        required_envelope=source['envelope'],session='CLOSE_REVIEW')
    assert [dict(m) for m in replay.messages]==seen[0]['messages']
    assert replay.input_hash==audit['input_hash']==source['request_transport_projection']['input_hash']
    assert projection['source_values_sha256']==audit['source_values_sha256']
