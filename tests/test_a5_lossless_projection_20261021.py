from copy import deepcopy
import json
import pytest
from liangjian_funnel.pipeline.team_v2_a5_projection import project_review_facts,expand_review_facts
from liangjian_funnel.pipeline.numeric_fact_domain import numeric_fact_domain


def test_repeated_encoded_db_rows_retain_exact_bytes_and_numeric_domain():
    row={'risk_budget':15000,'long_evidence':'真实来源 '*100,'items':[{'x':0,'symbol':'002270.SZ'}]}
    encoded=json.dumps(row,ensure_ascii=False,sort_keys=True,separators=(',',':'))
    facts={'a6':{'row_json':encoded},'legacy':{'row_json':encoded},'standalone':row,
        'citation_catalog':{'items':[{'id':'A5:A6','sha256':'a'*64}]}}
    original=deepcopy(facts)
    transport,audit=project_review_facts(facts)
    rebuilt=expand_review_facts(transport)
    assert rebuilt==facts==original
    assert numeric_fact_domain(rebuilt)==numeric_fact_domain(facts)
    assert audit['transport_chars']<audit['source_chars']
    assert audit['lossless_inverse_verified']


def test_noncanonical_json_whitespace_and_empty_missing_values_remain_exact():
    facts={'text':'{ "budget": 15000.0 }','blank':[],'absent':None,'false':False}
    transport,_=project_review_facts(facts)
    assert expand_review_facts(transport)==facts


@pytest.mark.parametrize('key',['$a5_fact_ref','$a5_canonical_json','_a5_transport','$a5_rows'])
def test_collision_cannot_be_interpreted_as_server_transport(key):
    with pytest.raises(ValueError,match='A5_TRANSPORT_RESERVED_KEY'):
        project_review_facts({'nested':{key:'outside'}})


def test_tampered_fact_is_rejected():
    value,_=project_review_facts({'budget':15000});value['budget']=123
    with pytest.raises(ValueError,match='A5_TRANSPORT_FACT_HASH_MISMATCH'):
        expand_review_facts(value)


def test_row_tables_preserve_missing_null_nested_values_and_numbers():
    facts={'observations':[{'a_very_long_field_name':i,'another_long_field_name':None,
        'nested_long_field_name':{'budget':i*100},**({'optional_long_field_name':False} if i%2 else {})} for i in range(30)]}
    projected,audit=project_review_facts(facts)
    assert '$a5_rows' in json.dumps(projected)
    assert expand_review_facts(projected)==facts
    assert numeric_fact_domain(expand_review_facts(projected))==numeric_fact_domain(facts)
    assert audit['transport_chars']<audit['source_chars']
