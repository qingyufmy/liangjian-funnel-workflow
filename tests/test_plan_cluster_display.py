import json
from liangjian_funnel.runtime.plan_cluster_display import project_cluster_display,FIELD
from liangjian_funnel.review.observability import _cluster


def test_existing_runtime_resolution_ignores_stale_display_and_is_not_changed():
    raw={'symbol':'601288.SH','cluster':'UNKNOWN','theme_id':'BANKS','industry_name':'银行'}
    out=project_cluster_display(raw)
    assert raw['cluster']=='UNKNOWN' and out['cluster']=='银行'
    assert _cluster({'payload_json':json.dumps(raw)})==_cluster({'payload_json':json.dumps(out)})
    assert out[FIELD]['basis']=='industry_name'
    assert project_cluster_display(out)==out


def test_missing_identity_stays_unknown_not_code_prefix_inference():
    assert project_cluster_display({'symbol':'601288.SH'})['cluster']=='UNKNOWN'
