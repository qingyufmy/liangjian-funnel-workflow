from copy import deepcopy
from dataclasses import replace

def test_new_news_contract_cannot_authenticate_deleted_coverage_with_old_c3_receipt():
    from test_team_v2_plan_geometry import projection_inputs
    from test_wp5_publication_receipt import build,bound
    from liangjian_funnel.runtime.publication_projection import capture_publication_projection
    from liangjian_funnel.pipeline.team_v2_plan_geometry import (project_stop_rr_geometry,
        STOP_PUBLICATION_CONTRACT,NEWS_STOP_PUBLICATION_CONTRACT)
    old=projection_inputs(20)
    raw=dict(old.normalized_payload.value,source_run_id=old.run_id)
    payload=project_stop_rr_geometry(raw)['payload']
    frozen=capture_publication_projection(replace(old,publication_contract=STOP_PUBLICATION_CONTRACT),
        shadow_projection=None,submitted_payload=bound(payload),published_payload=bound(deepcopy(payload)))
    assert build(frozen)['evidence_complete']
    new=replace(frozen,publication_contract=NEWS_STOP_PUBLICATION_CONTRACT)
    receipt=build(new)
    assert not receipt['evidence_complete']
    assert 'NEWS_PUBLICATION_SOURCE_RECIPE_UNPROVEN' in receipt['reason_codes']
    assert build(frozen)['evidence_complete']
