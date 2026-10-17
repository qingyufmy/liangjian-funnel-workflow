from copy import deepcopy
from liangjian_funnel.pipeline.team_v2_a2_schema import frozen_domain_diagnostics


def test_original_symbol_theme_direction_values_are_recorded_not_normalized():
    raw={'focus_decisions':[{'symbol':'600000.SH'},{'symbol':'outside'}],
        'theme_reviews':[{'theme_id':'foreign-theme'}],
        'market_read':{'dominant_themes':['allowed','another']},
        'rotation_reviews':[{'rotation_direction_id':'covered','reviewed_symbols':['600000.SH']},
            {'rotation_direction_id':'needs','reviewed_symbols':['outside']} ]}
    original=deepcopy(raw)
    result=frozen_domain_diagnostics(raw,candidate_ids={'600000.SH'},allowed_theme_ids={'allowed'},
        direction_symbols={'covered':{'600000.SH'},'needs':{'000001.SZ'}})
    assert raw==original
    assert result['outside_symbols']==[{'pool':'focus_decisions','symbol':'outside'}]
    assert result['outside_themes']==['foreign-theme','another']
    assert result['expected_no_focus_directions']==['needs'] and len(result['invalid_direction_reviews'])==2
