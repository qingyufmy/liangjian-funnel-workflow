"""Release policy for unqualified research changes, not a trading switch.

The research implementation is retained at its separate frozen Git HEAD.
This stable release has no runtime activation path. Enabling it requires a
new reviewed release with the model-quality evidence and Tony's authority.
"""
from types import MappingProxyType

RESEARCH_HEAD = 'd436e2c8fac2c057928bed1db3e11c1e9deeee78'
RESEARCH_FLAGS = MappingProxyType({
    'company_five_facet_contract': False,
    'startup_a3_qualification': False,
    'a2_research_continuation': False,
    'company_research_handoff': False,
    'a5_company_coverage_metrics': False,
    'a5_free_text_hypothesis_export': False,
})


def require_research_feature(name):
    if name not in RESEARCH_FLAGS:
        raise ValueError('RESEARCH_FEATURE_UNKNOWN')
    raise ValueError('RESEARCH_FEATURE_DISABLED_PENDING_QUALITY_GATE')
