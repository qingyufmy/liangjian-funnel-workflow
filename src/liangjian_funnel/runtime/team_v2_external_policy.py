"""Versioned, explicit Tony/Claude external adjustment values from risk YAML."""
from decimal import Decimal,InvalidOperation
from collections.abc import Mapping

KEYS=frozenset('version max_age_hours missing_adjustment_pp contraction_adjustment_pp '
    'nasdaq_contraction_lte_pct a50_contraction_lte_pct expansion_enabled expansion_adjustment_pp '
    'nasdaq_expansion_gte_pct a50_expansion_gte_pct expansion_vix_lte maximum_utilization_pct'.split())


def validate_external_policy(value):
    try:
        if (not isinstance(value,Mapping) or set(value)!=KEYS or value['version']!='external-adjustment/1'
                or type(value['expansion_enabled']) is not bool):raise ValueError
        for key in ('max_age_hours','maximum_utilization_pct','missing_adjustment_pp',
                'contraction_adjustment_pp','expansion_adjustment_pp'):
            if type(value[key]) is not int:raise ValueError
        if (not 0<value['max_age_hours']<=24 or not 0<=value['maximum_utilization_pct']<=95
                or not -100<=value['missing_adjustment_pp']<=0
                or not -100<=value['contraction_adjustment_pp']<=0
                or not 0<=value['expansion_adjustment_pp']<=100):raise ValueError
        for key in ('nasdaq_contraction_lte_pct','a50_contraction_lte_pct',
                'nasdaq_expansion_gte_pct','a50_expansion_gte_pct','expansion_vix_lte'):
            if isinstance(value[key],bool) or not Decimal(str(value[key])).is_finite():raise ValueError
        if (any(Decimal(str(value[k]))>=0 for k in ('nasdaq_contraction_lte_pct','a50_contraction_lte_pct'))
                or any(Decimal(str(value[k]))<0 for k in ('nasdaq_expansion_gte_pct','a50_expansion_gte_pct'))
                or Decimal(str(value['expansion_vix_lte']))<=0):raise ValueError
    except (KeyError,TypeError,ValueError,InvalidOperation):raise ValueError('EXTERNAL_POLICY_INVALID') from None
    return dict(value)
