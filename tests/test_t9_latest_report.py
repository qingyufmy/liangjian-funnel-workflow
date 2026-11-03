from copy import deepcopy
from datetime import datetime, timezone
import pytest
from liangjian_funnel.pipeline.deterministic import _latest_report_support
from liangjian_funnel.workflow import _compact_fundamental_rows, _COMPACT_FUNDAMENTAL_FIELDS

AS_OF = '2026-11-03T02:57:45+08:00'

def row(y, q, revenue=100., profit=10., **extra):
    end = datetime(y, int(q[1])*3, 28, tzinfo=timezone.utc)
    return dict(_dataset='INCOME', fiscal_year=y, fiscal_period=q,
                period_end_ms=int(end.timestamp()*1000),
                report_date_ms=int(end.timestamp()*1000),
                operating_income=revenue, parent_holder_net_profit=profit, **extra)

def check(rows):
    return _latest_report_support({'statements': {'INCOME': rows}}, as_of=AS_OF)

@pytest.mark.parametrize('q', ['Q1', 'Q2', 'Q3', 'Q4'])
def test_latest_period_same_year_comparison_and_whitelist(q):
    rows = [row(2025,q), row(2026,q,120,12, secret_field='excluded')]
    # Q4 2026 has not ended at this snapshot: use a past-year pair.
    if q=='Q4':
        rows = [row(2024,q),row(2025,q,120,12,secret_field='excluded')]
    projected=_compact_fundamental_rows(rows)['statements']['INCOME']
    assert all(set(r)<=set(_COMPACT_FUNDAMENTAL_FIELDS['INCOME']) for r in projected)
    result=check(projected)
    assert result['supported'] and result['fiscal_period']==q
    assert result['operating_income_yoy_pct']==20
    assert result['period_basis']=='CUMULATIVE_YTD'

def test_bad_latest_q3_cannot_fall_back_to_good_h1():
    result=check([row(2026,'Q2',200,20), row(2025,'Q2'),
                  row(2026,'Q3',80,8),row(2025,'Q3')])
    assert result['fiscal_period']=='Q3' and not result['supported']

def test_undisclosed_q3_falls_back_to_latest_h1_before_selecting():
    future=row(2026,'Q3',1,1)
    future['report_date_ms']=int(datetime(2026,11,4,tzinfo=timezone.utc).timestamp()*1000)
    result=check([row(2026,'Q2',120,12),row(2025,'Q2'),future])
    assert result['supported'] and result['fiscal_period']=='Q2'

def test_missing_comparative_or_metric_never_falls_back_or_synthesizes():
    rows=[row(2026,'Q3'),row(2026,'Q2',120,12),row(2025,'Q2')]
    assert check(rows)['reason_code']=='A1_LATEST_REPORT_COMPARATIVE_MISSING'
    rows.append(row(2025,'Q3',None,10))
    assert check(rows)['reason_code']=='A1_LATEST_REPORT_CORE_METRICS_MISSING'
    assert not check([])['supported']

def test_projection_dedup_keeps_latest_and_prior_with_four_row_budget():
    rows=[row(2026,'Q3',120,12),row(2026,'Q2'),row(2026,'Q1'),row(2025,'Q4'),row(2025,'Q3')]
    revision=row(2026,'Q3',130,13)
    revision['report_date_ms']+=1000
    rows.append(revision)
    before=deepcopy(rows)
    projected=_compact_fundamental_rows(rows)['statements']['INCOME']
    assert rows==before and len(projected)==4
    assert len({(r['fiscal_year'],r['fiscal_period']) for r in projected})==4
    assert check(projected)['operating_income']==130
    assert check(projected)['prior_operating_income']==100


def test_q3_period_survives_downstream_team_projection():
    from liangjian_funnel.pipeline.research.common import _compact_upstream_candidate
    from liangjian_funnel.pipeline.research.a2 import _compact_a2_upstream_candidate
    evidence=check([row(2026,'Q3',120,12),row(2025,'Q3')])
    candidate={'symbol':'600519.SH','fundamental_support':{'supported':True,'latest_report':evidence}}
    compact=_compact_upstream_candidate(candidate)
    assert compact['fundamental_support']['latest_report']['fiscal_period']=='Q3'
    assert compact['fundamental_support']['latest_report']['period_basis']=='CUMULATIVE_YTD'
    a2=_compact_a2_upstream_candidate(candidate)
    assert a2['report_fiscal_period']=='Q3' and a2['report_profit_yoy']==20
    assert not any(k.startswith('half_year') for k in a2)
    legacy={'symbol':'600519.SH','fundamental_support':{'latest_half_year':evidence}}
    assert _compact_a2_upstream_candidate(legacy)['report_fiscal_period']=='Q3'
