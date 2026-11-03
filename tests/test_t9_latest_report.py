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
    if q != 'Q1':
        rows += [row(2025, f'Q{int(q[1])-1}',40,4),row(2026,f'Q{int(q[1])-1}',50,5)]
    # Q4 2026 has not ended at this snapshot: use a past-year pair.
    if q=='Q4':
        rows = [row(2024,q),row(2025,q,120,12,secret_field='excluded'),
                row(2024,'Q3',40,4),row(2025,'Q3',50,5)]
    projected=_compact_fundamental_rows(rows)['statements']['INCOME']
    assert all(set(r)<=set(_COMPACT_FUNDAMENTAL_FIELDS['INCOME']) for r in projected)
    result=check(projected)
    assert result['supported'] and result['fiscal_period']==q
    assert result['operating_income_yoy_pct']==20
    assert result['period_basis']=='CUMULATIVE_AND_SINGLE_QUARTER'

def test_bad_latest_q3_cannot_fall_back_to_good_h1():
    result=check([row(2026,'Q2',200,20), row(2025,'Q2'),
                  row(2026,'Q3',80,8),row(2025,'Q3')])
    assert result['fiscal_period']=='Q3' and not result['supported']

def test_undisclosed_q3_falls_back_to_latest_h1_before_selecting():
    future=row(2026,'Q3',1,1)
    future['report_date_ms']=int(datetime(2026,11,4,tzinfo=timezone.utc).timestamp()*1000)
    result=check([row(2026,'Q2',120,12),row(2025,'Q2'),row(2025,'Q1',40,4),row(2026,'Q1',50,5),future])
    assert result['supported'] and result['fiscal_period']=='Q2'

def test_missing_comparative_or_metric_never_falls_back_or_synthesizes():
    rows=[row(2026,'Q3'),row(2026,'Q2',120,12),row(2025,'Q2')]
    assert check(rows)['reason_code']=='A1_LATEST_REPORT_COMPARATIVE_MISSING'
    rows.append(row(2025,'Q3',None,10))
    assert check(rows)['reason_code']=='A1_LATEST_REPORT_CORE_METRICS_MISSING'
    assert not check([])['supported']

def test_projection_dedup_keeps_latest_and_subtraction_operands():
    rows=[row(2026,'Q3',120,12),row(2026,'Q2'),row(2026,'Q1'),row(2025,'Q4'),row(2025,'Q3'),row(2025,'Q2',40,4)]
    revision=row(2026,'Q3',130,13)
    revision['report_date_ms']+=1000
    rows.append(revision)
    before=deepcopy(rows)
    projected=_compact_fundamental_rows(rows)['statements']['INCOME']
    assert rows==before and len(projected)==5
    assert len({(r['fiscal_year'],r['fiscal_period']) for r in projected})==5
    assert check(projected)['operating_income']==130
    assert check(projected)['prior_operating_income']==100


def test_q3_period_survives_downstream_team_projection():
    from liangjian_funnel.pipeline.research.common import _compact_upstream_candidate
    from liangjian_funnel.pipeline.research.a2 import _compact_a2_upstream_candidate
    evidence=check([row(2026,'Q3',120,12),row(2025,'Q3'),
                    row(2026,'Q2',50,5),row(2025,'Q2',40,4)])
    candidate={'symbol':'600519.SH','fundamental_support':{'supported':True,'latest_report':evidence}}
    compact=_compact_upstream_candidate(candidate)
    assert compact['fundamental_support']['latest_report']['fiscal_period']=='Q3'
    assert compact['fundamental_support']['latest_report']['period_basis']=='CUMULATIVE_AND_SINGLE_QUARTER'
    a2=_compact_a2_upstream_candidate(candidate)
    assert a2['report_fiscal_period']=='Q3' and a2['report_profit_yoy']==20
    assert a2['report_single_quarter']['supported']
    assert compact['fundamental_support']['latest_report']['single_quarter']['operating_income']==70
    assert not any(k.startswith('half_year') for k in a2)
    legacy={'symbol':'600519.SH','fundamental_support':{'latest_half_year':evidence}}
    assert _compact_a2_upstream_candidate(legacy)['report_fiscal_period']=='Q3'


@pytest.mark.parametrize('now,prior,cumulative_ok,single_ok', [
    ((120,12),(100,10),True,True),
    ((120,12),(110,11),True,False),
    ((90,9),(100,10),False,True),
    ((60,6),(100,10),False,False),
])
def test_both_bases_required(now, prior, cumulative_ok, single_ok):
    result=check([row(2026,'Q3',*now),row(2025,'Q3',*prior),
                  row(2026,'Q2',80,8),row(2025,'Q2',90 if not cumulative_ok and single_ok else 60,9 if not cumulative_ok and single_ok else 6)])
    assert result['cumulative_ytd']['supported'] is cumulative_ok
    assert result['single_quarter']['supported'] is single_ok
    assert result['supported'] is (cumulative_ok and single_ok)
    assert result['single_quarter']['operating_income']==now[0]-80
    assert result['single_quarter']['prior_operating_income']==prior[0]-(90 if not cumulative_ok and single_ok else 60)
    if not cumulative_ok:
        assert 'CUMULATIVE_YTD_GROWTH_NOT_CONFIRMED' in result['reason_codes'][0]
    elif not single_ok:
        assert 'SINGLE_QUARTER_GROWTH_NOT_CONFIRMED' in result['reason_code']


@pytest.mark.parametrize('missing', ['current', 'prior', 'metric', 'future'])
def test_single_quarter_missing_operand_is_not_zero(missing):
    rows=[row(2026,'Q3',120,12),row(2025,'Q3'),
          row(2026,'Q2',50,5),row(2025,'Q2',40,4)]
    if missing=='current': rows.pop(2)
    elif missing=='prior': rows.pop(3)
    elif missing=='metric': rows[3]['parent_holder_net_profit']=None
    else: rows[3]['report_date_ms']=int(datetime(2026,11,4,tzinfo=timezone.utc).timestamp()*1000)
    result=check(rows)
    assert result['cumulative_ytd']['supported']
    assert not result['single_quarter']['supported'] and not result['supported']
    assert result['single_quarter']['prior_parent_holder_net_profit'] is None or missing=='current'
    if missing!='metric':
        assert result['reason_code']=='A1_LATEST_REPORT_SINGLE_QUARTER_COMPARATIVE_MISSING'


def test_q1_single_quarter_equals_cumulative_without_subtraction():
    result=check([row(2026,'Q1',120,12),row(2025,'Q1')])
    assert result['supported']
    assert result['single_quarter']['operating_income']==120
    assert result['single_quarter']['subtracted_fiscal_period'] is None


def test_bounded_projection_keeps_both_years_q1_for_h1_and_no_fabrication():
    rows=[row(2026,'Q2',120,12),row(2026,'Q1',50,5),row(2025,'Q4'),
          row(2025,'Q3'),row(2025,'Q2'),row(2025,'Q1',40,4)]
    projected=_compact_fundamental_rows(rows)['statements']['INCOME']
    assert len(projected)==5 and check(projected)['supported']
    rows=[r for r in rows if (r['fiscal_year'],r['fiscal_period'])!=(2025,'Q1')]
    projected=_compact_fundamental_rows(rows)['statements']['INCOME']
    assert not check(projected)['supported']
    assert not any(r['fiscal_year']==2025 and r['fiscal_period']=='Q1' for r in projected)
