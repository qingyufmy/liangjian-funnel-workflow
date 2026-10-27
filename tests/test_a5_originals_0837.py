"""Same business originals, no regenerated facts and no model invocation."""
from copy import deepcopy
import hashlib,json
from pathlib import Path
import pytest
from liangjian_funnel.review.daily import (A5ReviewReport,_canonicalize_report_output,
    _enforce_verified_findings,_markdown,_model_fact_projection,_projection_evidence_ids)
from liangjian_funnel.review.fact_guard import reconcile_report,business_metrics,execution_blocker_counts

OPS=Path('D:/dev_A股/liangjian_team_v2_20261016/artifacts/a4-monitor-20261021')

@pytest.mark.parametrize('folder',['a5-0825-originals-20261027','a5-close-0837-originals-20261027'])
def test_two_original_a5_inputs_counts_availability_and_timeout_semantics(folder):
    root=OPS/folder
    if not root.exists():pytest.skip('Frozen 10-27 original is an external audit input, never fabricated')
    fact_path=next(root.glob('*-facts.json'));model_path=next(root.glob('*-model-*.json'))
    facts=json.loads(fact_path.read_bytes());raw=json.loads(model_path.read_bytes())
    original=deepcopy(facts);output=deepcopy(raw['output'])
    allowed=_projection_evidence_ids(_model_fact_projection(facts))|{'METRICS:DAILY','DATA_QUALITY:DAILY'}
    report=A5ReviewReport.model_validate(_canonicalize_report_output(output,allowed_evidence=allowed))
    _enforce_verified_findings(report,facts)
    report.fact_reconciliation.extend(reconcile_report(report,facts))
    rendered=_markdown(report,facts)
    metrics=business_metrics(facts)
    assert facts==original and raw['output']==output
    assert '未识别' not in rendered and 'UTC01:' not in rendered and 'UTC00:' not in rendered
    assert metrics['a2_focus_count'] is None and metrics['a2_watch_count'] is None
    assert 'A2审计不可用' in report.executive_summary
    assert '趋势五日线14个' in report.a3_review.summary
    assert '52014个' not in report.a3_review.summary
    assert str(metrics['a4_monitor_observation_count']) in report.a4_review.summary
    blockers=execution_blocker_counts(facts)
    assert blockers.get('JUDGE_TIMEOUT') in {40,74}
    for defect in report.core_defects:
        if defect.blocker_type=='JUDGE_TIMEOUT':assert not defect.blocked_by_data
    for proposal in report.improvement_proposals:
        if proposal.type=='SHADOW_TEST':assert f'影子观察 {proposal.min_shadow_days} 个交易日，尚未获批' in rendered

def test_unknown_a2_remains_unknown_even_without_full_a4_coverage():
    from test_a5_daily_review import _report
    report=A5ReviewReport.model_validate(_report())
    facts=dict(a2={'status':'NOT_AVAILABLE'},metrics={'a2_focus_count':0,'a2_watch_count':0})
    reconcile_report(report,facts)
    assert '数量未知' in report.a2_review.summary
    assert report.a2_review.verdict=='DATA_LIMITED'
