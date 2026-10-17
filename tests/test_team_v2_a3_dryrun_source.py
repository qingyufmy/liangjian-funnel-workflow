"""Source-parity gates prevent missing stage inputs masquerading as no action."""
from pathlib import Path
import importlib.util
import sys
from types import SimpleNamespace

scripts=Path(__file__).resolve().parents[1]/'scripts'
sys.path.insert(0,str(scripts))
spec=importlib.util.spec_from_file_location('a3_dryrun_source',scripts/'run_team_a3_frozen_dryrun.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)


def original():
    return {'output':{'core_watch_pool':[dict(symbol='000001.SZ',deterministic_gate_status='REVIEW_CANDIDATE',
        deterministic_strategy_profile='TREND_MA5',entry_reference_zone={'low':10,'high':11},
        daily_invalidation=9,no_chase_price=12)],'secondary_watch_pool':[],
        'rejected_candidates':[dict(symbol='000002.SZ',deterministic_gate_status='HARD_REJECT',
            deterministic_strategy_profile='LEADER_INTRADAY')]}}


def gate():
    return SimpleNamespace(review_symbols=('000001.SZ',),decisions=[
        dict(symbol='000001.SZ',status='REVIEW_CANDIDATE',strategy_profile='TREND_MA5',
            entry_reference_zone={'low':10,'high':11},daily_invalidation=9,no_chase_price=12),
        dict(symbol='000002.SZ',status='HARD_REJECT',strategy_profile='LEADER_INTRADAY')])


def test_exact_original_domain_geometry_and_review_scope_are_accepted():
    assert module.parity_issues(gate(),original())==[]


def test_missing_enrichment_cannot_be_an_empty_successful_dryrun():
    candidate=gate();candidate.review_symbols=()
    for row in candidate.decisions:row['status']='DATA_GAP'
    issues=module.parity_issues(candidate,original())
    assert 'A3_ORIGINAL_REVIEW_SCOPE_MISMATCH' in issues
    assert any('FIELD_MISMATCH' in issue for issue in issues)


def test_changed_price_or_missing_candidate_is_blocked_before_model():
    candidate=gate();candidate.decisions.pop();candidate.decisions[0]['no_chase_price']=13
    issues=module.parity_issues(candidate,original())
    assert 'A3_ORIGINAL_GATE_DOMAIN_MISMATCH' in issues
    assert 'A3_ORIGINAL_GATE_FIELD_MISMATCH:000001.SZ:no_chase_price' in issues
