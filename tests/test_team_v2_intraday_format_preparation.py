import importlib.util
from pathlib import Path

import pytest


@pytest.mark.parametrize('role',['A2','A3'])
def test_absent_historical_gate_evidence_stays_empty_format_only(tmp_path,monkeypatch,role):
    root=Path(__file__).resolve().parents[1]
    spec=importlib.util.spec_from_file_location('intraday_format_probe',root/'scripts/run_team_intraday_format_dryrun.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    day='2026-10-14';at=day+'T10:00:00+08:00'
    monkeypatch.setattr(module,'sealed_caches',lambda path:({(day,at):(
        dict(trade_date=day,as_of=at,breadth=0.2,coverage=1,current_session=True),'f'*64)},[]))
    _,context,prepared=module.prepare(tmp_path,tmp_path,day,role,'deepseek-v4-pro')
    assert prepared['positive_candidate_gate'] is False
    assert prepared['forced_format_sample_not_live_trigger'] is True
    assert prepared['original_session']=='NO_ORIGINAL_SESSION'
    assert prepared['replacements']['LIVE_MARKET_STATE']['emotion_state']=='DATA_BLOCK'
    assert prepared['required_envelope']['knowledge_cutoff']==at
    if role=='A2':assert context==dict(candidates={},themes={},emotion_state='DATA_BLOCK')
    else:assert context==dict(drafts={},envelopes={},first_five_days=True)
