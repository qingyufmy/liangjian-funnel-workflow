from copy import deepcopy
from datetime import datetime
import json
from pathlib import Path
from liangjian_funnel.review.observability import condition_observations,observability_markdown

O=Path('D:/dev_A股/liangjian_team_v2_20261016/artifacts/a4-monitor-20261021')

def test_original_pattern_matrix_survives_legacy_judge_timeout_without_becoming_trade():
    original=json.loads((O/'D8_EXACT_CARD_INPUTS_RO_0837_20261027.json').read_bytes())
    event=original['events'][0]
    payload=json.loads(event['payload_json'])
    pid=payload['plan_id'];plan=next(p for p in original['plans'] if p['plan_id']==pid)
    frozen=deepcopy(event)
    result=condition_observations([plan],[event],cutoff=datetime.fromisoformat(event['minute_end']))['plans'][0]
    required=payload['strategy']['entry_confirmation']['required_conditions']
    assert result['max_conditions_met']==len(required)==10
    assert set(result['total_conditions_at_max'])==set(required)
    assert result['technical_evidence_despite_judge_block'] is True
    assert result['max_conditions_met_at']==event['minute_end']
    assert event==frozen and event['action']=='DATA_BLOCK' and not event['effective']

def test_missing_values_and_original_utc_never_render_python_literals_or_false_zero():
    rows=observability_markdown(dict(conditions=dict(plans=[dict(symbol='000858.SZ',max_conditions_met=None,
        max_conditions_met_at=None)],cluster_counts={'UNKNOWN':1}),timings=dict(phases={'fetch':{'p50_ms':None}})))
    text='\n'.join(rows)
    assert all(s not in text for s in ('None','null','未知','UNKNOWN'))
    assert '未取得' in text and '最多满足 0' not in text
    rows=observability_markdown(dict(conditions=dict(plans=[dict(symbol='000858.SZ',max_conditions_met=1,
        max_conditions_met_at='2026-10-27T02:06:00+00:00')])) )
    assert '10:06:00 北京时间' in '\n'.join(rows)
