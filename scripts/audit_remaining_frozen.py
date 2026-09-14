"""Compare frozen A2 partitions and theme evidence, without a model or publication."""
import argparse
from collections import Counter
from copy import deepcopy
import json
from pathlib import Path

from liangjian_funnel.pipeline.deterministic import screen_a2, _membership_map
from liangjian_funnel.pipeline.emotion_theme import bind_emotion_themes


def audit(snapshot_path, research_path):
    frozen = json.loads(snapshot_path.read_text(encoding='utf-8'))
    research = json.loads(research_path.read_text(encoding='utf-8'))
    data = {**frozen['data'], 'as_of': frozen['as_of']}
    a1 = deepcopy(next(s['output'] for s in research['stages'] if s['stage'] == 'A1'))
    members = {r['symbol'] for r in a1['active_research_pool']}
    universe = set(_membership_map(data.get('THS_INDUSTRY_MEMBERSHIP'), taxonomy='INDUSTRY')) | set(_membership_map(data.get('THS_CONCEPT_MEMBERSHIP'), taxonomy='CONCEPT')) | members
    all_bindings = bind_emotion_themes(a1, data, universe)
    overlay = []
    for row in a1['active_research_pool']:
        if row.get('research_route') == 'DAILY_EMOTION_OVERLAY':
            binding = all_bindings[row['symbol']]
            row.update(primary_theme=binding['theme_id'], industry_chain_node=binding['node_id'], emotion_theme_binding=binding)
            overlay.append(row['symbol'])
    gate = screen_a2(data, a1, review_all_eligible=True)
    old_a2 = next(s['output'] for s in research['stages'] if s['stage']=='A2')
    old_selected = {r['symbol'] for pool in ('focus_pool','watch_only_pool') for r in old_a2.get(pool,[])}
    new_trends = {r['symbol'] for r in gate.decisions if r['status']=='REVIEW_CANDIDATE' and r.get('stock_behavior_type')=='TREND'}
    gap_symbols = {'000839.SZ','000981.SZ','600088.SH','600330.SH','605069.SH'}
    cards = []
    for row in gate.decisions:
        if row['symbol'] not in gap_symbols:
            continue
        binding = all_bindings[row['symbol']]
        cards.append({'symbol': row['symbol'], 'name': row.get('name'), 'status': row['status'],
            'execution_permission': row.get('execution_permission'), 'reason_codes': row.get('reason_codes'),
            'research_only_reason': row.get('research_only_reason'), 'route_eligibility': row.get('route_eligibility'),
            'unique_theme_selected': binding['resolved'], 'candidate_routes': binding['candidate_routes'],
            'source_hash': binding['source_hash'], 'as_of': frozen['as_of']})
    return {'snapshot_id': frozen['snapshot_id'], 'snapshot_hash': frozen['snapshot_hash'], 'models_called': False,
        'published': False, 'a1_count': len(members), 'a2_review_count': len(gate.review_symbols),
        'a2_outside_a1': sorted(set(gate.review_symbols)-members),
        'old_selected_missing_from_new_trend': sorted(old_selected-new_trends),
        'new_trend_outside_old_selected': sorted(new_trends-old_selected),
        'review_behavior_counts': dict(Counter(r.get('stock_behavior_type') for r in gate.decisions if r['status']=='REVIEW_CANDIDATE')),
        'research_only_symbols': [r['symbol'] for r in gate.decisions if r['status']=='REVIEW_CANDIDATE' and r.get('execution_permission')=='BLOCKED'],
        'status_counts': dict(Counter(r['status'] for r in gate.decisions)), 'five_symbol_cards': cards,
        'all_market_mapping': {'scope': len(universe), 'scope_basis': 'FROZEN_MEMBERSHIP_UNIVERSE', 'daily_overlay': len(overlay),
             'unique': sum(b['resolved'] for b in all_bindings.values()),
             'multiple_supported': sum(b['requires_theme_selection'] for b in all_bindings.values()),
             'missing': sum(not b['research_resolved'] for b in all_bindings.values())}}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('snapshot', type=Path)
    parser.add_argument('research', type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    receipt = audit(args.snapshot, args.research)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(receipt, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({k: v for k, v in receipt.items() if k!='five_symbol_cards'}, ensure_ascii=False))
