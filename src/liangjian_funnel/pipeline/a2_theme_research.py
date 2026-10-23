"""Independent, evidence-bound theme hypotheses; never execution authority.

The monthly primary identity and TOP8/flow basket remain unchanged. A stock
can contribute several frozen membership hypotheses to the *same* A2 call.
Membership neither proves a catalyst nor licenses copying another theme's
numeric score. Additional themes without their own quantitative population
remain explicitly unscored.
"""
from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime

from .feature_store import content_hash

SCHEMA = 'a2-independent-theme-research/1'
ATTENTION_SCHEMA = 'a2-hot100-independent-intake/1'


def hot100_research_intake(output, snapshot, g0, *, hard_risks):
    """Freeze attention leads separately from every formal A1 partition.

    This establishes only a dated taxonomy research question. It is neither a
    fundamental admission nor evidence of a catalyst, stage, shape or entry.
    """
    from ..data.hot100_observation import observe_snapshot_hot100, snapshot_decision_as_of
    from .emotion_theme import bind_emotion_themes
    observation = observe_snapshot_hot100(snapshot)
    if not observation.complete:
        return dict(schema=ATTENTION_SCHEMA, candidates=[], dispositions=[],
            source_health=observation.health(), execution_authority=False)
    capture = datetime.fromisoformat(snapshot_decision_as_of(snapshot))
    usable = dict(snapshot)
    membership_proofs = {}
    for taxonomy in ('INDUSTRY', 'CONCEPT'):
        key = 'THS_' + taxonomy + '_MEMBERSHIP'
        source = snapshot.get(key)
        valid = (isinstance(source, Mapping) and source.get('available') is True
            and isinstance(source.get('content_hash'), str) and len(source['content_hash']) == 64
            and all(c in '0123456789abcdef' for c in source['content_hash']))
        clocks = {}
        if valid:
            for field in ('as_of', 'event_time', 'fetch_time', 'ingest_time', 'publish_time'):
                if source.get(field) is None:
                    continue
                try:
                    stamp = datetime.fromisoformat(source[field])
                    valid = valid and stamp.tzinfo is not None and stamp.utcoffset() is not None and stamp <= capture
                    clocks[field] = stamp.isoformat()
                except (TypeError, ValueError):
                    valid = False
            valid = valid and bool(clocks) and isinstance(source.get('records'), (list, tuple))
        if not valid:
            usable[key] = {}
            continue
        membership_proofs[taxonomy] = dict(source_id=source.get('source_id'),
            declared_content_hash=source.get('content_hash'), frozen_source_sha256=content_hash(source), clocks=clocks)
    symbols = {row['symbol'] for row in observation.records}
    bindings = bind_emotion_themes(output, usable, symbols)
    partitions = {}
    original_rows = {}
    for partition in ('active_research_pool', 'monitor_pool', 'rejected_candidates'):
        for row in output.get(partition, ()):
            if isinstance(row, Mapping) and row.get('symbol') in symbols:
                partitions[row['symbol']] = partition
                original_rows[row['symbol']] = row
    candidates, dispositions = [], []
    for hot in observation.records:
        symbol = hot['symbol']
        partition = partitions.get(symbol, 'NOT_IN_MONTHLY_OUTPUT')
        if partition == 'active_research_pool':
            continue
        binding = bindings[symbol]
        reason = ('OUTSIDE_FROZEN_G0' if symbol not in g0 else
            hard_risks.get(symbol) or ('A1_EMOTION_MAJOR_RISK' if original_rows.get(symbol, {}).get('hard_risk_events') else None))
        if reason is None and not binding.get('research_resolved'):
            reason = 'DATED_EXACT_THEME_MEMBERSHIP_UNPROVEN'
        body = dict(schema=ATTENTION_SCHEMA, symbol=symbol, hot100_rank=hot['rank'],
            source_trade_date=observation.trade_date, capture_as_of=capture.isoformat(),
            source_health=observation.health(), original_a1_partition=partition,
            formal_a1_member=False, execution_authority=False, execution_permission='BLOCKED',
            confirms_catalyst=False, confirms_theme_stage=False,
            theme_research_binding=deepcopy(binding),
            membership_sources={taxonomy: membership_proofs[taxonomy]
                for taxonomy in sorted({m['taxonomy'] for m in binding.get('matches', ())})},
            status='DEFERRED_A2_RESEARCH_BUDGET' if reason is None else 'INELIGIBLE',
            reason_codes=[] if reason is None else [reason])
        body['candidate_id'] = content_hash(body)
        dispositions.append(body)
        if reason is None:
            candidates.append(body)
    return dict(schema=ATTENTION_SCHEMA, source_health=observation.health(), candidates=candidates,
        dispositions=dispositions, execution_authority=False,
        formal_a1_partitions_changed=False, new_model_calls_authorized=False)


def attention_theme_hypotheses(candidates):
    """Reuse exact route validation without pretending attention passed A1/A2."""
    result = {}
    for candidate in candidates:
        symbol = candidate['symbol']
        binding = candidate['theme_research_binding']
        for route in _exact_theme_routes(symbol, binding):
            body = dict(schema=SCHEMA, symbol=symbol, theme_id=route['theme_id'],node_id=route['node_id'],
                decision_id=candidate['candidate_id'], as_of=candidate['capture_as_of'],
                evidence=deepcopy(route['evidence']),membership_source_hash=binding['source_hash'],
                monthly_primary_theme_id=None, origin='HOT100_ATTENTION_RESEARCH_ONLY',
                formal_a1_member=False,execution_authority=False,confirms_catalyst=False,confirms_theme_stage=False)
            body['hypothesis_id'] = content_hash(body)
            result.setdefault(symbol, []).append(body)
    return result


def _exact_theme_routes(symbol, binding):
    """Only route proof is shared; formal eligibility is not synthesized."""
    if not isinstance(binding, Mapping) or binding.get('research_resolved') is not True:
        return []
    if (binding.get('schema_version') != 'emotion-theme-binding/1.1.0'
            or not isinstance(binding.get('matches'), list)
            or binding.get('source_hash') != content_hash({'symbol': symbol, 'matches': binding['matches']})):
        return []
    known_memberships = {content_hash(e) for e in binding['matches'] if isinstance(e, Mapping)}
    # Frozen older bindings retain their original routes, not invented proof.
    routes = binding.get('research_candidate_routes', binding.get('candidate_routes'))
    if not isinstance(routes, list):
        return []
    result, seen = [], set()
    for route in routes:
        if not isinstance(route, Mapping):
            continue
        theme, node, evidence = route.get('theme_id'), route.get('node_id'), route.get('evidence')
        if not isinstance(theme, str) or not theme or not isinstance(node, str) or not node:
            continue
        if not isinstance(evidence, list) or not evidence or any(
            not isinstance(e, Mapping) or e.get('theme_id') != theme or e.get('node_id') != node
            or e.get('match_method') != 'MATURE_THEME_REGISTRY_EXACT_NAME'
            or e.get('taxonomy') not in ('INDUSTRY', 'CONCEPT') or not e.get('taxonomy_code')
            or content_hash(e) not in known_memberships for e in evidence
        ) or (theme, node) in seen:
            continue
        seen.add((theme, node))
        result.append(route)
    return result


def theme_hypotheses(contexts, symbols):
    result = {}
    for symbol in sorted(symbols):
        row = contexts.get(symbol)
        if not isinstance(row, Mapping) or not (
            row.get('emotion_core_eligible') is True or row.get('independent_strategy_review') is True
        ):
            continue
        if row.get('deterministic_status', row.get('status')) != 'REVIEW_CANDIDATE':
            continue
        if row.get('hard_risk_events') or row.get('downstream_trade_eligible') is False:
            continue
        if not isinstance(row.get('decision_id'), str) or not row['decision_id']:
            continue
        try:
            observed = datetime.fromisoformat(row['as_of'])
            if observed.tzinfo is None or observed.utcoffset() is None:
                continue
        except (KeyError, TypeError, ValueError):
            continue
        binding = row.get('theme_research_binding') or row.get('emotion_theme_binding')
        for route in _exact_theme_routes(symbol, binding):
            theme, node = route.get('theme_id'), route.get('node_id')
            evidence = route.get('evidence')
            body = dict(schema=SCHEMA, symbol=symbol, theme_id=theme, node_id=node,
                decision_id=row['decision_id'], as_of=row['as_of'],
                evidence=deepcopy(evidence), membership_source_hash=binding.get('source_hash'),
                monthly_primary_theme_id=row.get('theme_id'), execution_authority=False,
                confirms_catalyst=False, confirms_theme_stage=False)
            body['hypothesis_id'] = content_hash(body)
            result.setdefault(symbol, []).append(body)
    return result


def add_unscored_theme_hypotheses(frozen, contexts, *, additional_hypotheses=None):
    """Keep old numerical populations intact; add no invented factor values."""
    hypotheses = theme_hypotheses(contexts, frozen['review_symbols'])
    if additional_hypotheses:
        hypotheses = {**hypotheses, **deepcopy(additional_hypotheses)}
    if not hypotheses:
        return frozen
    result = deepcopy(frozen)
    members = {}
    for symbol, rows in hypotheses.items():
        for row in rows:
            members.setdefault(row['theme_id'], []).append(symbol)
    for theme, symbols in sorted(members.items()):
        if theme in result['themes']:
            continue
        factors = {name: dict(member_count=0, effective_member_count=0, mean_score=None,
            members=[], field_refs=[], member_fields_sha256=content_hash({})) for name in result['weights']}
        result['themes'][theme] = dict(theme_id=theme, member_symbols=[], member_count=0,
            research_member_symbols=sorted(set(symbols)), factors=factors,
            missing_factors=sorted(result['weights']), globally_removed_factors=[],
            missing_weight='1', available_weight='0', effective_weights={}, base_score=None,
            status='SCORE_DATA_LIMITED', focus_policy='WATCH_ONLY', legacy_global_capital_rule_applied=False,
            score_basis='NO_THEME_SPECIFIC_QUANT_POPULATION_DO_NOT_COPY_MONTHLY_PRIMARY_SCORE')
    result['independent_theme_hypotheses'] = hypotheses
    result['score_snapshot_sha256'] = content_hash({k: v for k, v in result.items() if k != 'score_snapshot_sha256'})
    return result


def independent_theme_reviews(output, contexts, symbols, *, additional_hypotheses=None):
    """Preserve every batch's dated theme review before ordinary de-duplication."""
    hypotheses = theme_hypotheses(contexts, symbols)
    if additional_hypotheses:
        hypotheses = {**hypotheses, **additional_hypotheses}
    reviews = []
    for raw in output.get('active_themes', ()):
        if not isinstance(raw, Mapping):
            continue
        members = [h for rows in hypotheses.values() for h in rows if h['theme_id'] == raw.get('theme_id')]
        if members:
            review = dict(schema=SCHEMA, theme_id=raw['theme_id'],
                hypothesis_ids=sorted(h['hypothesis_id'] for h in members),
                review=deepcopy(dict(raw)), execution_authority=False)
            review['review_id'] = content_hash(review)
            reviews.append(review)
    return reviews


def evaluate_theme_hypotheses(item, context, reviews, *, bind_stage, evaluate, expected_trade_date,
                              include_evaluated=False):
    """Evaluate exact thematic alternatives without resolving entry identity."""
    symbol = item.get('symbol')
    hypotheses = theme_hypotheses({symbol: context}, {symbol}).get(symbol, [])
    checks, qualified, evaluated = [], [], []
    for hypothesis in hypotheses:
        theme = hypothesis['theme_id']
        matching = {}
        for row in reviews:
            if not isinstance(row, Mapping) or hypothesis['hypothesis_id'] not in row.get('hypothesis_ids', []):
                continue
            if row.get('schema') != SCHEMA or row.get('theme_id') != theme or row.get('execution_authority') is not False:
                continue
            if row.get('review_id') != content_hash({k: v for k, v in row.items() if k != 'review_id'}):
                continue
            original_review = row.get('review')
            if (not isinstance(original_review, Mapping) or not expected_trade_date
                    or original_review.get('stage_observed_as_of') != expected_trade_date
                    or not original_review.get('stage_evidence_hash')):
                continue
            matching[row['review_id']] = row
        if len(matching) != 1:
            checks.append(dict(hypothesis_id=hypothesis['hypothesis_id'], theme_id=theme,
                eligibility='DATA_GAP', reason_codes=['A3_THEME_RESEARCH_REVIEW_MISSING_OR_CONFLICTING']))
            continue
        review = next(iter(matching.values()))
        candidate = {**item, 'theme_id': theme, 'industry_chain_node': hypothesis['node_id']}
        for key in ('theme_stage', 'sector_stage', 'stage', 'theme_stage_evidence'):
            candidate.pop(key, None)
        research_context = {**context, 'theme_id': theme, 'execution_permission': 'BLOCKED',
            'research_only_reason': 'A3_INDEPENDENT_THEME_RESEARCH_ONLY'}
        for key in ('theme_stage', 'sector_stage', 'stage', 'theme_stage_evidence'):
            research_context.pop(key, None)
        candidate, binding = bind_stage(candidate, research_context, [review['review']])
        if binding.get('resolved') is not True:
            checks.append(dict(hypothesis_id=hypothesis['hypothesis_id'], theme_id=theme,
                eligibility='DATA_GAP', reason_codes=[binding.get('reason_code', 'A3_THEME_STAGE_UNPROVEN')]))
            continue
        verdict = evaluate(candidate, research_context)
        evaluated.append((hypothesis, verdict, binding))
        checks.append(dict(hypothesis_id=hypothesis['hypothesis_id'], theme_id=theme,
            theme_stage=review['review'].get('stage'), review_id=review['review_id'],
            **{key: deepcopy(verdict.get(key)) for key in ('strategy_profile', 'eligibility',
                'research_state', 'unmet_conditions', 'veto_conditions', 'reason_codes')},
            execution_authority=False))
        if verdict.get('eligibility') == 'QUALIFIED':
            qualified.append((hypothesis, verdict, binding))
    qualified.sort(key=lambda row: (row[0]['theme_id'], row[0]['hypothesis_id']))
    return (checks, qualified, evaluated) if include_evaluated else (checks, qualified)


def select_theme_research_observation(primary, checks, evaluated):
    """Retain non-executable alternatives without turning a gap into rejection.

    A ready primary is never downgraded. Actual alternative verdicts have
    already passed the same safety/technical evaluator. A *synthetic* missing
    review cannot override a known non-theme safety veto on the primary.
    """
    if primary.get('eligibility') == 'QUALIFIED':
        return None
    vetoes = set(primary.get('veto_conditions') or ())
    if vetoes - {'LEADER_THEME_RETREAT_OR_CLIMAX'}:
        return None
    gaps = [row for row in checks if row.get('eligibility') == 'DATA_GAP']
    if gaps:
        # This is an incomplete theme review, not a computed technical verdict
        # for that theme. Keep the original primary in the caller's evidence.
        reasons = sorted({code for row in gaps for code in row.get('reason_codes', ())})
        result = deepcopy(primary)
        result.update(eligibility='DATA_GAP', research_state='EVIDENCE_PENDING',
            reason_codes=reasons, required_conditions=['INDEPENDENT_THEME_RESEARCH_COMPLETE'],
            met_conditions=[], unmet_conditions=['INDEPENDENT_THEME_RESEARCH_COMPLETE'],
            veto_conditions=[], first_blocking_gate='INDEPENDENT_THEME_RESEARCH_COMPLETE',
            all_failed_gates=['INDEPENDENT_THEME_RESEARCH_COMPLETE'],
            gate_results={'INDEPENDENT_THEME_RESEARCH_COMPLETE': {
                'met': False, 'available': False, 'kind': 'DATA',
                'reason': 'A3_THEME_RESEARCH_EVIDENCE_INCOMPLETE'}},
            llm_review={'status': 'DATA_GAP', 'reason_codes': reasons})
        return None, result, None
    watch = sorted((row for row in evaluated if row[1].get('eligibility') == 'WATCH'),
        key=lambda row: (row[0]['theme_id'], row[0]['hypothesis_id']))
    if primary.get('eligibility') == 'REJECTED' and watch:
        return watch[0]
    return None
