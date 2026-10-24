"""Pure admission proof for an isolated outside-ACTIVE A3 research branch.

This module cannot call a model, enrich a snapshot, publish, or promote A1.
Its records are hypotheses awaiting the ordinary technical evaluator, never
formal A3 candidates. A separately bound research runner must consume them.
"""
from collections import defaultdict
from collections.abc import Mapping
from copy import deepcopy

from .a2_theme_research import SCHEMA as A2_SCHEMA, attention_theme_hypotheses, hot100_research_intake
from .feature_store import content_hash
from ..data.hot100_observation import observe_snapshot_hot100

SCHEMA = 'a3-attention-research-domain/1'


def _require(value, code):
    if not value:
        raise ValueError('A3_ATTENTION_' + code)


def _pretechnical_batches(records, size):
    """Separate same-stock hypotheses; no batch pretends one verdict covers two."""
    groups = defaultdict(list)
    for row in records:
        groups[row['theme_id']].append(row)
    result = []
    for theme in sorted(groups, key=lambda t: (min(r['source_slot'] for r in groups[t]), t)):
        pending = sorted(groups[theme], key=lambda r: (r['source_slot'], r['symbol'], r['hypothesis_id']))
        while pending:
            batch, remaining, symbols = [], [], set()
            for row in pending:
                if len(batch) < size and row['symbol'] not in symbols:
                    batch.append(row['case_id']); symbols.add(row['symbol'])
                else:
                    remaining.append(row)
            result.append(dict(theme_id=theme, case_ids=batch))
            pending = remaining
    return result


def build_attention_a3_domain(snapshot_data, a1_output, a2_audit, *, source_trade_date, batch_size=16):
    """Rebuild frozen membership, then require a successful exact A2 review.

    Only MONITOR leads that actually received an approved existing A2 slot are
    admitted here. Rejected/absent/ACTIVE stocks keep their original contracts.
    This does not assert financial eligibility, technical readiness, model-call
    authority, provider authenticity, or execution permission.
    """
    _require(isinstance(snapshot_data, Mapping) and isinstance(a1_output, Mapping), 'INPUT_REQUIRED')
    _require(type(batch_size) is int and 1 <= batch_size <= 16, 'BATCH_SIZE_INVALID')
    audit = a2_audit.as_dict() if callable(getattr(a2_audit, 'as_dict', None)) else a2_audit
    _require(isinstance(audit, Mapping) and audit.get('stage') == 'A2' and audit.get('status') == 'VALIDATED',
             'VALIDATED_A2_REQUIRED')
    output = audit.get('output')
    _require(isinstance(output, Mapping) and audit.get('output_hash') == content_hash(output), 'A2_OUTPUT_HASH_MISMATCH')
    observation = observe_snapshot_hot100(snapshot_data)
    _require(observation.complete and observation.trade_date == source_trade_date, 'FULL_DATED_HOT100_REQUIRED')
    g0 = snapshot_data.get('g0_symbols')
    _require(isinstance(g0, list) and g0 and len(g0) == len(set(g0)), 'FULL_G0_REQUIRED')
    from .research.common import _daily_emotion_hard_risks
    intake = hot100_research_intake(a1_output, snapshot_data, set(g0),
                                   hard_risks=_daily_emotion_hard_risks(snapshot_data))
    _require(a1_output.get('independent_research_intake') == intake, 'INTAKE_REBUILD_MISMATCH')
    candidates = {r['symbol']: r for r in intake['candidates']}
    _require(len(candidates) == len(intake['candidates']), 'DUPLICATE_CANDIDATE')
    slots = output.get('independent_research_sampling_plan', {}).get('selected_slots', [])
    _require(isinstance(slots, list) and len(slots) <= 30, 'SIDE_SLOT_PROOF_INVALID')
    by_symbol = {}
    for row in slots:
        _require(isinstance(row, Mapping) and isinstance(row.get('symbol'), str)
                 and row['symbol'] not in by_symbol and type(row.get('slot')) is int and 1 <= row['slot'] <= 30,
                 'SIDE_SLOT_PROOF_INVALID')
        by_symbol[row['symbol']] = row
    _require(len({r['slot'] for r in slots}) == len(slots), 'DUPLICATE_SLOT')
    diagnostics = audit.get('diagnostics')
    call_ids = diagnostics.get('team_call_ids', []) if isinstance(diagnostics, Mapping) else []
    _require(isinstance(call_ids, list) and call_ids and all(isinstance(x, str) and x for x in call_ids),
             'ACTUAL_A2_CALL_REFERENCE_REQUIRED')
    dispositions = output.get('independent_attention_research_dispositions', [])
    _require(isinstance(dispositions, list) and all(isinstance(r, Mapping)
             and isinstance(r.get('symbol'), str) and r['symbol']
             and type(r.get('hot100_rank')) is int and 1 <= r['hot100_rank'] <= 100
             for r in dispositions), 'DISPOSITIONS_REQUIRED')
    _require(len({r.get('symbol') for r in dispositions if isinstance(r, Mapping)}) == len(dispositions),
             'DUPLICATE_DISPOSITION')
    hypotheses = attention_theme_hypotheses(list(candidates.values()))
    reviews = output.get('independent_theme_reviews', [])
    _require(isinstance(reviews, list), 'REVIEWS_REQUIRED')
    records = []
    outcomes = [dict(symbol=symbol, status='NOT_ADMITTED', reason_code='A2_RESEARCH_DISPOSITION_MISSING')
        for symbol in sorted(set(candidates)-{r['symbol'] for r in dispositions})]
    for row in sorted(dispositions, key=lambda r: (r.get('hot100_rank', 999), r.get('symbol', ''))):
        symbol = row['symbol']; candidate = candidates.get(symbol)
        reason = None
        if row.get('status') != 'VALIDATED_THEME_RESEARCH':
            reason = 'A2_RESEARCH_NOT_COMPLETED'
        elif candidate is None:
            reason = 'FROZEN_CANDIDATE_UNPROVEN'
        elif candidate.get('original_a1_partition') != 'monitor_pool':
            reason = 'OUTSIDE_MONITOR_RESEARCH_SCOPE'
        elif row.get('candidate_id') != candidate['candidate_id']:
            reason = 'A2_CANDIDATE_IDENTITY_MISMATCH'
        elif (row.get('formal_a1_member') is not False or row.get('execution_authority') is not False
              or candidate.get('execution_permission') != 'BLOCKED'):
            reason = 'AUTHORITY_CONFLICT'
        elif (symbol not in by_symbol or 'HOT100' not in by_symbol[symbol].get('evidence_routes', [])):
            reason = 'SELECTED_SLOT_UNPROVEN'
        elif not row.get('team_call_ids') or not set(row['team_call_ids']).issubset(call_ids):
            reason = 'A2_CALL_REFERENCE_MISMATCH'
        if reason:
            outcomes.append(dict(symbol=symbol, status='NOT_ADMITTED', reason_code=reason)); continue
        for hypothesis in sorted(hypotheses.get(symbol, []), key=lambda r: (r['theme_id'], r['hypothesis_id'])):
            matches = {}
            for review in reviews:
                if not isinstance(review, Mapping) or hypothesis['hypothesis_id'] not in review.get('hypothesis_ids', []):
                    continue
                if (review.get('schema') != A2_SCHEMA or review.get('theme_id') != hypothesis['theme_id']
                    or review.get('execution_authority') is not False
                    or review.get('review_id') != content_hash({k: v for k, v in review.items() if k != 'review_id'})):
                    continue
                raw = review.get('review', {})
                if (not isinstance(raw, Mapping) or raw.get('theme_id') != hypothesis['theme_id']
                    or raw.get('stage_observed_as_of') != source_trade_date or not raw.get('stage_evidence_hash')):
                    continue
                matches[review['review_id']] = review
            if len(matches) != 1:
                outcomes.append(dict(symbol=symbol, theme_id=hypothesis['theme_id'],
                    hypothesis_id=hypothesis['hypothesis_id'], status='NOT_ADMITTED',
                    reason_code='EXACT_A2_THEME_REVIEW_MISSING_OR_CONFLICTING')); continue
            review = next(iter(matches.values()))
            body = dict(schema=SCHEMA, symbol=symbol, theme_id=hypothesis['theme_id'],
                node_id=hypothesis['node_id'], hypothesis_id=hypothesis['hypothesis_id'],
                original_a1_partition='monitor_pool', candidate_id=candidate['candidate_id'],
                source_trade_date=source_trade_date, capture_as_of=candidate['capture_as_of'],
                source_slot=by_symbol[symbol]['slot'], a2_call_ids=list(row['team_call_ids']),
                a2_output_hash=audit['output_hash'], review_id=review['review_id'],
                theme_review=deepcopy(review['review']), membership_source_hash=hypothesis['membership_source_hash'],
                hypothesis=deepcopy(hypothesis),
                formal_a1_member=False, fundamental_admission_passed=False, execution_authority=False,
                execution_permission='BLOCKED', risk_unit='NO_ENTRY',
                status='PENDING_ORDINARY_A3_TECHNICAL_REVIEW')
            body['case_id'] = content_hash(body); records.append(body)
    records.sort(key=lambda r: (r['source_slot'], r['symbol'], r['theme_id'], r['hypothesis_id']))
    result = dict(schema=SCHEMA, records=records, dispositions=outcomes,
        source_trade_date=source_trade_date, a2_output_hash=audit['output_hash'],
        a1_partitions_unchanged=True, formal_a3_domain_unchanged=True,
        new_model_calls_authorized=False, execution_authority=False,
        batching_basis='PRE_TECHNICAL_UPPER_BOUND_NOT_MODEL_SCOPE',
        pretechnical_batches=_pretechnical_batches(records, batch_size))
    result['content_hash'] = content_hash(result)
    return result


def _case_technical_input(data, a1_output, case, local_factors):
    """Scope the ordinary evaluator, without adding a formal A2 row.

    Behaviour uses the existing evidence adapter and classifier. Hot100 rank
    or theme membership alone cannot assign an emotion leader/trend identity.
    """
    from . import deterministic as d
    from .a2_role_logic import classify_a2_stock
    symbol = case['symbol']
    original = next(r for r in a1_output['monitor_pool'] if r['symbol'] == symbol)
    item = {**original, 'symbol':symbol, 'theme_id':case['theme_id'],
        'primary_theme':case['theme_id'], 'industry_chain_node':case['node_id']}
    observed_as_of = d._snapshot_as_of(data)
    _require(observed_as_of is not None, 'TECHNICAL_OBSERVATION_TIME_REQUIRED')
    factors = data.get('A2_FACTOR_SNAPSHOT', data.get('FACTOR_SNAPSHOT', {}))
    factor = d._symbol_scoped_row(factors, symbol)
    quote = d._candidate_map(data).get(symbol, {})
    amount = d._number(quote.get('amount')) or d._number(quote.get('turnover'))
    liquidity = d._liquidity_score(amount if amount is not None else 0.0)
    relative = d._relative_strength_score(factor, default=None)
    scores = d._a2_factor_scores(item=item, symbol=symbol, theme_id=case['theme_id'],
        snapshot=data, factor=factor, tier_snapshot=data.get('TIER_STRUCTURE_SNAPSHOT', {}),
        relative=relative if relative is not None else 0.0, liquidity=liquidity,
        cycle_score=None, attention=True, dragon=symbol in d._event_symbols(data.get('DRAGON_TIGER_SNAPSHOT')),
        local_market_factors=local_factors.get(symbol, {}))
    identifiability, _ = d._a2_identifiability(item=item, relative=relative if relative is not None else 0.0,
        liquidity=liquidity, factor_scores=scores, attention=True, dragon=False)
    evidence = d._a2_behavior_evidence(item=item, factor_scores=scores,
        identifiability=identifiability, minimum_identifiability_score=float(data.get('MIN_IDENTIFIABILITY_SCORE') or 60),
        relative=relative if relative is not None else 0.0, liquidity=liquidity,
        legacy_role=d._role(identifiability, liquidity, relative if relative is not None else 0.0),
        as_of=observed_as_of)
    if relative is None:
        evidence['relative_strength'] = dict(available=False, met=None, value=None,
            source_refs=[], as_of=observed_as_of, reason='A2_STOCK_RELATIVE_STRENGTH_UNAVAILABLE')
    if amount is None:
        evidence['identifiability_liquidity'] = dict(available=False, met=None, value=None,
            source_refs=[], as_of=observed_as_of, reason='A2_STOCK_LIQUIDITY_UNAVAILABLE')
    behavior = classify_a2_stock(symbol=symbol, name=original.get('company_name'),
        evidence=evidence, as_of=observed_as_of, trend_candidate=False)
    context = {**behavior, 'a2_factor_scores':scores, 'a1_formal_member':False, 'formal_a1_member':False,
        'fundamental_admission_passed':False, 'independent_strategy_review':True,
        'execution_permission':'BLOCKED', 'research_only_reason':'OUTSIDE_A1_ATTENTION_TECHNICAL_RESEARCH',
        'research_observation_scope':'RESEARCH_ONLY_NO_AUTOMATIC_ENTRY',
        'source_attention_case_id':case['case_id']}
    item = {**item, **context, 'candidate_origin':'WATCH_ONLY',
        'parent_monthly_identity':{key:original.get(key) for key in ('primary_theme','theme_id','industry_chain_node')},
        'independent_theme_hypotheses':[]}
    overlay = {'A2_BOTTLENECK_CONTEXT':{symbol:context},
        'A3_CANDIDATE_ORIGIN':{symbol:'WATCH_ONLY'}, 'A3_CANDIDATE_SCOPE':[symbol],
        'A3_ATTENTION_RESEARCH_CASE':deepcopy(case)}
    scoped = {**data, **overlay}
    upstream = dict(focus_pool=[item], watch_only_pool=[], active_themes=[deepcopy(case['theme_review'])],
        independent_theme_reviews=[], execution_authority=False,
        research_scope='PRIVATE_ATTENTION_TECHNICAL_INPUT_NOT_FORMAL_A2_OUTPUT')
    return scoped, upstream, behavior, overlay


def evaluate_attention_a3_domain(snapshot_data, a1_output, domain):
    """Run ordinary A3 checks for every exact case; zero model/source calls."""
    from . import deterministic as d
    _require(snapshot_data.get('DETERMINISTIC_RESEARCH_V2_ENABLED') is True, 'ORDINARY_V2_TECHNICAL_REQUIRED')
    _require(isinstance(domain, Mapping) and domain.get('content_hash') ==
        content_hash({k:v for k,v in domain.items() if k != 'content_hash'}), 'DOMAIN_HASH_MISMATCH')
    _require(domain.get('execution_authority') is False, 'DOMAIN_AUTHORITY_CONFLICT')
    _require(snapshot_data.get('A2_MARKET_REFERENCE', {}).get('market_trade_date') == domain['source_trade_date'],
             'TECHNICAL_SOURCE_DAY_MISMATCH')
    candidates = d._candidate_map(snapshot_data)
    local = d._build_a2_local_market_factors(snapshot_data, candidates=candidates,
        industry_membership=d._membership_map(snapshot_data.get('THS_INDUSTRY_MEMBERSHIP'), taxonomy='INDUSTRY'))
    rows = []
    for case in domain['records']:
        _require(case.get('case_id') == content_hash({k:v for k,v in case.items() if k != 'case_id'}),
                 'CASE_HASH_MISMATCH')
        scoped, upstream, behavior, _ = _case_technical_input(snapshot_data, a1_output, case, local)
        gate = d.screen_a3(scoped, upstream)
        _require(len(gate.decisions) == 1 and gate.decisions[0]['symbol'] == case['symbol'], 'TECHNICAL_DOMAIN_DRIFT')
        decision = deepcopy(gate.decisions[0])
        decision.update(execution_permission='BLOCKED', risk_unit='NO_ENTRY', fundamental_admission_passed=False)
        price = scoped.get('PRICE_LEVELS', {}).get(case['symbol'])
        reviewable = case['symbol'] in gate.review_symbols
        # The unchanged final A3 validator requires the explicit source flag.
        # Apply that evidence check before spending a research model request.
        if reviewable and (not isinstance(price, Mapping) or price.get('available') is not True):
            decision.update(status='DATA_GAP', sent_to_llm=False,
                reason_codes=list(dict.fromkeys([*decision.get('reason_codes', []),'A3_PRICE_LEVELS_UNAVAILABLE'])))
            reviewable = False
        rows.append(dict(case_id=case['case_id'], symbol=case['symbol'], theme_id=case['theme_id'],
            hypothesis_id=case['hypothesis_id'], source_slot=case['source_slot'],
            behavior=behavior, technical=decision, sent_to_llm=False,
            model_state='PENDING_AUTHORIZED_MODEL_REVIEW' if reviewable else 'NOT_MODEL_REVIEWED_LOCAL_GATE',
            execution_permission='BLOCKED', risk_unit='NO_ENTRY'))
    eligible = {r['case_id'] for r in rows if r['model_state'] == 'PENDING_AUTHORIZED_MODEL_REVIEW'}
    batches = _pretechnical_batches([c for c in domain['records'] if c['case_id'] in eligible], 16)
    result = dict(schema='a3-attention-technical-research/1', domain_hash=domain['content_hash'],
        source_trade_date=domain['source_trade_date'], records=rows, model_batches=batches,
        first_pass_model_requests=len(batches), new_model_http=0, new_source_http=0,
        formal_a3_domain_unchanged=True, execution_authority=False)
    result['content_hash'] = content_hash(result)
    return result
