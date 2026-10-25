"""Evidence-bound company research, separate from selection or trading rights.

An accepted citation proves provenance, not the economic truth of a moat or a
growth forecast. Missing/late company evidence must stay unknown. No scores,
financial forecasts, source downloads or eligibility decisions are made here.
"""
from collections import Counter
from collections.abc import Mapping
from datetime import date, datetime, timezone

from .feature_store import content_hash

SCHEMA = 'company-value-research/1'
FACETS = ('business_model', 'competitive_advantage', 'profitability_quality',
          'growth_drivers', 'valuation_expectations')
STATUSES = {'EVIDENCE_SUPPORTED', 'MIXED', 'CONTRADICTED', 'UNKNOWN'}
TRANSMISSION = ('ORDER', 'VOLUME', 'PRICE', 'REVENUE', 'GROSS_MARGIN', 'EXPENSE', 'CASHFLOW', 'EPS')
FIELDS = {'status', 'assessment', 'supporting_refs', 'opposing_refs',
          'unknowns', 'falsifiers', 'watch_metrics'}
_TIMES = ('published_at', 'publish_time', 'disclosure_date', 'ann_date', 'announcement_time', 'report_date_ms')


def _time(value):
    try:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            # report_date_ms is the existing financial adapter's publication
            # timestamp contract, not a period-end date or reception time.
            return datetime.fromtimestamp(value / 1000, timezone.utc)
        parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return parsed if parsed.utcoffset() is not None else None
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def company_evidence_scopes(values, symbols, *, knowledge_cutoff):
    """Only exact-symbol, dated company facts from the actual projected input.

    A report period is not a publication time. A date-only publication can be
    known on a later day, but cannot prove availability within that same day.
    Its date precision is retained; no midnight timestamp is invented. Global policy/sector
    citations never establish an individual company's competitive advantage.
    """
    from .research.common import _first_symbol
    cutoff = _time(knowledge_cutoff)
    if cutoff is None:
        raise ValueError('A1_COMPANY_KNOWLEDGE_CUTOFF_INVALID')
    result = {}
    for symbol in sorted(symbols):
        evidence = {}; excluded = Counter()

        def visit(value, path, inherited_time=None):
            if isinstance(value, Mapping):
                identity = value.get('symbol') or value.get('stock_code')
                if identity and _first_symbol(identity) != symbol:
                    excluded['OTHER_COMPANY'] += 1
                    return
                raw_time = next((value[k] for k in _TIMES if value.get(k)), inherited_time)
                external=value.get('schema')=='company-official-external-excerpt/1'
                if external:
                    from .company_external_evidence import validate_external
                    if not validate_external(value,symbol=symbol,knowledge_cutoff=knowledge_cutoff):
                        excluded['EXTERNAL_NOT_KNOWN_AT_CUTOFF']+=1;return
                    # An undated official page is usable only after its actual
                    # observation. This timestamp is never called publication.
                    raw_time=value['knowledge_available_at']
                refs = value.get('source_refs', [])
                refs = list(refs) if isinstance(refs, (list, tuple)) else []
                if value.get('source_ref'):
                    refs.append(value['source_ref'])
                financial_fact = (path.startswith('COMPANY_FUNDAMENTALS.')
                    and value.get('report_date_ms') is not None
                    and (value.get('_dataset') in {'INCOME', 'BALANCE', 'CASH_FLOW'}
                         or '.statements.' in path or '.latest_statements.' in path))
                if financial_fact and not refs:
                    # Explicit internal citation to supplied immutable facts,
                    # not a fabricated external announcement/page reference.
                    refs = ['A1:COMPANY_FACT:' + symbol + ':' + content_hash(value)]
                if refs:
                    published = _time(raw_time)
                    date_only = None
                    if isinstance(raw_time, str) and len(raw_time) == 10:
                        try: date_only = date.fromisoformat(raw_time)
                        except ValueError: pass
                    known_date = date_only is not None and date_only < cutoff.date()
                    reason = (None if known_date else 'PUBLICATION_TIME_UNVERIFIED' if published is None else
                              'PUBLICATION_AFTER_CUTOFF' if published > cutoff else None)
                    if reason:
                        excluded[reason] += 1
                    else:
                        for ref in refs:
                            if isinstance(ref, str) and ref.strip():
                                evidence.setdefault(ref, []).append(dict(
                                    source_field=path, published_at=value.get('published_at') if external else raw_time if known_date else published.isoformat(),
                                    **({'knowledge_available_at':value['knowledge_available_at']} if external else {}),
                                    time_precision='DATE' if known_date else 'DATETIME',
                                    citation_kind=value['availability_basis'] if external else 'FROZEN_FINANCIAL_FACT' if financial_fact else 'ORIGINAL_COMPANY_SOURCE',
                                    semantic_fact_sha256=(content_hash(dict(raw=value['original_raw_sha256'],excerpt=value['excerpt_sha256'],publisher=value['publisher'],symbol=symbol)) if external else
                                        content_hash(dict(pdf=value['original_pdf_sha256'],excerpt=value['excerpt_sha256']))
                                        if value.get('original_pdf_sha256') and value.get('excerpt_sha256') else
                                        value.get('content_hash') if path.startswith('COMPANY_RESEARCH_DOSSIER.') and value.get('content_hash') else content_hash(value)),
                                    fact_sha256=content_hash(value)))
                for key, item in value.items():
                    if key not in {'source_ref', 'source_refs'}:
                        visit(item, path + '.' + str(key), raw_time)
            elif isinstance(value, (list, tuple)):
                for i, row in enumerate(value):
                    visit(row, path + '[' + str(i) + ']', inherited_time)

        # Do not search a global list and accidentally inherit another issuer's
        # announcement. Broader source adapters need explicit symbol contracts.
        for field in ('MAIN_BUSINESS_EVIDENCE', 'COMPANY_FUNDAMENTALS', 'COMPANY_RESEARCH_DOSSIER'):
            block = values.get(field)
            row = block.get(symbol) if isinstance(block, Mapping) else None
            if row is not None:
                visit(row, field + '.' + symbol)
        body = dict(symbol=symbol, knowledge_cutoff=cutoff.isoformat(),
                    evidence={key: evidence[key] for key in sorted(evidence)},
                    excluded_evidence=dict(sorted(excluded.items())))
        result[symbol] = dict(body, scope_sha256=content_hash(body))
    return result


def company_output_contract(scopes):
    return dict(schema=SCHEMA, required_for_pools=['active_research_pool', 'monitor_pool'],
        field='company_value_research', scope_by_symbol=scopes, facets=list(FACETS),
        facet_fields=sorted(FIELDS), statuses=sorted(STATUSES),
        financial_transmission_stages=list(TRANSMISSION),
        unknown_facet_shape=dict(status='UNKNOWN',assessment='说明具体不能判断的事项，不编造结论',
            supporting_refs=[],opposing_refs=[],unknowns=['具体缺失的当时资料'],
            falsifiers=['未来取得原始证据后重新检验当前未知项'],watch_metrics=['需要跟踪的原始披露指标']),
        instructions=(
            '逐家公司填写company_value_research={schema,scope_sha256,facets}。facets必须且仅包含五个维度，'
            '每项必须包含列出的facet_fields。assessment简明区分事实与判断；business_model说明客户、产品与盈利方式；'
            'competitive_advantage说明相对竞争者的优势、替代难度及优势消失的条件，不把概念、专利数量或行业龙头标签当壁垒；'
            'profitability_quality说明利润来源、现金转化及一次性收益，单期利润不证明持续盈利；'
            'original_disclosed_accounting为对应公司原表当期明确金额，逐公司逐财年核对，不串股、不混用追溯调整前后列；'
            'growth_drivers说明需求→公司订单/交付→收入/利润/现金的传导和尚未兑现的假设，行业增速不等于公司增速；'
            'valuation_expectations区分业务前景与已定价预期，缺估值/一致预期数据则UNKNOWN，不编目标价。'
            '引用只能来自该股scope.evidence；原始完整事实仍在原输入。没有充分证据时status=UNKNOWN、'
            'A1:COMPANY_FACT:*是内部冻结财务事实引用，只用于company_value_research及financial_transmission，'
            '不得写进外层source_refs/supporting_refs/opposing_refs或冒充外部公告。'
            'supporting_refs/opposing_refs均为空、unknowns写具体缺项；UNKNOWN不是负面结论或通过。'
            '其它状态至少一条真实supporting或opposing引用；EVIDENCE_SUPPORTED必须有支持，CONTRADICTED必须有反证，MIXED两者均需。'
            'UNKNOWN同样要求falsifiers和watch_metrics均1至2条，不得留空，参照unknown_facet_shape结构；'
            'unknowns至多3条、refs各至多3条、每条文字最多600字。'
            '同一公司financial_transmission必须对financial_transmission_stages每个环节恰一条，包含原提示遗漏的EXPENSE。'
            '每条仅{stage,status,source_ref}，status取REALIZED/REALIZING/EXPECTED_ONLY/UNVERIFIABLE；'
            '已兑现、兑现中或有依据的预期必须引用该股scope.evidence；无法核实时UNVERIFIABLE且source_ref="-"。'
            '这是研究记录，不新增分数、不改变ACTIVE/MONITOR或任何技术、数据、风控、入场门。'))


def validate_company_research(output, scopes, *, required=False):
    from .research.common import _first_symbol
    issues = []
    for pool in ('active_research_pool', 'monitor_pool'):
        for row in output.get(pool, []) if isinstance(output.get(pool), list) else []:
            if not isinstance(row, Mapping):
                continue
            symbol = _first_symbol(row.get('symbol')); value = row.get('company_value_research')
            if value is None and not required:
                continue  # Historical originals stay historical, never marked researched.
            prefix = 'A1_COMPANY_VALUE:' + symbol + ':'
            scope = scopes.get(symbol)
            if (not isinstance(scope, Mapping) or not isinstance(value, Mapping)
                    or set(value) != {'schema', 'scope_sha256', 'facets'}
                    or value.get('schema') != SCHEMA or value.get('scope_sha256') != scope['scope_sha256']):
                issues.append(prefix + 'BINDING_OR_SCHEMA_INVALID'); continue
            for field in ('source_refs', 'supporting_refs', 'opposing_refs'):
                refs = row.get(field)
                if isinstance(refs, list) and any(isinstance(ref, str) and ref.startswith('A1:COMPANY_FACT:') for ref in refs):
                    issues.append(prefix + 'INTERNAL_FACT_USED_AS_OUTER_SOURCE')
            transmission = row.get('financial_transmission')
            stages = []
            if not isinstance(transmission, list):
                issues.append(prefix + 'TRANSMISSION_REQUIRED')
            else:
                for step in transmission:
                    if (not isinstance(step, Mapping) or set(step) != {'stage', 'status', 'source_ref'}
                            or not isinstance(step.get('stage'), str)):
                        issues.append(prefix + 'TRANSMISSION_SCHEMA_INVALID'); continue
                    stages.append(step['stage']); status = step['status']; ref = step['source_ref']
                    if (not isinstance(status, str) or status not in {'REALIZED','REALIZING','EXPECTED_ONLY','UNVERIFIABLE'}
                            or not isinstance(ref, str) or (status == 'UNVERIFIABLE' and ref != '-')
                            or (status != 'UNVERIFIABLE' and ref not in scope['evidence'])):
                        issues.append(prefix + 'TRANSMISSION_STATUS_OR_SOURCE_INVALID')
                if len(stages) != len(TRANSMISSION) or set(stages) != set(TRANSMISSION):
                    issues.append(prefix + 'TRANSMISSION_STAGE_DOMAIN_INVALID')
            facets = value.get('facets')
            if not isinstance(facets, Mapping) or set(facets) != set(FACETS):
                issues.append(prefix + 'FACET_DOMAIN_INVALID'); continue
            for name, facet in facets.items():
                code = prefix + name + ':'
                if not isinstance(facet, Mapping) or set(facet) != FIELDS:
                    issues.append(code + 'FIELDS_INVALID'); continue
                def texts(value, minimum=0, maximum=3):
                    return (isinstance(value, list) and minimum <= len(value) <= maximum
                        and all(isinstance(v, str) and 0 < len(v.strip()) <= 600 for v in value))
                status = facet['status']; support = facet['supporting_refs']; oppose = facet['opposing_refs']
                if (not isinstance(status, str) or status not in STATUSES
                        or not isinstance(facet['assessment'], str) or not 0 < len(facet['assessment'].strip()) <= 600
                        or not texts(facet['unknowns']) or not texts(facet['falsifiers'], 1, 2)
                        or not texts(facet['watch_metrics'], 1, 2) or not texts(support) or not texts(oppose)):
                    issues.append(code + 'CONTENT_INVALID')
                    for field,valid in (
                        ('status',isinstance(status,str) and status in STATUSES),
                        ('assessment',isinstance(facet['assessment'],str) and 0<len(facet['assessment'].strip())<=600),
                        ('unknowns',texts(facet['unknowns'])),('falsifiers_COUNT_1_TO_2',texts(facet['falsifiers'],1,2)),
                        ('watch_metrics_COUNT_1_TO_2',texts(facet['watch_metrics'],1,2)),
                        ('supporting_refs',texts(support)),('opposing_refs',texts(oppose))):
                        if not valid:issues.append(code+'FIELD_INVALID:'+field)
                    continue
                if any(ref not in scope['evidence'] for ref in support + oppose):
                    issues.append(code + 'COMPANY_EVIDENCE_INVALID')
                if len(set(support)) != len(support) or len(set(oppose)) != len(oppose):
                    issues.append(code + 'DUPLICATE_REF')
                if ((status == 'UNKNOWN' and (support or oppose or not facet['unknowns']))
                    or (status == 'EVIDENCE_SUPPORTED' and not support)
                    or (status == 'CONTRADICTED' and not oppose)
                    or (status == 'MIXED' and (not support or not oppose))):
                    issues.append(code + 'STATUS_EVIDENCE_MISMATCH')
    return issues


def company_research_coverage(outputs):
    """A5 auditable work coverage; not investment quality or realized returns."""
    counts = {name: Counter() for name in FACETS}; rows = 0; missing = 0
    for output in outputs:
        if not isinstance(output, Mapping):
            continue
        for pool in ('active_research_pool', 'monitor_pool'):
            for row in output.get(pool, []) if isinstance(output.get(pool), list) else []:
                if not isinstance(row, Mapping):
                    continue
                rows += 1; value = row.get('company_value_research')
                if not isinstance(value, Mapping) or value.get('schema') != SCHEMA:
                    missing += 1; continue
                facets = value.get('facets', {})
                for name in FACETS:
                    item = facets.get(name) if isinstance(facets, Mapping) else None
                    status = item.get('status') if isinstance(item, Mapping) else None
                    counts[name][status if isinstance(status, str) and status in STATUSES else 'INVALID'] += 1
    return dict(schema=SCHEMA, company_judgment_rows=rows, not_assessed_rows=missing,
        facets={key: dict(sorted(value.items())) for key, value in counts.items()},
        denominator='VALIDATED_MAPPING_JUDGMENT_ROWS_NOT_DISTINCT_COMPANIES_OR_TRADES',
        semantic_truth_verified=False, execution_authority=False)
