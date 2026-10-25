"""Source-bound company audit admission; no model, store or execution I/O."""
from collections.abc import Mapping
from copy import deepcopy
import re
from .feature_store import content_hash
from .team_v2_contract import TeamValidation


def company_a5_source_policy(bindings):
    """Fixed evidence tasks, not model-authored strategy or entry rules."""
    boundaries={};catalog={}
    tasks=[('COMPETITIVE_EVIDENCE','A1','取得同产品范围、同工况的可比资料后复核竞争力',
        '统一产品范围与工况的同行官方资料或独立测试原件'),
        ('REALIZATION_EVIDENCE','A1','取得同项目兑现原件后复核收入贡献',
        '同项目合同、交付、验收、收入确认及回款原件'),
        ('ORIGINAL_DATA_GAPS','DATA','取得缺失原始数据后复核资料完整性',
        '同证券、同源日的可交易性与精确行业历史原件'),
        ('A2_EXISTING_RULES','A2','原资料更新后按现有A2条件重新研究',
        '原A2题材与资金条件所需的真实原件，不增加新阈值'),
        ('A3_EXISTING_GATES','A3','原资料更新后按现有A3全部门重新核验',
        '原A3数据、技术与风险门所需的真实原件，不增加新阈值')]
    for symbol,binding in sorted(bindings.items()):
        boundaries[symbol]=dict(symbol=symbol,a1_review_id=binding['a1_review_id'],
            robot_segment_contribution='NOT_INDEPENDENTLY_VERIFIED_BY_THIS_AUDIT',
            conclusion='机器人细分收入或利润贡献金额及占比尚未独立核实，不能据此认定为零或未实现。',
            execution_authority=False)
        for task,owner,trigger,source in tasks:
            identity=content_hash(dict(symbol=symbol,current_binding=binding,task=task))
            catalog[identity]=dict(action_id=identity,symbol=symbol,owner=owner,trigger=trigger,required_source=source)
    return dict(fact_boundaries=boundaries,followup_catalog=catalog)

def current_company_audit_view(tracking,*,symbols,observed_at):
    """Current originals and judgments; historical audit prose is not facts."""
    from .company_value_research import _time
    cutoff=_time(observed_at);latest={}
    for row in tracking.get('reviews',[]):
        source=_time(row.get('source_as_of'));observed=_time(row.get('observed_at'))
        if row.get('symbol') not in symbols or source is None or observed is None or source>cutoff or observed>cutoff:continue
        key=(source,observed,row['review_id']);old=latest.get(row['symbol'])
        if old is None or key>old[0]:latest[row['symbol']]=(key,row)
    ids={row['review_id'] for _,row in latest.values()}
    return dict(status=tracking.get('status','AVAILABLE'),schema='company-current-audit-view/1',reviews=[deepcopy(latest[s][1]) for s in sorted(latest)],
        fact_audits=deepcopy(tracking.get('fact_audits',[])),
        observations=[deepcopy(r) for r in tracking.get('observations',[]) if r.get('review_id') in ids],
        missing_current_reviews=sorted(set(symbols)-set(latest)),
        historical_registry=dict(review_ids=[r['review_id'] for r in tracking.get('reviews',[])],
            audit_refs=[dict(call_id=r.get('call_id'),output_hash=r.get('output_hash')) for r in tracking.get('audits',[])],
            followup_action_count=len(tracking.get('followup_actions',[])),
            original_ledger_unchanged=True,historical_prose_is_current_fact=False),execution_authority=False)


def validate_company_a5_probe(raw,context):
    keys={'envelope','schema','observed_at','source_trade_date','company_reviews','followup_actions','automatic_changes','entry_profit_proven','execution_authority'}
    issues=[];judgments=[]
    if context.get('fact_boundaries'):
        policy=company_a5_source_policy(context.get('current_bindings',{}))
        if any(context.get(k)!=policy[k] for k in ('fact_boundaries','followup_catalog')):
            issues.append('COMPANY_A5_SOURCE_POLICY_CHANGED')
    if not isinstance(raw,Mapping) or set(raw)!=keys:return TeamValidation(False,('COMPANY_A5_SCHEMA_INVALID',),(),0)
    if (raw['envelope']!=context['envelope'] or raw['schema']!='company-chain-audit/1' or raw['observed_at']!=context['envelope']['as_of']
            or raw['source_trade_date']!=context['envelope']['source_trade_date']
            or any(raw[k] is not False for k in ('automatic_changes','entry_profit_proven','execution_authority'))):
        issues.append('COMPANY_A5_SCOPE_OR_AUTHORITY_INVALID')
    rows=raw['company_reviews'];seen=[]
    if not isinstance(rows,list):rows=[];issues.append('COMPANY_A5_COMPANY_REVIEWS_REQUIRED')
    for i,row in enumerate(rows):
        expected={'symbol','company_quality_vs_theme_strength','realized_vs_unrealized','remaining_blockers','source_refs'}
        if isinstance(row,Mapping) and row.get('symbol') in context.get('current_bindings',{}):expected.add('current_state')
        if isinstance(row,Mapping) and row.get('symbol') in context.get('fact_boundaries',{}):expected.add('realization_fact')
        if not isinstance(row,Mapping) or set(row)!=expected:
            issues.append('COMPANY_A5_REVIEW_FIELDS_INVALID');continue
        seen.append(row['symbol'])
        if row['symbol'] in context.get('current_bindings',{}) and row.get('current_state')!=context['current_bindings'][row['symbol']]:
            issues.append('COMPANY_A5_CURRENT_STATE_MISMATCH:'+row['symbol'])
        if row['symbol'] in context.get('fact_boundaries',{}) and row.get('realization_fact')!=context['fact_boundaries'][row['symbol']]:
            issues.append('COMPANY_A5_REALIZATION_BOUNDARY_MISMATCH:'+row['symbol'])
        if (not isinstance(row['symbol'],str) or row['symbol'] not in context['symbols']
                or any(not isinstance(row[k],str) or not 0<len(row[k].strip())<=1800 for k in ('company_quality_vs_theme_strength','realized_vs_unrealized'))
                or not isinstance(row['remaining_blockers'],list) or not row['remaining_blockers']
                or any(not isinstance(v,str) or not v.strip() for v in row['remaining_blockers'])
                or not isinstance(row['source_refs'],list) or any(not isinstance(v,str) for v in row['source_refs'])
                or set(row['source_refs'])!={'A5:COMPANY','A5:A2','A5:A3'}):
            issues.append('COMPANY_A5_REVIEW_BINDING_INVALID');continue
        if row['symbol'] in context.get('current_bindings',{}):
            text='。'.join([row['company_quality_vs_theme_strength'],row['realized_vs_unrealized'],*row['remaining_blockers']])
            if re.search(r'(?:未实现|未兑现)\s*[/／、或]\s*未核实|未核实\s*[/／、或]\s*(?:未实现|未兑现)',text):
                issues.append('COMPANY_A5_UNVERIFIED_NOT_UNREALIZED:'+row['symbol'])
            if re.search(r'机器人[^。；\n]{0,55}(?:尚未|没有|未)(?:形成|产生|带来)[^。；\n]{0,12}(?:收入|利润)(?:贡献)?',text):
                issues.append('COMPANY_A5_UNVERIFIED_NOT_ZERO_CONTRIBUTION:'+row['symbol'])
        judgments.append(dict(row,path='company_reviews['+str(i)+']'))
    if len(seen)!=len(context['symbols']) or {s for s in seen if isinstance(s,str)}!=context['symbols']:
        issues.append('COMPANY_A5_DOMAIN_INVALID')
    actions=raw['followup_actions']
    if not isinstance(actions,list) or not 1<=len(actions)<=10:issues.append('COMPANY_A5_FOLLOWUP_ACTIONS_INVALID')
    else:
        for a in actions:
            focused=isinstance(a,Mapping) and isinstance(a.get('symbol'),str) and a.get('symbol') in context.get('fact_boundaries',{})
            action_keys={'symbol','owner','trigger','required_source'}|({'action_id'} if focused else set())
            if (not isinstance(a,Mapping) or set(a)!=action_keys
                    or not isinstance(a.get('symbol'),str) or a.get('symbol') not in context['symbols']
                    or not isinstance(a.get('owner'),str) or a.get('owner') not in {'A1','A2','A3','DATA'}
                    or any(not isinstance(a.get(k),str) or not a[k].strip() for k in ('trigger','required_source'))):
                issues.append('COMPANY_A5_FOLLOWUP_ACTION_INVALID')
            elif focused:
                action_id=a.get('action_id')
                expected=context['followup_catalog'].get(action_id) if isinstance(action_id,str) else None
                if expected is None or a!=expected:
                    issues.append('COMPANY_A5_FOLLOWUP_NOT_SOURCE_CATALOG:'+a['symbol'])
            elif a['symbol'] in context.get('current_bindings',{}):
                trigger=a['trigger']
                if re.search(r'(?:连续|≥|>=|大于|超过|至少|达到)\s*[一二三四五六七八九十\d]+[^。；\n]{0,20}(?:净流入|主力|涨幅|资金)',trigger):
                    issues.append('COMPANY_A5_UNAPPROVED_MARKET_TRIGGER:'+a['symbol'])
    return TeamValidation(not issues,tuple(dict.fromkeys(issues)),tuple(judgments),0)


def offline_company_a5_review(raw,context):
    """A distinct server view; never repair raw model JSON into a success."""
    result=validate_company_a5_probe(raw,context)
    policy=company_a5_source_policy(context.get('current_bindings',{}))
    return dict(schema='company-a5-offline-review/1',origin='SERVER_OFFLINE_EVIDENCE_RECONCILIATION_NOT_MODEL_REPLY',
        original_model_output_hash=content_hash(raw),original_model_admitted=result.valid,
        original_issues=list(result.issues),current_states=deepcopy(context.get('current_bindings',{})),
        realization_facts=policy['fact_boundaries'],research_followup_tasks=list(policy['followup_catalog'].values()),
        tasks_dispatched=False,model_http=0,automatic_changes=False,execution_authority=False)
