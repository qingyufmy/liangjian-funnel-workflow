"""A5 priority facts and deterministic degraded output, without invented review."""
from copy import deepcopy
import hashlib,json

LIMIT=250000
HISTORY_KEYS=frozenset(('review_history','historical_reports','history_reports','past_report_full_text'))
PROTECTED=('metrics','a2','a3','a4','a6','activation_decisions','plan_causal_chains',
    'paper_broker','record_fallback','model_judgments','independent_verification','legacy_operational_evidence')

def _text(v):return json.dumps(v,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)
def _sha(v):return hashlib.sha256(_text(v).encode()).hexdigest()

def priority_facts(facts):
    """Only known historical full-report roots may be omitted; today stays whole."""
    result=deepcopy(facts);omitted=[]
    for key in sorted(HISTORY_KEYS):
        if key in result:
            value=result.pop(key);omitted.append(dict(path='$.'+key,sha256=_sha(value),chars=len(_text(value))))
    for key in PROTECTED:
        if key in facts and result.get(key)!=facts[key]:raise ValueError('A5_PRIORITY_TODAY_FACT_CHANGED')
    return result,dict(schema='a5-priority-budget/1',source_fact_sha256=facts.get('input_hash'),
        protected_today_sha256={k:_sha(facts[k])for k in PROTECTED if k in facts},
        omitted_historical_full_text=omitted,truncated=bool(omitted),
        critical_material_removed=False,priority='TODAY_PLANS_EXECUTION_ALERTS_BEFORE_HISTORICAL_FULL_REPORTS')

def deterministic_review(facts,diagnostics):
    """A factual output is useful but is never a validated model opinion."""
    plans=[]
    for pid,row in (facts.get('plan_causal_chains',{}).get('by_plan')or{}).items():
        if not isinstance(row,dict):continue
        plan=deepcopy(row.get('plan')or{})
        original=row.get('original_frozen_plan')or{}
        plans.append(dict(plan_id=pid,plan=plan,original_plan_sha256=_sha(original),
            geometry={k:deepcopy(original.get(k))for k in('trigger_low','trigger_high','stop_level','no_chase_price','reward_risk','team_plan_geometry')},
            condition_samples=deepcopy(row.get('condition_samples')),legacy_lifecycle=deepcopy(row.get('legacy_lifecycle')),
            replay_summary=deepcopy(row.get('replay_summary')),closed_5m_source_sha256=_sha(row.get('closed_5m')),
            boundary='Original condition samples and hindsight replay are separate; absent fill/confirmation remains unproven.'))
    # Section conclusions explicitly stay unassessed. No fake AI text, score,
    # missed-entry finding or investment recommendation is generated here.
    conclusions={key:dict(status='NOT_MODEL_REVIEWED',source_path='$.'+source,
        source_sha256=_sha(facts.get(source)),conclusion='模型复盘未完成，原事实保留，不据此判断策略优劣。')
        for key,source in [('a2_review','a2'),('a3_review','a3'),('a4_review','a4'),('a6_review','a6'),
            ('trader_vs_deterministic','model_judgments'),('risk_governor_value','a6'),('model_value_assessment','model_judgments')]}
    out=dict(schema='a5-deterministic-degraded/1',status='TRUNCATED',trade_date=facts['trade_date'],review_kind=facts['review_kind'],
        source_fact_sha256=facts['input_hash'],metrics=deepcopy(facts['metrics']),plans=plans,conclusions=conclusions,
        full_original_sections={k:dict(path='$.'+k,sha256=_sha(v))for k,v in facts.items()if k!='input_hash'},
        context_budget=deepcopy(diagnostics),model_reviewed=False,approved_report=False,
        strategy_change_authorized=False,missing_opinions='NOT_EVALUATED_NOT_ZERO_NOT_PASS')
    if len(_text(out))>LIMIT:
        # Even the factual view can exceed the cap. Keep complete facts in its
        # frozen archive and replace only the view's verbose rows with exact
        # references; never present this fallback as full-material PASS.
        out['plans']=[dict(plan_id=p['plan_id'],plan=p['plan'],original_plan_sha256=p['original_plan_sha256'],
            omitted_view_sha256=_sha(p),status='FULL_PLAN_CHAIN_IN_FROZEN_SOURCE')for p in plans]
        out['view_rows_referenced_not_embedded']=True
    if len(_text(out))>LIMIT:
        out['metrics_reference']=dict(path='$.metrics',sha256=_sha(out.pop('metrics')))
        out['view_rows_referenced_not_embedded']=True
    if len(_text(out))>LIMIT:
        out['plans_reference']=dict(path='$.plan_causal_chains.by_plan',sha256=_sha(out.pop('plans')))
        out['view_rows_referenced_not_embedded']=True
    if len(_text(out))>LIMIT:raise ValueError('A5_DEGRADED_REFERENCE_ENVELOPE_TOO_LARGE')
    return out


def plan_fact_lines(facts):
    """Readable plan facts for degraded/internal and legacy customer cards."""
    by_plan=(facts.get('plan_causal_chains')or{}).get('by_plan')or{}
    if not by_plan:
        replay=facts.get('a3_plan_replay')or{};frozen=replay.get('frozen_plans')or{}
        by_plan={pid:dict(plan=p,original_frozen_plan=p,
            replay_summary=[x for x in replay.get('plans',[])if x.get('plan_id')==pid])for pid,p in frozen.items()}
    if not by_plan:return ['• 逐计划复盘：缺少冻结计划原件，触达/确认/成交后指标均待证。']
    lines=['**逐计划原件核对｜主备身份以晨会原件为准**']
    for pid,row in by_plan.items():
        p=row.get('plan')or{};o=row.get('original_frozen_plan')or p
        replay=row.get('replay_summary')or[]
        def show(v):return '待证'if v is None else str(v)
        status={'ACTIVE_TODAY':'有效，仍需盘中准入','INVALIDATED':'已按原规则失效',
            'PENDING_MORNING_REVIEW':'待晨会复核'}.get(p.get('status'),'状态待证')
        # Technical trigger counts are hindsight replay evidence, never fills.
        counts=[x['technical_trigger_minutes']for x in replay if type(x.get('technical_trigger_minutes'))is int]
        technical=' / '.join(map(str,counts))+'个原回放分钟（分原件列示，非成交）'if counts else '待证'
        limits=o.get('source_data_limits')
        codes=limits.get('reason_codes')if isinstance(limits,dict)else None
        limited=[x for x in codes if isinstance(x,str) and x.startswith('STOCK_NEWS_')]if isinstance(codes,(list,tuple))else []
        news='受限：'+','.join(limited)if limited else '覆盖状态待原件证明'
        lines.append('• '+str(p.get('symbol')or o.get('symbol')or pid)+'：'+status+'；买入区'+
            show(p.get('trigger_low',o.get('trigger_low')))+'–'+show(p.get('trigger_high',o.get('trigger_high')))+
            '；失效位'+show(p.get('stop_level',o.get('stop_level')))+'；原技术触发回放'+technical+'；NEWS '+news+'。')
    lines.append('• 触达与确认、成交分别核验；未留存成交原件时，MFE/MAE标待证，不把后见区间波动当成交后收益。')
    return lines
