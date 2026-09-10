"""One bounded semantic repair of an archived A5 response, without market refetch."""
import argparse
import copy
import hashlib
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from liangjian_funnel.pipeline.model_client import ModelCallResult, OpenAICompatibleModelClient
from liangjian_funnel.review.daily import A5DailyReviewService, A5ReviewKind, A5ReviewReport
from liangjian_funnel.reporting import atomic_write_text, atomic_write_json
from liangjian_funnel.settings import Settings
from liangjian_funnel.workflow import WorkflowApplication, _primary_model_for_settings


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--facts',type=Path,required=True)
    parser.add_argument('--response',type=Path,required=True)
    args=parser.parse_args()
    facts=json.loads(args.facts.read_text()); response=json.loads(args.response.read_text())
    if facts['metrics']['a4_effective_event_count'] or facts['metrics']['a4_lifecycle_count']:
        raise ValueError('REPAIR_WITH_SIGNALS_REQUIRES_SIGNAL_REVIEW')
    settings=Settings.from_env(root=Path.cwd()); app=WorkflowApplication(settings)
    model=response['model']; _,_,lane=_primary_model_for_settings(settings)
    probe=ModelCallResult(model=model,output=response['output'],prompt_hash=None,input_hash=None,
        latency_ms=0,attempts=1,thinking_variant=response['thinking_variant'])
    if probe.output_hash != response['output_hash']:
        raise ValueError('ARCHIVED_RESPONSE_HASH_MISMATCH')
    verification=facts['independent_verification']; cases=verification['counterexamples']
    case_symbols={r['symbol'] for r in cases}
    allowed={str(r.get('evidence_id')) for r in cases}
    allowed.update(('METRICS:DAILY','DATA_QUALITY:DAILY','A5V:A2:SUMMARY','A5V:A3:SUMMARY','A5V:A4:SUMMARY'))
    checks=[]
    for row in verification['a4']['plans']:
        allowed.add(row['evidence_id'])
        checks.append({k:row.get(k) for k in ('symbol','evidence_id','discrepancy_class','orchestration_omission_count','observation_coverage')}
            | {'field_checks':{side:{field:{k:v.get(k) for k in ('status','mismatch_count','compared_count','not_comparable_count')}
                                  for field,v in row.get(side,{}).items()}
                               for side in ('cross_source_field_checks','archived_tdx_field_checks')}})
    context={'metrics':facts['metrics'],'data_quality':facts['data_quality'],'cutoff_at':facts['cutoff_at'],
        'trade_date':facts['trade_date'],'review_kind':facts['review_kind'],
        'counterexamples':cases,'a4_checks':checks,
        'a3_verification':{k:v for k,v in verification['a3'].items() if k!='plans'},
        'a2_verification':{k:v for k,v in verification['a2'].items()
                           if k not in {'counterexamples','stock_performance','market_cross_section_recovery','market_cross_section_missing_symbols'}},
        'a2_counts':facts['a2'].get('counts'),
        'allowed_evidence_ids':sorted(allowed)}
    # Aggregate every field comparison without truncating plans or failures.
    field_totals={}
    for row in checks:
        for side, fields in row['field_checks'].items():
            for field, values in fields.items():
                total=field_totals.setdefault(side+':'+field,{'statuses':{},'mismatch_count':0,'compared_count':0,'not_comparable_count':0})
                status=str(values['status']); total['statuses'][status]=total['statuses'].get(status,0)+1
                for key in ('mismatch_count','compared_count','not_comparable_count'):
                    total[key]+=values[key] or 0
    context['a4_checks']={'plan_count':len(checks),'field_totals':field_totals,
        'orchestration_omission_count':sum(r['orchestration_omission_count'] or 0 for r in checks)}
    keys=('overall_verdict','executive_summary','a2_review','a3_review','a4_review',
          'core_defects','improvement_proposals','data_collection_tasks','unresolved_questions')
    context['output_schema']=A5ReviewReport.model_json_schema()
    instruction='''你为已有A5复盘重建有证据支持的分析字段，不重新选择股票或创造策略结论。输出JSON，只包含overall_verdict、executive_summary、a2_review、a3_review、a4_review、core_defects、improvement_proposals、data_collection_tasks、unresolved_questions、case_assessments。
前9字段遵守output_schema。case_assessments为字典，键为全部反例的symbol，值为100字以内的分析字符串。股票、收益、实际阶段由服务器直接从冻结事实回填，模型不再重复输出。
权威量化事实优先于原响应：有效信号、生命周期数量严格取metrics；START_CONFIRMATION是观察，不是有效事件，不能据此推断成交。
策略数量严格取metrics.a3_strategy_counts。当a4_m15_macd_applicable_plan_count为0时，不能把预热数量0写成520预热失败。
每个反例的funnel_drop_stage严格取counterexamples.drop_stage的A1/A2/A3/A4前缀；不允许NOT_APPLICABLE。
counterexamples中的全部反例必须保留，阶段分布直接引用metrics。不得把A3未生成计划写成A2未聚焦。
A4成交量差异与零执行遗漏要分开；不能从价格触及区间推断有合规买点或确认漏执行。
其他原响应内容逐项对照本次提供事实；不受支持的判断删去或明确未知。所有引用只能来自allowed_evidence_ids。
AMOUNT缺数据不等于大量不匹配。A2主题重叠率读实际字段；ranking_comparable_to_production=false时不能当成板块强度选错证据。812是量化候选域而非已独立验证全市场。
最多2条提案，只做建议/影子测试，不能改变生产参数。仅分析今天、不回补历史。正文简洁，输出不超过5000汉字，不输出思考过程。
'''
    prompt=instruction+json.dumps(context,ensure_ascii=False,separators=(',',':'))
    prompt_hash=hashlib.sha256(prompt.encode()).hexdigest()
    artifact=args.response.with_name(args.response.stem+'-repair-'+prompt_hash[:12]+'-prompt.txt')
    atomic_write_text(artifact,prompt)
    client=OpenAICompatibleModelClient(app.review_model_client.settings,max_attempts=1,thinking_enabled=False)
    # Use an explicit bounded no-thinking request; never infer support from
    # a successful small probe alone. Retain actual output for fact auditing.
    client.thinking_variants=(("deepseek_explicitly_disabled_for_fact_repair",{'thinking':{'type':'disabled'},'enable_thinking':False}),)
    class RepairClient:
        def complete(self,selected,messages,**kwargs):
            if selected != model or kwargs['input_hash'] != response['input_hash']:
                raise ValueError('REPAIR_SOURCE_RESPONSE_IDENTITY_MISMATCH')
            print(json.dumps({'repair_prompt_chars':len(prompt),'model':model,'scope':'ARCHIVED_RESPONSE_FACT_REPAIR'}),flush=True)
            repaired=client.complete(model,[{'role':'system','content':prompt}],stage='A5',
                prompt_hash=prompt_hash,input_hash=kwargs['input_hash'],timeout_seconds=300,max_output_tokens=32768)
            atomic_write_json(artifact.with_suffix('.response.json'),repaired.output)
            if set(repaired.output) != set(keys)|{'case_assessments'}:
                raise ValueError('REPAIR_FIELDS_MISMATCH')
            assessments=repaired.output['case_assessments']
            if set(assessments) != case_symbols or any(not isinstance(v,str) or not v.strip() for v in assessments.values()):
                raise ValueError('REPAIR_CASE_COVERAGE_MISMATCH')
            output={k:copy.deepcopy(repaired.output[k]) for k in keys}
            output.update(schema_version='a5-daily-review/1.0.0',review_kind=facts['review_kind'],
                trade_date=facts['trade_date'],sample_sufficient_for_strategy_change=False,signal_reviews=[])
            output['missed_opportunity_reviews']=[{'symbol':r['symbol'],'name':r['name'],
                'theme':r.get('theme_name') or r.get('theme_id',''),
                'observed_performance':f"截至午盘相对昨收 {float(r['intraday_return'])*100:+.2f}%",
                'funnel_drop_stage':r['drop_stage'].split('_')[0],
                'assessment':assessments[r['symbol']],'evidence_ids':[r['evidence_id']],
                'is_confirmed_defect':False} for r in cases]
            return ModelCallResult(model=model,output=output,prompt_hash=prompt_hash,input_hash=kwargs['input_hash'],
                latency_ms=repaired.latency_ms,attempts=repaired.attempts,thinking_variant=repaired.thinking_variant,
                reasoning_tokens=repaired.reasoning_tokens)
    result=A5DailyReviewService(store=app.store,prompts=app.prompts,model_client=RepairClient(),
        output_dir=settings.workflow_output_dir,lane_id=lane,model=model,notification_publisher=None).run(
            review_kind=A5ReviewKind(facts['review_kind']),now=datetime.now(ZoneInfo('Asia/Shanghai')),frozen_facts=facts)
    atomic_write_json(args.response.with_name(args.response.stem+'-repair-result.json'),
        {'source_response_hash':response['output_hash'],'repair_prompt_hash':prompt_hash,'result':result})
    print(json.dumps(result,ensure_ascii=False,default=str),flush=True)


if __name__=='__main__':main()
