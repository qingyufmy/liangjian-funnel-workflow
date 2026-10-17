"""RO original role failures; write exclusive scoped diagnostic evidence."""
from contextlib import closing
from datetime import datetime
import hashlib,json,sqlite3,subprocess
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1]
BASE=ROOT/'artifacts/team-v2-20261016'


def main():
    now=datetime.now(ZoneInfo('Asia/Shanghai'));rows=[]
    for name in ('a5-author207-1015-real','a6-author105-1014-real','a6-author105-1016-real','pm-author404-1016-valid105-real'):
        path=BASE/name
        with closing(sqlite3.connect((path/'model.db').resolve().as_uri()+'?mode=ro',uri=True)) as db:
            record=db.execute('SELECT output_json,output_hash,issues_json FROM model_calls').fetchone()
            catalog=json.loads(db.execute('SELECT catalog_json FROM model_call_evidence').fetchone()[0])
        output=json.loads(record[0]);ids={row['id'] for row in catalog['items']}
        detail=dict(directory=name,original_output_sha256=record[1],issues=json.loads(record[2]),
            categories=catalog['categories'])
        if name.startswith('a5'):
            detail.update(first_review=output['missed_opportunity_reviews'][0],
                absent_refs=sorted({ref for row in output['missed_opportunity_reviews'] for ref in row.get('evidence_ids',[]) if ref not in ids}),
                original_catalog_items=catalog['items'])
        elif name.startswith('a6'):
            detail.update(utilization_cap=output.get('utilization_cap'),capacity_for_tomorrow=output.get('capacity_for_tomorrow'),
                envelope_adjustments=output.get('envelope_adjustments'))
        else:detail['output']=output
        rows.append(detail)
    result=dict(actual_collected_at=now.isoformat(),model_calls=0,production_writes=0,rows=rows)
    target=BASE/(now.strftime('%Y%m%dT%H%M%S')+'-AUTHOR298_FAILURE_DETAILS.json')
    with target.open('x',encoding='utf-8') as stream:json.dump(result,stream,ensure_ascii=False,indent=2)
    body=BASE/(now.strftime('%Y%m%dT%H%M%S')+'-AUTHOR298_FAILURE_QUESTION.md')
    text=f'''实际采集时刻（Asia/Shanghai）：{now.isoformat()}

0298新作者真实响应已全部保留，诊断原件 {target.relative_to(ROOT).as_posix()}，SHA {hashlib.sha256(target.read_bytes()).hexdigest()}。
纠正0299汇总正文手填的12:05:00：实际桥接头采集为12:04:36.652845；汇总标签不能作未来采集证据。

1. A5 2.0.7首日21/21复核引用失败已查明是服务端目录漏接，而非21条虚构。原counterexamples每项确有evidence_id=A5V:MISS:*，002552原a3_candidate确有A3:CANDIDATE:002552.SZ，顶层citation_catalog只导出11条总览未导出这些条目。4f1b809按原ID+原事实hash+字段路径补接，63项相关测试通过；旧响应仍为原SCHEMA_FAILED，不重写。新输入只做一次10-15实际确认，再按通过条件推进其余日。
2. A6 1.0.5两日失败：10-14源24.736s，scope/utilization、adjustment、capacity；10-16源22.983s，两处opposing_search_scope。请按原件裁定作者修正；校验器不扩枚举、不默认接受。10-15源14计划已PASS38.406s，不充三日通过。
3. PM4.0.4绑定实际新A6 PASS后仍PM_DUPLICATE_SLOT_BINDING和PM_PRIMARY_CLUSTER_CAP_EXCEEDED，36.376s。原输出见诊断，原全局唯一/同簇<=2校验保留，请作者修正。
4. A4 R1指定ATR(5m)，当前真实市场context没有5m ATR，只有发布时日线ATR14。请明确5m ATR周期/算法/种子不足时处理；暂不把日线ATR冒充5m，也不因未知距离丢弃计划。已知入场带内/引擎事件/持仓仍纳入，其余ATR未知暂纳入并标UNPROVEN，继续R2/R3离线分片量化；不是正式R1验收。

CODE局部绿；REPLAY六日连续执行与角色全门仍未过；OPERATIONS生产0写入0部署；STRATEGY收益未证。A3按0298段字节一致+唯一10-16确认同集合继承PLAN_REVIEW三日门，PM会话另算。可独立工程继续，不重试同输入模型。
'''
    with body.open('x',encoding='utf-8') as stream:stream.write(text)
    print(json.dumps(dict(body=str(body),evidence=str(target),head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip())))


if __name__=='__main__':main()
