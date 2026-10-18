"""Exclusive bridge-ready checkpoint derived from actual frozen originals."""
from datetime import datetime
import hashlib,json,subprocess
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1];BASE=ROOT/'artifacts/team-v2-20261016'


def main():
    now=datetime.now(ZoneInfo('Asia/Shanghai'));items={}
    directories=['a4-author104-sharded-1013-real-once','a1-author303-1014-discovery-real',
        'a2-author304-1014-real','a2-author304-1015-real','a2-author304-1016-real',
        'a3-author404-1016-confirm-real',*(f'a5-author207-10{day}-catalog-bound-real' for day in ('14','15','16'))]
    for name in directories:
        path=BASE/name/'RESULT.json';body=json.loads(path.read_bytes())
        items[name]=dict(path=str(path.relative_to(ROOT)),sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            status=body['status'],reason_codes=body.get('reason_codes',body.get('issues',[])))
    summary=sorted(BASE.glob('*-REAL_DRYRUN_SUMMARY.json'))[-1]
    counts=json.loads(summary.read_bytes());head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()
    a4=json.loads((BASE/'a4-author104-sharded-1013-real-once/RESULT.json').read_bytes())
    proof=dict(actual_collected_at=now.isoformat(),head=head,originals=items,
        summary=dict(path=str(summary.relative_to(ROOT)),sha256=hashlib.sha256(summary.read_bytes()).hexdigest(),
            requests=counts['actual_request_count_including_pending'],responses=counts['actual_response_count'],
            failed_claims=len(counts['provider_failed_requests']),pending=len(counts['pending_requests'])),
        production_writes=0,models=0,formal_release_gate='NOT_MET')
    evidence=BASE/(now.strftime('%Y%m%dT%H%M%S')+'-AUTHOR298_CHECKPOINT.json')
    with evidence.open('x',encoding='utf-8') as stream:json.dump(proof,stream,ensure_ascii=False,indent=2)
    body=BASE/(now.strftime('%Y%m%dT%H%M%S')+'-AUTHOR298_CHECKPOINT_QUESTION.md')
    text=f'''实际采集时刻（Asia/Shanghai）：{now.isoformat()}

0298继续执行，0300的作者格式及ATR定义问题仍待答；本轮不请求额外生产权限。
原件索引 {evidence.relative_to(ROOT).as_posix()} SHA {hashlib.sha256(evidence.read_bytes()).hexdigest()}。

已实证完成：
- A3 4.0.4 PLAN_REVIEW字节一致+一次10-16确认PASS集不变，按0298继承4.0.3三日门；PM另算。
- A5 2.0.7：原目录漏掉真实A5V:MISS与嵌套A3:CANDIDATE引用，已4f1b809补原ID+hash+字段路径（63相关绿）。新输入10-15一次PASS后10-14/16也PASS，三日152048/157948/134741ms。旧21条引用失败原件不改不追认PASS。后台标签、客户A5原文完全不改。
- A1 3.0.3：discovery一次真实VALIDATED，12主题41节点，CATALYST_GAP实际0，不是把缺失写成可用；股票映射尚未跑，不能报A1全会话三日门。C5及每主题3节点/2链段37相关绿。

新增真实失败：
1. A4 1.0.4：先0301报真实离线43事件计划/6片，才启动一次同批分片探针；4并发、每片<=30s、同47s截止。实际总耗时{a4['elapsed_seconds']:.3f}s，6个真实claims全MODEL_WALL_CLOCK_TIMEOUT，0响应/0订单，无校验成功可报，不做同输入重试。当前每8条约164k字符（单包原2179k），输入投影不足以通过30s门；不是网络错误定论，也不能归因模型思考，失败没有response latency字段。
   请裁定下一步输入/会话方案。R1本批43原动作全START_CONFIRMATION，按“引擎非默认动作”不能静默丢弃。可审方向：作者是否指仅新触发状态转换、或进一步无损紧凑OHLCV字段编码、或明确A4也关闭thinking？均未擅自启用；不增30s、不改47s、不换模型/限流、不靠截计划凑PASS。其他NO_ACTION批次5m ATR周期/种子仍待0300。
2. A2 3.0.4：10-14和10-15原入口VALIDATED；10-16被A2_SYMBOL_OUTSIDE_FROZEN_DOMAIN:focus_decisions、A2_THEME_OUTSIDE_FROZEN_DOMAIN、A2_ROTATION_COVERAGE_MISMATCH、TEAM_OPPOSING_REFS_INVALID:market_read挡住。完整原STAGE_AUDIT/model.db/FIELD_DIFF在a2-author304-1016-real，原响应不修，未再调用。请作者按该日真域修正。
3. PM4.0.4/A6 1.0.5的失败仍是0300所列，未自动排槽/改容量/放宽scope。

CODE：新A4并行capture→串行全局风控71相关绿，持仓首片、8上限/4并发、全局利用率第二单拦截、部分失败、43不丢、截止后不准入、重启不重调；红测原件保留，不充最终全量/VM。
REPLAY：{proof['summary']['requests']}个实际原请求、{proof['summary']['responses']}原响应、{proof['summary']['failed_claims']}失败claims、当前{proof['summary']['pending']}未完成；各角色各版本分组，旧成功不充新门。索引summary SHA {proof['summary']['sha256']}，本轮325文件已知两凭证精确字节扫描0命中，不宣称全机扫描。六日连续有资金/99%合法/护栏零越过/曲线一致门未完成，不部署Team。
OPERATIONS：本轮生产0写入0部署0客户通知；H1周日白天独立ACCEPT版本不变，不重测既有全量。STRATEGY收益未证。

继续可独立的映射与回放驱动接线；待新作者/工程裁定才改变上述原输入，不重复相同请求。
'''
    with body.open('x',encoding='utf-8') as stream:stream.write(text)
    print(json.dumps(dict(body=str(body),head=head,summary=proof['summary'])))


if __name__=='__main__':main()
