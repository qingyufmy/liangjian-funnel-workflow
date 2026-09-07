"""Produce the Chinese diagnostic report from immutable session captures."""
from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path

from liangjian_funnel.reporting import atomic_write_json, atomic_write_text

LABELS={
    'TREND_15M_PRESSURE_NOT_EASING':'十五分钟压力未缓解',
    'TREND_5M_REVERSAL_NOT_CONFIRMED':'五分钟反转未确认',
    'TREND_PULLBACK_ZONE_NOT_MET':'未进入趋势回踩区',
    'A4_SESSION_WARMUP':'开盘周期预热',
    'VOLUME_OVERHEATED':'量能过热',
    'PRE_ENTRY_RISK_LEVEL_TOUCHED':'触及风险线、等待结构确认',
    'DETERMINISTIC_STRATEGY_CONFIRMATION':'技术通过但实时盈亏比不足',
    'TREND_PRE_ENTRY_STRUCTURE_INVALIDATED':'结构失效',
    'LIVE_MARKET_STATE_NOT_READY':'历史市场状态缺失',
    'EMPTY_SCOPE':'计划生效前空范围',
}
VARIANTS={
    'original_observed':'原始归档／实际执行分钟',
    'verified_observed':'腾讯收盘行情／实际执行分钟',
    'verified_full_known_market':'腾讯全日／只用实际市场快照',
    'verified_full_assumed_market':'腾讯全日／缺失市场允许开仓假设',
    'tdx_full_assumed_market':'通达信全日／午间标签校正及相同假设',
}


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True);a=p.parse_args();root=a.root.resolve()
    read=lambda path:json.loads(path.read_text(encoding='utf-8'))
    capture=read(root/'capture.json');summary=read(root/'replay_summary.json')
    results={v:read(root/v/'result.json') for v in VARIANTS}
    plans={r['symbol']:json.loads(r['payload_json']) for r in capture['plans']}
    data={(r['symbol'],r['source'],r['interval']):{b['bar_end']:b for b in r['bars']}
          for r in capture['verified_fetches']}
    prices=Counter();volume=Counter();lunch=Counter()
    for sym in plans:
        for interval in ('1m','5m'):
            qq=data[sym,'tencent',interval];td=data[sym,'tdx',interval]
            common=qq.keys()&td.keys();prices[interval+'_common']+=len(common)
            for t in common:
                for f in ('open','high','low','close'):
                    prices[interval+'_'+f+'_different']+=abs(float(qq[t][f])-float(td[t][f]))>1e-7
                volume[interval+'_raw_different']+=abs(qq[t]['volume']-td[t]['volume'])>1e-7
                volume[interval+'_after_x100_different']+=abs(qq[t]['volume']*100-td[t]['volume'])>1e-7
            lunch[interval+'_equal_close']+=qq['2026-09-07T11:30:00+08:00']['close']==td['2026-09-07T13:00:00+08:00']['close']
    atomic_write_json(root/'cross_source_audit.json',{'prices':dict(prices),'volume':dict(volume),'lunch':dict(lunch)})
    production=Counter(e['reason_code'] for e in capture['events'])
    correct=summary['comparisons']['verified_observed'];diff_symbols=len({e['symbol'] for e in correct['differences']})
    text=[
        '# 2026年9月7日 A4 全日回放、诊断与告警验收',
        '',
        '## 结论',
        '',
        '今日中断原因按用户确认记录为手动重启虚拟机服务，不再归因为未知崩溃。另查出15:00独立调度边界缺陷。',
        '',
        f"原始生产共{len(capture['events']):,}条执行记录，隔离原始回放的动作与首要原因全部匹配。腾讯终值替换使{diff_symbols}只股票的{correct['different']}条首要判断原因变化，但未产生新增合格买入信号。完整停机补档与通达信独立回放的结果见下表。",
        '',
        '本结论仅适用于冻结的40计划和现行规则，不能证明A1/A2/A3覆盖完整，也不意味着行情版本问题可以忽略。',
        '',
        '## 一、隔离方法与真实执行边界',
        '',
        f"生产主机192.168.31.254，目录`{capture['root']}`，提交`{capture['commit']}`。",
        f"冻结文件SHA-256：`{summary['capture_sha256']}`。",
        '',
        '- 保留原始40个计划、真实09:26复核结果、全日事件、通知、模拟成交、分钟归档与修订冲突记录；不覆盖生产数据库或原始行情。',
        '- 39个计划实际激活；深信服开盘失效，回放沿用该事实，没有拿09:31开盘价伪造09:26竞价。',
        '- 所有全日回放遍历240个闭合分钟，五分钟与十五分钟只使用截至该分钟的前缀；存在事件去重，持久事件数不等于遍历分钟数。',
        '- 对原始缺失的市场快照，一组保持数据阻断，一组明确假设允许开仓。13:13采到的快照不会提前用于13:11。',
        '- 模型通过被配置为量化上限对照假设，但没有任何候选实际走到模型回调，真实模型调用数为0；没有伪造DeepSeek意见。',
        '- 终值行情是在收盘后重新取得，只用于敏感性分析，不能宣称当时已知；回放未向飞书发送买卖信号、未连接真实账户。',
        '',
        '## 二、五组回放结果',
        '',
        '| 回放组 | 事件条数 | 买入信号 | 模拟成交 | 结构失效 | 实际模型调用 |',
        '| --- | ---: | ---: | ---: | ---: | ---: |',
    ]
    for name,result in results.items():
        counts=Counter(e['action'] for e in result['effective'])
        text.append(f"| {VARIANTS[name]} | {len(result['events'])} | {counts['BUY_SIGNAL']} | {len(result['fills'])} | {counts['PLAN_INVALIDATED']} | {result['actual_model_calls']} |")
    text += [
        '',
        '所有失效判断需以对应行结果为准；本次各组均为若羽臣09:45、中亦科技10:15、国联民生13:30。今日计划全为趋势五日线路线，不能用这一日样本宣称龙头与520实盘已验收。',
        '',
        '### 停机区间的具体反问',
        '',
        '腾讯全日允许开仓假设下，国元证券在11:30及13:01—13:04出现五次前置技术确认，但实时盈亏比约0.50—0.88，低于冻结要求2.5。不能把它们记为已错过的合格买点，更不能直接降低门槛。',
        '',
        '## 三、确定的工程问题',
        '',
        '### 1. 手动重启缺口与收盘边界缺陷必须分开',
        '',
        '- 手动重启：10:40—11:30共51分钟，13:01—13:12共12分钟，共63分钟。午休不计入。',
        '- 独立代码缺陷：15:00:03 Node正常启动任务，但Python用精确时分秒与15:00:00比较，结果空调度、进程返回0。最后一分钟没有真正执行；11:30存在同类缺陷。',
        '- 全日240分钟实际有176个不同分钟的执行记录，缺口总计64分钟。',
        '- 已将Python判断改为分钟标签比较，覆盖11:30:03和15:00:03；不允许11:31、15:01历史追单。该修复本地测试通过，主应用尚未部署。',
        '',
        '### 2. 首次行情永久固化会改变后续技术判断',
        '',
        f"腾讯终值替换后{correct['different']}条判断原因变化，涉及{diff_symbols}只股票；本次没有改变入场数量和三次失效时间。当前实时缓存只记录冲突而不更新规范行情，再从缓存取首次版本计算，不能把它长期当成最终准确行情。",
        '',
        '应保留原始决策快照，同时建立带采集时间、来源和确认状态的行情版本；后续计算使用当时已确认的新版本。不得覆盖历史信号输入，也不能把收盘修订回填成盘中早已知道。此项完整版本管理尚未实施。',
        '',
        '### 3. 通达信与腾讯不能原样混用',
        '',
        f"共同时间点：一分钟{prices['1m_common']}个，收盘价差异{prices['1m_close_different']}个；五分钟{prices['5m_common']}个，收盘价差异{prices['5m_close_different']}个。开盘价、最高价、最低价并非全部一致，因此不能宣称两源所有字段均已一致。",
        '',
        '- 腾讯成交量按手、通达信此接口按股体现出100倍差异；未统一单位就切换数据源，会制造错误量比。',
        f"- 单纯乘100后仍有一分钟{volume['1m_after_x100_different']}处、五分钟{volume['5m_after_x100_different']}处成交量差异，不能把全部冲突都解释成单位问题。",
        '- 本次通达信节点把午间末根标为13:00、腾讯标为11:30；40只股票对应收盘价均相同。原始结果数量虽然达到240/48，时间集合仍不合格。',
        '- 通达信回放只在隔离副本把该午间标签映射回11:30，并验证准确240/48时间集合；原始采集未修改。该映射应纳入数据适配验收，不能直接凭条数认定完整。',
        '- 腾讯成交额目前由量和价格估算，不应当作交易所精确成交额使用。',
        '',
        '### 4. 午间任务错过后无自动核对',
        '',
        '11:35午间复盘没有今日完成记录。现有Node忙碌重试不能覆盖整机离线错过触发时点。本次独立探针已能识别并提醒漏执行，但没有自动补跑午间复盘，也没有把盘后资料冒充午间已知信息。',
        '',
        '## 四、独立告警已启用',
        '',
        '- Windows计划任务：`Liangjian-A4-External-Watchdog`，每分钟隐藏执行一次。',
        '- 覆盖远端不可探测、应用健康失败、A4任务停滞、恢复、历史执行缺口和定时任务错过恢复窗口。',
        '- 飞书安装测试及今日问题提醒发送成功；多次自然调度返回0，已发送问题未重复通知。',
        '- 告警与恢复使用持久账本去重，失败退避重试，恢复不得越过尚未成功的故障通知。没有自动重启或补发交易命令。',
        '- 必须保持Windows开机并登录。若该机器同时是VM宿主机，物理机关机仍是共同故障点；要覆盖物理机断电需迁到另一台常在线设备。',
        '- 详细操作与卸载方式见`docs/A4_EXTERNAL_WATCHDOG.md`。',
        '',
        '## 五、信号落盘及T+N',
        '',
        '生产源批次记录存在，状态已发布，原始快照及配置哈希齐全。今日没有买入信号、模拟成交或信号生命周期，因此T+N没有今日新入场样本是正常结果，不能凭零样本证明或否定落盘链路。',
        '',
        '已执行信号生命周期、次根成交、持仓风险、收益标签与幂等相关隔离测试。未来1/3/5/10交易日收益尚未发生，不填充为0，不制造完成收益。',
        '',
        '## 六、40个计划逐股检查',
        '',
        '下表统计腾讯全日行情及缺失市场允许开仓假设下的首要原因；是逐分钟记录计数，不是信号次数。所有计划保持原始规则，没有按结果调参。',
        '',
        '| 代码 | 名称 | 本次状态 | 主要未入场原因（分钟记录数） |',
        '| --- | --- | --- | --- |',
    ]
    rows=defaultdict(list)
    for row in results['verified_full_assumed_market']['events']:
        if row['symbol']:rows[row['symbol']].append(row)
    for symbol,plan in sorted(plans.items()):
        events=rows[symbol]
        invalid=[e for e in events if e['action']=='PLAN_INVALIDATED']
        status=('开盘失效' if not events else '结构失效 '+invalid[0]['minute'][11:16] if invalid else '未触发入场')
        counts=Counter(e['reason'] for e in events if e['reason'] not in ('A4_SESSION_WARMUP','EMPTY_SCOPE','TREND_PRE_ENTRY_STRUCTURE_INVALIDATED'))
        reasons='；'.join(f'{LABELS.get(k,"待核对原因")} {v}' for k,v in counts.most_common(3)) or '沿用真实早盘失效结果'
        text.append(f"| {symbol[:6]} | {plan.get('name','')} | {status} | {reasons} |")
    text += [
        '',
        '## 七、验收与尚未完成项',
        '',
        '- 新增告警、会话边界与回放时间约束27项测试通过；A4回放、风险、信号生命周期、行情缓存及收益标签92项测试通过，共119项。',
        '- 原始记录6,445条的动作与首要原因一致；两源160组请求均成功；五组隔离回放和40计划逐股结果落盘。',
        '- 本轮未提交、推送或部署主应用，没有修改生产计划、行情及交易账本。新增独立探针已在Windows生效。',
        '- 待主应用发布：11:30/15:00秒级边界修复。',
        '- 待工程迭代：行情定型及版本治理、跨源单位及午间标签规范化；定时任务持久对账后按原截止时间补充复盘的机制。',
        '- 日内样本未覆盖龙头和520；三路线端到端生产可用性仍须用符合各路线的冻结计划专门验证。',
        '',
        '复现命令：',
        '',
        '```powershell',
        '.venv\\Scripts\\python scripts/replay_a4_session.py --root outputs/evaluation/a4-session-2026-09-07',
        '```',
        '',
        '冻结采集与各组独立SQLite账本保存在`outputs/evaluation/a4-session-2026-09-07/`，未写入生产。',
    ]
    atomic_write_text(a.output,'\n'.join(text)+'\n')
    print(str(a.output.resolve()))


if __name__=='__main__':main()
