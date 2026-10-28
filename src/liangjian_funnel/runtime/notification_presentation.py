"""Delivery-only Chinese view; original internal body/hash remain immutable."""
from datetime import datetime
import re
from zoneinfo import ZoneInfo


EVENT_LABELS = {
    'MODEL_REQUEST_FAILED':'模型请求失败',
    'AUDITED_SCOPE_OR_BUDGET_STOP':'审计范围或预算守卫停止',
    'OWNED_CHILD_NONZERO_EXIT':'自有研究子进程异常退出',
    'COMPLETE':'完整','EMOTION_OFF':'情绪模式关闭','EMOTION_ON':'情绪模式开启',
    'MOOTDX':'通达信行情','PRIMARY':'主数据源','RISK_OFF_RETREAT':'风险退潮状态',
    'ROTATION_NO_MAINLINE':'轮动未形成主线','TEAM_MONITOR_DEGRADED':'盘中监测降级',
    'DECISION':'引擎准入核验','FILL':'模拟成交记录','REGIME':'市场状态变化','SETTLEMENT':'模拟账户结算',
    'A4_REVIEW_ENGINE_ONLY':'降级模式开始·仅引擎',
    'A4_REVIEW_RESTORED':'降级模式结束·模型复核已恢复',
    'A4_ENGINE_ONLY_ADMISSION':'降级模式·准入核验',
    'A4_REVIEW_TRUNCATED':'模型复核候选截断',
    'A4_REVIEW_ORIGINAL_BUY_REJECTED':'原买入结论被校验拒绝',
    'A4_ORIGINAL_ENTRY_REJECTED':'原买入结论被校验拒绝',
    'NATURAL_CLOSE_TIMEOUT':'自然收盘进程终止','CLOSE_AUDIT_GUARD_STOP':'审计研究守卫停止',
    'A4_BUY_VALIDATION_REJECTED':'原买入结论被校验拒绝',
    'AUDITED_RESEARCH_FAILED':'收盘研究进程终止',
    'AUDITED_RESEARCH_DATA_STALLED':'收盘研究数据阶段进度告警',
    'AUDITED_RESEARCH_REVIEW_NEEDED':'收盘研究等待范围裁定',
    'PREMARKET_BATCH_READY':'盘前计划最终就绪',
    'PREMARKET_A3_STATUS':'盘前计划最终状态',
    'MODEL_PROVIDER_UNAVAILABLE':'模型通道不可用',
    'MODEL_PROVIDER_RECOVERED':'模型通道恢复',
    'NODE_IDENTITY_CHANGED':'主程序进程变化','ALERT':'系统告警',
}


def native_card(*,kind,body,observed_at,event_id,diagnostics=None,internal=True):
    from .lark import LarkNotifier
    from .lark_notifications import _DISPLAY_LABELS, _ACTION_LABELS
    at=datetime.fromisoformat(observed_at) if isinstance(observed_at,str) else observed_at
    if at.utcoffset() is None:
        raise ValueError('NOTIFICATION_CLOCK_UNPROVEN')
    at=at.astimezone(ZoneInfo('Asia/Shanghai'))
    original=body['content']['text']
    codes=[];unmapped=[]
    aliases={'BUY':'买入','ADD':'加仓','SELL':'卖出','REDUCE':'减仓','NO_ACTION':'不动',
        'ACCEPTED':'准入通过','REJECTED':'准入拒绝','READY':'就绪','BLOCKED':'拒绝',
        'HTTP':'网络请求','MODEL':'模型','CNINFO':'公告源','GLOBAL':'全局事实',
        'NEWS':'新闻源','SENT':'已发送','UNKNOWN':'不可用','None':'不可用','null':'不可用',
        'POST_CLOSE':'盘后复盘','MIDDAY':'午间复盘','TEAM_A5_STAGE_FAILED':'复盘阶段失败',
        'CONTROL':'受控模拟','SHADOW':'影子观察','APPROVED':'准入通过','REDUCED':'缩量准入',
        'UNAVAILABLE':'不可用','MATCH':'一致','TEAM_A5_POST_CLOSE_FAILURE':'盘后复盘失败'}
    def label(match):
        value=match.group(0);codes.append(value)
        translated=EVENT_LABELS.get(value) or _DISPLAY_LABELS.get(value) or _ACTION_LABELS.get(value) or aliases.get(value)
        if translated is None:
            unmapped.append(value)
            return '系统内部状态，详见内部通道'
        return translated
    text=re.sub(r'\b(?:None|null|[A-Z][A-Z0-9]{2,}(?:_[A-Z0-9]+)*)\b',label,original)
    def timestamp(match):
        value=datetime.fromisoformat(match.group(0).replace('Z','+00:00'))
        return value.astimezone(ZoneInfo('Asia/Shanghai')).strftime('%m月%d日 %H:%M:%S 北京时间')
    text=re.sub(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|[+-]\d\d:\d\d)',timestamp,text)
    stock=re.search(r'\b\d{6}(?:\.S[HZ])?\b',text)
    title=EVENT_LABELS.get(kind,'系统状态通知')
    if stock:title+='｜'+stock.group(0)
    title+='｜'+at.strftime('%H:%M 北京时间')
    next_action = ('下一自然节拍按原计划与数据、仓位和风控门处理；不人工放行。'
        if kind=='A4_ENGINE_ONLY_ADMISSION' else
        '模型复核当日停用，引擎仅能通过原准入门及加严仓位、价格纪律；每次准入尝试另报。'
        if kind=='A4_REVIEW_ENGINE_ONLY' else
        '无需操作；首次有效模型复核已落盘，后续继续按自然节拍复核。'
        if kind=='A4_REVIEW_RESTORED' else
        '本次研究未完成，候选状态以正式封存与验收结果为准；不将未知记为零。'
        if kind=='AUDITED_RESEARCH_FAILED' else
        '该原买入结论未获发布；保留原回复与拒绝原因，交运维核验，不人工放行。'
        if 'BUY' in kind and 'REJECT' in kind else
        '保留本次原件与原因；系统按原调度和准入规则继续，运维核验异常。')
    if diagnostics is not None:
        diagnostics['unmapped_code_counts']={code:unmapped.count(code) for code in set(unmapped)}
    lines=[f'• 发生了什么：{text}']
    if stock:
        lines.append('• 价格与条件：以下原件有值才引用；未留存的当时价格或条件数明确不可用，不以当前值补齐。')
    lines.extend(['• 接下来：'+next_action,
        '追溯：'+event_id+('；'+kind+('；'+','.join(sorted(set(codes))) if codes else '') if internal else '')])
    if unmapped and internal:
        lines.insert(-1,f'• 说明映射告警：{len(set(unmapped))}种原码尚待补齐；原码已附追溯，请运维核验。当前状态不因此放行。')
    return LarkNotifier.build_card(title,lines,'orange')
