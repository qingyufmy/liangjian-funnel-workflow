/** Only source metadata is rendered, never provider error/customer bodies. */
export function healthEventText(event){
  const role=event.role??event.job??'系统';
  const status=({READY:'完成',DEGRADED:'降级',BLOCKED:'阻断',FAILED:'失败',CLAIMED:'未完成，终态未收口（不代表当前运行）',CAPTURED:'已捕获答复'})[event.status]??'状态未记录';
  switch(event.kind){
    case 'RISK_MEETING':return `A6晨审：${status}；${(event.reason_codes??[]).includes('A6_REVIEW_DEGRADED')?'采用降级保护':'权限以晨会原件为准'}。`;
    case 'PM_MEETING':return `PM：${status}${event.decision_authority==='SERVER_FALLBACK_NOT_MODEL_VALIDATED'?'；服务端后备，不是模型校验通过':''}。`;
    case 'MODEL_CLAIM':return `${role}请求：${status}${event.reason_code==='TEAM_MODEL_CALL_FAILED'?'；模型调用失败，HTTP状态见独立原件':''}。`;
    case 'PROVIDER_HTTP_ERROR':return `${role}：供应商HTTP ${event.http_status??'未知'}；只表示该次调用失败。`;
    case 'NODE_PROCESS_START':return `Node进程启动${event.pid?`（PID ${event.pid}）`:''}；启动不等于业务链已验收。`;
    case 'NODE_PROCESS_STOP':return 'Node进程停止原记录；恢复状态须看后续原件。';
    case 'NODE_OFFLINE_OBSERVED':return '当时观察到Node离线；这是观测界，不是精确故障起点。';
    case 'NODE_RECOVERED_HEALTH200':return '本回执观察到Node健康请求200；不是精确或首次恢复起点，不替代交易业务验收。';
    case 'MODEL_ROLE_CHANGED':return `实际模型配置切换：${Object.entries(event.model_roles??{}).map(([key,value])=>`${({research:'研究',review:'复盘',monitor:'客户A4',team_intraday:'Team A4'})[key]??'角色'} ${value}`).join('；')||'角色详情缺源'}。`;
    case 'JOB_TERMINATED':return `${role}任务已终止；没有补跑或补写。`;
    case 'JOB_FAILED':return `${role}任务失败；原因以原记录为准。`;
    default:return '已有历史元数据，尚无已核展示口径。';
  }
}
