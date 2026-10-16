# G5 实际数据源接线阶段回执

实际实施/采集：2026-10-17 01:53–02:10 Asia/Shanghai。依据 0247；无生产访问、源探测、真实模型、通知或发布。

## 源与运行边

1. 08:33 引擎晨会新增独立情绪基线：现有 `HithinkClient.ticker_catalog(limit=1000,max_pages=10)` + `limit_up_ladder()`，沿用客户端供应商限流。总等待 10s，单在途槽；worker 不写文件，迟到不会再请求梯队或发布 READY。失败原件保留、不静默重试，缺凭证不发请求。此基线不新增交易许可门。
2. 昨日连板使用现有 `project_closed_ladder` 的严格日期窗口投影，不用涨停池 `continue_day_cnt`。完整目标日梯队存在、每项 board_num 合法且无重复，才允许将其未列出的目录股票标为 0；带 `ABSENT_FROM_COMPLETE_DATED_LADDER` 依据，不是猜零。原 source result、原 timestamp、版本与内容 hash 一并封存。
3. `live_market` 新增可选 observer。只消费当前已完成的同一次全市场请求，不自行再抓行情；正常缓存复用不再次调用 observer。对 Team 请求，若缓存行情原时标早于本 5m，既有采集器重新取证，不能把“采集完成时间新”当“行情时标新”。索引降级不能供应情绪四量；observer 异常不破坏原 legacy primary 结果。
4. observer 的 normalized 源原行 + 逐股票身份字段/排除原因 + 统计结果以确定性 gzip 封存到自有 `state/team_v2/emotion_live`，缓存只保留短摘要、相对路径与原解压内容 SHA。没有把 normalized JSON 称为 HTTP 原字节，也不宣称 source_authenticated。
5. 07:00 自然 auction-base 已有内存快照额外输出紧凑板块历史上下文，引用 parent snapshot id/hash；失败隔离，绝不因此改变 auction-base READY/失败。读取需父自然回执仍 READY 且 id/hash 一致。板块历史 `as_of` 原样保留，明确 `HISTORICAL_SECTOR_GEOMETRY_NOT_YESTERDAY_REGIME`；不是伪造盘中板块历史。G5 每次用真实当前广度重算原分类，不延续昨日 regime。
6. CONTROL 闭合 5m 边接 `record_current_regime` → 原 `record_live_regime`，连续两根确认、不跨午休/缺口、同 tick 重试使用原冻结结果。情绪缺失不阻断有证据的普通主线分类，但 LEADER/radar 仍需 EMOTION_ON。LEADER 新开仓被该状态拦截，持仓退出独立。
7. G5 计算/写库异常只记 `CONTROL_LIVE_REGIME_UPDATE_FAILED` 并拦新开仓，不能打断本分钟持仓撮合/保护。所有转换通知仍 PENDING，尚未发送。

## 原始检查

- 首次晨间 source 关联 5 失败：新测试用了错误的 env 名 `HITHINK_API_KEY`；改为现有合同 `HITHINK_FINANCE_API_KEY` 后关联 52 绿（测试夹具修正，生产密钥未读写）。实际 HithinkClient + HTTP MockTransport 两个接口形状验证通过。
- G5 接线关联 91 通过/0 失败：`20261017T020545055952-g5-natural-source-first-receipt.json`，SHA `dac890451cde31104c8532e0e0c75c2056fa83625624401aafeb0f26ba6b17b4`。
- 新故障隔离差异及晨会/CONTROL 关联 50 通过/0 失败：`20261017T020647236545-g5-source-failure-isolation-final-receipt.json`，SHA `47225dd156c518fd23bfd78c3bc07ef8e3b90c648ff946da98f7c4912568d173`。与 91 有重叠，不相加。
- 5,000 行本机计算与压缩封存：0.391s，READY，源网络请求 0。首次合成样本误含未支持的 004xxx，正确排除 1001 行导致测试预期失败；改为全部支持域后单项通过。回执 `20261017T020743678138-g5-local-5000-supported-row-budget-receipt.json`，SHA `c7ef704b235be21629669e80c640884ec3e52a0a464195849b7de2dddfdce203`。不称 VM 或包含行情网络的 phase 预算通过。

回执均位于本仓 `artifacts/team-v2-20261016/`，所有初次失败保留。

## 未完成与四层口径

CODE：上述 source/auction sidecar/晨间基线/现有 CONTROL 接线关联通过；未宣称 Team 最终全量。当前运行边仍依赖晨会已建立的 CONTROL；空计划日的 G5 独立驱动尚待补齐，不能以此宣称无计划时雷达必能启动。

REPLAY：只有源、状态、连续确认与故障反例，不是正式五日情绪开关/六日团队回放。

OPERATIONS：未部署、未做 VM 网络/预算验收；转换和告警只记 PENDING。周一实际目录时标、梯队覆盖及自然运行仍待观察。

STRATEGY：收益/真实触发不作结论。下一步 A6 异常会话、G5 空计划日独立边、剩余角色/A5 接线，再正式干跑回放；H1 周日独立发布不变。
