# 竞价研究资料准备与增量刷新拆分

## 根因和最小修复

09:26入口原来调用完整`prepare_snapshot`，复用A1股票名单却重新遍历公告、
业务报告和PDF，最终触及30分钟任务上限。不能把这解释成A4分钟数据故障。

本次保留现有采集、事实冻结、A1 registry、A2/A3量化和模型审核设施：

1. 增加07:00 `run-auction-base`，交易日07:00—07:14允许启动。
   只为当前封存A1准备事实，不调用模型、不发布计划、不物化另一份特征维护任务。
   父进程最多60分钟，结果须在08:15前完成；失败不反复全量重试。
   只有工作槽繁忙的跳过可以在启动窗口内重试。
2. 09:26的A2/A3研究只读取同日已完成基线。
   校验A1代次、股票集合、完整快照哈希、开始/结束/冻结时间及最多3小时基线年龄。
   缺失或不匹配直接给出具体原因，不恢复旧研究，也不退回全量同步。
3. 新采集股吧前100、板块和实时报价；行情采集阶段超过180秒不采用该增量。
   该检查不延长底层请求超时，整个研究仍由原30分钟父进程期限兜底。
4. 日线、财报、宏观、公告风险证据保留原日期、哈希和原文件路径。
   `AUCTION_REFRESH_CONTEXT`及模型可见的竞价/市场上下文写明基线时间、
   实际刷新字段、基线完成后公告未再次查询，以及新热榜但无基线证据的待补股票。
   本次不是09:26再次查询全部公告，不能称所有证据实时刷新。
5. 新增量快照独立保存；不覆盖原始证据，不修改已发布A4计划。
   09:26原有A4盘前报价复核和09:31分钟任务保持原路径。

公告缓存TTL和策略阈值均未放宽。早晨基线按现有正式采集规则准备隔夜资料；
其后出现的新公告仍存在明确的信息截止边界。若未来需要以该增量自动发布新计划，
必须另行完成发布前风险增量复核；本版本不授予这项权限。

## 需求—测试—证据

| 需求 | 测试/证据 |
| --- | --- |
| 基线校验，失败不重做全量 | test_auction_base.py：同日、过期、未完成、A1代次/范围/哈希错误 |
| 实际入口走新链路 | test_workflow_orchestration_coverage.py：实际run_research入口，不允许full/resume |
| 不覆盖事实、不改日期、不扩大股票池 | test_auction_base.py：不可变基线、风险/日线日期、新热榜缺证据清单 |
| 行情日期正确 | test_auction_base.py：前一日热度/板块/报价全部拒绝 |
| 中止后可诊断 | test_auction_base.py：SIGTERM写失败回执、释放租约、恢复handler |
| 调度隔离和期限 | test/server/auction-refresh.test.ts：07:00单次、60分钟期限，09:26研究不阻断A4 |
| 不影响既有主流程 | runtime_scheduler、pipeline_research、premarket、CLI、server回归 |

## 实际命令与结果

`python -m pytest tests/test_auction_base.py tests/test_auction_refresh.py tests/test_runtime_scheduler.py tests/test_cli_workflow_coverage.py tests/test_premarket_research_contracts.py tests/test_pipeline_research.py tests/test_workflow_orchestration_coverage.py tests/test_mootdx_data.py tests/test_mootdx_probe.py tests/test_post_close_repairs.py tests/test_eastmoney_hot.py -o addopts='' -q`

253 passed，15.85秒，退出0。测试数据库/临时文件独立，未执行生产模型调用。

`npm test -- test/server/auction-refresh.test.ts test/server/primary-comparison.test.ts test/server/server.test.ts`

62 passed，退出0。

`npm run typecheck`，`git diff --check`：退出0。
早期测试运行发现测试导入名称、mappingproxy复制和worktree依赖解析问题，修正后以上最终集合通过；
未把失败运行记为通过。

## 验收边界与下一步

- CODE：上述范围通过。
- REPLAY：结构、错误分支和日期/证据不变性通过；未回放完整真实07:00→09:26流程。
- OPERATIONS：需部署后核对实际导入源码、Node任务注册、明日计划；明日自然调度仍待验证。
- STRATEGY：阈值、A4权限和T+1不变，不宣称策略收益改善。

明日检查07:00基线回执是否READY，以及09:26增量输入的基线哈希和来源时刻。
通达信协议解码失败是另一项残留数据源问题，本次未把它宣称为已恢复。
