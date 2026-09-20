# 实施累计报告

## S00

已建立隔离分支、保存原始 CI 基线，新增跨平台验收入口、JSON/manifest 产物、离线网络与通知保护、fake clock/provider/LLM 和临时 RuntimeStore fixture。当前业务代码尚未修改；已核实的问题保留到对应依赖阶段解决。

离线入口真实运行 53 个测试并返回 0；故障注入按预期返回 1，产物未发现凭证模式。S00 已封存，下一步进入 S01 契约与可观测性。回放、生产操作和策略有效性均未验收。

## S01

在既有 outcome v3 上补充非成功终态，并同步 Python、JSON Schema、Server 和 Web 生成类型。新增纯投影模块，A4 每轮输出稳定 `run_id/decision_id`、独立状态轴、范围分母、来源尝试、版本、真实耗时、未测耗时和排除时钟噪声的决策哈希。

首次全量回归暴露 `timing_coverage` 归属错误（5 failed），修复后全量为 1734 passed、4 skipped、覆盖率 78.62%；前端 86 tests、类型检查和构建通过。S01 只增加可观测性，没有改变策略阈值、候选数量或交易权限。

## S02

在既有 `RuntimeStore` 中增加 provider 配额、并发槽、请求 lease、`last_good` 和尝试账本；请求 lease 使用 fencing token，过期 owner 不能覆盖接管者结果。新增 `provider + capability + account_scope` 注册契约，明确字段、市场、单位、复权、时间精度、授权、配额、TTL、过期动作、备用候选和真实上游身份。

`ProviderGovernor` 实现进程内及持久层同请求合并、共享 quota scope、有界调用总时限、有限重试、429 `Retry-After`、失败冷却/单一半开探测、响应大小限制和可证明时间检查。坏响应不覆盖 `last_good`；旧缓存只允许展示，禁止用于新开仓。备用源通过代码、交易日、周期、单位、复权、字段覆盖、时间资格和上游独立性检查；URL 入口仅允许 HTTPS 白名单公网主机。

首次聚焦测试暴露默认时钟无时区及测试 SSRF fixture 缺少导入（9 failed），修复后 `SRC-01—SRC-08` 等 16 项测试通过。全量回归为 1750 passed、4 skipped、覆盖率 78.68%；前端 86 tests、类型检查和构建通过；离线验收入口为 78 passed。S02 只提供并验证治理层，尚未改变 A4 获取顺序；实际 A4 关键链接入留到 S03。

## S03

A4 现在从调度轮开始即使用同一绝对 deadline。已有持仓先走独立报价 gate 和确定性硬止损，风险意图在 A3 恢复、市场广度、候选历史、LLM 与归档之前落账；报价过期或缺失只产生明确 `DATA_BLOCK`，不虚构退出价。晨间 A3 恢复也接受该 deadline，并在原子状态变更前再次检查，迟到恢复不能修改当轮范围。

新增有界 daemon work gate：超时依赖继续占用有限槽位，后续请求得到 `BACKPRESSURE`，不会用 `future.cancel()` 假装取消，也不会在 executor 退出时无限等待。决策标的按股票独立获取，原生 5m 和失效计划归档从关键链移除；失效计划可由 `archive-a4-auxiliary-once` 使用独立 gate 补充归档，且不能产生交易事件。5m/15m 继续由同一冻结 1m 序列派生。模型返回越过绝对 deadline 时只写 `MONITOR_OVERRUN`，不能产生迟到买单。分钟归档与 RuntimeStore 保持独立 SQLite，归档写锁反例不阻塞风险意图短事务。

S03 聚焦及扩大回归覆盖 `A4-01—A4-12`。本机离线墙钟压测生成 240 分钟完整交易会话 fixture，在 50/100/200 计划档位各抽取 10 个风险轮次；200 计划风险 p99 为 1.130 秒，决策核心轮次为 1.207 秒。该结果仅是当前 Windows 开发机离线代码证据，不代表虚拟机或生产网络延迟，也不证明策略收益。
