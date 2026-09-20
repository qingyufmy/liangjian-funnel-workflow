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
