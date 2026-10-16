# CONTROL 实际入口接线检查点

实际采集时间：2026-10-17 00:44 Asia/Shanghai。源码 HEAD `5f1251b1345ee2b44941e6283afbf8b35e1c0139`。已实际全文消费 `0242-reply`，0238/0240 已答复，不再作为待裁定阻断。

## 完成范围

`run-monitor → run_scheduled → monitor_dispatch → run_monitor_control` 已接线；手动 `monitor-once` 同一分流。默认 CONTROL、显式 RECORD 回到已有 H-1 保护的旧路径，CONTROL 阻断不隐式执行第二个引擎。仅 local_paper，gm_sim/gm_live 未接时直接拒绝。

CONTROL 读取原计划/合法晨审激活及历史持仓归属，沿用既有行情回调和限流，冻结当前分钟源；已有委托撮合、每分钟硬退出先行，五分钟时才模型 → PM 槽位 → 最新账户风险交集 → 原子模拟委托。晨会缺失不影响已有持仓的硬退出。所有写入局限独立 Team/paper/model 库，不改 execution_plans。

涨跌停投影复用现有严格核心，不加代码前缀比例表。缺同日规则/原始 PIT 材料为 RULE_UNCONFIRMED。09:45 后缺当桶实时状态不沿用盘前许可。风险 yaml 与晨会 hash 不同则阻断新开仓，硬退出仍先执行。

新反例发现同一分钟第二次调用的市场/时间字段漂移导致 CONTROL_MINUTE_SOURCE_CONFLICT；已按原 packet 绑定首次市场视图并固定分钟 as_of。真实 handler 两次调用现只产生一次 HTTP mock、一次 paper 委托。

## 红绿证据

均位于 `artifacts/team-v2-20261016/`；回执绑定当时工作内容 SHA（运行时 git HEAD 仍 f6eb1d5），不是最终 Team 全量或 VM。

- `20261017T004119442370-control-dispatch-same-minute-red`：exit 1，真实输入冲突。
- `20261017T004203482059-control-dispatch-same-minute-green`：exit 4，命令误写不存在的 tests/test_cli.py，未运行测试，不能称绿。
- `20261017T004223900798-control-dispatch-same-minute-qualified`：90 通过、1 失败；重试业务已经通过，失败为新增断言误用 orders 表名（实际 paper_orders），原件保留。
- `20261017T004252817459-control-dispatch-retry-final`：91 通过、0 失败、exit 0。包含 dispatch/control/source/tradability/settings/CLI/scheduler。
- 最终 receipt SHA256：`419d427b6a8b209a3a1610ae6199da11d2c1fa8ad99f451ece800563de21ed38`。

前轮 64/36 项与本轮重叠，不累加。正向行情/模型/规则是隔离夹具与真实客户端 HTTP mock，不是实网、自然生产或完整规则 producer 验收。

## 四层及下一步

CODE：本切片通过。REPLAY：仅上述隔离链路，正式六日尚未完成。OPERATIONS：未部署、未触生产、未安装告警或发通知。STRATEGY：无收益结论。

仍须补当前正式规则/PIT producer、G5 实时四项计数 producer、0242 批准的双 fill_basis（逐笔/结算/A5 分列）、08:45 内部摘要与 EOD/09:26 调度、其余角色适配和全套发布门。估算基准现已获裁定，下一步实现，不把本轮 reported-only 投影说成已满足。外围扩张 yaml/最小既有供应商源排在核心之后。

H-1 f79 的最终 ACCEPT 和周日白天独立发布顺序不变。Team V2 未获得最终候选发布放行，不 push/deploy。
