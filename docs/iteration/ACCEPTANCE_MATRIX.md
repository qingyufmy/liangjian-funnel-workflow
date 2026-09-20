# 需求—测试—证据矩阵

| 需求 ID | 当前要求 | 测试/检查 | 当前证据 | 状态 |
| --- | --- | --- | --- | --- |
| S00-SAFE | 离线验收不得真实下单、联网或通知 | `test_iteration_offline_guard_blocks_network`；验收脚本配置检查 | `s00-offline-final-20260920/summary.json` 的 S00-SAFE-01..03 | 通过 |
| S00-HARNESS | 可执行入口、JSON 总结、真实退出码 | `test_acceptance_entry_help_is_executable`、`test_non_offline_profile_never_claims_pass_without_evidence` | 正常入口 0；注入失败入口 1；53 tests passed | 通过 |
| S00-FIXTURE | fake clock/provider/LLM/临时库 | `test_iteration_fixtures_are_injected_and_state_is_temporary` | `s00-offline-final-20260920/test-results/S00-OFFLINE-TESTS.log` | 通过 |
| A4-01 | 全局 deadline 覆盖所有关键等待 | S03 将新增故障与时间边界测试 | 未开始 | 待 S03 |
| A4-02 | 持仓保护不被候选/归档阻塞 | S03 将新增慢 provider 反例 | 未开始 | 待 S03 |
| A1-01 | 字段覆盖逐层可对账 | S04 将新增覆盖账本测试 | 缺生产冻结样本 | 待 S04 |
| EX-01 | 佣金与印花税分开计费 | S05 将新增小额卖出反例 | 当前实现已复现错误 | 待 S05 |
| EX-02 | 只用下一根完整 1m bar 模拟撮合 | S05 将新增跨时钟因果测试 | 当前 quote bar 语义不一致 | 待 S05 |
| RISK-01 | 冻结数量、资金预占和 T+1 生命周期 | S05-S06 生命周期测试 | 当前仅部分满足 | 待 S05-S06 |
| LLM-01 | 严格布尔、完整集合、超时过期 | S07 schema 反例 | 当前字符串布尔可误判 | 待 S07 |
| EVAL-01 | 分层增益与反事实不混用 | S10 回放/统计测试 | 未开始 | 待 S10 |
