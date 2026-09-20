# 需求—测试—证据矩阵

| 需求 ID | 当前要求 | 测试/检查 | 当前证据 | 状态 |
| --- | --- | --- | --- | --- |
| S00-SAFE | 离线验收不得真实下单、联网或通知 | `test_iteration_offline_guard_blocks_network`；验收脚本配置检查 | `s00-offline-final-20260920/summary.json` 的 S00-SAFE-01..03 | 通过 |
| S00-HARNESS | 可执行入口、JSON 总结、真实退出码 | `test_acceptance_entry_help_is_executable`、`test_non_offline_profile_never_claims_pass_without_evidence` | 正常入口 0；注入失败入口 1；53 tests passed | 通过 |
| S00-FIXTURE | fake clock/provider/LLM/临时库 | `test_iteration_fixtures_are_injected_and_state_is_temporary` | `s00-offline-final-20260920/test-results/S00-OFFLINE-TESTS.log` | 通过 |
| OBS-01 | 作业、数据、机会、资格独立表达 | `test_external_failure_has_independent_job_data_opportunity_and_eligibility_axes` | S01 全量回归 | 通过 |
| OBS-02 | 非关键降级可见但不误阻断 | `test_noncritical_degradation_remains_visible_without_blocking_trade` | S01 全量回归 | 通过 |
| OBS-03 | 稳定关联 ID 且秘密被清理 | `test_observability_redaction_removes_secret_and_url_query_values`；`test_monitor_consumes_persisted_revision_or_blocks` | A4 `observability` 投影与哨兵测试 | 通过 |
| OBS-04 | 同一冻结输入决策哈希稳定 | `test_observation_hash_excludes_wall_clock_and_timing_but_tracks_frozen_input` | S01 全量回归 | 通过 |
| OBS-05 | p50/p95/p99 来自真实 span，缺失明确 | `test_timing_percentiles_are_measured_from_spans_and_missing_is_explicit` | A4 `timing_summary/timing_coverage` | 通过 |
| SRC-01 | 50 路相同对象请求合并且结果隔离 | `test_src_01_fifty_concurrent_identical_requests_are_singleflight` | S02 聚焦测试及全量回归 | 通过 |
| SRC-02 | 配额/冷却跨重启且 quota scope 共享 | `test_src_02_quota_and_cooldown_survive_restart_and_share_scope` | 临时 RuntimeStore 重开验证 | 通过 |
| SRC-03 | 429、超时、恢复的重试有界 | `test_src_03_rate_limit_timeout_then_recovery_has_bounded_attempts` | 尝试账本顺序 `RATE_LIMITED→NETWORK_TRANSIENT→OK` | 通过 |
| SRC-04 | 空集、未发布、schema、鉴权状态不同 | `test_src_04_terminal_source_states_are_distinct_and_not_retried` | 参数化反例各只调用一次 | 通过 |
| SRC-05 | 坏响应不覆盖 last_good，过期不可交易 | `test_src_05_bad_response_keeps_last_good_and_stale_is_not_tradable` | 内容哈希及新开仓阻断断言 | 通过 |
| SRC-06 | 备用源口径与上游身份校验 | `test_src_06_fallback_requires_semantic_match_and_independent_upstream` | 单位/复权/日期/同上游冲突证据 | 通过 |
| SRC-07 | 总时限覆盖无响应调用 | `test_src_07_total_deadline_bounds_nonresponsive_adapter` | 50ms deadline、一次尝试、有界返回 | 通过 |
| SRC-08 | lease 恢复且旧 owner 迟到写被拒 | `test_src_08_expired_lease_recovers_and_stale_owner_cannot_publish` | fencing token 1/2 与最终结果断言 | 通过 |
| A4-01 | 全局 deadline 覆盖所有关键等待 | S03 将新增故障与时间边界测试 | 未开始 | 待 S03 |
| A4-02 | 持仓保护不被候选/归档阻塞 | S03 将新增慢 provider 反例 | 未开始 | 待 S03 |
| A1-01 | 字段覆盖逐层可对账 | S04 将新增覆盖账本测试 | 缺生产冻结样本 | 待 S04 |
| EX-01 | 佣金与印花税分开计费 | S05 将新增小额卖出反例 | 当前实现已复现错误 | 待 S05 |
| EX-02 | 只用下一根完整 1m bar 模拟撮合 | S05 将新增跨时钟因果测试 | 当前 quote bar 语义不一致 | 待 S05 |
| RISK-01 | 冻结数量、资金预占和 T+1 生命周期 | S05-S06 生命周期测试 | 当前仅部分满足 | 待 S05-S06 |
| LLM-01 | 严格布尔、完整集合、超时过期 | S07 schema 反例 | 当前字符串布尔可误判 | 待 S07 |
| EVAL-01 | 分层增益与反事实不混用 | S10 回放/统计测试 | 未开始 | 待 S10 |
