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
| A4-01 | 慢/永久阻塞工作有界且背压不泄漏关键 worker | `test_a4_01_hung_auxiliary_is_bounded_and_required_lane_stays_available` | 独立 gate 超时、背压及关键 lane 正常返回 | 通过 |
| A4-02 | 原生 5m 与失效归档不进入 A4 决策 wait-all | `test_a4_02_monitor_never_fetches_native_5m_or_archive_only_symbol` | 仅请求决策标的 1m，辅助范围显式延期 | 通过 |
| A4-03 | 单标的缺口不污染同 lane 健康股票 | `test_symbol_data_block_does_not_stop_healthy_plan`；`test_one_failed_symbol_does_not_block_other_symbols` | 扩大回归通过 | 通过 |
| A4-04 | 持仓保护不等待计划恢复、市场和 LLM | `test_a4_04_position_hard_stop_does_not_need_market_or_llm`；`test_a4_04_position_protection_is_written_before_plan_activation` | 硬止损在恢复屏障前已持久化 | 通过 |
| A4-05 | 模型迟到不得放行新开仓，退出不依赖模型 | `test_a4_05_model_completion_after_absolute_deadline_cannot_publish_buy`；`test_after_cutoff_buy_is_blocked_and_forced_exit_survives` | 迟到模型转 `MONITOR_OVERRUN`，无 BUY | 通过 |
| A4-06 | 绝对截止、迟到结果冻结、晨间恢复不越界写 | `test_a4_06_late_bounded_result_cannot_replace_terminal_timeout`；`test_a4_06_morning_recovery_cannot_mutate_after_round_deadline` | 超时结果不可被迟到值替换，计划仍待审核 | 通过 |
| A4-07 | 交易时段、午休、截止、收盘、节假日和恢复 | `test_expected_clock`；`test_a4_execution_cutoff_excludes_forming_provider_minute_and_respects_lunch`；scheduler 边界测试 | 当前规则表扩大回归通过 | 通过 |
| A4-08 | 缺分钟、零成交、停牌和涨跌停保持不同语义 | `test_tencent_quote_requires_same_day_fresh_positive_auction_volume`；`test_locked_limit_up_cannot_buy`；`test_invalid_1m_and_missing_data_are_data_blocked` | 既有分类/策略反例通过；未用插值授权 | 通过 |
| A4-09 | 重启、重复调度、lease 交接保持幂等 | `test_restart_does_not_recall_model_during_same_trigger_episode`；scheduler lease 测试；simulation 幂等测试 | 扩大回归通过 | 通过 |
| A4-10 | 合法冻结输入在有界包装前后逐事件一致 | `test_a4_10_absolute_deadline_wrapper_preserves_valid_deterministic_output` | action/reason/effective 逐事件相等 | 通过 |
| A4-11 | 过期持仓报价必须 DATA_BLOCK | `test_a4_11_stale_position_quote_is_data_block_not_success` | 不再显示风险清晰/成功 | 通过 |
| A4-12 | 归档 SQLite 写锁不阻塞风险意图短事务 | `test_a4_12_archive_sqlite_writer_cannot_block_risk_intent_store` | 独立 DB 写锁下风险意图 <0.5s | 通过 |
| A4-PERF | 50/100/200 全会话 fixture 与墙钟档位 | `run_iteration_acceptance.py --profile stress --scenario a4-s03` | 200 计划风险 p99 1.130s；决策核心 1.207s；仅本机离线证据 | 通过（非生产） |
| A1-01 | 原始字段解析缺口可定位且修复后进入 packet | `test_a1_01_parse_mapping_gap_then_fix_reaches_real_packet` | SQLite 覆盖行、A1 packet 投影 | 通过（离线） |
| A1-02 | 零、负数、缺失、非有限数、未授权、未发布、不适用不混淆 | `test_a1_02_*` | 参数化值状态和 gap reason | 通过（离线） |
| A1-03 | 1,000 标的、单轮 100 的公平补齐不饿死低优先级 | `test_a1_03_thousand_symbol_backfill_is_fair_under_new_high_priority_work` | 持续新增 HOLDING 时原 1,000 个 UNIVERSE 最终全部完成 | 通过（离线） |
| A1-04 | 重启后只续未完成任务，重复冻结尝试不重复计数 | `test_a1_04_restart_resumes_only_deferred_task_and_keeps_success` | 重开 SQLite、成功任务不再规划、attempt 幂等 | 通过（离线） |
| A1-05 | 晚公告不得进入历史 cutoff | `test_a1_05_late_announcement_cannot_enter_historical_cutoff` | `TIME_UNVERIFIED` 且 PIT=false | 通过（离线） |
| A1-06 | 单期摘要不得宣称多期/严格 PIT | `test_a1_06_one_f10_period_cannot_claim_multi_period_or_strict_pit` | 最低证据契约明确不完整 | 通过（离线） |
| A1-07 | 财务负面与采集未知分离 | `test_a1_07_negative_is_valid_but_missing_is_unknown` | NEGATIVE 为有效事实，缺失为 FIELD_MISSING | 通过（离线） |
| A1-08 | 入队幂等、retry-after/fencing 不可绕过、worker 先落证据再成功 | `test_a1_08_*` | 重启、双 owner、注入 worker | 通过（离线） |
| A1-09 | 压缩预算不能隐藏关键缺口 | `test_a1_09_packet_budget_never_hides_critical_gap_projection` | 保留 200 条关键缺口及投影数量/原因 | 通过（离线） |
| A1-10 | 任意 821 样本逐层集合数量与哈希可对账 | `test_a1_10_arbitrary_821_scope_reconciles_every_layer` | 821→821 raw→800 parsed，21 项有原因 | 通过（离线） |
| A1-11 | 不完整新代次不得覆盖活跃代次，过期活跃代次不得伪装合格 | `test_a1_11_incomplete_coverage_generation_cannot_replace_active`；既有过期测试 | coverage gate 原子拒绝，旧 pointer 保留 | 通过（离线） |
| A1-12 | 失败对象不退出分母；无适用字段显示 N/A | `test_a1_12_failed_fields_remain_in_denominator_and_empty_group_is_na` | 固定 denominator/version，N/A 非 100% | 通过（离线） |
| EX-01 | 佣金与印花税分开计费 | S05 将新增小额卖出反例 | 当前实现已复现错误 | 待 S05 |
| EX-02 | 只用下一根完整 1m bar 模拟撮合 | S05 将新增跨时钟因果测试 | 当前 quote bar 语义不一致 | 待 S05 |
| RISK-01 | 冻结数量、资金预占和 T+1 生命周期 | S05-S06 生命周期测试 | 当前仅部分满足 | 待 S05-S06 |
| LLM-01 | 严格布尔、完整集合、超时过期 | S07 schema 反例 | 当前字符串布尔可误判 | 待 S07 |
| EVAL-01 | 分层增益与反事实不混用 | S10 回放/统计测试 | 未开始 | 待 S10 |
