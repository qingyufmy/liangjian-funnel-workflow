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
| EX-01 | 佣金、卖出税和其他费用分账；最低佣金按订单累计 | `test_ex_01_fee_components_minimum_rounding_split_and_order_boundary` | Decimal 分账、单/双订单与部分成交算例 | 通过（离线） |
| EX-02 | 未来 close 不反改冻结委托数量 | `test_ex_02_future_close_does_not_change_frozen_order_quantity` | 两个不同未来 close 的冻结数量一致 | 通过（离线） |
| EX-03 | 审核在分钟中途完成时只使用之后完整窗口 | `test_ex_03_mid_bar_review_uses_only_following_complete_bar` | 10:00:25 审核只能使用 10:02 bar | 通过（离线） |
| EX-04 | 午休、闭市和到期不被补数激活 | `test_ex_04_lunch_and_expiry_are_session_aware_and_never_backfilled` | 11:30→13:01，会话前过期明确阻断 | 通过（离线） |
| EX-05 | 分钟容量不足只部分成交并释放残单预占 | `test_ex_05_capacity_caps_fill_and_records_remaining_quantity` | 500 委托、100 成交、400 过期残单、预占归零 | 通过（离线） |
| EX-06 | 数量/tick 由版本化规则提供者决定 | `test_ex_06_security_rules_come_from_versioned_provider` | 主板/创业板/科创板及不支持证券反例 | 通过（离线） |
| EX-07 | 费用、现金、仓位、权益原子对账；并发不双花 | `test_ex_07_accounting_is_atomic_and_concurrent_orders_do_not_double_spend` | 同账户并发订单最多一个成交且现金非负 | 通过（离线） |
| EX-08 | 旧成交模型历史不被新回放覆盖 | `test_ex_08_legacy_fill_history_is_not_overwritten_by_new_replay` | 独立 replay account/run，旧 fill 保持逐字段相等 | 通过（离线） |
| EX-09 | 合成 quote 不能充当分钟成交/容量证据 | `test_ex_09_synthetic_quote_cannot_supply_fill_or_capacity` | `SYNTHETIC_QUOTE` 明确 `FILL_EVIDENCE_INVALID` | 通过（离线） |
| EX-10 | 实际 workflow→订单→结算同样执行冻结/证据门 | `test_ex_10_workflow_freezes_order_contract_and_rejects_risk_quote`；工作流回归 | 合成 quote 不成交且生命周期仍待真实窗口 | 通过（离线） |
| RISK-01 | 并发委托原子预占现金、总仓位、单票、组合风险和主题暴露 | `test_risk_01_concurrent_reservations_atomically_enforce_cash_and_total_budget` | 同账户并发只有预算内预占成功；超限原因分离；未知主题进入显式桶 | 通过（离线） |
| RISK-02 | 强制退出优先于加仓和减仓冲突 | `test_risk_02_forced_exit_wins_conflict_without_increasing_risk` | 冲突归并结果为强制退出，不增加风险 | 通过（离线） |
| RISK-03 | 当日新仓触发硬止损仍遵守 T+1，下一交易日释放 | `test_risk_03_same_day_hard_stop_stays_pending_until_t1_release` | 当日卖出数量为零并保留风险计划；下一交易日可退出 | 通过（离线） |
| RISK-04 | 旧可卖持仓与当日加仓锁定批次分离且重启不丢失 | `test_risk_04_old_sellable_lot_and_new_locked_add_survive_restart` | lot 账本重开 SQLite 后仍保持 sellable_from 和剩余数量 | 通过（离线） |
| RISK-05 | 重复成交幂等且后续有效退出修订可继续 | `test_risk_05_duplicate_fill_is_idempotent_and_new_exit_revision_can_continue` | 同 fill 不重复扣款，新的退出 revision 不被永久吞掉 | 通过（离线） |
| RISK-06 | 部分成交消费实际预占并释放残量，取消/过期有终态 | `test_risk_06_release_and_partial_consumption_zero_out_reservation` | `CREATED→READY→RESERVED→SUBMITTED→PARTIALLY_FILLED`；过期预占归零 | 通过（离线） |
| RISK-07 | 同 episode 去重，不同减仓 episode 可再次生效 | `test_risk_07_new_reduce_episode_is_distinct_but_same_episode_is_idempotent` | 同分钟重复无效，两分钟后的新 episode 有效 | 通过（离线） |
| RISK-08 | A3 计划到期不关闭已建仓风险计划 | `test_risk_08_plan_expiry_does_not_close_position_risk_plan` | 执行计划过期后风险计划仍为 ACTIVE | 通过（离线） |
| RISK-09 | 跳空退出使用当前可证明窗口；锁板/零容量不得伪造成交 | `test_risk_09_gap_exit_uses_current_window_and_locked_bar_does_not_fake_fill` | gap 使用当前开盘；不可成交窗口保持未成交 | 通过（离线） |
| RISK-10 | 公司行为版本化调整 lot；无法解析时禁止新增风险 | `test_risk_10_corporate_action_is_versioned_and_unresolved_blocks_add` | 总仓与 lot 同比调整；未知公司行为保留持仓并阻断 ADD | 通过（离线） |
| RISK-11 | 成交、现金、持仓、lot、预占同事务；故障后可审计恢复 | `test_risk_11_failed_commit_leaves_no_half_written_fill_or_position` | 注入失败无半写；释放后账本审计一致 | 通过（离线） |
| LLM-01 | 每个合格计划恰有一条结果，布尔值严格且集合完整 | `test_llm_01_requires_exact_candidate_set_and_strict_boolean` | 缺失、重复、未知计划及字符串/空布尔全部阻断 | 通过（离线） |
| LLM-02 | 模型不得修改股票、动作、价格、数量、权限或数据状态 | `test_llm_02_action_price_quantity_symbol_and_unknown_fields_are_forbidden` | schema 拒绝所有越权字段 | 通过（离线） |
| LLM-03 | 审核绑定决策、快照和绝对截止，迟到或错配不得生效 | `test_llm_03_rejects_late_or_wrong_decision_and_snapshot`；模型墙钟测试 | 错误 ID/快照、迟到输出和持续流均 fail closed | 通过（离线） |
| LLM-04 | 模型失败不得阻断确定性持仓保护 | `test_llm_04_model_failure_cannot_block_deterministic_position_exit` | 模型异常时硬止损风险意图仍先落账 | 通过（离线） |
| LLM-05 | 外部文本只作不可信数据，引用必须来自冻结证据目录 | `test_llm_05_untrusted_text_is_data_and_fake_evidence_is_rejected` | 指令文本被隔离，伪造 evidence ref 被拒绝 | 通过（离线） |
| LLM-06 | 无模型/影子实验与正式账户、存储、输出物理隔离 | `test_llm_06_shadow_experiment_requires_separate_store_account_and_output` | 任一复用正式资源均阻断 | 通过（离线） |
| LLM-07 | 审核身份、模型、提示词、tokens/cost 可审计，未知值不伪造 | `test_llm_07_workflow_callback_returns_bound_transport_audit`；`test_llm_07_strict_monitor_persists_reason_reference_and_identity` | 事件账本持久化 reason/ref/identity；不可得计量明确为 null | 通过（离线） |
| UI-01 | A3 日线合格、A4确认、当前资格、有效期和目标证据分开 | Python/TS `UI-01` 反例 | 日线合格只显示 `PENDING_A4`；固定R目标明确不是市场阻力证明 | 通过（离线） |
| UI-02 | A2 主题强度不得冒充个股总分 | Python/TS `UI-02` 反例 | 同主题股票保留各自相对强度/角色；无显式个股总分显示未知 | 通过（离线） |
| UI-03 | 任务、数据、机会、可执行性与持仓未知状态不得折叠 | Python/TS `UI-03` 反例 | 关键缺口阻断、非关键降级、无机会及未知可卖状态分别呈现 | 通过（离线） |
| UI-04 | API、Markdown、前端和通知消费同一持久化投影 | `test_ui_04_attached_projection_is_the_single_persisted_stage_projection`；通知/报告回归 | `research-presentation/1.0.0` 随阶段行落盘；旧产物只经同语义兼容适配 | 通过（离线） |
| UI-05 | 页面刷新只读本地快照，能力健康不以供应商总绿灯替代 | TS `UI-05` 100次读取 | 未调用 `fetch`；逐能力展示成功、新鲜度、覆盖、失败、下次尝试和影响路径 | 通过（离线） |
| UI-06 | A4 展示计划/实际时间、数据截止、范围计数、deadline、模型、信号和成交 | TS `UI-06` 反例 | 从持久化 observability/simulation 投影，未知值不补零 | 通过（离线） |
| SRC-09 | 新源开关不改变正式候选/订单，旁路投影可见 | `test_src_09_sidecar_enablement_never_mutates_formal_candidates_or_orders` | authoritative 深拷贝逐字段一致；shadow 独立且无执行权 | 通过（离线） |
| SRC-10 | KPL题材、目录概念和主营披露身份不互换 | `test_src_10_taxonomy_identities_cannot_be_interchanged` | 三类 identity key 分离；题材/概念无主营占比 | 通过（离线） |
| SRC-11 | 只有HTTP文件时间不能成为当前报价 | `test_src_11_http_file_time_without_trade_date_is_not_tradable` | 无可靠交易日/报价时点为 `TIME_UNVERIFIED` | 通过（离线） |
| SRC-12 | 转载去重且过期观点无交易权 | `test_src_12_reposts_form_one_source_chain_and_expired_opinion_has_no_authority` | 10份转载归1条来源链；保留可检验条件和失效期 | 通过（离线） |
| SRC-13 | 主营提取失败时行业仅作分类回退 | `test_src_13_business_parse_failure_keeps_industry_as_classification_only` | 原文保留、主营段为空、S04覆盖记录 `PARSE_ERROR` | 通过（离线） |
| SRC-14 | 未授权/未实测/口径未验收不得正式 fallback | `test_src_14_unlicensed_or_unverified_source_cannot_become_fallback` | 即使误开 enabled/执行权仍因许可与LIVE状态阻断 | 通过（离线） |
| EVAL-01 | 未来标签/财务修订不改写冻结决策身份 | `test_eval_01_future_revisions_do_not_change_frozen_decision_identity` | 标签可追加，决策哈希不变 | 通过（离线） |
| EVAL-02 | 忠实回放与补数回放严格隔离 | `test_eval_02_observed_and_repaired_runs_are_strictly_separated` | 忠实缺证据阻断；补数报告不可声称历史可交易 | 通过（离线） |
| EVAL-03 | 重复回放幂等，阻断/未成交不算盈利 | `test_eval_03_replay_is_idempotent_and_nonfills_are_not_profitable_trades` | 重复订单按身份去重；仅真实 fill 计净收益 | 通过（离线） |
| EVAL-04 | 费用/成交/策略版本分开报告 | `test_eval_04_versioned_cost_fill_and_strategy_reports_never_overwrite` | 任一版本变化生成不同 report_id | 通过（离线） |
| EVAL-05 | 固定时间切分并检测标签窗口重叠 | `test_eval_05_time_split_is_fixed_and_detects_overlapping_label_windows` | walk-forward + purge + embargo；无随机拆行 | 通过（离线） |
| EVAL-06 | 模型否决的信号与账户效果分开 | `test_eval_06_llm_veto_signal_and_account_effects_are_not_added_together` | 账户只用真实成交，禁止直接叠加反事实收益 | 通过（离线） |
| EVAL-07 | 未来公告、错日期主题、未知复权和成本遗漏 fail closed | 参数化 `test_eval_07_known_future_and_cost_leakage_counterexamples_fail` | 四类污染均返回稳定错误码 | 通过（离线） |
| EVAL-08 | 少样本不足证据，基准与选择样本不污染 | `test_eval_08_small_samples_and_contaminated_benchmarks_are_not_evidence` | 少样本为 `INSUFFICIENT_EVIDENCE`；交集阻断 | 通过（离线） |
| EVAL-09 | A1/A2 全去向集合对账并保留失败样本 | `test_eval_09_a1_a2_reconciliation_keeps_rejected_and_missing_rows` | input=passed∪rejected∪missing 且互斥 | 通过（离线） |
