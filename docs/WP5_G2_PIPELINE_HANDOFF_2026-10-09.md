# WP5 G2 收盘流水线与 Claude 复审交付

本轮范围：承接 `ee5f70b` 的 G1 本地修复，完成已授权 G2 本地实现和测试。工作目录为 `D:/dev_A股/liangjian_wp5_hotfix_20261009`，分支 `codex/wp5-hotfix-close-scope`。用户任务书、Claude 原评审和原始证据未改；没有生产访问、push、部署、模型调用或真实通知。

## 1. 真实原因

串行限流没有因为增量少返回几行而减少请求数。3975×0.5秒的理论间隔下限约33分钟，不能以“15分钟内全市场同步”作为已实现事实。旧调用链先完成全市场日线，再定位全部 G0 公告；两个独立供应商的等待时间相加。

另一个实际约束是 EARLY_DISCOVERY 的送审排序依赖完整市场扫描。不能每批选若干早发现标的替代全局排序。新流水线仅提前对确定属于 `A1 ∪ HOT100` 且已在 G0 的股票执行独立预筛；早发现的新成员在原全局排序完成后补入。

来源证据沿用 `WP5_CLOSE_SCOPE_BASELINE_2026-10-09.md` 和 `WP5_CLAUDE_REVIEW_REPAIR_2026-10-09.md`；没有用后来的 A1 代际或缓存重造 10-09 原始范围。

## 2. 代码修改与反例映射

| 实现 | 实际调用路径 | 测试 |
| --- | --- | --- |
| 默认 SHADOW 开关 | Settings.from_env → prepare_snapshot → 原 `_prepare_snapshot` 串行路径 | `test_scope_mode_defaults_to_shadow_and_invalid_value_fails`、`test_shadow_wrapper_and_original_path_freeze_identical_bytes` |
| 日线批次通知 | HithinkIncrementalSynchronizer.sync → daily_batch_callback；仅给观察者，不改缓存/投影/重试 | `test_daily_batch_callback_receives_latest_closed_inputs_before_next_batch`、`test_daily_callback_is_optional_and_does_not_change_sync_projection` |
| 行业批次顺序 | 既有完整行业选择 → industry_batch_order → 同一全市场日线同步；不增删股票，不改最后的选股排序 | `test_industry_order_groups_existing_members_without_dropping_unmapped` |
| 候选公告并行 | 必需市场事件核验 → 初始范围封存 → 每批独立预筛及封存 → 共享原官方路由/缓存/限流客户端查询 | `test_candidate_pipeline_runs_before_global_daily_finishes_and_catches_discovery`、`test_global_event_failure_prevents_all_company_queries` |
| 去重、最终对账 | 原全局早发现排序 → 最终预筛 → 校验预取集合及逐股日线输入哈希 → 补剩余候选 → 按最终顺序合并 | `test_same_symbol_is_not_queried_twice_and_input_revision_blocks`、`test_final_discovery_catches_up_without_losing_or_capping_candidates` |
| 失败隔离与截止 | 复用 BoundedWorkGate 的进程内16槽上限；每次使用现有 cninfo_workers；同一原收盘预算绝对截止 | `test_deadline_does_not_accept_late_results_or_wait_on_shutdown`、`test_source_exception_is_not_an_empty_success_and_resources_close_after_last_worker`、`test_wrong_security_result_is_blocked` |
| A2 查询域回查 | ResearchPipeline._persist_gate 在模型审核前写范围对账；仅 CANDIDATE_DOMAIN 且非 COVERED 时阻断 | `test_candidate_scope_miss_blocks_a2_before_review_but_keeps_evidence` |

候选模式只用于从活动 A1 发起的收盘完整研究。月度/周度全量 A1、竞价路径、历史恢复和早盘不隐式使用该优化。延期成员明确记录 `DEFERRED_DISCLOSURE_NOT_COLLECTED`，不伪造“空公告成功”。候选成员的近期公告、主营报告、PDF与正式 A2 证据门沿用原规则。

流水线无超时结果成功回填。不能强杀 Python 线程：忽略取消的依赖继续占全局槽，最后一名工人才关闭其共享 HTTP 资源；主调用不等待无响应依赖退出。槽不足明确 BACKPRESSURE，不生成无限工人。原 Node 外层预算没有提高。

## 3. 测试与回放

Python：`D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe`，`PYTHONPATH=src`，工作目录为本 worktree。

- 首轮反例：`python -m pytest tests/test_wp5_disclosure_pipeline.py -o addopts='' -q --junitxml=artifacts/wp5-20261009/pipeline-before.xml`，退出1，缺流水线模块。
- 中间7项单测：初次实现暴露导入错误、Windows monotonic 的低时间分辨率和回执发布竞态。已改正确包路径，绝对截止仍用 monotonic，计时用 perf_counter，并在 Future 成功前记录窗口。没有用 sleep 把并发测绿。
- 集成夹具修正：保留真实官方路由，修正金融 transport 方法名，固定路由审计时钟，使用短测试路径避免 Windows MAX_PATH。没有 mock app 私有公告助手、同步器或流水线；替代的是本地源 transport 和无关的采集后存储。既有冻结事实的原函数路径与默认 wrapper 字节哈希相同。
- 最终14文件相关切片：退出0，**146通过**。完整命令见迭代状态；JUnit：`artifacts/wp5-20261009/pipeline-final-tests.xml`。切片计数不能再与全量相加。
- 首轮全量 fa632ed：退出1，2350 Python通过、94 Node通过、6跳过、1严格预期失败、1失败。失败是旧竞价诊断接收者绑定公开入口时，wrapper 在拒绝 auction_refresh 前调用内部方法，破坏原 fail-fast 合同。已将相同拒绝检查恢复到公开入口，未修改竞价权限或测试夹具；保留 `full-g2-pipeline-tests/evidence.json` 失败回执。
- 补查回执I/O故障：反例退出1（1失败/1通过），证明失败报告写盘失败可能覆盖首要运行错误。修复后保留原始失败并记录独立写盘错误；成功采集若无法保存强制回执仍阻断，不把无证据结果放行。`pipeline-receipt-io-before.xml` 保留反例。
- 最终全量入口为 `scripts/test_all.ps1 -PythonPath D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -OutputDirectory artifacts/wp5-20261009/full-g2-pipeline-v2-tests`；真实 HEAD、退出码、总数与日志哈希以此目录 `evidence.json` 和最终复审入口为准，不能沿用 ee5f70b 或首轮 fa632ed 的计数。

阶段回执：`research_checkpoints/scope_receipts/disclosure-pipeline-日期-哈希.json`，记录实际阶段秒数、每股查询窗口、日线/公告重叠区间的并集秒数、已提交与已完成集合、初始范围回执和最终预筛哈希。初始范围是 `PROVISIONAL_A1_HOT_NO_DISCOVERY`，不得误称完整收盘范围。每批预筛在查询前封存；原完整 scope receipt 仍由原全局集合生成。时间指标单独落盘，不改变 SHADOW 冻结事实。

这些并行/哈希测试是 CODE 证据，不是实际3975股速度或原始交易日 REPLAY。没有源 API 探测或补造当日快照。

## 4. 四层验收

- CODE：G2 相关测试通过，待新 clean-HEAD 全量回执；G1 的历史修订/通道/夜间修复见上轮回执。**自动复权源与价格基准仍为未闭环 P0，不得宣称整个热修已可发布。**
- REPLAY：真实200股增量等价、至少一只除权股、10-08/10-09完整原始输入仍待证据；早前9-23/9-28投影对账仍 DATA_LIMITED。
- OPERATIONS：未授权、未部署、未切候选模式、未启用22:00任务。没有自然调度连续5日及60分钟验收。
- STRATEGY：不适用，未调整策略、候选数量上限、公告硬门或供应商限流。

## 5. 未完成边界与配置建议

`CONFIG_CHANGE_PROPOSAL`：新增 `LIANGJIAN_DISCLOSURE_SCOPE_MODE`，当前默认/本轮验证基线为 `SHADOW`；建议未来在限定真实源证明、原始回放和自然影子覆盖后单独批准 `CANDIDATE_DOMAIN`。影响仅活动 A1 收盘公告工作域和采集并行；回滚为 `SHADOW`。本轮没有改任何服务器 env。

自动复权不能靠回拉 raw 价格解决：现有 adjustment-factors 静态规格并非实际因子能力证明。需要有授权范围的真实返回、因子基准、公司行动生效时间及调整后的技术序列合同。没有猜端点、参数或自造因子。P2-7 原完整 scope receipt 的 recorded_at 幂等和 P2-9 短历史 bootstrap 优化仍明确延期；不把可选优化伪装成核心闭环。

G3 的只读导出/probe、决策8发布、决策9 run-next-session-prep、G6开关切换及G7夜间调度均未自动执行。建议的日线≤40分钟/总≤60分钟只是评审建议，原用户任务书没有被改写；周末120分钟预算建议未采用。

## 6. 下一步

交 Claude 审查本轮新补丁及 HEAD 绑定全量回执，重点复核双时钟计时、候选域的实际调用路径与提前采集/最终对账；再给出限定 G3 证据获取任务和准确的发布前提，不能把未证明的 P0 忽略后批准生产。
