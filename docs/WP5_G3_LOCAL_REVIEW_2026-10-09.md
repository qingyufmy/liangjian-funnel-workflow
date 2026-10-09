# WP5 G3 本地复审修复

基线：`5855e29a1db1604b6738fe7bc20329d3a65ae946`；隔离分支 `codex/wp5-hotfix-close-scope`。未推送、未部署、未切配置。Tony 的 23:30 授权已扩大只读取证范围，生产发布仍需单独批准。

## G3-1：A3 当前技术价格口径（源码事实）

结论：正式 A3 阶段从 `LocalFactCache` 的 **不复权日线**计算日、周、月 MA / MACD / KDJ 与计划价格。没有公司行动因子计算、版本检测或 raw→前复权转换链。现有三根重叠修订检测解决原始行情修订，不等于解决除权价格连续性。

实际路径：`pipeline/data_sync.py` 请求/持久化 `adjust="none"` → `workflow.py` A3 stage 的 `query_daily_bars(adjust="none", as_of=current)` → `FactorEngine.compute(daily_bars=...)` → `factors._daily_bars` 直接解析 OHLC → 聚合周/月并计算指标 → `technical_aggregates.build_technical_aggregates` → `FACTOR_SNAPSHOT / PRICE_LEVELS` → `deterministic.screen_a3` 与模型否决审核。`feature_store.py` 负责落盘血缘，不转换价格。

`factors.py` 的 `_calculation_basis` 和 `technical_aggregates.py` 的 `_actual_price_provenance` **识别已有来源元数据**，不计算复权；可识别 qfq 不代表正式入口使用 qfq。A3 stage 从查询结果仅提取 `payload`，旧缓存 payload 未带来源 adjust 时，摘要可能为 `UNKNOWN`；查询键仍明确为 none。不能把 UNKNOWN 改成 qfq 或据标签推断正确。

除权连续性仍是 P0-A 待闭环：先取得并核验真实公司行动数据，再确定送转/分红/配股公式、因子版本与生效日契约，分别绑定指标序列和真实价格。不得把复权历史价格直接当实际撮合或持仓止损价格。

补正 Claude 推断：`probes/hithink.py` 的 `ex_date_ms/dividend_per_share/per_share_bonus` 是静态验证字段；静态 validator / MockTransport 不能证明真实端点存在。源探测已获只读授权，能力必须另行验证。

## G3-5：P1-A 与真实 A2 对照

反例 `test_stale_available_board_matches_real_a2_without_retaining_full_domain`：真实 `screen_a2` 当前信任 `available=True`，并未按 trade_date 拦截。三只合成标的中主方向、独立强趋势送审，下跌无通道标的不送审。旧预筛额外保留第三只导致全域采集。

修复仅使预筛的可观察成员条件与实际 gate 一致，有效日期不一致保留 `BOARD_DATE_MISMATCH` 诊断，不修改 A2、不新增过期执行授权、不将 stale 改成 fresh。缺日期/非法日期、无效映射/个股缺项仍保守保留；明确 unavailable 仍阻断趋势通道；人气/发现/涨停事件通道不受影响。此修复解决范围收益退化，不替代 WP4 新鲜度权限合同。

## G3-6：P2-A/B/C

| 项 | 修改 | 反例 / 证据 |
| --- | --- | --- |
| P2-A | 良性过取写入 receipt 的 `PREFETCH_OUTSIDE_FINAL_DOMAIN`（名单+原输入哈希）；最终域之外不得 `submit/results`。保留域日线哈希变化仍阻断；过取若也提供新哈希，变化同样阻断。 | `test_overfetch_is_audited_but_cannot_enter_final_results`；旧代码退出 1 |
| P2-B | Node 向 close 子进程传本次绝对截止，不继承旧值；候选流水线取剩余 Python/父进程预算的较小值，扣除已运行时间，内部预留最多 300 秒（短预算 10%）作收尾；不增加外层预算。receipt 记录预算依据。 | `test_collection_deadline_reserves_cleanup_and_preserves_elapsed_budget`、真实 wrapper receipt 测试、Node 两项合同测试 |
| P2-C | Thread 构造/启动失败回收尚未移交的槽与 active；已启动 worker 自行释放，最后关闭资源；实例绑定自己的 gate。 | `test_thread_start_failure_releases_only_unstarted_slot[1/2]`；首工人及部分启动失败旧代码均退出 1 |

内部预留只约束本流水线；若串行其他源完全不返回，仍由已有外层进程截止兜底，不能声称所有任务已有协作式取消。SHADOW 默认路径未改。

## 命令与真实结果

统一 Python：`D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe`，`PYTHONPATH=src`，`-o addopts=''`。

1. `pytest tests/test_wp5_g3_regressions.py tests/test_wp5_disclosure_formal_routes.py -q --junitxml=artifacts/wp5-20261009/g3-before.xml`：退出 **1**，5 failed / 7 passed。
2. 加 pipeline 切片：退出 **0**，24 passed（`g3-local-v1.xml`）。
3. 加 workflow integration / auction：退出 **0**，49 passed（`g3-integration-v1.xml`）；随后补 parent receipt 反例，Windows 默认测试临时目录过长导致 receipt 写失败，退出 **1**，1 failed / 49 passed（`g3-integration-v2.xml`）。只缩短测试临时 checkpoint 路径，不改生产 I/O。
4. 最终 `pytest tests/test_wp5_g3_regressions.py tests/test_wp5_disclosure_formal_routes.py tests/test_wp5_disclosure_pipeline.py tests/test_wp5_pipeline_workflow_integration.py tests/test_auction_refresh.py -q --junitxml=artifacts/wp5-20261009/g3-integration-v3.xml`：退出 **0**，50 passed。
5. `npm test -- --run test/server/wp5-deadline.test.ts`：退出 **0**，2 passed；`npm run typecheck`：退出 **0**；`git diff --check`：退出 **0**。
6. `scripts/test_all.ps1 ... -OutputDirectory artifacts/wp5-20261009/full-g3-local-tests` 在 ba2242e：退出 **1**，总 2462 / 2454 passed / 1 failed / 6 skipped / 1 xfailed。旧反例检出缺日期被当成已知成员缺席；修复恢复缺日期/非法日期的不确定保留，已有有效旧日期的 P1-A 反例仍按实际 gate 校准。失败回执 SHA256 `5d4c8e15cd05333b04c5578c6b26c78d4bf418d255f58f70ced3487c4f33b19c` 原样保留。
7. 修复后源码提交，再运行 `scripts/test_all.ps1 -PythonPath D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -OutputDirectory artifacts/wp5-20261009/full-g3-local-v2-tests`；最终 HEAD/树、退出码、完整统计与日志哈希以该目录不可变 `evidence.json` 为准，结果不预填。

## 四层验收与下一步

CODE：本地反例/切片通过，最终全量以新 HEAD 回执为准。REPLAY：Oct8/9 原始输入和 200 股真实等价尚待取证，合成测试不能代替。OPERATIONS：未部署，连续自然调度未验收。STRATEGY：本包不改策略，无有效性结论。

下一步：只读复制并校验 Oct8/9 原始快照/对应研究血缘；核实真实公司行动能力，继续 P2-7/P2-9 与竞价拆分，桥接提交本轮评审，不等待复审停工。
