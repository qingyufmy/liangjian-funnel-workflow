# WP5 任务 2：公告预筛全路径离线覆盖核对

## 结论与范围

本轮沿 `c0f7f91` 的隔离分支继续任务 1/2，没有修改用户任务书，没有推送、部署、查询生产库、调用模型或发送通知。**收盘公告查询范围仍未切换，夜间 22:00 调度未启用。**

重新核对实际调用：`ResearchPipeline → screen_a2 → _persist_gate → 模型审核`。V2 与兼容路径均调用同一个量化入口。A2 阶段补充的是新闻旁路；原始阶段补充输入、原运行配置及原快照未完整取得时，不能将基础投影重算叫作原决策精确复现。

## 已修复的问题

1. 板块日期缺失原先默认当日，成员数组中的 `None` / 缺轮动标识对象原先被当成无命中。这些输入不能证明板块不相关，现保留公告候选，标记 `UNCERTAIN_FACTS_RETAINED`。它不赋予 A2/A3 入选资格。
2. 涨停/梯队源日期缺失原先可以证明“当日没有事件”。现不能证明完整性，保留工作域；已知正向事件仍保留。若上游不能提供日期证明，需要补源契约，不可恢复隐式当日假设。
3. 原覆盖审计仅看排名后的 `review_symbols`。现在检查它与 `local_eligible_for_review` 的并集，包括排名后不再送审的观察票；分列情绪、主板块趋势、备用方向、强趋势观察、兼容通道及 `eligible_routes`。各路径允许重叠，不能把分组数相加当总数。
4. 覆盖对象除哈希外还核对候选/延期互斥完整分区及逐股记录。即使重算哈希，也不能用矛盾分区获得 `COVERED`。
5. 正式 `_persist_gate` 的影子审计记录原批次、lane、阶段快照标识/哈希和时间。漏项只产生审计证据，不删除实际送审股票，不伪造缺失公告。

## 离线入口与证据边界

新增 `scripts/audit_disclosure_batch_offline.py`，仅读取本地 JSON，复用正式 `screen_a2`，不实例化 Settings、WorkflowApplication、RuntimeStore、客户端或调度器。必须匹配同一基础快照的唯一合法 A1 阶段及其输出哈希，检查 A1 无重复、属于原 G0。审计保存逐股路径/状态/原因及完整决策哈希，不重复复制每只公司的大段业务原文；原输入仍完整保留。

预筛只读取 G0、日线、板块、热点、早发现和梯队投影，不读取模型最终 A2 结果。A1 输出只提供正式量化入口的上游，不用于倒推公告工作域。使用宽送审配置是明确的诊断设置，不冒充原运行配置。输出独占创建，拒绝覆盖输入或已有证据，发布前复核输入文件未变化。

即使重算没有漏项，基础投影模式仍退出 `2` / `DATA_LIMITED`，不会允许正式范围切换；`SCOPE_MISS` 退出 `1`。需要原始完整冻结输入、A2 阶段补充、实际配置以及非空路径样本后才能完成原批次覆盖验收。

## 9 月 28 日本地批次核对

匹配基础快照：`snapshot-20260928T160928+0800-e8bbe3734e35`。

- 研究投影文件 SHA256：`29b68cc978f793c6ddc43bd62e9e24ca2a88e7f129b637b517a77673fe82f389`。
- 同批次研究 lane 文件 SHA256：`40d9ac95329a4c062053cec7744ac686145ec7ede21c852ed68533d14658e181`。
- 工作域 910；A1 输出 764；重建预筛候选 332、延期 578，其中 4 只因为资料不确定保留。332 不是 300 上限，也不是应发布计划数。
- 当前代码排名前量化合格 38；最终量化送审 37；预筛漏项 0。不是原模型结果复现，也不是生产研究新批次。
- 路径：情绪 4、主板块趋势 3、强趋势观察 31（包含排名后未送审的 1 只），`MARKET_CORE` 38；备用路径和兼容通道为 0。后两种路径不能凭该样本宣称真实覆盖验收完成。
- 99 热度成员、100 早发现成员、29 事件成员、14 主/备用板块成员、282 趋势结构成员，存在交集；不可直接求和。
- 原始研究文件在运行前后逐字节哈希相同；没有重新采集、覆盖原事实、调用模型或写生产数据库。

此结果仅说明该基础投影下的预筛没有丢掉当前量化候选。不能证明 10-08 / 10-09 原工作域，也不能证明删减 578 只会让自然任务在 60 分钟内到 A3。

9-23 第二个本地批次同样执行离线入口，退出 2 / `DATA_LIMITED`：工作域 826、A1 758、公告候选 437、延期 389、排名前合格 152、最终量化送审 56、漏项 0、不确定保留 0。主板块趋势 26、强趋势观察 126（包含排名后未送审对象），`MARKET_CORE` 152；情绪/备用/兼容路径均为 0。两个批次仍没有非空备用/兼容路径的真实样本。

9-23 输入投影 SHA256 `8a40d23787aceb3b3a7dfc11149562c4caf466ac0979d4b9c04758618988fa0a`、同批 lane SHA256 `b301c16edf976a18f37ff9338f58e4383edacfaf5543ec6bfcceacfab48ebb8f`；输出 `artifacts/wp5-20261009/disclosure-batch-20260923-v1.json`，1,671,015 字节，SHA256 `e57db19cd17aa9b6c1f98dc5ab8838587f5b4c89bda64e88ddba117a82f86c37`。运行命令与 9-28 相同，将输入分别改为 `replay-20260923-snapshot.json`、`replay-20260923-research.json`，输出使用上述唯一文件。

## 需求—测试—证据

| 要求 | 测试 | 证据 |
| --- | --- | --- |
| 日期缺失/成员异常不是确定性无命中 | `test_unproven_board_absence_never_defers_disclosure`、`test_missing_event_date_cannot_prove_no_event_today` | `route-coverage-before.xml`：实现前 6 失败 / 18 通过；修复后相关切片通过 |
| 排名前合格股票不可被审计漏掉 | `test_coverage_checks_local_eligible_before_transport_ranking`、`test_observation_clipped_after_rank_still_has_auditable_route` | `route-coverage-final-v3-tests.xml` |
| 不能靠重算哈希伪造分区 | `test_even_rehashed_invalid_partition_is_not_coverage_evidence` | 同上 |
| 原 A1 配对、哈希与集合约束 | 四种 `test_mismatched_or_modified_a1_is_not_replay_input` | `batch-audit-before.xml`：入口实现前 6 失败；修复后通过 |
| 空池/缺原始输入不能授权切换 | `test_projection_and_empty_gate_never_authorize_query_scope_promotion` | 相关切片、真实本地投影输出 `DATA_LIMITED` |
| 不覆盖输入或原验收文件 | `test_cli_does_not_overwrite_input_or_existing_evidence` | 相关切片与双输入 SHA256 |
| 轻量审计不复制业务原文、不丢完整决策指纹 | `test_audit_receipt_keeps_decision_hash_not_repeated_company_evidence` | 修改前 1 失败 / 6 通过，修改后通过；764 股逐个哈希与精简前一致 |

## 实际执行命令

统一 Python：`D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe`，`PYTHONPATH=src`，隔离 worktree 下执行。

```powershell
& $py -m pytest tests/test_wp5_disclosure_prefilter.py -o addopts='' -q --tb=short --junitxml=artifacts/wp5-20261009/route-coverage-before.xml
# exit 1：6 failed / 18 passed。
& $py -m pytest tests/test_wp5_disclosure_batch_audit.py -o addopts='' -q --tb=short --junitxml=artifacts/wp5-20261009/batch-audit-before.xml
# exit 1：6 failed，离线入口尚不存在。
& $py -m pytest tests/test_wp5_disclosure_prefilter.py tests/test_wp5_disclosure_maintenance.py -o addopts='' -q --tb=short
# exit 0：39 passed，中间切片。
& $py -m pytest tests/test_wp5_disclosure_batch_audit.py tests/test_wp5_disclosure_prefilter.py -o addopts='' -q --tb=short
# exit 0：30 passed，中间切片。
& $py -m pytest tests/test_wp5_disclosure_batch_audit.py tests/test_wp5_disclosure_prefilter.py tests/test_wp5_disclosure_maintenance.py tests/test_wp5_disclosure_cache_worker.py tests/test_wp5_close_scope.py tests/test_wp5_daily_incremental.py tests/test_a2_stock_structure.py tests/test_deterministic_pipeline_v2.py tests/test_research_orchestration_coverage.py tests/test_workflow_orchestration_coverage.py tests/test_workflow_fact_projection.py -o addopts='' -q --tb=short --junitxml=artifacts/wp5-20261009/route-coverage-final-v2-tests.xml
# exit 0：161 passed。此前 final-tests.xml 为 160 项中间切片，不相加。
& $py scripts/audit_disclosure_batch_offline.py --snapshot D:/dev_A股/liangjian_a4_20260923/artifacts/readonly-snapshot-20260928.json --research D:/dev_A股/liangjian_a4_20260923/artifacts/readonly-research-20260928.json --output artifacts/wp5-20261009/disclosure-batch-20260928-v2.json
exit $LASTEXITCODE
# exit 2：DATA_LIMITED，缺原始冻结输入，38 个量化合格对象均被预筛保留。
& ./scripts/test_all.ps1 -PythonPath $py -OutputDirectory artifacts/wp5-20261009/full-route-coverage-tests
# 全量结果见下方追加记录；不是生产运行验收。
```

全量入口退出 0：**2,417 项总计，2,410 通过、6 跳过、1 已登记 strict xfail、0 失败**。证据 `full-route-coverage-tests/evidence.json`，SHA256 `89ac0a97385ebc7ef716ccf7a6ba8f5fd07e06193245250e608d310e34468c06`。这是基于 `c0f7f91` 的开发树验收，`release_qualified=false`，不是干净最终 HEAD 或生产验收。

全量之后仅精简离线 CLI 的审计输出，没有修改正式预筛/策略路径。先运行该文件测试，退出 1（新增精简反例 1 失败 / 6 通过），最小修改后重跑上述 11 文件切片，将输出改为 `route-coverage-final-v3-tests.xml`，退出 0、**162 passed**；不冒称完整基线重新运行了新增的第 7 个 CLI 测试。

用同一双输入重跑 CLI，将输出改为 `disclosure-batch-20260928-v3.json`，退出 2。完整决策集合哈希、764 股逐个决策哈希、预筛、覆盖结果及输入引用与 v2 一致；结构化等价检查退出 0。最终文件 1,746,288 字节，SHA256 `6bab8cf4ca35a526ac2a8f397cf3f5571fe4fa919178b695e3b492377b9b5b4b`。精简前生成的 v1/v2 是本轮衍生中间文件，不是原始证据；等价验证后清理，保留原双输入及 v3。

## 四层验收与下一步

- CODE：正式路径全量 2,410 项通过；随后离线审计精简最终相关 162 项通过。完整 WP5 仍未完成。
- REPLAY：9-23 / 9-28 基础投影各自重算漏项 0，但均仍 `DATA_LIMITED`。10-08 / 10-09 完整输入、备用/兼容路径真实样本待证据。
- OPERATIONS：无发布或生产行为，原范围/调度未变。连续 5 个交易日自然收盘研究耗时未验收。
- STRATEGY：阈值、候选预算、执行资格未变；不声明收益或漏信号改善。

下一条具体操作：继续完善原冻结批次的输入/阶段引用与非空备用路径离线验收；在取得合法原输入之前，不把本轮诊断输出提升为正式查询范围切换依据。自动复权变化识别、3,975 股票吞吐及周末延迟研究仍按既有待证据/待单独授权状态保留。
