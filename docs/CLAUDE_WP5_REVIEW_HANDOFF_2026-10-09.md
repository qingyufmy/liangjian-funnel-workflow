# WP5 热修提交 Claude 的验收交接

## 1. 真实原因与本次提交范围

本交接供代码评审，不是发布批准。隔离目录为 `D:/dev_A股/liangjian_wp5_hotfix_20261009`，分支 `codex/wp5-hotfix-close-scope`，基线 `7402e7fc570b7a91c5e19405308737715b0ad5c1`。未推送、合并、部署、重启、调用模型、发通知或访问生产数据库。用户未跟踪的任务书及原有未跟踪证据均保留。

10-09 原收盘公告同步到 1750/1825 后未形成完整研究快照，1825 是股票工作域，不是公告篇数。与原收盘 A1 1749 的 76 是净数量差；缓存证明范围 1774，其中 A1 外 111，命中人气 47，其余 64 的早发现本地条件均命中，但缺少原全局排名及原 G0 完整回执。因此不能宣称恢复了原 1825 只完整集合。

范围与来源证据见 `WP5_CLOSE_SCOPE_BASELINE_2026-10-09.md`、`WP5_SCOPE_RECONCILIATION_2026-10-09.md`。本轮继续完成任务 1/2 的可独立验证部分：日线增量、公告工作域封存与影子预筛、独立夜间缓存入口、正式 A2 前后排名的覆盖审计。**正式收盘仍同步原完整范围，22:00 新调度未启用。尚未解决生产收盘耗时阻断，不能将影子基础设施完成写成 WP5 热修全部完成。**

## 2. 已改代码与反例

| 文件/责任 | 修改与必须检查的边界 | 测试入口 |
| --- | --- | --- |
| `pipeline/close_scope.py`、`workflow.py` | 原批次、A1 代际、来源集合及哈希封存；不以最终 A2 结果反推公告范围 | `test_wp5_close_scope.py` |
| `pipeline/data_sync.py` | 本地已闭合日线游标、缺口增量、短历史回退、显式复权/修订重建；不完整重建保留 pending 状态 | `test_wp5_daily_incremental.py` |
| `pipeline/disclosure_scope.py` | 公告无关预筛，情绪/主板块/备用/个股趋势/早发现保留；不确定资料保留工作，不授予资格；无 300 上限 | `test_wp5_disclosure_prefilter.py` |
| `pipeline/disclosure_maintenance.py`、`disclosure_cache_worker.py` | 原范围绑定的 SHADOW 夜间队列、唯一账本、锁与恢复；只用正式缓存采集，不构造 RuntimeStore/模型/飞书 | `test_wp5_disclosure_maintenance.py`、`test_wp5_disclosure_cache_worker.py` |
| `research/common.py` | 正式 A2 量化入口输出只用于影子覆盖核对；核对排名前合格与最终送审并集，不删除漏项 | `test_a2_gate_persists_real_subset_miss_without_changing_gate` |
| `scripts/audit_disclosure_batch_offline.py` | 原 A1 输出匹配/哈希/集合约束、独占写证据、输入前后哈希；投影缺项始终 DATA_LIMITED | `test_wp5_disclosure_batch_audit.py` |
| `scripts/run_disclosure_maintenance.py` | 默认 dry-run 不加载生产环境或创建缓存；显式 execute 才构造缓存维护工人 | `test_cli_dry_run_never_loads_settings_or_workflow` |

此前各轮失败反例、实际命令与退出码已保留在三份实施文档及 `docs/iteration/ITERATION_STATE.json`，不能仅凭最后绿灯删除失败记录。

本次补 `tests/test_wp5_disclosure_formal_routes.py`，直接调用真实 `screen_a2`，明确物化当前个股日线结构，验证非空情绪/主板块/备用/强趋势/兼容路径。第一轮 2 失败/1 通过：其中强趋势失败是新构造 fixture 遗漏独立相对强度输入，并非策略缺陷；补全 fixture 后 1 失败/2 通过，剩余真实缺陷为全市场轮动 fallback 输出 TREND、原审计只有 LEGACY 分类。最小修改仅增加 `FULL_MARKET_FALLBACK` 审计分组，未改量化条件、排序预算或执行权限。最终相关 **165 项通过，退出 0**。

## 3. 测试与回放证据

最后切片命令（隔离目录；`PYTHONPATH=src`）：

```powershell
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -m pytest tests/test_wp5_disclosure_formal_routes.py tests/test_wp5_disclosure_prefilter.py tests/test_wp5_disclosure_batch_audit.py tests/test_wp5_disclosure_maintenance.py tests/test_wp5_disclosure_cache_worker.py tests/test_wp5_close_scope.py tests/test_wp5_daily_incremental.py tests/test_a2_stock_structure.py tests/test_deterministic_pipeline_v2.py tests/test_research_orchestration_coverage.py tests/test_workflow_orchestration_coverage.py tests/test_workflow_fact_projection.py -o addopts='' -q --tb=short --junitxml=artifacts/wp5-20261009/formal-routes-final-tests.xml
# 实际 exit 0，165 passed；切片不能与全量相加。
```

最终提交的统一验收使用既有 `scripts/test_all.ps1`，不重新安装依赖。完整命令、退出码、HEAD/tree、Python/Node/类型检查计数、失败/跳过清单与日志哈希统一存于 `artifacts/wp5-20261009/full-claude-handoff-tests/evidence.json`；交付指纹和验证退出码在同目录上级 `claude-review-manifest.json`。本文件在测试前封存，**最后全量数字以这份不可变回执为准**，不复用旧开发树 2410 通过来冒充最终验收。

真实本地 9-23 / 9-28 投影审计：排名前必需覆盖 152 / 38，漏项 0；候选工作域 437 / 332，均不受 300 上限限制。输入、输出哈希见 `WP5_DISCLOSURE_ROUTE_COVERAGE_2026-10-09.md`。两份审计均退出 2 / DATA_LIMITED：原 raw、阶段补充和当时配置不完整；当前新增路径分组的合成测试不是这两个原批次重新回放，也不是 10-08 / 10-09 的证据。

## 4. 四层验收与评审要求

- CODE：可独立实施切片已完成；最终统一测试结果必须按 manifest/receipt 核验。存在已登记 strict xfail，不把预期失败写成基础问题已修好。
- REPLAY：部分证据；200 股增量等价目前是构造输入，9-23 / 9-28 是基础投影诊断；真实 200 股、自动复权变化源及 10-08 / 10-09 原冻结输入仍待证据。
- OPERATIONS：未发布；正式范围和调度未变。3975 股票 <15 分钟、连续 5 日 15:10→A3 <60 分钟尚未通过。
- STRATEGY：不适用；阈值、策略与候选预算未变，没有收益或更多信号改善声明。

请 Claude 按**实际调用路径和代码**复核，而不是采信本文结论。输出 P0/P1/P2、精确文件/行、反例、修复建议和验收层级。至少检查：

1. 预筛是否覆盖所有当前量化路径，数据不确定时是否错误延期；正式 A2 公告硬门是否仍独立 fail-closed。
2. 增量边界/重复/历史修订/待重建游标是否丢数据；短历史、停牌和错误源状态是否冒充完整。
3. 夜间工人是否真的只维护缓存；6 小时/7 日、PDF 接收时间与内容、跨日恢复、目录身份失败是否严格。
4. 保留集与延期集、原代际与工作域、哈希和账本锁是否能被矛盾输入伪造；证据文件是否可被覆写。
5. 预筛目前是 SHADOW、夜间无调度接线：哪些证据必须先到位才可切换；不得将单元测试等同于发布批准。
6. 请求间隔 0.5 秒、3975 单股请求至少约 33 分钟的问题，不能靠减少每次返回天数宣称 <15 分钟。需提出有接口能力证据的批量或合法夜间预热方案，不降低源限流。

## 5. 未完成边界及相关分支

自动复权因子来源未验证，只有显式重建入口。夜间预算只在股票之间检查，不是网络调用硬总截止。没有当日完整快照时禁止套用 same-day recovery。未启用新浪、移植受限代码、扩采 mootdx、启动真实交易或外部评审调用。

任务书的“周末 `run-research --as-of 周五15:10` 实时采集”不能直接套用；实际入口区分历史重放和实时源时钟。已有 `run-next-session-prep` 使用真实周末源时间、前交易日行情截止和下一交易日目标；需决策 9 单独授权及正式合同验收，不能在周末伪造周五事实。

WP0 已在当前分支基线中，其全量/AST 等价证据见 `WP0_REPOSITORY_BASELINE_2026-10-09.md`；旧副本与环境未迁移或删除。WP1 在独立 `D:/dev_A股/liangjian_wp1_20261009`，提交 `25397335c8bda2bbf30a64dcbc3efd7a8ad7d629`，**未合并到本分支**。本次读取了其实施文档：仅首切片 140 项相关测试，有待连续滑窗、15 日执行输入和完整全量验收；本次未重跑其全量，不可用 WP5 全量替 WP1 背书。

## 6. 下一步

提交本交接、基线到最终代码差异、统一测试回执及未完成清单给 Claude；先评审待发布边界与证据缺口，再由用户逐项授权下一阶段。任务书草案见 `CLAUDE_NEXT_STAGE_BRIEF_DRAFT_2026-10-09.md`，不是自动生效指令。
