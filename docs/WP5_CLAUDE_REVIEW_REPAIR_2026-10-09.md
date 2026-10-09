# WP5 Claude 评审修复与逐项复核

本轮只在 `codex/wp5-hotfix-close-scope` 本地实施。用户的评审原文和任务书保持不变；没有 push、合并、部署、生产访问、模型调用或通知。前次 `1a0858a` 的全量回执不替代本轮回执。

## 1. 真实原因

评审 P0-1 的历史修订漏洞成立：原增量起点为缓存末根之后 1ms，无重叠即没有比较对象。P0-2 的全局保留漏洞也成立，但需要区分显式不可用板块与缺失板块字段：正式 `screen_a2` 在前者禁止趋势/备用/强趋势资格，后者仍支持 FULL_MARKET/LEGACY 兼容路径。情绪候选是独立通道，不能一起删除。

正式收盘流程在 `MARKET_EMOTION_FACTS_NOT_READY` 检查后才生成预筛。事件缺项不能成为全域公告保留理由；也不能用预筛绕过这个全局阻断。`event_scope` 读取的是涨停池和连板梯队，不是跌停池。

新增六个反例在修复前全部失败（pytest 退出 1）：重叠起点、历史修订、缺失重叠、板块不可用全域保留、事件缺项全域保留、夜间采集挂起。证据：`artifacts/wp5-20261009/claude-repair-before.xml`。

## 2. 代码修改与反例映射

| 问题 | 修改 | 对应测试 |
| --- | --- | --- |
| P0-1 历史修订 | 增量回拉最后三根真实缓存 K 线，不按三自然日；比较 OHLCV/成交额哈希，忽略抓取元数据及 int/float 表示差别。记录比较哈希、源接收时间与返回哈希；发现修订先持久化 pending reset，再全拉 | `test_incremental_revalidates_last_three_closed_bars`、`test_detected_revision_forces_complete_rebuild_preserving_old_versions` |
| P0-1 失败恢复 | 缺失/重复重叠或源失败不得被仅补末日的重试修成 READY；重建缺历史继续保持 pending，不覆盖旧版本 | `test_missing_overlap_cannot_be_repaired_by_only_latest_day`、`test_revision_partial_rebuild_remains_blocked_on_next_attempt` |
| P0-2 查询域 | 显式不可用板块关闭趋势保留；HOT100、早发现和已知涨停事件仍保留。仅仍可用路径内的单股结构/成员证据缺口保守保留；事件缺项不全域保留。审计发现板块不可用却授予趋势资格时为 SCOPE_MISS | `test_unavailable_board_real_gate_keeps_emotion_but_not_trend`、`test_board_contract_conflict_is_not_covered_even_if_hot_anchor_retained` |
| P1-3 截止 | 使用已有共享 `BoundedWorkGate`；复用检查、目录预热和采集均在同一绝对截止内，单项最长 60 秒且不得超过原总预算。迟到结果不写成功回执，挂起线程占槽使后续尝试 BACKPRESSURE | `test_hung_night_collector_returns_at_deadline_and_never_accepts_late_success`、`test_catalog_warmup_is_lazy_and_inside_the_maintenance_deadline` |
| P1-4 夜间缓存 | 只预热 450 日主营报告/补充查询及 PDF。近期风险公告明确 RECENT_NOT_REQUESTED，不冒充次日完整性。按 expected_pdf_count 对账；确实无 PDF 的完整查询可缓存，但 business_available 仍为 False | `test_no_business_filings_is_complete_cache_work_not_fabricated_business_evidence`、`test_business_cache_survives_six_hour_recent_ttl_without_certifying_next_day_news` |
| P1-5 构造 | 改为显式 `DisclosureCacheContext`，不再构造半初始化 WorkflowApplication。保留原正式辅助方法的调用合同，只 fake 网络/PDF 客户端做集成回归 | `test_cache_only_worker_reuses_formal_queries_and_business_pdf` |
| P2-8 投影 | 原代码本来已用 `rows[:compact_daily_bars]` 截断输出；没有改输出政策，仅补证明 | `test_compact_output_is_not_expanded_by_thirty_bar_readiness_window` |
| P2-10 口径 | 预筛回执明确 event_channel_definition 为 LIMIT_UP_POOL / LIMIT_UP_LADDER；独立记录源不可用/成员未证实/单股结构缺项 | `test_explicit_unavailable_board_does_not_retain_entire_scope` |

源码涉及 data_sync、disclosure_scope、disclosure_maintenance、disclosure_cache_worker、workflow 公告助手和夜间 CLI。正式助手 `include_recent` 默认仍为 True，正式近期公告硬门不变。预筛仍 SHADOW，正式公告范围没有切换。

硬截止不是操作系统杀线程：忽略取消的依赖可继续在受限后台线程维护缓存，但不能晚到后把失败报告改成成功。跨进程 CLI 退出会结束 daemon；本轮没有在生产验证进程恢复或缓存网络可靠性。

## 3. 测试与回放

执行 Python：`D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe`，工作目录为本热修 worktree，`PYTHONPATH=src`，统一使用 `-o addopts='' -q --tb=short`。

- 修复前六项反例：退出 1，6 failed。
- 第一轮 8 文件切片：退出 1，79 passed / 2 failed。两项旧 fixture 将末根移动到新日期、未保留旧 K 线，并用自然日判断稀疏合成历史；按新的真实重叠合同修正 fixture，没有放宽正式校验。
- 日线/真实 A2 定向复核：退出 0，32 passed。
- 14 文件切片 v1：退出 0，151 passed。
- 14 文件切片 v2：退出 0，152 passed。
- 14 文件切片 v3：退出 0，153 passed；包括 compact 投影证明。
- 最终切片 v4：退出 0，156 passed，证据 `artifacts/wp5-20261009/claude-repair-final-v4.xml`。另覆盖“字段存在但 null/空字典/缺 available”三个真实 A2 阻断形态；正式生产调用与离线审计显式传字段存在性，保留真正缺字段的兼容路径。
- 最终 clean-HEAD 全量入口：`scripts/test_all.ps1 -PythonPath D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -OutputDirectory artifacts/wp5-20261009/full-claude-repair-tests`。真实 HEAD、退出码、Python/Node/typecheck、文件哈希与豁免记录以该目录 `evidence.json` 为准；未执行或失败不得沿用前次全量计数。

200 只增量/全拉等价仍是合成样本测试，不是 200 只真实股票。没有重抓行情、重造 10-09 快照或覆盖旧冻结文件。本轮未完成 10-08/10-09 原始批次回放。

## 4. 四层验收

- CODE：本地修订检测、查询域和夜间截止修复待最终 HEAD 全量回执确认；**自动复权源部分仍待证据，G1 未整体通过，不能发布**。
- REPLAY：合成反例与真实函数路径通过不代表原始交易日回放；真实 200 股、除权股及 10-08 冻结输入待证据。
- OPERATIONS：未授权、未部署、未切开关，未证明自然运行时延或 A4 稳定。
- STRATEGY：不适用，没有修改策略阈值、增加候选上限或放宽公告硬门。

## 5. 未完成边界与评审校准

1. **复权源仍是 P0 未闭环项。** `probes/hithink.py` 有 adjustment-factors 路径规格，但静态规格不证明端点实际可用或已获授权。现有 validator 检验 ex_date_ms/dividend_per_share/per_share_bonus，证明的是公司行动字段，尚不是复权因子及基准口径。`data_source.py` 无生产因子客户端，也没有本轮合法真实返回。不得照猜参数接入。更重要：当前日线 `adjust=none` 是 raw，全量重拉 raw 本身不会产生前复权连续序列。下一步需要限定源能力验证与真实返回/口径，再实现因子变化比较、缓存基准版本与 A3 入模价格口径；不能宣称重叠修订修复已解决除权。
2. P0-2 评审示例公式还包含 structure_confirmed/structure_unavailable，与其“板块不可用不授予趋势资格”文字不一致。本轮以实际 screen_a2 为准，已经用非空情绪/趋势集成测试证明；缺失板块字段的兼容路径没有误删。
3. G2 流水线化与环境范围开关尚未实施，不能把已有影子预筛描述为节省了生产请求。必须先固定独立市场事实、批次封存和并发失败传播，不用最终 A2 反向定义查询域。
4. 日线 ≤40 分钟/总≤60 分钟可作为下一步建议；没有自行修改用户任务书。3975×0.5 秒请求间隔的理论下限仍约33分钟。未授权批量参数、改变限流或增加总预算。
5. 评审同时提出连续3天覆盖后切换和周一单日覆盖后切换，不能同时成立。切换仍需用户确认。建议120分钟周末预算与原任务书禁止增加预算冲突，本轮没有采用。
6. P2-7 receipt 幂等、P2-9 短历史 bootstrap 优化未实施；WP1 数据集导出仍需独立分支与原始 A4 冻结路径，不在本轮改写 WP1 或伪造样本。

## 6. 下一步

取得限定只读因子能力探测授权或带口径的有效返回，闭环 P0-1 剩余部分；同时继续可独立验证的 G2 本地流水线测试。发布、周末研究、真实数据导出及22:00接线仍分别授权。
