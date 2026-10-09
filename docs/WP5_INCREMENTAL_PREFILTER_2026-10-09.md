# WP5 任务 1/2：日线增量与公告前预筛切片

## 结论与范围

延续 `codex/wp5-hotfix-close-scope`，起点 `b405560466e61db4af59ef488eacf44485f8ffa2`。本轮只实施 WP5 任务 1/2 的独立切片，没有执行其他工作包、模型、真实通知、生产读写、推送或部署。用户任务书保持原样。

**日线增量代码已修改；公告预筛只进入影子验证，尚未缩小正式公告采集范围。** 这是明确的未完成项，不是已经解决 90 分钟研究超时的声明。

## 已落地的修改

### 日线请求与恢复

- `pipeline/data_sync.py` 复用同一次缓存查询进行闭合水位检查、增量游标选择和发现扫描；游标只来自请求截止之前的日线，不能使用未来日期的缓存。
- 缓存至少 30 根且缺少新闭合日线时，只请求最新本地日线之后（时间戳加 1 毫秒）的原始、不复权日线；缺历史/新股请求完整初始化范围，失败或缺少来源回执时重新验证。不生成停牌或退市的空蜡烛，不把空响应当成功。
- `include_financial=False` 的全市场发现扫描不再读取财务状态表或执行财务轮转计划。
- 明确的 `ADJUSTMENT_FACTOR_CHANGED` / `HISTORICAL_REVISION_CONFIRMED` 重建指令回退为完整历史请求。历史返回缺少已有时间点时，不落盘部分修订，不用“仅重试最新日线”解除阻断；未完成的重建原因持久化，下次任务继续重建。
- 每股保留请求方式、范围、来源返回状态、接收时间和重建原因。全市场发现扫描将请求账本写入独立、内容寻址的 `daily-sync-日期-哈希.json`，不塞入模型长输入。

### 公告前预筛与真实 A2 对账

- 新增 `pipeline/disclosure_scope.py`，复用现有 `stock_trend_structure` 判据，不复制或修改均线阈值。
- 人气榜、已送审的 EARLY_DISCOVERY、板块主方向/备选方向、股票自身趋势结构、涨停/梯队事件是互相独立的保留路径。预筛不读取当日公告、不接收最终 A2 或模型结果、不增加 300 只上限。
- 只有“中期结构明确不符合且没有其他路径”的股票才标记 `DEFERRED_DISCLOSURE_NOT_COLLECTED`；缺日线、板块或事件证据仍保留采集候选。这个状态不是 A2 淘汰、不是空公告成功、没有执行权限。
- `workflow.py` 在 CNINFO 同步之前记录预筛，冻结快照及正式研究输入保留同一哈希绑定对象。当前 `mode=SHADOW`、`changes_query_scope=false`，原查询范围和证据门仍然有效。
- 正式 A2 量化门落盘时，将真实 `review_symbols` 与较早产生的预筛范围对账。出现遗漏记录 `SCOPE_MISS` 及完整名单，不从实际 A2 中删股票；作用是发现预筛缺陷，不是用最终 A2 反向定义公告范围。

## 测试与实际命令

统一使用现有 Python 环境与当前 worktree 的 `src`，测试缓存全部位于 pytest 临时目录，不连接生产库。

```powershell
$env:PYTHONPATH='src'
$py='D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe'
& $py -m pytest tests/test_wp5_disclosure_prefilter.py -o addopts='' -q
# 实现前 exit 1：缺少 disclosure_scope 模块。
& $py -m pytest tests/test_wp5_daily_incremental.py -o addopts='' -q
# 实现前 exit 1：6 failed / 1 passed。
& $py -m pytest tests/test_wp5_daily_incremental.py -o addopts='' -q -k factor_reset_partial
# 反例先后揭示部分重建误放行、下次运行丢失重建原因，均 exit 1；随后修复。
& $py -m pytest tests/test_wp5_daily_incremental.py tests/test_pipeline_data_sync.py tests/test_wp5_disclosure_prefilter.py tests/test_a2_stock_structure.py tests/test_workflow_orchestration_coverage.py tests/test_workflow_fact_projection.py tests/test_research_orchestration_coverage.py -o addopts='' -q --junitxml=artifacts/wp5-20261009/incremental-prefilter-final-tests.xml
# exit 0，77 passed；随后补充的持久化恢复与接线断言由全量入口验证。
& ./scripts/test_all.ps1 -PythonPath $py -OutputDirectory artifacts/wp5-20261009/full-incremental-prefilter-tests
# exit 0：2384 项，2377 passed / 6 skipped / 1 已登记的 strict xfail。
# Python 2283 passed，Node 94 passed，typecheck exit 0。
```

全量回执 `artifacts/wp5-20261009/full-incremental-prefilter-tests/evidence.json`，SHA256 `f5388d2443c09a3af296d86e41cd7263c69c5df5522868a42e1305d66682f0b3`。本次是有开发改动的工作树，`release_qualified=false`，不是最终发布提交的干净 HEAD 证明。原失败、子集及全量回执均保留。

200 只增量/全量逐根对照是**构造数据**：每只股票原有 30 根日线，只增补第 31 根，结果完全相同。这不是 200 只真实股票的生产回放或上游接口性能实测。测试还覆盖未来缓存、短历史初始化、空响应阻断、历史修订版本保留、部分重建失败、原 A2 对账遗漏、事件格式未知、最终 A2 拒绝仍保留公告候选、401 候选不截断。

需求—测试—证据映射：

| 需求 | 测试 | 证据与边界 |
| --- | --- | --- |
| 只请求新闭合日线、不能取未来游标 | `test_incremental_requests_only_after_latest_closed_bar`、`test_future_cached_revision_is_not_incremental_cursor` | 全量 `pytest.xml`；构造缓存 |
| 增量与完整序列一致 | `test_two_hundred_incremental_series_equal_full_fetch` | 全量 `pytest.xml`；200 构造标的，不是真实源回放 |
| 重建不完整严格阻断并可恢复 | `test_confirmed_factor_change_forces_full_rebuild_and_retains_old_versions`、`test_factor_reset_partial_history_is_blocked_not_repaired_by_latest_day` | 全量 `pytest.xml`；已验证显式重建指令，自动因子来源待证据 |
| 新股/空响应不能伪造成功 | `test_short_new_stock_history_requests_bootstrap_not_narrow_delta`、`test_empty_suspended_or_delisted_response_does_not_manufacture_bar` | 全量 `pytest.xml`；不推断停牌、退市原因 |
| 预筛独立于最终 A2、所有现有路径保留 | `test_attention_or_discovery_is_collected_even_if_final_a2_rejects`、`test_primary_and_reserve_board_members_are_preserved`、`test_strong_trend_outside_top5_is_not_lost`、`test_event_anchor_is_retained_even_when_daily_trend_is_negative` | 全量 `pytest.xml`；纯预筛及影子路径，正式公告仍全量 |
| 不截断、不删漏掉的正式候选、冻结接线 | `test_no_300_cap_and_input_order_does_not_change_hash`、`test_a2_gate_persists_real_subset_miss_without_changing_gate`、`test_research_input_projects_phase_one_facts_without_semantic_substitution` | 全量 `pytest.xml`；真实冻结覆盖与夜间入口尚未验收 |

## 未完成与下一步

1. 当前 HiThink 调用契约没有已验证的复权因子变化字段。重建入口具备反例验证，但**自动检测与真实因子来源未接通**，不能宣称复权变化已完整解决。原始不复权价格与前/后复权不能混用。
2. 正常全市场逐股请求仍存在。默认间隔 0.5 秒时，3,975 个请求的间隔下限约 33 分钟，少请求几天并不减少请求次数。需要核实可批量、闭合且同口径的日线来源或合法缓存预热，不能降低源限流或拿实时快照伪造日线。
3. 对完整的 10-08 / 10-09 冻结输入进行预筛覆盖回放，补齐所有正式 A2 路径的反例。10-09 原收盘完整快照缺失，不能用 18:00 A1 维护快照冒充。
4. 实施夜间公告独立补采入口与队列，复用 `disclosure_incremental` 和组织 ID 目录；证明夜间失败不污染次日候选状态，再切换正式收盘公告范围。目前没有启用 22:00 新任务。
5. 最终发布提交需要另行授权、干净 HEAD 的统一验收回执；周末准备使用已核实的真实时间契约，不能伪造周五实时采集。五个交易日/60 分钟自然调度验收尚未开始。

## 四层验收

- CODE：当前切片全量入口退出 0，2377 项通过；完整任务 1/2 尚未通过，公告范围未切换。
- REPLAY：构造数据 200 股逐根一致；真实冻结输入、自动复权变化与真实吞吐仍待证据。
- OPERATIONS：未推送、部署或操作生产；五日自然调度未验收。
- STRATEGY：不适用，选股阈值、策略条件和候选数量预算均未改变。
