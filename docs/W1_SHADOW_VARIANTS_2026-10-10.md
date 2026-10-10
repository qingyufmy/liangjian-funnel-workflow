# W1 影子变体：接口、验收与冻结边界

## 1. 结论与范围

基于 WP1 HEAD `63ccc8ccb880255f731450849429f3e0eba9bcf1` 新增独立引擎、离线入口和测试；未改生产策略、既有 ablation engine、阈值、候选数、调度、通知或数据库。未提交、发布或接生产。W2 负责独立持久账本，root 负责 A3 附加字段与生产消费者接线。

真实六日只读核验：94 个原计划、20,820 原决策窗口，0 个计划满足新几何证据合同；94 个计划的原 `_daily_context` 均无 ATR14、前四日收盘。逐计划字段存在性、原计划 hash、原导出前后 hash、原矩阵回执及报告 hash 都在独立 artifact。`golden_pass=false`，首触发差异为 null（不可比较），不是零差异；周日六日黄金硬门仍未通过。既有 K1 黄金、37 格正式报告均未覆盖或改写。

## 2. 稳定接口与证据

`ShadowVariantEngine.evaluate_minute(items, *, minute, budget_seconds=5.0)` 返回 `signals/minute_summary/errors`。每个 item 提供 `plan_id/plan/bars/baseline/now/decision_time/market_context`：`baseline` 是调用方已经求值的内层正式纯策略原结果，不在影子引擎重复计算。`now` 是策略使用的闭合行情截止时钟；它可以早于调度 `minute`。W2 写入时间须为真实已知决策的 `observed_at`，不得把行情 cutoff 当成交知识时刻。

影子只认可 `strategy_facts.shadow_inputs.schema_version=a4-shadow-inputs/1`，字段为 `daily_ma5/atr14/previous_daily_closes/previous_daily_close_dates/daily_as_of/atr_source_hash`。收盘及日期须各四项，日期严格递增且在目标日前；MA5 与原冻结生产 MA5 一致，原 ATR 若存在也须一致；日线时间须带时区并早于目标日，来源 hash 必须显式。缺失、null、无效、未来日期或冲突全部 DATA_LIMITED，不能倒用旧窄区间宣称研究合格。合同校验不等于来源认证，不证明调整口径或 ATR 算法正确，生产 producer 须另留原来源与算法证据。

返回信号 `a4-shadow-signal/1` 包含变体、计划、分钟、原内层 baseline 动作/真实 reason_codes 首项/状态，变体动作、完整 met/unmet/veto/reason、effective_zone、真实 sequence trace 摘要及 hash、输入 hash、耗时。`actual_execution_authorized=false`。FIRST_TRIGGER 仅可由 status=OK 且 BUY_SIGNAL/ADD_SIGNAL 得到；其他初始状态也可审计，之后只发状态变化。进程内首次声明不是持久权威，跨重启去重由 W2 决定。

分钟 `a4-shadow-minute/2` 另有 `baseline_records`，每计划每分钟一次，即使没有状态变化或 worker busy/reentry。调用方可附加 `actual_outer_baseline/baseline_event_id/baseline_event_sha256`；记录原外层动作及首因、事件原身份、内外层原对象 hash 和字段状态。外层有 reason_codes 时只取其真实首项；只有原 reason 时标 `OUTER_REASON`；不从 unmet 推断。缺外层或事件保持 MISSING/null，不把内层 warmup 原因冒充外层生产原因。`source_scope=CALLER_SUPPLIED_NOT_REEVALUATED`：这是原证据投影，不是重新运行生产的证明。

## 3. 固定变体映射

`SHADOW_VARIANTS_V1` 不可变，版本 `a4-shadow-variants/1`。TREND_MA5 全部六种，MA520_SWING 仅 V1–V3，LEADER_INTRADAY/未知策略不参与。

| ID | 对应原矩阵 scenario |
|---|---|
| V1 | SCAN:MA5_ATR:1 |
| V2 | SCAN:MA5_ATR:1.5 |
| V3 | SCAN:REALTIME_MA5_SHIFT |
| V4 | SCAN:CROSS:MA5_ATR:1:TREND_CONFIRM_WITHIN_N:6 |
| V5 | SCAN:CROSS:MA5_ATR:1:TREND_PULLBACK_LEN_K:2 |
| V6 | SCAN:CROSS:REALTIME_MA5_SHIFT:TREND_CONFIRM_WITHIN_N:6 |

全部复用 `engine._evaluate_with_baseline(..., disabled=(), variant=...)`，只有独立研究计划副本补齐几何输入。K/N 是现有明确研究合同，不是已证等价的 CONTIGUOUS4_LATEST_CONFIRM N4/6/8 扫描；不会为触发跳根或引用未来行情。硬止损、T+1、涨停锁死、市场缺证和实际策略其他门都由同一引擎保留。

## 4. 隔离、预算与写入

独立非阻塞锁和最多一个工作线程，不接 JobRunner/BoundedWorkGate。每分钟预算必须大于零且不超过五秒，剩余等待有界；超时记 SHADOW_BUDGET_EXCEEDED 并弃掉该变体及剩余任务。超时线程没有写入能力，返回结果永不排队发出；线程尚活时后续请求返回 SHADOW_WORKER_BUSY，不扩槽。线程无法被 Python 安全强杀，异常挂起将导致后续影子 busy；这不是无限新建线程或可靠常驻 writer。线程自身只访问计划/bars/context/baseline 深拷贝，不能修改调用方生产对象。预算是单调时钟 admission/等待/结果截止合同，不宣称通用操作系统调度下的硬实时时限。

`emit_shadow_signals` 仅是可选调用方 sink 桥，写失败返回失败回执并可记录日志，不 throw 到生产；它不拥有 DB/JSONL、成交或未来收益。生产消费者仍须捕获入口异常、传真实 outer 证据及真实写入时钟；目前尚未接线。

## 5. 实际命令与反例

在本 worktree，设置 `PYTHONPATH=.../src`，使用 `D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe`。

初始 `pytest tests/test_w1_shadow_variants.py` 因新模块缺失 exit 1；实现后同分钟测试曾因测试把下一分钟仍配旧 decision_time 而 exit 1，纠正 fixture 时钟后通过。新离线入口缺失反例 exit 1；实际回执首次误读 `input_windows` 导致 native exit 2（PowerShell 工具外层 exit 1）；矩阵真实嵌套 `ablation.status` 首触发反例 1 failed/5 passed，exit 1。逐分钟 outer 基线新反例 2 failed/23 passed，exit 1。均保留 XML，不删除红证据。

最终相关命令：

```powershell
$env:PYTHONPATH='D:/dev_A股/liangjian_wp1_20261009/src'
& 'D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe' -m pytest tests/test_w1_shadow_variants.py tests/test_w1_shadow_offline.py tests/test_wp1_ablation_engine.py tests/test_wp1_atomic_scans.py tests/test_w2_shadow_evidence.py -q -o addopts='' --junitxml=artifacts/wp1-20261010/w1-shadow-final-v2.xml
```

122 passed，native exit 0。完整实际 engine fixture 首触发集合与六个对应参数格子一致，V4/V6 有真实 BUY（非 mocked）；同分钟重复没有第二次触发。另有小型离线真实 schema 对账 fixture（其中策略返回值显式 mocked，用于检查 reference 格式，不能混称实际行情黄金）。

```powershell
& 'D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe' scripts/run_shadow_offline.py --bundle artifacts/wp1-20261009/real-export-0908-1008-v2 --matrix artifacts/wp1-20261010/real-ablation-0908-1008-v5 --output artifacts/wp1-20261010/w1-shadow-sixdays-run2
```

真实六日 native exit 2，DATA_LIMITED。按日计划/原窗口：9/10=21/5019、9/11=4/56、9/23=34/7768、9/24=13/3107、9/29=1/239、9/30=21/4631。旧矩阵小报告及回执 hash 已核对；无合格新几何输入，故未读取大 rows、未重跑 37 格或再次执行六日纯策略。报告明确 rows_read=false，不能把存量 rows 期望 hash 宣称为此次已独立验证的大文件 hash。旧 run1 保留原版本绑定；run2 是新增 minute/2 源版本的独立清单。

## 6. 四层验收与剩余边界

1. 源码层：新独立模块/入口，固定参数与复用原引擎已实现；原生产/研究源没有修改。
2. 合同层：122 相关测试通过，含异常/超时/并发/缺证/原对象不变/账本失败/outer-inner 分离；不等于 VM 部署验收。
3. 本地运行层：完整实际 engine fixture 通过；六日原输入前后 hash 核验完成但新几何黄金 DATA_LIMITED，差异 null，硬门未通过。
4. 自然交易证据层：尚无接线、五日影子运行、真实 first-trigger fill 或收益；不得填零、声明二十样本或推荐改生产参数。补充历史原日线读取或修订发布硬门须 root/Claude/Tony 另作明确决定。

交付仅新增 `runtime/shadow_variants.py`、`scripts/run_shadow_offline.py`、两个 W1 测试和本文件。冻结 source/test/document hash 在独立 W1 artifact，不纳入其他代理正在修改的 W2/W3 文件。
