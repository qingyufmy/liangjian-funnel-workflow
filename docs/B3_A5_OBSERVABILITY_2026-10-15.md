# B3-2 / B3-6：A5 冻结条件与运行预算

## 16:43 全日补齐（旧自然报告不覆盖）

全日240个自然轮次/240份FINALIZED回执，无缺回执、非成功轮或Node重启。`artifacts/b3-a5-20261015/FULL_DAY_OBSERVABILITY_164334053647.json`，SHA `600803db87ee1da62c38922956fbf317a51167db1b9968d7c25b2ef0aa3ace31`。

五段p50/p95/max（ms）：startup 9785.20/15353.52/38689.91；fetch 714.74/3906.80/8703.37；judge 3638.96/8898.27/14780.30；persist 7.03/13.43/20.78；notify 15.19/26.40/43.80。>30s为9轮，13:01父56.371s、13:30父50.377s触发MONITOR_MINUTE_OVER_45S，不能用上午无预警概括全日。对应原50秒栈均位于历史`list_monitor_events`查询，不是网络fetch，尚不能据此解释全部startup。

全日条件最大值与上午截面相同：603565为4、603209为3，均09:51；结构失效前未评估的两只仍NULL。详见同目录`FULL_DAY_BUDGET_AND_VM_STATUS.md`。本地增强可观测性，不是重跑或改写16:00自然A5。

交付时刻：2026-10-15 12:27；12:57 补充 RR 反例（Asia/Shanghai）；隔离分支 `codex/b3-iteration-20261015`，基线 `4296925`。未提交、未推送、未部署、未访问生产、未调模型。

## 修改与真实调用路径

`build_a5_fact_snapshot` → 新 `review/observability.py` → `execution_observability`；事实 hash 在新字段构造后计算。`_model_fact_projection` 保留逐计划统计/计数/异常，只将 >30s 逐轮详情转为可穿透的数量、hash、archive_locator；`_markdown` 增加内部 A5 条件与预算段。新增证据 ID 已进入原严格引用目录，不放宽校验。未修改 workflow、策略、客户模板/通道或通知调用。

- 同一冻结分钟中仅 `kind=CONDITION`、`met is True`、`available is True` 的入场叶门计数。路由常量、VETO、V2 派生 AND 不重复计数；结构失效的“跌破已确认”不是正向入场条件。不同分钟不做 OR；同分取最早分钟。
- 每条激活计划有 `max_conditions_met/at/failing_conditions_at_max`，同时保留 available/total/unevaluated 名单、原 event ID、确认矩阵 hash。未进入入场评估的计划为 `NULL/DATA_LIMITED`，不是“零门失败”也不是“全部满足”。未激活计划不冒充已测试。
- 当前 `factor_ready_symbols=[]` 的生产设计显式写为 `DISABLED_BY_DESIGN_NOT_A3_COUNT`，不用于 A3 数量/技术资格。簇取冻结权威字段，不凭股票名称猜行业；新 `cluster_label` 优先，其后 industry/theme，UNKNOWN 单列。
- 五段 nearest-rank p50/p95/max 来源当日 FINALIZED trace receipts。fetch p95>25000ms 或 Python/Node 任一轮 wall duration>45000ms 记 A5 内部预算预警；不发额外客户提醒。父/子计时分别保留，阶段有嵌套不当作 CPU 时间加总。缺失/非法回执显式 DATA_LIMITED；Node 日志读取失败不使 A5 崩溃。

## 反例与测试回执

先运行未实现测试：exit **1**，`red.xml` 为导入缺模块；首轮实现中错误的投影断言导致 exit1，也原样保留 `green-initial.xml`。最终命令：

```powershell
& 'D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe' -m pytest tests/test_b3_a5_observability.py tests/test_a5_daily_review.py tests/test_a5_fact_guard.py tests/test_a5_context_scope.py tests/test_a5_plan_replay.py tests/test_a5_output_correction.py tests/test_a5_field_defects.py -q --junitxml=artifacts/b3-a5-20261015/green-rr-final.xml
```

退出 **0**；**97 passed / 4 skipped / 0 failed**（101 项，4 项原生产夹具缺失）。其中本次专属 14 项反例覆盖：不可用/预热、首因不等于全部条件、跨分钟 OR、已入场后不再算入场、未来/超有效期、路由/派生 AND/VETO、失效确认不可当买入进度、父/子预算差异、损坏回执/Node 无权限、真实 A5 构造入口/严格 ID/不可变投影、56 计划投影低于原 180k 目标、7叶门全过但RR失败仍明确列为失败。

最终回执 SHA256：`16aa27c61a918fd2f19960da4b842a14e22e7a3c9f226a36cc32f475e5227518`。

补充真实反例：10-13 601877 10:16 的7个技术入场叶门均满足，但 `A4_LIVE_ENTRY_GEOMETRY_ACCEPTED` 为False，原 reason 为 `A4_LIVE_REWARD_RISK_BELOW_MINIMUM`，另有 `ENTRY_BLOCKED_CURRENT_MINUTE`。计分不纳入这类正向政策常量，但失败清单及 reason 映射必须保留，不能把 max=7 读成“完整买入资格通过”。

## 真实只读截面（不是上线结果）

已有隔离 `artifacts/replay-20261015/source-1215/frozen.sqlite3` 用 `mode=ro&immutable=1` 查询截至10:28：5计划、160事件（159策略+1空池）。仅本地计算，无重新采集、无修改原件。

| 股票 | 最多满足入场叶门 | 最早时刻 | 当时未满足 | 未评估 |
|---|---:|---|---|---|
| 603565 中谷物流 | 4 | 09:51 | 15m压力缓解、前5m反转 | 入场带 |
| 603209 兴通股份 | 3 | 09:51 | 15m压力缓解、后5m确认、VWAP | 入场带 |
| 600026 中远海能 | NULL | — | 入场条件未评估即结构失效 | 7叶门 |
| 601866 中远海发 | NULL | — | 入场条件未评估即结构失效 | 7叶门 |
| 601872 招商轮船 | 不计激活样本 | — | 晨审未激活 | — |

保留的权威主题标签 `RESOURCES_ENERGY` 五条，同簇1（其字段不是人工“航运”分类）。`FROZEN_AM_OBSERVABILITY_1028_V4.json` 是当前结果，SHA `31eff9683fbe22c8ac67453d9eaa6c8bb3d41c6865e464d558183047c2358e21`。最初 v1 中把失效确认 met 当入场的错误输出保留为反例，**不能使用**；v2补入场叶门，v3补父duration，v4补所有非计分失败门与reason映射。原 SQLite/事件不变。

上午58轮五段 p50/p95/max（ms）：startup **9692.55/18532.16/36267.57**；fetch **774.80/3975.72/4316.72**；judge **2404.61/5322.83/7954.35**；persist **8.21/13.94/20.22**；notify **10.09/27.45/31.97**。预算预警为空。>30s两轮分别09:31和10:11，详见 `artifacts/b3-a5-20261015/SLOW_MINUTES_20261015_AM.md`：主要在startup，不是fetch；此分段不能进一步证明是哪条初始化函数慢。

## 四层验收与后续

CODE：相关反例及旧 A5 投影/引用/预算/一次严格纠正回归通过；父代理仍需整包全量。

REPLAY：仅上述冻结上午截面；不是六日六变体回放，不代替 B3-1，也不证明收益。

OPERATIONS：未发布；今晚发布前今日16:00自然 A5 仍为旧版，不能称新字段已自然生效。待整包 VM 隔离反例、实际部署、下一自然 A5/冻结事实独立试运行。

STRATEGY：不改策略，未证明门序/阈值有效。尤其 `_evaluate_trend` 先计算后续5m序列再 AND，并非“15m首因挡住说明后四门没有计算”。
