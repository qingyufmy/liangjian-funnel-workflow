# B3 六变体流式回放实现与午间证据

实际完成核对：2026-10-15 13:01:52（Asia/Shanghai）。仅本机隔离证据，未提交/推送/部署/联网/调用模型/写生产。

## 最小入口与工程验收

新增 `evaluation/b3_variant_replay.py`、`scripts/run_b3_variants.py`、`tests/test_b3_variant_replay.py`，复用父代理维护的 `b3_baseline.iter_frozen_windows` 和 `b3_variants`；没有重写原策略或基础数据读取器。

```powershell
& 'D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe' scripts/run_b3_variants.py --state-db artifacts/replay-20261015/source-1215/frozen.sqlite3 --market-root artifacts/replay-20261015/source-1215/files/a4_live_market --daily-db artifacts/replay-20261015/daily-122330/daily.sqlite3 --auction-evidence artifacts/replay-20261015/auction-evidence/20261015T123126131507.evidence.json --output artifacts/replay-20261015/variants-stream-final-v3 --captured-at 2026-10-15T12:13:21.493314+08:00
```

真实退出 **0**，耗时 **365.328s**。此退出码仅表示 V0 同窗对账无差异，不表示所有变体有完整证据或策略有效。输出 `comparisons.jsonl`（逐原窗口/变体）、`plan_metrics.jsonl`（逐计划指标）、`summary.json`（36行日期×变体矩阵及缺口）。开发中 v1/v2 保留，但仅 `variants-stream-final-v3` 用于本次送审。

红：实现前运行 `tests/test_b3_variant_replay.py`，exit1、缺模块，回执 `variant-runner-red.xml`。

绿（含原 reader、变体契约、A5/RR/引用/投影回归）：

```powershell
& 'D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe' -m pytest tests/test_b3_variant_replay.py tests/test_b3_baseline.py tests/test_b3_research_variants.py tests/test_b3_a5_observability.py tests/test_a5_fact_guard.py tests/test_a5_context_scope.py -q --junitxml=artifacts/replay-20261015/variant-runner-final-rr.xml
```

真实 exit0，**110通过/4旧夹具缺失跳过/0失败**（114项）；回执SHA `fd4a981b8898ac962af95cde086387231a5ff057305dea694b28a6ec31f2a611`。新增 runner 专属19项，包括 PRE_KERNEL_BLOCK不放行、原LLM不复用、非趋势控制、右删失、永久失效、成熟/PIT门、真实流式入口、7叶门全过但RR失败仍保留失败原由。原始参数变体规则另由 `test_b3_research_variants.py` 负责。

## 数据边界与同窗结果

源冻结于 **12:13:21**，不是今日收盘全日。10-09原池空（240条EMPTY_SCOPE），并非验证过任何股票策略；10-15仅截至午间。

V0 **21,165** 个可评估冻结窗口，kernel/action+首因、原monitor层动作+首因 **全部一致，差异0**。另外 **257条 PRE_KERNEL_BLOCK** 原样阻断，不用后来行情补造判断。原计划适用窗口有 **242个股票×分钟运营缺口**（10-08=95、10-12=144、10-14=3），与父代理 V0 coverage 独立对账一致；零差异不覆盖这些缺口。

| 日期 | V0 首次技术候选计划数 | V1 | V2 | V3 | V4 | V5 |
|---|---:|---:|---:|---|---|---|
| 10-08 | 0 | 1 | 0 | DATA_LIMITED | DATA_LIMITED | DATA_LIMITED |
| 10-09 | NO_PLAN | NO_PLAN | NO_PLAN | NO_PLAN | NO_PLAN | NO_PLAN |
| 10-12 | 0 | 0 | 0 | DATA_LIMITED | DATA_LIMITED | DATA_LIMITED |
| 10-13 | 1 | 1 | 1 | DATA_LIMITED | DATA_LIMITED | DATA_LIMITED |
| 10-14 | 0 | 0 | 0 | DATA_LIMITED | DATA_LIMITED | DATA_LIMITED |
| 10-15 午间 | 0 | 0 | 0 | DATA_LIMITED | DATA_LIMITED | DATA_LIMITED |

V1/V2/V3/V5 仅趋势五日线；520/龙头明确 `NOT_APPLICABLE_CONTROL`，不将不变规则的重复运行算作变体覆盖。V4跨三策略。V1/V2实际分别评估17,930个趋势原窗口；V4需原09:25证据的21,165个窗口全部缺证据。已有09:26/09:27 `POST_AUCTION_QUOTES` 不转换成09:25，未另设代理组。

## 候选、价格方向与不成立的收益结论

以下价格比值是独立的 **UNVERIFIED_SYMBOL_REFERENCE_SENSITIVITY**，不是策略收益、模拟成交或胜率证明；`reference_outcomes` 的严格 T+1/T+3 因缺PIT身份均为NULL，T+3未成熟的另标缺交易日证据。

| 原日/股票/变体 | 首次技术候选时刻 | 研究参考价 | T+1原symbol收盘/参考价 | T+3原symbol收盘/参考价 | 当日原观察窗口MAE |
|---|---|---:|---|---|---:|
| 10-08 603087 V1（相对V0新增） | 11:08 | 75.05 | 75.52 / **+0.626%** | 72.00 / **−4.064%** | −2.452% |
| 10-13 301152 V0/V1/V2（同一既有候选） | 13:26 | 23.22 | 21.90 / **−5.685%** | 未成熟/无目标日 | −1.421% |

301152 的 V0 技术候选在生产 LLM/episode 层被处理；V1/V2不借用原模型结论作为新审核通过。所有变体都没有新模型、组合风控批准、订单或成交。MAE只用候选时刻以后、原本已观察到的闭合分钟低点，不包含未观察的跨日持仓路径。

不能据以上样本宣称 V1 满足“T+1胜率不低于基线”；严格收益分母为0。不存在第二个真实新增技术样本以凑“盈利/亏损反例各一”；声明式正负价格算术测试不冒充真实策略样本。

## 可观测性与右删失

- 每计划保存首技术候选、首次永久失效、max_conditions_met/at、failing/unevaluated和reason映射；同一分钟计分，不跨分钟OR。不把路由常量、VETO、派生AND或“跌破已确认”等失效事实计成正向入场叶门。
- 10-13 601877 10:16 的7技术叶门均通过，实际RR不够；`A4_LIVE_ENTRY_GEOMETRY_ACCEPTED` 与 `ENTRY_BLOCKED_CURRENT_MINUTE` 失败仍列入，不能说漏信号。
- V2在原计划失效点仍未失效的，后续原池不再有冻结判断，明确 `RIGHT_CENSORED_ORIGINAL_TERMINAL`；10-08有5条、10-13有15条、10-15午间有额外源截断。不能用原计划停止后“没有记录”证明新策略没有机会。
- 缓存日线逐原payload `content_hash`核验，按原计划as_of和fetched_at筛版本；不回填symbol到原payload、不伪造原hash。ATR使用`shadow_inputs`同义 Wilder TR14 seed-mean，在现有原缓存前缀推导；另保留最后14根TR简单均值作口径比较，**不宣称与原563根完整Wilder seed等价**。

## 可复核文件SHA与四层结论

- comparisons：`27374ca7e74e8f3df5e13cff5c3f528dfb64e8e46ad9a0469d01ae580493fa62`
- plan_metrics：`744ab076c457b43277a744413367ad5ab18b4e80563715c83c2d2ac166002c4d`
- summary：`2a3ce8c3ad6c1d4756c2ddc6386fbfbd617c62c128c5f18fb101031e8b036e5e`
- 本地导出SQLite（不是完整生产库hash）：`6fe836160afc4a5f68d6526641653cc169f67a79eaff5ff2fa7ea91a042b804e`
- 日线导出SQLite：`09cb277ebf24b4a740e3cd920e4684452a609350d98a67e0012d54a06c4b01a3`
- runner源码：`22023f3d54a299bbdcc19922cda672941e2f5b5c1f5ed3e1a106e23c1d1d9cac`

CODE：相关反例通过，整包全量由父代理汇总。

REPLAY：V0已观察窗口0差异；V1/V2完成原窗口对比；V3/4/5缺09:25、T+严格收益缺PIT、10-15尚半日，整体仍待证据。

OPERATIONS：未发布，未改生产或模拟账本；242运营缺口保留，不能由回放通过掩盖。

STRATEGY：没有满足每日2–5的证据，也没有严格胜率证据，不据此擅自应用参数。
