# W2：独立影子证据账本与到达后因果模拟

## 1. 交付与边界

本切片落实周末任务书 W2 的纯账本/因果模拟部分。只新增 `runtime/shadow_evidence.py`、`tests/test_w2_shadow_evidence.py` 与本文；不修改 engine、price_limits、outcomes、strategy_day_adapter、RuntimeStore、生产发布/策略/通知或桥接 state。已完整读任务书与 canonical outbox 0034/0035/0036。09:26 真实行情采集、A3 附加字段、运行接线与部署由根代理负责，本文不声称已完成。

首触发仅写 `PENDING_NEXT_COMPLETE_MINUTE`。下一完整 1m 必须实际到达、完整及来源/时钟合格后，才能调用既有 `evaluate_outcome`。参考 minute 不等于决策已知时点；实际触发知识时点取调用方 `observed_at`，不能回拨到参考 bar。无真实成交、研究收益或账户 PnL 造值。

## 2. W1/W2 接口

```python
ledger = ShadowEvidenceLedger(independent_db_path, independent_jsonl_path)
ledger.seal_price_limit_evidence(plan_id, evidence, observed_at=capture_time,
                                 raw_response=original_response_bytes_or_none)
ledger.record_signal(w1_signal, frozen_plan, observed_at=decision_known_time)
ledger.record_minute(w1_minute_summary, observed_at=summary_known_time)
ledger.advance_outcomes(arrived_bars, observed_at=current_known_time,
                        outcome_labels=arrived_dated_raw_close_labels)
ledger.recover_mirror()
snapshot = ledger.snapshot()
# W3纯读：不实例化writer，不初始化/修改schema
snapshot = read_shadow_evidence(independent_db_path, independent_jsonl_path)
```

调用方显式选择独立本地 SQLite/JSONL 路径；不发现/打开生产数据库，不创建 RuntimeStore、账户、执行计划或订单。旧非影子 DB、未知 JSONL 不覆盖。公开写接口返回有限回执，不向生产传播存储异常；数据库失败 `ok=False/stored=False/SHADOW_EVIDENCE_WRITE_FAILED`，JSONL 失败则数据库已提交且 `ok=True/stored=True/mirror_status=PENDING`，由独立接线日志记录和重试恢复。

实际 W1 字段 `schema_version=a4-shadow-signal/1`、`cohort=REALTIME_SHADOW`、`variant_set_version`、`variant_id`、plan_id/symbol/profile、minute/observation_time、variant_action/status、完整条件/effective_zone/sequence_evidence/hash/耗时均原样封存。兼容 schema/profile/budget_exceeded_count 别名，但两个别名同时出现必须一致；不会用缺字段默认 0。原 frozen plan 需有真实 plan_id、symbol 与 profile 或 strategy_profile，必要由已激活原 wrapper 显式投影 ID，不能只凭信号自报身份。W1 DATA_LIMITED/ERROR/BUDGET 的 action=None 可以存审计，OK 缺 action 拒绝。

三时钟已按真实W1对齐：`observation_time` 是冻结市场输入cutoff，不是接收/决策时钟，要求 cutoff≤dispatch minute≤recorded_at。精确反例 cutoff13:05、dispatch13:06、实际shadow处理13:06:05合法；因果trigger_at仍13:06:05，下一完整minute为13:08，不回填13:07。REALTIME_SHADOW不得靠回拨调用方时钟冒充历史实时PIT，离线合成测试不构成盘中证据。

账本自己判定触发：仅 OK、非 data_block、BUY_SIGNAL/ADD_SIGNAL（BUY/ADD 别名）才进入模拟；W1 自报 FIRST_TRIGGER 不足以授权。每计划日×variant_set_version×variant_id 的首次触发唯一；其后仅状态变化产生信号行，同分钟完全相同重入幂等、冲突重入拒绝，跨进程 SQLite 事务/唯一键是权威。计划原字段 hash 不可中途重标；失效时点与 expiry 取最早值，合法下一分钟也须在有效区间。

每分钟 summary 要求明确的 evaluated_plan_count、evaluated_variant_count、shadow_budget_exceeded_count（或一致别名）、elapsed_ms；保留原预算状态，不把 W2 耗时说成 W1 的 5 秒预算证明。

W1 `a4-shadow-minute/2` 的 baseline_records 原样封存，含外层实际动作来源声明/事件身份与inner/outer hashes；`CALLER_SUPPLIED_NOT_REEVALUATED` 不认证生产重算，MISSING/null 不当逐分钟差异0。signal里的inner baseline与summary里的actual outer baseline不混为一谈。

## 3. PIT 与成交合同

09:26 evidence 用现有 `price_limits.py` 字段：symbol/trade_date、security_name、board、security_type/security_status/is_st/limit_regime、listing_date、rule_effective_from、preclose/preclose_basis、observed_at、source_ref/source_input_sha256；显式 upper_limit/lower_limit 等沿用原解析器。原 evidence 与影子规范化版本分别保留，带 evidence SHA、captured_at、limits/derived_limit_prices 与来源状态。observed_at 必须 aware、当日且不晚于 capture_time；名称含 ST 只增加正面 ST 标记，不以名称未含 ST、代码前缀或当前名单认证 ordinary。缺当日名称不能派生普通身份。

raw_response 仅接受原 bytes，提供时实际 SHA 必须匹配 source_input_sha256；未提供只记录调用方绑定，不冒充 HTTP raw。即使 bytes 相符也不认证交易所来源、字段映射或历史 PIT，`source_authenticated=False`。不会保存原响应 bytes、凭证或原始异常内容。调用方应只传公开数据字段/安全 source_ref。

缺前收、ST、前五交易日新股、普通/正常制度未证实、旧日/未来观察保留 UNKNOWN，不自行推导。除权引用 prior_raw_close 与实际显示 preclose 不同时仅加 `EX_RIGHTS_REFERENCE_OBSERVED`，计算仍使用 EXCHANGE_DISPLAYED_PRECLOSE；DERIVED_RAW_CLOSE 不能贴成交易所显示真值。显式上下限与派生冲突为 CONFLICT，不成交。

重要：冻结 price_limits 的 `RULE_REVIEWED_THROUGH=2026-10-10`。10-12 不能静默延长；无显式当日上下限时保留 RULE_DATE_UNPROVEN。真实规则复核/截止延展属于根代理另一个评审切片。原合同允许合法同日显式上下限，不要求为了显式限价另外冒造 ordinary；名称/ST/上市/制度不明仍分别保持未知。

到达 bar 必須显式：symbol、1m interval、完整 True、MARKET_BAR、RAW/none/unadjusted、shares volume_unit、有效 OHLC/source_id、source_ref/source_input_sha256、bar_end/captured_at。要求 bar_end≤captured_at≤observed_at。未达/未来/不完整/量单位未知/缺绑定拒绝，不把下一根后来的方便分钟替代缺失的准确分钟。后续同 bar 内容和来源不变而 capture 更晚，保留首次到达，不误算修订；矛盾 revision 单独 journal 并阻断该 bar，保留旧观察历史不删除。推进时钟不能倒退。

`evaluate_outcome`、`resolve_price_limits` 和 `_first_complete_bar_end` 原样复用，滑点/tick、锁板、价格在 OHLC 外不成交、14:45、T+1 与合法止损缺分钟合同不另写。没有实例化 PaperBroker/RuntimeStore：既有 outcome 内部只用无账户 stateless adverse price。无封存 PIT 的 A3 旧限价字段不能补成交证据；T+1 卖出不挪用买入日上下限。DATED_RAW_CLOSE label 必须在该目标日15:00及实际捕获后才可进入研究收益，未来 labels 不进入触发。T+1/T+3/T+5、MAE/MFE、stop_r 缺腿 null；当日止损只记风险触及，收益为研究价格观察，非账户 PnL。

## 4. SQLite/JSONL 一致性与恢复

独立表 `shadow_signals/shadow_states/shadow_minutes/shadow_identity/shadow_bars/shadow_labels/shadow_clock/shadow_events`；无 execution_plans 等生产表。信号/状态/结果变化与 outbox journal 在一个 SQLite 事务里提交。event 携 seq、event_key、payload SHA、previous_event SHA、event SHA 与实际五模块 source SHA 向量；JSONL 保存完整事件，不把后来 outcome 覆盖掉首次触发时的 pending 事实。

两种文件无法共同 SQLite 原子提交：数据库是权威，JSONL 是明确可恢复镜像。镜像在 SQLite 写锁下核对原文件是 journal 的准确前缀，临时文件 fsync 后 replace；只有本账本准确前缀的断尾可修复，外来/分叉材料保留且 PENDING，不覆盖。SQLite 失败整笔 rollback，不留部分信号。每个连接显式关闭，短寿命进程可重开/恢复；临时镜像失败时本次精确 tmp 文件清理，不删除证据。

当前镜像为完整 journal 校验/原子替换，成本 O(journal size)，outcome 推进亦非零成本；这是组件正确性实现，不是生产每分钟≤5s/内存/fsync验收。根接线应安排独立 writer 与有界队列/独立失败回执，不占 A4 BoundedWorkGate 或生产动作预算；真正VM峰值、积压、进程结束恢复和自然交易日需独立OPERATIONS验收，不能因本地37或40反例绿即上线。

W3纯读 `read_shadow_evidence` 使用SQLite URI `mode=ro`、一个读事务，无constructor/schema/recovery写。校验journal seq/完整hash链、signals当前outcome投影以及identities/minutes与原journal相符，防止仅把DB投影篡改后当可信事实。返回 `signals`（signal、原plan/hash、recorded/trigger_at、最新outcome）、`minutes`（原W1summary）、`price_limit_evidence`（原身份/规范化/limits/annotations）、`events`（append-only）及 `hash_chain_status`、`snapshot_canonical_sha256`。outcomes在signals内，逐次变化另在OUTCOME_UPDATE journal里。

read结果的canonical SHA绑定本次一致读取的内容，不是DB原文件SHA：活SQLite可同时写，`source_db_sha256=null/source_db_sha256_status=QUIESCED_COPY_REQUIRED`。导出owner需另做静止副本字节绑定。可选JSONL核对仅纯读：相等为SYNCED，缺腿/并发尾差异不当一致通过；源认证始终False。reader是O(journal)组件，不宣称常量内存或生产耗时。

## 5. 真实命令与退出码

工作目录 `D:/dev_A股/liangjian_wp1_20261009`，使用既有 venv，未调用真实行情/DB/模型。初始新 API stub 的 26 合同反例失败不是既有生产26缺陷。

```powershell
$env:PYTHONPATH='D:/dev_A股/liangjian_wp1_20261009/src'
& 'D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe' -B -m pytest tests/test_w2_shadow_evidence.py tests/test_wp1_ablation_outcomes.py tests/test_wp1_price_limit_evidence.py tests/test_w1_shadow_variants.py -q --tb=short --junitxml=artifacts/w2-20261010/w2-final-v3.xml
$taskExit=$LASTEXITCODE; Write-Output "PYTHON_EXIT=$taskExit"; exit $taskExit
```

| 保存记录/命令选择 | 实际结果 | native exit |
| --- | --- | --- |
| `w2-before.xml`，仅新tests/stub | 26 failed | 1 |
| `w2-after-v1.xml` | 25 passed / 1 failed | 1 |
| `-k same_day` 定位 | 1 failed | 1 |
| 本地 ExchangeTradingCalendar.next_trading_day(2026-10-08) | 2026-10-09 | 0 |
| `w2-after-v2` 命令错误写成不存在的 test_wp1_price_limits.py | pytest入口错误，无有效XML | 4 |
| `w2-interface-before.xml`，`-k 'actual_w1 or clock_not or alias_conflicts'` | 3 failed | 1 |
| `w2-after-v3.xml`，新W2+outcomes+真实price_limit_evidence测试 | 90 passed | 0 |
| `w2-arrival-before.xml`，`-k 'name_missing or later_capture or clock_regression'` | 3 failed | 1 |
| `w2-final-v1.xml`，增加W1集成切片 | 120 passed | 0 |
| `w2-binding-before.xml`，`-k 'pins_implementation or transaction_failure or partial_reference'` | 1 failed / 2 passed | 1 |
| `w2-final.xml` 首次收口 | 123 passed | 0 |
| `w2-reader-clock-before.xml`，真实dispatch/cutoff与纯读入口 | 2 failed | 1 |
| `w2-final-v2.xml`，W1正并行添加baseline_records反例 | 126 passed / 2 failed（仅W1新红例） | 1 |
| `w2-material-before.xml`，`-k material_projection` | 2 failed | 1 |
| 最终 `w2-final-v3.xml` | **131 passed** | **0** |

最终131=新W2 **45** +既有 outcomes/limits **61** +W1 **25**。T+1的旧合成fixture open=9.2/high=9.4/low=9但默认close=10违反OHLC，改close=9.3后保持风险/合法退出断言，未放松验证。另修了实际W1 schema_version、strategy_profile、None action 与预算字段的真实接口差异，并补别名矛盾/时钟回拨负例。W1并行新增红例由W1作者修复，本切片没编辑其源码/测试。没有扩大全量，根代理联合提交后验收。git diff --check exit0。

## 6. 冻结清单、SHA 与四层

新增源 `src/liangjian_funnel/runtime/shadow_evidence.py` SHA `c02c69beb58d798931a90c31d3d5a0204a281b824bc47f29ede97883bce9aaa0`；新测试 SHA `ddc64a90067d69f57179ca8005fde40ef4a7245aa74797465c56d0bc2d4a64ee`。本文SHA交接另列，避免自引用。未提交/push。

冻结复用模块 SHA：price_limits `ec1c395b452e8d6fa1e5b6016f05f88a2ccdb982296f1b8940b73ed0d8d4549c`，outcomes `f87dad9b6e9b9292e6bc22796d56d67de073f12a3da5345ef0286f322b1c18aa`，simulation `b0b5eddb3bb765007efc6f4331498f2ebfc06999717f1d15642b54104a678d97`，stock_trading_rules `9498df625973c528420613e86e255fc35b612eafc65868dc1999c542c5bfcb41`。本切片不改这些模块。

四个红XMLSHA按 before/interface/arrival/binding 顺序：`8d8c0e94a201b3074d2e11deefa4691babd90cc37604cd08c5c6e967b1107a4f`、`94985a04b2485a98cebbccff77b30e61e847a439e1b49e87df048155125b783a`、`d1e20abfc2002c97372609dc1400a6534ba03c0d2b089638359dac39bcae8dfa`、`647470d25b0c50720819ec296ef2b8fe2a020efff5072cced2cce5d33f2e5d8f`；final SHA `9cf4e975496c0a712ecce0dee9f82c60319b1055ec9d2332987f004797d77746`。

最后增补reader-clock/material红XML SHA `12a7c011b1582277f39d740d4a9f3164435f5d4db31372edb00dae3fe83df5a5` / `60c91acef2de94d049bedc7f962d0025bd3b8a47873bba33a34aff45cd45295e`；最终131项 `w2-final-v3.xml` SHA `b182cb778c314800a7dfc8bf68ef7a5f6be94a6cef182a47ee38f4f86cb06bdc`。

CODE：45新反例及相关131项通过。REPLAY：仅本机合成arrival/重入/失效/存储故障合同，未重复历史formal矩阵。OPERATIONS：09:26真实身份采集、bar到达、独立writer/预算、生产隔离与零客户通知未盘中验证，未部署。STRATEGY：没有新增真实成交样本或五日正期望证明；未达到20成交样本，不计算胜率或参数上线结论。周日决策14、周五决策16与A5/W3接线不由本切片批准。
