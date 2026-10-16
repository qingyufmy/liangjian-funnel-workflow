# A6 风控师任务书 · 2026-10-15 —— 角色、提示词、数据交互与联动（周五收盘后开发，与 B4-LLM Phase 1 同一候选）

> 依据：Tony 2026-10-15 20:20 "后面我们会补充一个专业的 A6 风控师角色"；20:24 "你把 A6 一并设计进去项目中，明天收盘后我们一并开发修改……包括详细的角色定义、提示词、以及与项目各个环节运行的数据交互和联动"。
> 前置：docs/CODEX_ITERATION_BRIEF_2026-10-15_B4_LLM.md 第 6 节（A4 交易员，周一 CONTROL 接管）。A6 与 A4 交易员同一候选发布（周五晚），周一同时生效。
> 阶段定位：当前为信号研究 + 模拟盘阶段，不连接真实账户。A6 管理的是**模拟账户**；所有仓位、成交、盈亏均为模拟，但口径按真实交易规则（T+1、涨跌停、最小手数、手续费）执行，为将来接真实账户做准备。
> 约束：确定性风控引擎是最终裁决（A6-LLM 只能在引擎给定的上限内收紧，不能放宽）；客户飞书现有模板不变（新增风控卡需 Tony 单独批准）；凭证不进桥接；不盘中部署。

---

## 1. 团队中的位置

```
A1 研究员 ─→ A2 策略师 ─→ A3 交易计划官 ─→ A4 交易员 ─→ A6 风控师 ─→ 模拟账户/客户信号
                                  ↑               ↑              │
                                  └── 风险容量 ────┴── 风险信封 ──┘
                                                     A5 复盘教练 ←── 全链路标签与盈亏
```

- **A6 对谁负责**：对模拟账户的资金安全与风险预算负责；对 Tony（最终风控授权人）负责。
- **A6 从上游拿什么**：A2 的市场状态与新开仓政策；A3 的计划及优先级；A4 的交易信号（含 `handoff_to_a6`）；引擎的实时行情与持仓估值。
- **A6 交给下游什么**：给 A4 每个计划的**风险信封**（可开仓否、风险单位、最大批次、禁入时段）；给引擎每笔信号的**仓位决定**（股数由引擎按风险单位换算）；给 A3/A2 **风险容量**（明天还能承接几条计划、哪些簇已满）；给 A5 **风险日志与账户曲线**。
- **谁考核 A6**：A5 按"风险事件命中率、回撤控制、拒绝信号的事后表现"考核；Tony 终审。

---

## 2. 职责按交易日时段划分

| 时段 | 任务 | 执行者 | 产出 |
|---|---|---|---|
| 08:35（盘前，A3 计划发布后） | **风险晨会**：读市场状态、账户、持仓、14 条计划；定当日风险预算与每计划风险信封 | 引擎算上限 → A6-LLM 在上限内裁定 | `risk_envelopes`（每 plan_id 一条）、`daily_risk_budget` |
| 09:26（晨审后） | 对激活/失效结果更新信封；失效计划信封作废 | 引擎 | 信封状态 |
| 盘中每笔 A4 信号（同步，<2s） | **仓位裁决**：信号 ∧ 信封 ∧ 组合限额 → 股数/批次/拒绝 | 引擎（确定性） | `risk_decisions` 一行；模拟成交 |
| 盘中每 5 分钟（随交易员节拍） | **异常复核**：仅在触发条件时调用 A6-LLM：日内亏损逼近限额、同簇暴露逼近上限、市场状态翻转、持仓触及减仓线 | A6-LLM | `risk_actions`（REDUCE/HALT_NEW_ENTRY/RESUME 建议 → 引擎执行） |
| 持仓每分钟 | 硬止损/失效位/涨跌停不可成交由引擎直接处理；T+1 可卖数量维护 | 引擎 | 持仓生命周期事件 |
| 15:10（收盘后） | **收盘风控复盘**：持仓逐只评估（继续持有/明日减仓/明日清仓建议）、账户曲线、风险事件归因；向 A3 输出明日风险容量 | A6-LLM | `eod_risk_review`、`risk_capacity_for_tomorrow` |
| 16:00（A5 前） | 把风险日志、账户曲线、拒绝信号清单交 A5 | 引擎 | A5 事实快照新增 `a6` 段 |

A6-LLM 每日固定 2 次（08:35、15:10）+ 盘中异常触发（预期 0–3 次）。盘中每笔信号的仓位裁决全部确定性，不进模型，保证延迟与可复现。

---

## 3. 确定性风控引擎（`runtime/risk_engine.py`，A6 的"算盘"）

### 3.1 模拟账户模型
- `sim_account`：初始资金、现金、持仓市值、NAV、当日已实现/未实现盈亏、峰值 NAV、回撤。
- `sim_positions`：symbol、plan_id、strategy_profile、cluster、开仓时间、批次、均价、数量、可卖数量（T+1）、止损位、失效位、目标位、状态（OPEN/REDUCING/CLOSED）。
- `sim_fills`：信号 → 成交：以信号分钟**下一根 1m VWAP** 成交（买入不高于 no_chase；涨停封板/一字 → UNFILLED），手续费按 `EXCHANGE_RULES`（佣金、印花税、过户费）扣除；跌停不可卖 → 延至次日。
- 所有表带 `trade_date`、`run_id`、`source_signal_id`，可回放。

### 3.2 限额（默认值，Tony 周五 18:00 前批准或修改）
| 参数 | 默认 | 说明 |
|---|---|---|
| `INITIAL_CAPITAL` | 1,000,000 | 模拟初始资金 |
| `RISK_PER_TRADE` | 1.0% NAV | 单笔风险额 = NAV × 1%；股数 = 风险额 ÷ (入场价 − 止损位)，向下取整到 100 股 |
| `RISK_UNIT_PROBE` | 0.5× | PROBE 信号按半个风险单位 |
| `MAX_POSITION_PCT` | 20% NAV | 单票市值上限 |
| `MAX_CLUSTER_PCT` | 40% NAV | 同簇（industry/theme）市值上限 |
| `MAX_POSITIONS` | 6 | 同时持仓数上限 |
| `MAX_NEW_ENTRIES_PER_DAY` | 3 | 当日新开仓笔数（决策 4 目标 2–5 信号，但仓位只给前 3） |
| `MAX_CLUSTER_SIGNALS_PER_DAY` | 2 | 同簇当日买入信号（Tony 10-15 裁定） |
| `DAILY_LOSS_HALT` | −2.0% NAV | 当日亏损触及 → 停止新开仓至收盘 |
| `DRAWDOWN_HALF_RISK` | −8% 峰值 | 回撤触及 → 风险单位减半 |
| `DRAWDOWN_HALT` | −15% 峰值 | 回撤触及 → 全面停止新开仓，待 Tony 解除 |
| `REGIME_RULES` | BEAR_RISK / RISK_OFF_RETREAT → 只允许 PROBE；ICE_POINT_NO_NEW_ENTRY → 禁新开仓 | 来自 A2 市场状态（当日 live_market_state 优先） |
| `NO_NEW_ENTRY_AFTER` | 14:30 | 之后不新开仓（沿用 A4 规则） |
| `MIN_CASH_RESERVE` | 10% NAV | 现金下限 |

### 3.3 引擎函数（确定性、可单测）
- `build_risk_envelope(plan, account, budget, regime)` → 信封：`eligible`、`risk_unit`（STANDARD/PROBE/NONE）、`max_batches`、`max_shares_cap`、`blocked_reasons[]`。
- `adjudicate_signal(signal, envelope, account, positions)` → `risk_decision`：`APPROVED(shares, batch_no) / REDUCED(shares, reason) / REJECTED(reason)`；拒绝码枚举：`ENVELOPE_INELIGIBLE / DAILY_ENTRY_CAP / CLUSTER_CAP / POSITION_CAP / CASH_RESERVE / DAILY_LOSS_HALT / DRAWDOWN_HALT / REGIME_PROBE_ONLY / NOT_TRADABLE / T1_NOT_SELLABLE`。
- `apply_fill(decision, next_bar)`、`mark_to_market(positions, quotes)`、`check_hard_exits(positions, bars)`（止损/失效位/目标位）、`sellable_quantity(position, trade_date)`。
- `exception_triggers(account, positions, regime)` → 是否调用 A6-LLM 及触发原因。
- `risk_capacity(account, positions, limits)` → 明日可承接计划数、各簇剩余额度、建议风险单位。

---

## 4. A6-LLM 的判断范围（引擎上限内）

| 场景 | 引擎给出 | A6-LLM 可做 | A6-LLM 不可做 |
|---|---|---|---|
| 晨会 | 每计划上限信封 | 在上限内**收紧**：降 PROBE、减批次、把某簇暂停、给出当日关注的风险事件；对计划排优先承接顺序 | 放宽任何上限；给出股数 |
| 盘中异常 | 触发原因 + 账户快照 | 建议 REDUCE（某持仓减半/清仓）、HALT_NEW_ENTRY、RESUME（仅当触发条件已解除） | 直接下单；绕过硬止损；改限额 |
| 收盘复盘 | 持仓估值、风险事件、拒绝信号清单 | 逐只持仓给 HOLD / REDUCE_TOMORROW / EXIT_TOMORROW 建议 + 理由；明日风险容量建议；给 A3 的簇配额建议；风险事件归因 | 改历史记录；给出收益预测 |

每个判断必须含 `supporting_refs`、`opposing_refs`（各 ≤3，引用证据目录 ID）、`what_would_change_my_mind`。

---

## 5. 提示词草案 `prompts/agent_6_risk_governor_v1.txt`（≤12KB，Codex 按此落地，前置共享团队章程）

```
Agent 6 · RiskGovernor · 风控师
version: agent-6-risk/1.0.0   前置: SHARED_SYSTEM_V2（团队章程）

【角色卡】
我是团队的风控师。我对模拟账户的资金安全和风险预算负责，对 Tony 负责。
我从策略师（A2）拿市场状态与新开仓政策，从交易计划官（A3）拿计划与优先级，
从交易员（A4）拿交易信号与 handoff_to_a6；确定性风控引擎替我计算一切数值并给出上限。
我交给交易员每个计划的风险信封，交给引擎减仓/停开仓建议，交给计划官明日风险容量，
交给复盘教练（A5）风险日志。我被 A5 按"风险事件命中、回撤控制、拒绝信号的事后表现"考核。
我只能在引擎上限内收紧，不能放宽；我不输出股数、金额，不下单，不改硬止损。

【输入（只读，均由引擎注入一次）】
RISK_SESSION          = PREMARKET | INTRADAY_EXCEPTION | POST_CLOSE
ACCOUNT_SNAPSHOT      = NAV、现金、持仓市值、当日盈亏、峰值回撤、已用风险预算
POSITIONS             = 逐只持仓：计划、簇、均价、数量、可卖数量、止损/失效/目标位、浮盈亏、持有天数
RISK_LIMITS           = 当前生效限额（只读）
ENGINE_ENVELOPES      = 引擎为每个计划算出的上限信封（PREMARKET）
EXCEPTION_CONTEXT     = 触发原因、相关持仓/簇、市场状态变化（INTRADAY_EXCEPTION）
MARKET_STATE          = 当日 live_market_state（优先）与 A2 昨日先验（标明时效）
PLANS                 = A3 计划（几何、策略、簇、priority_rank、交易计划简报）
TRADER_SIGNALS_TODAY  = 当日交易员信号与引擎裁决（POST_CLOSE）
RISK_EVENTS_TODAY     = 当日硬止损、失效、拒绝、UNFILLED 事件
EVIDENCE_CATALOG      = 可引用证据 ID 目录
PRIOR_OUTCOME_FEEDBACK= A5 回填：我过去 N 日的减仓/拒绝建议事后表现（方法级聚合）

【规则】
1. 数值只来自引擎；我引用，不计算、不估算。
2. 每个结论同时给 supporting_refs 与 opposing_refs（各≤3，只能引用 EVIDENCE_CATALOG），
   并写 what_would_change_my_mind（≤2 条）。只有一边的结论无效。
3. PREMARKET：对 ENGINE_ENVELOPES 中每个计划返回 envelope_adjustment：
   KEEP | TIGHTEN_TO_PROBE | REDUCE_BATCHES | SUSPEND，并给承接顺序 accept_order。
   不得出现 LOOSEN。簇暴露、持仓集中、市场状态是主要依据；单一标的的叙事不是。
4. INTRADAY_EXCEPTION：只对 EXCEPTION_CONTEXT 列出的对象给 action：
   REDUCE_HALF | EXIT | HALT_NEW_ENTRY | RESUME | NO_ACTION。RESUME 只能在引擎标记
   trigger_cleared=true 时给出。硬止损/失效位已由引擎执行，不重复建议。
5. POST_CLOSE：逐只持仓给 HOLD | REDUCE_TOMORROW | EXIT_TOMORROW；给
   capacity_for_tomorrow（建议可承接计划数、各簇建议配额、建议风险单位）；
   给 risk_event_attribution（每个风险事件：规则正确触发 / 规则过严 / 数据问题 / 待观察）。
6. 缺失数据 fail closed：输入不完整时对应对象只能 TIGHTEN/HALT/HOLD，不能 KEEP/RESUME。
7. 不输出 Markdown、推理过程或 Schema 外字段。

【输出】
{"schema_version":"a6-risk/1.0.0","session":"...","envelope_adjustments":[{"plan_id":"","adjustment":"KEEP|TIGHTEN_TO_PROBE|REDUCE_BATCHES|SUSPEND","accept_order":1,"reason_code":"","supporting_refs":[],"opposing_refs":[],"what_would_change_my_mind":[]}],
 "exception_actions":[{"target":"symbol|cluster|account","action":"REDUCE_HALF|EXIT|HALT_NEW_ENTRY|RESUME|NO_ACTION","reason_code":"","supporting_refs":[],"opposing_refs":[],"what_would_change_my_mind":[]}],
 "position_reviews":[{"symbol":"","stance":"HOLD|REDUCE_TOMORROW|EXIT_TOMORROW","reason_code":"","supporting_refs":[],"opposing_refs":[]}],
 "capacity_for_tomorrow":{"plans_acceptable":0,"cluster_quota":{},"risk_unit":"STANDARD|PROBE|NONE","notes":[]},
 "risk_event_attribution":[{"event_id":"","verdict":"RULE_CORRECT|RULE_TOO_TIGHT|DATA_ISSUE|PENDING","note":""}],
 "handoff_notes":{"to_a3":[],"to_a4":[],"to_a5":[]},
 "unresolved_questions":[]}
```

---

## 6. 数据交互与联动（逐环节）

| 环节 | 方向 | 数据 | 机制 |
|---|---|---|---|
| A2 → A6 | 输入 | `market_regime`、`new_entry_policy`、live_market_state | 晨会读昨收冻结；盘中引擎每 5m 读当日 live 状态；状态翻转触发异常复核 |
| A3 → A6 | 输入 | 计划几何、strategy_profile、cluster、priority_rank、交易计划简报 | 08:30 发布后立即生成引擎信封；A6 晨会 08:35 |
| A6 → A3 | 反馈 | `capacity_for_tomorrow`（计划数、簇配额、风险单位） | 写入 A3 下一次收盘研究的 `RISK_CAPACITY` 注入字段；**仅影响展示优先级与簇标签，不改 A3 技术门**（Tony 另批前） |
| A6 → A4 | 输入 | `risk_envelopes`（每计划：eligible、risk_unit、max_batches、blocked_reasons） | 09:26 后随激活计划注入交易员上下文；交易员对 `eligible=false` 的计划只能 NO_ACTION |
| A4 → A6 | 输入 | 交易信号 + `handoff_to_a6` | 信号产生后同步调用引擎 `adjudicate_signal`（<2s，在 monitor 分钟内） |
| A6 → 引擎/账户 | 执行 | `risk_decision` → 模拟成交 → 持仓 | 同一分钟内落库；UNFILLED 记录原因 |
| A6 → 客户 | 投递 | 现有信号卡**不变**；`risk_decision` 结果（APPROVED/REDUCED/REJECTED + 风险单位）暂只进内部封存与 A5 | 若 Tony 批准，加"风控卡"模板（另立） |
| A6 → A5 | 输入 | `a6` 事实段：账户曲线、持仓、风险决策、拒绝清单、异常复核、A6-LLM 原始输出 | A5 新增 `a6_review`、`risk_governor_value`（拒绝信号 vs 批准信号 T+1/T+3） |
| A5 → A6 | 反馈 | `PRIOR_OUTCOME_FEEDBACK`（方法级：减仓建议命中率、拒绝信号事后表现） | 标签满 10 日注入 |
| outcomes → A6 | 回填 | 持仓 T+1/T+3 估值、已实现盈亏 | 现有 outcomes 任务扩展读 `sim_positions` |
| Node 调度 | 任务 | 新增 `risk-premarket`（08:35，review 槽，60s）、`risk-eod`（15:10 收盘链路内，由 close 任务末尾调用，120s）；异常复核挂在 monitor 的 5m 交易员节拍内（同一请求后串行，≤20s） | 预算：monitor 58s 内 交易员 30s + 引擎裁决 2s + 异常 A6 ≤20s（仅触发时） |

---

## 7. 护栏与失效处理
- A6-LLM 调用失败/超时/解析失败 → 引擎按**上限信封的保守版**执行（STANDARD→PROBE、批次=1），记 `A6_DEGRADED`，不停止交易员路径；连续 2 次失败 → 当日 HALT_NEW_ENTRY 并通知 Tony（内部 webhook，决策 15）。
- 任何 A6-LLM 输出出现 LOOSEN 语义或 Schema 外字段 → 整份丢弃，按保守版执行。
- 限额修改只能通过 `config/risk_limits.yaml` + Tony 批准 + 发布；盘中不可改。
- DRAWDOWN_HALT 解除需 Tony 在聊天批准，Claude 记录 `tony:` 行。

---

## 8. 开发任务项（周五 15:10 后开始；与 B4-LLM Phase 1 同一候选，周五晚发布，周一生效）

| # | 项 | 验收 |
|---|---|---|
| R1 | `runtime/risk_engine.py`：3.1 账户/持仓/成交模型 + 3.3 全部函数 + `config/risk_limits.yaml`（3.2 默认值） | 单测：每条限额各 1 正 1 反例；T+1 可卖；涨停 UNFILLED；跌停延卖；手续费 |
| R2 | 信号裁决接线：A4 交易员信号 → `adjudicate_signal` → 模拟成交 → 持仓；每分钟 mark-to-market + 硬退出 | 六日冻结回放（用 L12 交易员回放信号）：账户曲线、拒绝码分布、成交率；monitor 分钟内 <2s |
| R3 | 风险信封：08:30 发布后生成、09:26 更新、注入交易员上下文 | 周一 08:35 可见 14 条（或新批次）信封 |
| R4 | A6-LLM 三会话（晨会/异常/收盘）接线 + 提示词 v1 + 失效保守路径（§7） | 10-14/10-15 冻结快照真实模型干跑 JSON 合法；故障注入 2 条反例（超时、LOOSEN 输出）均走保守路径 |
| R5 | Node 任务 `risk-premarket`/`risk-eod` + 异常复核挂钩 | 隔离调度 mock 测试；phase_timings 新增 risk 段 |
| R6 | A5 新增 `a6` 事实段与 `a6_review` / `risk_governor_value`；outcomes 读模拟持仓 | 周一盘后 A5 含 |
| R7 | 风险容量回灌 A3 注入字段（只展示，不改门） | 周一收盘研究 A3 回执含 `RISK_CAPACITY` |
| R8 | 内部告警：DAILY_LOSS_HALT / DRAWDOWN_* / A6_DEGRADED×2 → 决策 15 内部 webhook（非客户） | 反例 |
| R9 | 文档 `docs/A6_RISK_GOVERNOR_CONTRACT.md`：表结构、拒绝码、信封字段、会话时序 | — |

发布门：R1–R5 全绿 + 六日回放账户曲线可复现（两次回放逐字节一致）+ 全量/VM/回退（树 ≡ 当时生产 HEAD）+ 包 SHA；Claude ACCEPT 后 P0 委托发布。R6–R9 可随周一后续包。

---

## 9. 周一起观察点（叠加 A4 交易员）
- 08:35 晨会输出：信封数 = 计划数；SUSPEND/TIGHTEN 理由可引用。
- 盘中：每笔交易员信号有且一条 risk_decision；拒绝码分布；模拟成交 UNFILLED 比例；异常复核触发次数与耗时。
- 15:10：收盘复盘输出、账户曲线、明日容量；A5 含 `a6_review`。
- 一周后：拒绝信号 vs 批准信号 T+1/T+3；减仓建议命中率 → 决定限额是否调整（Tony 批准）。

---

## 10. 需要 Tony 周五 18:00 前裁定
1. §3.2 限额默认值（尤其初始资金、单笔风险 1%、日内亏损停开仓 −2%、回撤减半 −8%）。
2. 客户是否收到风控结果（新增风控卡）；默认不发，仅内部。
3. A6-LLM 使用 review_model（pro）；盘中异常复核是否允许调用（默认允许，≤20s）。

---

## 11. 发给 Codex 的正文
```
【任务书 A6 风控师 · 2026-10-15】以 docs/CODEX_TASK_BRIEF_A6_RISK_GOVERNOR_2026-10-15.md 为准，与 docs/CODEX_ITERATION_BRIEF_2026-10-15_B4_LLM.md 第 6 节（A4 交易员）同一候选，周五 15:10 收盘后开始，周五晚发布，周一生效。约束：确定性风控引擎最终裁决，A6-LLM 只能在引擎上限内收紧；模拟账户按真实交易规则；客户模板不变；限额只能经 config/risk_limits.yaml + Tony 批准修改。

R1 risk_engine（账户/持仓/成交模型、信封、裁决、mark-to-market、硬退出、T+1、容量）+ risk_limits.yaml 默认值（§3.2，Tony 周五 18:00 前确认）；R2 交易员信号→裁决→模拟成交接线（monitor 分钟内 <2s）；R3 08:30 信封生成/09:26 更新/注入交易员；R4 A6-LLM 三会话 + prompts/agent_6_risk_governor_v1.txt（§5）+ 失效保守路径（超时/LOOSEN → PROBE、批次 1；连续 2 次 → HALT + 内部告警）；R5 Node risk-premarket(08:35)/risk-eod(15:10 末) + 5m 异常复核挂钩（≤20s）；R6 A5 a6 段 + risk_governor_value；R7 容量回灌 A3 注入字段（只展示）；R8 内部告警（决策 15 webhook）；R9 合同文档。

发布门：R1–R5 全绿；六日冻结回放（叠加 L12 交易员信号）账户曲线两次回放逐字节一致；10-14/10-15 真实模型干跑 JSON 合法；两条故障注入反例；全量/VM/回退树≡生产 HEAD/包 SHA。Claude ACCEPT 后 P0 委托发布。
```

---

## 12. 追加（Tony 20:29）：模拟盘执行层 —— 本地开源模拟券商 + 平台适配器

> Tony："再补充一个开源的 A 股模拟盘，初始资金 100 万进行模拟盘的执行和跟踪。后续会对接掘金量化等平台的模拟盘和实盘。"

### 12.1 架构：券商适配层（Broker Adapter）
§3.1 的 `sim_account / sim_positions / sim_fills` 从 A6 引擎中**抽出**为独立执行层，A6 只通过统一接口读账户、下指令：

```
A4 交易员信号 → A6 风控裁决(股数) → BrokerAdapter.place_order()
                                         ├── LocalPaperBroker   （本地开源模拟盘，默认，周一起）
                                         ├── GmSimBroker        （掘金仿真账户，适配器，下一阶段）
                                         ├── GmLiveBroker       （掘金实盘，需 Tony 单独授权）
                                         └── QmtBroker          （预留）
```
接口 `runtime/broker/base.py`：
- `get_account() → {cash, nav, positions_value, frozen, pnl_realized, pnl_unrealized}`
- `get_positions() → [{symbol, qty, sellable_qty, avg_cost, market_value, open_date}]`
- `place_order(symbol, side, qty, order_type=LIMIT|MARKET, limit_price, client_order_id, plan_id, signal_id) → order_id`
- `cancel_order(order_id)`；`get_orders(trade_date)`；`get_fills(trade_date)`
- `on_bar(minute_bar)`（仅本地模拟盘需要：驱动撮合）
- 事件回调：`on_fill / on_reject / on_cancel` → 写 `exec_ledger`
所有适配器返回同一 schema；A6、A5、outcomes 只认这个 schema，与具体券商无关。

### 12.2 LocalPaperBroker（本地开源模拟盘，`runtime/broker/local_paper.py`）
- **独立包**：`liangjian_paper_broker/`（MIT，可单独开源），无生产依赖；输入仅为 1m K 线 + 交易规则快照。
- 账户：初始资金 **1,000,000**（`config/paper_broker.yaml`），全部持久化 SQLite（`paper_broker.db`），每日快照 + 可从 0 重放。
- 撮合规则（A 股）：
  - 限价买：下一根 1m 内 `low ≤ limit` 成交，成交价 = min(limit, 下一根 1m VWAP)；否则挂单至撤单/收盘自动撤。
  - 限价卖：`high ≥ limit` 对称处理。市价单：下一根 1m VWAP，受涨跌停限制。
  - 涨停封板买不成交（一字/封板判定沿用 `TRADABILITY_FLAGS`）；跌停卖不成交；停牌拒单。
  - 部分成交：按该分钟成交量的 ≤10% 为上限（`MAX_PARTICIPATION=0.10`），超出部分留挂。
  - T+1：当日买入 `sellable_qty=0`，次日开盘转可卖。最小 100 股，卖出可零股。
  - 费用：佣金万 2.5（最低 5 元）、印花税 0.05% 卖出、过户费 0.001%（沪）；参数在 yaml。
  - 盘后固定价（深市 15:05–15:30）默认关闭。
- 每日 15:30 结算：mark-to-market、生成 `daily_nav`、`daily_positions` 快照；账户曲线由此表画。
- 确定性：同一 K 线 + 同一指令序列两次运行账户逐字节一致（发布门）。
- 对账：每日收盘 `reconcile()` 对比 exec_ledger 与持仓，差异 → `BROKER_RECONCILE_MISMATCH` 内部告警。

### 12.3 掘金适配（下一阶段，不在本候选）
- `GmSimBroker`：用掘金 SDK 的仿真账户；指令与回报映射到 §12.1 schema；撮合由平台完成，本地只记账与对账。
- 切换方式：`LIANGJIAN_BROKER=local_paper|gm_sim|gm_live`；`gm_live` 需 Tony 聊天批准并记 `tony:` 行，且 A6 的 `DRAWDOWN_HALT`、`DAILY_LOSS_HALT` 在实盘下不可被 RESUME 自动解除。
- 平台凭证只进 VM env，不进仓库/桥接/回执（同决策 15 规则）。
- 本地模拟盘与掘金仿真可**并行**运行（同一指令双写），用于校准本地撮合规则与平台差异；差异报告进 A5。

### 12.4 对 A6 任务项的修订
- R1 拆分：R1a `risk_engine`（信封、裁决、容量、硬退出判定）；R1b `LocalPaperBroker`（§12.2）+ `BrokerAdapter` 接口；A6 通过接口取账户。
- R2 改为：交易员信号 → 裁决 → `place_order` → 成交回报 → 持仓；monitor 分钟内 <2s（本地撮合在下一根 1m 到达时完成）。
- 新增 R10：`paper_broker` 包的 README、yaml、单测（撮合 12 条规则各 1 正 1 反例）、六日回放逐字节一致证明。
- 新增 R11：掘金适配器接口桩 + 映射表（不接真实 SDK），下一阶段实装。
- §9 观察点新增：每日 15:30 结算快照、账户曲线、UNFILLED/部分成交比例、对账 0 差异。

### 12.5 Tony 裁定项（补充到 §10）
4. 本地模拟盘参与率上限 10%、费用参数、盘后固定价关闭——默认值是否接受。
5. 掘金仿真账户接入时间（建议：本地模拟盘稳定运行 5 个交易日后）。

---

## 13. 追加（Tony 20:31）：资金使用率、仓位管理、槽位管理的责任归属

> Tony："需要有角色对模拟盘的资金使用率、仓位管理和槽位管理负责……整个交易团队对最终模拟盘的表现负责。"

### 13.1 分析
三件事性质不同，不能都压给风控师：
- **资金使用率**是进攻指标（有机会时钱要用出去），风控的天性是收紧；让风控师同时负责"用满"和"守住"会产生角色内冲突，最终一定偏向守住——今天零信号的局面就是这种偏向的产物。
- **槽位管理**（同时持仓数、每策略/每簇的名额、谁优先占位）是组合层决策，发生在盘前计划阶段，与计划官的工作面重合。
- **仓位管理**（已持仓的加减、日内离场、隔夜去留）分日内与隔夜两段：日内归执行者，隔夜归组合层。
真实交易团队的分工是：**组合经理定配置、交易员管执行、风控独立约束、复盘官打分**。本项目按此落位，不新增第七个 Agent，而是把 A3 的角色从"计划核对员"升级为**交易计划官兼组合经理（PM）**。

### 13.2 责任矩阵（R=负责并被考核，A=裁决/否决，C=提供输入，I=知会）
| 事项 | A3 组合经理 | A4 交易员 | A6 风控师 | A5 复盘教练 | 引擎 |
|---|---|---|---|---|---|
| 资金使用率目标（按市场状态定当日目标区间） | **R** | I | A（上限） | 打分 | 算 |
| 槽位分配（持仓数名额、每策略/每簇名额、计划占位顺序） | **R** | I | A（簇/持仓上限） | 打分 | 算 |
| 日内入场/离场择时 | C（计划简报） | **R** | A（信封/裁决） | 打分 | 护栏 |
| 日内持仓管理（加仓、减仓、止盈、硬止损前的主动离场） | I | **R** | A | 打分 | 硬止损直接执行 |
| 隔夜持仓去留（HOLD / 明日减 / 明日清） | **R** | C（日内观察） | A（可否决为减/清，不可改为持有） | 打分 | 执行 |
| 风险限额、回撤熔断、簇暴露 | I | I | **R** | 打分 | 执行 |
| 模拟盘最终表现（NAV、回撤、使用率、命中率） | **R（主）** | R（执行质量） | R（回撤） | 出团队记分卡 | — |

### 13.3 各角色的具体职责调整
**A3 组合经理（在 B4-LLM L3 基础上扩展，提示词 `agent_3_portfolio_planner_v4.txt`）**
- 08:35 晨会（与 A6 同一节拍，A6 先出上限）：
  - `deployment_target`：当日资金使用率目标区间，默认按市场状态：TREND_MAINLINE 60–80%、ROTATION_NO_MAINLINE 40–60%、REPAIR 30–50%、RISK_OFF_RETREAT 0–30%、ICE_POINT 0%；A6 上限之内可下调，不可上调。
  - `slot_plan`：可用槽位 = `MAX_POSITIONS − 当前持仓`；为每个槽位指定候选计划（主/备各一），按策略与簇分配名额；给交易员的承接顺序。
  - 每条计划的 `intended_risk_unit`（STANDARD/PROBE）与 `max_batches`（在 A6 信封内）。
- 15:10 收盘：对每只持仓给 `overnight_stance`（HOLD / REDUCE_TOMORROW / EXIT_TOMORROW）及理由；A6 只能把 HOLD 否决为减/清，不能反向。
- 考核：使用率达标率、槽位使用率、占位计划的 T+1/T+3、隔夜决策命中率。
- 技术门与候选数仍由确定性规则决定；PM 只在合格计划之间做配置。

**A4 交易员**：新增日内持仓管理动作已在动作集（ADD/REDUCE/SELL）；对已持仓每 5 分钟节拍给 `position_action`；主动离场须引用证据并说明与硬止损的距离。考核：入场后 MFE/MAE、止损前主动离场比例、滑点。

**A6 风控师**：职责不变（§2–§5），新增对 PM 决策的**否决权记录**：每次否决写 `pm_override`（对象、PM 原意、A6 动作、理由）；A5 评估 A6 否决的事后表现，防止风控过度收紧。新增使用率**下限告警**：当 A3 目标区间下限连续 3 日未达且 A6 无限额阻断 → `UTILIZATION_SHORTFALL` 进 A5（责任归 A3/A4，非 A6）。

**A5 复盘教练**：新增**团队记分卡** `team_scorecard`（每日 + 滚动 5/20 日）：NAV 与基准（沪深 300 / 中证 1000 同期）、使用率 vs 目标、槽位使用、入场命中率、回撤、各角色 KPI、"闲置资金成本"（有合格计划未占位期间的机会收益估算，仅作观察）。按 §13.2 把每项偏差归因到角色。

### 13.4 数据与联动增量
- 新表 `pm_decisions`（trade_date、session、deployment_target、slot_plan、overnight_stances、原始输出哈希）；`pm_override`（A6 否决记录）；`team_scorecard_daily`。
- 时序：08:30 A3 计划发布 → 08:33 引擎算 A6 上限信封 → 08:35 A6 晨会（收紧）→ 08:38 A3-PM 晨会（在信封内配置槽位与目标）→ 08:45 信封 + 槽位注入交易员上下文 → 09:26 晨审更新。15:10：A3-PM 隔夜去留 → A6 否决审 → 引擎标记次日动作 → A5。
- Node：`risk-premarket` 08:35（含 A6 与 A3-PM 两次模型调用，串行，预算 120s）；`risk-eod` 同样两次串行。
- 工作项：R12 A3-PM 提示词与晨会/收盘接线；R13 `pm_decisions / pm_override / team_scorecard_daily`；R14 A5 团队记分卡；R15 使用率下限告警。与 R1–R11 同一候选。

### 13.5 结论
- **资金使用率与槽位管理 → A3 组合经理**（主责）。
- **日内仓位管理 → A4 交易员**；**隔夜去留 → A3 组合经理**，A6 只能向保守方向否决。
- **风险限额与回撤 → A6 风控师**，并被考核"是否过度收紧"。
- **A5** 出团队记分卡，把模拟盘表现逐项归因到角色；Tony 看记分卡裁定人事（提示词/权限调整）。
