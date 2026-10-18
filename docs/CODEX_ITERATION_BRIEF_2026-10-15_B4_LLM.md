# B4-LLM 任务书 · 2026-10-15 晚 —— 让五个 Agent 成为一个交易团队

> 依据：Tony 2026-10-15 20:12 "每个 LLM 分析都要对策略和角色定义有积极的帮助……多个 agent 每个扮演一个角色，相互协作进行选股、择时、复盘，形成一个完整的交易团队。今天就执行修改迭代并持续跟踪。"
> 审查依据：docs/REVIEW_2026-10-15_LLM_PROMPTS.md（生产 2b077f3 的实际接线与提示词问题 P1–P6）。
> 约束不变：确定性引擎仍是最终裁决者；LLM 不得新增价格/仓位/订单权限；客户飞书通道/模板/文案不变；不盘中部署；凭证不进桥接；发布 16:35 后、无活动任务、outcomes 之后、次日 08:30 前。

---

## 0. 设计原则（所有提示词改写共同遵守）

1. **角色而非过滤器**：每层提示词开头必须有"团队角色卡"：我是谁、我对谁负责、我交给下游什么、我需要上游什么、我被谁考核。
2. **每层必须留下可被下游使用的东西**：`handoff_notes`（给下一层的 ≤3 条提示）和 `what_would_change_my_mind`（≤2 条证伪条件）。空数组视为无效输出。
3. **盲审**：审查型角色（A3、A4）的输入中去掉服务端结论字段（eligibility / met_conditions / unmet_conditions / QUALIFIED / trigger 结果的 PASS 位），只给事实与证据目录；服务端事后比对，一致与否都记录。
4. **正反方必填**：任何判断同时给 `supporting_refs`（≤3）与 `opposing_refs`（≤3），只能引用证据目录 ID；只有一边视为无效输出。
5. **数值归引擎**：模型不输出 0–100 分、置信度、覆盖率等数值；只输出分类与证据 ID。共享信封中的 `data_quality_score / coverage / evidence_confidence` 由服务端回填。
6. **模型不改门**：所有改写不得改变确定性门控、阈值、候选数、计划上限；模型的新增输出（提名、优先级、假设）只写入研究字段与 A5，不进入 execution_plans。
7. **可校准**：每次模型判断（PASS/VETO/FOCUS/提名）都写入标签链路，T+1/T+3 可查；否决与放行同等记录。
8. **每个提示词 ≤ 12KB**；规则能用 schema/代码校验表达的，迁出提示词。

---

## 1. 团队角色定义（提示词改写目标）

| 层 | 角色名 | 对团队的积极贡献（新增） | 保留的硬边界 |
|---|---|---|---|
| 共享前置 | 团队章程 | 五个角色、交接合同、"谁考核谁"；确定性引擎 = 风控总监，拥有最终裁决 | 事实纪律 F1–F8、D1–D9 不变 |
| A1 | 宏观产业链研究员 | 每个主题/节点给 `thesis`、`catalysts`（带日期）、`falsifiers`；接收 A5 的方法级反馈（哪类证据反复出错） | 不出股价/仓位；强证据门不变 |
| A2 | 题材与情绪策略师 | ① 主题阶段 + 新开仓政策（保留）；② **模型提名观察池** `model_nominations`：量化已过滤但有题材/资金逻辑的股票 ≤5 只，只记录跟踪，不进 A3；③ 给 A3 的 `handoff_notes`（本主题最该盯的风险） | 删除 score_breakdown 主观打分；FOCUS/REJECT 规则不变；提名不改变任何池 |
| A3 | 交易计划官 | ① 盲审 + 正反方证据；② 对合格计划给 `plan_priority_rank`（研究优先级，只影响卡片展示顺序与 A5，不影响激活）；③ 给 A4 的 `handoff_notes`：这只票明天最可能的失效路径 | PASS/VETO/DATA_GAP 三态不变；不能促成；不改价格/路由 |
| A4 | 盘中风控官 | ① 否决理由收窄为枚举：`DATA_STALE / NOT_TRADABLE / PLAN_EXPIRED / PRICE_ABOVE_NO_CHASE / STRUCTURE_INVALIDATED / HALT_OR_ANNOUNCEMENT`；"板块或市场急变"改为必须引用 live_market_state 的具体字段；② 每次否决/放行记录 `decision_log`（理由码 + 证据 ID），T+1/T+3 回填 | 只有否决权；硬风险退出不等它 |
| A5 | 复盘教练 | ① `method_feedback`：按证据类型/判断方法聚合（不按个股），回灌 A1/A3 的 PRIOR_OUTCOME_FEEDBACK；② `hypotheses`：结构化假设（层、条件、预期效应、验证方式）进入离线回放队列；③ 对每层模型给 `model_value_assessment`（否决组 vs 放行组表现差异） | 不改生产；提案不自动生效 |

---

## 2. 分阶段执行

### Phase 0 · 今晚（候选 1，父 = 2b077f3，21:30 前送审，明早 08:30 前发布）
行为中性或纯收紧项，明天 B3 首日照常观察：

| # | 项 | 验收 |
|---|---|---|
| L0 | 共享前置加"团队章程"段（角色、交接合同、考核关系）；各层加角色卡；`prompt_version` 全部递增 | 冻结干跑 JSON 合法；确定性结论不变 |
| L1 | A2 提示词正文改为 TOP8 ∧ (当日>0 ∨ 3 日均值>0)，删除 `common.py` 末尾英文追加段；A2 transport 前置并入共享前置的 F/D 纪律 | 10-14/10-15 快照干跑：focus/watch/reject 集合与生产一致，差异逐只列出 |
| L4 | A4 否决理由枚举化 + `decision_log` 落库（plan_id、minute、llm_veto、reason_code、evidence_refs、judge 耗时）；"市场急变"须引用 live_market_state 字段 | 近 20 日冻结 A4 触发窗口重放：否决集合变化逐条列出；judge p95 不升 |
| L2 | 只读统计近 20 日五层模型调用：次数、耗时、PASS/VETO/DATA_GAP/FOCUS 分布、与确定性结论不一致率；输出 `artifacts/llm-audit-20261015/LLM_USAGE_20D.md` | 23:00 前交 |
| L7 | 归档三个死文件到 `prompts/_archive/`；信封数值字段改服务端回填 | 测试绿 |
| L8 | 模型判断标签：A3 PASS/VETO、A4 veto/pass、A2 FOCUS/提名 全部进入 B3 的标签捕获链路（复用 decision_capture），T+1/T+3 由 outcomes 回填 | 周五收盘可见标签行 |

### Phase 1 · 周五（候选 2，周五晚发布，下周一生效）
| # | 项 | 验收 |
|---|---|---|
| L3 | A3 盲审改造：输入去结论字段，正反方必填，`plan_priority_rank` + `handoff_notes`；服务端记录"模型 vs 确定性"一致率 | 六日冻结回放：PASS 集合变化逐只列出；若 PASS 数下降 >20% 先不发布，交 Claude |
| L9 | A2 去主观打分：`theme_score` 改服务端确定性计算（权重不变，分项由冻结事实映射）；模型只出阶段/政策/证据 | 10-12/13/14/15 四日干跑：选中主题集合不变或差异逐项说明 |
| L10 | A2 `model_nominations` 观察池：≤5 只，只写研究表 + A5，带 `nomination_thesis` 与 `falsifiers`；T+1/T+3 跟踪 | 不进 A3；不进客户卡片 |
| L11 | A1 加 `catalysts/falsifiers` 字段；接通 `PRIOR_OUTCOME_FEEDBACK`（方法级聚合，标签满 10 日才注入，否则注入"无反馈"显式占位） | A1 下次自然运行 JSON 合法 |
| L6 | A5 瘦身：40 条规则中可由 schema/校验器表达的迁出；新增 `method_feedback / hypotheses / model_value_assessment`；hypotheses 自动写入 `artifacts/replay-queue/` | 周五盘后 A5 含新字段 |

### Phase 2 · 下周（标签满 10 日后）
- 五层模型校准报告：否决组 vs 放行组 T+1/T+3；不一致率；提名池 vs 量化池表现。
- 据此裁定：A3/A4 模型保留 / 盲审常态化 / 降为注释；A2 提名池是否获得进入 A3 的资格（需 Tony 批准）。

---

## 3. 干跑与发布纪律（每个提示词改动必做）
1. 冻结干跑用真实模型调用（Tony 已知成本），输入 = 10-14、10-15 收盘冻结快照；输出与生产原响应逐字段 diff；差异清单进评审包。
2. `prompt_version` 递增且写入 envelope；旧版本提示词保留在 `prompts/_archive/`。
3. 解析失败 → 沿用 B2 的降级路径（A2_REVIEW_DEGRADED / A3 DATA_GAP / A4 不否决即放行？**否**：A4 解析失败按现有规则处理，不得默认放行）。
4. 全量 + VM 差异 + 前向回退（树 ≡ 2b077f3）+ 评审包 SHA；Claude ACCEPT 后 push + deploy.sh（P0 委托）。
5. 发布后 14 条 10-16 计划原行不变。

---

## 4. 周五观察点（叠加 B3 首日）
- L4 生效：A4 否决理由分布、decision_log 行数 = 触发数；judge p95。
- L1 生效：15:10 收盘 A2 过门数与 10-15 对照；FOCUS 数。
- L2 报告：各层不一致率基线。
- L8：标签行是否包含模型判断字段。

---

## 5. 发给 Codex 的正文
```
【任务书 B4-LLM · 2026-10-15 晚】以 docs/CODEX_ITERATION_BRIEF_2026-10-15_B4_LLM.md 为准，审查依据 docs/REVIEW_2026-10-15_LLM_PROMPTS.md。Tony 20:12 授权：今晚执行 LLM 部分修改迭代并持续跟踪。原则：确定性引擎仍最终裁决；模型不新增价格/仓位/订单权限；所有改写不改门控/阈值/候选数；每层输出必含 handoff_notes 与 what_would_change_my_mind；审查型角色盲审 + 正反方证据必填；模型不输出数值分。

Phase 0 今晚（候选父 2b077f3，21:30 前送审）：L0 团队章程 + 角色卡 + prompt_version 递增；L1 A2 正文改 TOP8 口径、删 common.py 英文追加段、transport 前置并入共享纪律；L4 A4 否决理由枚举化 + decision_log 落库 + "市场急变"须引用 live_market_state 字段；L2 近 20 日五层模型调用只读统计（23:00 前）；L7 归档三个死提示词、信封数值服务端回填；L8 模型判断进标签捕获链路。
验收：10-14/10-15 冻结快照真实模型干跑，与生产原响应逐字段 diff；近 20 日 A4 触发窗口重放否决集合差异；全量 + VM + 回退（树 ≡ 2b077f3）+ 包 SHA。

Phase 1 周五：L3 A3 盲审 + plan_priority_rank + handoff；L9 A2 主题分改确定性；L10 A2 模型提名观察池（研究用，不进 A3）；L11 A1 catalysts/falsifiers + PRIOR_OUTCOME_FEEDBACK 接通（方法级，满 10 日注入）；L6 A5 瘦身 + method_feedback/hypotheses/model_value_assessment + 回放队列。六日冻结回放；PASS 数降 >20% 先不发布。

Phase 2 下周：10 日标签后五层模型校准报告，裁定 A3/A4 模型去留与提名池资格。

明日 B3 首日观察不变（0184-reply §4）；叠加 L4/L1/L2/L8 观察点。
```

---

## 6. 追加（Tony 20:19/20:20）：A4 = 专业交易员；后续补 A6 风控师

> Tony 20:19："A4 我希望是专业交易员角色，根据前面的交易计划进行盘中的择时和风控判断入场、离场信号等。"
> Tony 20:20："后面我们会补充一个专业的 A6 风控师角色。"
> 这与原设计一致：归档文件 `agent_4_intraday_signal_v2.txt` 本就定义 A4 为"盘中交易信号员"，输出 BUY/ADD/SELL/REDUCE/CANCEL/BLOCK，并把仓位、账户、T+1 可卖数量交给 RiskGovernorAgent（Agent 6）。本节取代第 1 节表中 A4 行与 Phase 0 的 L4 定义。

### 6.1 A4 交易员角色定义
- 身份：盘中交易员。收到 A3 交易计划官的计划简报（几何、策略类型、失效路径、handoff_notes）后，在盘中对**已激活计划**做择时与风控判断，输出入场/离场信号。
- 动作集（沿用 signal_v2）：`NO_ACTION / START_CONFIRMATION / BUY_SIGNAL / ADD_SIGNAL / SELL_SIGNAL / REDUCE_SIGNAL / CANCEL_SIGNAL / BLOCK`。不输出股数、金额、仓位。
- 判断依据：闭合 5m/15m 结构、VWAP、1m 新鲜度、计划几何（入场带、禁追线、失效位）、live_market_state（当日）、板块实时强弱。现有确定性触发链（15m 卖压缓解 / 回踩缩量 / 5m 反转 / 5m 确认 / VWAP 收复）**改为交易员的参考证据**（逐项注入 met/unmet），不再是 AND 硬门；交易员须对未满足项逐条说明为何仍可入场或为何等待。
- 每个信号必含：`reason_code`、`supporting_refs`（≤3）、`opposing_refs`（≤3）、`what_would_change_my_mind`、`handoff_to_a6`（给风控师的仓位相关提示：建议风险单位 STANDARD/PROBE、是否分批、持有周期假设）。

### 6.2 确定性护栏（风控总监，不可被交易员越过）
入场类信号只有同时满足以下项才成为有效信号；否则记 `TRADER_SIGNAL_BLOCKED_BY_GUARDRAIL` 并保留原信号供校准：
1. 计划 ACTIVE 且未过期；行情/规则快照新鲜；可交易（非一字/停牌/涨停封板）。
2. 价格在入场带内且 ≤ no_chase；未触及硬止损/失效位；T+1 与交易时段规则。
3. 日内时间窗（TIGHTEN_AFTER / NO_NEW_ENTRY_AFTER）。
4. 同簇当日有效买入信号 ≤2（Tony 17:5x 裁定）。
5. 离场类：硬止损、失效位、涨跌停不可成交由引擎在 1m 路径直接执行，不等交易员。
交易员只能收紧，不能放宽任何 A3 几何。

### 6.3 节拍与预算
- 交易员每 **5 分钟闭合**调用一次（09:35 起，只对已激活计划，批量一请求），模型用 review_model（pro）；1m 路径保留确定性硬风险与数据安全。
- 预算：交易员调用 ≤ 30s；超时 → 本节拍 NO_ACTION（不默认放行）；monitor 58s 父预算不变。
- 替代现有每分钟 veto 调用（flash）；veto 逻辑并入护栏 + 交易员 opposing_refs。

### 6.4 上线路径（两种模式，Tony 选）
- **RECORD 模式**（建议先跑 5 个交易日）：交易员每 5 分钟产出信号并落库 `trader_signals`，与确定性触发链结果并排记录，**不投递客户、不产生有效信号**；A5 每日对比两条路径的信号数与 T+1/T+3。
- **CONTROL 模式**：交易员信号 ∧ 护栏 = 有效买入/卖出信号，投递客户卡片（模板不变，仅来源字段标 TRADER）。
切换由 Tony 在聊天批准；Claude 不得自行切换。

### 6.5 任务项（替换 L4，新增 L12–L14）
| # | 项 | 阶段 | 验收 |
|---|---|---|---|
| L4' | A4 veto 调用保留到交易员上线前；仅做理由枚举 + decision_log（同原 L4） | Phase 0 今晚 | 同原 L4 |
| L12 | 交易员提示词 `agent_4_trader_v1.txt`（≤12KB）：角色卡、动作集、证据目录、护栏声明、输出 schema（含 handoff_to_a6）；从 signal_v2 归档版改写，删除 A6 职责 | Phase 1 周五 | 六日冻结 5m 节拍真实模型回放：信号数、首信号时刻、与确定性链的一致/分歧表、T+1/T+3 |
| L13 | 交易员运行时：5m 节拍调度、批量请求、30s 预算、`trader_signals` 落库、护栏模块 `runtime/trader_guardrails.py`（含同簇 ≤2）、RECORD/CONTROL 开关（env `LIANGJIAN_A4_TRADER_MODE=record|control`，默认 record） | Phase 1 周五晚发布 | 周一起 RECORD；phase_timings 新增 trader 段；monitor p95 不升 |
| L14 | A6 风控师接口预留：`handoff_to_a6` schema + `risk_governor_stub`（透传 STANDARD/PROBE，不做仓位计算）；A6 正式角色另立任务书 | Phase 1 | schema 测试绿 |
| L15 | A5 新增"交易员 vs 确定性链"对比表与 `trader_calibration`（5 日后出 CONTROL 建议） | Phase 1 | 周一盘后含表 |

### 6.6 对第 1 节角色表的修订
- A3 交易计划官 → 其 `handoff_notes` 改为"交易计划简报"（给交易员）：最可能的失效路径、关键观察位、建议确认节拍。
- A4 行按 6.1 替换。
- A6 风控师（待建）：接收交易员信号 + 账户快照，决定仓位/分批/风险预算；A4 不可见账户。

### 6.7 Tony 裁定（20:2x，聊天）
tony: A4 交易员**下周一直接 CONTROL 模式接管**。
执行口径：
- L13 的 `LIANGJIAN_A4_TRADER_MODE` 周五晚发布时即设为 `control`；RECORD 模式保留为回退开关（切回即恢复现有确定性链 + veto 路径，不需重新部署）。
- 因此 L12/L13 的六日冻结回放是**发布门**而非参考：交易员在六日回放中必须 (a) 对每个激活计划分钟产出合法 JSON ≥ 99%；(b) 无一例越过 6.2 护栏（护栏拦截数单列）；(c) 分歧表完整；(d) 30s 预算 p95 达标。任一不达标 → 周一仍以 RECORD 起步，Claude 报 Tony。
- 周一 09:35 起 Claude 加密复核：09:40 / 10:10 / 11:40 / 14:10 / 16:40；首个 BUY_SIGNAL 需在 5 分钟内由 Codex 投递原始上下文 + 交易员输出 + 护栏结果。
- 客户卡片模板不变；来源字段标 TRADER。
