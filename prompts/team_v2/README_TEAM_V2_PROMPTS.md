# Team V2 提示词包 · 2026-10-15（Claude 统一给出，Codex 按此接线）

目录 `prompts/team_v2/`，接线前不替换现有生产提示词；周日候选内由 `STAGE_PROMPT_FILES` 等切换。

| 文件 | 角色 | 字节 | 替换 | 接线/校验改动（Codex 必做） |
|---|---|---|---|---|
| 00_shared_system_v3.txt | 团队章程 + 事实纪律 | 8.0K | 00_shared_system_v2 与 agent_2_transport_system_v1（A2 不再用独立前置） | `common.py:_prepare_stage_request` 统一前置；删除末尾英文 TOP8 追加段；信封数值字段服务端回填 |
| agent_1_macro_chain_v3.txt | 宏观产业链研究员 | 4.1K + v2 正文 | agent_1_macro_chain_v2 | 按文件内"修订清单 R1–R6"合成 v3 完整文件（保留 v2 各节）；schema 加 catalysts/falsifiers/handoff/opposing_refs；PRIOR_OUTCOME_FEEDBACK 方法级注入（<10 日 INSUFFICIENT） |
| agent_2_theme_sentiment_v3.txt | 题材情绪策略师 | 7.7K | agent_2_theme_sentiment_transport_v2 | 删除 `_a2_theme_reasons` 对 score_breakdown/theme_score 的校验，改为服务端按 THEME_SCORE_WEIGHTS 对冻结事实确定性计算；新增占位 SELECTED_BOARD_SUMMARY、A2_QUANT_FILTERED_SAMPLE、EVIDENCE_CATALOG；model_nominations 写研究表 + 标签，不进 A3/客户卡 |
| agent_3_portfolio_planner_v4.txt | 交易计划官兼组合经理 | 8.6K | agent_3_technical_planner_v2 | PLAN_REVIEW 盲审：A3_FACT_SHEET 投影去掉 eligibility/met/unmet/review_status；服务端事后比对写 model_judgments；新增 PM_PREMARKET / PM_POST_CLOSE 两会话（risk-premarket 08:38 / risk-eod）；pm_decisions 表 |
| agent_4_trader_v1.txt | 交易员 | 5.4K | agent_4_intraday_veto_v3 | 5m 节拍、30s 预算、TECH_REFERENCE 投影（现确定性链 met/unmet）、trader_guardrails、trader_signals 表、LIANGJIAN_A4_TRADER_MODE；A6 裁决同步调用 |
| agent_5_daily_reviewer_v2.txt | 复盘教练 | 6.1K | agent_5_daily_reviewer_v1 | v1 规则迁出映射：目录引用/编码解码/枚举/数量只引用 metrics/不生成字段 → schema 校验器与 context 渲染；新增 a6/paper_broker/model_judgments 事实段；team_scorecard/method_feedback/hypotheses → replay-queue |
| agent_6_risk_governor_v1.txt | 风控师 | 5.7K | （新） | risk_engine 信封/裁决；三会话接线；失效保守路径；pm_override 记录 |

归档：agent_2_theme_sentiment_v2、agent_4_intraday_signal_v2、agent_5_review_calibrator_v2 → `prompts/_archive/`。

## 共同契约
- 合同层级（0194 裁定）：
  - 输出级（根对象）：`handoff_notes` 与 `what_would_change_my_mind` 必填非空；所有角色、所有会话。
  - 判断级（每个决策项）：`supporting_refs`（1–3）与 `opposing_refs`（1–3）必填；A5 的 `evidence_ids` 即 supporting_refs。
  - 哨兵：`opposing_refs == ["NO_OPPOSING_EVIDENCE_FOUND"]` 仅当同一项含 `opposing_search_scope`（≥2 个 EVIDENCE_CATALOG.categories 类别名）时合法；服务端封存查找范围到 model_judgments，并按角色统计哨兵率（A5 记分卡项）。
  - 校验器对缺项/空数组/目录外 ID 判 INVALID → 走该层既有降级路径（A2_REVIEW_DEGRADED / A3 DATA_GAP / A4 NO_ACTION / A6 保守信封 / A5 DEGRADED）。
- EVIDENCE_CATALOG 注入结构：`{"categories": ["MARKET","SECTOR","STOCK_TECH","FUNDAMENTAL","NEWS","RISK","ACCOUNT",...], "items": [{"id":"...","category":"...","summary":"..."}]}`。
- A3 候选域 = A2 FOCUS + WATCH_ONLY（均为技术送审来源）；仅 model_nominations 不进 A3。
- EVIDENCE_CATALOG：每层注入的可引用 ID 目录；引用不在目录内 → INVALID。
- 模型判断全部进 `model_judgments`（decision_id/plan_id 唯一、原始输出与解析状态不可变、T+1/T+3 独立列），不混入有效入场 cohort。
- prompt_version：shared 3.0.0 / A1 3.0.0 / A2 3.0.0 / A3 4.0.0 / A4-trader 1.0.0 / A5 2.0.0 / A6 1.0.0；envelope 原样回填。
- 体积：角色专属 ≤12KB（A1 ≤24KB 合成后），共享另计。

## 干跑要求（发布门）
10-14 / 10-15 / 10-16 冻结快照 + 真实模型：七份提示词 JSON 全部合法；A2 FOCUS/REJECT/主题集合、A3 PASS 集合与生产原响应逐字段 diff；A4 交易员六日 5m 回放；A6/PM 晨会与收盘会话各三日；差异逐项说明，不自称行为中性。
