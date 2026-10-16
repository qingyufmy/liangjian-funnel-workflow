# A3 收盘盲审接口切片

实际采集时刻：2026-10-17 04:58（Asia/Shanghai）。依据 master WP-A/Claude A3 4.0.1 原提示词；非发布批准请求。

## 改了什么

新增 `pipeline/team_v2_plan_review_schema.py` 和 `runtime/team_v2_plan_review.py`，供自然研究适配器及正式干跑调用。本轮不改 Claude 提示词、不切换原 ResearchPipeline、不写 execution_plans、不重跑研究、不通知客户。

逐候选精确覆盖 A2 FOCUS + WATCH_ONLY 的冻结域，MODEL_NOMINATION 来源不能送审。模型不能改策略、簇或放宽原风险单位，不能输出新价格；PASS 必须有简报、证伪条件和本批次连续唯一展示排名。非 PASS 必须 NO_ENTRY。根信封、交接、正反方目录引用和哨兵规则严格校验；多字段、遗漏、重复、串绑均 INVALID，不做自动修复。rejected_candidates 若非空，使用非PASS判断结构；目前测试用明确的 secondary DATA_GAP，不凭模型拒绝补造事实。

完整原始 engine_decisions、技术事实、上下文、目录、提示词/请求绑定先在 owned Team 库封存。模型仅收到显式月/周/日技术字段投影和 A2 来源/策略标识；不含服务端资格、门结果、满足/未满足列表、发布/执行结论和排名结论。底层测量值与原价格合同保留，原源对象不变。引用目录必须由后续自然入口按这些合法事实构建，当前组件不声称已完成该入口。

真实客户端 → 严格 schema → model_judgments → owned team_plan_reviews 已用 HTTP mock 跑通。原输出合法与否均留模型研究账本，T+1/T+3 保持 NULL。盲审之后才生成引擎/模型逐股比较：engine=false/model=PASS 保留分歧，但 engine_and_model_pass=false；比较表本身 publication_authorized=false，不是计划发布权限。失败产生 DATA_GAP，不默认 PASS。

先 claim 源、再调用模型、再提交结果；提交故障后复用原 capture，不再 HTTP。模型请求失败也保持失败，不隐式重试。相同 review_id 的后来事实、引擎结论或风险域改变会冲突，不悄悄复用结果。

## 红绿证据

| 回执（artifacts/team-v2-20261016） | 结果 | SHA256 |
|---|---|---|
| 20261017T045404894796-a3-plan-strict-baseline-red-receipt.json | 旧通用校验器18失败/1正对照通过/exit1 | cbb10e2123dbafa52fb2049c5cb75b28f6c90ca6786b1693d7269e83b7d10e84 |
| 20261017T045745801253-a3-plan-final-associated-receipt.json | 162关联通过/0失败/exit0 | 86193012dae68c73037325c45d90f8296300fb7b4683e513964250909d079927 |

关联范围：本切片 schema/真实客户端mock/故障恢复 + 共享合同/模型捕获 + PM schema/session + A5逐事件ID。不是最终全量，不与旧集合相加。没有 VM、真实模型或生产调用；客户 A5 和客户发送器无改动。

## 四层与下一步

CODE：本切片通过。REPLAY：只有隔离反例，正式六日未完成。OPERATIONS：未部署，A1/A2/A3 自然研究适配器仍未接通。STRATEGY：收益未证。下一步补原冻结事实→合法目录/盲审请求→原引擎输出的自然适配与逐字段diff，A2确定性主题分和A1方法反馈仍待接线；不把组件绿当成七角色真实干跑通过。

0259 收盘15:00原bar结算红件仍独立待裁定，不受本162绿覆盖。H1独立周日发布安排不变。
