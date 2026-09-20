# S00—S11 最终验收状态

事实截止：2026-09-20。本文件只描述隔离开发分支证据，不代表生产发布。

| 维度 | 状态 | 结论 |
| --- | --- | --- |
| CODE | `CODE_ACCEPTED` | S00—S11 的离线契约、负例、Python/Node 回归、类型检查和构建通过；详见 `ITERATION_STATE.json` 与验收产物。 |
| REPLAY | `PENDING_READONLY_PRODUCTION_EVIDENCE` | 忠实/补数/事后模型三类回放代码已分离；当前只有测试 fixture，没有获授权的真实只读证据包，不能标为 `REPLAY_ACCEPTED`。 |
| OPERATIONS | `PENDING_PRODUCTION_EVIDENCE` | 本机压测、故障反例和 SQLite 副本迁移演练通过；尚无目标 VM 连续五个完整交易日影子证据，不能标为 `OPERATIONS_ACCEPTED`。 |
| STRATEGY | `INSUFFICIENT_EVIDENCE` | 没有用少量样本推断胜率、年化或参数优劣；当前实现只具备可信评价和提案能力。 |
| DEPLOYMENT | `DEPLOYMENT_APPROVAL_REQUIRED` | 未 push、未合并、未部署、未重启、未写生产数据库、未发真实通知、未下真实订单。 |

最终本机回归证据：Python `1901 passed, 4 skipped`，总覆盖率 `79.05%`；新增 S10 核心模块分支覆盖率 `98%`，新增 S11 核心模块分支覆盖率 `95%`；Node 类型检查、`91 tests` 和 Web/Server build 通过；统一离线入口 `229 passed`、退出码 0。测试明细见 `artifacts/iteration/s11-regression-20260920/` 与 `artifacts/iteration/s11-offline-20260920/`。

## 已完成

- S00 基线与统一离线入口；S01 决策可观测契约；S02 来源治理；S03 A4 关键等待链和持仓保护隔离。
- S04 A1 字段覆盖/缺口/补齐账本；S05 成交因果、费用和分钟容量；S06 组合预占、T+1、订单生命周期。
- S07 严格模型审核；S08 前后端统一语义；S09 新源旁路与授权门；S10 分层评价与策略变更治理。
- S11 证据包、迁移副本演练、影子隔离、故障与稳定性报告，以及发布/回滚手册。

## 仍需真实证据

1. A1 真实冻结快照的字段覆盖率、缺口补采吞吐和积压恢复。
2. 严格 LLM 审核在真实历史隔离回放中的时限、覆盖、否决和账户替代效应。
3. 新补充数据源的许可、上游身份、PIT、覆盖、时效与语义对照；当前全部禁用。
4. 真实 AS_OBSERVED 证据包上的回放，以及补数差异逐对象解释。
5. 目标 VM 至少五个完整交易日的影子稳定性和七类故障恢复证据。

## 下一条具体操作

由用户决定是否授权“只读导出生产证据包”。若授权，先按 `IMPLEMENTATION_RUNBOOK.md` 第 3 节显式导出，不部署代码；随后运行 replay profile 并更新 REPLAY 状态。生产发布仍是之后独立决策。
