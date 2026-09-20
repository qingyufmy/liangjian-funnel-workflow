# 经当前代码复核的发现

状态含义：`REPRODUCED` 为已有可运行反例；`CONFIRMED_STATIC` 为当前调用路径直接证明；`NEEDS_RUNTIME_EVIDENCE` 为需生产副本或只读实测；`ALREADY_FIXED` 为旧意见已不适用。

| ID | 状态 | 当前证据 | 影响与后续 |
| --- | --- | --- | --- |
| A4-01 | REPRODUCED_FIXED | 基线在采集 deadline 建立前刷新市场状态；S03 改为轮次入口创建绝对 deadline，并传入恢复、行情、模型与 lane | 离线边界已验收；生产延迟仍待 OPERATIONS 证据 |
| A4-02 | REPRODUCED_FIXED | 基线合并决策和归档标的并整批等待；S03 先处理持仓，归档范围显式延期 | 风险不再等待归档/辅助；历史归档完整率需运维观察 |
| A4-03 | REPRODUCED_FIXED | 基线同 worker 顺序取 1m、辅助 5m、quote；S03 使用分级 gate，原生 5m 不进入执行链 | 关键链只用冻结 1m 派生周期；原生 5m 保留后续审计职责 |
| A4-04 | REPRODUCED_FIXED | 基线无界 `as_completed`/executor 退出等待；S03 有限 daemon gate 在超时后返回终态并保留背压 | 不宣称能强杀第三方调用；泄漏 worker 由固定槽位限制 |
| A4-05 | ALREADY_FIXED | `workflow.py:4104-4108` 明确原生 5m 仅为审计，执行 5m/15m 从冻结 1m 派生 | 不再把原生 5m 冲突当隐藏执行输入；仍需 provider 治理 |
| EX-01 | REPRODUCED_FIXED | 基线把佣金和卖出税合并后整体套最低佣金；S05 改为 Decimal 订单级佣金累计与独立卖出税/其他费用 | 分单、部分成交、临界金额反例通过；生产实际费率仍需 OPERATIONS 配置证据 |
| EX-02 | REPRODUCED_FIXED | 基线把 `_quote_risk_bar` 传给 PaperBroker 并标为完整分钟；S05 明确 `SYNTHETIC_QUOTE`，只以冻结真实 1m 结算 | 合成报价仍可触发持仓风险意图，但不能证明成交或容量 |
| RISK-01 | REPRODUCED_FIXED | S05 冻结订单因果与费用；S06 增加原子风险预占、订单事件、持仓批次、T+1 释放、公司行为版本和账本审计 | 离线反例通过；主题/组合新增上限默认关闭，待影子统计和授权后才能成为生产门 |
| LLM-01 | REPRODUCED | `workflow.py:5629` 使用 `bool(signal.get("llm_veto", True))`，字符串 `"false"` 会变成 True | 严格 schema、类型、完整性和重复项校验留到 S07 |
| A1-01 | REPRODUCED_FIXED | S04 已建立字段覆盖投影，冻结输入和 packet 可逐层对账；821 样本、失败分母、PIT、预算投影和代次门反例通过 | CODE 离线通过；真实 VM 冻结样本覆盖率和积压清空能力仍待 OPERATIONS 证据，未宣称生产缺口已补齐 |
| SRC-01 | REPRODUCED_FIXED | 基线只有 `live_fetch._NODE_LOCK` 和节点 JSON 健康文件，无法在不同能力/进程间共享配额或 fencing；S02 已增加 RuntimeStore 协调表和 typed governor | `SRC-01—SRC-08` 离线反例通过；A4 实际接入属于 S03，生产恢复能力仍待运维证据 |

S00 没有改变上述业务行为。表中的行号基于基线提交；后续修改后以符号和 Git diff 为准。
