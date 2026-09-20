# 经当前代码复核的发现

状态含义：`REPRODUCED` 为已有可运行反例；`CONFIRMED_STATIC` 为当前调用路径直接证明；`NEEDS_RUNTIME_EVIDENCE` 为需生产副本或只读实测；`ALREADY_FIXED` 为旧意见已不适用。

| ID | 状态 | 当前证据 | 影响与后续 |
| --- | --- | --- | --- |
| A4-01 | CONFIRMED_STATIC | `workflow.py:3792-3798` 在采集 deadline 建立前刷新市场状态；deadline 到 `3827` 才创建 | 全局截止时间未覆盖市场刷新，S01-S03 修复 |
| A4-02 | CONFIRMED_STATIC | `workflow.py:3813` 合并决策和归档标的；`3848-3864` 等待整批 future；`4065-4077` 才执行持仓观察与结算 | 归档和慢标的可延迟持仓保护，S03 隔离 |
| A4-03 | CONFIRMED_STATIC | `workflow.py:3832-3845` 同一 worker 依次取 1m、辅助 5m、quote | 辅助源会先于风险报价占用关键链，S02-S03 调整 |
| A4-04 | CONFIRMED_STATIC | `workflow.py:3853` 与 `4165` 使用无 timeout 的 `as_completed`；线程池上下文退出还会等待 worker | deadline 传给 provider 不等于调用者终止等待，S02-S03 加有界收敛 |
| A4-05 | ALREADY_FIXED | `workflow.py:4104-4108` 明确原生 5m 仅为审计，执行 5m/15m 从冻结 1m 派生 | 不再把原生 5m 冲突当隐藏执行输入；仍需 provider 治理 |
| EX-01 | REPRODUCED | `runtime/simulation.py:160-162` 把佣金和卖出税合并后整体套最低佣金 | 小额卖出少计税；S05 增加反例并修正 |
| EX-02 | CONFIRMED_STATIC | `_quote_risk_bar` 构造合成 quote bar，`_settle_prior_signals` 将其传给 PaperBroker，同时账本写 `NEXT_COMPLETE_1M_BAR_SIMULATION` | 成交标签与真实输入语义不一致；S05 必须以冻结完整 1m 撮合 |
| RISK-01 | CONFIRMED_STATIC | `runtime/simulation.py:359` 用未来撮合价作为仓位 cap 的 `mark_price` | 初始委托数量没有完全冻结；S05/S06 划分冻结数量与成交缩减 |
| LLM-01 | REPRODUCED | `workflow.py:5629` 使用 `bool(signal.get("llm_veto", True))`，字符串 `"false"` 会变成 True | 严格 schema、类型、完整性和重复项校验留到 S07 |
| A1-01 | NEEDS_RUNTIME_EVIDENCE | 当前有 registry、事实缓存和增量路径，但尚无统一字段覆盖账本证明采集→解析→时点→特征→模型输入分母一致 | S04 建账本并用冻结样本对账；不得把缺生产证据记为通过 |

S00 没有改变上述业务行为。表中的行号基于基线提交；后续修改后以符号和 Git diff 为准。
