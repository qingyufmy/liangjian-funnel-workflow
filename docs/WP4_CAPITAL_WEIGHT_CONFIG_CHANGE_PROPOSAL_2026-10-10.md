# CONFIG_CHANGE_PROPOSAL：决策 11（待 Tony）

## 1. 已核实的当前规则

`data/a2_market.py:392–417` 逐股指数累加已观测窗口权重，只要求 today 存在，然后按 `weighted / available_weight` 输出。逐股权重 today=.35、3d=.25、5d=.25、10d=.15；板块复合指数是另一套 today=.5、5d=.3、10d=.2，不能混为一谈。

腾讯 today 单窗口也能通过现有逐股规则，因此 EASTMONEY 模式（它也先采腾讯）存在 DEGRADED_RENORMALIZED 值；这是源码与固定真实调用路径核验，不是本次生产逐行抽查。现行代码保留。缺历史并不自动证明分数错误，但它不能和全窗口同名而不说明覆盖。

## 2. 备选方案与建议

A：明确接受现行降级归一化。必须按行展示 FULL_WINDOWS / DEGRADED_RENORMALIZED、观测窗口和原权重；参数不变，但该政策仍需 Tony 确认。

B：正式指数只有四窗口齐全才可用，缺任一窗口输出 UNKNOWN；today 原始净额/净占比和百分位单独作为 evidence_only。它改变资本因子的可用性与 A2 候选，不能在本次悄悄应用。

建议：先取得同输入影子排名差集和五日覆盖，再决定 B 是否替换现行规则。不得为保持候选数量补历史、填中性分或换权重。

## 3. 当前切片与测试证据

`docs/WP4_CAPITAL_SOURCE_ISOLATION_2026-10-10.md`：LOCAL 模式禁东财逐股回退及六板块请求，原 today 单独留证、复合分 UNKNOWN；默认 EASTMONEY 路径不变。相关测试 `artifacts/wp5-20261010/capital-isolation-final-v4.xml` 57 passed，exit0，尚待新 HEAD 全量。

`inspect_legacy_capital_weighting` 只核验原包 hash 与公式，按行注解，不修改原包。真实独立影子采集/同输入 A2 排名 diff 尚未完成，不以 NOT_COLLECTED 充当已对照。

## 4. 影响和回滚边界

选择 B 需新的策略/因子合同版本、影子与生产可用性差集、单独发布批准；回滚到原版本不重写历史账本。此文件与桥接 gate 只提出决策，不改 env、默认路径、阈值、数量、通知或数据库。

CODE：局部隔离验证；REPLAY：固定注入输入，不是五日影子；OPERATIONS：未发布；STRATEGY：未批准政策。
