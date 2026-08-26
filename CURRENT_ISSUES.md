# 当前项目问题与风险清单

审计时间：2026-08-25 15:38（Asia/Shanghai）  
审计范围：当前本地代码、配置、Windows 计划任务、SQLite 状态、2026-08-25 最新快照及真实运行产物。  
项目状态：`OPERATIONAL_FAIL_CLOSED / NOT_END_TO_END_ACCEPTED`——失败关闭机制有效，但当前不能视为“已稳定完成每日三层分析和模拟交易闭环”。

## 1. 结论摘要

当前最需要解决的不是基础数据采集或单元测试，而是以下运行正确性问题：

1. 2026-08-25 15:10 收盘任务的三个研究模型全部在 A1 阶段失败，A2、A3 均未执行，没有生成任何候选计划。
2. 每分钟监控任务与 15:10 收盘任务在同一时刻调用同一个 `run-due`，形成租约竞争；Windows 任务结果和业务运行结果可能显示为成功，但实际研究已被阻断。
3. 生产调度只按“周一至周五”判断交易日，没有使用沪深交易所/同花顺交易日历，法定节假日仍会错误触发研究和模型请求。
4. 09:25 才开始全量取数和三模型分析，真实早盘运行超过 09:40 发布截止时间，未来即使收盘生成计划也可能无法在早盘激活。
5. 普通 `SELL_SIGNAL` / `REDUCE_SIGNAL` 没有进入模拟撮合，当前模拟离场链不完整。
6. 方案要求的独立 `RiskGovernor`、持久化 `PositionRiskPlan` 和公司行动持仓调整仍未实现。

此外，产业链收入传导、行业利润、板块历史/资金流和完整持仓拥挤度仍无正式事实源，因此即使模型调用恢复，也可能因提示词所需证据不足而无法生成有效单子。

## 2. P0：会直接阻断完整工作流的问题

### P0-1 三个研究 lane 的最新真实收盘运行全部阻断

最新真实报告：`outputs/research/research_2026-08-25-close-461f831cd2f0.md`。

| Lane | 模型 | A1 结果 | 原因 | A2/A3 |
|---|---|---|---|---|
| lane_1 | `deepseek-v4-pro-0813` | `BLOCKED` | `STREAM_CHOICES_INVALID`，重试 3 次 | 因上游阻断未执行 |
| lane_2 | `moonshotai/kimi-k3-free` | `BLOCKED` | `NETWORK_RETRY_EXHAUSTED`，重试 3 次 | 因上游阻断未执行 |
| lane_3 | `z-ai/glm-5.3-free` | `BLOCKED` | `NETWORK_RETRY_EXHAUSTED`，重试 3 次 | 因上游阻断未执行 |

影响：

- 本次运行总状态为 `BLOCKED`。
- 没有 A1 候选池、A2 二次筛选、A3 计划，也没有次日可进入早盘复核的计划。
- 三个虚拟账户仍各为 1,000,000 元，计划数、有效盯盘事件数和成交数均为 0。

需要注意：2026-08-25 12:53 的四模型最小能力探针全部返回 HTTP 200，只能证明小请求、基本思考字段和网关端点可用，不能证明完整 A1 大上下文可以稳定完成。当前“探针 PASS、真实任务 BLOCKED”是已确认的验收缺口。

可能原因与下一步验证：

- DeepSeek 返回了当前严格 SSE 解析器不接受的事件结构；现有错误码没有记录脱敏后的事件字段结构，暂时无法区分网关错误事件、兼容性元数据还是实际畸形响应。
- Kimi/GLM 是网络/超时重试耗尽；完整 A1 输入明显大于能力探针，需分别记录连接、首字节、流式总耗时和脱敏后的最终异常类型。
- 应增加“完整真实提示词规模”的探针或录制回放测试；最小探针不能继续作为端到端可用证据。

相关代码：`src/liangjian_funnel/pipeline/model_client.py:314`、`src/liangjian_funnel/pipeline/model_client.py:350`、`src/liangjian_funnel/workflow.py:436`。

### P0-2 15:10 存在两个计划任务竞争，并可能掩盖真实失败

当前三个 Windows 计划任务都执行同一个 `scripts/run_due.ps1`：

- `LiangjianAStockResearchClose` 在 15:10 启动。
- `LiangjianAStockMonitor` 从 09:25 每分钟重复 5 小时 45 分，也会到达 15:10。
- 监控任务配置了 `StopAtDurationEnd=true`，会在 15:10 到期停止。

今天的真实证据：

- 15:10 调度日志出现 `close_1510 ... LEASE_BUSY`。
- `LiangjianAStockResearchClose` 的 Windows 最近结果为 `0`。
- `LiangjianAStockMonitor` 的 Windows 最近结果为 `267014`，不再是文档所写的 `0`。
- 尽管 Windows 任务结果看起来没有明确报出研究失败，最终研究报告实际为 `BLOCKED`。

根因有两层：

1. 安装脚本让监控重复周期恰好覆盖到收盘任务启动时刻，两个进程同时争抢 `scheduler:close_1510` 租约。
2. `run-due` 只在调度记录为 `FAILED` 或 `MISSED` 时返回非零；研究回调正常返回一个 `status=BLOCKED` 的结果时，调度仍记录为 `DISPATCHED`，PowerShell/Windows 任务因此可能返回 0。

影响：

- 运维看到“任务成功”并不能证明 A1→A2→A3 成功。
- 监控任务在收盘边界被强制终止，存在日志未完整写入或子进程生命周期不清晰的风险。
- 关闭任务的真正执行者不固定，排查失败时难以从 Windows 任务历史直接定位。

相关代码：`scripts/install_scheduled_tasks.ps1:15`、`scripts/install_scheduled_tasks.ps1:38`、`scripts/install_scheduled_tasks.ps1:41`、`src/liangjian_funnel/workflow.py:520`、`src/liangjian_funnel/cli.py:157`。

### P0-3 调度器没有接入真实 A 股交易日历

`Scheduler` 未注入交易日函数时，默认仅使用 `weekday() < 5`。当前 `WorkflowApplication.run_due()` 创建调度器时没有传入同花顺或交易所日历，因此：

- 周末可以跳过。
- 工作日内的春节、国庆等休市日会被误判为交易日。
- 休市日仍可能采集数据、消耗模型额度并生成无意义的失败报告。
- 模拟账户的“开始新交易日”也可能在非交易日触发。

虽然同花顺能力探针已经验证交易日历接口可用，但该能力尚未进入生产调度链。

相关代码：`src/liangjian_funnel/runtime/scheduler.py:94`、`src/liangjian_funnel/workflow.py:520`、`src/liangjian_funnel/probes/hithink.py:57`。

### P0-4 早盘全量流程与 09:40 发布截止时间不匹配

方案中提到的盘前预取没有对应 Windows 任务；安装脚本实际上只有 09:25、15:10 和从 09:25 开始的分钟任务。当前早盘任务要在 09:25 后完成全市场、候选财务、分钟线、公告/PDF、政策、资讯以及三模型 A1→A2→A3。

今天最新早盘冻结快照直到 10:13 才形成，而代码在 09:40 后硬拒绝早盘计划发布：

- 真实早盘产物：`outputs/runs/2026-08-25-morning-b42ba164cd93.json`。
- 发布截止：`src/liangjian_funnel/workflow.py:750`。
- 实际任务定义：`scripts/install_scheduled_tasks.ps1:16`。

影响：即使前一晚产生 `PENDING_MORNING_REVIEW`，当前耗时结构也可能导致早盘 tighten-only 结果无法在截止前激活。需要把可缓存事实移到盘前预取，09:25 后只冻结竞价/最新行情并完成收紧复核，或重新设计可证明满足时限的分段任务。

### P0-5 普通卖出与减仓信号没有进入 PaperBroker

监控层能够产生 `SELL_SIGNAL`、`REDUCE_SIGNAL` 和强制退出，但 `_settle_prior_signals()` 目前只处理：

- `BUY_SIGNAL`
- `ADD_SIGNAL`
- `FORCED_RISK_EXIT`

普通 `SELL_SIGNAL` 和 `REDUCE_SIGNAL` 会被直接跳过，见 `src/liangjian_funnel/workflow.py:839`。这意味着模型给出的正常离场/减仓建议不会产生模拟成交，持仓主要只能依靠强制风险退出离场。

当前测试也没有覆盖“SELL/REDUCE → 下一闭合 Bar → PaperBroker 成交”的端到端路径。该问题会直接扭曲持仓周期、模型收益比较和复盘结果。

### P0-6 风险治理与持仓风险计划仍是方案项，不是已实现能力

最终方案要求独立 `RiskGovernor` 负责风险预算、仓位上限、T+1、加仓规则、强制退出和对账，并要求每次入场持久化 `PositionRiskPlan`。当前源码中没有对应实现，`config/funnel_config_v2.yaml:503` 也仍把相关 Agent 标记为 `PLANNED`。

当前实际路径是工作流把有效监控事件直接交给 `PaperBroker`。同时，没有发现除权除息、送转、配股等公司行动对模拟持仓数量和成本进行版本化调整的逻辑。

影响：

- 方案中定义的风险权限边界尚未真正落地。
- A3 计划过期、股票退出研究池后，持仓缺少独立持久化风险计划保证。
- 公司行动可能把价格跳变错误解释为盈亏或止损。
- 现有 PaperBroker 规则不能等同于完整 RiskGovernor 验收。

## 3. P1：即使模型恢复，也会限制筛选和出单的问题

### P1-1 提示词要求的关键事实仍未接入

最新 15:11 冻结快照中以下事实明确不可用：

| 事实 | 当前状态 | 原因 |
|---|---|---|
| `INDUSTRY_PROFIT_DATA` | 不可用 | `SOURCE_NOT_CONFIGURED` |
| `EXISTING_CHAIN_GRAPH` | 不可用 | `SOURCE_NOT_CONFIGURED` |
| `CAPITAL_FLOW_SNAPSHOT` | 不可用 | `SOURCE_NOT_CONFIGURED` |
| `FUND_HOLDINGS` | 不可用 | `SOURCE_NOT_CONFIGURED` |
| `THEME_REGISTRY` | 不可用 | `SOURCE_NOT_CONFIGURED` |
| `RESEARCH_CONSENSUS` | 不可用 | `SOURCE_NOT_CONFIGURED` |
| `SECTOR_CYCLE_SNAPSHOT` | 不可用 | 缺同花顺行业指数历史和板块资金流 |
| `CROWDING_SNAPSHOT` | 仅局部代理 | 只有龙虎榜和关注热度，不是完整基金/融资/持仓拥挤数据 |

影响：

- A1 无法可靠完成“政策/产业链/行业利润向公司收入贡献的传导验证”。
- A2 的行业周期、板块资金和拥挤度判断只能降级或失败关闭。
- 当前没有单子不只是模型故障；事实源补齐前，严格提示词本身也可能正确地拒绝全部候选。

这些字段目前由通用占位值补齐为 `available=false, reason_code=SOURCE_NOT_CONFIGURED`，见 `src/liangjian_funnel/workflow.py:555` 和 `src/liangjian_funnel/workflow.py:693`。

### P1-2 最新快照仍有候选级数据降级

15:11 最新快照原计划最多选择 20 只，最终只冻结 18 只，已记录两个候选的源失败：

- `600183.SH`：同花顺 `INCOME`、`INDICATORS` 请求被限流。
- `688825.SH`：mootdx 返回 `INSUFFICIENT_BARS`。

当前处理是失败关闭并排除对应候选，安全性正确；问题在于运行覆盖率下降，且没有自动补入排序靠后的候选以维持配置的 20 只研究规模。长期存在限流或次新股历史不足时，实际研究池会小于预期。

### P1-3 A4 盯盘和模拟成交尚无真实有效计划验证

当前状态：

- `ACTIVE_TODAY=0`。
- `PENDING_MORNING_REVIEW=0`。
- `effective_event_count=0`。
- 三个账户余额与初始资金完全相同。

因此，A4 Flash 每分钟否决、触发确认、T+1、滑点、费用、下一闭合 Bar 撮合等功能目前主要由离线测试证明；尚没有由真实 A1→A2→A3 计划驱动的完整 Shadow 闭环证据。不能把“每分钟任务在运行”解释为“盯盘模型与模拟成交已经真实验收”。

### P1-4 Windows 任务依赖交互式登录和供电状态

当前三个任务均显示 `Logon Mode: Interactive only`，并且配置为电池模式不启动、切换到电池时停止。这意味着：

- 用户未登录时任务可能不运行。
- 电脑睡眠、关机、断网或电池供电会造成漏跑。
- 当前没有独立常驻服务或远端运行节点保证无人值守连续性。

对个人本机试运行可以接受，但不满足长期无人值守模拟的可靠性要求。

### P1-5 A4 实际输入没有满足盯盘提示词的数据合同

A4 提示词要求实时报价/盘口、1m/5m/15m 闭合 Bar、实时均线、VWAP、板块上下文和真实可交易状态。当前运行时注入的数据是：

- `REALTIME_QUOTE`：最新一根 1m Bar，不是完整实时报价/盘口。
- `CLOSED_BARS`：仍是同一根 1m Bar，没有独立的 5m/15m 序列。
- `REALTIME_MA.available=false`。
- `SECTOR_CONTEXT.available=false`。
- 只要取得 Bar 就硬编码 `tradable=true`。

相关代码：`src/liangjian_funnel/workflow.py:789`。在这个状态下，即使 Flash 被调用，也不能视为已按提示词完成有效盯盘；严格执行时更可能因 `MA_DATA_MISSING` 或上下文缺失而否决。

### P1-6 Flash 的 429 当前不会重试

研究模型客户端默认可以进行多次尝试，但监控模型客户端被显式配置为 `max_attempts=1`，见 `src/liangjian_funnel/workflow.py:98`。因此 Flash 收到一次 429 或 5xx 后就会直接耗尽，不符合“429 直接重试、未来调度不熔断”的要求。

此外，模型侧当前退避是本地固定策略，没有读取服务端 `Retry-After`。需要在不设置跨时段熔断的前提下，为单次分钟任务设置能满足 60 秒节拍的有界重试预算。

### P1-7 调度租约在进程被强杀后不能按 TTL 重跑同一任务

租约默认 TTL 为 90 秒，但状态库先检查 `last_dispatch_key` 是否相同，再判断租约是否过期。相同逻辑任务在进程崩溃后，即使 TTL 已过，也会永久返回不可获取。

相关代码：`src/liangjian_funnel/runtime/state.py:847`、`src/liangjian_funnel/runtime/scheduler.py:121`。

现有测试覆盖“回调抛异常后主动释放租约”，没有覆盖“进程取得租约后被强制终止、来不及释放”。结合当前监控任务在 15:10 被 `StopAtDurationEnd` 停止的配置，这是实际恢复风险，不只是理论边界。

### P1-8 三个非空 A4 lane 串行执行，最坏耗时超过一分钟

`monitor_once()` 顺序遍历三个 lane，每个 Flash 请求上限 45 秒，单 lane Monitor 上限 50 秒。三个 lane 都有活动计划时，最坏耗时可超过 135 秒，无法保证一分钟节拍。

另外，MonitorEngine 每个 lane、每次调用都会重新创建；其进程内 overrun 冷却状态无法跨下一分钟保留。当前没有“三个 lane 均非空且连续运行”的真实负载验收。

相关代码：`src/liangjian_funnel/workflow.py:473`、`src/liangjian_funnel/workflow.py:496`。

## 4. P2：可观测性、文档和维护问题

### P2-1 `status` 不能回答“最近一次工作流是否成功”

当前 `status` 只输出账户、计划数量、有效事件数量和 SQLite 健康状态，不包含：

- 最近一次早盘/收盘 run 的总状态。
- 各 lane 最近失败原因和尝试次数。
- 最新快照的数据源失败项。
- Windows 计划任务最近结果和下一次运行时间。
- 最新模型/同花顺能力探针时间是否过期。

因此 `state_healthy=true` 容易被误读成完整工作流健康，实际上今天收盘任务三个 lane 全部 `BLOCKED`。

相关代码：`src/liangjian_funnel/cli.py:117`。

### P2-2 错误诊断信息不足

模型客户端正确地不保存思考正文和敏感响应，但当前也没有保留足够的脱敏结构诊断。例如 `STREAM_CHOICES_INVALID` 无法说明是：

- `choices` 字段缺失；
- 字段类型错误；
- 网关发送了独立错误事件；
- 网关发送了当前解析器未识别的合法元数据事件。

建议只保存安全的结构信息，例如事件序号、顶层字段名、字段类型、HTTP 状态、首字节/总耗时和网关 request id，不保存正文、认证头或思考内容。

### P2-3 文档存在状态漂移

当前文档中有几处已不符合最新现场：

- `IMPLEMENTATION_STATUS.md` 写监控计划任务最近结果为 0，当前实际为 267014。
- `PHASE1_PHASE2_ACCEPTANCE_REPORT.md` 仍把同花顺行业反查和新闻列为缺口，但后续实现已经补齐行业成员映射与开放资讯。
- 同一验收报告记录 204 项测试，当前完整测试为 221 项。
- README 的“已接通流程”容易被理解为已产生真实计划/盯盘事件，但当前真实运行仍停在 A1。

这些历史验收文档可以保留，但应明确标注“历史快照/已被后续状态取代”，并增加一个单一的当前运行状态入口。

### P2-4 本地产物没有备份与保留策略

当前状态库、分钟线缓存、事实快照、PDF、研究报告和日志都只保存在本机目录。项目内没有发现自动备份、日志轮转、磁盘额度或产物保留期配置。

长期每分钟运行后可能出现：

- 日志和快照持续增长。
- SQLite 或磁盘损坏后缺少可恢复副本。
- 清理历史数据时难以同时保持事实哈希、研究报告和模拟账本的引用完整性。

### P2-5 多股票持仓的总仓和权益不是按各自最新价盯市

PaperBroker 计算总仓上限时，使用当前待交易股票的 `fill_reference` 去估算全部已有持仓；持仓股票价格差异较大时，会高估或低估总仓。账户权益对其他持仓也主要使用平均成本，而不是各股票最新价。

相关代码：`src/liangjian_funnel/runtime/simulation.py:226`、`src/liangjian_funnel/runtime/simulation.py:353`。

这会影响新单是否通过、账户净值、风险预算和三个模型的收益比较。在多股票账户出现前必须修复并增加组合级盯市测试。

### P2-6 T+1 日切依赖早盘研究任务成功进入函数

目前只有 `run_research("morning")` 调用 `broker.start_trading_day()`。如果早盘任务因为关机、数据失败、租约或进程崩溃没有进入该函数，分钟监控仍可能继续，但前一交易日持仓不会正确解锁可卖数量。

相关代码：`src/liangjian_funnel/workflow.py:436`。日切应成为基于真实交易日、可重复执行的独立状态动作，或在当天首次 monitor/研究入口统一保证。

### P2-7 有效盯盘 Markdown 不是可重建的事务产物

监控事件先写 SQLite，随后再追加 Markdown。如果进程在两步之间退出，SQLite 已存在相同 event key，下一次会判重，但 Markdown 不会自动补写；直接 append 也不是原子发布。

相关代码：`src/liangjian_funnel/runtime/monitor.py:464`、`src/liangjian_funnel/runtime/monitor.py:506`。

SQLite 应作为权威账本，`effective_signals.md` 应由账本确定性、幂等地重建，避免用户最终文件永久漏项。

### P2-8 “只保留有效结果”的业务定义需要明确

当前有效事件除买卖信号外，还包括 `LLM_VETO`、`PLAN_INVALIDATED` 和 `DATA_BLOCK`。因此 `effective_signals.md` 虽然不会保存 `NO_ACTION`，仍会保存否决和数据故障。

如果“有效”指所有会改变计划/风险状态的事件，当前定义合理；如果用户只想保留可执行买卖和强制退出，则当前范围偏宽。需要在文档和输出标题中明确口径。

## 5. 测试覆盖仍缺少的关键路径

当前 221 项离线测试通过，但以下组合路径尚未覆盖或没有真实验收证据：

- 最新真实网关 SSE 事件结构，以及 `STREAM_CHOICES_INVALID` 的安全兼容处理。
- Flash 使用实际工作流配置时的 429 重试。
- 09:25 全量取数和三模型能否在 09:40 前发布。
- 沪深交易所法定节假日。
- 进程取得租约后被强杀的 TTL 恢复。
- `SELL_SIGNAL` / `REDUCE_SIGNAL` 到下一闭合 Bar 模拟成交。
- 多股票持仓的总仓和权益盯市。
- 公司行动、除权除息、停复牌进入模拟持仓。
- 三个非空 A4 lane 总耗时低于 60 秒。
- SQLite 已落事件但 Markdown 写入失败后的重建。
- 至少一个真实 lane 完成 A1→A2→A3→早盘复核→A4→完整买入和离场。

## 6. 当前已确认正常的部分

以下内容本次复核未发现异常：

- `.env` 能正常加载，两个密钥仅显示 present/not present，未写入报告。
- `doctor` 通过，核心配置、提示词路径、状态库和模型名正确。
- 当前 221 项离线测试全部通过。
- SQLite `state_healthy=true`，三个虚拟账户结构正常。
- 同花顺最小能力探针和四模型最小能力探针最近一次均为 PASS。
- 最新快照成功冻结 5,559 只全市场、5,343 只研究域、5,008 只沪深模拟域。
- 同花顺行业成员、巨潮公告/PDF、国务院政策、开放资讯、mootdx 分钟线及确定性 K 线聚合已经进入快照。
- 失败时没有绕过 A1→A2→A3 血缘，也没有产生虚假模拟单，fail-closed 行为有效。

## 7. 建议修复顺序

1. **先修交易闭环的确定性缺陷**：补 SELL/REDUCE 撮合、RiskGovernor/PositionRiskPlan、组合盯市和幂等交易日切。
2. **修调度正确性**：拆开 monitor/research 的任务入口，消除 15:10 竞争；修复崩溃租约恢复；让业务 `BLOCKED` 正确传递为任务失败；接入真实交易日历。
3. **重构早盘路径**：增加盘前预取/缓存刷新，证明 09:25–09:40 内只做必要冻结和 tighten-only 复核。
4. **修模型完整请求兼容性**：针对最新三种失败增加脱敏传输诊断，使用完整 A1 规模逐模型验证；为 Flash 增加节拍内有界重试。
5. **补齐 A4 与事实数据**：先补 A4 的多周期 Bar、MA/VWAP、板块与交易状态，再补产业链收入传导、行业利润、板块历史/资金流和完整拥挤度。
6. **做真实 Shadow 验收**：至少一个 lane 连续完成收盘计划、次日早盘复核、分钟盯盘、虚拟买入、减仓/卖出和 T+1。
7. **完善运维面**：扩展 `status`、增加失败告警、无人登录运行能力、Markdown 可重建机制，以及 SQLite/快照/日志备份和保留策略。

## 8. 当前是否可以长期自动运行

结论：**可以继续以 fail-closed 方式采集、冻结和观察，但暂时不应标记为“完整工作流正常运行”或“长期无人值守验收通过”。**

进入完整运行状态至少需要满足：

- 三个研究模型在真实完整输入下完成 A1→A2→A3，而非仅最小探针 PASS。
- 早盘与收盘任务无并发竞争，任何业务阻断都能在任务状态和告警中被看见。
- 调度使用真实 A 股交易日历。
- 早盘流程能在 09:40 前完成，且三个非空 A4 lane 能保持一分钟节拍。
- SELL/REDUCE、独立风险治理、组合盯市、T+1 日切和公司行动处理完成。
- 至少一次真实计划完成“收盘计划→次日早盘复核→分钟盯盘→虚拟买入→减仓/退出”的闭环。
