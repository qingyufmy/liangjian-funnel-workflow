# Team V2 隔离开发合同

本文件仅说明当前隔离开发合同，不宣称所有工作包完成或已投入运行。

## 边界

Claude 提供的角色正文和根/判断证据规范为唯一提示词来源。缺交接、证伪、真实目录引用即无效；反方哨兵必须附至少两个实际证据类别。解析失败不得放行。根会话 schema 与正式调用器已有组件接线；接线、真实模型、源回放、自然生产验收分别记录，不互相替代。

引擎拥有硬风险和股数裁决。模型只能在已封存信封内收紧、排序、择时；研究提名永不进入 execution_plans。数量、费用和资金计算全部 Decimal；布尔输入不允许字符串真值转换。

## 独立模拟盘

`liangjian_paper_broker` 不导入生产 RuntimeStore，不连接生产数据库或真实券商；`runtime.broker` 是唯一适配入口。100 万初始资本与风险 YAML 必须一致。撮合仅消费已完整到达的闭合分钟，价格无来源或单位不明不猜测。订单不能在信号所属分钟成交，不得使用未来分钟；共享成交量上限、涨跌停不可成交、T+1 和费用均是硬规则。

账本按信号/订单幂等，部分成交原子扣款/扣可售量。收盘 NAV 必须有当日 15:00 收盘标记，否则 DATA_LIMITED/NULL，不能以末次 VWAP 冒充。账户曲线可复现比较采用 canonical snapshot bytes，不宣称 SQLite 物理文件字节相同。

## 未完成的发布门

六日冻结源的连续有权交易链、三日各角色/会话/提示词 SHA 的真实模型门、最终候选全量、VM 切片、回退树及最终 Claude ACCEPT 尚未全部完成。A6/PM/trader 编排、model_judgments/标签、Node 调度和故障注入已有源码与组件证明，不把旧文档里的“尚未接线”继续当成当前源码事实，也不把 mock 当成自然生产验收。真实模型失败原件不得追认；逐次最新状态以 execution state 与实际回执为准。

H1 最终候选 f79abd4 与其独立回退 edac351 已单独验收，部署只在周日白天；本文件不构成发布批准。H1 生产验收后，Team 整包回退树必须等于实际 H1 生产 HEAD，不能撤销 H1。

## 发布模式（0277，覆盖初稿默认 CONTROL 的发布口径）

源码 `Settings.a4_trader_mode` 和环境变量缺省值仍为 `control`；未设置变量并不等于安全回退。正式发布配置必须显式记录 `LIANGJIAN_A4_TRADER_MODE`，并验收加载后的实际值。0277 的六日正确性 A 门不过时，发布选择只能为 `record`；单次合法研究探针或零模型分母的两遍账户一致不能替代 A 门。当前源回放分母为 0、合法率为 NULL，不能宣称 CONTROL 门通过。是否获得后续有效证明，以最终评审包和实际裁定为准，本段不改变生产环境或源码默认值。

`WorkflowApplication.monitor_dispatch` 的显式 record 分支：没有 owned 敞口时走旧 H1 路径；存在 owned 敞口时仅执行持仓保护/取消未成交买单，不新增 Team 买单。发布后的业务验收需区分该实际运行路径与仅设置了环境变量。

## A1 独立降级（0323）

若 A1 3.0.6 任一冻结日期失败，最终包可在研究配置 `agent_1.team_role_mode` 显式选 `production`；缺省 `team` 保持新 A1。非法值阻断，不能触发隐式回退。`production` 只取消 Team adapter 对 A1 的接管及新 A1 催化剂后处理，A2/A3 仍走新版；自然收盘 `from_active_a1` 沿用原生产 A1 generation，不重写原池、不重跑 A1。既有 A1 维护入口在该配置下仍走原生产 A1 规则/提示词，不把它的结果标成 3.0.6 通过。运行中失败不会在同次请求里自动改模型或重试旧提示词。配置选择要随最终评审包列出，本段不是当前生产变更。

## A4 1.1.0 候选复核（0308，覆盖旧宽域/分片合同）

引擎每分钟跟踪全部原计划与 owned 持仓，模型不再任意扫描计划池。只有确定性 BUY/ADD 且信封 eligible、计划有效、价格在带内且不超 no_chase 的入场候选，或确定性主动 SELL/REDUCE 持仓事件，才进入本节拍候选域。已闭合 5m 节拍一次最多送 2 个对象、单请求、不分片；旧状态转换/15m 心跳/24 对象分片仅保留历史兼容，不用于 1.1.0 调用。未送入的入场候选不能默认通过；未复核的确定性离场单列 ENGINE_EXIT_NOT_REVIEWED。

模型只能确认本次原动作，或对入场回 NO_ACTION、对主动离场回 HOLD。校验绑定计划 ID/持仓 symbol 和原动作，不能从 BUY 升 ADD、对未送入对象发信号或放大仓位。ADD 的原持仓以只读上下文给模型，但不因此取得该持仓的额外 SELL/REDUCE 权限。

模型预算至多 30s，仍受共享 47s 全局期限约束；没有延长 monitor 58s 父预算。入场超时/无效不下单，主动离场超时/无效保留原引擎动作并标 ENGINE_EXIT_FALLBACK；硬止损、T+1、停牌/涨跌停、账户/PM 限额仍由原引擎独立裁决。安全超时分类只存稳定码，不存异常正文、请求头或密钥。A5 按实际送入对象统计确认、否决和超时，未知失败的超时率为 NULL，未调用不能计成模型通过。

0308 要求先清点原 kernel 候选，再做实际探针；原最终 monitor 动作可能已是 LLM_VETO 或去重 NO_ACTION，不能据此漏掉 kernel BUY。缺历史 PM/A6 或完整报价时不补造权限/报价；源接线、空账户两遍一致与 funded CONTROL 门必须分开。单对象超时后的纯引擎降级是作者已定义的评审路径，未实际触发和验收前不称已上线。

## A2 3.0.5 自然反馈边界（0308）

确定性 TOP8 主题分与原候选域不变。具体目录外引用导致的 TEAM_SUPPORTING_REFS_INVALID / TEAM_OPPOSING_REFS_INVALID，可进入最多两轮的自然反馈；空引用、错误结构和其它无效输出不获新增重试。反馈仅列无效 ID 和目录类别，第二轮仍完整校验。首轮失败原件和两轮合计耗时保留，不把旧 BLOCKED 改写成成功。这个 TEAM_* 重试资格是本次运行时新增，不冒称原生产路径早已覆盖。

## 内部通知（2026-10-17 接线切片）

`run-team-internal` 08:45–17:20 每分钟、独立 notification 槽8s，不占交易47s或模型 review 槽。只读投影已提交的 owned 账本到 `team_notifications`；游标和去重拒绝计数也只写 Team 库。只使用独立 SHADOW_RESEARCH_WEBHOOK，不复用客户发送器或模板。正文原样hash绑定、先提交 SENDING 再请求、明确 code=0 才 SENT，未知状态不自动重发。原状态切换/A5卡投递标记跟随真实回执。相关测试与未完成的自然/VM证明见 `TEAM_V2_INTERNAL_DELIVERY_2026-10-17.md`；不以开发 mock 代替投递验收。

## A5双路径（0258）

客户v1提示词/service/publisher不变；Team A5 使用当前 manifest 绑定的独立角色版本（现为 2.0.7）、独立研究表和内部卡，不读取客户报告正文，不向客户DTO投影。signal_reviews用原model_judgments.decision_id或原旧路径事件evidence_id精确覆盖，禁止按同股三元组合并。假设只写研究库和`artifacts/replay-queue`，不自动应用。两个自然Team A5任务各240s，模型请求前等待退出75可限速重试；模型失败退出2不能重试或回退v1生成。初始组件回执见 `TEAM_V2_A5_203_WIRING_2026-10-17.md`，后续源码/版本不借旧回执冒充最终证明。15:00 原 bar 存在与缺失两条正确性分支仍需纳入完整回放。

## A3 PLAN_REVIEW（组件切片，尚未切换自然研究）

冻结候选域仅 FOCUS/WATCH_ONLY，原引擎资格只在盲审之后比对。模型PASS不能放宽风险单位、改策略/簇/几何；engine=false不能因模型PASS获得计划权限。逐事件模型原判断与无效输出都进独立研究账本，不混有效成交cohort；排名只用于本批次展示，全局合并需自然适配器绑定。原技术事实投影、完整源hash、严格schema、真实客户端mock及故障恢复见 `TEAM_V2_A3_PLAN_REVIEW_2026-10-17.md`。组件返回 publication_authorized=false，不是发布入口。

`ResearchPipeline(team_research_adapter=...)` 已能在实际A3分派/批次/本地门合并中调用新接口；当前候选的 CLI 研究命令通过 `team_v2_wiring.RESEARCH_COMMANDS` 显式启用 Workflow adapter，只读/行情命令不因构造 Workflow 而启用角色或创建研究 journal。生产尚未发布，旧提示词 checkpoint 不满足新角色。原引擎 provenance/风险门仍适用，跨批排名只作标明来源的展示穿插；原 FOCUS 且 plan_mode=PROBE 也不得升 STANDARD。初始红绿原件见 `TEAM_V2_A3_RESEARCH_ADAPTER_2026-10-17.md`，实际三日角色门见最新 execution state；组件返回 publication_authorized=false 不替代正式发布入口。
