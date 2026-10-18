# Team V2 内部通知接线回执

实际采集时刻：2026-10-17 04:14（Asia/Shanghai）。隔离开发，未发真实消息、未读取秘密文件、未连接或修改生产。

## 已实现

`run-team-internal` 是独立命令，交易日 08:45–17:20 每分钟唤醒，Node 独立 `notification` 槽、父预算 8 秒。不会占用 primary/review/comparison 槽，不改变 monitor 58s、快照 50s 或交易决策 47s。命令在 legacy WorkflowApplication/RuntimeStore 构造前分流。

投影读取既有 paper/team 账本，不构造 broker、不补做晨会/结算/模型调用：08:45 首次晨会事实卡；APPROVED/REDUCED/REJECTED（数量、拒绝码、原裁决时间）；实际撮合成交及原 fill_basis；已冻结结算（NAV、持仓、真实日初基线存在时的当日净值变动）；市场/情绪确认切换；风控熔断、A6 连败、对账失败、CONTROL 当日去重拒绝超过5；正式规则变化原告警；17:10 起缺 Team V2 A5 报告时的 NO_REPORT_TODAY。无法证明的 origin/日初基线不编造，分别显示 UNAVAILABLE/资料不足。G3/G4 完成后需把其 origin 贯通原 admission；当前未宣称新路线来源齐全。

只新增 owned Team 表：`team_notifications`、`team_notification_cursors`、`team_guard_rejections`。纸面账本只读短事务，释放后才落通知。每流最多100条新增记录；空池日保存真实历史末行锚点，避免每分钟重复扫历史；锚点内容改变明确 DATA_LIMITED。入队后、游标提交前崩溃可恢复同一事件，不重复计拒绝数。

正文仅接受服务端构造的有界 text 卡，正文 SHA 原样绑定。只能使用 SHADOW_RESEARCH_WEBHOOK，不回退客户变量。仅从独立 env 选取该单键，支持 feishu/larksuite 正式域，拒绝重复键、无效 URL、跳转。每次最多投递3条，请求每项网络操作 timeout=2s，外层 Node 8s 负责进程硬截止；不把单项 timeout 伪称整个 HTTP 请求的严格总时限。只有 HTTP 2xx 且整数 code=0 才 SENT；未知/拒绝/SENDING 崩溃不自动重发，持续 DATA_LIMITED 待核验。没有远端 exactly-once 承诺。源规则告警文件不改写；状态切换/A5 卡的投递字段只跟随实际 outbox 状态。

## 实际测试及原件

以下均为源内容 SHA 绑定的开发回执（回执 HEAD 是修改前检查点373f248），不是最终 Team HEAD 全量；存在重叠，不相加：

| 回执（artifacts/team-v2-20261016/） | 实际结果 | receipt SHA256 |
|---|---|---|
| 20261017T041141574130-internal-qualified-final | Python关联97通过/0失败/exit0 | bd65605afd7ed018b30ceedab3b0b1d64ce78438a7a1b5fa01a7e518a0ecc413 |
| 20261017T041412855819-internal-empty-day-anchor-final | 最后修改的投影/命令20通过/0失败/exit0 | ab91980103c0939688687c132c51b2745794c99446bf1eb0576ebb71c848ddb4 |
| 20261017T040818286160-internal-node-final | Node全21文件160通过/0失败/exit0 | 77b317d77907ae42f8a791e4d92e5be8223017538688603c4749cfeae44cc21b |
| 20261017T041150533731-internal-node-shutdown-delta | 最后测试差异3通过/0失败/exit0 | 32c52fc8b2c4aef5f8e07d842537382f633484340f86c4f6f2da1c4c987cc311 |
| 20261017T040820112432-internal-server-ts | server TypeScript exit0 | 36436fce49b13bff32d1641b8a1acfd963c2879dfc0d8af2036e5f4761bad03a |
| 20261017T040821235716-internal-front-ts | front TypeScript exit0 | d3ae0987b06a401416a2cbef881d8f1c3e149361f38839195f42e68f1e27a95e |

红件全部保留：首轮缺模块退出2；填充夹具 REPORTED_VWAP 被测试误写为 ESTIMATED_VWAP 的1失败；测试导入路径写错1失败；新独立通知任务导致旧调度 mock 总调用数/首调用 BUSY 假设失效（Node先2项、全套10项失败）。修正的是上述测试假设，新通知时序和已有业务时序均显式断言，未改交易/竞价/结算语义绕测试。模拟 HTTP 的超时、未知响应、重定向、超大响应、拒绝码、重复键、hash篡改、旧日隔离、未确认不重发均通过。没有真实网络模型或客户/内部群请求。

## 四层结论和后续

CODE：本切片关联测试通过。REPLAY：本地故障注入/真实 owned ledger API 夹具通过；官方六日 Team 回放、三日七提示词真实干跑未完成。OPERATIONS：未做 VM 或生产通知验收，不宣称部署就绪。STRATEGY：没有新增收益结论，也没有修改阈值。

A5 契约问题0256仍待答复：展示兼容映射与多事件 decision_id；不自改 Claude 提示词。内部发件箱已能消费未来实际封存的 A5 卡，但未冒称自然 A5 角色切换完成。后续优先继续 A5 接线/正式门与尚未接线的研究角色。H1 仍为独立周日安装→部署→内部唯一 probe，当前没有提前部署/发送；最终 Team 回退必须包含届时 H1 实际生产 HEAD。
