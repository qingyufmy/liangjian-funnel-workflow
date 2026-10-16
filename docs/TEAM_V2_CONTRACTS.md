# Team V2 合同草案

本文件仅说明当前隔离开发合同，不宣称所有工作包完成或已投入运行。

## 边界

Claude 提供的角色正文和根/判断证据规范为唯一提示词来源。缺交接、证伪、真实目录引用即无效；反方哨兵必须附至少两个实际证据类别。解析失败不得放行。根会话 schema 与正式调用器接线是后续发布门。

引擎拥有硬风险和股数裁决。模型只能在已封存信封内收紧、排序、择时；研究提名永不进入 execution_plans。数量、费用和资金计算全部 Decimal；布尔输入不允许字符串真值转换。

## 独立模拟盘

`liangjian_paper_broker` 不导入生产 RuntimeStore，不连接生产数据库或真实券商；`runtime.broker` 是唯一适配入口。100 万初始资本与风险 YAML 必须一致。撮合仅消费已完整到达的闭合分钟，价格无来源或单位不明不猜测。订单不能在信号所属分钟成交，不得使用未来分钟；共享成交量上限、涨跌停不可成交、T+1 和费用均是硬规则。

账本按信号/订单幂等，部分成交原子扣款/扣可售量。收盘 NAV 必须有当日 15:00 收盘标记，否则 DATA_LIMITED/NULL，不能以末次 VWAP 冒充。账户曲线可复现比较采用 canonical snapshot bytes，不宣称 SQLite 物理文件字节相同。

## 未完成的发布门

六日真实冻结输入回放、三日七提示词真实模型干跑、A6/PM/trader 完整编排、model_judgments/标签、Node 接线、故障注入、最终 HEAD 全量、VM 切片、回退树及最终 Claude ACCEPT。以上任何缺项都不是生产就绪。

## 内部通知（2026-10-17 接线切片）

`run-team-internal` 08:45–17:20 每分钟、独立 notification 槽8s，不占交易47s或模型 review 槽。只读投影已提交的 owned 账本到 `team_notifications`；游标和去重拒绝计数也只写 Team 库。只使用独立 SHADOW_RESEARCH_WEBHOOK，不复用客户发送器或模板。正文原样hash绑定、先提交 SENDING 再请求、明确 code=0 才 SENT，未知状态不自动重发。原状态切换/A5卡投递标记跟随真实回执。相关测试与未完成的自然/VM证明见 `TEAM_V2_INTERNAL_DELIVERY_2026-10-17.md`；不以开发 mock 代替投递验收。

## A5双路径（0258）

客户v1提示词/service/publisher不变；Team A5 2.0.3独立模型、独立研究表和内部卡，不读取客户报告正文，不向客户DTO投影。signal_reviews用原model_judgments.decision_id或原旧路径事件evidence_id精确覆盖，禁止按同股三元组合并。假设只写研究库和`artifacts/replay-queue`，不自动应用。两个自然Team A5任务各240s，模型请求前等待退出75可限速重试；模型失败退出2不能重试或回退v1生成。完整回执见 `TEAM_V2_A5_203_WIRING_2026-10-17.md`。收盘尾15:00原bar缺失反例0259仍是整包发布门未完成项，不能用A5关联绿代替。
