# A1 3.0.1 真实研究入口适配切片

实际时刻：2026-10-17 06:04–06:13 Asia/Shanghai。源码 `66409607ff8c04df083eb9e474041dfdedfd0ea3`。本机隔离/真实客户端 HTTP mock，真实模型调用0、VM访问0、生产修改0。

## 改了什么

显式注入的 `TeamResearchAdapter` 新支持A1的 POLICY_MACRO_DISCOVERY / COMPANY_MAPPING。沿用既有快照投影、发现包和预算，原样加载Claude的45KB角色正文；元数据/证据目录经Team传输，模型原输出和每个判断进入独立journal。A1 context现在穿过真实 `_run_stage` 分发，不再掉回旧提示词。

捕获后用单请求的pipeline浅拷贝运行**原**A1后处理与校验：发现源交集、12–18主题/40–80节点/月度20基础决策、主营证据、财务/质量阈值、池划分和原公司域/产业链绑定均不放宽。浅拷贝不修改共享client/adapter，批次并发不会串线。已捕获的响应可在本地审计写失败后恢复，不再HTTP；旧语义修复若仍要求重试，明确BLOCKED并保留原校验原因，不暗中追加模型调用。

新增角色字段校验：根handoff与证伪、逐项正反证据/哨兵、催化剂原引用及合法日期、主题证伪和A2交接。公司data_quality/evidence_confidence忽略模型回声，复用已有确定性local_active_items投影计算，再送原阈值；原输出未改，仍可审计。

真实红例发现旧SNAPSHOT_MANIFEST会夹带完整原块，未经验证的个股收益反馈能绕过单独的feedback占位。新manifest只保留快照身份/hash/as_of/源键及质量摘要。无已证明成熟的方法级反馈时强制 `INSUFFICIENT`，不接收调用者任意READY/个股涨跌。

**尚未完成**：方法级反馈满10交易日后的成熟源读取器；A1/A2最终Workflow构造器启用；三日真实模型干跑。此切片不是这三项完成证明；A2扣分区间仍等0265，其他工作继续。

## 红绿回执

- 初始真实分发红：9失败exit1，`artifacts/team-v2-20261016/20261017T060652003639-a1-natural-real-red-receipt.json`，SHA `678788377206bff304ff50e3218d8b5b3a984bdc19c50ee06f8e5a9b9b1b4223`。
- 反馈manifest泄漏红（1失败/22通过）：`20261017T060723879613-a1-natural-fix-receipt.json`，SHA `8b0cf1408f282f94d7ed1867bac40f4eda692b07081451534494ecea772f6f3d`。
- 最终关联：215通过exit0，`20261017T061115394044-a1-actual-source-final-receipt.json`，SHA `b06d3d5953e0937c259665e7abe619733e09a84abb091f7ba5825026fb362f3c`。包含A1两个会话、引用/字段/重复域反例、公司质量不可冒充、发现原强证据门、审计故障恢复、父预算、既有A3/Team合同与原research/A1合同回归。不与旧切片相加，不充最终候选全量。

旧失败/日志保留，提示词8文件原字节未改。

四层：CODE关联通过；REPLAY仅合成反例，正式六日未过；OPERATIONS未VM未部署；STRATEGY收益未证。H1最终f79独立周日白天发布安排不变。
