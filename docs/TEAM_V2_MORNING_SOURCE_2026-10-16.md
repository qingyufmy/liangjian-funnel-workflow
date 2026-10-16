# 晨会三段源接线（414057f）

实际整理：2026-10-16 23:12 Asia/Shanghai。依据 0234，继续开发，不请求生产发布。

## 本轮完成

08:33 `run-team-morning-engine` → 08:35 `run-team-morning-risk` → 08:38 `run-team-morning-pm` 已接 Node 和真实 CLI。各自 review 槽、独立 120s；A6/PM 模型单次最多 100s，现有 monitor 58s 未改。错过阶段窗口不补跑。

源使用 SQLite mode=ro、query_only、短 BEGIN/ROLLBACK；不构造 WorkflowApplication/RuntimeStore。冻结原计划字符串及全行排序哈希，后两段重新核对，行变更立即阻断。写入仅独立 team/paper/model 库，不激活计划。

首次用五条真实冻结生产行验证，发现 PENDING_MORNING_REVIEW 的 valid_from=NULL 合法：原读取器误判。已修为仅 PLANNING 接受空值并保留 None；执行入口仍拒绝，无人工激活时间。旧 payload expiry 原样保留，以 DB 日期测算，不修历史。

账户基准由独立 paper 一次封存；账户/持仓/基准同一只读事务读取。已有持仓的旧计划只取原身份和簇，不因为计划过期而丢掉持仓或重新激活。

A6 缺回执时，PM 明确收到 A6_MORNING_MISSING，只用引擎保守 PROBE 信封；PM 缺回执时无分配。A6 下调使用率上限后先裁剪 PM 基线，不能反向放大上限。

## 红绿原件

目录均为 `artifacts/team-v2-20261016/`。回执绑定测试时源码 SHA（提交前），与 414057f 的当前源码逐文件核验；不是最终 Team HEAD 全量。

| 项 | 回执前缀 | 结果 | receipt SHA256 |
|---|---|---|---|
| 入口红测 | 20261016T230308660193-morning-integration-initial-red | CLI 未接，1 失败 | 原件保留 |
| Python 最终关联 | 20261016T230911141898-morning-final-associated-green | 238/0 | c731a14c201e24f7413a18a9ee8863fa0ee8552858db15e85ee98123c31b1f57 |
| Node 八文件 | 20261016T230453064371-morning-three-edges-node | 83/0 | bdbe918cefb4b468014dbab257959c4707a2f3da60970f76516ac7e9b99fe2b5 |
| TypeScript | 20261016T230452989150-morning-three-edges-tsc | exit0 | 51c595dde860df4d1da25806ef853e85277bff825c458e5ae762374fa9ff6e39 |
| 五条原生产行本机复现 | 20261016T231027188441-morning-original-five-current-source | exit0，5/5；原行/原件不变 | 见同名前缀 receipt |

真实行原件 SHA：df50dcc6e8a4b961f620ad539fbc7dae2a178cd1570e0dec408938056001f05c；按 symbol/plan_id 排序行哈希 ec2a29eb6875edf6d53a4625757a9cd04bea3c447abce6880d915e19f6c10864。四簇来自原 primary_theme/theme_id，不是证券代码推断。

第一次原件脚本导入到了旧安装包、随后临时 SQLite 未 close 和原件排序差异，均保留失败回执并修正验证工具；只有 NULL 激活日期是源适配的真实业务缺陷，不能把工具错误当生产故障。

## 未完成与发布门

这是本机真实客户端 HTTP mock，不是真模型、VM 或自然调度验收。08:45 内部摘要、09:26 更新及 monitor 5m 接线、收盘 CLI 接线、A6 异常、全角色研究与标签仍在开发。

外围快照源未接：冻结回执显式 NOT_APPLIED_SOURCE_MISSING，不称已实施外围仓位调整；发布前必须补正式源、收缩规则和缺失路径验证。高确信 5% 的服务端资格来源也未接，当前不放大到 5%。这些不能冒称晨会整体完工。

六日冻结回放、三日真实模型干跑、最终 Team 全量/VM/带 H-1 回退尚未通过，不发布。G-5 四量与域证明源缺项仍 DATA_BLOCK，五日状态回放不造证据。CODE 为局部通过，REPLAY/OPERATIONS/STRATEGY 不升级。
