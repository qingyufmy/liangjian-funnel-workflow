# 工作流整改追踪表

> 总方案：`WORKFLOW_REVIEW_MASTER_REMEDIATION_PLAN_2026-08-29.md`  
> 基线提交：`caa46ddde97f85463b2dd5de0fcc1b55d95b5883`  
> 状态更新时间：2026-08-29  
> 安全边界：研究与隔离模拟；禁止连接真实交易

## 基线证据

- 虚拟机提交：`caa46ddde97f85463b2dd5de0fcc1b55d95b5883`
- 基线检查时没有活动中的 A1/A2/A3、回放或特征维护进程。
- active generation：`feature-full-614bf4fb253344a89a8f6888`
- previous generation：`feature-777fe89a93c0542596b1141c`
- generation 数：5；run binding 数：1。
- `market_fact_cache.sqlite3`：2732077056 bytes。
- `research_feature_store.sqlite3`：693551104 bytes。
- `workflow.sqlite3`：987136 bytes。
- `/www/backup/liangjian-funnel-workflow`：约2.8GB。
- 根分区：38GB，已用28GB，剩余约7.3GB，使用率80%。

当前剩余空间不足以在原分区无条件再复制一份完整事实库和特征库。部署前必须先生成备份空间预算；未经单独确认不得删除旧备份或运行数据。

## 问题追踪

| ID | 问题 | 当前状态 | 本地验证 | CI验证 | VM验证 | 业务验收 |
|---|---|---|---|---|---|---|
| R1 | 历史回放污染active generation | VERIFIED_LOCAL | 回放隔离、CAS、绑定测试通过 | 待CI | 待部署验证 | 不适用 |
| R2 | A2数据不足误报无机会 | VERIFIED_LOCAL | 数据不足/真实空源/脏数据矩阵通过 | 待CI | 待真实源验证 | 待真实样本 |
| R3 | 六端状态不一致 | VERIFIED_LOCAL | v3合同、生成类型、Python/Node/UI测试通过 | 待CI | 待部署验证 | 不适用 |
| R4 | run opportunity语义混淆 | VERIFIED_LOCAL | 作业、充分性、研究/焦点/行动/发布轴测试通过 | 待CI | 待真实运行 | 待真实样本 |
| R5 | primary仍等待comparison | VERIFIED_LOCAL | 主任务先发布、comparison子运行和租约隔离通过 | 待CI | 待真实运行 | 不适用 |
| R6 | 趋势代理冒充真实梯队 | VERIFIED_LOCAL | 真实梯队、观察为空、UNKNOWN与趋势代理分离测试通过 | 待CI | 待真实源验证 | 待真实样本 |
| R7 | Feature Store物化不完整 | VERIFIED_LOCAL | full/incremental manifest和基本面物化测试通过 | 待CI | 待迁移验证 | 待真实覆盖 |
| R8 | A1候选元数据丢失 | VERIFIED_LOCAL | 各阶段代码/名称/行业/主题/映射缺口测试通过 | 待CI | 待真实结果 | 待真实样本 |
| R9 | 关键模块覆盖率不足 | VERIFIED_LOCAL | 711项；整体行81.74%；六组关键行/分支均≥90% | 待CI | 不适用 | 不适用 |
| R10 | 存储与恢复合同缺失 | VERIFIED_LOCAL | 在线备份、完整性、引用保留、只读清理测试通过 | 待CI | 待备份恢复验证 | 不适用 |
| R11 | 验收报告数值手工漂移 | VERIFIED_LOCAL | 验收报告由持久化manifest/API/SQLite生成测试通过 | 待CI | 待生成VM报告 | 不适用 |
| R12 | 真实10日、金股、自然计划不足 | PENDING_BUSINESS_ACCEPTANCE | 工具已有 | 工具已有 | 单日样本 | 待外部样本 |

## 本地硬验收结果

- Python：`711 passed`。
- 全项目行覆盖率：`81.74%`，门槛 `80%`。
- generation、outcome、A2、PIT、primary/comparison、PaperBroker 六组关键函数行/分支覆盖率均不低于 `90%`。
- Node：`36 passed`；TypeScript 类型检查和生产构建通过。
- `research-outcome/3.0.0` JSON Schema 与生成 TypeScript 无漂移，schema SHA-256 前缀为 `fe14e7d01c0614db`。
- 并发 CAS 压力复测通过；对比任务租约只允许实际持有者释放。
- 当前结论是 `VERIFIED_LOCAL`，不等于 CI、虚拟机或真实业务验收完成。

## 发布阻断门

- R1、R2、R3、R4 任一未达到 `VERIFIED_CI`：禁止部署。
- 历史回放前后 active generation 不一致：立即回滚，不执行正式研究。
- A2关键覆盖不足仍输出 `ABSENT`：立即回滚，不执行A3。
- primary结果仍等待comparison：不恢复正式调度。
- 数据库备份未验证或磁盘低于安全水位：禁止迁移和全量重建。
- 任何真实交易连接被启用：立即终止验收。
