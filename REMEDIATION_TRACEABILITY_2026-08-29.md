# 工作流整改追踪表

> 总方案：`WORKFLOW_REVIEW_MASTER_REMEDIATION_PLAN_2026-08-29.md`  
> 基线提交：`caa46ddde97f85463b2dd5de0fcc1b55d95b5883`  
> 状态更新时间：2026-08-30
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
| R1 | 历史回放污染active generation | VERIFIED_VM | 回放隔离、CAS、绑定测试通过 | 通过 | active未变化、历史run独立绑定 | 不适用 |
| R2 | A2数据不足误报无机会 | VERIFIED_VM | 数据不足/真实空源/脏数据矩阵通过 | 通过 | 正确输出DATA_GAP；真实源待交易日积累 | 待真实样本 |
| R3 | 六端状态不一致 | VERIFIED_VM | v3合同、生成类型、Python/Node/UI测试通过 | 通过 | run/lane/job终态一致，currentStage为空 | 不适用 |
| R4 | run opportunity语义混淆 | VERIFIED_VM | 作业、充分性、研究/焦点/行动/发布轴测试通过 | 通过 | A1有研究机会、A2数据不足、A3不适用 | 待真实样本 |
| R5 | primary仍等待comparison | VERIFIED_CI | 主任务先发布、comparison子运行和租约隔离通过 | 通过 | primary已独立发布；实际comparison子运行待验 | 不适用 |
| R6 | 趋势代理冒充真实梯队 | VERIFIED_CI | 真实梯队、观察为空、UNKNOWN与趋势代理分离测试通过 | 通过 | 待真实源验证 | 待真实样本 |
| R7 | Feature Store物化不完整 | VERIFIED_VM | full/incremental manifest和基本面物化测试通过 | 通过 | active与历史run代际隔离、quick_check通过 | 待真实覆盖 |
| R8 | A1候选元数据丢失 | VERIFIED_VM | 各阶段代码/名称/行业/主题/映射缺口测试通过 | 通过 | 120只候选详情可持久化查询 | 待真实样本 |
| R9 | 关键模块覆盖率不足 | VERIFIED_LOCAL | 713项；整体行81.74%；六组关键行/分支均≥90% | 标准CI通过；新门待workflow权限 | 不适用 | 不适用 |
| R10 | 存储与恢复合同缺失 | VERIFIED_VM | 在线备份、完整性、引用保留、只读清理测试通过 | 通过 | 数据库完整；磁盘82%保留警告 | 不适用 |
| R11 | 验收报告数值手工漂移 | VERIFIED_VM | 验收报告由持久化manifest/API/SQLite生成测试通过 | 通过 | VM报告已生成并标记业务待验 | 不适用 |
| R12 | 真实10日、金股、自然计划不足 | PENDING_BUSINESS_ACCEPTANCE | 工具已有 | 工具已有 | 单日样本 | 待外部样本 |

## 本地硬验收结果

- Python：`713 passed`。
- 全项目行覆盖率：`81.74%`，门槛 `80%`。
- generation、outcome、A2、PIT、primary/comparison、PaperBroker 六组关键函数行/分支覆盖率均不低于 `90%`。
- Node：`37 passed`；TypeScript 类型检查和生产构建通过。
- `research-outcome/3.0.0` JSON Schema 与生成 TypeScript 无漂移，schema SHA-256 前缀为 `fe14e7d01c0614db`。
- 并发 CAS 压力复测通过；对比任务租约只允许实际持有者释放。
- 当前工程功能结论为 `VERIFIED_CI`；R9 新覆盖率门仍为 `VERIFIED_LOCAL`。这不等于虚拟机或真实业务验收完成。

GitHub Actions 运行 `33264017720` 已成功完成。此前运行 `33263380912` 在 Linux 暴露了兼容发布入口读取 active generation 后发生 CAS 竞态；提交 `45d980c685f3f23ee4785e6f68a29bc97a5a2a3c` 只在兼容入口刷新其内部推导的 expected generation，显式 `activate_generation` 仍保持冲突即失败。该修复通过 100 次并发压力复测及 Linux CI。当前仓库凭据缺少修改 `.github/workflows` 所需的 `workflow` scope，因此 GitHub 仍运行原有全套测试/类型检查/构建工作流；新的 80%/关键90%门禁脚本已纳入仓库并在本地通过，但尚未接入远端工作流。此项不得误报为远端关键覆盖率门已启用。

## 发布阻断门

- R1、R2、R3、R4 任一未达到 `VERIFIED_CI`：禁止部署。
- 历史回放前后 active generation 不一致：立即回滚，不执行正式研究。
- A2关键覆盖不足仍输出 `ABSENT`：立即回滚，不执行A3。
- primary结果仍等待comparison：不恢复正式调度。
- 数据库备份未验证或磁盘低于安全水位：禁止迁移和全量重建。
- 任何真实交易连接被启用：立即终止验收。

## 最终工程验收增量（2026-08-30）

### 发布证据

- 本地、GitHub `main` 与虚拟机均为提交 `45d980c685f3f23ee4785e6f68a29bc97a5a2a3c`。
- GitHub Actions `33264017720`：`success`。
- 虚拟机 Node `/api/health`：`ok`；部署只重启 Node，没有运行中的研究任务被中断。
- 虚拟机根分区剩余约 `6.5 GB`，使用率 `82%`；数据库完整但存储水位继续标记为警告，不允许无预算地再次全量复制事实库。

### 本轮持久化运行

- run id：`2026-08-28-close-acceptance-0f58e9a`
- snapshot：`snapshot-20260828T210944+0800-2e278ff757d1`
- 总耗时：`1266s`；任务终态：`READY_DEGRADED / SUCCEEDED`
- 全市场目录：`5563`；研究池：`4017`
- A1：本地确定性筛选 `4017`，模型复核 `92`（`5/5` 批成功），最终研究池 `120`
- A2：输入 `120`，焦点池 `0`，观察池 `120`，状态 `DEGRADED_UNDERFILLED_DATA_GAP`
- A3：状态 `NOT_RUN_UPSTREAM_BLOCKED`，原因 `A3_NOT_APPLICABLE_UPSTREAM_DATA_GAP`

A2 的零结果不能解释为真实无机会。该历史点时日没有已持久化的同日全市场资金流，系统拒绝拿当前资金流冒充 2026-08-28 历史事实；关键覆盖分别为资金流 `0`、梯队 `0`、龙头 `0.008333`、行业链共振 `0.716667`，低于 `0.9` 门槛。A3 因此按合同不运行，但 A1、A2、A3 各自的 Markdown、lane JSON、决策 JSON/NDJSON 和 run summary 均已落盘。

### 状态与代际隔离

- `/api/overview`：run、lane、job 均为终态，`currentStage=null`；旧进度文件不再被前端投影为“仍在运行”。
- A1 进度：本地筛选 `4017/4017`，模型复核 `92/92`、`5/5` 批。
- A2 进度保留数据不足终态；A3 不再伪装成执行成功或零机会。
- active generation 仍为 `feature-full-614bf4fb253344a89a8f6888`。
- 历史运行绑定到隔离代际 `run-2e278ff757d1-fa673d29a972`，没有覆盖 active generation。
- `research_feature_store.sqlite3` 的 `PRAGMA quick_check` 返回 `ok`。

### 最终判定边界

工程层已完成代码、自动测试、CI、部署、终态前端、持久化和历史代际隔离验收。业务层仍为 `PENDING_BUSINESS_ACCEPTANCE`：需要后续真实交易日自动积累同日 A2 资金流/梯队数据，至少 10 个独立点时回放、4 个月合法券商金股盲测、一次自然非空 A3 计划，以及一次实际 comparison 子运行证据。不得用当前数据回填历史日期、放宽阈值或人工造单来关闭这些验收项。
