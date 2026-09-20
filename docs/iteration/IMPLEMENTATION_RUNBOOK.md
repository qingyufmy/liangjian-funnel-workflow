# S00—S11 验收、证据、迁移与回滚手册

## 1. 保护边界

当前分支只允许离线测试、只读证据校验、隔离回放和 SQLite 副本迁移演练。不得把脚本输出目录指向 `state/`、`storage/`、`outputs/`、`cache/` 或 `config/`，不得使用生产账户、通知、模型凭证或真实订单。生产发布、服务重启、数据库迁移和 feature flag 切换仍需单独授权。

退出码统一为：`0` 表示该 profile 的必需条件通过；`1` 表示真实失败；`2` 表示参数错误；`3` 表示缺少环境或证据；`4` 表示保护策略阻断。`3` 不能解释为通过。

## 2. 离线代码验收

```powershell
.venv\Scripts\python.exe scripts/run_iteration_acceptance.py `
  --profile offline `
  --output-dir artifacts/iteration/<run-id>
```

该入口清除模型/通知凭证并设置离线环境，只运行测试和配置保护检查。它最多证明 `CODE_ACCEPTED`，不能证明真实历史回放、虚拟机稳定性或策略收益。

## 3. 生产只读证据包

导出脚本不扫描磁盘，也不寻找 SSH 密钥；所有输入文件必须由操作者显式列出，并且位于新的独立包目录内。包至少含六类：调度/阶段耗时、分钟数据与 Provider 状态、计划/决策/模型结构化结果、订单/成交/费用/持仓、A1 覆盖账本、价格/公司行为/交易日/主题参考。要执行 S10 分层回放还需 `layered_evaluation_input`，影子运维报告还需 `shadow_sessions`。

```powershell
.venv\Scripts\python.exe scripts/export_iteration_evidence.py `
  --root D:\isolated\liangjian-evidence-YYYYMMDD `
  --file scheduler=D:\isolated\liangjian-evidence-YYYYMMDD\scheduler.json `
  --file minute_data=D:\isolated\liangjian-evidence-YYYYMMDD\minute-data.json `
  --file decisions=D:\isolated\liangjian-evidence-YYYYMMDD\decisions.json `
  --file orders=D:\isolated\liangjian-evidence-YYYYMMDD\orders.json `
  --file a1_coverage=D:\isolated\liangjian-evidence-YYYYMMDD\a1-coverage.json `
  --file market_reference=D:\isolated\liangjian-evidence-YYYYMMDD\market-reference.json `
  --file layered_evaluation_input=D:\isolated\liangjian-evidence-YYYYMMDD\layered.json `
  --code-version <commit> --config-version <hash> --rules-version <version> `
  --model-version <model> --prompt-version <hash> `
  --start <ISO8601> --end <ISO8601>
```

脚本流式计算 SHA-256，拒绝目录外文件、符号链接、凭证字段和模型私有推理。没有某类输入时 manifest 明确 `complete=false`；不要用空文件伪装完整。

## 4. 忠实回放

```powershell
.venv\Scripts\python.exe scripts/run_iteration_acceptance.py `
  --profile replay `
  --manifest D:\isolated\liangjian-evidence-YYYYMMDD `
  --output-dir artifacts/iteration/replay-<run-id>
```

只有非测试、哈希完整、六类基础证据齐全且包含 `layered_evaluation_input` 的包才会继续。补数回放或事后模型研究会保持相应标签，不能输出历史可交易声明。测试包、缺项包返回 `3`。

## 5. SQLite 迁移演练

先由已有授权流程获得一致性 SQLite 只读副本，再执行：

```powershell
.venv\Scripts\python.exe scripts/rehearse_runtime_migration.py `
  --source D:\isolated\runtime-copy.sqlite3 `
  --workdir artifacts/iteration/migration-<run-id>
```

脚本再次备份到隔离目录，保留 rollback 副本，在目标上迁移两次，核对完整性、关键账本记录数、幂等性和源文件哈希。它不会迁移生产数据库。若中断，报告为 `FAILED_ROLLBACK_AVAILABLE`；此时只能重做副本演练，不能自动恢复生产。

生产上线后若已有新事件，禁止用旧备份覆盖。优先关闭新功能并前滚修复；只有明确停写窗口、快照后事件补放方案和再次授权时，才讨论物理恢复。

## 6. 影子稳定性

影子存储、账户和输出必须与正式路径物理分离，且路径不能互为父子或经过符号链接。影子默认禁用通知和真实订单。

```powershell
.venv\Scripts\python.exe scripts/run_iteration_acceptance.py `
  --profile shadow-report `
  --manifest D:\isolated\liangjian-evidence-YYYYMMDD `
  --output-dir artifacts/iteration/shadow-<run-id>
```

运维验收建议至少五个完整交易日，并包含 kill/restart、网络中断、慢源、LLM 超时、磁盘满、数据库锁和损坏输入七类故障证据。报告同时展示终态率、可评估率、报价覆盖、模型成功、真实无机会、p95/p99、积压和数据库等待；全部 BLOCK 不能冒充稳定。

## 7. 发布与回滚

当前交付不发布生产。获得明确授权后，仍应先确认目标 VM、运行目录、分支/提交、配置差异、数据库副本演练和影子报告，再使用仓库既有 `deploy.sh`。发布后需要核对实际 HEAD、Python 导入路径、Node 健康、调度、飞书账本和业务结果；`/health` 不能替代这些证据。

回滚优先级：关闭新 feature flag → 回滚应用代码 → 保留新 schema 与事件账本 → 前滚修复。不得删除或覆盖历史信号、成交、预占、持仓和通知证据。
