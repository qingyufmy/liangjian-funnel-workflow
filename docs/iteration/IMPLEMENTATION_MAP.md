# 实施文件映射

本目录按实施总提示词的文件名直接落地，不另造第二套状态体系。

| 总提示词交付物 | 当前文件 | 权威用途 |
| --- | --- | --- |
| 主任务 | `LIANGJIAN_CODEX_IMPLEMENTATION_MASTER.md` | 不变需求与边界 |
| 续跑状态 | `ITERATION_STATE.json` | 当前阶段、命令、退出码、证据状态 |
| 基线 | `BASELINE_AUDIT.md` | 当前 HEAD、调用路径与原始 CI 结果 |
| 发现 | `FINDINGS.md` | 经当前代码复核的问题及证据等级 |
| 决策 | `DECISIONS.md` | 设计选择与明确不做事项 |
| 追溯矩阵 | `ACCEPTANCE_MATRIX.md` | 需求—测试—证据 |
| 使用 | `RUNBOOK.md` | 隔离验收命令 |
| 回滚 | `ROLLBACK.md` | 开发分支和未来发布的回退边界 |
| 最终验收 | `FINAL_ACCEPTANCE.md` | 分级验收；完成 S11 前不得标总通过 |
| 实施报告 | `IMPLEMENTATION_REPORT.md` | 各阶段累计改动与未完成项 |

运行产物位于 `artifacts/iteration/<run_id>/`，由 `.gitignore` 排除。该目录不使用生产数据库、生产写目录或真实通知配置。
