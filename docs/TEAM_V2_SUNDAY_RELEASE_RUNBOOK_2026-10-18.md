# Team V2 周日发布前提（0327-review-verdict）

当前仅为候选流程，不是发布验收。周日先按 0234 发布 H1 并完成唯一内部 PROBE；Team 最终回退树须等于当时实际 H1 生产 HEAD。F1–F3 新 HEAD 的全量和 VM 切片通过、Claude 最终 ACCEPT 后，才能按已有 deploy.sh 发布。任何前提失败，维持 H1，不在周一盘中补发。

## C1 显式 RECORD 与 Settings 可加载

生产根目录固定 `/www1/Agu/liangjian-funnel-workflow`。在现有 Node 服务的实际环境与该根目录 `.env` 两处显式配置 `LIANGJIAN_A4_TRADER_MODE=record`；`LIANGJIAN_BROKER=local_paper`。只修改这两项，不打印整个 env、不复制凭证到证据或桥接。保留原配置的受控回退方式。

安装后在根目录、生产解释器下执行作者要求的命令：

```sh
.venv/bin/python -c "from liangjian_funnel.settings import Settings;print(Settings.from_env().a4_trader_mode)"
.venv/bin/python -c "from liangjian_funnel.settings import Settings;s=Settings.from_env();print(s.a4_trader_mode,s.broker_adapter)"
```

必须 exit 0，依次输出 `record`、`record local_paper`。同样从 Node 服务实际继承的环境验证这两个允许输出的字段，不能以交互 shell 的结果冒充 Node 配置。记录实际加载路径/新 PID、RECORD 无敞口旧 H1 分支或有敞口保护分支的证据。非法模式会在 Settings 校验时失败，不能继续部署或回退到隐式 CONTROL。

## C2 账户无敞口，只读确认

检查 `state/team_v2/paper.db`：不存在可记录 ABSENT；存在则 SQLite URI `mode=ro` + `PRAGMA query_only=ON`，确认 `paper_lots` 的 `qty>0` 行数为 0，`paper_orders` 的 `status IN ('OPEN','PARTIAL')` 行数为 0。缺表、锁库、无法校验均不能当 0。只记录数量和只读退出码，不迁移、不建库、不清仓、不删订单。

只要任一有效持仓或 OPEN/PARTIAL 订单存在，停止 Team 发布并报告，不自动处理敞口。

## C3 完整提示词和 LF

安装后核对 VM 实际 `prompts/team_v2/` manifest 与候选固定字节：10 个文件逐个 SHA256、字节数、LF，无缺失/CRLF/孤立 CR。核对生产实际运行所读的提示词目录，不只检查 ZIP 或另一个 checkout。以候选 TeamPromptRepository 与 manifest 既有校验器为准，不调整角色正文或修改 pin 让错误通过。记录实际 VM 路径、清单与退出码；失败不放行研究链。

## C4 不运行生产计划导出脚本

`scripts/export_team_publication_plans_readonly.py` 不在发布、安装、验证、测试自动收集流程中执行。它会连接生产，不能以“只读”代替发布授权。测试仅消费已封存本机原件，VM 切片是离线沙箱。发布回执记录本次实际命令清单，确认没有调用该脚本；不得通过 import 顺带触发。

## F3 时刻表（Asia/Shanghai）

客户 `a5-close` 仍 16:00，原 BUSY 重试窗口最晚 17:00，单次上限 15 分钟；客户命令、模板与通道不变。

Team `team-eod` 固定不得早于 17:20，并须同日自然 close 已成功，独立 120 秒 review 槽。Team `team-a5-close` 只在 17:25–17:45 调度，独立 240 秒；只限既有 pre-model wait/BUSY 重试，不补跑模型失败。内部通知独立槽延至 17:55，以便发送晚移后的 Team A5；Python 与 Node 窗口相同。客户窗口结束前没有 Team EOD/Team A5 占 review 槽，两者并发反例必须证明客户不进 BUSY 重试。

## 来源说明与名单血缘

`min_cash_reserve_pct: 0.10`、`no_new_entry_after: 14:30` 来源均为引用文档 `CODEX_TASK_BRIEF_A6_RISK_GOVERNOR_2026-10-15.md` §3.2 参数表（MIN_CASH_RESERVE / NO_NEW_ENTRY_AFTER），不是开发临时拟值。master §10 写“其余默认接受”，但其括号摘要未逐项列出这两项，须把精确出处交 Claude 确认。尤其“14:30 沿用 A4”这句任务书注释不等于当前生产事实：现有 `funnel_config_v2.yaml` 的旧 A4 截止是 14:45:00，不能声称二者相同。此处不修改任一阈值；RECORD 不应用 Team 新开仓限制，转 CONTROL 前按最终裁定处理。

新版 Team A2/A3 将影响以后自然收盘正式发布的客户计划名单：研究命令显式 adapter → A2 FOCUS/WATCH_ONLY → A3 盲审与原硬门 → 主 lane READY 的 `_publish_plans` → `publish_plan_batch` → `execution_plans` 的待晨审正式批次 → 下一交易日 `publish_a3_premarket_analysis` 只读选最新 source_run_id → 原客户发布器。模型 PASS 不等于必发，研究提名不进 A3，风控与原硬门仍最终裁决。RECORD 只切 A4 执行，不关闭新版 A2/A3 研究。

周一 10-19 晨卡仍取周五 10-16 已有的 5 条待晨审计划；周日不恢复研究、不改这些计划。新版若于周日正式发布，首个自然 A2/A3 是周一 15:10，产物首先影响周二 10-20 晨卡，不把周一旧批次标成新版结果。
