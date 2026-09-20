# 隔离验收运行手册

## S00 离线入口

在仓库根目录执行：

```powershell
python scripts/run_iteration_acceptance.py --profile offline --output-dir artifacts/iteration/<run_id>
```

退出码：`0` 必需检查通过；`1` 实际失败；`2` 参数错误；`3` 缺证据或该 profile 尚未实现；`4` 输出路径触发安全保护。

正常入口会读取三份运行配置、验证模拟/影子边界，并运行 `tests/iteration` 与既有 calendar、scheduler、simulation、A1 packet/registry 测试。它不会调用公网、模型或通知，也不会替代完整 CI。

故障传播验证：

```powershell
python scripts/run_iteration_acceptance.py --profile offline --inject-failure --output-dir artifacts/iteration/<failure-run-id>
```

该命令必须返回 `1`，且 `summary.json` 必须列出 `S00-INJECTED-FAILURE`。不要在生产 `state/`、`storage/`、`outputs/`、`cache/` 或 `config/` 下指定产物目录；脚本会拒绝。

## S03 A4 离线墙钟压测

```powershell
python scripts/run_iteration_acceptance.py --profile stress --scenario a4-s03 --output-dir artifacts/iteration/<run_id>
```

该入口生成 50/100/200 计划和 240 个交易分钟的临时 fixture，测量持仓风险 p50/p99 与一次决策核心轮次。结果只代表本机离线墙钟，不代表虚拟机、生产数据源或策略收益。

失效计划分钟证据使用独立适配器，不放回 A4 决策轮：

```powershell
python -m liangjian_funnel.cli archive-a4-auxiliary-once
```

该入口只写分钟归档，不生成信号、生命周期或订单；生产调度是否启用留待 OPERATIONS 验收。

## S04 A1 覆盖与补齐

以下命令只读取 A1 registry 覆盖投影，不触发外部请求：

```powershell
python -m liangjian_funnel.cli a1-coverage-report --scope A1_BASE --as-of 2026-09-20T15:00:00+08:00 --output-dir artifacts/iteration/a1-coverage
python -m liangjian_funnel.cli a1-gap-report --scope A1_BASE --as-of 2026-09-20T15:00:00+08:00 --output-dir artifacts/iteration/a1-coverage
python -m liangjian_funnel.cli a1-backfill-plan --scope A1_BASE --as-of 2026-09-20T15:00:00+08:00 --limit 100 --retry-budget 5 --dry-run --output-dir artifacts/iteration/a1-coverage
python -m liangjian_funnel.cli a1-backfill-run --scope A1_BASE --as-of 2026-09-20T15:00:00+08:00 --limit 100 --retry-budget 5 --dry-run --output-dir artifacts/iteration/a1-coverage
```

`--source` 是精确 `source_version` 限制，不会扩大来源权限。当前没有注册真实抓取适配器；去掉 `--dry-run` 的 `a1-backfill-run` 必须返回 `A1_BACKFILL_SOURCE_ADAPTER_NOT_CONFIGURED`，不能把入队或 lease 当作字段已经进入 A1 packet。生产 VM 执行前还需单独完成来源授权、影子积压和冻结输入对账。

## S05 费用与因果撮合

离线聚焦验收：

```powershell
python -m pytest -q tests/iteration/test_execution_causality.py
```

生产费率不从测试算例推导。通过 `LIANGJIAN_SIMULATION_COMMISSION_BPS`、`LIANGJIAN_SIMULATION_MINIMUM_COMMISSION`、`LIANGJIAN_SIMULATION_SELL_TAX_BPS`、`LIANGJIAN_SIMULATION_OTHER_FEE_BPS`、`LIANGJIAN_SIMULATION_MAX_VOLUME_PARTICIPATION` 和 `LIANGJIAN_SIMULATION_FEE_MODEL_VERSION` 配置，并在生产启用前与实际模拟账户合同单独对账。`SYNTHETIC_QUOTE` 只能触发风险意图；`virtual_fills` 的成交证据必须是 `MARKET_BAR`。本阶段没有授权生产迁移或部署。

## S06 组合风险与订单生命周期

离线聚焦验收：

```powershell
python -m pytest -q tests/iteration/test_portfolio_risk_lifecycle.py
```

生产迁移前必须在数据库副本执行 `audit_virtual_account(account_id)`，核对现金重建差异、position 与 lot 数量、活动预占；旧持仓若没有 lot，应先明确标记为遗留待迁移，不能自动伪造取得日期。新增组合风险和主题集中度参数为 `LIANGJIAN_SIMULATION_MAX_PORTFOLIO_OPEN_RISK_PCT`、`LIANGJIAN_SIMULATION_MAX_THEME_POSITION_PCT`，默认不启用；只读回放和单独授权之前不得把测试值带入生产。

排障时按 `order_identity` 联查 `risk_reservations`、`simulation_order_events`、`simulation_intents`、`virtual_fills` 和 `position_lots`。持久化结果不确定时先审计，不要盲目释放或重试；同日硬止损存在但 `sellable_qty=0` 是 T+1 待执行状态，不是风险计划完成。

## S07 严格模型审核

离线聚焦验收：

```powershell
python -m pytest -q tests/iteration/test_llm_review_contract.py
```

严格链路由 `LIANGJIAN_STRICT_LLM_REVIEW_V2=true` 控制，仓库默认和当前运行配置均为关闭。启用前必须先在独立 store、account 和 output root 完成回放/影子验收，确认精确候选集合、截止时间、模型/提示词身份和证据引用；不得直接在生产账户试开。

排障时查看策略事件中的 `strategy.llm_review`：`decision_id/snapshot_id/input_hash` 必须和冻结轮次一致，`reason/evidence_refs` 必须存在于冻结证据目录，`transport` 中不可得的 tokens/cost 应为 null。`MODEL_UNAVAILABLE`、`MODEL_TIMEOUT`、`MODEL_RESPONSE_INVALID` 与明确 `MODEL_VETO` 不得合并；持仓硬止损不依赖该审核。任何外部文章或公告文本都按不可信数据处理，不能把其中指令当作系统指令或证据引用。

## 尚未开放的 profile

`replay`、`shadow-report` 在依赖阶段完成前明确返回 `3` 和 `PENDING_EVIDENCE`；`stress` 目前只开放 `a4-s03`，其他场景不会用空数据返回成功。
