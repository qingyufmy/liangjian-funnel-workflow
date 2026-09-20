# S00 基线审计

## 身份与隔离

- 基线提交：`360c8612c6dc28052509cad5c833b57eeb1b2d79`。
- 开发分支：`codex/implementation-s00-s11-20260920`。
- 隔离工作树：`D:/dev_A股/liangjian_funnel_iteration`。
- 开始时唯一未跟踪文件是用户提供的实施总提示词；主工作树中的其他未跟踪文件未复制、未修改、未提交。
- 本阶段未访问或写入生产数据库，未发通知，未调用模型，未重启服务，未部署，未 push。

## 实际技术基线

- Python 3.11.15；项目要求 Python >=3.11。
- Node 22.23.1、npm 10.9.8；项目要求 Node >=20。
- CI 权威命令来自 `.github/workflows/ci.yml`：Python 全量覆盖测试、TypeScript 类型检查、Vitest、前后端构建。
- 运行配置 `config/runtime.yaml` 禁用 `external_orders` 和 `live_trading`；`config/exchange_rules.yaml` 为 `simulation_only`；`config/funnel_config_v2.yaml` 为 `SHADOW` 且订单权限禁用。

## 真实基线结果

| 命令 | 退出码 | 结果 | 证据 |
| --- | ---: | --- | --- |
| `python -m pytest --cov=liangjian_funnel --cov-report=term-missing --cov-fail-under=65` | 0 | 1721 passed, 4 skipped, 78.60% | `artifacts/iteration/s00-baseline-20260920/python-ci.log` |
| `npm run typecheck` | 0 | 通过 | `artifacts/iteration/s00-baseline-20260920/node-typecheck.log` |
| `npm test` | 0 | 8 files / 86 tests passed | `artifacts/iteration/s00-baseline-20260920/node-test.log` |
| `npm run build` | 0 | Web 与 server 构建通过 | `artifacts/iteration/s00-baseline-20260920/node-build.log` |

这些结果只证明基线代码和构建可运行，不证明历史回放、生产稳定或策略有效。

## A4 真实调用路径

`workflow.py::monitor_once` 当前先激活计划和加载持仓，再把决策与归档标的合成同一批；刷新全市场状态后，才创建采集 deadline。每个标的依次取 1m、可选原生 5m、报价，整批结束后才构建 `risk_bars`，随后才调用 `_settle_prior_signals`。确定性触发进入 `MonitorEngine`，模型回调为 `_a4_callback`，最后 `_settle_prior_signals` 将模拟动作交给 `PaperBroker`。

该路径证明“持仓保护在整批取数之后”是当前静态事实；是否在生产发生超时仍需只读运行证据。

## A1/A2/A3 与评价路径

- A1 继续以现有 A1 registry、事实缓存、feature store、A1 packet 和 sources 为权威入口；本任务不建立平行股票池。
- A2/A3 继续消费冻结研究结果；后续覆盖账本只增加可追溯投影，不靠扩大候选数制造改善。
- A5、outcome 和 evaluation 保持只读评价职责。S00 未执行历史回放，也未把当前测试结果解释为策略增益。
