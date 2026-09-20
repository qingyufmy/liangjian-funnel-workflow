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

## 尚未开放的 profile

`replay`、`stress`、`shadow-report` 在依赖阶段完成前明确返回 `3` 和 `PENDING_EVIDENCE`，不会用空数据返回成功。
