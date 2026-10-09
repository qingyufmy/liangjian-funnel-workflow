# WP5 原冻结资料绑定核验

结论：10-08 的投影、原始 FrozenInputSnapshot、研究 lane 和模型 attempt 可绑定；尚不能把原 A2 的完整派生输入重放验收标为 COVERED。10-09 收盘在冻结前失败，没有完整当日收盘快照，晚间 A1 快照不能替代。

新增只读入口 `scripts/audit_snapshot_binding_readonly.py` 不加载 Settings、不构造 RuntimeStore、不联网。它复算投影 canonical hash、验证原始模型的内容哈希、逐阶段 output hash，并对账 attempt 的 record_hash、input_hash、prompt_hash 与 A2 snapshot_id。输入文件执行前后字节哈希必须一致；已有输出拒绝覆盖。

实际命令（`PYTHONPATH=src`，Python 使用现有开发 venv）：

```powershell
$dir = 'artifacts/wp5-20261009/readonly-20261008'
python -B scripts/audit_snapshot_binding_readonly.py `
  --projection "$dir/snapshot-20261008T163351+0800-5c1c660d0b7c.json" `
  --raw "$dir/snap-3377a346d297f37ef472d6e8.json" `
  --lane "$dir/research_2026-10-08-close-a2-audit-172755_lane_1.json" `
  --attempt "$dir/attempt-1-03272918b38b86c6.json" `
  --output artifacts/wp5-20261009/snapshot-binding-20261008-v1.json
```

返回 DATA_LIMITED，三个主体绑定及 attempt 绑定通过。Python 入口返回 2；该次外层 PowerShell 工具未显式传递 native exit，呈现 1，不能写为通过或混同两个退出码。对照脚本与篡改反例切片为 8 passed、退出 0，回执 `snapshot-binding-tests-v1.xml`。

当前没有原版 A2 NEWS_HEAT/bottleneck 完整 overlay payload 与原运行参数的可复算副本；coverage 文件和九只送审上下文不是完整 overlay。旧版本也未生成公告同步前的预筛 receipt。这些缺项不能用当前 collector、当前配置或最终 A2 结果补造。

独立预筛诊断 `disclosure-batch-20261008-v1.json` 得到 1,451 输入、184 保留、9 个原必需送审成员、0 遗漏，入口退出 2；它证明这份投影上的行为，不足以证明原批次完整回放。

四层：CODE 局部测试通过；REPLAY 部分绑定核验通过、整体 DATA_LIMITED；OPERATIONS 未发布；STRATEGY 未验证。本入口永久禁止自身输出执行授权，不能用于绕过范围切换门禁。
