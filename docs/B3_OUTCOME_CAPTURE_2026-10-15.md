# B3 0174 决策标签解耦

实际核验时间（Asia/Shanghai）：2026-10-15 17:56。依据 `0177-reply` 第3节；本页为本地工程切片，不是生产补写或发布回执。

## 改了什么

`common.py` 在真实 `_persist_gate` 边界捕获紧凑字段，按当前 `run_id/lane_id/local_stage` 绑定原 snapshot id/hash 和知识时钟。`evaluation/decision_capture.py` 仅保留判定、原因、基线维度和哈希；不保留整份因素包，不启用、不查询 feature_store。`run()` 开始即清空上次捕获，历史回放设置禁止标签写入。当前 gate 缺失时不从最终计划或旧 A1 代际倒造。

完成态审计后，仍调用既有 `record_stage_decisions` 和既有 outcome ledger；没有迁移或第二套收益计算。失败、未执行和未知状态不造 REJECTED。缺失最终分区的未知成员只保留在 `outcome_capture/<run>/<lane>/<stage>.json`，明确 `PARTIAL_UNKNOWN`，不把分母不完整称为通过。

A2 标签的 PASSED 明确为 **最终 focus 或 watch 中被保留**；metadata 同时存 `a2_focus`、`a2_watch_only`、正式 `_build_a3_candidate_domain` 算出的 `a3_eligible`，以及 `stage_review_status`。这不是宣称观察股都经过模型批准或都有 A3 技术机会。A1/A3 的 PASSED 仍取原主选集合；A3 次级池通过 `a3_secondary_watch` 与主池区分。记录的 gate / 审核输出哈希、run/lane/stage 和源时间可独立对账。原冻结输入、gate 和审核输出均不修改。

## 真实测试

命令（cwd 为当前 worktree，`PYTHONPATH=src`）：

```
D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -m pytest tests/test_b3_outcome_capture.py tests/test_deterministic_pipeline_v2.py tests/test_outcome_labels.py -q --junitxml=artifacts/b3-engineering-20261015/label-capture-green-final-v5.xml
```

- 初轮 red.xml：10失败，其中6项空 tuple/list 夹具错误；原件保留。
- 修正夹具后 red-corrected.xml：4失败、6通过，pytest 退出1；真实缺陷是 feature_store 禁用导致所有标签跳过。
- green-final-v5.xml：**77通过、0失败、0跳过，退出0**，其中15项为本次专项；耗时6.450秒。
- v3 单次新增不可变测试因 `deepcopy(mappingproxy)` 的夹具错误失败，pytest 实际退出1；同一 PowerShell 后续 hash 命令掩盖 shell 退出码，不作为通过。修正为复制 `dict(frozen.data)` 后 v4/v5 均真实退出0，v3原件保留。

六类必需反例已覆盖：禁用因素库仍记录 A2/A3；失败/未执行不造拒绝；run/lane/snapshot 隔离；真实 run 入口回放保护；无 gate/复用 A1 不逆造；focus/watch/送 A3 资格分离。另覆盖捕获后原对象修改、未知分区、审核降级、跨 lane 审计、输入与模型输出不变。

## 文件与 SHA256

- `src/liangjian_funnel/pipeline/research/common.py`：`2e467d0a34f3aefa2a241e219f57cbb275be3d1abcbec0b057c1aed99ee5ae6d`（包含主代理既有 P-STRICT 改动，不全归本切片）。
- `src/liangjian_funnel/evaluation/decision_capture.py`：`6b7102de0c645cceb77b1c1a70b3721846914cade5d7e283fe96fdbb6552268d`。
- `tests/test_b3_outcome_capture.py`：`93a787f5e3b6fe10aa9152ce3510ed1100b314c735b2fbf90be137f8b1e07f3c`。
- green-final-v5.xml：`b19d8dd2f42e1483a18b7d603dacfe95058ddd81aa1a7b6dcb4b3c1016e91d98`。
- red-corrected.xml：`32a9af29f68b11f07990895db0f198114deab453658c59fee4eb405343d1652e`。

CODE：相关切片通过，最终候选全量由主代理执行。REPLAY：本项不回填历史，不宣称收益覆盖。OPERATIONS：待最终 VM 隔离反例及发布后自然新标签证据。STRATEGY：无参数或模型输入变化。本执行未访问 VM、生产数据库、网络或模型；测试数据库仅在临时目录，已由 pytest 清理。
