# WP5：64 只来源对账与范围提前封存

## 1. 真实原因

按用户更新的任务书 8.1/8.3 执行，任务书 SHA256 为 `99c8bd974205e6058ee40832ba9d9becbe090c21156c9654fa2e34831235265c`。本轮先核对原代际与发现池，没有修改确定性预筛、公告筛选门或策略阈值。

既有只读探针的 v4/v6 核实了以下事实：生产仍 `47f6ef03b042c0f9a207400c72d2ecffff338244`，SSH 为 `192.168.1.254/debian`。

- 原 `outputs/runs/2026-10-09-close.json` 不存在。调用链仅在研究结束时写总结；本次在公告同步超时，所以不能从不存在的回执读出代际。
- 活动指针仍为 `a1-full-20261008T180015072172Z-fe5a5e7ecc11`，激活时间 10-08 18:00:15。截至 v6 采集点没有 10-09 新封存代际。因此没有发现“用 18:00 新代际错算差集”的情形，但不能把活动指针强称为原任务绑定回执。
- 15:10 发现扫描实际处理 **4,236** 只，0 缓存命中、9 个失败，2534 秒时完成（日志实收时间 **15:52:36.096**）。没有保存原始 G0 清单或发现池全局排名。
- 对缓存子集中 64 只 A1 外且不在人气榜的股票，只取 `fetched_at <= 15:52:36.096` 的日线历史版本，逐内容哈希验证后，用生产当前实际的发现规则复算：**64/64 都出现 TREND_RESTART_WATCH**；33 只还包含 MA520_CROSS_PREPARATION，12 只还包含 REPAIR_WATCH（标签可重叠）。29 个新形态、35 个持续观察形态。输入内容哈希错误 0；没有采行情、调用模型或使用晚间修订。

这使 EARLY_DISCOVERY 成为有数据支持的来源解释，但仍是事后受限复算：扫描结束时间不是每只股票的精确读取时间，没有全局前 100 排名，不能宣称“64 只原始送审成员全部确认”，也不能把 76 净差写成新增 76 只。原始 1,825 完整集合仍缺证据。

证据：`artifacts/wp5-20261009/close-scope-baseline-20261009-v6.json`，SHA256 `430d5e0c7531a10dc1cbff7cc7dfe1080ca4fc4e560b28e8972afd1ad20d26ed`；其中保留逐股形态、年龄、历史版本引用及输入哈希。v4 保留文件/缓存库存及原回执缺失证明；没有改写 v3。

另有新生成的 `snapshot-20261009T191648+0800-3289851d3f2d.json`（270,802,980 字节）。它晚于原 15:10 任务终止，处于 18:00 A1 维护阶段，不能替代原收盘的完整冻结输入。

## 2. 代码修改

- `pipeline/close_scope.py` 增加 `seal_scope_receipt`：保存真实研究时间、行情截止时间、实际传入的 A1 代际/哈希、原人气资料、全部发现记录（含未送审记录）、G0、真实选中集合及逐股重叠来源。实际选中集合不一致就拒绝写入；无代际绑定明确标 `UNBOUND_A1_REFERENCE`。内容寻址文件不覆盖前次回执。
- `workflow.py` 在现有并集与 G0 过滤完成后、行业与公告/PDF 长链之前调用封存；`run_research` 传入本次已加载的 A1 代际引用，不重新读取活动指针。原选股逻辑不变，快照保存回执路径与哈希。
- `audit_preopen_readonly.py` 增加代际指针、原收盘回执存在性、阶段边界、候选文件与缓存库存；可选 `--reconstruct-discovery` 仅用扫描结束前接收的原缓存版本，显式标注不是原前 100 证明。SQLite `mode=ro`，没有构造运行时写库类。
- 测试补充“后续失败仍能保留来源”“后来代际不改写旧回执”“发现池溢出不得冒充送审”“真实选中集合冲突”“周末研究时间与周五行情时间分离”。

## 3. 测试与回放

实际命令及退出码：

```powershell
$env:PYTHONPATH='D:/dev_A股/liangjian_wp5_hotfix_20261009/src'
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe scripts/audit_preopen_readonly.py --close-scope-day 2026-10-09 --output artifacts/wp5-20261009/close-scope-baseline-20261009-v4.json
# exit 0
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe scripts/audit_preopen_readonly.py --close-scope-day 2026-10-09 --reconstruct-discovery --output artifacts/wp5-20261009/close-scope-baseline-20261009-v5.json
# exit 1：新增远端分支遗漏 timedelta 导入；未生成结果文件。
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe scripts/audit_preopen_readonly.py --close-scope-day 2026-10-09 --reconstruct-discovery --output artifacts/wp5-20261009/close-scope-baseline-20261009-v6.json
# 修正导入后 exit 0；64/64 符合发现条件，不等于原排名验收。
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -m pytest tests/test_wp5_close_scope.py -o addopts='' -q
# 增加反例后、实现前 exit 1：缺少 seal_scope_receipt。
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -m pytest tests/test_wp5_close_scope.py tests/test_workflow_orchestration_coverage.py tests/test_workflow_fact_projection.py tests/test_pipeline_data_sync.py tests/test_workflow_fact_result_cache.py tests/test_cli_workflow_coverage.py -o addopts='' -q --junitxml=artifacts/wp5-20261009/scope-reconciliation-tests.xml
# exit 0；64 passed。
```

全量入口先因新 worktree 没有 Node 依赖退出 1：Python 2257 通过、6 跳过、1 预期失败，前端报 `vitest/tsc` 不存在。保留原失败回执 `full-scope-receipt-tests/evidence.json`，没有改写为通过。

核对 WP0/WP5 的 package-lock.json SHA256 相同后，建立本地 `node_modules` Junction 复用 WP0 已安装依赖（不是复制仓库或生产环境）。随后执行：

```powershell
& ./scripts/test_all.ps1 -PythonPath D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -OutputDirectory artifacts/wp5-20261009/full-scope-receipt-tests-with-deps
# exit 0；Python、npm test、typecheck 各 exit 0。
git diff --check
# exit 0。
```

统一回执共 **2358 项：2351 通过、6 跳过、1 已登记的预期失败**，失败 0；其中 Python 2257 通过，Node 94 通过。回执 SHA256 `bcc18565edfbb98e06cac76d54be0e2e269e7ea8a5edbfb55b4021dc275d1f76`。这是当前开发切片验证，测试时工作树有改动，`release_qualified=false`，不是部署许可或最终发布提交的验收凭证。200 只日线全量/增量对照尚未执行。

## 4. 四层验收

- CODE：范围提前封存与代际绑定切片全量入口退出 0（2351 通过）；任务 1/2 完整业务热修尚未完成。
- REPLAY：原版本受限形态复算 64/64 一致；原 G0 与全局送审排名仍 DATA_LIMITED，未宣称完整 1,825 范围重放通过。
- OPERATIONS：只读对账完成；未推送、部署、重启、补跑或发通知；五个交易日/60 分钟目标待证据。
- STRATEGY：不适用，策略与风控条件不变。

## 5. 未完成边界及入口修正

任务书 8.3 建议的普通 `run-research --slot close --as-of 2026-10-09T15:10:00+08:00` 在周六新采集会遭 `LIVE_FACTS_POINT_IN_TIME_UNSUPPORTED`（请求研究时刻距真实时间超过 600 秒），不是可直接执行的恢复路径。新增反例明确证明该阻断，不修改保护条件。

已有正式 `run-next-session-prep` 调用链使用周六真实研究时间、交易日历算出的周五 15:10 行情截止时间和周一目标交易日；从当前封存 A1 读取，并记录实际代际。它是周末新增研究，不是周五当时可执行性的回放。决策 9 将来授权时应按该契约执行并核对 10-12 目标、资料接收时间和待晨审状态，本轮没有执行或自行修改用户任务书。

确定性预筛和公告候选域仍未更改。日线增量当前实际已有“闭合日期水位 + 七日重叠请求”，并非每次请求 800 日；但 4,236 只逐股请求仍耗时约 41 分钟。下一步需要在不损害历史修订/复权证据的前提下改增量契约，并独立验证请求数与时间；不能将“少请求几天”宣称解决串行请求瓶颈。

WP5 是同一 Git 仓库登记的临时 worktree（`git worktree list --porcelain` 可查），与 WP1 分支隔离。纳入 WP0 收敛清单；决策 5 未授权前不删除、改名或迁移用户文件，也不建立新副本。

## 6. 下一步

继续 WP5 任务 1/2：以当前来源对账和已落地的范围封存为基础，分别验证日线增量与公告前确定性候选域；发布及周末准备仍等待独立授权。
