# W1 shadow 首触发状态两阶段提交

已修复独立 session 因结果越分钟/超预算丢弃后，engine 仍消耗首触发状态的问题。tentative 只修改独立局部候选 state，消费者验证实际结果时钟及独立持久化回执后才 commit；discard 不覆盖任何已提交 state。默认 `evaluate_minute` 仍返回原 dict，并立即提交其已完成的内存 state，不改变正式 A4 evaluator、路由、阈值或 source。

基线 HEAD `3f49f78ca19d753e9a6202729797f8dc06030994`。本轮是等待修复之后的独立授权切片；旧 `shadow-session-wait-final-v3.xml` 的 130 项不冒称覆盖此新事务代码。旧缺陷诊断 `shadow-session-wait-discard-state-diagnostic.*` 保留原 source SHA，不能将该旧反例命令继续当当前源码通过证据。

## 正常构造公共合同

```python
pending = engine.evaluate_tentative(items, minute=M, budget_seconds=5)
pending.result                     # 原 engine 结果，无 DB/provider/network
engine.commit_tentative(pending)    # 全部 touched keys
engine.commit_tentative(pending, accepted_keys={(plan_id, variant_id)})
engine.discard_tentative(pending)
```

`TentativeShadowEvaluation` 由正常 dataclass 构造。token 是 engine 当前持有的 opaque reservation，不是外部可声明的证书；commit/discard 必须收到同一正常返回 handle 的对象 identity。候选保存当前 base state version、result hash、目标交易日和每个 touched key 的候选状态。改 result、外来 handle、旧 handle、错误 key、版本冲突都不能提交。

同 engine 仅一个 pending。reservation 存在时，无论 default 或 tentative 并发调用均返回 `SHADOW_TRANSACTION_PENDING`，不评估、不推进 state。commit/discard 持同一短锁验证版本，重复/并发提交最多一个成功；不会拿过期整份快照回滚新提交数据。commit 只合并指定 keys，不把未写成功 keys 当成功。

tentative 跨日先使用空的局部日内 state，不提前清空已提交上一日 state；discard 后原 day/state 保留，明确 commit 新日才转换。已转换到新日后旧日请求 `SHADOW_CLOCK_REGRESSED`，不能回退再补历史信号。pending 不会无限增加；明确 discard 释放 reservation，但已超时尚在运行的 worker 仍占唯一 `_active` 槽，下一轮 `SHADOW_WORKER_BUSY`，不扩 worker、不加入任务队列，晚 worker 结果不能自行 commit。

engine 公共 commit 是纯内存提交协议，本身不声明真实源时间/持久化资格；实际同分钟及 source 原完成包验证由 session 完成。不允许用手造 handle 或旧 minute result 代替当下调用证据。

## Session 接受与写入边界

Session 改用 `evaluate_tentative`。原完成包等待、closed cutoff、当前 scope、原事件/zlib hash 校验不变。真正 source-ready、evaluation-start、result-ready 时钟顺序及 wall/monotonic ≤5 秒、同 M+60 fence 全部满足后才能进入独立 writer。engine budget/reentry/worker busy 等非完整 `OK` summary 一律 discard，部分已完成 engine rows 也不能冒称全域完成。

此后评估＋依赖＋写入共用同一总预算，而非评估结束重新给 writer 5 秒：wall deadline 为 `min(M+60, evaluation_started_at+budget)`，monotonic deadline 为 `evaluation_mono+budget`，任一边界到达即停止。回执附 `shadow_total_deadline_at/shadow_total_budget_seconds`。identity/arrival provider、各次 ledger 写入及状态接收前后统一检查；wall 或 monotonic 相对上一次观测回退也停止。原完成包源等待不误套首次 poll 的 5 秒，统一执行 deadline 从实际 engine 评估开始计时。

结果超时/越分钟/回拨时：没有 signal/minute ledger 写，`finally` discard 候选 state。下一分钟仍需新的实际 current-round source，再评估当下条件；仍触发时可以首次输出，不补旧分钟 shadow 成交。此前已 commit 的其他 keys 不会因 discard 清除，也不会在下一分钟重复首触发。

writer 回执按已有真实 `ShadowEvidenceLedger._write` 合同处理（原实现未改）：

| 原独立 signal 回执 | engine/session 行为 |
| --- | --- |
| `ok=True, stored=True` | key 可提交；即使 JSONL mirror=PENDING，SQLite 已提交事实不得假装未写 |
| `stored=False` | 明确未落库，停止剩余写；此前已保存 keys 部分提交，未存 keys 不消耗首触发，可下一分钟重新真实评估 |
| 缺 stored、异常、无法确认提交结果 | 不猜成功/回滚；`writer_state=UNKNOWN_SESSION_PAUSED`，session 后续显式 `SHADOW_WRITER_COMMIT_UNPROVEN_SESSION_PAUSED`，等待外部独立对账，不自动重试 |
| signal 已保存，minute summary 写失败 | 已保存 keys 保留，不重发；本轮 `SHADOW_WRITE_FAILED`，不能把 summary failure 否认成 signal 未写 |

每次 signal/辅助写入启动前核实际 wall/monotonic clock，不能在统一总预算或分钟 fence 后新启动剩余写；provider 取回也再核时钟。若已启动的同步 SQLite/mirror 写入超总预算或跨 fence 后才返回，本轮 `DATA_LIMITED / SHADOW_WRITER_TOTAL_BUDGET_EXCEEDED` 或 `SHADOW_WRITER_MINUTE_FENCE_REACHED`，保留真实 `stored=True` keys 并停止剩余写入，不宣称全轮完成。monotonic/wall 回退为 `SHADOW_WRITER_CLOCK_REGRESSED`。已确认保存 keys 的内存去重补记不赋予额外写账资格。同步底层 IO 没有 OS 级强制抢占协议，本切片没有修改 ledger 或假装可以撤销已提交字节。

commit 繁忙/版本异常或确认已存 state 却无法提交时，session 进入明确的不确定暂停；不清空整个 engine，也不通过超时扩大正式生产时限。未知状态只能留 gap，不能自动生成完整回执。

## 反例与实际局部回执

新增专用 `tests/test_w1_shadow_commit.py` 共 25 项。本轮只对旧 session 测试 Engine spy 增加显式 tentative/commit/discard 接口，signal mock 增加真实 `stored=True` 语义；未改既有 22 项 wait 测试的时钟断言。正式默认 engine 的既有 first-trigger、去重、worker/5 秒容量、原 baseline 不重算回归一起运行。

覆盖：tentative discard 后首次触发保留；已 commit 后不重发；pending 同 engine 并发/direct 阻挡；同 handle 并发 commit 单赢家；foreign/mutated/stale handle；部分提交 keys；跨日 discard/commit/旧日拒绝；超时 worker 单槽与晚返回无自提交；session 超预算/跨分钟 discard；已存 V1/未存 V2 后下一分钟只重试剩余；未知首条/第二条写入暂停且保留已存 key；stored=False 不因 ok 变真而消费；SQLite 已存但 mirror pending；minute writer 失败；provider/辅助 writer 跨 fence 不宣称完整。

总预算审查新增三项真实 red→green：评估消耗 4 秒，首个 signal 同步写入再耗 1.5 秒，在仍同分钟情况下原会继续全部写并误报 OK；现仅保留已确认 V1，剩余不写/不记 summary，下分钟只重新评估未存 V2–V6。依赖刚好耗尽总 5 秒不得启动首写；依赖期间 monotonic 回退也不得写或消耗 state。

Claude 0042 要求的独立预算/不污染生产反例也覆盖：原 outer `START_CONFIRMATION`、dispatch、实际 created_at、原 +47 秒 production deadline 与输入保持逐对象不变；shadow 注入 worker 超时仅产生自身 DATA_LIMITED，无信号落账、无 engine 状态提交。测试禁用 production `evaluate_strategy`，同时比较 workflow 原字节 SHA 前后不变；不是启动真实 A4 或生产超时运行。

保留红/绿原回执：

| 回执 | 真实命令 exit / 结果 |
| --- | --- |
| `shadow-commit-before.xml` | 1；12 failed，缺新事务接口及实际首触发漏账边界 |
| `shadow-commit-engine-v1.xml` | 0；34 passed / 5 deselected，纯 engine 与原相关回归 |
| `shadow-commit-v1.xml` | 0；39 passed |
| `shadow-commit-related-v1.xml` | 0；147 passed / 9.90s |
| `shadow-commit-unstored-before.xml` | 1；1 failed / 17 deselected，ok=True+stored=False 不得提交 |
| `shadow-commit-final.xml` | 0；148 passed / 12.32s |
| `shadow-commit-unknown-receipt-before.xml` | 1；1 failed / 19 deselected，当前回执必须明确未知暂停 |
| `shadow-commit-final-v2.xml` | 0；150 passed / 13.00s |
| `shadow-commit-aux-fence-before.xml` | 1；2 failed / 20 deselected，arrival/minute writer 跨 fence 原误报 OK |
| `shadow-commit-final-v3.xml` | 0；152 passed / 14.27s |
| `shadow-commit-total-budget-before.xml` | 1；3 failed / 22 deselected，总预算与 monotonic 回退反例 |
| `shadow-commit-total-budget-final.xml` | 0；155 passed / 12.88s |

最终命令，工作目录 `D:/dev_A股/liangjian_wp5_hotfix_20261009`：

```powershell
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -m pytest tests/test_w1_shadow_commit.py tests/test_shadow_session.py tests/test_w1_shadow_session_wait.py tests/test_w1_shadow_variants.py tests/test_w2_shadow_evidence.py tests/test_audit_frozen_a4_clocks.py --junitxml=artifacts/wp5-20261010/shadow-commit-total-budget-final.xml
```

最终 source/receipt SHA256：

| 文件 | SHA256 |
| --- | --- |
| `runtime/shadow_variants.py` | `adc1de89ecc1661de813ebaa8394718c204dad0807c78fd91e8c226c382ed76a` |
| `runtime/shadow_session.py` | `11c2b499eab5ddf9ca389338d68cd268e77171ffbdc3ccc5480335adba2ad058` |
| `tests/test_w1_shadow_commit.py` | `15f60a4e3affb63325c3dd382e56ec52ac0aeb12a7a58ea4c7b281e365b96fd3` |
| `tests/test_shadow_session.py` | `f93c04ae6bdd4f3df235e1dd146e792b9a6833fb3d90d6e276408613fbcfffcf` |
| `shadow-commit-total-budget-final.xml` | `3e56acc677e1a0a09cb05f5f7cf09903fd3dbc7b4b6bdfc786b848cc3ce8a5c7` |

`git diff --check -- src/liangjian_funnel/runtime/shadow_variants.py src/liangjian_funnel/runtime/shadow_session.py tests/test_shadow_session.py` exit 0。workflow SHA 仍 `7bf62d19640973f102830f612be2e1dea05812dfaf4d7359b4a4605b1bb30c09`，正式 strategies SHA `079600edc90a6cdb69060ea272a63a46fe2cd19e61ea2746ccb43bb3840b05b3`，本轮未改。

## 四层状态与交付边界

源码：两阶段独立 shadow 合同已实现；正式策略/A4 主路径未改。测试：上述局部 155 项通过，不是新 HEAD 全量。运行：未启动生产/读取生产 DB/VM、未部署/通知/模型/提交。业务：仍 IMPLEMENTATION_PARTIAL，PIT 和成交 provider 真实来源/独立服务尚未接线，不能宣称自然分钟、影子成交或收益通过。

根代理本轮已在独占 CLI 增加显式 `--monitor-latest`，本 agent 只读确认，没有修改 CLI；不传仍 UNWIRED。真实 VM 原 marker 的纯结构/hash 验证由根独立做，不能以当前时钟回放旧触发。installed/site-packages 下 checkout-relative helper 定位仍是另一个未完成项。

state 提交是内存边界，独立 ledger 是每条 SQLite durable 边界，两者不是跨进程 distributed transaction；未知写入暂停的恢复需要核对真实独立 ledger，不自动重启/重建首触发。生产 47 秒预算与影子评估＋依赖＋写入总 ≤5 秒接受边界各自独立；已启动同步 IO 不可强制抢占，越界不称成功。没有借事务逻辑延长生产时限或接入 A4JobRunner。

本 agent 的 source/test/本说明冻结交根统一集成；不自行 commit/push，不动其他 agent 源码、state 或桥接记录。
