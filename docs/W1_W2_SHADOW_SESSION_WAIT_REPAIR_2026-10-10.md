# W1/W2 独立 shadow 等待生产完成包修复

本切片只修复独立消费者的等待与知识时钟边界。原先首次 poll 后 5 秒结束等待、15 秒 lateness 拒绝，会把正式 A4 在本分钟 +20/+40 秒提交的合法原记录永久跳过。现改为等待原 `monitor/latest.json` 完成包，在同一 wall 分钟剩余完整评估预算内只评估一次。没有修改正式 A4、调度、模型输入、交易或生产配置。

源码调查基线为本地 HEAD `3f49f78ca19d753e9a6202729797f8dc06030994`；本轮仅本地 fixture 验证，旧 HEAD 全量回执不覆盖本轮变更。

## 真实调用与时间合同

- `workflow.monitor_once`（4580 起）记录实际 `monitor_started_wall`，从实际启动起配置 47 秒预算；`current` 为 dispatch 分钟 M，不能假设生产必在 M+47 秒前提交。
- `_a4_execution_cutoff`（9235）选择此前已闭合 1m 观察点，通常为 M-1，午后开盘采用真实会话边界。事件的 dispatch M 不等于可使用 M 当根未闭合 bar。
- `runtime.state.record_monitor_event`（2352）用真实 `_now()` 记录 `created_at`；部分 lane 已有行不代表全轮完成。
- `workflow.monitor_once`（5486–5549）构造实际 `round_total.finished_at`、`DecisionObservation`，随后将全轮 payload 原子写入 `monitor/latest.json`。这是现有全轮完成边界，不新增 A4 certificate、callback 或状态写入。
- 此包的 `decision_hash` 本来不包含所有时钟/逐行字节；消费者同时 pin 原文件字节 SHA256，不能用 ID 集合或 filemtime 替代原包。

本轮只读工作核对后，`workflow.py` 字节 SHA256 仍为 `7bf62d19640973f102830f612be2e1dea05812dfaf4d7359b4a4605b1bb30c09`；未改 A4 主路径。

## 接口与安全等待

```python
ReadOnlyShadowSource(state_db, minute_db, *, lanes, monitor_latest=None)
source.read_completion(minute, *, observed_at, observation_clock=None)
source.read_round(minute, *, observed_at, observation_clock=None)
ShadowSession(source, *, ledger, started_at, budget_seconds=5,
              wall_clock=None, clock=time.monotonic, ...)
session.poll(observed_at=actual_poll_clock)
```

`monitor_latest` 必须由调用者显式指定；不从 Settings、环境变量或目录猜测。默认 `None` 返回 `PRODUCTION_COMPLETION_UNWIRED`，不是空包通过。完成文件与输入/输出文件的路径及已有 hardlink 别名拒绝。

原包缺失或仍是旧分钟：`WAITING_PRODUCTION_COMPLETION`，不锁死当前分钟，不评估可见部分 scope。包未来/错误 hash/错 snapshot/错 cutoff/缺 lane/完成后 DB 缺 baseline：本分钟 `DATA_LIMITED`，不按部分域宣称完整。当前分钟的包应同时满足正常 `DecisionObservation`/`TimingSpan` 构造、真实 schema 与 decision hash、scheduled/start/finish/observed 时钟、snapshot、选定 lane 与所有当前活动 plan 外层 action/reason 对账；原读源中的 plan/window/event UUID/payload hash/zlib SHA/closed-only 校验不取消。

完成包读取前后字节一致，才能绑定 DB 读事务；中途原子替换给 `PRODUCTION_COMPLETION_CHANGED_DURING_SOURCE_READ`，不得 pin 旧包却使用新轮记录。包读取有 8 MiB 边界；源分钟窗口原有单 blob 8 MiB、总 32 MiB 边界保留。

`observed_at` 仍记录实际 poll 起点。读到原包后另取实际 `wall_clock()`，记录 `production_completion.observed_at`，解决包恰在 poll 与文件读取之间落盘的竞态；不把 +41 秒才提交的行冒称 +40 秒已知。读时钟不得回拨到 poll 前。评估开始时钟不得早于该完成包的真实观察时钟。

全局 fence 为 M+60 秒。启动评估前必须留出完整 `budget_seconds <= 5`，且其预算终点严格早于 fence（默认 +55 秒起拒绝）。源读取耗尽剩余时间则不启动 engine。+5/+15 秒仍可等待，+20/+40 秒真实完成后在同分钟评估，不排到下一分钟才评估。

engine 返回后再次记录真实 `result_ready_at`，要求 wall 与 monotonic 评估耗时均不超过预算、结果未越 M+60、时钟未回拨。违反时没有 signal/minute ledger 写入。独立 ledger/provider 使用结果实际已知时刻，不倒填 dispatch M。跨日/分钟只读当前轮，旧轮迟到不补历史 shadow 成交。

原 `wait_seconds`/`max_lateness_seconds` 参数暂保留兼容解析和有限值验证，已不作为源 readiness gate；回执显式标 `DEPRECATED_NOT_APPLIED_TO_SOURCE_READINESS`，不误把 5 秒评估预算当 5 秒源等待限额。

## 反例及旧测试合同修正

新增 22 个反例/场景包括 +5/+15 等待、+20/+40 完成一次评估、+55/+56/+59 拒绝、次分钟不回补、wrong hash、future finish、错 snapshot/current bar cutoff、缺 lane/plan baseline、包替换、源读取耗尽预算、engine 越分钟/预算、实际 result-ready 写入时钟、默认未接线、poll/read 落盘竞态与三层时钟回拨。

旧 fixture 原将 dispatch M 当 closed bar_end；现按实际 `_a4_execution_cutoff` 改为 M-1，增加原完成包和明确 fixture 时钟。旧断言“+20 秒必迟到”改为“+55 秒没有完整剩余预算”；已完成包却缺行不能继续伪等 5 秒。次日老包只能等待次日原完成包，不能重用旧行；fixture wall clock 也真实推进至次日。真实 engine/独立 ledger 回归继续禁止重算 production baseline，缺 PIT/成交输入仍 DATA_LIMITED。

保留全部失败回执，不改名冒称成功：

| 回执 | 退出码与真实结果 |
| --- | --- |
| `shadow-session-wait-before.xml` | 1；11 failed（旧接口未实现） |
| `shadow-session-wait-v1.xml` | 1；3 failed / 38 passed（旧等待/rollover/无完成包 fixture） |
| `shadow-session-wait-v2.xml` | 1；1 failed / 118 passed（终结后应 DUPLICATE） |
| `shadow-session-wait-v3.xml` | 0；127 passed |
| `shadow-session-wait-read-clock-before.xml` | 1；2 failed / 19 deselected（poll/read 竞态与回拨） |
| `shadow-session-wait-final.xml` | 1；1 failed / 128 passed（次日 fixture 未推进读时钟） |
| `shadow-session-wait-final-v2.xml` | 0；129 passed |
| `shadow-session-wait-observation-order-before.xml` | 1；1 failed / 21 deselected（评估时钟早于 source 观察时钟） |
| `shadow-session-wait-final-v3.xml` | 0；130 passed / 9.56 秒 |

最后局部命令（PowerShell；绝对 Python，不使用默认 hermes Python）：

```powershell
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -m pytest tests/test_w1_shadow_session_wait.py tests/test_shadow_session.py tests/test_w1_shadow_variants.py tests/test_w2_shadow_evidence.py tests/test_audit_frozen_a4_clocks.py --junitxml=artifacts/wp5-20261010/shadow-session-wait-final-v3.xml
```

`git diff --check -- src/liangjian_funnel/runtime/shadow_session.py tests/test_shadow_session.py` 退出 0。

最终 source/test/receipt SHA256：

| 文件 | SHA256 |
| --- | --- |
| `runtime/shadow_session.py` | `562c8da6a7200fe11906d71ab8a595ba0d19a685f78564b15103e3100f9a317e` |
| `tests/test_shadow_session.py` | `ae05c7fd71b14df5991c6a100dde65760bfd9f3e75416a8b3864af66a84d09bf` |
| `tests/test_w1_shadow_session_wait.py` | `a387a653d4787ea5503d14534d297c55210a04b73470a91e3fdbf54c42c8d83f` |
| `shadow-session-wait-final-v3.xml` | `65d49b94390955237f12c86566f56a1f7af0493bf584eb241c135eedbec1d3f4` |

## 四层证据与未完成项

源码：此独立消费者修复已实现，生产 A4 和原 CLI 字节未改。局部测试：上述 130 个 fixture/真实本地 engine+独立 ledger 回归通过，不是新 HEAD 全量。运行：未启动生产独立进程、未访问生产 DB/VM；`run_shadow_session.py` 尚无 `monitor_latest` 参数接线（原 SHA256 `8371805a5e44088adce373dff444bebfd6d2474dd919cba33aed1a23df411e16`），默认诚实 UNWIRED。业务：仍 IMPLEMENTATION_PARTIAL，不能宣称自然分钟、影子成交或收益已通过。

PIT provider 必须读取该 plan/day 原 09:26 sealed receipt，不可盘中重建并重标采集时刻；bar-arrival provider 未接线，不抓新行情。使用真实 `result_ready_at` 后，现有 fill 模型只允许知识到达之后完整新分钟，不能把已开始的下一根或旧闭合分钟补成交；本轮不改 fill 模型。

尚未解决 installed/site-packages 中 checkout-relative audit helper/source 哈希路径可能不存在。源读取与同步独立 writer 不提供 OS 级强制抢占；源读取结束仍检查分钟剩余预算，writer 记录耗时但不宣称严格写盘截止。engine 如果返回超预算，ledger 无结果写入；此模块不提供其内部 first-trigger state 的事务回滚/retry，当前分钟终结不重跑。上述边界不应通过延长正式 A4 时限、后补历史或假时钟绕过。

原 marker 的独立结构验证交根代理：真实既有路径 `/www/wwwroot/Agu/liangjian-funnel-workflow/outputs/monitor/latest.json`，由 `scripts/watch_a4_runtime.py:48` 与上述 publisher 确认；本轮本地 artifacts 文件名检索未取得原字节，状态 `NOT_LOCALLY_AVAILABLE`，不是 `SOURCE_ABSENT`。正常构造 `ReadOnlyShadowSource`（两个明确未打开的 DB 占位路径、从原包选定实际 lane、精确 marker 路径），只调用 `read_completion` 可核原 `DecisionObservation`/hash/cutoff/round_total，不调用 `read_round` 或 `poll`、不构造 App/Store、不评策略。独立核验应另记 VM HEAD、候选模块 SHA、原 bytes 前后 SHA、marker 业务日期和实际读取时钟；即使验证通过，也只是旧原包结构证据，不能用当前时钟回放触发。此切片未执行 VM 读取。

冻结本说明及三个 owned 源码/测试文件，不自行 commit，不触碰其他 agent 的 scope/state/源码变更。
