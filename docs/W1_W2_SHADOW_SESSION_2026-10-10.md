# W1/W2 standalone shadow session（2026-10-10）

本切片是独立只读 A4 monitor 消费器，不是 Node/A4 JobRunner 接线或周一盘中验收。仅新增 runtime/shadow_session.py、scripts/run_shadow_session.py、test_shadow_session.py 与本文。未改正式 A4 body、workflow、producer、W4、shadow_day_adapter、accumulation、shared state 或桥接。

## 入口及真实源合同

`ReadOnlyShadowSource(state_db, minute_db, *, lanes)` 需要显式现有本地两个文件与 lane 集。运行源用 SQLite `mode=ro`、`query_only=ON`、短 BEGIN read snapshot，**不对更新中的 WAL 数据库使用 immutable=1**。没有 RuntimeStore/Settings、自动路径发现、源网络补采或生产访问。对应现有 schema 来自 runtime/state.py 的 execution_plans / monitor_events / virtual_positions，以及 data/cache.py 的 minute_decision_snapshots。测试建最小对应表，不使用生产库。

`read_minute(minute, *, observed_at)` 只读取当前 minute 的 monitor_events，不历史追赶。所有当前 ACTIVE_TODAY 计划（含不适用影子但仍属于实际活动域的 profile）必须有唯一事件记录，不按部分域放行。当前状态不是历史 status ledger：updated_at 晚于事件 created_at 即来源修订未证；没有历史补状态。任意所选 paper:lane 正持仓使当前整个窗口 POSITION_REPLAY_UNPROVEN，不宣称持仓完整生命周期可重放。

计划外/内 symbol、plan_id、lane、valid_from、expires_at、same-day start 与 observed_at 范围校验；原 invalidated/plan_invalidated 不放行。invalidation 时刻只来自该 plan 原 effective PLAN_INVALIDATED 的 minute_end，不以 updated_at 代替历史时刻。已 INVALIDATED 行不会改回 ACTIVE。事件 id 按真实 state.record_monitor_event 的 uuid5(event_key) 复核，outer action/reason/effective 与 monitor 原 internal/effective event_key 格式对应。原 bytes/canonical hash 证明当前所读对象绑定，不冒称外部认证或不可变历史发布认证。

精确按 minute_snapshot_id+symbol+interval 读取原 zlib。decision_as_of 必须等于 dispatch、captured_at 在 closed≤captured≤observed；验证原解压 bytes SHA256。拒绝 future、synthetic、重复、乱序、错股/interval、显式未完成及缺最新 closed1m。单包解压上限8MiB、总包32MiB，计划/事件/terminal 各最多1000，超过则显式缺证，不裁剪成完整。1m 与5m冻结包都要求存在；正常尚无原5m包或其他早盘包未冻结，会 DATA_LIMITED，不网络补窗。

直接复用 scripts/audit_frozen_a4_decisions.py 的纯 replay_clocks / frozen_market_overlay，动态载入但不执行 main/REMOTE。LEGACY_EVENT_CLOCK 不当显式 observation 证据。复用 workflow._intraday_market_context 的闭合 bars 纯构造，并覆盖原 strategy.market_gate；不读取旧市场文件，不用当前报价替换。缺原 market_gate、跨日或未来 gate 明示缺证。没有原实时 quote 的 arrival 还原；这是一条闭合 bars + 原 permission overlay 路径，不是执行价格/成交证明。

## 公共 consumer/导出接口

```python
source = ReadOnlyShadowSource(state_db, minute_db, lanes=("lane_1",))
session = ShadowSession(
    source, ledger=independent_ledger, started_at=actual_aware_start,
    engine=ShadowVariantEngine(), budget_seconds=5, wait_seconds=5,
    max_lateness_seconds=15,
    identity_provider=None, bar_arrival_provider=None,
)
receipt = session.poll(observed_at=actual_aware_now)
```

poll 单工非阻塞锁、无任务队列，仅当前分钟。minute 早于进程真实 started_at、迟于 current-minute 默认15s（最大可设30s）、跨日旧窗口/时钟回退均不得触发；重入 BUSY、已终结分钟 DUPLICATE 不重算/重写。缺域有界等待最多5s，再标 DATA_LIMITED，晚来记录不重新把终结窗口改成实时触发。下一交易日只读该日当前窗口，旧事件不补影子成交。

`plan_window_census` 保留真实当前事务的 active_plans（id/symbol/status/version/窗口/row hash）、expected/event/missing/unexpected plan_ids、各原 outer_baseline_records（action/reason/event_id/event hash/minute）。这是 reader.last_census；scope 失败时也尽可能附当前已读 census，不输出 payload 大对象。source_bindings 含原 plan payload_json UTF8 bytes hash、plan row canonical hash、minute_snapshot_id、两原 minute zlib解压bytes SHA、原 observation basis。完整调用时 engine minute_summary.baseline_records 亦附 receipt、独立分钟账本，保留 inner/outer 区别，不把 warmup inner DATA_BLOCK 误报为 outer START_CONFIRMATION。

engine 输入 `{plan_id,plan,baseline,actual_outer_baseline,baseline_event_id,baseline_event_sha256,bars,now,decision_time,market_context,source_binding}`。baseline 是原 strategy，不调用 evaluate_strategy 重算 baseline；研究变体由既有 ShadowVariantEngine 自身隔离执行，预算严格≤5s，不占 A4 gate/JobRunner。source reader 与独立 writer 是同步操作，有 SQLite timeout/包体上限；**5s不是整个进程硬抢占截止**，文件系统停顿也不受 Python 函数强抢占。writer_elapsed_ms 包括 identity/signal/arrival/minute 独立 ledger 写时间，elapsed_ms 为整 poll 结束测量，source与CLI实现实际 SHA 随receipt绑定。

默认 PIT/arrival provider **UNWIRED**，receipt status DATA_LIMITED、gap 明示，不声称成交或真实运营完整。geometry 变体可由既有引擎产研究状态；默认无 arrival 推进，所以不能产生回填 fill。注入接口（仅本地 fixture 开发已验证）：

- identity_provider(plan, observed_at) → `{status, evidence, raw_response:bytes|None, observed_at}`，通过既有 ledger.seal_price_limit_evidence；缺 READY/原bytes仍 DATA_LIMITED。另一 agent 的 build_plan_pit_capture/capture_shadow_pit 适配尚未接线，caller/provider声明不是外部认证。
- bar_arrival_provider(observed_at) → 原 arrival 证据列表，交 ledger.advance_outcomes，仍由原 ledger 验证 source_ref/hash/captured/complete 及下一完整1m等合同；没有网络默认 adapter，也不从 frozen历史决策包造下一分钟 arrival。

实际 PIT agent 校准：capture/build 的授权窗口是09:26≤真实knowledge clock<09:30，不能在盘中identity_provider(now)直接重新build或把原captured clock改成now。正式adapter必须由09:26独立batch用原execution_plans完整row捕获/seal；本session传provider的durable plan只是原payload+服务器窗口投影，不是该完整row。盘中provider只能读取已冻结同plan/day的真实seal/原clock/bytes引用。此adapter尚未实现，默认保持UNWIRED。

独立 writer 可同步写，沿用 ShadowEvidenceLedger 的独立 DB/JSONL hash-chain，不改其合同。不允许源/输出路径一致、解析后alias或已存在hardlink；CLI在构造ledger前检查。writer/engine/source异常仅产生 fixed-code shadowreceipt，不改源，不调用模型/通知/交易。evaluator异常signal reason只保留固定码，不把异常原文本写入新signal。

## CLI 与本轮证据

```powershell
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe scripts/run_shadow_session.py --help
```

exit0。入口绑定本脚本相邻src，不误加载另一 venv editable checkout。首次 --help 实际exit1（旧editable安装不含新module），该路径问题已修；没有生产执行。需要显式 --state-db/--minute-db/--lane/--shadow-db/--shadow-jsonl/--receipt-jsonl；receipt必须唯一新文件。默认独立寿命60s，最多86400s，无scheduler/systemd、网络/PIT自动来源。exit0仅表示有界CLI正常结束（receipt仍可能DATA_LIMITED），**不是 LIVE 或 READY**；配置/源缺失失败输出JSON/exit2。argparse缺参/格式错误遵循stderr/exit2，不是有JSON的测量回执。SIGINT exit130不是采样完整完成证明。

部署未解决项：`_audit_helpers` 与 source_hashes 目前以本模块 parents[3] 指向显式源码checkout及其scripts；installed/site-packages 安装位置可能没有这些脚本，尚未支持/验证。当前CLI相邻src bootstrap仅在完整checkout有意义，不代表wheel/site-packages或正式systemd可运行。正式交付需另完成helper/package资源定位及安装布局反例，不能把本地checkout单测当installed acceptance。本轮实际执行器绝对路径是 `D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe`（工具现场sys.executable一致），不使用默认python。

红测试19 failed/exit1：artifacts/wp5-20261010/shadow-session-before.xml。v1 19pass、v2 26pass；首相邻slice104pass。新增真实小数据CLI test 首断言把合法 DUPLICATE 排除，107pass/1fail（final-v2.xml）保留；仅改该断言，源码未因测试问题改变。

最终命令：

```powershell
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -m pytest tests/test_shadow_session.py tests/test_w1_shadow_variants.py tests/test_w2_shadow_evidence.py tests/test_audit_frozen_a4_clocks.py --junitxml=artifacts/wp5-20261010/shadow-session-final-v3.xml
```

108 passed，exit0，8.71s；其中30个新实例。覆盖真实schema temp SQLite、source bytes前后相同、wronghash/精确snapshot/time/未来bar/错身份/eventid、持仓/validfrom/expiry/invalidated、部分scope/有界等候/census、重复/迟到/启动历史/跨日、outer key冲突、源缺失、engine/writer异常与重入隔离、正常 engine+独立 ledger hash链（缺shadow字段真实 DATA_LIMITED不触发，不重算baseline）、PIT UNKNOWN与CLI help/local tiny invocation。未跑全量；没有生产、模型、通知、网络、VM或Node进程接线，没有commit/push。

CODE=独立入口实现；TEST=离线局部/既有引擎账本测试通过；OPERATIONS=未接systemd/未自然盘中采集；STRATEGY=执行字节/门/路由不变、无成交或收益结论。decision14/systemd与真正PIT/arrival adapter仍独立未完成，不以本切片宣布W1/W2整体完成。

| 冻结文件 | SHA256 |
| --- | --- |
| runtime/shadow_session.py | 0eadbb313817c2882ca09241a7861a2d35431910b26083c3cb3e275ea4e1d489 |
| scripts/run_shadow_session.py | 8371805a5e44088adce373dff444bebfd6d2474dd919cba33aed1a23df411e16 |
| tests/test_shadow_session.py | 3ddba3a253c9720d163e5fffdbabf6818832d86542a132a557e2347a4e1900db |
| before.xml | d276169bd7ca38dfedbfdffc3b9b406c12edb078816a8641ba6df9fc867b4369 |
| final-v3.xml | 33bd8da2a3bfd8ed19c6fe59a50946303627c42d5b92d5a2ef6ed1194e5288ca |
