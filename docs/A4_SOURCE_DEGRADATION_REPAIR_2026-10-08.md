# A4 盘中数据源降级：诊断、修复与验收

## 结论与边界

本次不是通过放宽数据要求或策略条件提高通过率，而是修复取数预算、等待链和故障恢复。代码已在隔离分支 `fix/a4-source-degradation-20261008` 落地。基线 HEAD 为 `23eb4187304e5fd8bad386da95d04332f4eb91ea`，生产只读核验 HEAD 为 `41af0333abf5626fe51e3940b852f81aa293292d`。

最终核心及执行回归 289 项通过；请求证据字段补齐后相关回归 87 项通过，两组包含重复用例，不相加宣称总测试数量。本次精确文件集合已提交到本地隔离分支，保留无关 artifacts。本轮没有推送、合并、部署、重启、写生产数据库、调用模型、补发信号或通知，没有修改策略阈值。

附件《20261003_影子系统数据源审计_v2_对照首板B系列.md》已完整读取。吸收的是分清报价、真实一分钟线、派生周期及辅助复核的职责，以及请求级证据账本；附件中的 GM 排队、20ms 配置和 B 系列统计来自另一个系统，不能直接归为本系统根因，也没有移植其代码。

## 一、今日生产事实

证据截止北京时间 2026-10-08 15:34:40，原始冻结记录不改写：

- 237 个执行分钟，7,585 条记录，54 条数据阻断。
- 33 条 `TENCENT_QUOTE_REQUEST_FAILED`：报价失败，不等于一分钟 OHLCV 全部失败。
- 14:59 有 13 条 `TENCENT_REQUEST_FAILED`、8 条 `MINUTE_FETCH_DEADLINE_EXCEEDED`：共 21 个一分钟输入缺口。
- 缺少 10:02、10:03、15:00 三个执行分钟。10:03:54 有控制面启动记录，上午缺口对应重启。14:59 任务在 55,041ms 后被 Node 以 SIGTERM 终止；15:00 返回 `LEASE_BUSY`，因此没有生成该分钟账本。
- 5 次计划结构失效；没有买入或模拟成交。多数记录仍是技术条件不满足，不能把无信号全部解释为数据源问题。
- 午间 A5 已生成 `DEGRADED` 报告并有 `SENT` 投递记录。
- 今日另有一分钟健康通知失败 1 条、数据告警失败 2 条。本次不冒充已修复通知投递；仍需分别检查投递失败码。

生产 SSH 解析为 `192.168.1.254`，主机 `debian`，目录 `/www/wwwroot/Agu/liangjian-funnel-workflow`。所有数据库读取使用只读 URI。15:10 自然收盘研究正常推进，本次没有中断它。

## 二、重新核实的根因及修改

### 1. 主源卡住占用备源预算

原流程先重试同一主源，再访问独立来源。套接字的连接、读取 timeout 不等于函数总耗时上限，调用不返回时可能耗尽整轮预算。

`data/live_fetch.py` 调整为主源一次、独立备源一次、仅对暂态错误重试主源。每次都有单独绝对截止时间，并受整轮截止约束。主、备源有独立的有界并发槽；不可取消的迟到工作只保留有限 daemon 槽，不能无限积累线程，也不能把迟到结果加入冻结输入。TDX 节点仍在实际调用返回前保持占用，避免超时后并发重用同一节点。

保留每次请求的提供者、主备角色、请求/返回股票、请求数量、返回数量、首末 K 线、应有末根、发出/收到时间、耗时、失败码及安全的异常类型。失败请求不再只剩最终成功来源。

### 2. 发布确认退出时仍等待全部工作

原 `ThreadPoolExecutor` 上下文在退出时等待未完成线程，造成调用层虽然设定截止时间、退出阶段却继续等待的冲突。

`data/publication.py` 改用现有 `run_many_bounded`，各股票独立终结，不再触发 wait-all 退出屏障；发布确认不完整、身份不符或迟到的结果仍拒绝。未调整收盘定稿宽限、完整性门槛或稳定性要求。

公共 `runtime/bounded_work.py` 同时拒绝截止时间后才到达的 READY 值，包括队列边界竞态，保留全局和每批并发上限。

### 3. 报价超时连带丢弃成功 K 线

原每股任务串行取得一分钟线、再取得报价；后者卡住时外层只能看到整项失败，已成功取得的 K 线也不能进入原决策包。

`workflow.py` 先取得并确认真实一分钟 OHLCV，再使用独立报价并发槽附加报价。报价失败可以阻断需要当前报价的新增仓，但不能改写成一分钟线缺失；已取得的 K 线保留供记录、技术计算与复盘。已有持仓优先风险通道、硬止损、47 秒整轮预算及三策略条件保持原样，报价也不充当成交价格证据。

### 4. 被终止进程留下租约，下一分钟无法恢复

原 owner 为固定字符串，无法判断旧租约是否属于活进程；14:59 被终止后下一分钟被旧租约阻挡。

增加 `runtime/process_identity.py`，Linux owner 记录本机标识、启动 boot、PID 和进程启动 tick。调度仅在“同机、能证明旧进程已退出或 PID 已复用、旧任务时间严格早于当前分钟”时按旧 owner 条件释放，再走既有原子 acquire。活进程、异机、不明身份和无权限状态一律不抢占。只恢复下一分钟，不补跑被杀分钟的历史买点，不缩短 TTL 制造并发执行。

旧版本产生的静态 owner 不具备可证明的进程身份，不能伪装成可安全回收；发布后的新任务才可获得这一恢复能力。

### 5. 实时报价没有独立执行备源

新增 `data/live_quote.py`：同一绝对预算内有界获取报价，并严格复核股票、有限数值、交易日期及现有 90 秒新鲜度。腾讯解析不再把返回的其他股票重命名为请求股票；新增新浪报价适配器保留提供者时间、累计成交股数和金额，不能替代真实一分钟 K 线，也不具备盘口模拟成交权威。

配置 `LIANGJIAN_A4_QUOTE_BACKUP_MODE` 默认为 `SHADOW`，默认不会从执行链请求新浪。显式 `SINA` 才允许进入独立备源链，本轮未修改生产环境配置，也未授权晋升。

15:49 的生产网络旁路探测：腾讯与新浪均返回 HTTP 数据，但新浪时间为 15:34:59，已经过期，被 `QUOTE_NOT_CURRENT` 拒绝；腾讯时间为 15:49:23。该收盘后单样本只能证明当次传输和解析，不能证明盘中可用率或成交真实性。两源价格相同不是启用过期来源的理由。该备源是否可晋升，仍需盘中按收到时间验证。

## 三、理顺后的执行口径

| 数据职责 | 可用时做什么 | 不可用时做什么 |
| --- | --- | --- |
| 当日真实、已闭合 1m OHLCV | 严格完整性及发布检查后聚合本地 5m/15m，供策略与模型同包使用 | 按股票阻断相关执行；保留具体缺点和取数尝试，不填前值、不伪造 K 线 |
| 当前实时报价 | 当前价与持仓硬风险判断；独立来源仍逐项验证 | 独立记录报价降级；保留已成功 K 线，不假称 OHLCV 也失败 |
| 原生 5m 辅助复核 | 现有旁路核验及冲突证据 | 不作为整个一分钟执行链失败；已有依赖契约不取消 |
| 下一根完整 1m 模拟成交依据 | 原模拟撮合、费用、T+1、订单生命周期 | 不用信号价或实时盘口冒充原成交模型 |

增加不可变逐分钟 `source_health`：股票/决策时点、闭合截止、期望/实际末根、缺失时间点、内容哈希、各请求和报价提供者时间分开保存。明确标注辅助源非执行依赖；`REQUIRED_INPUTS_READY` 只代表这部分输入到齐，不代表指标预热、风险、策略、模型或订单已经通过。

新增只读汇总入口 `scripts/report_a4_source_health.py`，按决策时点＋股票去重；旧证据没有新健康字段时标为未知，不输出假的绿色健康结论。新增 `scripts/probe_live_quote_backup.py` 仅执行显式公共报价旁路探测，不派发 A4、模型、订单或通知。

## 四、需求—测试—证据映射

| 要求 | 反例/回归 | 本轮证据与结果 |
| --- | --- | --- |
| 主源挂起不饿死独立备源 | `test_hung_primary_cannot_consume_independent_fallback_budget`、`test_hung_quote_primary_does_not_starve_backup` | 修复前窗口测试失败；修复后通过，有界返回并保留主源失败 |
| 发布确认不等待挂起项退出 | `test_publication_retry_returns_before_hung_dependency_and_keeps_good_symbol` | 好股票不受挂起股票拖累，错误项仍阻断 |
| 迟到工作不得进入冻结包 | `test_dependency_result_after_absolute_deadline_never_enters_frozen_packet` | 单项及批量 gate 均返回 TIMED_OUT，无值 |
| 报价失败不抹掉真实一分钟线 | `tests/iteration/test_a4_orchestration.py` 挂起报价参数化用例 | 已成功 K 线仍归档，不误报 MINUTE_FETCH 失败 |
| 过期/错股票/缺报价/非有限值拒绝 | `test_fallback_old_day_remains_blocked_and_attempts_are_preserved`、新浪旧日/未来测试、腾讯错股测试 | 失败不降门槛；15:49 新浪真实旁路样本仍拒绝 |
| 辅助故障与必需缺口区分 | `test_source_health_auxiliary_failure_is_not_required_input_failure`、汇总去重测试 | 辅助失败不污染必需输入状态，真实缺点仍阻断 |
| 失活租约安全恢复下一分钟 | 进程身份 7 类反例、`tests/test_runtime_scheduler.py` 新恢复用例 | 已死同机恢复、活进程/未知身份不抢占；Linux 只读进程探针通过 |
| 三策略/模拟/T+1/风险不变 | runtime/strategy/simulation/workflow/settings/minute 回归 | 289 项通过；策略文件规范化内容哈希与生产一致 |

其中 `test_primary_only_publishes_before_idempotent_comparison_enqueue` 原测试 fixture 以 `object.__new__` 构造 app 却缺少正式交易日历，本轮宽回归暴露后只补齐 fixture；`run_research` 函数 AST 与基线一致，没有为测试放宽生产研究逻辑。

## 五、真实冻结回放的限度

冻结审计处理 7,585 条：1 条非策略，7,563 条可复算，21 条缺真实一分钟输入。7,530 条原完整策略判断的动作和首因一致。

另 33 条原报价失败在仅闭合 K 线复算中变成 `START_CONFIRMATION`，不是 BUY。审计没有还原当时已收到的实时价格，不能把这些结果当作当时可下单、备源可恢复或漏买证据。21 条缺口继续缺失，没有用后补行情“证明”当时可执行。没有修改历史动作、时间和收益。

本地 Windows 原始文件 SHA 与 Linux 因 CRLF 不同；规范化 UTF-8 策略内容 SHA 均为 `4cac45023dcb56b0063c2a9aaae29b7fa586116d965a7c8c2312e0b398592385`。两种哈希及核验范围分别留存，没有用 Git HEAD 代替实际导入代码核验。

## 六、命令与退出码

以下均在本地仓库执行，Python 为 `D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe`，`PYTHONPATH=src`、`PYTHONIOENCODING=utf-8`。审计脚本内部 SSH 只读生产，输出为新的本地文件。

1. `python scripts/audit_session_readonly.py --day 2026-10-08 --output artifacts/diagnosis-20261008/session-1535.json`：退出 0。
2. `python scripts/audit_frozen_a4_decisions.py --day 2026-10-08 --output artifacts/diagnosis-20261008/a4-frozen-available-windows.json`：退出 0；统计与限度见第五节。
3. `python scripts/probe_live_quote_backup.py --symbol 600189.SH --output artifacts/diagnosis-20261008/quote-backup-shadow.json`：退出 0；新浪报价新鲜度拒绝。
4. 修复前 `python -m pytest tests/test_source_degradation_recovery.py -o addopts='' -q --tb=short` 的首个反例切片：退出 1，2 失败、1 通过。
5. 核心扩大回归：退出 0，143 通过；随后 workflow 扩大回归退出 1，227 通过、1 fixture 失败。补 fixture 后对应切片退出 0，91 通过。没有把失败隐去。
6. 最终回归：

```powershell
python -m pytest tests/test_runtime_monitor.py tests/test_runtime_strategies.py tests/test_runtime_state.py tests/test_runtime_simulation.py tests/test_simulation_coverage.py tests/test_workflow_integration.py tests/test_workflow_orchestration_coverage.py tests/test_workflow_lark_notifications.py tests/test_monitor_version_wiring.py tests/test_minute_cache.py tests/test_minute_quality.py tests/test_settings.py tests/test_source_degradation_recovery.py tests/test_runtime_scheduler.py tests/iteration/test_a4_orchestration.py tests/test_minute_publication.py tests/test_tencent_minute.py tests/test_a4_runtime_repair.py tests/test_minute_version_contract.py tests/test_live_market_state.py -o addopts='' -q --tb=short -rs
```

退出 0，**289 passed in 24.04s**。

7. 请求证据角色/提供者字段补齐后的最终切片：

```powershell
python -m pytest tests/test_source_degradation_recovery.py tests/test_minute_publication.py tests/test_live_market_state.py tests/test_runtime_scheduler.py tests/iteration/test_a4_orchestration.py -o addopts='' -q --tb=short
```

退出 0，**87 passed in 6.76s**。

8. `git diff --check`：退出 0。

完整本地证据目录：`D:/dev_A股/liangjian_a4_20260923/artifacts/diagnosis-20261008/`。包括 `session-1535.json`、`a4-frozen-available-windows.json`、`frozen-audit-classified.json`、`strategy-source-identity.json`、`linux-process-identity.json`、`quote-backup-shadow.json`。不提交凭证、生产数据库或整个 artifacts 目录。

## 七、验收状态与下一步

- **CODE**：修复和比例相称的反例/回归已通过；隔离分支未发布。
- **REPLAY**：可用冻结 K 线窗口已核对；缺口、实时价格到达和成交因果性未伪造通过。
- **OPERATIONS**：生产仍 41af033；本轮仅只读诊断。新健康账本、租约身份及修复链尚未经过生产自然调度验收；通知失败另待诊断。
- **STRATEGY**：三套策略、阈值、硬止损、模拟撮合及 T+1 没有修改；本轮不能证明收益提高或应新增买点。

下一条具体操作：获准发布后，在现有收盘工作流自然结束、无并发任务且 deploy.sh 时段保护允许时，按现有发布流程发布本地已验证提交；新浪仍保持 SHADOW。核对生产 HEAD、实际导入文件哈希、下一自然分钟任务、进程 owner 和健康证据，随后用下一交易日逐分钟自然账本验收。任何源晋升先取得盘中时效/覆盖证据，再单独更改配置，不因为本地模拟备源成功就声称生产已恢复。
