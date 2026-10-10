# W3 独立报告运行入口（0061，尚未部署）

本切片提供 `scripts/run_shadow_reporting.py` 和独立 timer 模板，不接生产 JobRunner、不重跑 A4/A5、不构造 RuntimeStore/Settings、不发通知。CODE 与本地 REPLAY 已验证；OPERATIONS 未运行，STRATEGY/成交收益没有通过结论。缺 PIT、成交 provider 和 derived outcome 消费仍为 DATA_LIMITED，仅允许技术触发研究。

## 读取与原件绑定

`ReportingPaths` 显式指定生产 state DB、独立 shadow DB/JSONL、lane、A5 原输出目录、原 Node DTO/日志，以及三个独立输出目录。`read_reporting_day` 只通过 SQLite URI `mode=ro` 的短一致性事务读取 `execution_plans`/`monitor_events`，设置 query_only 和 SQL 进度预算；不对活库使用 immutable、checkpoint 或文件拷贝。源码中的 2s 是读取接受预算，不是文件 IO 硬抢占。

实际 activated census 取同日 valid_from 的计划，不把最终 EXPIRED/INVALIDATED 状态补成历史 PENDING。原 payload_json UTF-8 字符串 SHA 单独保留，不通过重编码冒充原字节；交易事务 canonical SHA 不是活 DB 文件 SHA。窗口来自真实 valid_from/expires 和有效 PLAN_INVALIDATED minute_end，不以 updated_at 推定失效时刻。覆盖为显式 lane 的计划域，09:31–11:30/13:01–15:00；非此域的持仓管理事件不冒称全账户覆盖。计划域外的原事件保留且标 gap，不悄悄缩小分母。

生产等价证明对原 outer action/reason/event_id/整行 canonical SHA 与影子 baseline_records 比较，没有重新执行生产策略。scope、缺记录、错 hash 都维持 DATA_LIMITED/差异，MATCHED 仅说明实际比较域，不证明部署或完整全天。minute 中原 plan payload byte pin 缺失仍另列 PLAN_PAYLOAD_BYTE_BINDING_UNPROVEN。

A5 复用 `shadow_a5_observer`：同日 POST_CLOSE ledger + 原固定 JSON 输出的 input_hash/output_hash + Node 成功且非 skip 的真实 finishedAt DTO + 唯一非重叠日志区间联合绑定。DEGRADED 是报告质量，不是进程失败。lease completed_at、终态日志 timestamp、health、不同日期 report 不得替代真实完成钟。可选 observation_clock 在读源后再核时钟和 16:45 边界，原 finishedAt 不改。

**P0 来源缺口**：现有 Node 精确 recentJobRuns DTO 没有自动持久化原件入口。overview GET 会调用 status（可能构造业务 Store），故本 CLI 没有 GET 或隐式发现。部署参数 `--node-receipt` 必须指向已授权的真实 DTO 原件；缺失只能 WAIT_A5→16:45 兜底，不能宣称生产成功后自动正式化。后续最小方案是独立原 DTO 留证接口/文件，经审查后接线；本切片不实施它。

## 调度与输出

15:30 独立只读冻结 draft.json + draft-receipt.json，交易所日历硬门不改。首次晚启明确 LATE_DRAFT；首次读取跨 16:45 后以读源后实际钟兜底。正常 A5 观察完整且不晚于 16:45 才正式化；否则草稿附 A5_NOT_COMPLETE_OR_AMBIGUOUS。formal_written_at 的 basis 明确是输出写入前的接受钟，不冒称同步 IO 完成钟；文件写入无法硬抢占，输出失败只属于影子，RuntimeMaxSec 也不是 A5 的预算。

输出包括独立 archive/<day>/report.json、final-manifest.json、docs/shadow/SHADOW_DAILY_<day>.md、PRODUCTION_EQUIVALENCE_<day>.json，以及独立 shadow_outbox/<day>/W3_SHADOW_DAILY.json。不是 canonical 编号 inbox/outbox，也不改 bridge state。文件 O_EXCL 创建、拒绝路径 alias/symlink、每个原件 SHA 绑定；重复运行验 manifest 和完整输出 bytes 后幂等，不覆盖。部分写入失败不自动修复/删除，需保留证据再单独处理。

独立账本缺失不写成 0 触发/0 成交：输出 gap 报告、数量/收益缺腿 null。既有原 observed 数据保留审计，但当前 qualified fill_count/fill_rate/收益均值等保持 null。technical_research_cohorts 按 inputs_origin × variant × profile 分组；仅带真实 isolated engine basis 且 BUY/ADD 动作的 technical_trigger 去重，不能用自报布尔值换成交首次触发。PLAN_FROZEN 与 SHADOW_PREOPEN_SIDECAR 不混 cohort。

周五16:00 单独 `--week-session` 五个真实连续交易日，复用纯 build_shadow_week。当前日正式稿未到可以取已 pin 草稿并标 draft_days；缺日/缺账本不填0。不可覆盖既有周报；后续正式修订应使用另命名快照。

## 本地部署模板，不是安装回执

`config/deploy/shadow-reporting/` 提供 daily15:30/16:45、weeklyFriday16:00 service/timer 与独立 env 示例。所有目录/User/Python 都是待决策14核验的占位符，没有隐式 /www 路径。模板限制写目录为独立账本/报告/桥接目录，不改生产 env/DB/scheduler。systemd 语法、目标 OS 权限、真实路径和单位安装未在 Windows 单测中验证，也没有 VM 安装/restart/daemon-reload。

新增明文一次性 `liangjian-shadow-preopen-20261012` timer：2026-10-12 08:40 Asia/Shanghai，Persistent=false，调用冻结的 `run_shadow_preopen_sidecar.py --target-trade-date 2026-10-12`。SHADOW_FACT_DB 是现有只读 LocalFactCache，PREOPEN_INPUTS_DB/唯一 PREOPEN_RECEIPT 仅独立目录。适用周一旧计划缺字段，不硬编码19作成功分母；已有 PLAN_FROZEN 不重写。原 builder 的生成 <09:00 门仍不变，没有 recurring A1/研究或网络补数。

一次性 session timer 为10-12 09:30，命令显式 `--preopen-inputs-db ${PREOPEN_INPUTS_DB}`、`--monitor-latest ${SHADOW_MONITOR_LATEST}`，`After=...preopen...service` 仅排序，不加 Requires。先确认 sidecar 原 receipt，再按决策14安装/启动 session；sidecar 缺失时现有 CLI 会给 SETUP DATA_LIMITED/exit2，不启动“假完整”session，不影响 A4。sidecar 存在但个别源缺失则保留逐计划 DATA_LIMITED。PIT/成交默认不可得仍明确，绝不称实时可交易。唯一 receipt 不能复用；同日进程重启需另新路径，不自动覆盖。

## 真实测试与边界

解释器：`D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe`，PYTHONPATH=src。

命令：`python -B -m pytest -o addopts= -q tests/test_w3_shadow_reporting.py tests/test_w3_shadow_a5_observer.py tests/test_w3_shadow_close_schedule.py tests/test_w3_shadow_daily_cli.py tests/test_w3_shadow_week.py tests/test_w3_shadow_day_adapter.py tests/test_w3_shadow_accumulation.py --junitxml=artifacts/w3-reporting-20261010/related-final-v2.xml`；native exit0，111 passed。真实 CLI `--help` native exit0。本地 subprocess 的16:45兜底与原 DB 前后 bytes 不变已测，不是生产自然报告。

反例原件保留：before.xml 缺模块 exit2；seal-before.xml 3fail exit1；clock-before.xml 2fail exit1；missing-before.xml 1fail exit1；technical-before.xml 2fail exit1；preopen-before.xml 1fail exit1；draft-fence-before-v2.xml 1fail exit1。draft-fence-before.xml 最初没有暴露缺陷而通过，仍保留，不谎称红；second.xml 的1fail是 fixture spy 参数名冲突，不算业务缺陷。

本切片没有全量测试、六日黄金重跑、真实源获取、生产安装或自然触发/收益；统一集成全量与决策14由根代理负责。
