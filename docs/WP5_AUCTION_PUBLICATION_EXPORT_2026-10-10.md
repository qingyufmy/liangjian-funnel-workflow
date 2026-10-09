# WP5 竞价晨审域：本地只读 publication/positions reader

本切片基于 `2479b2dce3fc3218a14e32877fc5d0c4e0b2a403`，完整读取 canonical bridge `0006/0007/0008` 及 `artifacts/wp5-20261010/auction-source-adapter-design-2479b2d.md`。只新增 `runtime/auction_publication_export.py`、对应测试及本文；不改 workflow/research、现有 auction_preparation 或 scheduler，不提交、不调用网络/模型/通知，不读取生产运行库。状态继续 `IMPLEMENTATION_PARTIAL`，`eligibility_released` 恒 False。

## 范围和输出

reader 接受本地一致化 SQLite 副本、外部完整 DB SHA256 pin、明确 observation 时间、原 publication summary 与 lane audit 的路径/字节 SHA pin、目标交易日和显式账户集合。正常构造，无 RuntimeStore/Settings。晨审声明域为原成功 close 的目标日 PENDING_MORNING_REVIEW ∪ 指定账户正持仓；不包含全域公告、A1 重建或研究资格变更。非 A1 的正持仓仍留在风险域，账户状态仅如实记录，不当执行权限。

`LOCAL_DECLARATION_BOUND` / `scope_complete=True` 仅表示本地声明绑定通过，不叫 READY，不是 source LIVE、自然日 REPLAY、历史 pending 或交易权限。任何缺项为 DATA_LIMITED 或明确 ExportError。publication 一部分计划非法时不放出该 publication 的局部计划，保留仍有效的持仓风险观察并标不完整；跨 publication 若任何 gap 也不能把全域当完整。现有 fast_preflight 未接本输出。

输出包括 DB before/after SHA、schema SHA、所读原行集合 SHA、原引用路径/字节 SHA、原观察计划状态、选中计划 payload 字节 SHA、A3 原计划 canonical SHA、账户/正持仓、目标/前交易日、日历库版本、gap 及完整 receipt SHA。不输出模型名称、原模型文本、outcome_json 或账户资金。rowset hash 内部绑定完整所读行但不泄露其文本。

## 本地 SQLite 边界

`freeze_local_sqlite(source, expected_sha256, observed_at, output)` 对已冷冻本地文件走 SQLite backup：源以 `mode=ro&immutable=1`、query_only、BEGIN 打开，quick_check 验证；目标 exclusive create；源 bytes before/after 相同；完成得到目标字节 pin。不会覆盖已有目标，也不迁移表或更新源。它不是在线生产 DB backup 接口，不对 active WAL 做单文件复制；任何 `-wal/-shm/-journal`（包括空文件）均拒绝。失败只移除本次 exclusive 新建的目标。

`ReadOnlyPublicationReader(FrozenDatabase(...))` 每次 read 同样做 immutable/ro、query_only、trusted_schema=OFF、BEGIN、quick_check、sidecar 检查和 before/after 字节 pin；表必须是真实 table 且包含 actual required columns，额外迁移列允许并纳入 schema hash。输入文件不变不等于 observation 时钟已认证；调用方给出的 acquisition/observed_at 来源在本模块不认证，所有 historical_*_proven 恒 False。不把本地冷副本描述为实际历史会话留证。

## 实际 schema 与发布证据

已只读核对 WP1 既有冷副本 `D:/dev_A股/liangjian_wp1_20261009/artifacts/wp1-20261009/readonly-source-20261009/workflow.sqlite3`，外部 pin 为 `42d2854bd28fa64ade8ae02f89f6cd622bddaab2bf3d75723f07ee402f0d62e4`，以及 `runtime/state.py` 实际建表/迁移：

| 表 | 本 reader 使用的真实列 |
| --- | --- |
| execution_plans | plan_id, lane_id, symbol, status, plan_version, valid_from, expires_at, payload_json, created_at, updated_at |
| workflow_runs | run_id, lane_id, trade_date, slot, status, snapshot_hash, created_at, updated_at；实际另有 model/prompt_hash/config_hash/reason_codes_json/outcome_json |
| workflow_stages | run_id, lane_id, stage, status, updated_at；实际另有 reason_codes_json/outcome_json |
| virtual_accounts | account_id, status, created_at, updated_at；资金/model 不作 DTO 输出 |
| virtual_positions | account_id, symbol, total_qty, sellable_qty, updated_at；实际另有 avg_cost/stop_level/plan_id |

DB slot 实际为 `CLOSE`，summary slot 则由 workflow 持久化为 `close`；reader 分别验证。`workflow.py _publish_plans` 使用 `publish_plan_batch` 后再 `mark_workflow_runs_published`，不是一个共同事务，计划行存在不能证明成功 run。reader 必须核对原 run/lane PUBLISHED、前交易日/close、成功 summary READY/READY_DEGRADED、primary/full 角色与 primary lane、publication CLOSE/atomic/created IDs，以及实际 DB A3 与原 audit 同状态。`research/common.py _PUBLISHABLE_STAGE_STATUSES` 真实允许 VALIDATED/VALIDATED_NO_SETUP；仅此集合可用，不把所有 completed/degraded 状态视为可发布。

原 lane audit 用 `LaneResult.as_dict()`；final_output 与 A3 output、A3 output_hash 都复算一致；created IDs 必须对应该原池计划按真实 `{run}:{lane}:{logical_id or raw_hash[:16]}` 生成的 ID，且与该 source_run_id 的 DB plan 集合完全对账。symbol、payload source_run_id、payload symbol、lane、时间、日期、状态均核对；摘要/行缺失或歧义不填字段、不猜列、不借 created_at 或 plan_id 前缀推历史状态。

## 保守日期合同（root 已确认，桥接 0009 待外部校准）

源码事实：普通 close 的 `summary.target_trade_date` 可为 None（workflow summary）；`_plan_payload` 保留原模型 plan_expiry；`_plan_expiry` 则由服务器真实 next-trading-day 日历设目标日 15:00 最小 horizon，可能不重写旧 payload 日期。因此 reader 不要求不存在的 DB target column，也不修正旧 payload。

有显式 summary/payload target 时都必须等于目标日。无显式 target 时，仅成功原 close summary、真实前后交易日历、服务器 expires_at 目标日且恰为15:00共同成立，才记 `TARGET_BOUND_BY_SERVER_EXPIRY`。payload plan_expiry 若存在，必须为 aware 时间且与服务器 expiry 完全一致；缺该字段不补写，仍可由服务器 horizon 声明目标。未来/错误目标、过期、valid_from 日期冲突或 payload 旧日期均 gap；不自动采纳服务器日期覆盖模型冲突。真实 `_plan_expiry` 也有“原expiry>=minimum且同目标日则返回原值”分支，源码确可产生15:30/16:00；本切片保守只支持默认15:00，不把later分支未经独立归一化血缘核验纳入支持范围，不声称生产代码只支持15:00。

## 原计划与当前 DB payload 对账

单凭当前 DB payload byte pin 不能证明它仍是那次原 A3 计划。源码 `workflow.py:8189 _plan_payload` 保留 `**dict(raw)`，但规范化 symbol/trigger_low/trigger_high/stop_level/no_chase/confirmation_bars/action；`_publish_plans` 在其上加 source_run_id、TREND_MA5 的 trend-ma5/2，并可能从 A2 authority/snapshot context 改写 execution_permission/research_only_reason、添加 a2_*。`state.py:1964 publish_plan_batch` 只 `_json` 序列化，不保存原 payload 子对象/中间归一化 receipt；已有plan_id内容变动会 PLAN_ID_CONTENT_CONFLICT，但只读旧副本不能仅凭现状断言从未被另路径改过。

reader 对 raw 未改写字段逐项 canonical 精确对账；几何 alias 按真实正浮点转换规则对账，不把字符串价和规范化浮点价误判成 raw exact 相等。confirmation/action/source/TREND版本按源码对账。原 trigger_zone/strategy 等保留字段或 stop/trigger alias 变化为 PLAN_AUDIT_PAYLOAD_CONFLICT。A2权限派生原authority未包含于本切片，因此权限改变/添加 a2_*、未解释附加字段均 PLAN_AUDIT_PAYLOAD_BINDING_UNPROVEN，不根据“ALLOW_A4”字符串给完整性；后续要原 A2/snapshot context 或实际中间 receipt 验证。几何源非有限值也不臆测。本切片绝不导入 workflow 去绕构造器调用私有规范化函数，不重新实施策略选择/校准。

合法空 publication 必须原成功 PUBLISHED run + 成功 A3/audit + 原 summary 明确目标日 + created=[] + 原 DB 该 run/lane 没有 linked plans。无 row 无法靠 expires_at 推目标，因此 summary target 缺失给 EMPTY_TARGET_UNPROVEN。没有 publication reference、失败 run、缺账户/表、不完整持仓都不能当合法零域。整个 `empty_domain_proven` 还要求所有显式账户完整且无正持仓。

seal 前再次验证 summary/audit 字节 pin；物化后变化为 REFERENCE_CHANGED，不保留一个已变化路径指向的 ready 声明。同lane目标日其他source的pending为 OTHER_PENDING_PUBLICATION_PRESENT，无法归属的pending为 UNATTRIBUTED_PENDING_PLAN。依据原 `publish_plan_batch` 的 close complete-replacement（含合法empty）语义，若实际同lane/CLOSE/前交易日/PUBLISHED有别的run，且created_at比原ref更晚而不超过observation，保守 PUBLICATION_SUPERSEDED；仅标gap，不自动改选run、不根据updated_at推历史发布时刻。该边界已由root确认。查询到的候选记录和lane行扫描也进入rowset hash。

## 派生 overlay 和原 10-08 数据受限

真实原 lane 文件 `artifacts/wp5-20261009/readonly-20261008/research_2026-10-08-close-a2-audit-172755_lane_1.json` 字节 SHA256 为 `9dda1540cf45ad2c0f0ba4806de0b984d59a50405786cd87785d0921f41e812b`。A1 ID 为基础 snapshot；A2/A3 ID 为多级 overlay；A3 VALIDATED，final_output 等于 A3 output。3 个 core 原 plan_expiry=2026-10-08 15:00，服务器 DB horizon=10-09，存在真实冲突；2 个 secondary expiry=None。这些原数据没有修改。

该目录未见该 run 原 publication summary。本轮不拼接其他 run/DB 或造摘要，因此不能用此样本称真实晨审导出通过。A1 base ID 匹配、原 A3 audit byte pin 和 overlay ID 可记 BASE_ID_PLUS_PINNED_AUDIT_OVERLAY_DECLARATION，但仍 AUDIT_OVERLAY_BINDING_UNPROVEN；仅有前缀/known 标签/任意12hex后缀不是原派生快照 hash-chain proof。当前 reader 没有 overlay 原文件验证入口，不能移除该 gap；需要另切片核验实际原派生文件/内部链后评审。

9-30 的47行在既有 observed artifact 中为31 EXPIRED+16 INVALIDATED，不能改成当时 PENDING；历史 REPLAY 按0008为 DATA_LIMITED。真实冷 DB 在本 reader 无原 publication reference 的只读运行 exit0，返回 DATA_LIMITED/PUBLICATION_REFERENCE_MISSING、3账户、0正持仓、source_unchanged=True，不证明合法empty；最终代码结果保存在 `artifacts/wp5-20261010/auction-publication-export-real-missing-reference-v2.json`（旧版结果不充当最终代码回执）。其 observed_at 是显式请求标签，不冒称认证的历史 acquisition 时钟。

## 测试和使用

反例先写测试再实现：PATH 中 python 无 pytest 的初次命令 exit1 仅环境错误，不算合同反例；使用项目既有 venv 后，`auction-publication-export-before.xml` 为缺模块 collection error/exit1，不宣称每个反例都分别失败。首次实现后再加7个实质反例（4种当前DB stop/trigger/strategy篡改、A2 authority缺失、15:30/16:00），旧reader真实7fail/exit1记在 `auction-publication-export-lineage-before.xml`，修复后65pass/exit0。另有读后reference变化/其他source pending/旧empty掩盖新run3fail/exit1的收口反例。测试还涵盖旧状态、部分非法范围、真实日期冲突、成功合法empty/缺target、A3 publishable状态、摘要/输出/DB hash、账号/持仓、sidecar、只读 SQL、源变化、exclusive backup/archive、overlay 任意后缀、无网络/Store/Settings import 及不输出模型文本。均为明标本地 fixture，不等于自然会话。

```powershell
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -m pytest tests/test_wp5_auction_publication_export.py tests/test_wp5_auction_preparation.py tests/test_runtime_calendar.py --junitxml=artifacts/wp5-20261010/auction-publication-export-final-v3.xml
git diff --check
```

公共入口为 `FrozenDatabase` / `FilePin` / `PublicationReference` 的正常构造，`ReadOnlyPublicationReader.export(...)` 返回声明；`write_export(path, result)` 复算 receipt hash 后 exclusive 创建归档。不提供 Settings/RuntimeStore 默认路径或生产 CLI，不接现有资格/fast/scheduler。原 FIS/A1/close scope、异构慢源/快源 adapter、绝对 deadline worker、真实 pending 自然样本、OPERATIONS/G7 均仍待独立实施/授权。

最终上述 slice 命令为129 passed（72 reader本地合同、55既有竞价合同、2既有交易日历）/exit0，JUnit为final-v3。这是比例相称的本地回归，不是新HEAD全量或自然交易日验收。三个新增文件交回后冻结，由root单独评审、显式提交和HEAD全量；本子任务不提交。
