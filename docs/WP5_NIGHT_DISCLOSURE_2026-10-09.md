# WP5 任务 2：夜间公告补采隔离切片

## 当前结论

基于 `63748cce11c8d5b3c2b2d3f38279c36e29783fd4`，已实现原批次绑定队列、默认只读的独立入口、缓存采集适配器和逐股恢复账本。**正式收盘公告采集仍为原范围，22:00 自然调度未启用。** 没有推送、部署、生产查询/写入、模型调用、真实通知或历史研究补跑。

范围仅限已授权的 WP5 任务 1/2；用户任务书没有改动。此文件接续 [日线与预筛切片](WP5_INCREMENTAL_PREFILTER_2026-10-09.md)，不覆盖此前证据。

## 实现与实际调用路径

`prepare_snapshot → 原收盘范围回执 → 公告前确定性预筛 → seal_maintenance_queue`

队列在公告查询之前保存，因此后来 GOV_POLICY/CNINFO/PDF/模型失败也不抹去已经形成的工作域。它绑定原任务 `run_id`、原 `a1_generation_id`、原范围回执哈希、预筛哈希、真实研究时间和闭合市场日期，保留原始来源集合；不能拿 18:00 新代际替换 15:10 的代际。不具备原 A1 引用时记录 `ORIGINAL_A1_REFERENCE_REQUIRED`，不猜代际、不生成可执行队列。

- `pipeline/disclosure_maintenance.py` 校验原回执、集合账本、来源、日期、候选/延期完整互斥分区和逐股状态。没有 300 只上限；不接收最终 A2 结果。
- 内容寻址队列已有文件冲突时阻断，不覆盖原文件。
- `scripts/run_disclosure_maintenance.py` 默认只读预览；只有显式 `--execute` 才初始化环境、官方客户端和事实缓存。没有完整 `WorkflowApplication.__init__`、RuntimeStore、A1 registry、模型、账户或飞书对象。
- `pipeline/disclosure_cache_worker.py` 复用 `_fetch_cninfo_candidate_queries`、`disclosure_incremental`、组织 ID 目录、官方备用路由和既有 PDF 选择/验证/业务抽取。维护任务串行逐股采集，不降低原限流；PDF 每股上限沿用配置，不改变策略门槛。
- 组织目录失败只记录稳定失败码，继续既有逐股查询，不把缺 orgID 变为空公告成功。完整空公告查询和查询失败仍严格区分。
- 公告缓存仅由既有完整查询/完整增量合成写入；PDF 刷新失败不替换已有成功证据。每股明确记录查询完整性、业务证据、PDF 失败、缓存引用及哈希；旧业务资料有价值不等于当前查询窗口完整。

执行账本独立保存在操作者指定的输出目录：

```text
<queue-hash>-attempt-<unique-id>-<symbol>.json
<queue-hash>-report-<unique-id>.json
```

每次尝试与汇总均新建文件。失败股票不会修改候选资格、原快照、原 A4 信号或 A1 代际。恢复必须同时满足同一队列/当日查询、六小时内有效成功回执、当前公告和 PDF 缓存仍完整且哈希吻合；跨日重新确定查询窗口，过期/删除/修订/未来知识不允许跳过检查。同一队列和同一事实缓存各有独占锁；异常退出残留锁不会自动删除，需按真实进程核实处理。

任务总预算默认 3,600 秒，**只在逐股工作边界检查**，达到后剩余股票记录 `MAINTENANCE_BUDGET_EXHAUSTED`；单股网络请求仍受既有客户端超时/重试限制。这不是可硬中断所有网络操作的全局截止证明，也不是 WP1 的全局截止验收。

## 运行入口

以下为入口说明，不代表本轮操作过生产。只读预览不会创建输出目录、事实缓存或客户端：

```powershell
$env:PYTHONPATH='src'
& 'D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe' scripts/run_disclosure_maintenance.py `
  --queue '<原批次内容寻址queue.json>' --output-dir '<独立维护账本目录>'
```

采集执行还需要明确授权以及原批次合法队列，届时在同一命令加入 `--execute`。未接入自动扫描“最新队列”，避免重复采集或悄悄替换代际。本轮未启用 22:00 scheduler，未调用免费接口做外部采集。

## 测试与证据

统一用既有 Python 环境，`PYTHONPATH=src`，所有数据库/PDF/账本在 pytest 临时目录；HTTP、PDF 使用假适配器，没有生产模型调用。

```powershell
& $py -m pytest tests/test_wp5_disclosure_maintenance.py -o addopts='' -q
# 实现前 exit 1，缺少 disclosure_maintenance 模块；实现后 exit 0，14 项通过。
& $py -m pytest tests/test_wp5_disclosure_maintenance.py tests/test_workflow_orchestration_coverage.py tests/test_workflow_fact_projection.py -o addopts='' -q
# exit 0，36 项通过（中间切片）。
& $py -m pytest tests/test_wp5_disclosure_cache_worker.py -o addopts='' -q
# 前两次 exit 1，假 PDF 缺必要字段；补齐合法测试夹具后，又发现测试用旧时间插入版本不能覆盖最新不可变版本。
# 按实际版本时间改正测试，不修改缓存排序/过期语义、不放宽模型。
& $py -m pytest tests/test_wp5_disclosure_maintenance.py tests/test_wp5_disclosure_cache_worker.py tests/test_wp5_disclosure_prefilter.py tests/test_wp5_close_scope.py tests/test_workflow_fact_result_cache.py tests/test_disclosure_incremental.py tests/test_workflow_orchestration_coverage.py tests/test_workflow_fact_projection.py -o addopts='' -q --junitxml=artifacts/wp5-20261009/night-maintenance-final-tests.xml
# exit 0，98 passed。此前 96 项子集不是额外通过数。
& ./scripts/test_all.ps1 -PythonPath $py -OutputDirectory artifacts/wp5-20261009/full-night-maintenance-tests
# 全量结果以下方更新记录为准。
```

需求—测试—证据映射：

| 需求 | 反例/验证 | 证据 |
| --- | --- | --- |
| 原代际与完整分区，不用最终 A2 定义范围 | `test_queue_binds_original_generation_and_partition_not_final_a2`、六类 `test_invalid_inputs_cannot_create_a_night_scope` | `night-maintenance-final-tests.xml` |
| 不截断、队列不可覆盖 | `test_queue_has_no_candidate_or_night_work_cap`、`test_queue_seal_does_not_overwrite_conflicting_original` | 同上；401 延期股票为构造输入 |
| 默认预览不触发环境/缓存/模型 | `test_dry_run_does_not_fetch_or_create_cache_or_output`、`test_cli_dry_run_never_loads_settings_or_workflow` | 同上 |
| 失败隔离、不可伪造成功、不可重复运行 | `test_failure_is_separate_from_original_scope_and_never_ready`、`test_incomplete_lane_cannot_be_success_or_resumed`、`test_lock_and_corrupt_resume_fail_closed` | 同上 |
| 恢复需当前证据，跨日重查 | `test_resume_requires_same_date_fresh_receipt_and_verified_cache`、`test_resume_rechecks_pdf_content_not_just_old_success_receipt`、`test_future_pdf_evidence_is_not_resume_proof` | 同上 |
| 复用正式采集与有效缓存，目录故障不伪造空查询 | `test_cache_only_worker_reuses_formal_queries_and_business_pdf`、`test_failed_wider_recent_query_preserves_cache_and_never_marks_ready`、`test_catalog_failure_does_not_turn_missing_identity_into_empty_success` | 同上；临时 SQLite/假官方返回/假 PDF |

## 尚未完成与下一条操作

1. 10-09 原收盘完整快照缺失，真实预筛覆盖仍 `DATA_LIMITED`；不能用后来的 A1 维护快照冒充。原 64 只核对仅证明本地条件命中，不证明原全局排名/送审集合。
2. 本切片的夜间工作域是 `SHADOW`，原收盘仍采集全部股票。需用完整冻结批次比较公告前预筛与 **所有正式 A2 量化送审路径**，出现 `SCOPE_MISS` 先修反例，再决定收窄查询和 22:00 调度接线。
3. 自动复权因子检测、3,975 请求的真实吞吐、连续五个自然交易日 60 分钟到 A3 仍未完成。不能将本地缓存命中或单元测试等同于生产提速。
4. 下一条具体操作：离线导出已有合法完整冻结批次的量化 A2 工作域，逐源对照预筛保留路径；缺证据的批次明确登记，不重新采集或调用模型。正式范围切换、发布、周末延迟研究仍需独立授权。

## 四层验收

- CODE：相关切片 98 项通过；全量入口退出 0，2,404 项中 2,397 通过、6 跳过、1 已登记 strict xfail、0 失败。Python 2,303 通过，Node 94 通过，typecheck 退出 0。完整 WP5 未完成。
- REPLAY：构造缓存/假源隔离与恢复通过；真实冻结批次全覆盖待证据。
- OPERATIONS：未推送、部署或启用 22:00 任务；未验收自然交易日。
- STRATEGY：未改策略阈值或候选预算，没有收益或信号改善声明。

全量证据：`artifacts/wp5-20261009/full-night-maintenance-tests/evidence.json`，SHA256 `ad640304744160e7f33a54b460bf64d064e81de471b1c367ebf927029ab4fa5a`。测试覆盖的是基于 `63748cc` 的本轮开发树，`release_qualified=false`，不能冒充最终干净 HEAD 的发布验收。`git diff --check` 退出 0；用户任务书及先前无关未跟踪文件保留。
