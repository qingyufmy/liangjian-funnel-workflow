# WP6：9 月 28 日研究快照本地核验与加载剖析

本轮仅验证本机既存不可变文件、正常研究快照构造和与正式 loader 相同的字节/日期/哈希条件，没有构造 `WorkflowApplication`、`RuntimeStore` 或 `Settings`，没有网络、模型、生产读写、信号补发或发布。运行平台是 Windows，不把本机采样代替 Debian 自然 FULL 的资源验收。

## 结论与输入范围

实际文件 `D:/dev_A股/liangjian_a4_20260923/artifacts/readonly-snapshot-20260928.json` 为 92,154,548 字节，外部 SHA256 `29b68cc978f793c6ddc43bd62e9e24ca2a88e7f129b637b517a77673fe82f389`，执行前后完全相同。

这是 `{snapshot_id, snapshot_hash, as_of, data}` 的 **production research snapshot**，不是 `pipeline.snapshot.FrozenInputSnapshot` 的证券全集封存模型。正常 `pipeline.research.FrozenInputSnapshot` 构造、`workflow._hash_json` 对原生 dict 的两次核验均匹配内部哈希 `e8bbe3734e35100c67fdf92f6af78dc0600aba7e58033b1c5c2f42c79acde75a`。

`snapshot_id=snapshot-20260928T160928+0800-e8bbe3734e35`，as-of 为北京时间 `2026-09-28T16:09:28+08:00`。G0 910 只，无重复；全集 5,578、声明研究域 3,929、声明可交易域 3,860、选定交易候选 903。`selected_count=910` 不等于 `research_universe_count=3929`。这份快照不是可直接用于完整 A1 FULL 的范围证据，活动 A1 代际与完整 scope 未对账；不擅自补成全域、不用该文件证明 FULL 模型完成。

## 真正执行与结果

工作目录 `D:/dev_A股/liangjian_wp5_hotfix_20261009`，原 HEAD `3839a6be563873a8b7ce62e75c0a6af127a2aead`。只读 loader/hash 源文件 `workflow.py` 前后 SHA `967e071ba7c4bf92f776813bfab2de032e4ef38b3ab1ac2bc09bec0ed4e9a086` 不变。结果文件为独立 artifact，不修改输入或原回执。

```powershell
$env:PYTHONPATH='src'
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe artifacts/wp5-20261010/inspect_september_snapshot.py --format research --input D:/dev_A股/liangjian_a4_20260923/artifacts/readonly-snapshot-20260928.json --expected-sha 29b68cc978f793c6ddc43bd62e9e24ca2a88e7f129b637b517a77673fe82f389 --output artifacts/wp5-20261010/snapshot-0928-research-schema-profile-v2.json
```

退出码 0；输出状态 `LOCAL_FROZEN_SCHEMA_AND_HASH_VERIFIED`，独立 hash 匹配 true。结果 SHA256 `9a2e083a276d31c07451ceda384a8d828eb8108e07146dd6b7ca85dd6d41c6c8`。

| 阶段 | 带 tracemalloc 耗时 | 本阶段新增 Python 分配峰值 | 说明 |
| --- | ---: | ---: | --- |
| read UTF-8 + JSON load | 2.244s | 443,108,811 bytes | 包含完整文本与 JSON 树加载，不能只测对象已加载之后 |
| loader 检查及正常 research wrapper 构造 | 9.274s | 246,676 bytes | 流式内部哈希验证；已有输入树不计入新增分配 |
| 对真实 dict 的第二次内部哈希检查 | 9.306s | 143,284 bytes | 不修改数据，不把 MappingProxy 的 repr 当 JSON |

load 后 Windows `GetProcessMemoryInfo.WorkingSetSize` 为 489,472,000 bytes（约 466.8 MiB）。该独立进程的 OS 生命周期 working-set 高水位 683,130,880 bytes（约 651.5 MiB），包括导入和加载；它不是完整 A1 run 峰值。两点 RSS 不代表连续采样峰值，tracemalloc 增量分配不等于已存活输入树的总占用。没有观测 Debian swap、PSI、cgroup 或自然任务资源。

## 两份早期核查纠正，不删除负证据

1. `snapshot-0928-schema-profile.json`（SHA `faf487893ed7e4f1edb4e68cd589c116d35080cba27db5010fc52ed3f7fa4328`）选错证券全集封存模型，报六字段 missing。它证明脚本当时选错类型，不证明原研究快照损坏。
2. `snapshot-0928-research-schema-profile.json`（SHA `9c826ec72806398d00338bb55ef5557f0cffc5fe5eaed0378408c7f799b17b27`）正常 loader 检查已经匹配，但第二次独立检查错误地将 MappingProxy 传给 `default=str` 的 `_hash_json`，哈希了 repr。因此其 `internal_hash_matches=false` 和该步骤约 405MB 分配不作为业务缺陷或优化前基线。报告状态只说明前一检查，不能覆盖独立核验失败。
3. 最终 v2 明确浅转真实 dict，与正式 loader 的原生 dict 哈希口径一致；独立检查失败时输出降为 DATA_LIMITED。前两份保持原字节；以最终 v2 作为正确本地输入核验，而不是静默改旧报告。

## 同一真实快照的纯 packet 组件前后对照

正常既有 `build_a1_research_packet(envelope)` 在本机纯计算 monthly context 和 packet，没有网络/模型/应用构造。完整输入仍是相同 92MB 文件，内部哈希核验匹配、G0 910 条执行前后不变；helper 源、packet 源和 monthly_strategy 源前后 SHA 一致。对照 oracle 保存原 `json.dumps`/hash/diagnostics 精确公式，没有缩小包或删除记录。

命令：`python artifacts/wp5-20261010/profile_existing_a1_packet.py`，同解释器/PYTHONPATH/cwd，exit 0。`artifacts/wp5-20261010/a1-packet-0928-real-component-profile.json` SHA `046406de2f62db3216e8f090ed0095ccff81198c3702bde0d46dd269d6732b93`，状态 LOCAL_COMPONENT_EQUIVALENT。

实际包 288,229 canonical 字符，81,090 estimated tokens，原预算 100,000，within_budget=true；402 行业、20 canonical 月决策、148 source_index 条目。原 coverage 状态 **INCOMPLETE**：分母 7,280、packet_ready 7,222（差 58）；没有用预算通过掩盖字段缺口，更不证明 A1 选股或审核通过。这个 source_index 只是已有投影内容，不新增来源授权。

| 组件 | 旧新增分配峰值 bytes | 新新增分配峰值 bytes | 旧带采样耗时 | 新带采样耗时 |
| --- | ---: | ---: | ---: | ---: |
| canonical hash | 2,028,174 | 8,496 | 15.739ms | 41.258ms |
| packet diagnostics | 2,021,010 | 55,216 | 62.161ms | 106.217ms |

全 hash 与全 diagnostics dict 均相等，原 packet 不变。测量窗口不含输入/packet 构造，也不含整个 A1；仅 tracemalloc新增 Python 分配，不是 RSS 或 Linux swap。两条组件本次均更慢，iterencode有真实 CPU 成本，不能把内存下降称总耗时改善。测试时有其他离线任务，单次耗时不是生产 SLA。原 snapshot、全部事实和原 A1 缺口未改；行业节点 spool/完整合并及真实 FULL 仍未做。

## 四层验收与下一步

- CODE：已有研究 wrapper / loader hash 的只读路径核查；不是本次新增生产资源接线的测试。
- REPLAY：这份快照加载、hash验证及纯packet组件旧新对照完成；完整 A1、行业节点 spool、模型/审核、合并分区的前后对比未执行，不将组件结果推广成完整 FULL。
- OPERATIONS：未发布、未启动自然维护；11 月 2 日 FULL 资源回执仍待真实运行，不做扩容决定。
- STRATEGY：不适用；策略资格、阈值、候选数量、源授权未修改。
