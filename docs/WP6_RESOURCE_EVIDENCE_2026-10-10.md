# WP6 纯资源观察证据合同

本切片实现纯 builder/validator，不是 OS 采集器、生产进度接线、A1 内存优化、整任务剖析或 WP6 验收。范围仅三个新增文件：`runtime/resource_evidence.py`、`tests/test_run_resource_evidence.py`、本文。基于 HEAD `3839a6be563873a8b7ce62e75c0a6af127a2aead` 的只读基线 `artifacts/wp5-20261010/WP6_RESOURCE_CALL_PATH_BASELINE.md`（SHA256 `d6c1cdc777d566ccfca51e5448cbe679ec00aaf4e961a5ada7f37768d9b6e48b`）。同仓其他人员的修改保持原样，未改 resource_guard/progress/workflow/UI/state，未提交。

## 结果与四层状态

| 层 | 本轮状态 | 证明 / 不证明 |
| --- | --- | --- |
| 源码合同 | IMPLEMENTED_LOCAL_CONTRACT | 正常 dataclass 构造；验证来源/单位/支持/identity/aware 窗口；JSON及哈希；无 OS/文件/网络/数据库/App 依赖。 |
| 测试 | PASSED_SLICE | 最终 76 新增 fixture 合同测试 + 3 既有 resource_guard 回归 = 79 passed，exit 0。不是全仓通过声明。 |
| 本地运行 / 数据 | FIXTURE_BOUND_ONLY | builder 样本来自 LOCAL_FIXTURE；三个既有 helper 测试中有一个读取本机 helper，仅非负可序列化 smoke。没有使用该 smoke 为新合同填指标或证明 Windows RSS。根代理的独立 92MB profile另列范围。 |
| 生产 / 自然任务 | UNWIRED_UNPROVEN | 未采集生产 OS / VM / DB，无模型、通知、生产接线；未证自然 A1 FULL / INCREMENTAL / close / auction-base资源窗口，未作扩容决策。 |

每份回执固定 `implementation_status=IMPLEMENTATION_PARTIAL`、`acquisition_authenticated=false`、`run_execution_authenticated=false`、`eligibility_released=false`、`pressure_is_capacity_decision=false`。`LOCAL_OBSERVATION_BOUND` 仅说明所提供的内容和窗口一致，不能认证采集来源、运行成功或容量。`run_completed` 只表示输入的 SUCCEEDED 声明 + 完整观察合同；不能取代工作流 run 的业务状态或作为未来 UI 成功标志。

## 公共 API 与正常构造

```python
MetricObservation(metric, value, unit, support, source)
ResourceObservation(run_id, invocation_id, pid, process_started_at,
                    host_id, observed_at, metrics, cgroup_id=None)
RunResourceWindow(run_id, invocation_id, pid, process_started_at,
                  host_id, started_at, observed_until, status,
                  ended_at=None, termination=None, cgroup_id=None)
RunResourceEvidenceBuilder().build(window, observations)
validate_run_resource_evidence(receipt, window, observations)
evidence_hash(strict_json_value)
```

dataclass `__post_init__` 验证并把 aware datetime 转 UTC；metrics 可由明确 list/tuple正常构造，立即封为 tuple。调用方对象不被改写，不绕构造器、不读环境、Settings/RuntimeStore/WorkflowApplication，不查 mutable cache。builder 接受 list/tuple，捕获观察序列，超过 100,000 项拒绝而非丢样本；此防御上限不是生产采样频率或业务预算。只有固定有限数字/枚举/受限 identity 与 ISO 时间输出，不支持任意日志/模型文本/错误消息。

必须给出显式 host observation epoch identity（例如 collector 已核实的 host+boot 绑定）、PID、实际 process_started_at 和 invocation_id；不能仅以 PID 推断进程身份。cgroup 指标 AVAILABLE 时须有显式 cgroup_id，并在窗口内保持一致。标识为声明，合同不认证 OS PID/starttime、boot ID或cgroup membership。真实采集器后续应在采集层核实这些身份，不从当前 API 推断为真实。

## 指标、来源与单位

| 指标 | 范围 | 允许真实源 / 原单位 |
| --- | --- | --- |
| RSS_CURRENT | process | LINUX_PROC_STATM（collector 先乘 page size转 bytes）、LINUX_PROC_STATUS（KiB）、WINDOWS_PSAPI（bytes） |
| PROCESS_RSS_LIFETIME_PEAK | process | GETRUSAGE_LINUX（KiB）、GETRUSAGE_DARWIN（bytes）、LINUX_PROC_STATUS（KiB）、WINDOWS_PSAPI（bytes） |
| SYSTEM_SWAP_USED | host | LINUX_PROC_MEMINFO（KiB） |
| PROCESS_VMSWAP | process | LINUX_PROC_STATUS（KiB）；不把 host swap写进此字段，不提供推断 Windows VmSwap |
| PAGECACHE_CACHED / DIRTY / WRITEBACK | host | LINUX_PROC_MEMINFO（KiB） |
| PSI_MEMORY_SOME_TOTAL_US / FULL_TOTAL_US | host | LINUX_PSI_MEMORY（us累计整数） |
| CGROUP_MEMORY_CURRENT | cgroup | LINUX_CGROUP_V2（bytes） |
| CGROUP_MEMORY_EVENTS_OOM / OOM_KILL | cgroup | LINUX_CGROUP_V2（count累计整数） |

上述每项允许显式 FIXTURE 来源；memory fixture 可标 bytes/KiB/MiB，仅测试换算，不伪装真实采集。真实 `/proc/status` 与 `/proc/meminfo` 原 KiB 不接受 MiB/bytes 重贴标签；已换算的真实采集数据不应伪称原文本读数，后续如需新归一化来源应另加明确适配器合同。未知指标/源、交叉错误源、MB/pages/percent等错单位拒绝。bytes 归一化中使用 1024，raw value/unit/source/support 与归一化值一同进入哈希；不靠数值大小猜 Linux/macOS单位。

支持状态为 AVAILABLE / UNSUPPORTED / UNAVAILABLE / ERROR。后三者 value必须null，unit可null或该指标合法单位，source可NONE或该指标明确源；不得以0冒充不支持。真实 AVAILABLE 零合法，不等于 unsupported null。数值必须有限非负、非 bool、非字符串，标准化不得超过 JSON精确整数上限 2^53-1；us/count必须整值。NaN/Infinity/负值/超大整数明确 ValueError，不在 JSON里 coercion成文本。**只有**派生的系统 swap end-minus-start允许负值。

pressure/cgroup/pagecache缺失不网络补齐、不生成未知指标或推断压力；保留每采样缺失/unsupported/error gap。当前全部已定义指标组成局部内容完整集，缺一项即 DATA_LIMITED；无cgroup/PSI支持的主机可留下正确 DATA_LIMITED声明，而不是要求伪造指标来READY。pagecache及PSI原始计数不生成“内存压力良好/坏”或容量结论。本切片未实现 fault、pgscan、pgsteal、swap in/out或PSI avg10等其他指标，不能将12字段称完整OS压力采集。

## 运行窗口与派生值

1. 所有样本须匹配 run/invocation/PID/process-start/host/cgroup身份，同一次窗口内严格时间递增，必须处于 start至实际 declared end（若有）或 observed_until区间；不自动排序、重命名、合并跨进程。重复时间点/越界/乱序拒绝。重入或PID复用需单独窗口/回执，不把多个 invocation峰值相加为run峰值。
2. `sampled_rss_peak_lower_bound_bytes=max(available RSS_CURRENT)`，仅采样下界，没有连续采样或真实run峰值认证。`process_lifetime_peak_reported_max_bytes` 单列生命周期高水位；不减start peak，不合并跨PID，不当任务精确峰值。高水位不得小于同点RSS，也不能在同身份源下降；同指标AVAILABLE来源变化拒绝。累计PSI/OOM counter下降同样拒绝，真实reset须新epoch，不能隐藏reset。
3. host swap只取时间**恰好等于** started_at / ended_at 的支持样本，计算 end-start。缺start或end不拿中间点顶替、不从生命周期peak推差；`attributable_to_this_run=false`。全局差值可能被其他进程影响，也可能因释放为负。VmSwap有自己的process start/end/sampled max，与system swap不混用。
4. RUNNING不得有结束声明，保last_observed_at和已有样本，无结束读数/增量，RUN_NOT_FINISHED + END_SAMPLE_MISSING；FAILED必须有明确end和EXIT_FAILURE/EXCEPTION；TERMINATED明确SIGKILL/SIGTERM/UNKNOWN_TERMINATION，可缺end，固定 observation_status=INTERRUPTED，绝不run_completed。即使TERMINATED有外部观察end也不能作SUCCEEDED。
5. SUCCEEDED需要正常声明end且无termination，但声明不能造采样；缺end/start/任一指标时 observation_status=INCOMPLETE_WINDOW、window_coverage_complete=false、run_completed=false，原declared_status保留。只有全部支持且起止匹配才LOCAL_OBSERVATION_BOUND，仍是内容绑定非执行认证。
6. builder不会采样、监控、等待、读取当前时间或SIG handler，也不会替进程终止补end样本。SIGKILL无法由Python finally留证；父采集器后续需记录真实中断/最后样本，不能按预设时刻编值。

## 严格 JSON 与 validator

输出仅普通 dict/list/string/int/finite float/null/bool。标准 canonical JSON为 ensure_ascii=False、sort_keys=True、separators(',',':')、allow_nan=False；SHA256计算无evidence_hash的body。当前小资源回执使用完整json.dumps，不宣称流式处理或大样本内存已剖析；真实采集频率/保留策略/预算另行决定，不改生产时限。

validator必须收到同次原window与observations，重建期望body，对比自hash和完整内容hash；重签改过的receipt不能匹配原输入。正确 DATA_LIMITED receipt可以valid=true（验证声明真实反映缺口），并不意味着窗口完整/资格/成功。未知对象、非JSON值或原输入不匹配返回valid=false，不吞掉或输出任意异常原文。哈希证明内容一致，不证明fixture来源是真实内存或原参数来自真实调用点。

## 测试命令与真实负回执

所有新增样本为明确本地fixture，无模型。原负回执保留不覆盖：

| XML路径（artifacts/wp5-20261010） | 真实结果 | 修复 |
| --- | --- | --- |
| resource-evidence-before.xml | pytest收集缺模块1 error，exit1 | 反例先存在；不是76逐项失败 |
| resource-evidence-source-unit-before.xml | 4 failed / 71 deselected，exit1 | 超大int转换OverflowError；proc原KiB源曾接受错单位 |
| resource-evidence-incomplete-status-before.xml | 1 failed / 75 deselected，exit1 | 缺end样本的声明SUCCEEDED曾保observed SUCCEEDED；改INCOMPLETE_WINDOW |
| resource-evidence-final-v2.xml | 79 passed / 0 failed / 0 skipped，exit0 | 76新增 + 3既有helper；最终受测owned源码 |

```powershell
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -m pytest tests/test_run_resource_evidence.py tests/test_resource_guard.py --junitxml=artifacts/wp5-20261010/resource-evidence-final-v2.xml
```

覆盖生命周期peak大于本次RSS、负系统swap差、独立VmSwap、跨run/PID/复用/重入/host/cgroup、时序/窗口、naive时间、异常数字和单位、unsupported缺测、pressure缺口、累计回退、来源变化、无start/end、失败/终止RUNNING、输入不变、规范哈希和receipt重签伪改、AST无App/I/O依赖。旧 `resource-evidence-final.xml` 为上一源版本78pass，不作最终回执。未扩跑全仓；当前他人a1_packet/a1_contract改动不属本切片，也未还原。

## 根代理独立92MB核查的类型纠正（引用，不重跑）

本轮读取根代理最终 `artifacts/wp5-20261010/snapshot-0928-research-schema-profile-v2.json`，实际文件SHA256 `9a2e083a276d31c07451ceda384a8d828eb8108e07146dd6b7ca85dd6d41c6c8`。它不是本builder输出，也未将profile点值拼成合格窗口：该报告无本合同完整起止/压力/采集身份项，不能回填缺测证据。

该原文件是 ResearchSnapshot `{snapshot_id,snapshot_hash,as_of,data}`，对应 research正常FrozenInputSnapshot构造，不是 `pipeline.snapshot` Universe/FrozenInputSnapshot schema。先前wrong-type负报告属于核查器选错类型，不说明原文件坏；第二份把MappingProxy传给workflow._hash_json触发default=str，也属于profile脚本错误；前两份原证据留存但不作正确内存对比。最终按实际loader的native dict进行hash，内部hash匹配true，原字节SHA前后为`29b68cc978f793c6ddc43bd62e9e24ca2a88e7f129b637b517a77673fe82f389`不变。

最终报告声明910 G0、5578 full、3929 research、3860 trade、903 selected trade；selected910不等于research3929，full_a1_domain_verified=false，尚未active代际/正式scope对账，不能授予A1 FULL资格。该Windows报告仅load+normal constructor+hash，非完整A1：read/load2.244s；阶段tracemalloc分配peak443108811 bytes，OS**进程生命周期**peak683130880 bytes、read后点RSS489472000 bytes；hash验证约9.274s及独立hash9.306s，阶段新增分配peak246676/143284 bytes。两点RSS不是连续任务峰值、tracemalloc不是总live图或OS RSS；报告中的当前机器观察不用于描述VM现状或扩容。

## 未完成项与交回

未实现OS/proc/PSAPI采集适配器、采样源/身份认证、周期与whole-run窗口、终止父观察器、逐run资源存档、progress/DTO/UI、任何生产接线或门控调整；未完成A1行业节点spool/流式merge等价，未取得自然FULL资源峰值或11-02扩容判据证据。接下来的scope与ownership由根代理决定，不隐式开始source接线。源码/test/doc及XML字节hash在最终artifact清单记录；交回后三文件冻结，无commit/push。
