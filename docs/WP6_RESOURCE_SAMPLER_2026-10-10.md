# WP6 显式只读资源采样器

日期：2026-10-10。工作树：`D:/dev_A股/liangjian_wp5_hotfix_20261009`。读取/冻结 HEAD：`ae2d098b5eeb3a369e23d080fd006b099d7962fc`。本切片未提交，未接入生产。

## 1. 真实原因

canonical bridge outbox0019接受 `resource_evidence.py` 的纯合同，但指出真实Debian进程RSS/swap/pressure待进程内固定间隔采样，Windows观测不能替代Linux证据。本切片只提供可显式调用的Linux只读采样器/纯解析，不包含下一次发布、VM采样或自然15:10接线。任务书WP6的11-02 FULL和2.2GB容量决策仍待真实证据。

完整读取0019、现有 `runtime/resource_evidence.py` 和canonical AGENTS；既有合同六字段身份、计数器回退检测、采样峰值下界与进程生命周期峰值分离、host swap不可归因继续原样复用。合同源SHA256 `656b4c912924dceee8d8b3e3cbfbfd666b682a92bce7d4ffeb3c35be681cdb63`，未修改。

## 2. 代码修改

只新增三文件：

| 文件 | 内容/反例 |
|---|---|
| `src/liangjian_funnel/runtime/resource_sampler.py` | pure parse_status/parse_meminfo/parse_pressure，SamplerIdentity、显式CgroupV2Binding、同步LinuxResourceSampler.sample/collect、SamplingBatch。无服务、线程、自动启动、env/flag/调度/写入。 |
| `tests/test_wp6_resource_sampler.py` | 临时目录伪proc、注入UTC/monotonic/wait，不真睡；PID错配、缺项/坏单位/负数/重复字段/超范围、cgroup成员/挂载/控制器/路径身份错误、固定间隔/容量上限/慢读missed/窗口后不补、非Linux禁止OS读。 |
| 本文档 | 读取合同、退出码、使用边界与未验证项。 |

反例先行：模块不存在时21 failed，exit1，`artifacts/wp5-20261010/resource-sampler-before.xml`。首轮16 failed/5 passed、exit1，status解析调用的source/support参数位置错误；修后相关100 passed、exit0。追加错PID不能认证其cgroup反例，25 passed/1 failed、exit1（原cgroup仍有值）；修为process/cgroup均NULL，host指标独立保留。

入口身份必须显式提供 run_id/invocation_id/pid/process_started_at/host_id，可选cgroup_id。不自动取PID或用当前时间冒充process_started_at；ResourceObservation复用现有类型校验。Linux `/proc/self/status` 的Pid须与声明一致，错配或无身份字段时process指标ERROR/NULL，cgroup也不可用。process_started_at是调用方声明，采样器不证明其真实性；所有authentication权限仍由现有合同保持False。

指标合同：

| 来源 | 指标 | 单位/作用域 |
|---|---|---|
| `/proc/self/status` | VmRSS、VmHWM、VmSwap | 输入kB按Linux KiB解析，交既有contract归一bytes；process。VmHWM为进程生命周期高水位，不是本run精确峰值。 |
| `/proc/meminfo` | SwapTotal−SwapFree、Cached、Dirty、Writeback | KiB；host。host swap增减不归因本run，Cached不冒充全部缓存/单进程缓存。 |
| `/proc/pressure/memory` | some/full的total | us；host累计计数，不用avg10等做新门槛。 |
| 显式验证cgroup v2的memory.current/memory.events | memory.current、oom/oom_kill | bytes/count；cgroup，不冒充单进程数值。 |

纯解析源标签描述字段来源，不认证采集；伪proc显式标FIXTURE。缺字段UNSUPPORTED/NULL、文件不存在UNAVAILABLE/NULL、权限/解析/范围错误ERROR/NULL，不填0；只有实际读到的合法0才为AVAILABLE。每次读取最多262144字符，超过上限ERROR，不无界读文件。既有builder仍检测计数器倒退、RSS超过VmHWM、窗口身份和起止覆盖。

cgroup不自动发现或未经验证拼路径：调用方显式提供root、directory、canonical绝对POSIX relative_path和cgroup_id；检查身份相等、路径规范、无`..`、resolved路径不变、directory位于root且与相对路径一致，再读 `/proc/self/cgroup` 和 `/proc/self/mountinfo` 证明同一成员路径与cgroup2 root挂载，验证memory控制器，拒绝指标文件符号链接后才读取。无绑定/证明失败返回NULL。子树挂载root不是`/`的namespace布局本版不推测转换，保持不可用。该验证是局部只读一致性，不构成OS/宿主认证，也不是恶意并发改挂载/路径的安全隔离。

固定间隔合同：collect显式接收duration_seconds、interval_seconds和max_samples（1–10000）；起点+整数slot×interval固定deadline，慢读错过slot只增加missed_interval_count，不补造中间观测；超过duration不再新采样，容量限制的是attempts（含晚到样本），耗尽truncated=True。`sample()` 的observed_at取读取完成UTC时刻，read_spans记录开始/结束，不声称三个文件或proc/cgroup读取原子。collect读取后monotonic已超过end的样本放入late_sample_count/late_sample_issues/late_read_spans，**不进observations或peak**；所有monotonic读取由同一helper检查相邻不回退，不只与起点比较。UTC在同次读取中回退也拒绝。

SamplingBatch记录实际观测、每次issues、非AVAILABLE指标计数、间隔/时长/容量、attempts/late、sampled_peak_is_exact=False。unsupported_counts只合计保留观测里的非AVAILABLE状态，原观测仍保留具体ERROR/UNAVAILABLE/UNSUPPORTED，不能解释成全部OS不支持，也不拿晚到丢弃样本统计填0。非Linux且非显式fixture立即返回unsupported指标，不访问真实/proc，不为无支持平台真睡一个完整窗口。

## 3. 测试与回放

实际Python：`D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe`，PYTHONPATH=`D:/dev_A股/liangjian_wp5_hotfix_20261009/src`，工作目录为本worktree。

| 实际执行 | 结果/回执 |
|---|---|
| `python -m pytest tests/test_wp6_resource_sampler.py -o addopts='' -q --junitxml=artifacts/wp5-20261010/resource-sampler-before.xml`（新模块前） | 21 failed，exit1。 |
| 同新测试，`resource-sampler-first.xml` | 5 passed/16 failed，exit1，参数顺序实现错误原样保留。 |
| 新测试+既有resource_evidence/resource_guard，`resource-sampler-related-v1.xml` | 100 passed，exit0。 |
| 新测试新增PID/cgroup反例，`resource-sampler-hardening-before.xml` | 25 passed/1 failed，exit1，错误PID仍关联cgroup。 |
| 相关组，`resource-sampler-related-final.xml` | 105 passed，exit0（26新+79既有）。 |
| root追加读取完成时间/晚到剔除/相邻时钟回退反例，`resource-sampler-lifetime-before.xml` | 26 passed/3 failed，exit1，原读取前timestamp、late仍入observations、slot内回退未拒绝。 |
| 最终：`python -m pytest tests/test_wp6_resource_sampler.py tests/test_run_resource_evidence.py tests/test_resource_guard.py -o addopts='' -q --junitxml=artifacts/wp5-20261010/resource-sampler-lifetime-final.xml` | **108 passed，exit0（29新+79既有，同一命令，不将各轮相加）**。 |
| `git diff --check` | exit0，他人既有脚本LF/CRLF提示保留。 |

真实OS Linux读取/VM/生产自然日均未执行；本地fixture与windowsunsupported反例不冒充Debian峰值证据。fixture和时钟不使用网络、模型、进程控制或DB，不改resource_guard阈值。

显式调用示意（不是接线/已执行命令；process_started_at须由调用方提供真实身份）：

```python
sampler = LinuxResourceSampler(SamplerIdentity(
    run_id=run_id, invocation_id=invocation_id, pid=pid,
    process_started_at=process_started_at, host_id=host_id), cgroup=verified_explicit_binding)
batch = sampler.collect(duration_seconds=duration, interval_seconds=interval, max_samples=capacity)
```

batch.observations是原有ResourceObservation；调用方以后可显式交RunResourceEvidenceBuilder，但必须有真实RunResourceWindow声明及起止样本。采样器不自行声称成功/结束、不写receipt、不自动发布，固定tick未命中结束时刻仍END_SAMPLE_MISSING。

## 4. 四层验收

- CODE：相关108通过，exit0；全仓全量待root联合新HEAD执行。
- REPLAY：临时伪proc/确定性时钟覆盖；不是历史业务/真实Linux回放。
- OPERATIONS：未接线、未发布、未读VM；10-12自然收盘/11-02 FULL待证据。
- STRATEGY：不适用；容量决定、策略阈值、权限与生产行为均未改。

## 5. 未完成边界

这是同步显式采样器，持续调用会占用调用线程；未实现daemon、后台线程寿命、退出flush或hot-path非等待写入，也未接workflow/scheduler。将来进程内接线应独立审查采样生命周期、可靠终止/起止覆盖与实际文件读取耗时，不在本切片扩大成调度系统。

固定采样最大RSS仅下界、VmHWM仅process lifetime high-water，不是run精确峰值；host/cgroup范围数据不可自动归因process/run。采样间发生的短峰值可能漏掉；missed/truncated/NULL不能填0或当完整窗口。源读取时间、实际权限、真实cgroupnamespace/容器布局、采样开销尚无VM证明。

canonical outbox0019另一条C/Python hash编码黄金及真实VM wall time不由本采样器完成；其它agent的workflow/capital/hot/hash文件及共享state未改。全量与生产接线/发布均由root另行整合。

## 6. 下一步

冻结三文件与独立hash回执交root联合full；本切片不自行commit、发布或启动真实资源采集。
