# W4 收盘研究资源观察接线

工作树：`D:/dev_A股/liangjian_wp5_hotfix_20261009`。起点 HEAD `fd0e872f1a018f570c3800654b2d318428ab9976`。本切片源和测试已冻结，未提交、未部署。

## 接线与不变边界

完整读取周末任务书及 canonical outbox0034/0035/0036，复核0027对 LinuxResourceSampler 的 ACCEPT。0035 的 A-LABEL prompt 条件由 root 独立处理，不属于本切片。

`WorkflowApplication.run_research` 仅增加保留公开签名的薄 decorator。只有 normal close（非 historical/comparison/auction）启动观察，包括走同一 close 入口的周末准备；morning 不启动。原 run_research 方法体、53个既有 WorkflowApplication 方法体均未改，A4 执行函数、策略、门槛、字段、发布与通知逻辑不变。接线不是 W1、W2 或 fsync writer。

新 `_CloseResourceObservation` 只调 ACCEPT `LinuxResourceSampler.sample()`；固定2s monotonic slot，无补造迟到tick。新增的是调用/停止调度边界，不复制 /proc 指标解析器。进程身份从实际 PID、`/proc/stat btime`、`/proc/<pid>/stat starttime`、`SC_CLK_TCK` 与既有 process_identity._host() 的 machine-id 哈希获得。缺失/错误不以当前 invocation 时间冒充进程出生。非 Linux 标 UNSUPPORTED，Windows 不是 Linux 证据。没有猜 cgroup binding，因此 cgroup 指标保留 UNSUPPORTED gap。

独立观测预算：最多3600s、1802个保留样本、累计 sample 读取耗时2s。慢读取不能硬中断，超限完成样本记预算gap；完成超过观测窗口或原 run 结束时刻的样本拒收。以上只限制资源观察，不改原研究deadline、parent预算或resource_guard阈值。最终停止等待最多50ms，daemon只持有身份、小型观察列表与路径，不持有/复制 snapshot/result 的大对象。

wrapper `try/finally` 覆盖原方法内资源门、快照/研究/发布/持久化异常；原异常原样传播。stop超时返回 observer_stopped=False，初始侧车继续 RUNNING；读取稍后返回时写 UNFINISHED，并以原证据合同 RUNNING/ended_at=null 构建，不宣称完成。SIGKILL/OOM使finally不能运行时只保留初始RUNNING（或侧车尚未创建），不伪造成功，不能保证最后峰值已持久化。

侧车：`workflow_output_dir/resource_observations/close-resource-<unique invocation>.json`，初始RUNNING、最终原子写同一独占文件；重复/并发 logical run 有不同 invocation，互不覆盖。采样/身份/写盘失败只固定诊断码，不能改变原研究资格、生产集合或通知。独立writer不阻塞研究；异步写尚未完成或失败时引用保持 ASYNCHRONOUS_UNCONFIRMED，不冒称持久化成功。

返回 dict 仅额外增加 `resource_sampling` 路径/身份/停止状态引用，原结果对象不原地修改。原正式 runs/<run_id>.json **完全不追加此字段**，不改发布payload。资源证据不进入 prepared.snapshot、Pipeline构造/运行参数、A2/A3 prompt；不持有模型原文。真实结束后以 canonical_research_run_id 旁路绑定，不改写观测样本的独立 run_id。失败前未取得正式run_id时null，不猜。

复用 RunResourceEvidenceBuilder：采样RSS最大值仅下界，VmHWM是进程生命周期峰值；host swap不可归因本run，VmSwap独立；缺支持不填0。实际读取结束时刻通常不等于run精确端点，故 START/END_SAMPLE_MISSING、cgroup缺项等仍 DATA_LIMITED、run_completed=False；不能把常规控制流SUCCEEDED标签当完整资源窗口或业务READY认证。

## 反例与实际回执

所有测试仅本地 tmp_path / FIXTURE proc，真实 OS Linux、VM、生产、网络、模型、通知均未执行。实际 run_research 集成用既有独立 orchestration fixture 和 FakePipeline；FakePipeline不是模型验收。

| 回执（artifacts/wp5-20261010） | exit | 结果 |
|---|---:|---|
| w4-resource-wiring-before.xml | 1 | 13 failed，新增接线未实现。 |
| w4-resource-wiring-v1.xml | 0 | 13 passed。 |
| w4-resource-wiring-budget-before.xml | 1 | 2 failed / 1 passed。真实预算窗口外样本未剔除1项；另1项是测试把序列化tuple与JSON list直接相等比较，已校正测试，不算业务缺陷。 |
| w4-resource-wiring-v2.xml | 1 | 1 failed / 120 passed：10ms fixture预算被本地初始写盘先耗尽，未开始预期慢读，是时序fixture问题；保留负回执，调整为200ms窗口+250ms慢读。 |
| w4-resource-wiring-v3.xml | 0 | 123 passed。 |
| w4-resource-wiring-final.xml | 0 | **125 passed，1.92s**：18个新反例+105个既有采样/证据合同+2个原资源门/发布顺序回归，不累计不同轮次。 |

实际最后命令：

```powershell
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -m pytest tests/test_w4_resource_sampling.py tests/test_wp6_resource_sampler.py tests/test_run_resource_evidence.py tests/test_workflow_orchestration_coverage.py::test_resource_gate_finishes_progress_and_does_not_start_pipeline tests/test_workflow_orchestration_coverage.py::test_primary_only_publishes_before_idempotent_comparison_enqueue --junitxml=artifacts/wp5-20261010/w4-resource-wiring-final.xml
```

测试覆盖：原参数/结果既有字段不变、原run文件不变、原快照对象保持且无资源字段、异常finally、资源门失败、identity/sample/write故障隔离、慢读超时、RUNNING→UNFINISHED、重入不覆盖、独立预算结束不改变READY、越预算晚到样本不计峰值、非目标入口不启动。

读取方法体证据：起点git blob与本轮checkout源解析，53个既有方法源码段（仅CRLF统一为LF）0变更。run_research源码段SHA256 `927f8651254075ca1467f3fa87d2d0c915e25bdfaeead2f8f6de831ca2de1274`；_a4_callback源码段SHA256 `7ebfc3018ea998460de86bf5f097977fa69d143ce5327add8bd418d6de00ce99`。第一次 PowerShell pipe 按Python默认编码解读失败（UnicodeEncodeError）；明确stdin UTF-8后exit0。不是源码损坏。

本轮测试时workflow实际文件SHA256 `ffad1e83c834e82917a47f940a67213e2845c9d1166318e25593dfe0fa6644ee`，新测试SHA256 `2df7c1570977a624227151b4b2871d875d0c1efd679ca477cb4d8bf46b98d44c`，最终XML SHA256 `807b7499b5c9f77cabc5b900db941cced3101b654ce99e9b998c879629354b3d`。后续 W2 可独立修改publisher，届时整文件SHA改变不意味着本轮wrapper被重测；以方法/新边界切片hash与各自回执分开绑定。

## 四层状态

CODE：观察接线已实现，未改A4/采样器/证据合同。REPLAY/TEST：本地125项fixture通过，无真实Linux峰值。OPERATIONS：未部署，周一VM自然运行峰值/副作用/持久化仍待证据；异步最后写盘可能在进程退出/被杀时丢失。STRATEGY：无策略、阈值、资格或容量判断改变；RSS下界不用于2.2GB容量结论。
