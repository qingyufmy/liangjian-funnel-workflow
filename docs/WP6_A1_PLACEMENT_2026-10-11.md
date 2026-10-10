# W8：A1执行位置与容量测量

实际测量于2026-10-10完成；本文件按任务书W8命名。结论：A1仍应在VM统一执行。现有全范围快照加载/研究包构建峰值落在1.5GiB以下，但未完成完整FULL研究，因此尚不能批准取消独占时段或据此断定不需扩内存。

## 真实测量

主机debian，生产HEAD adafe50；实际安装路径 `.venv/lib/python3.13/site-packages` 的 a1_packet/a1_contract SHA已记录。无活动研究时启动独立只读Python；不构造WorkflowApplication/RuntimeStore、不访问模型/网络、不封存代际、无生产写入。每2秒使用既有LinuxResourceSampler，并在阶段边界采样；getrusage记录本进程生命周期VmHWM。进程自身RLIMIT_AS=2.2GiB作为容量保护，未改系统/服务/cgroup配置。

输入为VM现存10-09快照：270,802,980 bytes，G0与manifest research_universe均4264；内部snapshot_hash匹配，文件SHA前后均 `b2651b26222254f3633b38234f314399bf3d1e7051d1ad8f1b17a4c477145fc6`。现存文件并非原任务书举例的92MB/910只旧本地切片。

| 项目 | 实测 |
|---|---:|
| 加载JSON→验证快照hash→构建packet→hash/diagnostics | 7.584秒 |
| getrusage峰值RSS | 1,333,028KiB（约1.271GiB） |
| packet字符 | 303,143 |
| 估计输入token | 83,156 |
| 主机MemTotal | 3,977,080KiB（约3.79GiB） |
| 主机MemAvailable，前/后 | 1,898,400 / 1,317,880KiB |
| 主机swap使用增量 | 512KiB |

主机swap增量不归因为该进程；过程中的VmSwap/PSI及原始采样在回执中。2秒采样不是精确连续峰值，getrusage是本进程生命周期峰值；两者不能替代并发自然任务证据。

## 验收与建议

CODE：既有packet/hash入口，未修改A1算法。REPLAY：4264只现存资料投影/封存hash验证，原件不变。OPERATIONS：独立VM只读dry-run完成，非自然A1 FULL、不含同步公告/PDF、模型审核和代际封存。STRATEGY：不适用。

当前可以支持“加载/packet阶段本身未越容量线”。不能把7.6秒宣称FULL只需7.6秒；失败prep的1.48GiB也不是FULL峰值。建议保持VM执行、现有时段与资源保护，下一次自然FULL采集全链峰值，再对决策17作最终容量建议。不更改A1执行位置/VM内存/调度。

命令：`D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -B artifacts/wp5-20261010/profile_a1_vm_dryrun.py`，本地/SSH均exit0，结果COMPLETED且input_unchanged=true。

回执 `artifacts/wp5-20261010/w8-vm-packet-dryrun-20261010.json`，SHA `e0eebde7ac8fc83a6a2b1c265ef1726140fdf1cebd6dcf16646934df72c91606`。脚本无远端临时文件；本地脚本与回执作为可复查证据保留。
