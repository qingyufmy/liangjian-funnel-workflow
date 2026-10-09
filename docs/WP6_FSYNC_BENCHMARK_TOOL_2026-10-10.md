# WP6 参数化 fsync 工具：本地验证与待授权边界

日期：2026-10-10。源码基线 HEAD：`cfed3a536e65ffd308633ca8ef4de3c2d9b224b3`。本切片只新增脚本、测试与本文，不接 A4、生产调度、状态或配置；没有 SSH、VM、网络或生产 DB 操作。工具可运行不等于 Tony 已批准测量，更不等于完整 WP6 或架构选择已验收。

## 授权依据

完整读取 canonical `.claude_bridge/outbox/0016-reply.md`（位于 `D:/dev_A股/liangjian_a4_20260923`），SHA256 为 `4c0bad363c49fed7aab84f1860ec155557f79cb8c8f519f3da0f3b92cc7c4ef2`，以及其引用的 `0014-reply.md`，SHA256 为 `7e22fbcd973f4e7292fc29712504b253776f4ceb1cfd8633e2ef56951d493e21`。0016 修正了 0014 的权限解释：mkdir/write/fsync/delete 都是写操作，Claude 技术审查不是 Tony 授权。当前 `NEED_TONY`，没有实施 VM idle 或 busy 组。

0016 待批方案为独立唯一目录、与数据目录同挂载、至少 300MB 空间、串行 150KB × 1000 次，收集 file/dir fsync 尾延迟与系统信息；不得改 ACL、owner、服务、环境、挂载或制造负载。10-12 自然 close 的 busy 证据另有确认边界。本工具不默认任何 `/www` 路径，不验证 Tony 身份，调用者的 `allowed_parent` 只表示本次路径范围。输出始终保留 `authorization_authenticated=false`、`architecture_status=WAITING_BUSY_EVIDENCE`、`measurement_context=CALLER_SCOPED_NOT_PRODUCTION_CERTIFIED`。

## 公共接口与路径边界

`BenchmarkConfig(parent, allowed_parent, mount_reference, iterations, file_size_bytes, minimum_free_bytes=300_000_000)` 是正常冻结 dataclass。`run_benchmark(config)` 返回 JSON 可序列化字典；`percentiles_ms(durations_ns)` 使用 nearest-rank 分位数；`main(argv)` 提供 CLI。

全部路径必须显式绝对路径、不得含 `..`，既有父目录与 allowlist 目录必须存在，父目录必须位于 allowlist 本身或其子目录；拒绝路径或祖先 symlink。父目录可写且可遍历，空间至少 `max(minimum_free_bytes, 2*iterations*file_size_bytes)`，最低阈值不能小于 300,000,000 bytes。范围为 1–1000 迭代、每文件 1–153600 bytes，布尔值和不合法整数拒绝。参考路径只读取文件系统元数据，不读其内容。stat device 与实际 mount ID 都必须匹配。

工具以 UUID 新建 `liangjian-fsync-benchmark-<32hex>` 子目录，`exist_ok=False`，不复用同名目录、不创建缺失父目录；新目录再核对 device/mount。身份无法证明时留下目录并报告，不把不明路径当成可清理对象。没有递归删除、目录扫描删除、覆盖文件或修改权限。

Linux 使用 `/proc/self/mountinfo` 的最长适用挂载，保留 mount ID、FS、设备 major/minor、source；可读时补 sysfs 型号或 virtio 线索。Windows 使用只读卷 API 记录挂载根、FS 与卷序号，物理磁盘型号为 `UNKNOWN`，不由卷序号猜物理盘。其他平台或未能证明挂载身份时阻断。系统版本、内核、Python 与脚本执行前后 SHA256 均归档；型号等缺项为 null/UNKNOWN，不编造。

## 迭代与清理语义

每轮独立 `xb` 文件执行 open、write、flush、fsync(file)、close、fsync(dir)。报告每项 monotonic 纳秒以及 p50/p95/p99/max 毫秒，空样本为 null，不填 0。

按 root 的审查建议，本工具采用 `PER_ITERATION_EXACT_UNLINK_AND_DIR_FSYNC`：每轮结束即校验原 FD 捕获的 device/inode/size/mtime/ctime 和目录、父目录、挂载身份，unlink 自己当前文件，然后 fsync scratch 目录，再开始下一轮。不会先累积 1000 文件。0016“逐文件删除后 rmdir”没有明确累积或每轮删除；这里是本工具提出的具体行为，不冒称 Claude/Tony 已批准这一负载细节。额外 cleanup fsync 会影响下一轮缓存与负载，VM 运行前应以本脚本版本重新确认。

`total_ns` 是原 write/file+dir fsync 流程，不含 unlink 与 cleanup dir fsync；`unlink_ns`、`cleanup_directory_fsync_ns`、`iteration_total_ns` 单独报告。不得把含清理的总迭代 p99 当成 A4 写入热路径 p99。父目录创建/删除屏障在迭代之外记录。unsupported dir fsync 保留尝试耗时，但实际 `directory_fsync_ns` 为 null；包含尝试开销的 total 不是完整 file+dir fsync 证据。

清理只操作 FD 已证明拥有的路径，不通过失败后 `path.lstat()` 接管陌生替换文件。unlink 成功立即移出所有权列表，不在 finally 重删后来替换的路径。最终只逐项清理剩余已知文件并 rmdir 当前精确目录；陌生目录项保留，rmdir 失败留明确证据。局部失败、空间不足、改挂载、hash 读取失败、脚本中途改变及 KeyboardInterrupt 均保留状态/原因码和可获得的部分迭代数据。

检查与 unlink 不是抵御恶意同用户并发替换的原子条件删除；唯一私有目录及无并发写者是使用边界。SIGKILL、主机崩溃、默认 SIGTERM 或 finally 中再次中断可能产生 NO_CAPTURE 与遗留目录，不宣称强杀恢复或掉电证明。

## 退出码和 durability

| 情况 | 退出码 | JSON / durability |
| --- | --- | --- |
| Linux 所有 file、dir 及清理屏障实际完成 | 0 | COMPLETED / FSYNC_FILE_AND_DIR |
| 工作负载完成但目录屏障不支持，如本地 Windows | 2 | UNSUPPORTED / PROCESS_CRASH_ONLY（Windows） |
| 预检查、执行、清理、hash 失败或中断 | 3 | BLOCKED/FAILED/INTERRUPTED / UNSUPPORTED_DURABILITY |
| argparse 缺必填参数、未知 flag 或整数语法错 | 2 | usage 写 stderr，没有 JSON，也没有测量 |

参数完整且通过 argparse 后，配置范围无效返回 exit 3 的 JSON，记录明确原因及可读取脚本 SHA。不能将 argparse 无 JSON 的命令记为一次测量或部分测量。`completed_iterations` 包括工作负载完成但目录屏障 unsupported 的轮次；实际完整屏障数量另有 `file_and_dir_fsync_completed_count`。失败轮原数据保留但不进入成功轮分位数。

`FSYNC_FILE_AND_DIR` 只证明系统调用正常返回，`power_loss_proven=false`；Windows 的 `PROCESS_CRASH_ONLY` 分类也不是故障注入实测。不得由本工具推出 ACK 跨平台持久性、突然掉电安全、同步批处理可发布或 NodeIPC 必须选择。

## 本地验证与真实性

最终执行（PowerShell，仓库根）：

```powershell
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -m pytest tests/test_fsync_benchmark.py --junitxml=artifacts/wp5-20261010/fsync-tool-final.xml
```

真实 exit 0，37 passed in 0.98s。测试主要为 tmp_path 内 128 bytes × 3；CLI 完整参数 subprocess 为 64 bytes × 2。Windows file fsync、元数据与 CLI 路径实际执行；dir fsync unsupported 是预期 exit 2，不改成成功。Linux 支持分支使用明确 FIXTURE 的 mount/dir fsync 注入，不是 Linux/VM 实测。没有执行 1000 × 150KB、idle/busy 时段、生产目录或架构尾延迟测量。

反例覆盖：错范围/单位及容量、mount/reference 不匹配、目录重用、symlink、原文件被替换、未知目录项、局部 write/flush 失败、目录身份未证、hash 变化/失败、中断 JSON、每轮清理先于下一轮写入、清理屏障失败、不把 unsupported 填 0，以及无参数 argparse 边界。

保留原退出证据：

- `fsync-tool-before.xml`：exit 1，缺模块导致 collection error；不是 37 条测试逐项失败。
- `fsync-tool-cleanup-before.xml`：exit 1，3 failed / 26 deselected，暴露部分 flush 失败时误接管替换路径、after-hash 异常丢回执、新目录 mount ID 缺核对。
- `fsync-tool-interrupted-before.xml`：exit 1，2 failed / 32 deselected，暴露中断无 JSON 与无效配置原因/SHA 不完整。
- `fsync-tool-v3.xml` 的 Linux mountinfo fixture 曾因 Windows path 分隔符比较错误失败，已改为 `as_posix()`；不将此测试夹具错误归因真实 Linux 文件系统。
- `fsync-tool-final.xml`：最终 37 passed / 0 failed / 0 errors。最终代码、文档、XML SHA 见 `artifacts/wp5-20261010/fsync-tool-manifest.json`。

四层状态：SOURCE_IMPLEMENTED；LOCAL_TEST_VERIFIED；VM_MEASUREMENT_NOT_RUN_NEED_TONY；ARCHITECTURE_WAITING_BUSY_EVIDENCE。本切片完成工具及本地验证，不替整体 WP6 或生产行为验收背书。
