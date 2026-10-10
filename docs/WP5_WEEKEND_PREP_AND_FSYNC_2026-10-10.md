# WP5：周末研究与空闲 fsync 实测

## 1. 真实原因与授权

10 月 12 日计划在研究启动前为 0。决策 9 和 fsync 父目录修订此前尚未批准；Tony 当前线程回复“批准”，只承接这两项。生产已发布 `adafe5054b9ff283ea03925e2b0c6baf2146dcfe`，不追加发布 A-LABEL。

证据目录：`artifacts/wp5-20261010/authorized-prep-fsync-20261010T0743/`；授权原文范围见 `AUTHORIZATION.md`。不使用 `--as-of 2026-10-09T15:10` 伪装周六为周五。正式 `run-next-session-prep` 使用真实周六采集时间、10-09 已闭合市场日期与 10-12 目标交易日。

## 2. 本轮修改与执行

未改生产源码、策略阈值、供应商限流或 `.env`。仅新增本地操作和只读验收脚本，更新状态文档。临时 systemd 单元以 `www` 运行既有正式 CLI，`LIANGJIAN_ROOT` 仅指定本次进程的既有项目根目录。

空闲基准先完成，之后才启动研究，避免研究 I/O 污染 idle 样本。VM 身份 `aurum-vm=192.168.1.254/debian`，项目根 `/www/wwwroot/Agu/liangjian-funnel-workflow`。基准父目录 `/www/wwwroot/Agu`，新建精确 UUID 子目录，不递归删除父目录或项目文件。

## 3. 实测、命令与退出码

- `python .../run_idle_benchmark.py`：首次本地脚本嵌入 NUL，退出 1、尚未 SSH；修复本地字节替换后实际基准只执行一次，SSH / 原基准 / 本地退出均 0。
- 原基准：`benchmark_fsync_latency.py --parent /www/wwwroot/Agu --allowed-parent /www/wwwroot/Agu --mount-reference /www/wwwroot/Agu/liangjian-funnel-workflow/state --iterations 1000 --file-size-bytes 150000 --minimum-free-bytes 300000000`。
- 1000 次，累计 150,000,000 字节；`total_ms` 热路径 p50 0.440947ms、p95 0.948251ms、p99 1.380766ms、max 2.183266ms。临时子目录已清理，父目录前后清单相同，HEAD 不变。
- 基准回执 `fsync-idle-authorized.json` SHA256 `0aa90edf32e59ed0264b7d17c204abcd7e97b6ecf610dd584241b899fd7d8fc6`；架构状态 `WAITING_BUSY_EVIDENCE`，不证明断电耐久性。
- `python .../preflight_and_launch_prep.py`：退出 0。只读预检无活动工作流、无未过期活动租约、无同代际成功研究回执，A1 为 SEALED。
- 唯一单元 `liangjian-next-session-prep-20261010-authorized-0743.service`，07:49:13 启动，PID 80967。正式命令 `.venv/bin/python -u -m liangjian_funnel run-next-session-prep`；启动回执不是研究完成回执。
- 预检 SHA256 `1c9a2b37e0e40aa55df60700122baf1c607396378a36bf06d49800f4dca12ba2`。
- `python .../audit_prep_readonly.py`：退出 0。07:50 检查 RUNNING / UNIVERSE_SYNC，实际进度 run_id 相符，尚无最终回执或计划。回执 `prep-audit-20261010T075017.json` SHA256 `8c6ffa3dc0723731fd75882e95a3e05e7805f22a25444de18b854876c759875d`。

研究 run_id：`next-session-prep-2026-10-12-a1-12c5a938f622-prep-v2`。使用当前活动 A1 `a1-incremental-20261009T180041188447Z-a2074f8fdabf`，不冒充 10-09 15:10 当时的代际。SHADOW 仍为原全范围公告路径；不能声称已经启用候选域流水线。

## 4. 四层验收

- CODE：已发布 adafe50 的既有验收保持有效；本轮没有新增生产代码。
- REPLAY：本轮未重放，不补历史信号；空闲基准是实测而非交易回放。
- OPERATIONS：idle 实测和清理通过；正式研究已启动，最终模型审核、池包含关系、发布有效期、待晨审与通知回执均待结果。
- STRATEGY：本轮不评估策略效果、不调整参数，不保证非零计划。

## 5. 未完成边界

busy 组未授权；A-LABEL 新 HEAD 未授权发布；六日原始快照备份位置未获用户回答，不能宣布永久丢失。研究正常运行不终止、不重复调用模型、不提前激活计划；遇真实阻断记录原因，不放宽条件制造计划。

## 6. 下一步

跟踪唯一单元自然结束，核对冻结输入、A1⊇A2⊇A3、去重、模型审核和 10-12 正式待晨审计划，再补齐本文件与桥接回执；完成或失败后结束本次跟踪。

已创建本线程跟踪 `10-10`，每 15 分钟只读验收；正常运行保持安静，完成/失败/需要决策时通知并删除。没有创建第二个研究或模型任务。

已读取新桥接答复0034–0036：SHADOW范围校准获确认；A-LABEL0035接受但并入新发布前必须移除模型投影中的新增标签或单独获授权；该条件不影响当前既有adafe50研究，未自行追加发布。

## 7. 08:00 后纠正与根因证据

以上 RUNNING 与 WAITING_BUSY_EVIDENCE 是当时观察，不能继续作为当前状态。唯一研究单元在 08:00:41 退出 4，CLI 仅记录 ValueError 类名；没有完成 A2/A3 或发布 10-12 计划。只读审计 `prep-audit-20261010T080602.json` 退出 0，SHA256 `85a562268355aea330ed1a8b50047442e53227d2cbb8862f332abd6dc8c7c6de`。systemd 峰值内存 1,593,823,232 字节，不是 OOM 证据；退出码标签也不是权限问题证据。

原回执纯函数复现定位到 close-scope JSON 哈希合同：整数 MA5/20/60 键序在落盘字符串化后改变。原预筛 1636 只、候选1035、延后601 已保存，维护队列随后校验失败；在副本只恢复已知两个 MA 路径的2004个键，两项原 hash 精确恢复。原文件未改。完整来源证据为 `artifacts/wp5-20261010/PREP_FAILURE_SOURCE_20261010.json`（SHA256 `c9395bbda7876be51ea291097bfb26f99a50dcd31ae5f0463fe792fd04603b5f`）；原日志没有错误全文，不将纯函数复现冒充原 traceback。

开发修复在隔离分支：新回执先转换为真实 JSON 形状再哈希，并拒绝字符串化键碰撞；旧回执只做精确哈希验证的窄兼容，不重新签名、不免校验。未重启失败研究、未重复模型调用、未再次发布。一次性自动化 `10-10` 已因研究失败删除。

已读桥接0037：空闲 fsync 支持未来采用同步尾部写入设计，忙时证据从自然运行回执积累，不再等待人为 busy 基准。本周末仍只测不接 writer；该结论不等于断电耐久性验收。
