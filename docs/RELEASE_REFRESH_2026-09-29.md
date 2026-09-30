# 9月29日发布与 A1—A3 刷新

## 最终验收：刷新完成，跟踪已删除

2255任务于23:29:32完成，退出0，业务结果READY。A1于22:47:42发布的新SEALED代际被本次研究明确复用，未再次运行A1模型。以下历史进度仅作过程证据，不再代表当前状态。

- A1研究池1472只；A2聚焦14只、观察181只，交给A3的集合共195只；A3正式发布23只。
- 三阶段均VALIDATED。只读集合断言通过：1472⊇195⊇23，三个集合无重复，正式execution_plans与A3核心池完全一致。
- 正式策略分布：趋势五日线20、520策略2、龙头策略1。所有计划PENDING_MORNING_REVIEW、valid_from为空、到期时间2026-09-30T15:00:00+08:00，未提前激活。
- 两只520（603110.SH、688155.SH）五分钟历史均144/144、READY。
- 下游独立快照snapshot-20260929T231600+0800-6ce06621ba10，哈希6ce06621ba1041ce75761f58fd36d78e2a21ad33edd7f23bfb280c07d3ce6265。不能称其与A1原快照相同。
- A2新闻旁路正常落盘：输入200、有效事实120、事件120；SHADOW_ONLY、llm_input_enabled=false、ranking_changed=false。上游省略1432条，不能宣称全量新闻覆盖。
- 本次run_research收盘入口不自动发送常规收盘报告；查询本批次通知账本无记录，不宣称飞书已投递。次日盘前/晨审通知及自然A4执行仍待到时验收。
- A1 PDF成功5005/5092，下游成功2147/2178，缺项保留，未靠放宽条件补足。

证据位于生产outputs/audits/2026-09-29-close-refresh-2255-operator-result.json、outputs/runs/2026-09-29-close-refresh-2255.json、outputs/research/research_2026-09-29-close-refresh-2255_lane_1.json及只读state/workflow.sqlite3执行计划。远程Python集合/有效期断言退出0。最终systemctl显示inactive、ExecMainStatus=0；pgrep未找到操作脚本进程（退出1表示无匹配，不是研究失败）。确认无进程使用后删除/tmp中2253、2255两个临时操作脚本，保留全部原始证据；删除一次性跟踪9-29-a1-a3。

CODE：318通过、1缺夹具跳过。OPERATIONS：部署、新A1发布及A2/A3正式计划发布通过；明日自然执行待验收。REPLAY：既有239窗口字段敏感性验证，不是新23计划全日回放。STRATEGY：阈值未修改，不作收益或稳定盈利结论。

## 历史接续记录（已完成，不再执行）

23:21复核：SSH身份一致，2255 unit/PID354309仍active，已进入FEATURE_SOURCE_GENERATION并持续研究心跳。PDF处理2178/2178，成功2147、失败31，保留缺项等待最终质量结论；系统可用1116MiB。未完成发布，不重复启动或改动生产。

23:11复核：SSH身份一致，同一2255 unit/PID354309仍active，CNINFO_SYNC至800/1593，累计失败仍2，系统可用1471MiB。进度持续推进，无完成/失败事件，保留任务和跟踪，不重复启动。

23:01复核：SSH身份192.168.1.254/debian一致，2255 unit active、PID354309。下游范围1593只，公司资料已1593/1593、该阶段失败2项；正在CNINFO_SYNC，75/1593持续推进。系统可用1516MiB，无退出事件，保留当前任务。1593是资料准备范围，不是A2最终入选数。尚未验收A2/A3研究及发布。

**接续纠正：2253任务在模型调用前失败，原因SNAPSHOT_ID_REQUIRES_HISTORICAL_REPLAY。此前文档“正式close支持显式snapshot_id”的判断不成立，不通过设置历史回放或比较运行绕过。现唯一有效接续为 `liangjian-a2a3-refresh-20260929-2255.service`，run_id=`2026-09-29-close-refresh-2255`，脚本 `/tmp/liangjian-refresh-a2a3-20260929-2255.py`。固定active_a1_generation_id不变，但删除显式snapshot_id/hash，设置reuse_resume_snapshot=False，由正式入口从刚发布A1范围重新准备下游快照。允许复用资料缓存，不重跑A1模型；新下游快照必须单独记录，不能称与A1快照完全相同。禁止重启2253或再启动同类任务。**

A1于22:47:42退出0且返回PUBLISHED，已只读核对活动指针指向SEALED代际 `a1-incremental-20260929T204729047284Z-813f00ba3f8b`。as_of/activated_at为任务起点2026-09-29T20:47:29.047284+08:00，实际完成发布时间以journal为22:47:42，不混淆两者。

新快照 `snapshot-20260929T222653+0800-d08e659a80d9`，哈希 `d08e659a80d93df357a04c206450c1a8768a52b41068eba18aa9776f65d894c7`。A1 systemd记录峰值内存2.2G、峰值swap398.6M，无OOM，结束后系统可用约2.2GiB。

确认无活动研究进程后，启动唯一A2/A3 unit **`liangjian-a2a3-refresh-20260929-2253.service`**，run_id **`2026-09-29-close-refresh-2253`**。入口临时操作脚本 `/tmp/liangjian-refresh-a2a3-20260929-2253.py`（本地artifacts同名逻辑），只调用正式run_research，使用上述固定A1代际、快照、哈希，主模型且关闭比较，发布次日计划。脚本校验同日/活动代际一致、已有结果拒绝重复；启动命令退出0不是业务验收通过。结果将落 `outputs/audits/2026-09-29-close-refresh-2253-operator-result.json`（以settings输出目录为准）。**后续只查询此unit与结果，禁止再次启动A1或A2/A3。**

完成后核对代际包含关系、模型审核、新闻旁路、正式计划与通知；验证后清理本次/tmp操作脚本，不删除原证据。

## 发布完成

- 代码提交/主分支/生产 HEAD：`0614b7a15a1f03b6c60fb5dbc5b8b6396e3af321`。
- SSH：aurum-vm → 192.168.1.254，hostname=debian。
- 生产：`/www/wwwroot/Agu/liangjian-funnel-workflow`。
- 执行既有 `bash deploy.sh`，退出0，未绕过交易时段保护，发布前无活动工作流。保留生产未跟踪 `.htaccess`。
- 七个模块 workflow、pipeline.research、pipeline.deterministic、pipeline.a1_registry、pipeline.a2_news_context、review.daily、review.verification 实际 `.venv/lib/python3.13/site-packages` 源码SHA256均与仓库一致。
- Node `/api/health` 正常；`/api/overview.scheduleMeta.running=true`，盘前、晨审、竞价刷新、A4、两次A5、收盘研究、A1维护均enabled；比较模型disabled。调度就绪不等于明日业务已验收。
- 相关合并测试318 passed、1 skipped，45.82秒，退出0。跳过旧生产夹具缺失项。未提交 `artifacts/` 中原始生产数据。

## 历史启动记录（当时尚未完成）

用户已授权部署及刷新A1—A3。20:47:28 使用 www 用户及既有env启动：

```
systemd-run --unit=liangjian-a1-refresh-20260929-2050 --property=User=www --property=WorkingDirectory=/www/wwwroot/Agu/liangjian-funnel-workflow --property=Environment=PYTHONUNBUFFERED=1 /www/wwwroot/Agu/liangjian-funnel-workflow/.venv/bin/python -m liangjian_funnel run-a1-maintenance --mode incremental
```

启动命令退出0；A1进程仍运行，非完成退出。run_id=`2026-09-29-a1-incremental-20260929T204729047284+0800`，初始PID353319。20:48资料同步3625只研究范围，RSS约424MB（命令kB值约424000），VmSwap=0。系统3.8GiB内存、约2.2GiB可用，交换区已有约1.7GiB占用；/www剩余21GiB。不要因系统已有swap就宣称本任务内存溢出。

## 初始接续设想（已被顶部纠正取代，不再执行）

1. 检查上述unit、journal和进程资源；禁止重复A1任务。必须核对 `state/a1_registry.sqlite3` 的活动代际已经更新、状态SEALED、as_of属于本次、快照ID/哈希一致。失败不跳过、不使用旧池冒充新结果。
2. A1成功后且无并发研究，调用正式 `WorkflowApplication.run_research('close', primary_only=True, schedule_comparison=False, publish_plans=True, from_active_a1=True, active_a1_generation_id=<本次代际>, snapshot_id=<本次快照>, snapshot_expected_hash=<本次哈希>, run_id_override=<唯一批次>)`。该方法已支持这些参数。使用当前北京时间，不设historical_replay；跨日需先检查正式目标交易日契约。使用www用户、既有env及systemd后台任务，保留日志/结果，不改变生产模型配置。
3. 不用默认CLI run-research隐式再次做A1，不用旧close快照脚本冒充本次新快照。新完整快照由A1本次维护采集，A2/A3复用这一份，避免再全量采集。
4. 验收A1⊇A2⊇A3、去重、审核状态、新闻旁路落盘、计划发布与次日到期、待晨审、通知回执；不提前激活次日计划。

已创建本任务续办心跳 `9-29-a1-a3`，每10分钟核对并继续上述已授权工作。正常运行不通知；完成、失败或需决策才通知，结束删除跟踪。没有预先报告A1/A2/A3成功。

CODE：318通过/1跳过。OPERATIONS：发布及调度就绪通过，数据刷新执行中。REPLAY：既有239窗口字段敏感性通过，非此次新计划策略回放。STRATEGY：门槛未改、无收益有效性结论。

## 21:01 续办核对

SSH身份再次一致；原A1 unit仍active、PID353319，未重复启动。公司资料同步已完成3625/3625（缓存3512、补采113、累计失败计数9），随后进入CNINFO_SYNC；21:01:39进度75/3625仍前进。该9项为同步阶段累计缺项，尚未取得最终质量/发布结论，不能等同整任务失败或忽略。RSS约674MiB，系统可用1615MiB；未因等待而重启或终止。A2/A3继续等待本次A1成功封存发布，自动跟踪保留。

21:11复核：同一PID/unit持续运行，CNINFO_SYNC已575/3625，累计失败仍9未增加；RSS约664MiB、VmSwap约17MiB、系统可用1595MiB。无完成或失败事件，不启动A2/A3、不重启、不重复发布，继续现有跟踪。

21:21复核：SSH身份一致，同一任务CNINFO_SYNC推进至1075/3625，累计失败仍9；RSS约675MiB、VmSwap约17MiB、系统可用1587MiB。任务仍active且进度持续前进，A1尚未发布，本次不启动下游、不改变生产状态。

21:31复核：SSH身份一致，原unit/PID仍active，CNINFO_SYNC推进至1525/3625，累计失败仍9。RSS约686MiB、VmSwap约17MiB、系统可用1551MiB。持续推进，无新增阻断，保留任务和后续跟踪；未启动A2/A3。

21:41复核：SSH身份一致，同一A1任务CNINFO_SYNC至1975/3625，累计失败仍9；RSS约696MiB、VmSwap约17MiB、系统可用1555MiB。仍在正常推进、未发布，不启动下游，保留原任务及续办跟踪。

21:51复核：SSH身份一致，原unit/PID正常运行，CNINFO_SYNC至2425/3625，累计失败仍9。RSS约706MiB、VmSwap约17MiB、系统可用1522MiB。进度持续前进，未出现新阻断；A1未发布，继续等待、不启动A2/A3。

22:01复核：SSH身份一致，原unit/PID仍active，CNINFO_SYNC至2850/3625，累计失败仍9；RSS约716MiB、VmSwap约17MiB、系统可用1513MiB。持续前进，保留现有任务和跟踪；未认定A1成功或启动A2/A3。

22:11复核：SSH身份一致，原A1 unit/PID仍active，CNINFO_SYNC至3275/3625，累计失败仍9；RSS约726MiB、VmSwap约17MiB、系统可用1523MiB。资料同步持续推进，尚未进入成功发布状态；不重复运行、不启动A2/A3，保留自动跟踪。

22:21复核：SSH身份一致，原unit/PID仍active，已从公告目录同步转入CNINFO_PDF_SYNC。22:21:43 PDF处理4802/5074，成功4782、失败20；这是PDF阶段计数，不能与此前公司资料9项混加或视为模型研究失败。RSS约913MiB、VmSwap约16MiB、系统可用1312MiB。任务继续推进，等待最终质量及封存发布核验，未启动下游或更改阈值。

22:31复核：SSH身份一致，同一unit/PID仍active。PDF阶段最终5092/5092、成功5005、失败87（总量随补采调整），随后已完成阶段切换至FACT_MANIFEST_SYNC、OPEN_MACRO_SYNC。RSS约1667MiB、峰值约1676MiB、VmSwap约23MiB，系统可用625MiB，内存压力较前增加但当前没有退出/OOM证据。保留正常任务，不并发启动下游；最终必须保留87份文档失败的质量归因，尚不能认定A1已发布。

22:41复核：SSH身份一致，原任务经过FEATURE_SOURCE_GENERATION，于22:39:48进入RESEARCH_MACRO_DISCOVERY，持续有55秒研究心跳，未退出。RSS约1431MiB、峰值约1676MiB、VmSwap约192MiB，系统可用758MiB；有交换使用但没有OOM/失败证据。仍需等待模型研究与代际发布，A2/A3未启动，不重复调用或部署。
