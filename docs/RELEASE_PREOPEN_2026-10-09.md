# 2026-10-09 发布与盘前验收

## 结论

08:13之前已通过既有`bash deploy.sh`完成发布，退出0。生产与origin/main功能提交为`f9bbe9a953ed95cbbc63c6092de2f6865f06d7d2`。**代码发布通过，今日新增仓业务准备未通过：今日正式执行计划0，竞价资料基线失败，板块来源仍有缺口。**不能把服务在线称为今日A4稳定、也不能保证会产生交易信号。

生产地址重新通过SSH别名核验为`192.168.1.254`，hostname=`debian`，目录`/www/wwwroot/Agu/liangjian-funnel-workflow`，操作使用既有www用户和env。没有修改代理、凭证、策略阈值、数据库记录或历史交易信号；没有激活过期计划。

## 发布证据

- 发布前生产`522ec802f00c800c6b70611101efef8e3929a502`，无活动工作流，G0 bootstrap inactive。
- 仅提交本轮13个归属明确的代码、配置、测试和说明文件，包含已提交的`5f2d88d`参考源旁路。推送origin/main快进成功；保留其他未提交文档、网络探针、artifacts及生产.htaccess。
- `bash deploy.sh`正常fetch、ff-only更新、强制重装Python wheel、Node前后端构建、BaoTa停止/启动、持续健康检查，退出0。没有使用`--allow-intraday`。
- 11个模块实际导入路径均在Python3.13 site-packages，字节SHA256与src一致：board_reference、hithink_board_reference、rotation_theme、data_source、auction_base、settings、workflow、publication、live_fetch、tencent_minute、monitor。
- Node PID51130工作目录正确，scheduler_enabled=true，08:07:01记录调度器启动；健康200。健康与启动只是服务证据，不代替业务验收。
- 本轮相关本机测试242 passed，已在数据迭代报告记录命令及退出0。未因发布重复执行同一切片，未将本机单测称为全日生产验收。
- 虚拟机总内存3883MiB、可用约1827MiB；swap使用1057/2047MiB；磁盘可用17GiB。当前未观察到活动任务或OOM证据，不能由swap使用量推断基线被OOM杀死。

## A1—A4准备情况

| 项目 | 真实状态 | 影响 |
|---|---|---|
| A1 | 10月8日18:00维护，3975评估、1749入选，活动FULL代际SEALED | 不必再启动全量A1 |
| 活动代际 | `a1-full-20261008T180015072172Z-fe5a5e7ecc11` | 当前A1是新代际；昨晚下游使用1451池，不是本代际1749池 |
| A1快照 | `snapshot-20261008T194056+0800-987228a8c39f` | hash=`987228a8c39ff32d87d34daf13e5b000325c2ee496c699c4a5aa38c18b22f7dc` |
| 最近A2/A3 | `2026-10-08-close-a2-audit-172755`；A2焦点4，A3研究核心3、观察2，但正式发布0 | PUBLISHED批次不等于有执行计划；不能激活研究股票代替发布流程 |
| 今日正式计划 | 按今日expires_at查账本为0，无持仓 | A4新增仓无标的，不能误称“行情正常所以可正常发信号” |
| 07:00 auction-base | 07:00启动，08:00由Node60分钟超时终止，退出2，marker BLOCKED/AUCTION_BASE_PROCESS_TERMINATED | 09:26竞价研究没有READY基线；不是本轮部署终止任务 |
| 主板块源 | 实际Settings为EASTMONEY；最新27份成员缓存9月21日，至今18天，超过既有14天上限 | 不能延长有效期或改日期强行恢复 |
| 新同花顺源 | 代码已安装，但仅完成18/27主题映射，配置仍未切换 | 不能把部分映射冒充全量top5；9个宽方向见数据迭代报告 |

07:00任务根因目前能证明的是**prepare_snapshot慢资料同步未在父进程预算内完成**。该调用没有传入阶段progress，现存全局progress是03:30 features NOOP，不能据它断言基线卡在CNINFO、网络或模型。没有模型调用，不能把60分钟归因于推理。需要增加阶段时间与全局预算、复用经核验的慢变资料并只补增量，而不是简单提高任务时限或伪造READY。

08:30 run-premarket是已有A3的盘前分析/通知；09:26 morning是已发布计划晨审；09:26 auction-refresh明确`publish_plans=False`，研究不替换A4计划。因此上述自然任务即使成功，也不代表当前空执行池可以自动恢复。恢复新增仓需要板块链路完整验收后，走正式研究与发布契约；本次没有重复启动已知数据缺口的模型研究。

## 数据与通知

08:11虚拟机低频样本：东财目录及BK1134成员各请求一次，均HTTP502、157字节，正文hash均`24fe03fdb26088bfff8b1363911c29c79a19614bca42cdcdb9fefdf52917aeb1`。这证明当前两个接口不可用，不证明永久公网IP封禁，也不代表全分页验收。

腾讯600026.SH、603906.SH、600519.SH各12根一分钟线均返回OK，末根为10月8日15:00；报价分别为10月8日16:14时段，严格返回QUOTE_TRADE_DATE_MISMATCH。盘前没有今日成交时这是正常的日期阻断，不是放宽为今日报价的理由。上述样本只证明传输及标准化，**今天开盘后的新鲜度和闭合分钟仍待自然观测**。

飞书配置存在，昨日下午A5成功投递；14:58—14:59三个数据告警最后为LARK_NETWORK_ERROR，两次尝试后FAILED。保留历史记录，不补发旧交易信号，不声称今日通知已经通过。今日08:30尚未到达，今日通知账本为空不能判为漏执行。

Windows独立任务Liangjian-A4-External-Watchdog=Ready，最后自然运行08:10:10、退出0，下一次08:11:11。它仍使用原项目工作目录；08:20之前的空问题清单不能当成已经完成远端交易时段验收。

## 验收分层与后续

- CODE：本轮242项相关切片通过；发布、构建和11模块实际源码核对通过。
- REPLAY：此前离线来源/日期/完整性反例通过；新27方向全量冻结行情对照尚未完成。
- OPERATIONS：服务与调度启动通过；今日计划、基线和板块链路BLOCKED；08:30、09:26、09:31之后自然业务与通知待证据。
- STRATEGY：评分、数据门禁、三套技术策略、T+1未改；今日机会与收益未验收。

已创建本线程10月9日08:35、09:35两次只读跟踪（automation_id=`10-9-a4`）。08:35检查盘前通知与空池说明；09:35检查晨审/竞价刷新、真实A4分钟执行、当日报价、watchdog。只报告变化、新失败或完成；09:35完成后删除，不跨日、不自动再发布或补信号。

下一步需要落地的修复：核验并补齐剩余9个宽方向的显式组件映射、完成27方向独立影子验收；为auction-base增加独立阶段进度及有界增量准备，定位并消除重复慢同步。不能以延长旧映射有效期、缩减主题数或增加候选数制造准备就绪。

## 命令与证据

```powershell
git push origin HEAD:main
ssh aurum-vm 'cd /www/wwwroot/Agu/liangjian-funnel-workflow && bash deploy.sh'
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe scripts/audit_preopen_readonly.py --output artifacts/readiness-20261009/deployed-preopen-0813.json
```

以上均退出0。初次只读探针误读不存在的Settings.model后退出1，改为research_models/monitor_model/review_model复核成功；这不是生产模型故障。初次root直接git遇到仓库所有权保护，改用仓库所有者www后成功，没有修改全局safe.directory。

本地证据`artifacts/readiness-20261009/deployed-preopen.json`及`deployed-preopen-0813.json`不可覆盖；前者保留完整A1 manifest，后者缩减为计数，避免重复大输出。新增audit_preopen_readonly.py为本机诊断入口，只在本地创建新证据，不改生产数据库，不调用模型或通知；可用于后续两次跟踪。
