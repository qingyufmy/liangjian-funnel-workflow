# WP5 G3-3 真实 200 股名单与独立只读 probe

已完成封存名单、独立脚本、合成夹具验证及一次真实只读源实测：200/200最终规范化序列MATCH，199实际INCREMENTAL、1合法FULL_REFRESH。脚本不修改 synchronizer/workflow，不创建 RuntimeStore、不调用财务、模型或通知；仅显式 `--execute` 读取配置和源。这闭环本次真实源序列等价取证，不是10-08/10-09历史PIT或冻结血缘回放验收，不是复权因子契约验收。

## 封存名单

名单：`artifacts/wp5-20261010/daily-incremental-200-manifest.json`。文件 SHA256：`5534fb343c14f0b8ead9de241795555ba2b8cc01cbfa027f40145cdcd6771180`。

源为本地 10-08 raw 快照 `snap-3377a346d297f37ef472d6e8.json`，as_of `2026-10-08T16:33:51+08:00`，5578 个唯一 symbol，SHA256 `54330ad74aec6150132a2c3f69cceb16a5e23274483a2b85e3d4c799b706d16a`。不从 A5 投影或短历史推定新股。普通样本依证券代码/交易所归为 MAIN、CHINEXT、STAR、BJ，排除下面 10 个特殊，按 `SHA256('WP5-G3-3-200-v1:'+symbol)` 升序、symbol 作为次序键，确定性选 80/50/40/20。特殊另列 10，200 股不重复；“普通”只是本次非特殊分层，不宣称它们全部没有公司行动或停牌。

特殊日期的官方参考文本和 URL 封存在 `daily-incremental-200-special-reference.json`，SHA256 `fc8d1fc962baa728a9aebbaf918c8a352807eecef0a66edd81832fde3a2cfbfd`。这里封存的是名单及人工核对的官方链接/日期文本，不宣称已经下载、哈希并封存原 PDF 字节。

| symbol | 事件实际日期 | 官方来源 |
| --- | --- | --- |
| 301697.SZ 贝特利 | IPO 09-01 | [上市公告，08-31发布](https://static.cninfo.com.cn/finalpage/2026-08-31/1225523803.PDF) |
| 301688.SZ 格林生物 | IPO 09-02 | [上市公告，09-01发布](https://static.cninfo.com.cn/finalpage/2026-09-01/1225537225.PDF) |
| 301699.SZ 洛轴股份 | IPO 09-09 | [上市公告，09-08发布](https://static.cninfo.com.cn/finalpage/2026-09-08/1225551178.PDF) |
| 301689.SZ 电科思仪 | IPO 09-10 | [上市公告，09-09发布](https://static.cninfo.com.cn/finalpage/2026-09-09/1225554589.PDF) |
| 688801.SH 燧原科技 | IPO 09-11，不是公告日09-10 | [上市公告](https://static.cninfo.com.cn/finalpage/2026-09-10/1225556294.PDF) |
| 688837.SH 信诺维 | IPO 09-16 | [上交所上市公告，09-15发布](https://www.sse.com.cn/disclosure/announcement/listing/ipo/c/c_20260915_10832185.shtml) |
| 301686.SZ 中塑股份 | IPO 09-22 | [公司上市首日风险提示](https://static.cninfo.com.cn/finalpage/2026-09-22/1225577873.PDF) |
| 001246.SZ 力勤资源 | IPO 09-30 | [公司上市首日风险提示](https://static.cninfo.com.cn/finalpage/2026-09-30/1225586764.PDF) |
| 600825.SH 新华传媒 | 09-07停牌，09-08继续停牌 | [公司停牌公告，09-08发布](https://static.cninfo.com.cn/finalpage/2026-09-08/1225552231.PDF) |
| 600929.SH 雪天盐业 | 停牌后09-14复牌 | [上交所官方摘要，09-12发布](https://www.sse.com.cn/disclosure/listedinfo/summaries/indexDetail.shtml?SEQ=334691316) |

600929 是九月停复牌事件，不将 09-05 进展公告日当停牌起始日。实际源是否提供停牌日 bar 由 core 校验；不把空响应补为零成交或造 bar。

## 实测接口与输出

入口 `scripts/probe_wp5_daily_incremental_200_readonly.py`，复用 `HithinkIncrementalSynchronizer.sync` 与 `LocalFactCache`。同一限流 client 逐股执行：独立缓存 full bootstrap（10-08 15:10），该缓存 incremental（10-09 15:10），另一空缓存 full（10-09 15:10）。均 `lookback_days=800, compact_daily_bars=1000, include_financial=False, collect_early_discovery=False, adjust=none`，不改变 core 的 last-3 overlap、短历史 readiness、修订 full-refresh 和 transport retry/throttle。

原 raw 快照仅约30根日线，不足以假充800日基线，所以真实 probe 至少600次 `history_1d` 调用，另有现有 core 修订回退和 transport 重试；不是只请求200次。实际 HTTP attempts 保留在回执，`source_calls` 是调用 history 方法次数。不构造 `history_coverage`，真实 bootstrap 返回才由现有 core 写 coverage receipt。

回执与bar数组都是现有client/core输出的类型化或规范化投影：`compact_daily_bars=1000`是core投影上限，比较的是同一800日查询窗口内core实际返回的timestamp/OHLCV/turnover，不是原HTTP body逐字节比较。既有history paginator未返回原HTTP body SHA，真实600份receipt的response_sha256均null；不能将规范化数组hash冒充raw transport hash。重试计数依现有transport返回的attempts元数据（未提供时使用其一次成功调用约定）。

执行时配置仅供 Settings/HithinkClient；不采用 Settings 配置的生产 cache 路径。新输出目录下写 `incremental.sqlite3`、`full.sqlite3`，逐股 `records/<symbol>.json`、总 `report.json`、`manifest-binding.json`，所有 JSON 使用独占创建不覆盖。SQLite 只用于本次隔离验证，不代表生产数据库。每次源方法调用前检查真实当前交易日与09:00–15:30窗口，原限流低于0.5秒拒绝。进程中断时已完成逐股文件保留；不自动重用旧输出，另起新目录。

逐根比较 target 相同800天范围，timestamp 与 OHLCV/turnover 六字段严格比较；int/float 归一化使用 core 价格量 identity，输出每根差异字段、双方根数、规范化数组 SHA。缺字段、空数据、任何阶段 core/source 失败明确 DATA_LIMITED，原 exception/provider metadata 不落盘；失败不会被相同空结果升级 MATCH。合法 full fallback 单独统计 mode，MATCH 仅表示最终 raw 序列一致，不等于所有股票都走纯增量、更不证明未知复权契约或09月停牌处理全部正确。

本次按历史区间查询现在的源，`observed_at/fetch_time` 是真实观察时间；10-08/10-09 是行情查询截止日，不是伪造历史 point-in-time receipt。不能将该 probe 当作10-08/09冻结血缘回放或生产发布证据。

## 可复现命令与退出码

PowerShell 每条 Python 命令均保存并显式返回 `$LASTEXITCODE`，避免 shell 把真实 Python 2 映射成1。

首次反例命令：

```powershell
$env:PYTHONPATH='D:/dev_A股/liangjian_wp5_hotfix_20261009/src'
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -m pytest tests/test_wp5_daily_incremental_200_probe.py -q
$taskExit=$LASTEXITCODE; Write-Output "PYTHON_EXIT=$taskExit"; exit $taskExit
```

结果：实现前 `FileNotFoundError` collection 失败，`PYTHON_EXIT=2`。实现后首次6项退出0；加失败/secret反例后的独立7项退出0。最终相称验证：

```powershell
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -m pytest tests/test_wp5_daily_incremental_200_probe.py tests/test_wp5_daily_incremental.py -q --junitxml=artifacts/wp5-20261010/daily-incremental-200-tests-final.xml
$taskExit=$LASTEXITCODE; Write-Output "PYTHON_EXIT=$taskExit"; exit $taskExit
```

合成测试覆盖：名单证据/日期、SHA篡改与重复、默认不读配置不造cache、实际core三阶段真增量及>30根完整历史、差异/缺字段/空样本、已有目录拒绝、交易窗拒绝、失败不泄漏；既有14项保持验证last3、历史修订、短IPOcoverage、空响应、future cursor。真实源行为仍未验证。

最终组合回执 `daily-incremental-200-tests-final.xml`：21项、0失败、0错误，`PYTHON_EXIT=0`。随后最小修正使manifest binding在源调用前写入、缺request元数据不导致失败日丢失，独立7项再跑，`daily-incremental-200-probe-final-v2.xml` 7项通过、退出0。最终脚本 SHA256 `343d4f22facb480955e484f8c722377a5ec39c55258f202cff60b72bd3df8c2c`。

封存命令已执行，`PYTHON_EXIT=0`：

```powershell
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe scripts/probe_wp5_daily_incremental_200_readonly.py --build-manifest --raw-snapshot artifacts/wp5-20261009/readonly-20261008/snap-3377a346d297f37ef472d6e8.json --special-reference artifacts/wp5-20261010/daily-incremental-200-special-reference.json --output artifacts/wp5-20261010/daily-incremental-200-manifest.json
```

最小 dry-run 已执行，退出0、`source_calls=0`：

```powershell
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe scripts/probe_wp5_daily_incremental_200_readonly.py --manifest artifacts/wp5-20261010/daily-incremental-200-manifest.json --expected-sha256 5534fb343c14f0b8ead9de241795555ba2b8cc01cbfa027f40145cdcd6771180
```

以下是原Windows工作副本最小实测命令模板；正式实测已按后节提交LF归档命令执行一次。将 `--env-root` 换成已核实的配置目录；不把 API key 放命令行。输出必须是新目录，在非交易时段执行：

```powershell
$env:PYTHONPATH='D:/dev_A股/liangjian_wp5_hotfix_20261009/src'
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe scripts/probe_wp5_daily_incremental_200_readonly.py --manifest artifacts/wp5-20261010/daily-incremental-200-manifest.json --expected-sha256 5534fb343c14f0b8ead9de241795555ba2b8cc01cbfa027f40145cdcd6771180 --execute --env-root '<verified-config-root>' --output artifacts/wp5-20261010/daily-incremental-200-real-v1
$taskExit=$LASTEXITCODE; Write-Output "PYTHON_EXIT=$taskExit"; exit $taskExit
```

## f0c20b7 真实源实测回执

证据根 `artifacts/wp5-20261010/daily-incremental-200-real-f0c20b7-20261010T0038/`，`FINAL_RECEIPT.md`记录完整命令与退出码。完整结果是`download/result/report.json`、`download/result/records/<symbol>.json`、`download/result/manifest-binding.json`，输入binding为根部`input-files-binding.json`、`preflight-receipt.json`；输出binding为`download/output-files-binding.json`和`download/output-audit.json`。官方日期原参考在`artifacts/wp5-20261010/daily-incremental-200-special-reference.json`，提交LF字节副本在证据根`archive-input/artifacts/wp5-20261010/`，各自真实byte hash不得混用。

VM重新核验aurum-vm=192.168.1.254/debian，生产前后HEAD `47f6ef03b042c0f9a207400c72d2ecffff338244`。使用www UID1001、生产现有venv和只读Settings，启动前无并发源任务；source/src/probe/manifest来自`f0c20b7858d729eb12106e86a085763e594c7e3a`的git archive，173输入文件上传/执行前后全部SHA一致，模块实际导入唯一/tmp隔离src而非生产旧site-packages。生产.env、缓存、DB、服务未写；源实测期间本地tracked diff为0。

| 项目 | 实际结果 |
| --- | --- |
| 结果 | MATCH200 / CONFLICT0 / DATA_LIMITED0 |
| 分层 | MAIN80 / CHINEXT50 / STAR40 / BJ20 / SPECIAL10全部MATCH |
| 请求 | history方法600，response_received600，现有attempts回执合计600，重试0 |
| seed/full模式 | 各200 FULL_REFRESH / HISTORY_SHORT_BOOTSTRAP |
| 增量模式 | INCREMENTAL199 / FULL_REFRESH1 |
| 单个合法fallback | 001246.SZ，seed2根→target3根，HISTORY_SHORT_BOOTSTRAP |
| 修订与额外重拉 | HISTORICAL_REVISION_CONFIRMED0，incremental额外full请求0 |
| 规范数组hash | 200/200双方相同 |
| raw HTTP hash | 未提供，hashed_response_count0，不伪造 |
| 业务退出 | SSH session52667，REMOTE_PROBE_EXIT0，report.exit_code0 |
| 回收 | 204文件逐一SHA复核0冲突，含2DB与200records |

提交LF manifest SHA `729b6ffd79332668c11b1af538dc6d2684b4518636771f59fe6076f33bbd6d9c`；原Windows CRLF副本SHA `5534fb343c14f0b8ead9de241795555ba2b8cc01cbfa027f40145cdcd6771180`。提交probe SHA `c7aa2ba81e3aab5f3cbe28b0463e30231a51b840d71531d0ebc2da46256ba520`；input archive SHA `76a483355cdd58cbc74ae4fa545d2f9a36d028fe9779092e462782a97d7f8d32`；output archive SHA `bdb74b89b00f9642f5a0d2b066ec02932d0414300bfa84d73fe4ea81d0b8bc7b`。首次preflight因checkout/commit换行hash混用退出1，首次旧manifest参数CLI在门禁退出1，二者均源调用0；只修artifact binding/启动参数，正式源仅执行一次，没有边运行边改源码。

正式命令（实际退出0）：

```sh
cd /tmp/liangjian-wp5-g3-200-f0c20b7-2XchyDsA
runuser -u www -- env PYTHONPATH=/tmp/liangjian-wp5-g3-200-f0c20b7-2XchyDsA/src PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 /www/wwwroot/Agu/liangjian-funnel-workflow/.venv/bin/python -B scripts/probe_wp5_daily_incremental_200_readonly.py --manifest artifacts/wp5-20261010/daily-incremental-200-manifest.json --expected-sha256 729b6ffd79332668c11b1af538dc6d2684b4518636771f59fe6076f33bbd6d9c --execute --env-root /www/wwwroot/Agu/liangjian-funnel-workflow --output /tmp/liangjian-wp5-g3-200-f0c20b7-2XchyDsA/result
```

审计辅助脚本只读JSON和文件字节，不打开SQLite。下一独立核验应检查200record规范hash、timestamp≤target、600call mode与本地DB完整性，无需重复源请求。源输出是当前查询历史，不是历史PIT。

主代理独立第二次读取已完成，命令 `python -B artifacts/wp5-20261010/verify_200_probe_independently.py` 退出 0；回执 `independent-second-reader.json`。逐一检查 204 输出字节、200 只完整数组逐字段相等、排序/唯一/查询截止、规范数组 SHA 与请求数/模式；两份 SQLite 用 `mode=ro&immutable=1` 的 `PRAGMA quick_check` 均为 `ok`，检查前后全部输入 SHA 不变。未重复网络请求，未创建 WAL 或修改数据库。该回执补充的是源增量等价，不升级为历史 PIT、HTTP 原文或复权字段语义证明。

本地输出204文件SHA复核后，经主代理授权，readlink/stat确认唯一/tmp resolved路径为上述本次目录、www所有且非symlink，仅精确清理该/tmp目录，退出0。所有本地证据、input/output归档保留可恢复；不修改生产.env、缓存、服务。
