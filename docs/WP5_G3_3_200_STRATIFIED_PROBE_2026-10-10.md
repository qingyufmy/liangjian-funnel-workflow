# WP5 G3-3 真实 200 股名单与独立只读 probe

当前完成的是封存名单、独立脚本和合成夹具验证；尚未发起 Hithink 源调用，不是 G3-3 真实等价验收完成。脚本不修改 synchronizer/workflow，不创建 RuntimeStore、不调用财务、模型或通知；仅显式 `--execute` 读取配置和源。桥接 outbox 0002 的真实 200 股取证缺口仍在，0004 尚无新答复。

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

以下实测启动命令仅交主代理核对，尚未执行。将 `--env-root` 换成已核实的配置目录；不把 API key 放命令行。输出必须是新目录，在非交易时段执行：

```powershell
$env:PYTHONPATH='D:/dev_A股/liangjian_wp5_hotfix_20261009/src'
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe scripts/probe_wp5_daily_incremental_200_readonly.py --manifest artifacts/wp5-20261010/daily-incremental-200-manifest.json --expected-sha256 5534fb343c14f0b8ead9de241795555ba2b8cc01cbfa027f40145cdcd6771180 --execute --env-root '<verified-config-root>' --output artifacts/wp5-20261010/daily-incremental-200-real-v1
$taskExit=$LASTEXITCODE; Write-Output "PYTHON_EXIT=$taskExit"; exit $taskExit
```
