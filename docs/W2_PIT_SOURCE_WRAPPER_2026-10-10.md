# W2 同次响应 PIT 来源包装器（2026-10-10）

## 结论和范围

纯本地 source wrapper 已实现并冻结；48 项新测试与既有 capture/Tencent 切片合计 **84 passed，Python native exit 0**。未接生产或真实源，未修改 provider、A4、App、既有 capture/price_limits/ledger、共享 state、限流、通知和数据库。没有新网络请求、部署、提交或 push。这是组件合同通过，不是周一 PIT 已上线，更不是自然交易日/真实成交验收。

授权原文是 canonical `.claude_bridge/outbox/0041-reply.md`，SHA256 `5480eccf93678ffde0c427a0ed323e9d5629c287f56cb8b09cceac86c756599e`。本轮只落实其同次原字节包装、当前名称、前收交叉校验及原 ticker 包引用合同。根代理另封存 `artifacts/wp5-20261010/official-rules-20261010`，receipt SHA `bbcdb3dd7ddf487593d88b89c10a49176907aa837b1c924d6161c0d7b8a8a813`；没有新修订搜索命中不能证明未来无修订。**`RULE_REVIEWED_THROUGH` 仍为 2026-10-10，未延至 10-16；10-12 DERIVED 因规则日期缺证保持 UNKNOWN/RULE_DATE_UNPROVEN。**

本轮唯一新增源码/测试：

| 文件 | SHA256 |
| --- | --- |
| `src/liangjian_funnel/runtime/shadow_pit_sources.py` | `9002b6c46bd7088ef863cab4618ec3d1e085ceed8793b46793126159fe74e5f0` |
| `tests/test_w2_shadow_pit_sources.py` | `80d56c681eef62b5489bed49f0dcf3fd229b42f844bb7d0664c9c978b9610af8` |

## 可消费接口

`RecordingTextFetcher(fetch_response=None, *, clock=None, archive=None)` 兼容 `TencentIntradayAdapter(text_fetcher=...)` 的 `(url, params, timeout) -> str` 回调。只允许既有 `https://qt.gtimg.cn/q` 及单证券 q 参数；注入的 `fetch_response` **只调用一次**，原 timeout 不变，没有 wrapper 重试、额外请求或新限流。获取该响应的 `content` bytes、请求起止时钟、状态和参数，再按既有 GBK 解码语义返回文本。默认不构造 HTTP client，缺 fetcher 明确 UNWIRED；注入时钟也明确 `INJECTED_CLOCK_NOT_AUTHENTICATED`，不冒充 wall clock。

`record_raw_response(bytes, *, source_ref, endpoint, request_parameters, request_started_at, response_received_at, ...) -> RawResponseReceipt` 保存原 bytes SHA、size、元数据 receipt SHA、byte_kind、clock_basis、source_authenticated=False。拒绝缺 bytes、凭证参数、非字符串 JSON 键、错误时间范围和损坏绑定。

`RawReceiptArchive(new_root).store(receipt)` / `.load(receipt_sha)` 只写新的独立目录：每份 receipt 为 `response.bin` 和 `receipt.json`。写入 fsync 后 rename，重复相同内容幂等；已有损坏或冲突不覆盖。wrapper 保存失败记 `archive_errors`，不改变现有 provider 返回值，不写生产缓存/DB。

`TickerCatalogPackage(pages=tuple[RawResponseReceipt,...], complete=True)` 表示原 ticker 多页响应。`JSONFieldSource(receipt, field_map=None)` 表示原 T−1 日线或公司行动 JSON，显式 JSON pointer 进入来源证明。

```python
result = build_tencent_pit_source(
    symbol, quote_receipt, observed_at=received_processing_clock,
    ticker_catalog=raw_catalog_package,
    prior_close=raw_daily_source,
    corporate_action=explicit_ca_source_or_none,
)
source_receipt = result['source_receipt']  # PITSourceReceipt | None
# 由现有 capture 验证 plan target/pending/active 等合同后才允许独立 seal。
# build_plan_pit_capture(plan, source_receipt, observed_at=..., known_at=...)
```

`result` 保留 `status/reason_codes/evidence/limits/preclose_crosscheck/source_bindings/original_quote_raw_sha256/consumer_input_sha256`。未知限价为 upper/lower null；source 失效可保留安全解析的 audit 字段，但不发可消费 receipt。只有 wrapper 与既有 capture 均 COMPLETE，再真实独立持久 seal 成功，consumer 才有资格声明 READY。单个 typed status 不是实盘认证。

## 字节和身份血缘

| 证据层 | 实际口径 |
| --- | --- |
| 原响应内容 | `HTTP_RESPONSE_CONTENT_BYTES` 是注入响应的 `response.content`，不是压缩 wire/TLS 字节，更不自动认证 HTTP 签名。caller 原包另标 ORIGINAL_PACKAGE_BYTES 或 CONSUMER_INPUT_BYTES。 |
| 原 receipt | 原 bytes SHA、请求参数、timeout_argument、endpoint、起止时钟、状态、complete 和 byte/clock basis 共同绑定。不能通过 mtime 重标旧日。 |
| composite consumer JSON | 原 quote/catalog/prior/CA bytes 以 BASE64 可逆嵌入，分别保留 receipt/hash 与提取指针、实现 SHA。它自身标 `CONSUMER_INPUT_BYTES`，不是单次 HTTP 原响应。 |
| capture limit 绑定 | `source_input_sha256` 绑定 composite bytes。`raw_http_bytes_available=False`；单次 quote 的原响应 SHA 是独立血缘，不偷换为 composite SHA。hash 证明一致性，不证明来源认证。 |

名称采用同一腾讯原 quote 的 index 1，证券 code index 2，昨收 index 4，行情时间 index 30，复用 `_quote`。名称含 ST/*ST 提供 is_st=True；**名称不含 ST 不证明 is_st=False 或普通/非新股制度**。quote 必须错股拒绝、同日 quote/received、quote 不晚于 received/known，09:25 后且不超过 180 秒，处理时钟在交易日 09:26 至 09:30 前。严格 GBK identity 解码失败不接受名称。

vendor 昨收先标 `VENDOR_RELAYED_EXCHANGE_PRECLOSE`。仅在显式交易日历 T−1、同股、raw/none adjustment 的原 close 精确 Decimal 相等时，按 0041 合同映射成 capture 的 `EXCHANGE_DISPLAYED_PRECLOSE` 兼容口径；证明中保留 vendor 出处与 `MATCHED_T1_RAW_CLOSE`。T−1 原包允许 T−1 收盘后观察，不能接受其上午尚未收盘包。精确相等不自动认证不存在公司行动。

不等时必须有同股、当日 ex_date、明确 event_id、`EXCHANGE_CORPORATE_ACTION_REFERENCE` basis 的 source reference 精确相等才走 CA 核对，并记录 `EX_RIGHTS_REFERENCE_OBSERVED`；没有证据则 `PRECLOSE_CROSSCHECK_MISMATCH`。qfq 因子、价格差或 local 派生参考不能贴成交易所显示真值。现有 `data/corporate_actions.py` 的确定性参考/因子合同不被改变；**真实 CA 原包字段语义及认证仍待接线**。数字 prior_raw_close 保留 capture 数字口径，精确 decimal text 则保留在独立证明，避免字符串类型差异伪造除权注记。

上市日期只从原 `ticker_catalog` pages 提取：endpoint `/api/meta/tickers/list`、asset_type/exchange/offset/limit、code0、完整 rows/total、所有页当前观察日期、重复冲突检查。日期别名或身份字段必须原包明确提供，原 row/page/JSON pointer 可追溯；不从 SecurityRecord、symbol 前缀或 listed_days 推断。缺原包、旧包、partial、缺 total、未来日期或上市日期缺失保持 UNKNOWN。普通制度所需 board/security_type/security_status/is_st/limit_regime 也必须原包明确给出。

没有读取腾讯未经验证的涨跌停索引；`explicit_limit_indices=null`、`PENDING_REAL_0926_MULTI_BOARD_SAMPLES`。DERIVED 仅调用原 price_limits 的严格普通证券合同；ST、IPO 正常期缺证、BJ、身份未知、规则超期不放宽。显式同日 limits 的优先/冲突规则仍由既有 capture/price_limits 持有。

## 实际源码路径与未接线入口

`data/tencent_minute.py:103` 默认 fetcher 把响应变成 GBK `response.text`，原 bytes 丢失；`:189` fetch_quote 和构造器已有 text_fetcher 注入点，`:270` 是真实 parser。**当前 App/09:26 生产实例未注入新 wrapper**。根代理必须在授权 consumer 装配处包装同一次原请求；另外启动 shadow adapter 再取一次 quote 会成为额外请求，不符合 0041 同请求要求。

`pipeline/data_source.py:197` ticker_catalog 调 `/api/meta/tickers/list`，`:889` `_request_page` 的 JSON/HithinkRow 投影未携带原 response.content；`:77` HithinkFetchResult 不能当原响应 receipt。现有 typed catalog 无法自动恢复原字节。需要已有同次传输原包或合法封存完整 pages；本模块不改其同步器或 provider。

本轮在本地指定 artifact 目录只找到一个实际腾讯解码文本样本，无 09:26 多板块 HTTP 原字节、完整当日原 ticker 包或公司行动原包证据。这是所查范围的缺证，不声称全仓所有位置都无证据。

## 本地实际样本核对

源 `D:/dev_A股/liangjian_a4_20260923/artifacts/diagnosis-20261008/quote-backup-shadow.json` SHA `039e4a73764dddad97360ebc53e980a65ba2a4c9048b97950066d6492600983c`。

`scripts/probe_live_quote_backup.py:24-25` 实际保存的是 GBK response.text 再 UTF8 encode 的 SHA，不是 response.content。核对得到 600189.SH/泉阳泉、vendor 前收 8.95、quote 2026-10-08T15:49:23+08:00，接收 15:49:41.771517，88 个分隔字段。已保存 decoded UTF8 SHA `3c5f508caa540b1eae770ddf15c685e8c4e271cd9d1d582235f28075f22c33f1`；不以 GBK 重编码伪造原 HTTP 字节，不解释其余数字为显式 limits。

独立 inventory `artifacts/w2-sources-20261010/existing-quote-sample-inventory.json` SHA `eb200198bd5815764f799476e4f439b88bf287ba6e471f986fb4d3a295504ba8`；probe `inspect_existing_quote_sample.py` SHA `e608c4e9ad9fccb0a772a4b0058bfbc083f071060e52bfaf1a081539da16e3e7`。probe 输入前后 SHA/bytes 不变，源请求 0，真实 native exit 0。它只验证 parser 的现有字段，不证明 09:26、跨板块索引、原字节或 PIT 上线。

## 反例与实际命令

工作目录 `D:/dev_A股/liangjian_wp5_hotfix_20261009`，实际解释器 `D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe`。所有 pytest 命令前设置 `PYTHONPATH=D:/dev_A股/liangjian_wp5_hotfix_20261009/src`；完整结果保存以下 XML，未重复全量测试。

| 阶段 | pytest 参数（共同前缀 `python.exe -B -m pytest`） | 实际结果 / native exit |
| --- | --- | --- |
| 最初红 | `tests/test_w2_shadow_pit_sources.py -q --tb=short --junitxml=artifacts/w2-sources-20261010/sources-before.xml` | 37 failed / 1 |
| 首轮绿 | 同切片，XML `sources-after-v1.xml` | 37 passed / 0 |
| 新增绑定/encoding 反例红 | 同切片，XML `sources-before-guards.xml` | 4 failed, 41 passed / 1 |
| 相关绿 | 三切片，XML `sources-related-final-v1.xml` | 81 passed / 0 |
| T−1/CA/额外绑定反例红 | 新切片，XML `sources-before-prior-guards.xml` | 3 failed, 45 passed / 1 |
| 联合中间结果 | 三切片，XML `sources-related-final.xml` | 1 failed, 83 passed / 1；保留缺证 audit 字段断言未满足 |
| 最终相关绿 | 三切片，XML `sources-related-final-v2.xml` | 84 passed / 0 |

三切片是 `tests/test_w2_shadow_pit_sources.py tests/test_w2_shadow_pit_capture.py tests/test_tencent_minute.py`。新增 fixture 时仅修正旧/未来 receipt 自身 start/received 合理范围和 T−1 已收盘日期，不删除业务断言。反例包括单次请求/timeout、UNWIRED、archive 失败隔离、错股/旧日/未来/竞价过期/partial、缺昨收/名称/上市日、非 raw close、CA 假参考/缺 event/错日期、ST/IPO/BJ、catalog 页不全/投影/重复冲突、hash 篡改、原 bytes 可逆及 consumer byte basis、无通知/模型/DB依赖。最终 XML SHA `c5c83b46766a459191b511ddbfcaf1e5ba83e202f79dd9ff78bedf64eafa5419`。

实际最终和 inventory 命令如下；`$LASTEXITCODE` 在输出前保存，避免 PowerShell 将 Python 非零退出口径变为 tools 1。

```powershell
$env:PYTHONPATH='D:/dev_A股/liangjian_wp5_hotfix_20261009/src'
& 'D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe' -B -m pytest tests/test_w2_shadow_pit_sources.py tests/test_w2_shadow_pit_capture.py tests/test_tencent_minute.py -q --tb=short --junitxml=artifacts/w2-sources-20261010/sources-related-final-v2.xml
$taskExit=$LASTEXITCODE
Write-Output "PYTHON_EXIT=$taskExit"
exit $taskExit
# 84 passed / PYTHON_EXIT=0
```

```powershell
$env:PYTHONPATH='D:/dev_A股/liangjian_wp5_hotfix_20261009/src'
& 'D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe' -B artifacts/w2-sources-20261010/inspect_existing_quote_sample.py
$taskExit=$LASTEXITCODE
Write-Output "PYTHON_EXIT=$taskExit"
exit $taskExit
# source_unchanged=true, source_requests=0 / PYTHON_EXIT=0
```

## 四层边界与剩余工作

1. 源码：纯 wrapper 与原合同兼容，未接生产，同次响应只有 injection 才成立。
2. 单元/合同：合成原 bytes 与临时文件通过 84 项，不证明实际供应商 full catalog/CA 字段已提供。
3. 本地真实证据：只证明 10-08 decoded text 的已知 parser 字段；09:26 raw、不同板块 explicit limits、当前上市制度和原 ticker 完整包仍 pending。
4. 运行/业务：无自然交易日捕获、生产变更、成交解锁或账户收益；规则超期/身份缺项继续 DATA_LIMITED，不改影子触发记录合同。

composite 每证券可嵌入完整原 catalog pages，内存/输出量为 O(total original bytes)，16MiB 上限触发 SOURCE_COMPOSITE_TOO_LARGE；不声称 constant memory 或 5 秒预算通过。上市字段真实语义、原公司行动映射、同请求 producer hook、独立 archive/ledger 路径接线及真实 09:26 多板块样本由后续授权集成解决。现有 v1/v2 证据和源文件保持不变。
