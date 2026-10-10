# W2 09:26 独立 PIT 采集适配器（本地合同，未接真实来源）

本切片完成显式输入的采集、字段血缘、原字节 SHA 绑定与独立账本封存；**真实 fetcher 和调度均未接线，默认 SOURCE_UNWIRED**，不能宣布周一 PIT 已上线。未读取生产数据库、调用网络/模型/通知，也未修改 workflow、execution_plans、既有 provider、规则截止日或冻结账本。

已读周末任务书及 canonical outbox 0034/0035/0036/0037。以下仅为获授权 W2 的本地实现，不是决策 14 发布批准。

## 真实源码审计与尚缺数据

| 实际入口 | 目前能证明 | 不能提供/不能替代 |
|---|---|---|
| `runtime/auction_refresh.py:19 collect_fresh_quotes` → `data/rotation_theme.py:1180 _parse_tencent_reference_quote` / `:1204 _default_tencent_quote_batch_fetch` | provider_time 同日、09:25 之后、age≤180s；symbol/latest_price/previous_close/quote_time/source_id，95%范围门 | reference 投影没有证券名称、显式上下限、上市日、原 HTTP bytes；不自行增加供应商字段或改变限流 |
| `workflow.py:1776 prepare_snapshot` 与 `runtime/auction_base.py:118 project_auction_delta` | `auction_refresh_quotes` 规范化对象被冻结；AUCTION_SNAPSHOT/base_hash/evidence_as_of | 基础/公司/宏观证据明确复用原日期，不能把旧 base 的名称、上市日或普通身份改标今日 |
| `data/tencent_minute.py:38 MarketQuote`、`:270 TencentIntradayAdapter._quote` | name、symbol、quote_time、price、open、previous_close、volume、amount；实际解析索引 1/2/3/4/5/6/30/37 | 没有已验证的上下限/上市日解析；默认 text fetcher 返回 decoded text，不是 response.content；重新 encode 字符串不是原 HTTP 字节 |
| `workflow.py:5653 review_pending_morning` | 现有 quote model_dump 用于生产止损/追高检查 | 本切片不复用激活/通知动作；morning-review 回执只有 evidence_symbols 等，不能当原 quote body |
| `runtime/auction_publication_export.py:237 ReadOnlyPublicationReader.export` | pending 发布计划、目标日期、payload hash 和来源引用 | 非证券行情/PIT 身份；最终状态非历史 pending 账本。本切片没有实例化该 reader |
| `pipeline/local_fact_cache.py:62 _sanitize` | fetched_at、规范化内容 hash | 丢弃 raw_response/headers/credentials，不是原 HTTP bytes；未实例化缓存或改其 DB |
| `pipeline/data_source.py:197 HithinkClient.ticker_catalog` | 既有 `/api/meta/tickers/list` 的规范化目录读取路径 | 本轮未读真实响应，尚无上市日字段、日期合同/来源 mapping 的实证，不能拿当前目录补历史身份 |

因此已冻结竞价 normalized quote 可作为降级审计输入，却不足以认证交易所显示前收与原响应 SHA。是否原 Tencent 返回串另有 explicit limits、是否当前基础数据确有上市日字段，须后续**只读源码/现有封存证据**核实；本切片没有猜字段索引、调用新端点或请求当前行情。

## 公共接口与时钟

`runtime/shadow_pit_capture.py` 导出：

```python
PITSourceReceipt(source_ref, captured_at, complete,
    format='EXPLICIT_JSON_FIELDS', raw_response=None, normalized=None,
    byte_kind='CONSUMER_INPUT_BYTES', field_map=None)
build_plan_pit_capture(plan_row, receipt, *, observed_at) -> dict
capture_shadow_pit(plan_rows, *, observed_at, source_receipts=None,
    source_fetcher=None, ledger=None, clock=None) -> dict
serializable_capture_report(result) -> dict
```

plan_row 是原行合同：plan_id/symbol/status/valid_from/expires_at/payload_json（或 payload）；payload 必须显式 target_trade_date。仅当日 PENDING_MORNING_REVIEW/ACTIVE_TODAY，分别计数，不推断 target、不激活。ACTIVE 的 09:32 future start 可于 09:26 观察，但 entry_effective_now=False。过期、其他日、失效、NULL active start 排除；重复 plan_id 在任何 fetch 前拒绝。

每个入口均守住命名交易日、09:26≤knowledge time<09:30。来源 quote observed_at 必须同日，09:25 之后，age≤180s；quote observed_at≤source captured_at≤实际处理 knowledge time。`source_fetcher(symbol, observed_at=initial_now)` 每唯一证券至多调用一次，不新增重试或限流；实时调用者必须注入 `clock()` 取得 fetch 收齐时刻。遗漏 clock 只承认显式 observed_at，不会接受之后到达的未来 receipt。CLI 是本地 pinned 输入处理，明确 `observed_at_basis=CALLER_SUPPLIED_NOT_FILE_MTIME`、`historical_pit_authenticated=False`，不是实际历史到达认证。

未接 fetcher、没有该股输入或不完整来源，结果保持 SOURCE_UNWIRED/SOURCE_PARTIAL、null limits。异常只输出稳定代码，不返回源异常或密钥。写失败仅降级 SHADOW_EVIDENCE_WRITE_FAILED；没有 RuntimeStore/生产执行/客户通知依赖。

## 字段与 bytes 合同

EXPLICIT_JSON_FIELDS 从调用者提供的**实际 bytes** 解析字段白名单；可显式 JSON pointer 映射，重复 key/NaN/非法对象拒绝。原 bytes SHA 原样作为 source_input_sha256，与 ledger.seal_price_limit_evidence 的 raw_response 校验绑定。

`byte_kind=ORIGINAL_HTTP_RESPONSE_BYTES` 是调用方关于 bytes 来源的声明；`CONSUMER_INPUT_BYTES` 只证明消费者实际提供的字节。二者均 `source_authenticated=False`，hash 相等不是交易所来源认证。CLI 不存整段原 HTTP body：原件由调用方 input 文件保留，输出保存源文件 SHA/大小/引用；不声称原 body 被新增档案完整归档。

NORMALIZED_QUOTE 只保留 normalized_object_sha256，raw_response_sha256=null，raw_http_bytes_available=False；previous_close 的 basis 固定 NORMALIZED_PROVIDER_PREVIOUS_CLOSE，不能标 EXCHANGE_DISPLAYED_PRECLOSE 或解锁限价。证券名称即使有 source name，只证明当日该字段值，不认证普通/非 ST/非新股制度；含 ST 只做正向 is_st=True，无 ST 不推出 False。代码前缀只由现有规则做一致性检查，不做身份认证。

上市日要求严格 ISO date、listing_date_basis=SOURCE_LISTING_DATE，并有同日 listing_observed_at≤captured_at；日期必须≤当日。旧缓存观察时间、缺来源日期、时间戳冒充日期均 null。缺 preclose/普通身份/制度不补0、不猜。`preclose_basis` 必须实际显式显示口径；prior_raw_close 不替代 preclose，除权差异由既有 ledger 的 EX_RIGHTS_REFERENCE_OBSERVED 独立注释，不能由差异认证除权事件。

显式当日上下限优先用冻结合同；字段不足时身份仍 DATA_LIMITED。原显式与合法推导冲突保持 CONFLICT。**RULE_REVIEWED_THROUGH=2026-10-10 未修改；10-12 推导保持 RULE_DATE_UNPROVEN**，不能承诺解锁全部成交。即时 signal/outcome 仍由既有账本等待下一根完整 1m 到达，本切片不提前制造 bar/fill，也不重新计算历史收益。

## g3 consumer 接线合同（未实施）

09:26 standalone 用原 plan_rows +真实 receipt +独立 ledger 调用 batch；entry 含 evidence/raw_response/observed_at/source_lineage/limits/status/sealed_receipt。g3 的 `identity_provider(plan, observed_at)` 只能投影**已有当天封存**：原 captured clock、原 evidence SHA、实际字节/原件引用，不得在 09:30 后再次调用采集接口或把证据时间重标 now。

当前 `runtime/shadow_session.py:224` 的 identity_provider 默认 None；`:285` 盘中输入是 durable plan，并非完整发布 row，且期待 status=READY 的 provider 投影。这两处没有改动。只有真实完整采集和 bytes 能支持 READY；normalized/缺腿/未封存须 DATA_LIMITED。仍需真实 source fetcher、上市日映射、独立09:26启动入口/生命周期，以及盘中只读 seal provider；默认真实链路仍 PIT_PROVIDER_UNWIRED。

## 离线 CLI 与不可变输出

新增 `scripts/capture_shadow_pit.py`，仅读取本机 pinned manifest：

```json
{"schema_version":"shadow-pit-local-input/1",
 "plans":{"path":"plans.json","sha256":"<64hex>"},
 "quotes":{"600001.SH":{"path":"quote.json","sha256":"<64hex>",
   "source_ref":"frozen:quote-response","captured_at":"2026-10-12T09:26:00+08:00",
   "complete":true,"format":"EXPLICIT_JSON_FIELDS","byte_kind":"CONSUMER_INPUT_BYTES"}}}
```

plans.json 是上述 row 数组。quotes={} 允许诚实 UNWIRED。文件必须位于 manifest 所在目录及子目录，拒绝 absolute/.. /解析后逃逸；manifest 与每输入文件 SHA 必须预先匹配。校验前不创建输出；已有输出目录拒绝，不能覆盖/复用生产 state。新输出仅 shadow.sqlite3、price-limit-evidence.jsonl（有事件时）、capture-report.json、manifest.json。SQLite/JSONL 一致性、冲突幂等复用冻结 W2 账本，mirror PENDING 不当成功。

最小启动命令（需主代理提供真实已封存输入；本轮没有执行真实源）：

```powershell
$env:PYTHONPATH='D:/dev_A股/liangjian_wp5_hotfix_20261009/src'
& 'D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe' -B scripts/capture_shadow_pit.py --input <absolute-input.json> --input-sha256 <64hex> --observed-at '2026-10-12T09:26:00+08:00' --output-dir <new-independent-directory>
$taskExit=$LASTEXITCODE; Write-Output "PYTHON_EXIT=$taskExit"; exit $taskExit
```

COMPLETE=0，覆盖/来源/写入不足=2；2 是正式不足结果，不改称成功。此命令不默认网络采集，历史本地输入不认证历史 PIT。

## 反例、实际命令与四层证据

统一 cwd=`D:/dev_A股/liangjian_wp5_hotfix_20261009`；Python/PYTHONPATH 如上。每轮运行实际命令为 `& <python> -B -m pytest <tests> -q --tb=short --junitxml=<xml>`，末尾均保存并打印 `$LASTEXITCODE` 后 `exit $taskExit`，下表是 Python 原生退出码。

| tests / XML（artifacts/wp5-20261010/） | 结果 | native exit |
|---|---:|---:|
| test_w2_shadow_pit_capture.py / w2-pit-before.xml | 24 failed（接口未实现） | 1 |
| 同上 / w2-pit-after-v1.xml | 24 passed | 0 |
| 同上 / w2-pit-guard-before.xml | 2 failed、26 passed（单入口截止/日期类型） | 1 |
| 同上 / w2-pit-cli-before.xml | 2 failed、31 passed（CLI 未实现；Windows stderr 解码警告亦记录，随后修 fixture errors=replace） | 1 |
| 同上 / w2-pit-after-v2.xml | 33 passed | 0 |
| test_w2_shadow_pit_capture.py + test_w2_shadow_evidence.py + test_auction_refresh.py + test_auction_base.py + test_wp5_auction_publication_export.py / w2-pit-related-final.xml | 195 passed | 0 |

反例覆盖错股、旧日、未来、过时竞价、采集时窗、partial、缺昨收/上市日、旧 listing、ST 正向、规则截至/显式冲突、normalized 与原bytes区别、重复/冲突 seal、writer失败隔离、同股一次fetch、clock收齐时点、输入原件不变、新目录/文件 SHA/路径拒绝。不重复全量，不改旧 fixture；CLI 测试都为合成本地 bytes 和临时目录，不是真实 source 实测。

CODE：新增合同与 CLI；REPLAY：上述 195 本地测试。OPERATIONS：未安装/调度/真实09:26采集、无真实fetcher与真实上市日源证据。STRATEGY：没有阈值或执行策略改变，PIT 不足保持 null；不把研究模拟成交写成账户收益，不声称自然交易日验收或5秒预算证明。

冻结源码 SHA256：shadow_pit_capture.py `2cdd9253f913d9fbebea9efacf973f39c1d3023eef4f0b053e3025cf333a3bcc`；capture_shadow_pit.py `7ecba1bd07bbe9b2076ea83c22c945aefb150439459de0eef61216fcd1ada320`；test_w2_shadow_pit_capture.py `9cc86078f525028cfbc22960c79772ff0d898c263e7d91d2d38d68b5fc5e12df`。未修改依赖：shadow_evidence.py `c02c69beb58d798931a90c31d3d5a0204a281b824bc47f29ede97883bce9aaa0`；price_limits.py `ec1c395b452e8d6fa1e5b6016f05f88a2ccdb982296f1b8940b73ed0d8d4549c`；outcomes.py `f87dad9b6e9b9292e6bc22796d56d67de073f12a3da5345ef0286f322b1c18aa`。
