# W2 影子专用限价推导包装

## 结论与范围

canonical 0046/0049/0050 已完整读取。0049批准的“当日版本确认后，影子专用推导”已落为新 `runtime/shadow_price_limits.py`，仅 opt-in 纯函数，无共享入口替换。本切片未修改原 `price_limits._derive/resolve_price_limits`、`shadow_rule_preflight`、PIT包装/采集、observer/workflow/settings/HEAD；封存时workflow.py已有其他owner的并行修改，不将整个dirty worktree称为无变更。10-12原 API仍返回 `UNKNOWN/RULE_DATE_UNPROVEN`，不通过改常量、FunctionType、globals副本、猴补或伪装显式价格绕开。

本切片只证明离线合同：新87项＋相关123项共210通过，native exit0。三份实际已封存官方原字节重新只读核对与正文解析通过、前后SHA不变；**真实交易日盘前receipt没有取得**。测试的10-12接收钟是明标合成fixture，不把10-10原响应重新标成10-12。默认缺receipt/source即 DATA_LIMITED；未联网/SSH/生产DB/模型/通知/部署/接调度/写影子账本。

## 公共入口

```python
validate_shadow_rule_receipt(approved, receipt, *, at)
derive_shadow_price_limits(evidence, *, symbol, at, rule_receipt, approved, source=None)
build_shadow_plan_pit_capture(plan, source, *, observed_at, rule_receipt, approved)
```

`approved`为既有 `load_approved_rule_archive()` 获得的原 `ApprovedRuleArchive`。`receipt`为当日既有 `confirm_rule_version/fetch_rule_preflight()` 的完整结果；必须保留三份原字节BASE64和metadata，摘要/状态字符串不替代它。`source`为既有 `PITSourceReceipt`，含实际显式JSON字节和原字段路径，不接受仅normalized quote或缺raw输入。

`at/observed_at`由调用方明确提供aware时间，本包装不采系统时钟、不产生新源请求。规则receipt的known_at和三腿收到时间须同目标日、≤调用时钟、严格早于09:26。只确认10-10已批准版本，计划窗口沿原preflight至10-16；10-16不是未来规则已读证明。规则receipt整个结果hash、原metadatahash、各原HTTP内容SHA、固定URL、HTTP状态/完整性、空参数、字节口径、正式正文和calendar都通过才返回当日确认。

## 不信自签状态或metadata声明

validator先重算整个 canonical receipt SHA；再用三腿BASE64实际原字节和各metadata重新 `record_raw_response()`，对照原 `source_receipt_sha256/raw_sha256`。随后调用原 `confirm_rule_version()`，复用固定批准包原SHA与三份正文解析，最后对完整 canonical结果逐字段对照。

因此即使攻击者更改状态/日期/正式生效日/metadata conflict并重新计算顶层SHA，也不能获得确认。额外字段、旧日、未来/晚known_at、缺腿/重复腿、换raw/endpoint/options/hash、非正式日期、过due或批准原包损坏均fail closed。允许 `source_requests_started=0`（纯确认）或3（完整独立抓取）的原结果，类型需严格int，其余执行声明拒绝。不输出任意异常/原模型文本/raw BASE64，只留hash、必要时钟、正式依据与固定缺口码。

SSE页meta“尚未施行”与07-06正式正文冲突仍保留 `META_NOT_EFFECTIVE_VS_FORMAL_BODY`，不能删除声明后重新签SHA，也不能靠meta单字段否决正文。source_authenticated始终false：实际bytes＋局部checksum不认证网络来源、不可篡改时钟或所有修订缺席。HTTP_RESPONSE_CONTENT_BYTES是response.content，不是TLS/wirebytes。该模块只接受已有来源声明，不新生成“真实HTTP”证明。

## 普通身份/价格合同

新公式显式重复原受审ordinary基础合同：现金A股、原板块一致、ORDINARY、is_st=false、NORMAL、正常上市期、当日原显示前收、正数整tick、正式07-06生效日期，10%主板/20%科创创业板，Decimal ROUND_HALF_UP、最少一tick、最低一tick。复用原 `_normal_listing_day/_decimal/_explicit/_hash` 和 `stock_trading_rules` 非mutation helpers，不改策略或ST/IPO规则。

只有公共validator重验当日成功，私有 `_derive_confirmed_day` 才被公共包装调用；confirmed_day不是用户可提交的证书。该私有helper的历史公式对照是测试fixture，不代表历史10-09批准可重构。区别于原reviewed-through常量，新输出声明 `rule_confirmed_trade_date`，不把已读期限写成10-16。

推导还从原PIT source提取实际字段：原字节SHA必须等于evidence source_input_sha256，source_ref与全部FIELDS逐项匹配；quote/capture/listing接收时钟、同日上市日依据和新鲜度继续验证，ST名称不能与普通声明矛盾。raw T-1 close不是EXCHANGE_DISPLAYED_PRECLOSE；缺显示前收不借昨收填补。partial明确限价与同源完整推导可逐腿核对，一致保留原合同，不一致/alias冲突CONFLICT，错误显式腿不能被推导洗掉。

`build_shadow_plan_pit_capture` 先调用原 `build_plan_pit_capture`，目标日、status、expires/invalidated、来源、09:26–09:30窗口等原门不变。只有旧结果存在 `RULE_DATE_UNPROVEN`、且新包装真的KNOWN，才移除这个单项缺口；其他missing身份/来源/计划gap保留，不能借此释放资格。已有完整显式腿无需规则receipt，返回原结果逐字段相同。不把计算出的upper/lower写回原source/原evidence冒充源显式腿，也不seal、不激活计划。

新包装中的 `COMPLETE` 只表示此纯PIT入口合同完整。原ledger、standalone session、原resolve consumer均未接新API；本切片不宣称它们已可用或已解锁，也不绕其原日期限制。后续实际消费者接线/当日独立规则抓取、PIT原包及封存均为另一步。同步PDF/HTML重验可能每次调用重复解析；本轮没有证明生产每分钟5秒或整段端到端SLA，也没有引入缓存复用旧TTL。

## 先红后绿与真实证据

目录 `artifacts/w2-shadow-derive-20261010`：

| 回执 | 实际结果 | native exit |
| --- | --- | --- |
| before.xml | 新模块缺失，collection error1 | 2 |
| after-v1.xml | 65 passed /1 failed | 1 |
| related-v2.xml | **210 passed /0 failed /0 error /0 skipped**，21.880s | **0** |
| original-archive-readonly.json | 原四文件SHA前后相同、正式正文解析成功，当日receipt NOT_OBTAINED | **0** |

首实现失败是测试错误地将一致partial explicit leg当缺口；真实原resolve会以同源完整derive核对一致的partial腿。已将反例修为partial12（与derive11冲突），另加partial11保持原行为正例，没有删掉冲突防御，也没有放松原算法。20项四板块×五价位严格比upper/lower/rate/tick/rounding等字段零差；19项ST/IPO/日期/缺价/错basis等拒绝reason与原函数逐项相同。10-12原API仍关闭反例、新wrapper成功、原输入不变、显式路线原样、计划expiry/invalidated/错target/status、原SHA/normalized缺raw、伪造receipt/metadata/conflict等均覆盖。

实际测试命令（统一从WP5目录执行）：

```powershell
$env:PYTHONPATH='D:/dev_A股/liangjian_wp5_hotfix_20261009/src'
& 'D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe' -B -m pytest tests/test_w2_shadow_price_limits.py tests/test_w2_shadow_rule_preflight.py tests/test_w2_shadow_pit_sources.py tests/test_w2_shadow_pit_capture.py -q --tb=short --junitxml=artifacts/w2-shadow-derive-20261010/related-v2.xml
$w2DerivedExit=$LASTEXITCODE
Write-Output "PYTHON_EXIT=$w2DerivedExit"
exit $w2DerivedExit
```

前两轮仅新test文件，XML分别before.xml/after-v1.xml。实际只读原包命令是相同解释器与PYTHONPATH下 `-B artifacts/w2-shadow-derive-20261010/inspect_original_archive.py artifacts/w2-shadow-derive-20261010/original-archive-readonly.json`，native0。该脚本的原批准包作为daily receipt拒绝检查使用明确比较参数14:05，不是声称实际读取时刻或新盘前接收钟；checksum本身即不符合daily schema。原三个 observed_at逐项保留、未回填未来日期。

## 最终源pin与四层边界

WP5 HEAD保持 `aa68a7848a673238124b481ac7a646cd211b9155`；没有commit/push/deploy。共享dirty docs/state及其他人的未跟踪源码不覆盖。

最终封存用 `final-slice-v2.json`；旧final-slice.json保留对应文档措辞澄清前SHA，不重写旧回执。v2只澄清本slice未改共享源与其他owner的dirty路径，不改已测试源码/tests；不得将这次文档改动当新的source测试。

| 文件 | SHA256 |
| --- | --- |
| 新runtime/shadow_price_limits.py | `88b58a9cf7e9a47d4bbac68af6cec23ef0de30178a0346892c87ba794b7be8b8` |
| 新tests/test_w2_shadow_price_limits.py | `d8c8fe3accb1c1a5fa53a68d429428586b198b24fc8c1ae3b5aaf89ed723a122` |
| related-v2.xml | `2869320dc11a952c280bbd4641add013994ac03094ff8a0b6aa008e9da985843` |
| original-archive-readonly.json | `fe37b52fbe16a921df977ff3e2353c6ca40947c2ea517272d701c730fe1b480b` |
| 原price_limits.py（前后不变） | `ec1c395b452e8d6fa1e5b6016f05f88a2ccdb982296f1b8940b73ed0d8d4549c` |
| 原shadow_rule_preflight.py（前后不变） | `efbb4d10067e4bc3e976c3e0f49a5530aa45e97b97b00c0f742e494ae4cd77fc` |
| 原shadow_pit_sources.py（前后不变） | `9002b6c46bd7088ef863cab4618ec3d1e085ceed8793b46793126159fe74e5f0` |
| 原shadow_pit_capture.py（前后不变） | `2cdd9253f913d9fbebea9efacf973f39c1d3023eef4f0b053e3025cf333a3bcc` |

CODE：新纯包装及旧API隔离/零差对照已测。REPLAY：只合成当日arrival fixture；原官方包已实际读验，但真实每日receipt未取得。OPS：默认UNWIRED、未部署/接线，无真实自然09:26成功证明。STRATEGY：不改生产/研究规则、资格、成交或收益，未进行策略自然验收。不能将本切片210项局部通过替代root未来新HEAD全量或0050正式复审。
