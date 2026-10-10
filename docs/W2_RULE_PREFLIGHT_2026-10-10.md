# W2 独立盘前规则版本核验（2026-10-10）

## 结论

0046 工程合同已落为新的纯组件：`rule_review_observed_at=2026-10-10` 与 `planned_revalidation_due=2026-10-16` 分离。只有三份固定官方原响应内容逐份同 SHA、可解析、同交易日且 09:26 前到达，才输出 `RULE_VERSION_CONFIRMED`；缺腿或新版本不自动接受，保持 DATA_LIMITED。计划复核期限不是“已读到 10-16”，也不是未来规则不会变化的证明。

生产接线仍 **UNWIRED**。现有 `price_limits._derive()` 在 `price_limits.py:119` 硬约束 `day <= RULE_REVIEWED_THROUGH=2026-10-10`，没有消费新 preflight receipt 的 API；因此即使组件对合成 10-12 当日响应给出版本确认，**现有 DERIVED 仍 UNKNOWN/RULE_DATE_UNPROVEN**。本轮没有改常量、伪造显式 limits 或把确认状态注入原身份，后续接线必须另授权严格 API。显式同日限价腿仍走原合同，不受这里替代。

仅新增 `runtime/shadow_rule_preflight.py`、专用测试、本文件和独立 artifacts。不修改 price_limits、shadow_pit_sources、既有 production CLI/scheduler/provider、已审文件、共享 state；无新网络、生产、模型、通知、DB、commit/push/deploy。工作树基线为 `aa68a7848a673238124b481ac7a646cd211b9155`。canonical 完整裁定：`.claude_bridge/outbox/0046-reply.md`。

## 输入与验收接口

```python
approved = load_approved_rule_archive(original_archive_directory)
result = confirm_rule_version(
    approved, observations, trade_date='2026-10-12', known_at=actual_processing_clock,
)
# 真实源尚未接；没有 fetch_response 时明确 HTTP_UNWIRED，不构造 client。
result = fetch_rule_preflight(
    approved, trade_date='2026-10-12', fetch_response=original_http_callback,
    budget_seconds=10,
)
```

`load_approved_rule_archive()` 只读四个固定文件，不接受 caller 自行换 allowlist。receipt 原 bytes 必须 SHA `bbcdb3dd7ddf487593d88b89c10a49176907aa837b1c924d6161c0d7b8a8a813`，三份 raw、URL、最终 URL、byte size、原日期和状态逐项验证；dataclass 被 replace 也会重新校验，不能以 typed object 绕过。路径解析不得逃出输入目录。损坏批准包抛稳定 `RulePreflightError(APPROVED_ARCHIVE_UNPROVEN)`；运行确认/抓取函数则返回稳定 DATA_LIMITED，不把来源异常文字交 owner。

`observations` 必须是三个实际 `RawResponseReceipt`，复用已有 `record_raw_response` 结构。再次重算 raw SHA 与所有元数据 receipt SHA，不信任 status/hash 字符串。必须固定三个 endpoint、空 GET 参数、原 `HTTP_RESPONSE_CONTENT_BYTES`、HTTP200/complete、aware 请求开始/响应收到时间、同当日、start ≤ received ≤ known，09:26 前，显式 ExchangeTradingCalendar 确认为交易日。限定本轮复核计划 10-10 至 10-16，不自动沿用超过 due 的批准。

原响应内容 bytes 是 `response.content` 口径，不是压缩 wire/TLS bytes，不认证 HTTP 签名；caller 提供时钟明确 `CALLER_SUPPLIED_NOT_AUTHENTICATED`，注入 clock 为 `INJECTED_CLOCK_NOT_AUTHENTICATED`，默认真实 clock 为 `PROCESS_WALL_CLOCK`。这些字段说明证据来源，不自动证明客户端不可篡改。

结果保存 raw BASE64、原 SHA、已批准 SHA、源 receipt SHA、到达/开始时钟、提取正文文本 SHA、正式生效日期和 metadata conflicts。收到的新版本或不可解析原字节在 hash 绑定通过后可保留 audit row，但 `validation_status=DATA_LIMITED`、整体 confirmed_trade_date=null；不自动批准。未完成三腿时，局部确认 row 也不让整体通过。

`receipt_sha256` 是去掉顶层 checksum 后的 canonical 结果 SHA，标 `CANONICAL_RESULT_NOT_HTTP_RAW`；`canonical_receipt_bytes()` 只是纯序列化 helper，不是任意外部结果验真器。本组件**不写 receipt/归档或修改规则**，不能把别人填写的 RULE_VERSION_CONFIRMED 当可写凭证。后续 consumer 必须从已 hash-bound 原包重复验证，不得只检查状态字符串。

## 三份固定原件与正文解析

批准目录 `artifacts/wp5-20261010/official-rules-20261010`：

| 文件 | 官方 URL | 原内容 SHA256 |
| --- | --- | --- |
| sse-notice.html | `https://www.sse.com.cn/lawandrules/sselawsrules2025/fund/trading/c/c_20260424_10817739.shtml` | `12886b912a2160c03c414d2b56ff67c772b18e3247f2c97fc2dfcd185b6e0a33` |
| szse-rules.pdf | `https://docs.static.szse.cn/www/lawrules/rule/trade/current/W020260424690713155663.pdf` | `9b66f8b0db70f84a25ef1ccb4ee2351001724e408117552d75f6d8993483c586` |
| szse-notice.html | `https://investor.szse.cn/lawrules/rule/trade/t20260424_620190.html` | `0dcb0971ac96123f369178ce8f5397d948190d41281175694664de1d1dd4dce5` |

HTML 严格 UTF8，去 script/style 后解析实际文本；PDF 用项目已有 pypdf 解析固定 raw，要求规则标题、2026 修订和正文单一“自 2026 年 7 月 6 日起施行”口径。SSE 原 HTML:325 meta“尚未施行”与 :332 正式通知冲突，记录 `META_NOT_EFFECTIVE_VS_FORMAL_BODY`，以正式通知正文为依据；SZSE 通知 :232 及 PDF 10.9 同样指明 07-06。不把 meta 单字段当有效性，不重写原页。

这是“同三份已读版本”的复核，不是全体交易所/所有修订公告完整扫描。SSE 附件暂缓条文等不在这三份抓取内；`latest_revision_absence_proven=false`、`future_no_change_proven=false` 一直保留。新 SHA 即使仅网页小改动，也不自动接受。

## 预算与 owner 隔离

`fetch_rule_preflight()` 的注入 HTTP 签名是 `(url, remaining_timeout_seconds) -> response`，要求 `.content/.status_code/.url`。默认没有 requests/client。最多三次 GET，无 wrapper 重试或新限流，不接生产，不放 09:26 关键路径。

独立预算默认 10 秒，允许有限正数 ≤30 秒，bool/NaN/inf/负数拒绝。worker 每次请求前重新检查 stop/deadline，包括注入 wall() 返回后；过期时钟不得再启动请求。caller 只收 deadline 前结果，晚响应丢弃，超时后不得启动后续请求/写产物/发信号。Thread.start 故障返回 RULE_PREFLIGHT_WORKER_START_FAILED，不污染 owner。

不配合 timeout 的 HTTP 调用可能在 daemon 内继续等待，Python 不能强杀它；必须由授权传输遵守 remaining timeout。固定 pin PDF/HTML 解析和本地 calendar/clock 是同步 CPU/初始化工作，解析后检查 deadline 是接受门，**不是解析的强中断**；不声称绝对端到端 10 秒、生产 5 秒 SLA 或 constant-memory。超时记录源请求已开始数量，不能将未收到结果当成功。

## 先红后绿及真实本地核对

反例涵盖缺页、重复源、raw/hash 变、错 endpoint/参数、旧日/未来/naive 时钟、恰在或晚于09:26、非交易日、超过 planned_due、partial/HTTP失败、consumer bytes 冒充 HTTP、不可解析、批准包损坏/路径越界、dataclass 篡改、默认 UNWIRED、异常文字屏蔽、迟到不发/不再请求、worker start 失败、clock 阻塞到期不请求、现有限价 API 仍拒绝新日期。

| XML（`artifacts/w2-rule-preflight-20261010/`） | 实际结果 | Python native exit |
| --- | --- | --- |
| before.xml | 4 failed +33 setup errors（未实现） | 1 |
| after-v1.xml | 37 passed | 0 |
| before-audit-guards.xml | 3 failed +37 passed（新 raw 审计与处理时钟反例） | 1 |
| related-final.xml | 121 passed | 0 |
| before-worker-guards.xml | 2 failed +40 passed（真实线程/时钟到期反例） | 1 |
| related-final-v2.xml | **123 passed：42新 +48sources +33capture** | **0** |

工作目录 `D:/dev_A股/liangjian_wp5_hotfix_20261009`。实际命令统一前缀：

```powershell
$env:PYTHONPATH='D:/dev_A股/liangjian_wp5_hotfix_20261009/src'
& 'D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe' -B -m pytest tests/test_w2_shadow_rule_preflight.py tests/test_w2_shadow_pit_sources.py tests/test_w2_shadow_pit_capture.py -q --tb=short --junitxml=artifacts/w2-rule-preflight-20261010/related-final-v2.xml
$taskExit=$LASTEXITCODE
Write-Output "PYTHON_EXIT=$taskExit"
exit $taskExit
```

三个红轮/首绿仅跑 `tests/test_w2_shadow_rule_preflight.py -q --tb=short` 并指定表中 XML；联合121轮用上面三切片但 XML 为 related-final.xml。保存 `$LASTEXITCODE` 后输出/退出，没有把 tools 的非零映射当 Python 原码。不重复全量。

实际原包只读核对命令也用该解释器及 PYTHONPATH：`-B artifacts/w2-rule-preflight-20261010/inspect_approved_rule_archive.py`，native exit **0**，新结果 `approved-archive-readonly-inventory-v2.json` SHA `2f52aa2c32be507f7e7a4e7f98785e0666f80ab071c9fa05352ba81e3b7102f9`。三份正式正文均实际解析成功，原四文件前后 SHA 不变；source_requests=0。**旧 10-10 原捕获时钟未重标到 10-12**，10-10 周六按实际日历只得 DATA_LIMITED/RULE_NON_TRADING_DAY，不能拿批准包冒充未来盘前确认。早先 inventory-v1 对应补 worker 守卫前 source SHA，原样保留，不用于最终源码绑定。

## 冻结清单和四层证据

| 文件 | 最终 SHA256 |
| --- | --- |
| src/liangjian_funnel/runtime/shadow_rule_preflight.py | `efbb4d10067e4bc3e976c3e0f49a5530aa45e97b97b00c0f742e494ae4cd77fc` |
| tests/test_w2_shadow_rule_preflight.py | `55c9a632c465133382112c847ae749c700542bff8c4276a101402bfedc7dca85` |
| artifacts/w2-rule-preflight-20261010/related-final-v2.xml | `b03244a46be6978b19f556dbca9457028d017229032dd826342f79058cb01160` |
| artifacts/w2-rule-preflight-20261010/inspect_approved_rule_archive.py | `14cc4cfe6c78ca31e6d9fadc0521eb0417855d6718c13156b352351378a7125a` |

既有 price_limits SHA 保持 `ec1c395b452e8d6fa1e5b6016f05f88a2ccdb982296f1b8940b73ed0d8d4549c`，shadow_pit_sources SHA 保持 `9002b6c46bd7088ef863cab4618ec3d1e085ceed8793b46793126159fe74e5f0`。

源码合同与 123 项组件/相关测试已通过；本地真实原件只证实批准版本可解析、hash 不变；当日独立实际 HTTP 任务尚未接线；自然交易日规则确认、严格 DERIVED consumer 和实际成交解锁均未实施/未证明。已有规则、原限价、生产数值和正式输出不变。
