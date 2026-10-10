# W2 每日规则与 PIT 原件绑定 bridge（2026-10-10）

本切片是纯本地准备/核验合同，不是 W2 端到端完成。无网络、SQLite、RuntimeStore、Settings、模型、通知、交易、调度或默认 provider。原 `price_limits.py`、规则 preflight、影子 derive wrapper、PIT capture、ledger 和 session 均不修改。派生成交消费保持 `UNWIRED`，不可把 DERIVED 上下限改称原源 EXPLICIT 字段。

## 公共接口与原件边界

新增 `runtime/shadow_pit_rule_bridge.py`：

- `load_daily_rule_package(*, receipt_path, receipt_file_sha256, approved_archive_root, target_trade_date, known_at)`：显式本地文件读取，不自动找目录或联网。文件必须是非 symlink 的存在普通文件，最多 16 MiB，严格小写 64 位原字节 SHA。加载固定已审三公告和 archive receipt；同日 aware `known_at` 必须在 09:26 前，receipt 原 completion clock 不得晚于该 knowledge clock。严格 JSON、真实三份原字节、元数据、原 SHA、正式生效条文和固定 archive 均由既有 validator 重验，状态字符串不构成证明。
- `prepare_rule_bound_pit(plan, source, *, observed_at, rule_package)`：只收原 execution-plan row 和 `PITSourceReceipt`。`payload_json` 必须是实际原字符串；不存在或仅有 parsed dict 不得重编码替代。计划内外 symbol/target 一致，内部若声明 plan/lane/version 必须与 row 一致，外部 lane/version 必须存在。调用已冻结 `build_shadow_plan_pit_capture`，只在原计划窗口、身份、上市、非 ST、实际 raw bytes、昨收和规则全通过时准备 material。缺项不填。
- `validate_rule_bound_material(material, *, plan, source, rule_package, at)`：核验 material 原 SHA 与 schema，同日 current clock 不早于 capture，原计划 expiry/已知 invalidation 不能已过。按原 `captured_at` 重建而非按当前时钟重抓 PIT；所有 canonical material bytes 必须一致。当前 `ACTIVE_TODAY` row 不能拿来替换早前 `PENDING` 原 row 通过 full-row hash；后续 stable source-binding/provider 仍需独立接线。

`FrozenDailyRulePackage` 保留 exact receipt bytes、file SHA、resolved 文件引用、固定 archive 原字节、target、原 knowledge clock。`FrozenRuleBoundPITMaterial` 只保留 canonical material bytes 与 SHA；不存在可修改的 material dict 内部状态。调用方有需要可以读 JSON，但仅有自行声明的对象/状态不能通过原输入重建校验。

## 哈希与时间的不同语义

规则文件 file SHA 覆盖实际文件所有字节，包括缩进。receipt 的 canonical SHA 是既有内部业务校验和，不能冒充文件原 SHA 或 HTTP body SHA。规则包保留三份 HTTP-response 原字节及元数据；来源鉴权和原钟真实性不是 SHA 能证明的，始终 `source_authenticated=false`。

`plan_binding.payload_json_bytes_sha256` 只 hash `original_payload_json.encode('utf-8')`，不 parse/reencode 后冒充原字节。完整 row canonical SHA 另有明确名字。计划模型文本仅影响这些哈希，不复制到 material；material 仅白名单 identity、几何、时间、source-lineage、rule-binding 与 authority。

`captured_at` 是调用方实际完成 PIT 准备的 observation clock；source 自身原接收钟另保留在 `source_lineage.captured_at`。09:32 的 material 校验保留 09:26 的原钟，不把历史 quote 当实时新触发。当前模块没有真实 clock sampler，不认证调用方时钟。

material 分开声明：

- `SHADOW_DAILY_RULE_DERIVED` + `DERIVED_FROM_PRECLOSE_AND_BOARD_RULE`；
- `EXPLICIT_SOURCE_LIMITS` + `EXPLICIT_FROZEN_SAME_DAY`。

两种均是 `PREPARED`，`ledger_seal_status=NOT_WRITTEN`，`provider_integration=UNWIRED`，`derived_outcome_consumption=UNWIRED`。每日规则包本步无论路线均要求完整，原 standalone explicit route 的行为不改。没有原件不能用本地人工 certificate 填补。

## 反例与实际回执

`artifacts/w2-pit-rule-bridge-20261010/before.xml`：新模块未实现，真实 collection ImportError，native exit 2。

`first.xml`：初版全部 46 项通过，native exit 0。补查发现 lane/version 内外冲突、JSON 数字溢出与过期 material 尚未守卫；`binding-before.xml` 四反例真实失败，native exit 1。修复后联合相关 slice 记录在 `related-final.xml`，最终命令/数量/源 SHA 在 `final-slice.json`，不用先前回执冒称当前源码。

测试覆盖缺文件/错 SHA/旧日/未来日/09:26 后加载/naive clock、缺固定 archive/三原件/伪造状态/schema/重复 JSON/非有限数，原字符串缺失/对象替代/plan identity/窗口失效，以及 ST/IPO/缺上市/缺昨收/错口径/错股/旧日/未来源、缺 source/rule、material SHA/body 篡改、原字符串 whitespace 修改、raw source 更换、捕获钟重标与原输入不变。无真实 10/12 receipt，10/12 完整链仅为明确本地 fixture，用固定已审 10/10公告内容加 synthetic arrival clocks；不宣称这些字节 10/12 自然取得。

复现入口（WP5 checkout 根目录）：

```powershell
$env:PYTHONPATH='src'
& 'D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe' -B -m pytest tests/test_w2_shadow_pit_rule_bridge.py tests/test_w2_shadow_price_limits.py tests/test_w2_shadow_rule_preflight.py tests/test_w2_shadow_pit_sources.py tests/test_w2_shadow_pit_capture.py -q --junitxml=artifacts/w2-pit-rule-bridge-20261010/related-final.xml
```

## 四层状态与未完成项

CODE：新 bridge 的 fixture 合同与相关回归可验证；REPLAY：仅合成本地 fixture，不是当日生产回放；OPS：真实 10/12 日规则原件、09:26 raw catalogue、PIT sealing、独立 provider 接线均未完成；STRATEGY：没有影子成交/收益/策略成功结论。

下一步由独立 owner 审核既有 ledger 的 rule-bound seal 接口以及 preserved capture-clock provider，不新增第二套 SQLite。不允许直接把此 material 转成既有 outcome 的显式上下限，否则会丢失 DERIVED authority。原 outcome 消费、后续 stop/entry 日规则和自然运行证据均未由本切片解决。
