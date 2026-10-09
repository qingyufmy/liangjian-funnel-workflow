# WP4 板块引用纯合同审计（2026-10-10）

本切片实现 `board_reference/1.0` 的纯版本审计，未接生产，也未完成 WP4 的真实采集、回放或运行验收。`research_available` 仅指成员合同在本地冻结证据内可评估；`actual_execution_authorized`、`new_entry_authorized`、`source_authenticated` 始终为 `false`。对象哈希与响应字节一致性不构成来源认证、供应商授权或成交执行资格。

## 输入与输出

入口 `data/board_reference_contract.py:audit_board_reference(binding, catalogs, memberships, *, as_of, response_bodies=None, history_contract=None)` 为纯函数，不读写文件、不采集、不选最新对象、不维护状态、不运行行情或模型。输入不被修改。

复用 `board_reference.py` 的 `digest/_valid/_symbol/aware`；不复制 collector。`catalogs/memberships` 直接使用既有 `hithink_board_reference.collect_catalog/collect_members` 的真实对象形状。

`binding` 沿用显式 `theme_id/source_id/board_id/board_name/category/approved`；新增两个必填字段：

- `source_identity`：调用者显式指定的 `THS_[A-Z0-9_]+` 合同身份；既不是东财 BK，也不是供应商颁发的代码证书。输出另列 `strategy_direction_id=theme_id`，不以策略方向冒充原生指数身份。原生代码保留为组件 `board_id`，严格六位 `.TI`。
- `reference_versions`：每个显式组件必须且只能有一项 `{board_id, catalog_hash, membership_hash}`。只能选精确哈希对象；无自动回退、名称近似映射或新旧混选。

单方向使用自身三字段作为一个显式组件；复合方向沿用 `membership_operation=EXPLICIT_COMPONENT_UNION`、`board_id=STRATEGY_COMPOSITE:<theme_id>` 和组件清单。组件须唯一；任一缺失、身份/版本/日期/分页失败，整个方向不可用。组件记录同一股票的不同名称亦阻断，不合并冲突数据。

`response_bodies` 是调用者传入的本地原响应 SHA256 → `bytes` 映射。响应 SHA 必须复算相符；验证响应状态、请求身份、endpoint、业务状态、解析记录、原响应内分页元信息，并重投影与原成员/目录对象比较。未给原字节不能凭现有 `response_sha256` 声明就绪，结果为 `DATA_LIMITED`。

已有 THS endpoint 是 full-list 合同，因此期望响应页数为 1，实际页数取原 `pages` 长度，并核对原响应 `pagination.page/pages`、成员 `pagination.page_count`。多页未实现，明确拒绝，不用硬编码页数替代实采页。未提供独立 `total` 时保留 `provider_total=null` 和 `FULL_LIST_ENDPOINT_NOT_INDEPENDENT_TOTAL`，不把返回记录数当独立供应商总数。提供 `total` 时必须严格整型且等于实返数；后续页/截断/错误布尔类型均拒绝。

每个通过组件记录目录/成员 `content_hash`、完整原对象的规范 JSON 哈希、采集时间、实际/期望页数、响应 SHA、成员记录哈希及条数。`original_object_sha256` 指包括 `content_hash` 在内的原输入对象规范 JSON 哈希，不宣称等于原 JSON 文件字节 SHA；响应 SHA 则是提供的原字节 SHA。`binding_hash` 与输出 `content_hash` 支持复核精确参数和结论。

## 新鲜度与权限

日期基于带时区 `as_of` 和原 `observed_at`（上海时区），自然日计龄，不补日期，不用组装时间刷新旧组件。目录和成员取最老采集时间/最大年龄。未来时间（含同日未来秒）、无时区、缺时间都拒绝；若原对象有供应商 revision，则同时检查非未来且不超过三日，不伪造 revision。

| 最旧有效采集年龄 | freshness | 研究成员合同 | 实际新增仓权限 |
| --- | --- | --- | --- |
| 当日 | FRESH | 所有证据通过后为 true | false |
| 1–3 日 | DEGRADED | 所有证据通过后为 true | false |
| >3 日、未来、任一组件冲突 | UNAVAILABLE | false | false |

证据缺失返回 `status=DATA_LIMITED` 且 `freshness=UNAVAILABLE`；完整研究证据返回 `RESEARCH_CONTRACT_VALID`。不用 `READY` 或现有消费者的 `available`，防止未经接线的纯对象被当成执行输入。

## 等权历史方法身份

可选 `history_contract` 要求 `schema_version=board_reference_history/1.0`、`method=CONSTITUENT_EQUAL_WEIGHT_RETURN/1.0`、相同显式 `source_identity`，组件 `{board_id,membership_hash}` 与审计选用版本精确相等（不依赖列表顺序、不允许重复/漏项/额外版本）。这只是显式方法和成员版本身份匹配；不计算价格、指数或历史收益。

匹配输出 `IDENTITY_MATCHED_DATA_LIMITED`、`prices_available=false`、`returns=null`；未提供历史合同仍 `DATA_LIMITED`；提供了不一致的合同则方向研究不可用。成员研究有效不等于历史行情合格。真正等权历史仍需要价格、复权/PIT、区间成员版本与计算基准证据，不能用此声明填补历史数据。

## 反例与验证

工作目录 `D:/dev_A股/liangjian_wp5_hotfix_20261009`；Python 为 `D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe`，`PYTHONPATH` 为本 worktree `/src`。测试完全使用固定本地响应与 `httpx.MockTransport`，未调用 provider。

命令模板（PowerShell）：

```powershell
$env:PYTHONPATH='D:/dev_A股/liangjian_wp5_hotfix_20261009/src'
& 'D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe' -m pytest tests/test_wp4_board_reference_contract.py -q --junitxml=artifacts/wp5-20261010/wp4-board-contract-before.xml
& 'D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe' -m pytest tests/test_wp4_board_reference_contract.py -q --tb=short --junitxml=artifacts/wp5-20261010/wp4-board-contract-first.xml
& 'D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe' -m pytest tests/test_wp4_board_reference_contract.py -q --tb=short --junitxml=artifacts/wp5-20261010/wp4-board-contract-hardening-before.xml
& 'D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe' -m pytest tests/test_wp4_board_reference_contract.py tests/test_board_reference.py tests/test_hithink_board_reference.py tests/test_rotation_theme.py -q --tb=short --junitxml=artifacts/wp5-20261010/wp4-board-contract-final.xml
git diff --check
```

| 证据 | 实际结果 | exit |
| --- | --- | --- |
| before | 27 failed：新审计模块尚不存在；这是接口缺失证据，不宣称既有 collector 全部错误 | 1 |
| first | 27 passed | 0 |
| hardening-before | 35 passed / 4 failed：`complete='false'/0`、`truncated='false'` 被接受；bytes 身份使输出哈希异常 | 1 |
| final | 128 passed / 0 failed / 0 skipped（39 新测试 + 89 相关既有测试） | 0 |
| diff-check | 无输出 | 0 |

反例覆盖未来/缺采集日期、4/14 日旧缓存、伪 BK/东财源、组件缺失/冲突/改名、精确哈希错配、重哈希伪记录、原字节篡改、请求身份错、假页数/总数/错误布尔声明、历史成员版本错配、输入不变与确定性输出。额外硬化反例发现后再修复，旧失败 XML 保留。

读取时 HEAD 为 `e02b6c46a43a9cadbb43cddaab7a2d62714092cb`，验证时共享 HEAD 已由其他操作者更新到 `d641b909c2bc51a2126881673400692bbfadf2b7`；本切片未 commit。三个依赖源 SHA 前后相同：

| 路径 | SHA256 |
| --- | --- |
| src/liangjian_funnel/data/board_reference.py | c03ab6726bdda50db0bf782db3dc3e104f854a145ba735b8092603a4fd0d2c91 |
| src/liangjian_funnel/data/hithink_board_reference.py | aa1b4df39d63f233d1b2792189d7154de19bd1e132ac8b26ad326063e25ea8b7 |
| src/liangjian_funnel/pipeline/data_source.py | ea652a859102711201d096030a8fd6fda8c069d86993f6c131132a9061184f58 |

独立冻结回执：`artifacts/wp5-20261010/wp4-board-contract-receipt.json`，含本切片三个文件、XML 哈希和退出码。

## 尚未接线及验收缺口

既有 `hithink_board_reference.load_rotation_references`、Settings 和当前消费者仍沿用原合同/年龄策略，本模块不会把它们自动改成三日政策。需要 root 后续显式接线和验收；不能只因本测试绿就宣称生产已经不读东财或新开仓安全。

当前 collector 只保存原响应 SHA 和解析记录；真实缓存若无可复算字节，审计保持 DATA_LIMITED。本切片未扩写 collector，不采集/归档缺失原字节，不读取生产数据。

消费者盘点的缺口仍在 `artifacts/wp5-20261010/WP4_CONSUMER_INVENTORY.md`：09:26 竞价 base 热榜必需门、收盘东财板块流串行等待、LOCAL_REFERENCE 的 BK 历史门、27 方向历史不足。本模块不修改它们。

27 方向真实大文件覆盖、腾讯全成员 180 秒耗时、27 方向及复合等权历史、5 日影子差集、连续三日 09:26 READY 与人为断东财仍 READY 都尚未提供证据。不调整预算、阈值、候选域、源配置，不更新桥接 state/ITERATION_STATE，不提交或部署。
