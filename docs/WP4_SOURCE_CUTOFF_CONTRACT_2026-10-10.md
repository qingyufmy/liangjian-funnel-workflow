# WP4 资金缓存双截止合同

依据桥接 0018 ANSWER，纯合同及现有缓存 reader 可选接入。未切生产源、未采集新数据、未改权重或阈值、未修改原缓存。默认旧 reader 契约不变，不宣称生产链已经全面消除未来数据问题。

## 确定性合同

`SourceCutoffs(requested_market_cutoff, frozen_received_cutoff)` 必须 aware；received cutoff 为真正采集完成/封存时间，不是请求起点。逐来源检查两条独立上界：业务市场观察时间不晚于 market cutoff，实际接收时间不晚于原运行 seal。命中未来界限 `SOURCE_CACHE_FROM_FUTURE`，原来源整体不可用，不寻找最近文件、不增加容差。两截止时间由调用方原回执给定，validator 不读取当前时间。

新来源必须明确版本 `source-dual-cutoff/1`、source_id、trade_date、market_observed_at、ingested_at、observation_basis、end_of_session_semantics。缺版本或时间 `LEGACY_UNVERSIONED`，无法凭文件 mtime 或请求 as_of 修补。来源、业务日期、观察日期不一致不能复用；时区缺失、接收早于观察、未知语义保持缺证。

`PROVIDER_EVENT_TIMESTAMP` 不允许未来业务事件借收盘旗标倒填。仅显式 `CLOSED_SESSION_VALUE` 且同一交易日、cutoff=15:00、观察晚于收盘时，实际观察记录保留、有效业务时间绑定该收盘；这是供应商闭合业务值声明，不是独立认证。盘中无实际 provider event time 的采集是 `OBSERVED_AT_UNPROVEN`，不能以一条配对报价的日期证明所有个股资金时间。

## 现有 reader 接入

`a2_market.load_capital_flow_snapshot/_load_capital_flow_cache_state` 新可选 `cutoffs` 与 `expected_source_id`；启用合同必须显式声明预期供应商，不从 payload 自证。`load_board_capital_flow_snapshot/inspect_board_capital_flow_snapshot` 新可选 `cutoffs`，预期板块来源固定既有东财身份。

原 JSON、schema、hash、date 校验保留；截止校验在 TTL 之前执行。公开 reader 返回 None，状态 reader 保留真实失败码；板块检查附纯 cutoff 证据。旧调用不带合同保持原行为，不能把“存在 opt-in API”称为正式调用已接线。采集器尚未新增真实业务时间/封存时间，旧缓存带 contract 的读取因此会如实 DATA_LIMITED。接线必须从实际 provider 与实际 seal 获取，不能追溯写回历史文件。

## 待接线与口径校准

六个东财板块历史项和逐股回退的 LOCAL_REFERENCE 隔离仍在下一切片，不将 SHADOW 标签等同执行隔离。已向 Claude 0020 提问：逐股原 WINDOWS 为 .35/.25/.25/.15，板块为 .5/.3/.2，不能混作同一个 capital 口径。腾讯 today 原事实可保存，复合 score 缺历史不能放大到 1.0。未得到该校准答复前不新定义权重。

## 命令与验收

`PYTHONPATH=src`，共享虚拟环境执行 pytest。

- 合同反例 before：exit1/module missing，`source-cutoff-before.xml`。
- 初合同：20 pass/1 fail（未来观察先落到时间顺序错误码），原 `source-cutoff-final.xml` 保留；修正上界判断顺序后21 pass，exit0，`source-cutoff-final-v2.xml`。
- 新缓存反例：21 pass/5 fail，exit1，`source-cutoff-cache-before.xml`。
- 缓存+既有两个市场模块：46 pass，exit0，`source-cutoff-cache-final.xml`。
- 补旧 provider 日期及公开板块 loader 转发，与WP5两跟进联合：136 passed，exit0，`review-cutoff-joint-final.xml`；不与前轮计数相加。
- 无效预期来源（None、空串、空白、bool、整数）原先可与payload相同而自认证；新增五个反例全部真实失败，`source-cutoff-identity-before.xml`。修复后显式来源ID必须为合法非空文本，最终联合141 passed，exit0，`review-cutoff-joint-final-v2.xml`。

CODE 局部合同通过待全量新HEAD；REPLAY 本地显式元数据反例；OPERATIONS 未接正式采集/封存及未部署；STRATEGY 不适用。source 时间声明本身不证明供应商来源真实性。
