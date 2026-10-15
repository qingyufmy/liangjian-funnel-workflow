# B3-10：竞价原时标与发布ATR证据（不重锚）

0177答复已实际读取。以下为隔离候选实现，不是生产发布或自然验收。

## 09:25原时标

新增独立Node job `auction-evidence` → `run-auction-evidence`，只在Asia/Shanghai 09:25:00–09:25:30派发一次；父预算30秒，使用review槽，不占09:26晨审的primary槽。不借此启动A2/A3研究；窗口过后不补采、不拿09:26价格冒充竞价原价。

CLI独立分派，不构造WorkflowApplication或RuntimeStore；SQLite只读取得当日、主lane、待晨审/活动计划的真实范围和行哈希。`outputs/opening_evidence/<日期>/once.json`以排他创建封存本日采集权，重启不重复调用源；原观察以独立哈希文件封存，不改计划。复用已有腾讯批量报价函数，不改供应商限流。

`auction_final_price_claimed=true`的精确定义：同股票同来源的原provider-reported时间恰为当日09:25:00、正价、有报告成交，且本机在09:25:30前收到。它是**基于供应商时标的竞价价声明**，不是交易所直连认证；`exchange_provenance_authenticated=false`固定保留。09:25:01/09:26、旧日、未来、无成交、错股票或迟到一律false，原观察仍保存，不改写时标。若实际供应商时标无法满足，则如实DATA_LIMITED，不制造五日有效样本。

## 发布ATR

未来A3发布时，在既有非执行权的 `strategy_facts.shadow_inputs.atr14_evidence` 中追加Wilder TR14证据，与独立shadow projection同封存。既有shadow_inputs的AVAILABLE/DATA_LIMITED状态不因这一证据而提升；不修改模型原输出、提示词、风险线、入场带、追价线或晨审行为。

读取当前knowledge cutoff前的**全部本地PIT日线**，`limit=None`，不以最后14根均值或70根截取替代；逐行校验股票、raw复权口径、哈希、闭合、知识时钟、顺序与最新闭合日。TR的14值均值作seed，随后对全部剩余根做Wilder递推。记录seed_start、source_rows_count/hash、knowledge_at、latest_source_observed_at与captured_at。

“全部”仅指本地PIT缓存可用窗口；不声称从上市首日完整，也不宣称除权事件检测已覆盖：`listing_history_completeness=NOT_CLAIMED`。未来V3/V4严格验收仍需验证其完整seed合同，不能因出现一个正ATR就自动放行。原factor身份/时间冲突仍禁止读缓存，不借新证据绕过旧门。

## 簇与卡片证据

只在内部卡封存的plan_binding追加真实冻结簇标签、依据及数量；UNKNOWN不作为共享暴露簇。客户标题/正文/颜色/发送次数保持相同；不降级任何计划，不增加A4信号上限。日期绑定以数据库expires_at的上海日期为准，不用旧payload日期覆盖。

## 本地反例与边界

- opening红测：缺模块退出1；13项首轮绿，补CLI无Workflow构造和空范围后15项绿，退出0。
- Node独立任务/窄窗口红测2失败退出1；与原auction-refresh/58秒预算相关11项绿，退出0；typecheck退出0。
- ATR红测：缺模块退出1；8项ATR绿。发布路径首次回归发现旧factor冲突仍读缓存的4失败；已补冲突前置保护，52项相关绿，退出0，失败原件保留。
- 竞价/ATR/卡片合计43项绿（后另补2项CLI测试）；独立旧卡日期反例4失败→相关58绿，客户调用逐项相同。
- 最终HEAD全量与最终候选VM差异回执仍按总任务节点收口，不以这些切片替代。

当前14条10-16正式计划原行不变，不运行恢复研究，不补原A5、历史竞价或历史信号；候选获实际ACCEPT后方可部署。
