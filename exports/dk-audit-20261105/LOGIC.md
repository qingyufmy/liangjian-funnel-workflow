# DK 的准确实现逻辑

以下代码链接均指本导出的**原始字节**；行号与来源提交一致。新包装仅负责隔离导入/读取输入，不改计算。

## 1. 定义、参数与公式

实现：[ssl.py](snapshot/src/liangjian_funnel/market_state/ssl.py#L16)，入口 [dk_state](snapshot/src/liangjian_funnel/market_state/ssl.py#L137)，增量 [ssl_append](snapshot/src/liangjian_funnel/market_state/ssl.py#L75)。默认N=20，只接受精确整数10/20/30，True、5、19、40均拒绝。**N是15分钟根数，不是交易日数。** 采用Decimal，局部精度34，不在比较前四舍五入。

对于当前第t根闭合15m K线：

```
upper[t] = mean(high[t-N+1 : t+1])
lower[t] = mean(low [t-N+1 : t+1])
```

窗口**包含当前根**。小于N根时轨道为None；满N根可以有轨道，但前3N根仍完全不建立方向。代码先判断 `memory.count >= 3*N`（此时尚未计入当前根），所以N=20时第1～60根为UNKNOWN/无事件，第61根才READY；N=10/30分别第31/91根。READY仍可能是UNKNOWN，不能把它当已经有多空方向。

预热后：`close > upper → D`；`close < lower → K`；中间区和**等于任一轨道**均保持前状态。只有 `new_state != old_state` 才产生事件；UNKNOWN→D/K也计首事件。D事件trigger_level为当根upper，K为lower。事件时间是实际bar_end，不回填成历史摆动低点/高点。`last_event`和`bars_since_event`只供记录，事件根为0、随后逐根+1；没有基于它们的冷却门。

## 2. 输入、周期与复权

[Bar15m结构](snapshot/src/liangjian_funnel/market_state/bars15m.py#L24)包含symbol、bar_end、status、expected_5m、present_5m、OHLC以及可空的volume_shares/reported_amount_cny/estimated_amount_cny、source_id、adjust_mode、reasons。核心SSL只用**high/low/close**计算；open参与OHLC合法性检查。价格要求正、有限Decimal。数量/成交额不是本DK公式条件，缺量不会自动发明量价确认。

[compose_15m](snapshot/src/liangjian_funnel/market_state/bars15m.py#L80)接收已归一化的5m MARKET_BAR，严格symbol/interval/source/adjust一致。每日16个15m结束槽：09:45、10:00、10:15、10:30、10:45、11:00、11:15、11:30、13:15、13:30、13:45、14:00、14:15、14:30、14:45、15:00。每根由 end-10m、end-5m、end三根5m组成，开=首根、收=末根、高/低取极值；09:30竞价行排除，不跨午休造K线。半日/未知交易时段布局会拒绝。

合成器允许声明forward以供历史锚点分析，但**SSL核心只接受none/raw**；当前腾讯指数/个股价格解析声明none，没有在本模块里做前复权或调用复权因子。截图站点的前复权校准和这个最终自研未复权实现是不同路线。跨除权窗口可能造成原始价格跳变，尚没有以本轮导出证明该边界的业务质量；迁移方需核数据窗口和复权标签，不能擅把forward改为none绕过。

日线/周线趋势位于独立 [index_trend.py](snapshot/src/liangjian_funnel/market_state/index_trend.py)，与DK并列形成M0研究事实；日线MA5/10/20/60、130日需求**不是DK预热条件**。DK没有日线确认，也没有把原5m当直接信号周期；5m仅用于合成15m。最终[配置](snapshot/config/market_state.yaml#L53)也没有把DK与日线/现有形态相与生成交易权限。

## 3. 闭合、时钟、缺口与防重绘

合成器及SSL都要求观察时刻≥bar_end+60s，且三根5m都齐。核心校验源必须精确为`TENCENT:ifzq.gtimg.cn`、status=CLOSED、symbol为SH/SZ显式身份。重复、乱序、混symbol、混复权、未来/未闭合、缺5m或内部15m断档均报错，不静默拼接。序列跨午休、隔夜和节假日的下一合法槽，由真实XSHG日历决定，不采用周一至周五近似。

[build_dk_shadow](snapshot/src/liangjian_funnel/market_state/dk_shadow.py#L82)另核PIT：源/目标交易日、INTRADAY模式、as_of、收盘时点、provider_timestamp、capture起止及原SHA/collection_status。输入最后一根5m必须与as_of相同，provider clock不得早于输入，capture不得晚于观察。明确指数注册键和计划股票域不可混淆。尾部应有最新闭合15m，过期结果DATA_BLOCK。320根窗口最前方未凑齐的一天允许排除不完整前导槽，但排除之后的内部缺口仍被SSL拒绝。

固定输入下的每个前缀输出与整段对应前缀相等；增量memory为不可变dataclass，后续bar不回写过去，没有ZIG/BACKSET或反向找极值。但这只证明对**不变输入**的因果性；不能替供应商保证历史行情永不修订。[merge_bars](snapshot/src/liangjian_funnel/market_state/dk_archive.py#L20)会拒绝重叠原件修订，保first_seen_at和源SHA。

[evaluate_ssl](snapshot/src/liangjian_funnel/market_state/ssl.py#L142)读取事件后1/4/16根收盘以离线算收益，**这是后见评估，不能送回当时信号或冒PIT**。不足未来窗口的事件列right_censored，不进该窗口分母；4根内反向切换率同样排除右侧未成熟样本。K组仍报告原始价格收益，不翻转成“真实做空收益”，没有佣金、滑点、订单或券商成交。

## 4. 多空、买卖、确认、失效与去重

实际存在的是D/K/UNKNOWN研究状态及状态改变事件，**没有已接线的下单买卖、止损退出、仓位、持仓管理或风控许可**。没有附加多根确认、成交量阈值、MACD/KDJ过滤、A/B/C确认、固定冷却或计划级事件投递去重。相同方向连续保持不会重复发新状态事件，反方向可在下一根闭合bar直接切换；重复bar本身拒绝，不能据此声称生产事件去重已实现。

错误输入在纯函数中抛异常；影子构建器转换为该series的DATA_BLOCK。异常不修改旧memory，但调用者不能跳过错误bar后直接沿用旧状态，因为连续性仍需通过。SHADOW_RECORDED仅表明本轮影子输入被记录，不代表N已预热、20日覆盖通过或允许交易。

## 5. 实际调用链和各阶段用途

1. [指数解析parse_index_5m](snapshot/src/liangjian_funnel/market_state/tencent_indices.py#L23)或[个股_equity_prices](snapshot/src/liangjian_funnel/market_state/dk_shadow.py#L15) → [compose_15m](snapshot/src/liangjian_funnel/market_state/bars15m.py#L80)。指数走六指数显式注册，不走个股map_symbol，`000001.SH`不冒成平安银行。
2. [build_dk_shadow](snapshot/src/liangjian_funnel/market_state/dk_shadow.py#L82) → [ssl_series](snapshot/src/liangjian_funnel/market_state/ssl.py#L129) → [ssl_append](snapshot/src/liangjian_funnel/market_state/ssl.py#L75)；取末点可用[dk_state](snapshot/src/liangjian_funnel/market_state/ssl.py#L137)。影子同时输出N=10/20/30及评价；默认period20并不隐藏比较组。
3. [track2_assemble_tencent.py:74](reference/scripts/track2_assemble_tencent.py#L74)与[track2_close_shadow.py:67](reference/scripts/track2_close_shadow.py#L67)是冻结原件离线组装入口；[track2_collect_tencent.py:169](reference/scripts/track2_collect_tencent.py#L169)是历史独立采集脚本的影子调用，非生产调度。
4. [track2_archive_dk.py:35](snapshot/scripts/track2_archive_dk.py#L35) → [merge_bars](snapshot/src/liangjian_funnel/market_state/dk_archive.py#L20) / [archive_evaluation](snapshot/src/liangjian_funnel/market_state/dk_archive.py#L60)，记录真实采集日与历史bar日的差别，不以回溯6天bar冒5天实际影子。

|阶段|源码/历史证据所支持的实际用途|未接线部分|
|---|---|---|
|A1|无DK消费者|未作为池筛选、必需数据或硬门|
|A2|无DK消费者|未作为研究评分/排序依据；独立TH1也不自动等于DK|
|A3|无DK消费者|未改变LEADER_INTRADAY/MA520_SWING/TREND_MA5发布或参数|
|A4|无DK消费者|现有A/B/C由runtime/entry_confirmation.py独立计算；没有DK入场/退出门|
|A6|只在设计中拟将M0事实用作收紧依据|未发现实际输入/权限消费者|
|PM|无DK消费者|没有DK组合排序/仓位意见或CAS接线|

本机对源提交src/scripts/server/config的导入/调用搜索只发现market_state内部及上述Track2脚本。已知GitHub生产基线e9的tree不含这个market_state目录/配置。本轮没有连接VM，因此不把本地基线和历史`deployed=false`摘要夸成所有机器永久未部署的证明；准确结论是**未找到DK生产部署证据，已查调用链未接策略**。

## 6. 开关及尚未验证边界

`config/market_state.yaml`顶层enabled=false、authority=SHADOW_ONLY；dk.enabled=false、status=SELF_RESEARCH_PENDING、formula=SSL_SMA、period=20、comparison_periods=[10,20,30]、mix_sources=false。`load_market_definitions`严格验证这份关闭配置，改true会拒绝。另一方面，离线函数仍可显式调用，false不是禁止计算所有研究样本；它没有向生产注册任何消费者。

仍缺：20个预热后完整交易日回算、5个真实连续采集日影子验收、实际计划股范围/结果、真实A6/独立PM/风控CAS、集成H/R全量及F7/D8/FINAL、证券复权/特殊交易时段的完整业务验收。实际策略与交易权限的接入不在本次导出范围。
