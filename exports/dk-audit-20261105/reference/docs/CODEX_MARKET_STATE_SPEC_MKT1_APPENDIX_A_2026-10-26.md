# MKT-1 附录 A：指标计算与取数规范（M0 市场状态层）

签发：Claude　时间：2026-10-26 21:10　Tony 要求："指数趋势、DK、宽度、量能如何计算、针对现有数据源如何取数，给出详细方案，否则 Codex 可能直接做错。"
**Tony 20:42：DK 取消；指数集合、趋势分类、量能阈值按本附录建议值确认（不再是待确认值）。**
本附录是 MKT-2/4 的**唯一实现口径**。凡本附录未写明的，先问再做，不得自行补口径。所有阈值写入 `config/market_state.yaml`（已确认值，仍需先影子运行 5 日再接权限）。
代码依据（隔离树 `.team-v2-postclose-0761-20261026`）：`pipeline/data_source.py`（HithinkClient）、`data/tencent_minute.py`、`data/mootdx.py`、`runtime/live_market.py`、`runtime/team_v2_emotion_source.py`、`runtime/team_v2_regime.py`、`facts/hithink.py`。

---

## A0. 现有数据源能力盘点（只用已有源，不新接源）

| 源 | 已有方法 | 能取到什么 | M0 用途 | 注意 |
|---|---|---|---|---|
| HiThink（同花顺授权 API） | `index_history_1d(thscode,start,end)` → `/api/a-share-index/prices/historical`，interval=1d | 指数日线 OHLCV(+amount 待核) | **指数日线主源** | 只支持 1d；`thscode` 须带 `.SH/.SZ` 后缀；`max_pages=1`，一次取完区间 |
| HiThink | `index_snapshot(thscodes)` → `/api/a-share-index/prices/snapshot` | 指数实时/收盘快照 | 当日日线补齐、成交额交叉核对 | 一次可传多个指数 |
| HiThink | `market_snapshot()` → `/api/a-share/prices/snapshot`（分页 1000×10） | 全市场个股 last/prev/high/volume(/amount 待核) | **宽度、两市成交额(口径 B)、涨跌停派生**的唯一主源 | `live_market` 每 5 分钟已经在拉，**复用同一次请求，不得新增第二次全市场请求** |
| HiThink | `limit_up_pool / limit_down_pool / limit_break_pool / limit_up_ladder` | 涨停池、跌停池、炸板池、连板天梯 | 情绪四数的**交叉核对** | 收盘采集链 `collect_market_results` 已在用 |
| Tencent `ifzq.gtimg.cn/appstock/app/kline/mkline` | `TencentIntradayAdapter.fetch_bars`（仅 1m/5m，最多 320 根） | 分钟 K 线 | **M0 不使用**（DK 已取消） | 现有 `_provider_symbol` 走**个股** `map_symbol`，**不能直接用于指数**（见 A1） |
| Tencent `qt.gtimg.cn/q` | `fetch_quote` | 实时报价，fields[37] 成交额(万元) | 指数报价交叉核对 | 同上，指数要单独代码路径 |
| Mootdx | `fetch_bars` 1m/5m | — | **不用** | LJ-073：协议解析失败，5m 最近成功 09-10 |
| `runtime/team_v2_emotion_source.emotion_pool_stats` | 已实现 | 涨停/跌停/炸板率/最高连板（ST、退市、上市<5 日、北交所排除） | 情绪四数**主口径** | 依赖前一交易日连板 `previous_streaks` |

**没有**的能力：HiThink 指数分钟线；任何源的"指数 DK"；两市成交额的历史序列（需用指数日线 amount 构造，见 A5）。

---

## A1. 通用约定（所有指标适用）

1. **时区/日历**：一律 `Asia/Shanghai`；交易日只用 `runtime/calendar`（节假日、调休、半日市以日历为准），不得用"工作日"推算。
2. **只用已闭合 K 线**：日线以 15:00 收盘后的值为准；盘中任何"日线指标"必须标 `intraday_partial=true`，**不得**把盘中半根日线并入 MA 计算并当成收盘信号。
3. **PIT 字段**（每个输出对象必带）：`source_trade_date`、`market_close_at`（收盘版=当日 15:00）、`provider_timestamp`、`capture_started_at`、`capture_completed_at`、`source_id`、`input_sha256`。沿用 LJ-063 闭市标记，盘后采集不得把 `market_data_as_of` 推到采集时刻。
4. **缺失即 DATA_BLOCK**：字段级标记；不补零、不前值填充、不用昨日冒今日、不用其他指数替代。
5. **数值**：`Decimal` 计算，输出保留 4 位小数（价格类 2 位）；比较阈值时用原精度，不用四舍五入后的值。
6. **一条序列只用一个源**：同一指标的历史与当日必须同一口径；跨源只做核对，不拼接。
7. **⚠ 指数代码绝不走个股 `map_symbol`**：`map_symbol('000001')` 会解析成**平安银行 000001.SZ**；`000300/000852/000688` 也会被当成深市个股。必须新建 `config/market_indices.yaml` 注册表，指数只按注册表的显式代码取数，并**校验返回名称**等于注册表名称，不等即 `INDEX_IDENTITY_MISMATCH` 阻断。

### 指数注册表（Tony 20:42 已确认）

| key | 名称 | HiThink thscode | Tencent code | 用途 |
|---|---|---|---|---|
| SSE | 上证指数 | 000001.SH | sh000001 | 大盘主判 |
| SZSE | 深证成指 | 399001.SZ | sz399001 | 深市、成交额口径 A |
| CYB | 创业板指 | 399006.SZ | sz399006 | 成长风格 |
| HS300 | 沪深300 | 000300.SH | sh000300 | 大盘蓝筹 |
| CSI1000 | 中证1000 | 000852.SH | sh000852 | 小盘风格 |
| KC50 | 科创50 | 000688.SH | sh000688 | 科创风格 |

HiThink 的指数 thscode 格式**先用 `index_snapshot` 探针一次性核实**（返回 name 逐一比对），若 HiThink 用其他编码（如同花顺内部码），以探针结果为准写入注册表，不得猜。

---

## A2. 指数日线趋势（收盘版，MKT-2）

### 取数
- **历史**：每日收盘后对 6 个指数各调用一次 `index_history_1d(thscode, start=今日-400 自然日(ms), end=今日 15:00(ms))`，原始点位（指数无复权，不传 adjust）。要求每个指数 ≥130 个交易日（MA60 斜率 + 周线 MA20 需要）。不足 → 该指数 `HISTORY_INSUFFICIENT`。
- **当日 bar**：15:05 后若 history 已含当日 bar，直接用；若不含，用 `index_snapshot` 构造当日 bar（open/high/low/last/prev_close），要求 `provider_timestamp ≥ 15:00` 且 **`prev_close` 与 history 最后一根 close 差 ≤0.01 点**，否则 `DAY_BAR_CONTINUITY_FAILED`（防缺日/错日）。
- 采集放在收盘链**种子阶段之外的独立小作业**，预算 ≤30 秒；**不得进入 15:10 收盘链 90 分钟父预算**（LJ-086 教训）。失败不阻断收盘链，只让 M0 该部分 DATA_BLOCK。

### 计算（t = 当日收盘）
```
MA_n(t)      = mean(close[t-n+1 .. t])                n ∈ {5,10,20,60}；不足 n 根 → null
slope_n(t)   = MA_n(t) / MA_n(t-5) - 1                MA 的 5 日变化率
dist_n(t)    = close(t) / MA_n(t) - 1
ret_k(t)     = close(t) / close(t-k) - 1               k ∈ {5,20,60}
pos20(t)     = (close - min(low[t-19..t])) / (max(high[t-19..t]) - min(low[t-19..t]))   分母为 0 → null
arrangement  = BULL  若 MA5>MA10>MA20>MA60
               BEAR  若 MA5<MA10<MA20<MA60
               MIXED 其余
```
**短期趋势分类**（按顺序判定，阈值入 yaml，已确认）：
```
UP    : close > MA20  且 slope_20 > 0  且 MA5 > MA10 > MA20
DOWN  : close < MA20  且 slope_20 < 0  且 MA5 < MA10 < MA20
RANGE : 其余
```
**中期趋势分类**：
```
MID_UP   : close > MA60 且 slope_60 > 0
MID_DOWN : close < MA60 且 slope_60 < 0
MID_FLAT : 其余
```
**周线**：按交易日历把日线聚合成自然周（周内最后一个交易日为周收盘）；O=周内首日 open，H=max，L=min，C=周末 close。**当周未结束 → `partial=true`，不参与周线 MA 与周线趋势判定**（只展示）。周线 MA5/10/20 与周趋势分类同上（用 MA5/10/20 替代 MA5/10/20/60，`slope` 用 MA 的 2 周变化率）。

**综合与风格**（只做事实，不打分）：
```
market_trend_votes = {UP: n, DOWN: n, RANGE: n}   按 SSE、HS300、CSI1000 三者
style_divergence   = HS300.trend != CSI1000.trend  （大小盘分化事实）
```

### 输出（每指数）
`{key, name, thscode, close, ma:{5,10,20,60}, slope:{20,60}, dist:{20,60}, ret:{5,20,60}, pos20, arrangement, trend_short, trend_mid, weekly:{ma, trend, partial}, pit:{...}}`

---

## A3. ~~15 分钟结构与 DK（MKT-3）~~——**已取消（Tony 20:42，无公式）。本节不实现、不建 15m 归档、不新增 Tencent 指数分钟请求；保留文字仅供将来参考。**

### A3.1 15m K 线的唯一构造方式
- **源**：Tencent 5m（HiThink 无指数分钟线；Mootdx 不可用）。**新增指数专用取数函数**：用注册表的 `tencent code` 直接拼 `param=sh000001,m5,,320`，**不得复用 `_provider_symbol`/`map_symbol`**；解析时校验返回 `data` 的键等于该代码。
- **单位**：Tencent 指数 5m 的 volume 单位**未核实**（个股是"手"，代码里 ×100）。DK 定义若不需要成交量，15m 只用 OHLC；若需要，先用当日 `index_snapshot.volume` 做单位校准，未校准不得使用 volume。Tencent 分钟 `amount` 是 OHLC 估算值（`amount_kind="ohlc_estimate"`），**不是成交额，禁止用于量能**。
- **聚合**：每日 16 根 15m，结束时刻固定为
  `09:45 10:00 10:15 10:30 10:45 11:00 11:15 11:30 13:15 13:30 13:45 14:00 14:15 14:30 14:45 15:00`。
  每根由其 3 根 5m 组成（如 09:45 = 09:35、09:40、09:45 三根）。O=首根 open，H=max，L=min，C=末根 close，V=sum。**缺任何一根 5m → 该 15m `INCOMPLETE`，不得用 2 根拼**。
- **09:30 戳记**：若 Tencent 返回 09:30 的 5m（集合竞价），**不并入、不单独成根**；09:35 那根的 open 用其自身 open。⚠ 通达信/同花顺对集合竞价的处理可能不同——这是 MKT-3 校准第一项，以 Tony 软件的 15m 第一根 OHLC 为准再定。
- **闭合判定**：`now ≥ bar_end + 60s` 且三根 5m 都已到达才算闭合；未闭合的 15m 只能展示，不能更新 DK 状态。
- **历史**：Tencent 一次最多 320 根 5m ≈ 6.7 个交易日 ≈ 106 根 15m。**从上线日起每日收盘后把当日 16 根 15m 追加写入本地归档**（append-only，带 SHA），DK 预热所需的更长历史靠归档累积；预热不足 → `DK_WARMUP_INSUFFICIENT`，不得用日线或个股数据冒充。

### A3.2 DK 定义（**Tony 提供前不得实现任何公式**）
Tony 需给出：①公式或指标源码；②计算用哪个指数（建议上证指数，也可多个）；③状态集合（例如 D/K 两态还是含中性）；④"新 D""新 K"的事件定义。
**Codex 收到定义后必须先做"未来函数审查"**：通达信/同花顺不少买卖点指标使用 `ZIG`、`BACKSET`、`PEAKBARS/TROUGHBARS`、`REFX`、`DRAWTEXT` 回溯等**会重绘**的函数。凡依赖未来数据的，**不能用于实盘**——须书面报告并给出无未来函数的等价或近似方案，由 Tony 拍板；不得悄悄改写。
**实现要求**：纯函数 `dk_state(bars_15m_closed) -> {state, last_event:{type: NEW_D|NEW_K, bar_end, price, values}, bars_since_event}`；只吃已闭合 15m；同一输入同一输出；逐根增量计算结果必须与整段一次性计算逐根一致（防重绘的硬测试）。

### A3.3 调度与性能（LJ-084 教训）
- **不放进 A4 每分钟主循环的关键路径**。独立轻作业在每个 15m 边界 +60s 运行：6 个指数各 1 次 Tencent 5m 请求（≤6 次/15 分钟），单次超时 3s，总超时 10s。
- 失败只让 DK `DATA_BLOCK`，A4 照常运行；A4 只读最近一次已写入的 DK 结果，并检查其 `bar_end` 是否为当前应有的最新闭合 15m，过期即视为 DATA_BLOCK。

---

## A4. 市场宽度（MKT-4）

### 取数
- **复用** `live_market.load_or_refresh_live_market_state` 中同一次 `HithinkClient.market_snapshot()` 结果（已有 `snapshot_capture/snapshot_observer` 钩子），**不新增全市场请求**。
- **统一 universe**：与 `team_v2_emotion_source.derive_identity` 完全相同——沪深 A 股（`asset_type=='a-share'`，SH/SZ），排除北交所、ST/*ST、名称含"退"、上市不满 5 个交易日、当日无成交。宽度与情绪必须用同一 universe。
- ⚠ 现有 `classify_full_market` 用的是全部行（未排除 ST/北交所等），口径不同。**MKT-1 不改它**，新口径输出为 `breadth_v2`，旧字段原样保留，二者并列。

### 计算（盘中每 5 分钟桶 + 收盘版）
```
chg_i        = last_i / prev_i - 1         （prev_price 来自快照；缺 prev 的票剔除并计数）
adv, dec, flat
breadth      = adv / (adv + dec)            分母 0 → null
adv_share    = adv / N_included
median_chg, mean_chg（等权）
up5_share    = #(chg ≥ +5%) / N ; down5_share = #(chg ≤ -5%) / N
coverage     = N_included / N_expected      < 0.75 或 N_included < 500 → DATA_BLOCK（与现有阈值一致）
```
收盘版可选（第二阶段，需要全市场 20 日个股日线；若种子 G0 已含则复用，否则不做）：`pct_above_ma20 = #(close_i > MA20_i)/N`。

---

## A5. 量能（成交额，MKT-4）

### 两个口径，用途分开，**不得混拼**
| 口径 | 定义 | 历史可得性 | 用途 |
|---|---|---|---|
| **A（官方口径）** | `T_A(d) = amount(000001.SH, d) + amount(399001.SZ, d)`，来自 `index_history_1d` 的 amount 字段；当日用 `index_snapshot` 收盘 amount | 立即有 ≥130 日历史 | **量比、放缩量判定** |
| **B（诊断口径）** | `T_B = Σ amount_i`，全市场快照中沪深 A 股（**含 ST**，不含北交所）逐股成交额求和 | 只有上线后的归档 | 盘中节奏、与 A 交叉核对 |

- **先探针核实**：HiThink 指数历史/快照是否含 amount 字段、单位（元/万元/亿元）、上证指数 amount 是否等于沪市全部 A 股成交额。核实结果写入 yaml；未核实不得启用口径 A。若 HiThink 指数不含 amount，口径 A 改为 Tencent 指数日线（需另批新端点）或推迟，**不得用分钟 amount 估算**。
- 交叉核对：收盘 `|T_A - T_B| / T_A ≤ 3%`（阈值待定），超出 → `TURNOVER_DEFINITION_DIVERGENCE` 告警，不改值。

### 计算（收盘版，口径 A）
```
vr5   = T(t) / mean(T(t-5 .. t-1))
vr20  = T(t) / mean(T(t-20 .. t-1))
pct60 = T(t) 在过去 60 个交易日中的分位
volume_state = EXPAND 若 vr5 ≥ 1.20 ；SHRINK 若 vr5 ≤ 0.80 ；NORMAL 其余      （已确认）
```
### 盘中节奏（口径 B，需归档）
```
pace(t) = 当日截至 t 的累计 T_B  /  过去 5 个交易日同一时刻累计 T_B 的均值
```
- **同一时刻**按 5 分钟桶对齐（如 10:30 对 10:30）。归档不足 5 日 → `PACE_HISTORY_INSUFFICIENT`。
- ⚠ **禁止用"已过分钟数/240"线性外推全天成交额**：A 股成交量日内呈 U 形，线性外推会在早盘系统性高估。

---

## A6. 涨跌停与情绪（MKT-4，基本复用）

- **主口径**：现有 `emotion_pool_stats`（快照派生：涨停数、跌停数、炸板率=炸板/(涨停+炸板)、最高连板=前日连板+1）。universe 同 A4。依赖前一交易日完整连板表 `previous_streaks`（`known_at` 当日 09:30 前），不全即 DATA_BLOCK——**不要放宽这条**。
- **核对口径**：收盘后 `limit_up_pool / limit_down_pool / limit_break_pool / limit_up_ladder` 的计数与最高板，与主口径比对：差异 >3 只或最高板不等 → `EMOTION_SOURCE_DIVERGENCE` 告警，主口径不变。
- **新增（收盘版，可选）**：昨日涨停今日溢价 `premium = mean(chg_i)`，i ∈ 昨日涨停名单（来自前日归档，ST 排除）；连板梯队分布 `{2板:n,3板:n,...}` 取自 ladder。
- 产出 10-20～10-26 **生产实测完整率报告**：每日每 5 分钟桶 READY 比例、DATA_BLOCK 原因分布、与核对口径差异。

---

## A7. 存储、调度、性能汇总

| 作业 | 时点 | 新增请求 | 预算 | 失败影响 |
|---|---|---|---|---|
| 指数日线收盘版 | 15:05 后（独立于收盘链） | 6×`index_history_1d` + 1×`index_snapshot` | ≤30s | M0 日线部分 DATA_BLOCK |
| 宽度/情绪盘中 | 每 5 分钟桶（复用现有快照） | 0 | 计算 ≤200ms | 对应字段 DATA_BLOCK |
| 量能收盘版 | 15:05 后 | 复用日线作业结果 | — | 量能 DATA_BLOCK |
| 归档 | 收盘后 | 0 | — | 次日节奏/预热不足 |

存储：`state/market_state/{trade_date}/close.json`、`intraday/{HHMM}.json`、`archive/index_15m/{key}.jsonl`、`archive/turnover_b/{trade_date}.json`；append-only，写前后 SHA，不改历史文件。

---

## A8. 验收（每条都要原件）

1. **指数身份**：探针返回名称 6/6 与注册表一致；`000001` 负例必须不会解析成平安银行。
2. **日线 MA**：任取 3 个交易日，6 指数 MA5/10/20/60 与 Tony 软件读数逐一比对，误差 ≤0.01 点。
3. ~~15m K 线~~、4. ~~DK~~：已取消。
5. **成交额**：10 个交易日口径 A 与交易所官方公布的沪深两市成交额比对，误差 ≤1%；口径 A/B 差异报告。
6. **宽度/情绪**：完整率报告；与 HiThink 池计数核对。
7. **负例**：缺一根 5m、重复 bar、未来时间戳、节假日、半日市、ST 混入、北交所混入、prev_close 不连续、当周未结束当成完整周——全部必须阻断或标记。
8. **性能**：A4 单轮 p95 在接入前后无回退（对照 LJ-084 修复后基线）。

---

## A9. Codex 易错清单（逐条自查后才可提交）

1. 指数走个股 `map_symbol` → 000001 变平安银行。
2. 指数调用个股端点或传 `adjust=forward`。
3. 盘中把未收盘的日线并入 MA 当收盘信号。
4. 09:30 集合竞价 5m 被当成一根普通 K 线。
5. 15m 缺一根 5m 时用两根凑。
6. 用 Tencent 分钟 `amount`（OHLC 估算）当成交额。
7. 成交额历史用口径 A、当日用口径 B 拼量比。
8. 盘中线性外推全天成交额。
9. DK 公式含 ZIG/BACKSET 等未来函数而未报告。
10. 当周未结束的周线当完整周算。
11. 用"工作日"代替交易日历。
12. 单位混用：手/股、元/万元。
13. 宽度与情绪用不同 universe。
14. 数据缺失时沿用昨日状态。
15. 把 M0 取数放进 A4 主循环或收盘链 90 分钟预算。
