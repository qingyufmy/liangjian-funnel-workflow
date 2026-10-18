# 10-16 原始回放源补充及五日行情可用性

实际采集 2026-10-17 07:38–07:42 Asia/Shanghai。SSH 严格原公钥、192.168.0.254、主机 debian、新生产路径均核实；生产仍 `2b077f3`。复制前确认无活动自然任务、可用内存 ≥800MiB。只用 Settings 解析路径、SQLite mode=ro/query_only/读事务及原始文件读取；没有 RuntimeStore/WorkflowApplication、模型或行情采集、服务操作、业务写入。

首次元数据遍历达 15000 项上限，不能据此声称全盘不存在其他归档。随后对已知市场生产目录做精确范围检查；cached_results 命名空间仅见公告、公告组织、PDF、新闻，不是历史报价。没有读取 env 或凭证。

## 新原件

`artifacts/team-v2-20261016/source-1016-original-20261017T0740/`

- `source.jsonl.gz`：11,588,242B，SHA `2168d8bd09f3f22353fa5b9430935a73b9ea103a787c8c566aee453c81960ed0`。
- `frozen.sqlite3`：39,698,432B，SHA `6f1bb2e2a8371c5a65123d8a842f5b2adb7d15508451da39a613acf7457f88cb`。
- 本次读取不是跨三库的同一事务；各库独立只读事务，原 captured_at 保留。不得把复制时刻当成历史分钟到达时刻。
- 原有失败、生命周期、通知原行均只复制，不修复、不激活、不回填。

| 原始表 | 10-16 复制行数 |
|---|---:|
| execution_plans | 14 |
| monitor_events | 1523 |
| a4_signal_lifecycles | 1 |
| astock_outcome_labels | 6886 |
| notification_deliveries | 16 |
| minute_decision_snapshots | 3107 |
| minute_bars / minute_bar_versions | 各 4032 |
| daily_bars（相关股票 10-08～10-16） | 252 |

本机逐表、逐原行比对 gzip 原件与 SQLite 副本，原始压缩二进制也按原字节比较；249 个文件逐字节与归档比较，3107 份快照解压后 hash 全部匹配，内部共 372840 根累积窗口 bar。审计前后副本 SHA 不变。

12 条计划仍各只有 107 个事件分钟，华鑫另有持仓风险事件而共 238 分钟；不能误称其余股票全天覆盖。华鑫原生命周期仍为 UNFILLED / ENTRY_NEXT_BAR_MISSED，原有 BUY_SIGNAL 1 条，维持 H-1 事故原件，不重新解释为无买点。

## 数据缺口（不是回放通过）

10-12/13/14/15/16 市场摘要分别 49/50/50/50/50 份，共 249 份，源状态均 READY。但这些只是广度摘要，不含涨停数、跌停数、炸板率、最高连板四项及所需原始全市场身份范围。五天的 emotion_state 历史回放均应列 DATA_BLOCK，不造开关序列。

10-16 事件里有 1391 个原 reference_price 标量，完整报价对象为 0；旧六日包的对应完整报价对象也未证实。标量、原始 bar、quote_complete 标记及传输时间元数据都不是完整 MarketQuote，不能拼造当时的昨收、开盘、成交额或到达时间来通过入场护栏。正式 CONTROL 回放仍待输入适配及源缺口裁定，不能拿 B3 的 21285 个窗口零差异顶替。

本机核验回执 `20261017T074233070522-supplement-1016-row-integrity-receipt.json`，SHA `f6072f26e8c30da5babdc79ae527756c8939dad31e9f143da40e63353a3822fc`，位于 `artifacts/team-v2-20261016/`。CODE 为只读封存／核验完成；REPLAY 未过；OPERATIONS 无新部署；STRATEGY 无新增收益证据。
