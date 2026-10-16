# Team V2 交易员隔离执行链阶段回执

实际记录：2026-10-16 21:05 Asia/Shanghai。源码 HEAD `5bd8d2f13ee25e2172496e25fdd96aa9530a3f42`。

## 已接通的路径

`TeamPromptRepository` 原字节 3.0.2 提示词 → 既有 `OpenAICompatibleModelClient` → A4 严格 schema → 独立 model_calls/model_judgments → 最新账户/挂单/当日风险基准 → 真实 guard/risk_engine → 原子 paper admission/订单/现金预占 → 下一根真实撮合代码。

本地 HTTP 使用 MockTransport；没有调用真实供应商、生产模型或行情，也没有修改生产。

- 会话先绑定 decision_id、call_id、快照、计划/信封、账户库身份及配置；捕获响应后、订单提交前或提交后崩溃均可续接，既不重调模型也不重复订单。未知在途调用不伪造降级成功。
- 非法输出/网络失败为 TRADER_DEGRADED_NO_ACTION，原非法 JSON 保留，不从模型答案修成 BUY。模型完成跨分钟时旧报价不下单。
- PROBE/批次提示只能压低既有风险信封；每股实际数量仍由当前账户和风险引擎计算。新增开仓、簇计数、挂单冻结在事务内读取，不因进程重启清零。
- 日内亏损基准盘前只封存一次；不能盘中用初始资金顶替。日亏 HALT 当日持续；回撤 HALT 跨日及重启持续。当前持仓缺分钟估值时拒绝新开仓。
- 持仓退出绑定原 plan_id，遵守 T+1，不因旧计划到期而拒绝合法减仓。
- CANCEL_SIGNAL 只撤销精确绑定该计划的当日未完成 BUY，不撤其他计划或保护性 SELL。撤单和审计同事务；record 不撤单、不下单。
- 不写 execution_plans，不连接旧 RuntimeStore，不触客户卡片及通道。

## 测试原件（均相对本工作树）

| 原件前缀（artifacts/team-v2-20261016/） | 实际结果 |
| --- | --- |
| 20261016T205654961049-trader-session-red | exit 2，缺新模块的采集失败，不冒称业务反例失败 |
| 20261016T205854801129-trader-session-associated | 79 pass / 0 fail / exit 0 |
| 20261016T210104063473-trader-execution-team-regression | 353 pass / 0 fail / exit 0，HEAD bf08513；不是最终整项目全量 |
| 20261016T210221088109-trader-cancel-red | 两条撤单反例真实失败 / exit 1 |
| 20261016T210314073208-trader-cancel-atomic-final | 撤单影响切片 45 pass / 0 fail / exit 0 |
| 20261016T210336223443-trader-cancel-protective-exit | 撤单与 T+1 切片 5 pass / 0 fail / exit 0 |
| 20261016T210424569309-trader-time-rule-preservation | 最新会话切片 11 pass / 0 fail / exit 0 |

这些切片重叠，不相加；提交只改变 Git 元数据，回执中源码 SHA 对应上述实际内容。353 回执 SHA `a095ba9b148fe74163b39fd04bc75f91d1ebf97e2b8536a8dc5f413b680705d3`；45 回执 SHA `590060bad8f1aaed5e56760fd32261aeabdecab2215839e482ed66c7426bbad2`。初轮时钟/原因优先级夹具失败均保留，不据此放宽护栏。

## 四层结论及下一步

CODE：本阶段所列隔离执行链已通过，非最终 Team 全量。REPLAY：合成 HTTP/纸面账户故障注入成立；六日正式回放、三日真实模型干跑未完成，真实供应商调用 0。OPERATIONS：尚无 Node/CLI/source 自然接线或生产验收，生产未动。STRATEGY：收益、合法率 ≥99% 等发布门未证。

H-1 f79 最终包 0224 待实际复审，不重测未变项。接下来 Node/源接线、G5、正式回放和后续工作按 0222 顺序推进；风险晨会 wall-clock 与 120 秒父预算存在冲突，另发 question，不自行改时间/预算。
