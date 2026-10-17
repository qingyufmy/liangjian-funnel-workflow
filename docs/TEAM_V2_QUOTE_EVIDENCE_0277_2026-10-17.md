# 0277 A/C：回放报价隔离与完整原报价归档

实际验证：2026-10-17 08:36–08:42 Asia/Shanghai；仅本机隔离测试，未部署或读取新增行情。

`ReplayBarDerivedQuote` 是与生产 `MarketQuote` 不同的冻结类型：last=原 1m close、quote_time=原 bar_end、bid/ask=NULL，固定 `REPLAY_BAR_DERIVED` 身份并绑定原 bar hash。未收盘／未来 bar 拒绝。生产采集器拒绝此保留来源身份，不能通过序列化成普通 MarketQuote 洗掉标记；原拒绝反例在修改前实际失败，修改后通过。

自然 CONTROL 在使用报价前，将完整原 typed MarketQuote 以及 QuoteResult 的来源、provider time、接收时间、原尝试元数据、packet/scope hash 写入独立 `state/team_v2/market_quotes.sqlite3`。原对象 JSON 与归档正文分别 hash；唯一身份为 packet+symbol，重试逐字节对照，不重复写行。UPDATE/DELETE 有 SQLite 触发器拒绝，读取者使用 mode=ro。已有外来库、篡改 packet、回放来源、路径越界均不接收。

这里的“完整”指当前 MarketQuote 类型的全部原字段，不声称拥有原 HTTP 报文或该类型从未包含的盘口字段。报价缺失保留 UNAVAILABLE，不填造。归档失败则当前分钟不使用该报价授予新入场权限；仍允许独立闭合 bar 的既有保护路径继续，不调用旧引擎作暗中降级，也不影响客户通道。

初轮关联 72 项通过；补充归档失败隔离、回放字段约束后的最终边界 30 项通过，各轮重叠不相加。最后回执位于 `artifacts/team-v2-20261016/20261017T083719851300-quote-archive-replay-boundary-green-receipt.json`（72 项）和 `*quote-archive-final-boundaries-receipt.json`（30 项），不是最终 HEAD 全量或 VM 证据。

CODE 仅该 A/C 子项通过。六日正确性回放／合法率／护栏越权统计／两次账户逐字节对照尚未执行。0277 B 的周一首段 PROBE、日新开≤2、报价新鲜度阻断率 >20%／护栏异常触发持久 record 回退和内部告警尚待接线，不可提前宣称整门完成。emotion 原四量缺失仍 DATA_BLOCK、LEADER 回放关闭；禁止从该回放宣称收益、胜率。
