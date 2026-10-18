# 0242 双撮合基准切片

实际采集时间：2026-10-17 00:51 Asia/Shanghai。源码 HEAD `c7e941af6c3120320ce676f1e62062216ee00122`，直接接在 CONTROL 入口切片之后。依照已全文读取的 `0242-reply`，不是新增策略提案。

## 已实现

- reported 分钟仍以 amount / shares 的真实 VWAP 撮合，`fill_basis=REPORTED_VWAP`。
- ohlc_estimate / legacy_unspecified 成交额不转成 VWAP。纸上分钟的 vwap 必须为 None，`fill_basis=OHLC_ESTIMATED_PAPER_FILL`；BUY=(high+close)/2，SELL=(low+close)/2。悲观价超过委托限价时不撮合，不通过截成更优限价捏造可成交。
- 原参与率、真实 shares、整手、T+1、追价、涨跌停和停牌证明不变。缺 volume 或规则仍不能成交。
- 同一 100 万账户的逐笔 fill 与 FILL 事件保存基准；独立 paper schema 增列，已有无基准成交保持 LEGACY_UNCLASSIFIED，不回填为 reported。生产库未访问。
- 日结输出 `fill_basis_report`：当日逐基准成交量/费用/现金流；全账户至当日的已实现 PnL 分 reported-only、estimated-only、MIXED_BASIS、legacy；未平仓按买入基准分列。真实买入/估算卖出不能混进 strict VWAP 绩效。
- 同一账户受共享现金/风险约束，未伪称两个独立注资净值曲线；胜率/收益率仍 NULL。A5 adapter 尚未接线，不能说 A5 已自然分列。

## 证据（原件位于 artifacts/team-v2-20261016）

- `20261017T004615793166-fill-basis-0242-red`：7 个新反例失败，exit 1。
- `20261017T004737346463-fill-basis-0242-first`：68 通过，exit 0。
- `20261017T004842677481-fill-basis-0242-associated`：126 通过，exit 0；覆盖撮合/结算/CONTROL/原子准入/真实客户端 HTTP mock/共享 H-1 合同。receipt SHA256 `fe82dfd3a2a5aaf09132c3e62761f1bafe7ceda107d423e209f91b8f37b985a8`。
- 最后补充费用余量分摊、累计口径标识和实际 CONTROL 估算源→后续闭合分钟成交后：`20261017T005018671978-fill-basis-0242-final-delta`，35 通过，exit 0。receipt SHA256 `940be4d98dbcb542b64e2d2120c9e9709a53201ab0af79a8c96dbce62ccab6df`。

126/35 重叠，不相加；回执为当时工作内容 SHA，不拿旧 git HEAD 充最终候选全量。

反例含：假 VWAP/缺 close、零量、越限价、涨停与规则未知、跨基准平仓、旧 schema 升级、部分平仓成本/费用与账户 PnL 对账、两个独立隔离库逐字节复现、实际 CONTROL 只调用一次模型并在未来闭合分钟成交。后者仍为源夹具和真实客户端 HTTP mock，不是供应商实网或六日原冻结回放。

## 四层与下一步

CODE：本切片绿。REPLAY：仅上述隔离可复现；六日正式曲线/分歧表未完成。OPERATIONS：未部署，未改变生产。STRATEGY：无胜率、收益有效性结论。

下一步核心为正式规则/PIT 与 G5 当桶 producer 接线；其后把日结基准分列交给 A5，继续其余角色与调度、三日真实干跑和正式发布门。外围扩张配置及最小既有供应商源仍排核心之后。H-1 周日白天独立发布不变；本切片不是 Team V2 最终候选发布申请。
