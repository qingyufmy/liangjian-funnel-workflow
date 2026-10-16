# 晨审只读绑定、信封刷新与分钟采集检查点

实际采集：2026-10-17 00:05 Asia/Shanghai。源码 `604ecd6b2993a43741ba049102990969d2471772`；隔离开发，未推送、未部署。

## 已落地

1. `read_execution_scope` 用一次 SQLite `mode=ro/query_only/BEGIN/ROLLBACK` 读取当前计划与历史持仓归属。独立核对晨会原行 hash；允许晨审修改 status/valid_from，但 payload_json/symbol/数据库 expiry 必须仍绑定原计划。待晨审、未来激活、过期和晨会之外的计划都不获得新开仓权限；历史失效持仓保留归属，只用于退出，不激活。
2. `refresh_execution_envelopes` 从当前行情/账户参数重新调用真实引擎计算 EXECUTION 上限，再与原 A6 晨会 PLANNING 上限取交集。不会将 PLANNING 改名充作可执行信封；保留暂停、PROBE、批次与股数/风险额上限。后续一次 A6 失败收紧到 PROBE，两次失败保持 HALT。缺晨会审查的旧持仓不能因此获得新槽位。
3. `collect_monitor_packet` 接受现有行情适配器回调，复用 `FetchResult/QuoteResult`、报价新鲜度校验与 `confirm_publications(closed_window=True)`。所有股票共享调用者全局截止与原 30s 采集预算；初始采集 gate 为 8，发布重试仍使用既有独立 gate（亦为 8），两者不是合计 8。挂起工作继续占有各自槽位，不增设无限线程。包在独立 state/team_v2/minutes 以 O_EXCL 封存，同分钟重启直接重用，不重取；篡改或范围变化明确阻断。午休/首根未闭合不发起采集；报价失败不抹掉已获取的闭合分钟；跨分钟或未来报价标不足。
4. 新隔离集成反例实际贯穿：本地原表只读 → 晨会 engine/A6/PM → 正常激活 → 执行上限刷新 → 真实 A4 client/严格 schema → broker 锁内裁决 → 本地模拟委托。HTTP 是替身，不是真实付费调用；行情/可交易性是显式本地夹具。原计划库字节不变。不能将此称为生产自然运行。

## 原始回执（均在 artifacts/team-v2-20261016）

| 前缀 | 结果 | receipt SHA256 |
|---|---|---|
| 20261017T000000792500-execution-source-envelope-refresh-final | 54 passed，退出 0 | 7131cc0cab2bc96798f45046f5d475560e2639a256c9d8cdeef2468e25f9ef59 |
| 20261017T000426150584-actual-source-packet-boundaries | 40 passed，退出 0 | 421b8b55b65adf9e42fd16166f28fee779320c911782bc7641a8e0e55c13b7e1 |

两组有重叠，不相加，不是最终全量。235601 的 27 项、235826 的 39 项和 235920 单条集成原件保留，不重复计数；本轮没有复跑 H-1 或先前已证回放。

## 真实未完项

- 生产 Workflow/CLI 的单一 CONTROL 分流尚未接；不能在旧 lane 已经写单之后再追加交易员。现在只有只读投影、采集函数和隔离协调层通过，不称 live 接线完成。
- 规则/身份适配仍待 0238 口径确认；源包 CAPTURED 仅代表采集，不授予可交易证明。`tradability_authority=UNPROVEN_RULE_ADAPTER_PENDING` 明确保留。
- 0240 的 Tencent estimated/VWAP 口径仍待答。reported 路径继续，估算不能顶严格撮合门。
- 六日正式回放、三日真实模型干跑、完整 A1/A2/A3/A5 接线、A6 异常、G-3/G-4、最终全量/VM/回退/候选发布均未完成。

四层：CODE 本地关联通过；REPLAY 只有合成隔离反例；OPERATIONS 未部署/无自然证据；STRATEGY 无收益结论。未触生产业务库、客户通道、secret；H-1 保持周日白天独立发布安排。下一步完成 source→单一 CONTROL 入口，不等局部问题而停掉其他独立任务。

## 00:07 增补（不覆盖桥接 0241 原件）

源码推进至 `9d70a2d18dacb1efe3d7eac072a835e993927b6d`：包内另保留完整只读 scope 原行、原缺项原因和 producer SHA，不能只留下一个事后无原件可重算的 hash。有效股票能采集时，另一计划的缺项仍使总体标 DATA_LIMITED，不静默抹掉。新增 1 条反例、相关采集文件共 9 项通过，退出 0；前述 54/40 原件不冒充新源码全量。

最新 receipt `20261017T000712440566-source-packet-original-scope-final-receipt.json`，SHA256 `163cbe169e889d2def72d6f2abafd8f88dbd426d0fc1dfac3443a10e6ad9246e`。其余未变源码不重跑。仍不是 Workflow/CLI 接线完成或发布批准。
