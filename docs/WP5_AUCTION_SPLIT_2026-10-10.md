# WP5 任务 3：慢包/快段独立影子合同（2026-10-10）

## 结论

IMPLEMENTATION_PARTIAL：本轮有可运行的显式本地文件 CLI、慢包原子独占归档/哈希验证、快段预检和 fail-closed 反例；没有完整 22:00 慢采 adapter、07:00 在线 fast adapter、真实 prior-A3/持仓导出 adapter 或生产 scheduler 接线。不能把人工合同 READY 当任务 3、OPERATIONS 或真实 source READY。

代码仅新增 `runtime/auction_preparation.py`、`scripts/audit_auction_fast_preflight.py`、`tests/test_wp5_auction_preparation.py`。不读取 Settings/RuntimeStore/生产库、不发网络请求/模型/通知、不改 env/候选数/阈值/公告门，不接 22:00/07:00 或 09:26 调度；没有 commit/push。root 接手的 corporate_actions 模块与共享 workflow/state 保持不动。

已完整读取 `D:/dev_A股/liangjian_a4_20260923/.claude_bridge/outbox/0004-reply.md`（SHA256 `0d004f2e6f19e913e803f478060253ea2466ce646fe45619995342e14f11955f`）。本轮 CODE：55 项新合同测试和 20 项既有 disclosure_incremental 回归共 75 passed，exit=0；diff --check exit=0。REPLAY 仍缺 10-09（0）和 9-30（47）的历史晨审时点 PENDING/持仓证据及对应真实慢快输入，未改最终状态来构造真实 replay。OPERATIONS 连续 5 个交易日 07:00 <20 分钟且 READY 未验证；22:00 接线仍在 G7 单独授权边界。STRATEGY 不适用。

root 导出的 `artifacts/wp5-20261010/auction-plan-evidence-observed-20261009.json` 已只读核对（文件 SHA256 `47b38d68fc5d36bb0a478a57456ee67125161ffe9ac500266ddfc371dbd04799`，原只读 DB SHA256 `42d2854bd28fa64ade8ae02f89f6cd622bddaab2bf3d75723f07ee402f0d62e4`，source_unchanged=True）。9-30 创建 47 行，当前 31 EXPIRED、16 INVALIDATED；当前 PENDING=0，10-09 创建=0，观察到正持仓=0。它明确 historical_pending_state_proven=False/historical_positions_proven=False，explicit_target_trade_date 缺失，expires_at 是 10-08；部分 payload plan_expiry 仍写 9-30，不能猜补目标日。测试验证 EXPIRED/INVALIDATED 不进晨审域，但这不是“9-30 有 47 PENDING”的真实历史快段回放，更不是完整 REAL_READY。

## 慢包

`build_slow_bundle` 要求显式 input_root、generation_id、trade_date、created_at、完整 scope、六类组件引用与 snapshot_reference。六类固定为 FINANCIALS、MEMBERSHIPS（行业/概念成员图）、GOV_POLICY_90D、BUSINESS_REPORTS、BUSINESS_PDFS、DEFERRED_QUEUE。这里只验证封存的来源合同，不实施财务/成员图/GovPolicy/全范围报告及 PDF 的夜间采集。

每组件只保留 path、文件字节 sha256、source_id、真实 source_fetched_at、complete、scope、window_from/window_through；不复制 raw 大 payload。source time 必须显式带时区、不能晚于慢包封存时间；不使用文件 mtime、当前时间补填或重标源时间。complete=True、全部 scope 相同、窗口终点=代际交易日；Gov90 窗口至少覆盖该日前 90 日。source 时间/完整性/组件语义是调用者应从真实源 receipt 提供的断言，这个本地模块无法认证空文件是否真的含有完整财务或 PDF；该缺口是 IMPLEMENTATION_PARTIAL 的原因，不能以 manifest 完备替代采集完备。

snapshot_reference 含相对路径、字节 sha256、snapshot_id、snapshot_hash；复用实际 `pipeline.snapshot.FrozenInputSnapshot.model_validate_json` 的内部 canonical content hash 验证，核对身份、as_of 与完整 universe scope；snapshot as_of 日期必须等于代际日期且不晚于 created_at，旧代际快照不能裹新时间。没有使用“自动寻找最新快照”来代替明确输入，也没有拷贝快照大 payload 到慢包。

慢 manifest 内部 canonical SHA 绑定代际、scope hash、所有组件 metadata/hash 与 snapshot 引用；`write_slow_bundle` 以 xb 独占创建，不覆盖已有同路径，返回完整文件字节 SHA。快段要求单独 expected_slow_sha256 pin；即使攻击者改 metadata 并重新计算内部 hash，仍不能绕过原外部 pin。组件读取时以 streaming file_digest 复核字节 hash。所有引用必须相对 input_root，resolve 后禁止越界和 symlink 逃逸。

这里“不变”是内容/身份哈希绑定与不覆盖归档，不是 OS 权限锁或其他进程无法修改文件的承诺。后续篡改会在读取验证中阻断；本轮不改 ACL 或生产文件。

## 快段与有界 delta

`fast_preflight` 仅消费本地显式文件/结果；fast_inputs 的键严格为 PREVIOUS_MARKET_FINAL、NEWS、MACRO。前日行情明确 finalized=True 且 trade_date 为显式交易日历的前一交易日；资讯/宏观绑定目标日。输入完整性、源时间不得晚于 now、源采集日期不得早于所绑定的行情/资讯/宏观窗口日期，字节 hash 必须匹配；不能把昨日 NEWS 采集时间保留却重标今日快段。禁止在快段输入财务/PDF 等慢采角色，也没有任何网络慢采 fallback 参数或执行入口。缺慢包返回 SLOW_BUNDLE_MISSING、research_index_allowed=False、delta_count=0。

代际滞后按显式排序唯一交易日历计算，不用自然日或周一至周五猜交易日；慢包须为前一交易日或更早，当前日/未来代际不能伪装夜包。落后目标日 >1 个交易日为 SLOW_EVIDENCE_STALE，可保留 research_index_allowed=True，但 eligibility_released 恒 False、publish_plans 恒 False。资料损坏不是 stale fallback，直接 BLOCKED。

晨审域来自调用者冻结导出：

- plan_publication：complete=True、source_sha256、previous_close_trade_date 与日历前交易日一致；只取 status=PENDING_MORNING_REVIEW、target_trade_date=目标日、a3_published=True、published_trade_date=前交易日的股票。
- positions：complete=True、source_sha256、带时区且不晚于 now 的 as_of；只取有限正 total_qty 的股票（total_qty=0 不进入，bool/负值等非法量不能当空持仓）。

scope 是两者去重并集，不对“通常 <50”设硬阈值或改候选数。晨审域股票（包括正持仓）不在慢包范围时仍保留 delta 风险审查，另列 stock_slow_gaps 与 SLOW_EVIDENCE_SCOPE_GAP，不扩研究候选或解除资格阻断；同股票 delta 失败优先保留 RECENT_DISCLOSURE_GAP，慢资料缺口列表不丢。前述 source_sha256 与状态是导出方提供的来源声明；默认 CLI 记录整个 request 的实际字节 hash，但没有从 RuntimeStore/生产 SQLite 认证计划发布状态。未提供真实导出时，本地人工输入不能被当正式晨审授权。

delta_results 只能有并集内的键；任何越界结果立即 DELTA_SCOPE_OUT_OF_BOUNDS，不默默丢弃越界股票。并集内缺结果/失败/不完整/错误 symbol/旧 end_date/未来源时间/过滤 keyword/STALE_VERIFIED_FALLBACK 均逐股 RECENT_DISCLOSURE_GAP。

复用 CninfoFetchResult.model_validate 与 disclosure_incremental.covers_query：近期查询 start=今日减 10 日、end=今日、keyword=""；源 fetched_at 距 now 必须在 [0,6h]（精确 6h 允许，超过 1 秒 GAP），不放宽既有 6 小时 TTL。另核对 total 与唯一记录数、sec_code、发布时间/查询窗口。报告仅留结果 hash、固定状态，不重复公告正文或任意原消息。慢包 stale 与 delta gap 同时存在时，顶层 SLOW_EVIDENCE_STALE，失败股票仍保留 RECENT_DISCLOSURE_GAP，不以慢包日期重标。

此 CLI 仅审计已获取 delta 的契约；真正 delta 网络访问 adapter 尚缺，未来若接入，只能针对此晨审域，不能重新全范围公告查询。09:26 原失败关闭门未被更改；report 中没有“放行资格”的真值。

## 预算与退出码

budget_seconds 为 request 显式给值，必须有限、正且 <=1200（0004 的 20 分钟边界）；计时为本次 monotonic wall，结束时超预算/时钟回拨为 FAST_BUDGET_EXCEEDED。该预算只服务影子审计，不读/改 Node timeout、parent close deadline 或生产 job 限时。

这是本地同步文件审计的预算判定，不是可以抢占阻塞文件 IO 的硬截止调度器；在线 bounded work/限流/重试还需后续正式 adapter 接入，不能凭几秒 fixture 声称线上 20 分钟达标。

默认 CLI：

    D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -B scripts/audit_auction_fast_preflight.py --input-root D:/explicit/local-export --request request.json --output D:/explicit/isolated/receipt.json

request.json 必须恰好包含 expected_slow_sha256、expected_scope、now（带时区 ISO）、trade_calendar、plan_publication、positions、fast_inputs、delta_results、budget_seconds、slow_bundle_path（相对 input_root）。schema 对应上述合同；CNINFO 结果使用 CninfoFetchResult.model_dump(mode="json") 既有字段。

request 只读一次原字节并留 hash，避免结束后重读变化文件而错绑输入；组件/snapshot 引用不能逃逸 input_root。输出路径明确且独占创建，既存输出/覆盖竞争返回 REFUSE_OVERWRITE，不安装依赖或建隐式目录。所有固定 safe error 不带原路径/异常/秘密；stdout 仅合同状态、部分实现、规模、elapsed 和输出 hash。

exit=2：输出成功（包括本地 READY/STALE/DEGRADED/BLOCKED），整体仍 IMPLEMENTATION_PARTIAL；不能用退出 0 冒称完整任务通过。exit=3：CLI 请求/路径/输出失败。没有正式生产 READY/exit0 路径。

## 验证证据

先行新增 tests 后 exit=1：auction-preparation-before.xml 为 ModuleNotFoundError 收集失败，不冒称所有反例逐项红。v1 为 26 passed/1 failed：测试误把 source_id/generation_id 的 fixture 标签当 raw payload；修正为禁止 raw 的 fixture 字段键，非实现绕过。

    D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -B -m pytest tests/test_wp5_auction_preparation.py tests/test_disclosure_incremental.py --junitxml=artifacts/wp5-20261010/auction-preparation-local-final-v3.xml
    git diff --check

75 passed in 2.68s；两条命令 exit=0。反例覆盖组件字节篡改、内部/外部 hash 重标、invalid/partial scope/snapshot/越界引用、独占覆盖、缺慢包、节假日交易日 stale、旧 snapshot 代际重标、范围外持仓缺慢资料、越界 delta、逐股 6h/today/完整近期窗口门、stale 不遮蔽 delta gap、坏源时间/Gov90/完整性、前日行情定稿、最终计划状态不能改 PENDING、非法计划/持仓、预算和实际 CLI 独占输出。没有生产网络慢采回退、调度重启、模型或通知。
