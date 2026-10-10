# W1 P0 盘前影子inputs与日期消费边界（0061 19:25）

## 1. 范围与结论

旧计划缺shadow_inputs时，新增显式盘前sidecar CLI：只读原execution_plans与LocalFactCache，将同一`raw-daily-ma5-wilder-tr14/1` builder结果写入独立SQLite `shadow_preopen_inputs`。原计划payload、hash、status不变，不激活计划，不采网络或构造RuntimeStore。已有任何PLAN_FROZEN inputs字段（包括DATA_LIMITED/null）不被sidecar覆盖。

默认PIT/成交腿仍UNWIRED。缺PIT的BUY/ADD继续只记DATA_LIMITED audit；新technical_trigger/technical_action保存真实研究结果，不能当可执行BUY、模拟成交或收益。此前0053全passive baseline与entry集合规则不变。

## 2. 时钟与builder

closed日期严格<T；SOURCE_OBSERVATION_AS_OF允许目标日晨间，source clock<=真实观察/生成clock，source clock<目标日09:30。显式last_closed_daily_bar_end必须<T且不晚于source clock。consumer只增加日期证据边界，不改数学、变体集、策略、线程或原resolver。当前副本此前已经接受晨间as_of，不声称又修复了一次原`date>=target`旧判断；本切片补09:30和closed/generated证据守卫。

日期判定统一用上海交易日，UTC输入不能把09:30界线变成UTC09:30。session传入原PLAN_FROZEN row.created_at或已验证sidecar.generated_at作为生成clock；仅是影子消费上下文，不回写字段，也不改变shadow_inputs原hash。源钟晚于这个生成clock仍拒绝。

sidecar生成必须在目标交易日09:00前，读完cache后取实际生成clock；跨09:00读取不准回拨时间。盘中消费的是此前合格盘前记录，不能解释为09:30后生成仍有效。源选择复用真正LocalFactCache.query_daily_bars（none、800、as_of、latest revision），但只读子类不调用其初始化/WAL写入；closed cutoff与四个前序日来自原ExchangeTradingCalendar，不用weekday近似。builder逐row验证symbol、adjust、fetched/time/hash、完整OHLC，原生产MA5及已有ATR14一致性不放宽。

## 3. 独立ledger合同

`PreopenInputsLedger(path)`构造不建schema；`record(record)`才写影子库，FULL同步、key=target_date:plan_id，相同原对象幂等、不同内容同key拒绝且不覆盖。`lookup(plan_row, *, target_trade_date, observed_at)`严格mode=ro，不初始化schema；查缺、hash/身份/目标日/生成clock冲突返回DATA_LIMITED/null inputs。

record=`shadow-preopen-inputs/1`，origin=`SHADOW_PREOPEN_SIDECAR`；记录original plan_id/lane/symbol/version/created_at、原payload UTF8字节SHA、生成clock、shadow_inputs、builder source_ref/atr_source_hash、整体canonical record_hash。实际cache适配还记录独立cache路径和查询边界；这些hash是本地完整性/引用，不声称来源认证。status/updated_at不被拿来造历史身份；生成允许ACTIVE_TODAY/PENDING_MORNING_REVIEW，真正entry仍由原session当前row+同分钟event/fence独立验证。sidecar失败保留每计划failure，不换源、不填历史。

## 4. session与CLI

`ReadOnlyShadowSource(..., preopen_inputs_provider=ledger.lookup)`是显式可选接口。source保留原plan给原基线/ledger，只有缺inputs且重新验真sidecar的entry item新增`shadow_research_plan`隔离副本，engine仅使用该copy。已有冻结PLAN_FROZEN保持原对象，不调用sidecar。lookup失败不改生产、不删除完整baseline，原engine对缺inputs输出DATA_LIMITED。

生成CLI：`run_shadow_preopen_sidecar.py --state-db <original> --fact-db <original> --preopen-inputs-db <independent> --receipt-json <unique-new> --target-trade-date YYYY-MM-DD --lane <lane>`。仅本地显式路径，scope上限1000，非交易日/无可用计划不得空窗PASS。实际runtime clock来自系统；测试main(clock=...)是明确固定fixture，不能当真实盘前运行。

消费CLI：既有`run_shadow_session.py`仅新增`--preopen-inputs-db <existing>`，纯lookup接线；源/输出/receipt/monitor路径别名拒绝，不自动寻找数据库/网络或生成sidecar。缺flag保持旧接口。

## 5. W3稳定字段与PIT区分

ledger signal追加`inputs_origin=PLAN_FROZEN|SHADOW_PREOPEN_SIDECAR|UNKNOWN`；`technical_trigger`仅同次isolated engine status=OK且action BUY/ADD时true；`technical_action`保存同次真实数学action，`technical_trigger_basis=ISOLATED_RESEARCH_ENGINE_NOT_PIT_OR_FILL`。缺PIT时实际ledger字段仍status=DATA_LIMITED、variant_action=null，evaluation_variant_action保留旧字段，并且不消费执行首次触发状态。W3只能按day/plan/variant/origin独立取最早技术触发研究projection，不能相信event_kind声明或把它混入PIT/成交统计。minute summary另列inputs_origin_by_plan，完整passive baseline不受隔离copy影响。

## 6. 验收与未完成边界

0063 followup：在隔离临时SQLite调用真实`WorkflowApplication.review_pending_morning`（workflow.py:5657）和`RuntimeStore.activate_pending_plan_batch`（state.py:2151）。PENDING→ACTIVE的原payload字节SHA前后均为`4c4f01bddd06c104c09a431e57b6aef53e286d8eaefe3fd073b60ca8c8a2ac33`，plan_version/created_at与区间/失效位/追高线/MA5不变，原sidecar消费AVAILABLE；几何改动或仅加空格仍拒绝。因此保留完整原字节绑定，不引入宽松lineage或只比plan_id。此证据是调用真实函数的fixture，不是生产19条晨审结果。

ledger小修仅保留明确`SIDECAR_REENTRY_CONFLICT`/`SIDECAR_RECORD_HASH_CONFLICT`错误码，并以`closing()`确保成功、幂等和冲突路径关闭连接；成功/幂等显式commit，冲突不提交且不覆盖原record。新`w1-sidecar-0063-followup`回执保留反例红/绿；原0061 freeze229不覆盖，shadow_variants f233及正式晨审/state源未改。

独立artifact `artifacts/wp5-20261010/w1-preopen-sidecar-0061`封存red/after/final XML及freeze清单。测试用真实临时SQLite、真实LocalFactCache envelopes/选择器、真实原builder；验证未来bar、MA5冲突、>=09:00/>=09:30晚生成、源钟未来、wrong target/plan/payload bytes/hash拒绝，原execution_plans行/整SQLite字节SHA/status一致，盘中源/闭合clock守卫及只读消费。session fixture证明只有engine copy附inputs，独立ledger仍收到原计划，原outer warmup不变，缺PIT技术BUY被研究audit保留但不是执行BUY。

没有复制/修改真实19条计划，没有生产运行、网络、模型、部署、自然日或收益证据；W3正式接线和统一新HEAD全量由root完成。原adc1de89 G-B封存仍绑定旧源码，本日期边界新SHA不得冒充已跑候选的证明；所有数学/策略/resolver字节仍未改。新版consumer需要独立全量回执，不覆盖旧freeze。
