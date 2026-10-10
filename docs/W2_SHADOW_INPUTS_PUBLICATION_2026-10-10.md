# W2 附加日线观察字段（2026-10-10）

本切片仅实现周末任务书 §1.3 的 `published payload.strategy_facts.shadow_inputs`。不实现 W2 的竞价身份封存/模拟成交，也不证明 W1 六日黄金或自然盘中通过。来源是当前本地 `LocalFactCache`，没有网络补采、历史计划回填或生产部署。

## 实际接线与字段不变边界

`WorkflowApplication._publish_plans` 原全部资格、权限、价格几何、父计划收紧、有效期判定之后，已有 `batch` 排序/原子发布之前调用 `shadow_inputs_for_publication`。仅复制 payload 与 strategy_facts 容器并增加 `shadow_inputs`，原模型 A3 audit、snapshot、原计划字段不改。源失败不改变 created/blocked 或激活规则。原 strategy_facts 非 Mapping 时原样保留，不为插入新字段覆盖旧值。

`FACTOR_SNAPSHOT[symbol].as_of` 是实际查询接收证据上限，且必须不晚于原 snapshot_manifest.as_of 或本次 observed_at；factor 和 latest 的 symbol 必须匹配。原 `timeframes.daily.latest.end` 是 bar cutoff，查询 `adjust=none, end=cutoff+1µs, as_of=source_as_of, descending=True, limit=800`（既有 end 排他、与 A3 本地加载窗口一致）。由实际 trading_calendar 向前取得目标日之前最新四个交易日，不排除 T 收盘。缓存缺失/异常、factor 缺失/时间冲突均 DATA_LIMITED，不从其他 mutable 源猜时刻。

纯 builder 输入为实际 LocalFactCache envelope：symbol/timestamp/adjust/fetched_at/content_hash/payload。content_hash 按实际 collector→data_sync→cache lane 的 canonical payload hash 验证，不误当整个 envelope 哈希。外层及 payload 股票、raw none 口径、实际 fetched_at、闭合日期、重复、geometry 与最新四日完整性都先验，再复用 `pipeline.factors._daily_bars` 的正常 OHLCVBar 构造。该 parser 本身会静默跳过未来/未闭合行且不校验 payload 身份，所以这里的额外保护不可省略。

MA5 是最后五根原 raw close 均值；ATR14 是十五根起始 bars 形成十四根 TR 的均值种子，后续 `(13*ATR+TR)/14`。`version=raw-daily-ma5-wilder-tr14/1`、`atr_method=WILDER_TR14_SEED_MEAN`。最长800根实际返回历史的 seed_start 明确留证，**不是证券全生命周期 Wilder 种子，也不是 qfq 指标**。不调整量额，不改执行价格。生产 `_daily_context(plan).ma5` 必须有限正数且与重算值以 1e-12 容差一致；已有 atr14 也必须一致，否则 DATA_LIMITED、不改原指标。缺项关键字段为 null，不填0/1。

`source_ref` 仅保留原 envelope 的 canonical rows hash、数量、symbol、实际 as_of、cutoff、target、adjust、algorithm、seed_start；`atr_source_hash` 再绑定这整个引用对象。发布不重复原行情 payload 或任何模型文本。哈希是 canonical 对象绑定，不是原 SQLite 文件字节哈希。本切片不承诺 raw 历史全交易日完整性或公司行动因子一致，缺 qfq 来源不得称 technical qfq。

W1 已确认消费 `a4-shadow-inputs/1` 的 MA5/ATR/四个close/date、aware daily_as_of、64位 lowercase atr_source_hash，并核对原 MA5/已有 atr14。LEADER_INTRADAY 不适用，附 NOT_APPLICABLE gap 不查缓存。A3/A2 在发布之前运行，不接触新增字段；实际 `_a4_prompt_plan` 白名单也不含 strategy_facts，测试确认 A4 prompt 投影不变。

## 明确未闭合边界

原 W1 拒绝 `daily_as_of.date >= target_trade_date`，root已确认这是 consumer 过窄边界，将独立修复并补反例。本producer最终支持目标日晨间的真实 factor.as_of：`daily_as_of_semantics=SOURCE_OBSERVATION_AS_OF`，额外 `last_closed_daily_bar_end` 保留前日实际15:00 cutoff，绝不把07:00源时刻伪改前日。实际 adapter 仍严格 source_as_of ≤ manifest.as_of、observed_at；所有 close 日期及 cutoff 严格早于目标日。producer AVAILABLE 不代表尚未修正的 consumer 已消费通过，该 consumer 修复不在本切片。

历史六日94计划不回填；没有真实历史 ATR/四日前收来源，黄金仍由 W1 报告 DATA_LIMITED/exit2，不以本地 fixture 补证。没有运行模型/通知/网络/生产库/VM，没有提交或部署。新测试使用纯 Fixture store/cache；相邻原 publisher 回归仅使用既有 tmp_path SQLite，非生产数据。

## 反例与真实回执

新模块缺失的红测试：19 failed / 1 passed，exit1，保留 `artifacts/wp5-20261010/w2-shadow-inputs-before.xml`。首实现20 passed，exit0；补投影/源时间等42 passed / 17 deselected，exit0。v1最终54 passed后，新增实际晨间as_of合同反例1 failed/exit1（`w2-shadow-inputs-morning-before.xml`），再最小修复来源语义/时间界限。最终命令：

```powershell
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -m pytest tests/test_w2_shadow_inputs_publication.py tests/test_pipeline_factors.py tests/test_workflow_integration.py -k 'shadow or publication or recovery or publish or factor or prompt_plan' --junitxml=artifacts/wp5-20261010/w2-shadow-inputs-final-v2.xml
```

最终54 passed / 16 deselected，exit0，2.94s。36个新测试实例包括未来/错股/重复/未闭合/不足15/geometry/NaN、接收时刻越界、错误hash/adjust、近四日缺日、原MA5/ATR冲突、无缓存/异常、不合格计划不读源、原输入不变、源非价格字段变化导致hash变化、实际原发布字段相等和 A4 prompt 不变。`git diff --check -- src/liangjian_funnel/workflow.py` exit0。未跑全量。

四层：CODE=最小接线实现；TEST=局部fixture/相邻publisher通过；OPERATIONS=未部署且未自然盘中测量；STRATEGY=原执行/资格/阈值不变，无收益或黄金通过声明。

冻结来源 HEAD `fd0e872f1a018f570c3800654b2d318428ab9976`，工作区有 root 并行变更；本回执仅覆盖以上源与命令，不替代 root 新 HEAD 全量。

| 文件 | SHA256 |
| --- | --- |
| runtime/shadow_inputs.py | 1721f0535cb4e4dbdd89d4c98da47b3c5fa791ebc723be3b636c74aed8cc9b87 |
| test_w2_shadow_inputs_publication.py | e72fa78a613c9fdb4da57c180083c6970e3cc928cd94eed2c695bb3cb2d69408 |
| workflow.py（含已交付W4边界） | 7bf62d19640973f102830f612be2e1dea05812dfaf4d7359b4a4605b1bb30c09 |
| before.xml | 7023c6a048e16401604ba012b98c1f83185c6bb88ae1459d23d5832b8a08783f |
| final-v2.xml | aac57dbc069e097200c5ed81117fdc628d7d45143ddcfed10091f3aa5569d740 |
