# WP5 G3 本地复权证据合同（2026-10-10）

## 结论与范围

CODE：新增隔离模块 `src/liangjian_funnel/data/corporate_actions.py` 与 34 项合同测试；连同既有公司行动只读探测测试共 61 passed，exit=0。模块没有 IO、网络/凭证、RuntimeStore、模型、通知或状态写入，不修改 workflow/settings/config/daily 同步。未 commit/push。没有将该合同接入技术指标、A3 eligibility 或执行价格；现有生产行为不变。

主代理复核新增两项真实反例：正常日线追加不得改变因子版本；已核验的除权参考字段修订必须改变因子版本。旧实现实际运行为 2 failed / 34 passed（`corporate-factor-version-before.xml`，exit=1），最小修复将公司行动、因子与单次序列三个版本分离。最终本模块 36 项及既有探测 27 项共 **63 passed**（`corporate-factor-version-final.xml`，exit=0）。先前 34/61 为初版回执，不代表最新代码。新增版本合同仍未接入生产指标或同步 reset，不能据此宣称复权链路已完整验收。

复核另发现有限输入的极端相对误差可能转成 `inf`，破坏证据 JSON。新增反例旧代码 1 failed（`corporate-forward-extreme-before.xml`，exit=1），修复只把不可有限表达的误差标记为 `FORWARD_ERROR_NOT_FINITE` / GAP，不扩大 0.5% 比对容差。最终相称切片 **64 passed**（37 项因子合同 + 27 项既有探测，`corporate-factor-final-v4.xml`，exit=0）。

REPLAY：仅本地人工确定性夹具验证，不是实际日线逐根对照。OPERATIONS/LIVE：未新增真实源请求，没有完整窗口原事件/交易日/raw 前收/独立 forward 的真实链证据，不宣称 LIVE 或源 MATCH。未应用生产 technical/A3/pending reset。旧 raw/旧因子版本仍由调用者保管，本模块不覆盖任何文件。

依据是已完整读取 `D:/dev_A股/liangjian_a4_20260923/.claude_bridge/outbox/0003-reply.md`，该答复明确用比例法，取代旧歧义；本模块不承担答复内有关第三方实现方式的外部事实验证。

## 最小调用合同

`build_adjustment_evidence(symbol=..., raw_bars=..., events=..., coverage=...)` 只接收本地证据：

- raw_bars：同一 A 股 symbol、严格递增且唯一的 ISO date、有限正 OHLC、合法 OHLC 包络、非负 volume/amount、adjust_mode=raw/none。保留独立 raw_rows 副本及 raw_series_sha256。最多 10000 根。
- coverage：complete=True，symbol、from/through 必须等于 raw 窗口边界；trade_dates 必须等于全部日线日期；expected_event_dates 必须是窗口内排序唯一日期并与事件集合完全一致；evidence_sha256 必须是严格小写 64 位 SHA。该证书是调用者的本地声明，本模块不认证它是否真的来自完整官方查询。没有证书或漏事件时 GAP，空事件列表自身不能证明因子为 1。
- events：symbol/ex_date/previous_trade_date；preclose 必须精确等于覆盖日历中除权前一交易日 raw close；cash_paid/cash_reference、differential_distribution、bonus_total、rights_ratio/rights_price 都必须显式提供、数值有限非负，现金普通分红必须相等。配股必须是显式核验的零/零，或正比例/正价格，不默认补零。每事件必须提供非空 source_event 原 payload、source_event_sha256、reference_sha256、verification_scope=SAMPLE_VERIFIED 与逐字段 verified_fields（cash_reference、differential_distribution、bonus_total、rights_ratio、rights_price）。

原事件 SHA 的明确口径是原 payload 的 canonical JSON（sort_keys=True，紧凑分隔符，ensure_ascii=True，allow_nan=False），不是 HTTP 原始字节 SHA；不能直接冒用 probe 的 response_sha256。原 payload 哈希不证明规范化映射正确；字段核验与参考来源仍是调用者须提供的独立证书。本模块没有 Hithink 字段自动转换器，也不自行签发证书。reference_sha256 绑定调用者证据版本，不自行下载或核验 PDF。所有输出始终 live_verified=False。

## qfq-ratio/1

    R = (Cprev - D_eff + rights_ratio * rights_price) / (1 + bonus_total + rights_ratio)
    event_ratio = R / Cprev
    technical_price(t) = raw_price(t) * product(event_ratio for ex_date > t)

锚定覆盖窗口最新日线；当日事件只作用于该日前历史 OHLC。factor_chain 保留每事件 raw 前收、参考现金、参考价、比值、原事件 SHA 与规范化事件 SHA；factors 与 raw 日期逐根对应。比例计算使用 Decimal（40 位精度），公开数值为有限正 float；极端无效比值/历史乘积返回 GAP，不输出伪连续序列。

只调整 OHLC。volume/amount 原值不变，raw_rows 不变，execution_price_basis=RAW_ONLY。technical_rows 的 adjust_mode 为 qfq-local；不是生产数据源 adjust_mode 或执行报价。serial version 同时绑定合同版本、公式版本、raw hash、technical hash、规范化事件 hash 和 coverage hash；原事件版本变更通过规范化 hash 进入版本。

未知/不完整/冲突证据统一为 ADJUSTMENT_DATA_GAP（reasons 为固定码），不是投资 REJECT。technical_rows/factors 为空、technical hash/version 为 null，raw_rows/hash 保留；input_events_sha256 和逐个可哈希 original_payload_sha256s 即使 GAP 仍保留，用于反查输入而非认证通过。非有限原输入无法 canonical hash 时对应 hash 为 null，绝不伪造 hash。事件资料缺失不编造 event_ratio=1；只有显式完整无事件覆盖可以生成 1。

## 特殊字段与真实证据边界

300196：实派 0.2 与公告除权参考现金 0.1974617 不同。差异分红只能使用明确核验的 cash_reference；缺少它时 GAP，不能以实派近似。测试用确定性前收 100 验证该数值边界，不冒称真实前收。

688808：已有 probe 的 per_share_bonus=0.48 与公告转增 0.48 相符，只能用于精确核对的该样本，不证明全源字段等于送股+转增合计。该模块仅接受逐事件 SAMPLE_VERIFIED 的 bonus_total，不接受 GLOBAL_LIVE 声明，不自动把该字段映射为合计。当前参考文件的现金、配股等仍有未核实项，不能只凭 0.48 给整条真实事件 LOCAL_READY。0003 要求至少 3 只同时送股和转增样本才能证明通用字段语义；本轮未提供该证明。测试以 688808 symbol 标记的人工夹具中现金/前收/配股值都是显式夹具，非官方新增证据。

独立 `compare_forward(evidence, forward_rows, source_id=..., raw_source_id=...)`：源标识必须不同且非空；检查完整同 symbol/date 窗口、有限 OHLC、technical hash 未改动；每根每项 OHLC 相对误差 <=0.5% 才返回 LOCAL_MATCH。部分交集、同源、越界误差返回 ADJUSTMENT_DATA_GAP；forward hash/最大误差/比较根数留证。forward 是比较依据而非计算依据；源独立性仍为调用者声明，人工夹具 LOCAL_MATCH 不是 LIVE/MATCH 或真实源认证。暂不实现对异常事件的解释批准。

`corporate_action_version`绑定股票与按日期排列的原事件SHA；`factor_version`绑定该版本、qfq-ratio/1及已核验规范化事件（含真实前收与参考字段）。两者不含每日追加的raw整序列或coverage日期；正常新增日线不触发公司行动reset。`series_version`仍绑定raw/technical/coverage/factor版本，作为单次技术序列身份。

`pending_reset_evidence(previous_factor_version, current_factor_version)`只比较独立因子版本，不接受调用者把series_version当作公司行动版本的解释。两个合法因子hash不同时返回ADJUSTMENT_FACTOR_CHANGED、pending_reset_required=True、state_mutated=False；缺失/非法版本为UNPROVEN。不修改iteration_state、计划、资格或任何文件。这是本地证据函数，没有自动接入日线同步器。

## 反例与命令

先行新增测试后命令 exit=1（corporate-factor-before.xml）：模块不存在导致收集阶段 ModuleNotFoundError；不是逐项数值反例已运行的证明。实现后 34 项测试覆盖比例而非仿射、两事件乘积与当天边界、量额/raw 保留、差异现金、送转未知、缺事件/覆盖/配股/前收、日期/股票/重复/范围、非有限/负值/bool/坏 OHLC、无事件正证据、原事件篡改、版本绑定、纯 reset、全窗口独立 forward、0.5% 边界和伪 GLOBAL_LIVE 声明。

    $env:PYTHONPATH='D:/dev_A股/liangjian_wp5_hotfix_20261009/src'
    D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -B -m pytest tests/test_wp5_corporate_action_factors.py tests/test_wp5_corporate_actions_probe.py --junitxml=artifacts/wp5-20261010/corporate-factor-local-final-v3.xml
    git diff --check

61 passed；两条命令 exit=0。补充 GAP 原事件 hash 保留断言时，中间版本因重复事件的测试预期漏算第二条 hash 而 1 failed/60 passed（final-v2.xml）；纠正测试预期为逐个保留，不改变重复事件 GAP 规则，最终证据为 final-v3.xml。此范围是独立本地合同及既有探测回归，不是全量测试/自然运行/发布验收。本轮唯一源码与测试为上述两个新文件，报告为本文件；其余工作者修改保持不动。
