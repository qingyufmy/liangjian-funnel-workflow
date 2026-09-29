# 9月29日 A3—A5 迭代验收

## 当前生产证据

只读核对 SSH 为 192.168.1.254/debian，生产提交仍为 8c97d658d4667232e670e31322c511d020e4fe8f。用 www 用户读取 Git，未更改全局 safe.directory。冻结文件复制到 `artifacts/a3-a5-20260929/`，未修改远端数据。

- 今日执行池的 A4：240 条判断记录；激活窗口内预期/实际观察均 239，遗漏为 0，交易信号为 0。不能把盘后新研究池当成今日 A4 入口。
- 盘后研究 `2026-09-29-close-3ccf727422e8` 的 A1/A2/A3 均 VALIDATED；A3 核心研究 15、观察 60、拒绝 8。这些不是已经发布或激活的次日执行计划数，本轮未验收发布。
- 午间 A5 模型原文两条反例写 `UNRESOLVED_A3_LINEAGE_MISSING`，但阶段字段只接受 A1/A2/A3/A4/UNRESOLVED，复现结构错误。输入 63045 字符，不是输入超限。
- 盘后 A5 已有报告，不能再称全天 A5 未执行。报告显示 A4 全日最高价及成交量各 1 处异源差异：10:17 最高价 9.03 对 9.10；09:31 成交量 1623700 对 2102100。金额不可比较保留 DATA_LIMITED，不虚构补齐。

## 修复

1. A5 对本系统明确产生的 `UNRESOLVED_A3_LINEAGE_MISSING` 做无损阶段归一为 UNRESOLVED；未知代码仍拒绝。模型原文归档不变，提示词同步明确原因代码与阶段枚举的区分。
2. 服务端事实对账遇到上述血缘缺失时，强制“未确定”，取消“已确认选股缺陷”结论；不能借单日上涨倒推 A3 错误。
3. 模型结构校验失败持久化具体字段及校验类型，不保存敏感输入到错误日志；原始响应继续保留，失败响应不发布为报告。
4. A4 复盘 `cross_source_status` / `archived_tdx_status` 改为全部必需字段的汇总。任一真实差异为 MISMATCH；缺字段为 DATA_LIMITED；新增 `*_close_status` 明确保留仅收盘价比较结果。原容差、原值、交易规则均未修改。
5. A3 既有主题证据日期/哈希传递修复纳入本轮回归，保留 RETREAT、首板观察、顶部风险等真实限制。本轮不为每个阶段硬凑新改动，也不把资料修复等同策略合格。

## 验证记录

运行环境：`PYTHONPATH=src`，Python 为 `D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe`。

```
python -m pytest tests/test_a5_daily_review.py tests/test_a5_independent_verification.py tests/test_a4_review_field_status.py tests/test_a3_theme_stage_handoff.py tests/test_a4_runtime_repair.py tests/test_plan_session_contract.py tests/test_a5_fact_guard.py tests/test_a5_plan_replay.py tests/test_a4_replay.py -o addopts='' -q --tb=short -rs
```

最终退出 0：97 passed, 1 skipped。跳过 `test_a5_fact_guard.py:336`，原因为 Frozen production fixture unavailable，不计入通过。首次小范围测试曾因旧测试把收盘价匹配当作全部字段匹配而失败（1 failed, 54 passed）；已修订该断言，保留真实字段缺项。

新增离线工具 `scripts/audit_a5_archived_schema.py`。分别使用午间和盘后原始响应及冻结事实，加 `--validate-evidence` 执行：结构验证 → 引用目录验证 → 服务端事实约束 → 对账 → 再次结构/阶段验证，最终两份均退出 0。

```
python scripts/audit_a5_archived_schema.py --response artifacts/a3-a5-20260929/midday-bd1a578cdeab-model-db1f57ba73aa.json --facts artifacts/a3-a5-20260929/midday-bd1a578cdeab-facts.json --validate-evidence
python scripts/audit_a5_archived_schema.py --response artifacts/a3-a5-20260929/post-close-853a8225ed2b-model-edfecca81069.json --facts artifacts/a3-a5-20260929/post-close-853a8225ed2b-facts.json --validate-evidence
git diff --check
```

工具首次仅引用原始目录，漏算生产使用的投影聚合引用，出现 A5_OUTPUT_EVIDENCE_INVALID；补用现有 `_projection_evidence_ids(_model_fact_projection(facts))` 后复验通过。没有新增白名单伪造引用。原模型和事实文件未改写；没有模型调用、生产数据库写入或通知发送。

## 尚待解决，不提前宣称通过

- A4 两处异源差异的具体成因和动作影响需读取当时冻结窗口逐项复算；本轮修复的是核验语义，不是认定其中一个源错误。
- 竞价刷新热门榜不可用仍需独立定位，不能按模型建议简单把失败变成空数据成功。
- 今日新 A3 的完整冻结技术回放、次日计划发布和激活没有在本轮执行；15 个核心研究结果不是 15 个可交易计划。
- 本轮未提交、部署、重启，也未补跑/补发午间 A5。历史失败可离线复验通过不等于生产补跑完成。

CODE：97通过、1缺夹具跳过。REPLAY：两份原始 A5 输出与冻结事实离线复验通过，非全日异源策略重放。OPERATIONS：只读证据核对，未部署。STRATEGY：门槛未改，不证明收益改善。

下一步：对 10:17 与 09:31 异源差异做冻结窗口动作归因，并对今日 A3 研究结果核对正式发布血缘；完成后再安排单独授权的部署验收。

## 后续核验：20:30 左右只读检查与字段敏感性实验

重新确认 SSH 192.168.1.254、debian、生产 HEAD 8c97d658d4667232e670e31322c511d020e4fe8f。

新增 `scripts/audit_a4_field_sensitivity.py`，只读本地冻结事实。先复现原 239 窗口的全部原因计数，再按归档异源差异分别替换 HIGH、VOLUME，及同时替换。Tencent 原 amount_kind=ohlc_estimate，反事实金额同步使用原 OHLC 均价乘成交量公式，不冒充真实成交额。闭合聚合继续走实际 `_intraday_market_context` / `evaluate_strategy`。检查左值、拒绝截断差异样本、缺窗口、缺市场状态、重复决策及已有入场/失效生命周期，避免生成伪完整回放。

- 原事实 SHA256：`5da5ca0f75a43ebd00b64fc53ae374147d7225069c57d26f0016df96acdbec09`。
- 策略源码 SHA256：`079600edc90a6cdb69060ea272a63a46fe2cd19e61ea2746ccb43bb3840b05b3`。
- 239 个基线窗口与归档原因计数一致。三个情境买入触发均 0、动作变化均 0、原因列表变化均 0。
- 这排除了两处已知字段差异在本实验条件下造成技术买点消失的解释；没有复原历史收包时序、实时报价、账户及模型，不能判定哪个源正确或证明盘中成交。

生产只读 SQLite 查询 `state/workflow.sqlite3.execution_plans` 确认该收盘批次已发布 15 个不同股票计划，股票集合与本地研究 core_watch_pool 完全一致：000036.SZ、001207.SZ、002487.SZ、002712.SZ、002803.SZ、300185.SZ、300450.SZ、300690.SZ、300890.SZ、601360.SH、603067.SH、603598.SH、603602.SH、605136.SH、688155.SH。14 TREND_MA5、1 MA520_SWING；全部 PENDING_MORNING_REVIEW，valid_from=null，expires_at=2026-09-30T15:00:00+08:00。**已发布不等于已激活**，也不是本地未部署修改生成的结果。

实际执行：

```
python scripts/audit_a4_field_sensitivity.py --facts artifacts/a3-a5-20260929/post-close-853a8225ed2b-facts.json
python -m pytest tests/test_a4_field_sensitivity.py tests/test_a4_review_field_status.py -o addopts='' -q --tb=short
git diff --check
```

均退出 0；本次 5 passed（含真实冻结事实测试、不可变性、左值不匹配、生命周期保护）。此前 97 passed/1 skipped 为上一轮结果，不混加为一次全量运行。探索期间尝试 runtime 路径不存在、VM 无 sqlite3 CLI、state/runtime.sqlite3 无 execution_plans 表；最终用 Python sqlite3 的 mode=ro 查询实际 workflow.sqlite3，未创建数据库或改配置。

CODE：本轮工具与5项测试通过。REPLAY：字段敏感性已完成，原始接口差异成因仍待证据。OPERATIONS：正式发布记录已核对，未部署、激活、通知或重启。STRATEGY：不改变门槛，不宣称策略有效或完全无漏单。下一步需追踪保存的原始接口响应，区分接口历史修订、开盘成交范围和适配器处理；缺少原响应时保留未确定，不能修改行情值来消除差异。
