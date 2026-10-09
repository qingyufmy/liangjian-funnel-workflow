# WP5 回执幂等与短历史增量

## 1. 原因与证据

Claude P2-7：`recorded_at` 参与范围回执地址，每次相同输入重试都新建文件，无法识别重复。P2-9：少于 30 根缓存日线每天重新从 800 日起点请求。旧代码反例退出 1，3 failed / 20 passed（`artifacts/wp5-20261009/p2-7-9-before.xml`）。这不是策略参数校准。

## 2. 最小修改

`close_scope.py` 新版回执 `/2` 的 `receipt_hash` 绑定原 run、截止、A1、来源与集合，排除实际观测时间；另有 `observation_hash` 绑定完整文件与首次 `recorded_at`。相同事实重试复用首次文件，不重标时间；变更代际仍新建；任一哈希冲突阻断。旧 `/1` 仍按原整份哈希验证，历史文件不改写。`disclosure_maintenance.py` 共用验证入口；测试夹具明确生成新版哈希。

`data_sync.py` 成功 FULL_REFRESH 后在同一游标存完整请求范围、返回行数、原数据哈希、实际收到时间及 coverage_hash。少于 30 根只有这份成功完整源回执覆盖本轮起点、至少 3 根可重叠校验时，才能增量或命中运输缓存。旧游标、短于 3 根、坏哈希、窄范围、失败状态仍全拉；三根 OHLCV 修订与 pending reset 不变。后续增量保留原完整历史覆盖证据。

运输缓存 READY 不代表 A3 技术历史长度合格；未改 FactorEngine、A3门、策略或候选数。完整性依据仍是现有供应商 `ok and complete` 契约，没有根据缺失日猜停牌或补造 K 线。此项不解决复权 P0-A。

## 3. 测试

统一 `PYTHONPATH=src`，Python 为 `D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe`。

- `pytest tests/test_wp5_close_scope.py tests/test_wp5_daily_incremental.py -o addopts='' -q --junitxml=artifacts/wp5-20261009/p2-7-9-before.xml`：退出 1，3 failed / 20 passed。
- 5 文件切片：退出 0，55 passed（`p2-7-9-final-v1.xml`）。
- `pytest tests/test_wp5_close_scope.py tests/test_wp5_daily_incremental.py tests/test_wp5_disclosure_maintenance.py tests/test_pipeline_data_sync.py tests/test_wp5_g3_regressions.py tests/test_wp5_disclosure_formal_routes.py tests/test_wp5_pipeline_workflow_integration.py -o addopts='' -q --junitxml=artifacts/wp5-20261009/p2-7-9-final-v2.xml`：退出 0，74 passed。覆盖旧版哈希、改观测时间、短历史窄覆盖/坏哈希/失败回执等。
- `git diff --check`：退出 0。上个 HEAD 的全量回执不能作为本次源码发布凭证；本轮未发布。

## 4. 四层

CODE：74 项相关测试通过；最终新 HEAD 全量待执行。
REPLAY：200 股真实源对照待证据，合成短历史不替代真实源。
OPERATIONS：未 push、未部署；默认 SHADOW，22:00 未接线。
STRATEGY：不适用；没有参数变更或收益结论。

## 5. 未完成边界

真实公司行动能力、前复权/原价绑定、Oct8 完整原始血缘和自然 5 日耗时仍未验收。竞价两段资料的日期/代际/覆盖与增量契约须继续落地，不能以夜间成功标记替代晨间新鲜度。

## 6. 下一步

继续只读源探测与真实 WP1 多日基线，完成竞价基线拆分及阶段计时，提交下一轮桥接复审。
