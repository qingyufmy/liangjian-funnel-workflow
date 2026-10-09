# 2026-10-09 A2权限与本地板块修复

## 真实原因

生产原批次`2026-10-08-close-a2-audit-172755`的五只技术研究候选（600026、601975、600663、603906、600310）均有独立趋势研究资格，但`trend_core_eligible=false`，板块通道`ROTATION_THEME_ROWS_EMPTY`。情绪周期另禁止新开仓。旧代码将情绪限制全局继承，即使将来趋势板块门禁合格仍会误拦趋势/520。本轮修的是这个权限冲突，不能用旧缺数据事实直接解禁五只。

当前活动A1已有1749只，旧A2批次只使用较早1451只。今日07:00资料基线被父任务60分钟上限终止，不是A1清空或内存耗尽的证据。旧基线缺阶段进度，不能断言具体卡在某个供应商。

## 代码修改

1. `a2_role_logic.route_execution_permission`：仅在确定性A1正式成员、独立趋势资格、有效趋势板块通道和所选技术路线同时成立时，将情绪周期限制限定到龙头路线。缺数据、硬风险、观察/储备限制仍阻断。A3、最终结果规范化和正式发布共用该判断；正式发布使用A2审计/冻结上下文，不能用模型自己声明的权限解禁。计划保留原A2权限和原因审计字段。
2. 明确9个复合行业映射；27方向实际成员采集完成。复合行业不是同花顺原生指数，单源、明确组件、完整性/身份/日期/哈希核验、按股票去重，任一组件缺失全方向不可用。组装时间不刷新组件年龄；组件证据进入公开板块与个股板块映射。
3. 早盘基线使用独立`auction_base/<date>-progress.json`，中断时保留最后阶段和计数；近期公告可用最近2天完整哈希基线＋覆盖上一结束日的完整增量，冲突、缺页、旧日期均回退完整查询，原公告日期不变。年度/主营仍复用既有完整增量契约。
4. 显式会员图晋升入口`promote_rotation_reference.py`：默认只读，完整当前27方向才能切换3个专属env键。保留原env备份和其他配置，交易时段禁止切换，会员完整不等于实时资金/报价完整。原东财档案不改，新日度数据使用独立`rotation_theme/local_reference`。

## 实际测试和数据

反例先行：趋势路线误继承BLOCKED测试退出1；公告窗口整段重扫及基线进度缺失测试退出1。最小修改后相关181项退出0；新增正式发布四种路线和权限伪造反例后94项退出0。

完整相关切片：

```powershell
$env:PYTHONPATH='D:/dev_A股/liangjian_a4_20260923/src'
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -m pytest -o addopts='' -q tests/test_hithink_board_reference.py tests/test_board_reference.py tests/test_rotation_theme.py tests/test_rotation_diagnostics.py tests/test_rotation_evidence_overlay.py tests/test_a2_a5_rotation_remediation.py tests/test_ths_taxonomy.py tests/test_ths_industry.py tests/test_hithink_fact_endpoints.py tests/test_settings.py tests/test_auction_base.py tests/test_auction_refresh.py tests/test_auction_data_repair.py tests/test_feature_source_materialization.py tests/test_hithink_fact_normalization.py tests/test_hithink_probe.py tests/test_a1_sources.py tests/test_a2_features.py tests/test_a2_role_logic.py tests/test_a3_strategy.py tests/test_workflow_integration.py tests/test_disclosure_incremental.py tests/test_early_discovery.py
```

退出0，386 passed。后续新增公开组件传递与计划权限落盘修改：workflow_integration/rotation_theme 51 passed，退出0；独立晋升入口2 passed，退出0。计数范围重叠，不累加。`git diff --check`退出0。

本地实际35次增量采集，目录概念390、行业320，27/27方向完整。版本：`artifacts/board-reference-20261009/hithink-final/refresh-audit-fc6719b62987afbab461d6221ed78147c367a642b725de2bbe10bc3a848c2a0a.json`，原成员版本按哈希保留。新增行业代码/名称来自这个真实目录，不由英文策略名猜代码。

腾讯尝试的批量资金接口返回空/invalid；没有采用未验证的新解析器，也没有猜单位。批量报价仍复用既有实现；个股资金仍走原严格来源。整个市场的实时采集耗时与180秒竞价上限仍需现场检验，不用扩大上限掩盖。

## 验收边界

- CODE：相关切片通过；不是全项目测试。
- REPLAY：权限冲突/伪造、缺组件、身份/日期/去重、公告缺页与全量回退通过；尚无新版本自然盘中机会验证。
- OPERATIONS：待本轮部署、安装源码与真实任务验收。今日原基线已失败，今天不能用昨日缺板块事实、延长旧计划、重标快照日期或直接写数据库制造执行池。
- STRATEGY：策略阈值、板块数量、量化指标、T+1均未放宽。无信号不等于执行故障；但0执行计划必须在工作台和通知中作为业务阻断展示。

下一步：安全发布新代码和完整会员图；验证实时板块排名/资金覆盖及09:26自然任务；无新合规执行计划时保留阻断。盘后正式收盘研究使用最新A1代际，核对A1⊇A2⊇A3和次交易日计划，不伪造今日信号。下个自然基线必须读取独立阶段计数核验耗时，不能提前宣布60分钟超时已彻底消失。
