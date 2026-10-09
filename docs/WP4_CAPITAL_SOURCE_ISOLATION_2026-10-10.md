# WP4 LOCAL_REFERENCE 资金源隔离

## 1. 原因和证据

现有 `workflow._research_input` 在两个模式都先采腾讯，失败回退东财，再同步调用 industry/concept × today/5d/10d 六个东财 collector。`rotation_membership_source=LOCAL_REFERENCE` 只隔离了另一路板块成员，不隔离该资金路径；`CAPITAL_FLOW.available OR BOARD_CAPITAL_FLOW.available` 仍会放行资本因子。以实际 `_research_input` 固定输入反例复现：腾讯健康时 board=6；腾讯失败时 fallback=1、board=6，均不是 LOCAL 主链零东财调用。没有用全链health代替此调用证据。

## 2. 最小修改

`workflow.py` 只在已有 LOCAL 模式下禁止东财 fallback 和六板块调用。执行板块包显式 `available=False`/空taxonomy；原逐股包经 `data.capital_source_policy.project_local_capital_evidence` 输出未知复合因子，每股 `capital_flow_score=None`。冻结的 `CAPITAL_FLOW_RAW_TODAY_EVIDENCE` 单独保存原腾讯 today值/percentile/日期/接收声明及原采集snapshot hash，明确 evidence_only、acquisition_authenticated=False、正式双截止采集尚未接线。投影在 provider_attempts 装饰之前捕获原hash，不能把装饰后的hash叫原缓存hash。

逐股today .35/3d .25/5d .25/10d .15 与板块today .5/5d .3/10d .2是不同因子；不重新归一化、不填东财历史或0/50。默认 EASTMONEY 原provider路径保持不变；未改设置/配置/阈值/供应商限流。影子记录明确 `NOT_COLLECTED`，没有占位冒充采集或五日差集。

`inspect_legacy_capital_weighting` 仅对实际原输入/hash审计每行窗口和原available_weight，验证原公式后区分FULL_WINDOWS/DEGRADED_RENORMALIZED；不能证明公式或元数据缺项为DATA_LIMITED。原score/hash不重写；这是后续影子排名的注解组件，不是已完成影子采集或已经批准旧权重。

## 3. 真实验证

PowerShell，`PYTHONPATH=src`，共享venv：

```powershell
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -m pytest tests/test_wp4_capital_source_isolation.py tests/test_a2_market_data.py tests/test_a2_market_facts.py tests/test_a2_features.py tests/test_workflow_fact_projection.py -q -o addopts='' --tb=short --junitxml=artifacts/wp5-20261010/capital-isolation-final-v4.xml
```

exit0，57 passed；不能与前轮55项相加。新测试11项，固定collector注入、真实 `_research_input` 调用；无网络/模型/DB/通知。固定输入的市场为RISK_OFF，证明源隔离与其他门独立，不宣称此fixture能产交易机会。LOCAL健康/失败均0东财调用，执行因子和总资本可用门均False；EASTMONEY健康/失败分别保留0/1fallback和6board调用。错误source/hash/date/universe不能将缓存自认证为腾讯原值。

原 `capital-isolation-before.xml` collection error 是测试import路径错误；v2/v3缺行业事实导致未走到目标路径，属于fixture修正，均保留。v4实际2 fail/2 pass准确复现旧路由。首修53 pass/2 fail暴露provider_attempts后的hash不能指向原缓存，真实修复后55 pass。归一化审计新测试首个缺API真实exit1；随后1 fail/56 pass是fixture只给净额未给净占比，原算法不能计算percentile，已补真实固定ratio再运行最终57。没有把这些失败当成生产故障或删除回执。

## 4. 四层验收

CODE：局部57项通过，待与消费者修复统一新HEAD全量及Claude复审。REPLAY：固定输入/原字段哈希，不是五自然日影子比较。OPERATIONS：未发布、未切源、未真实采集；默认生产路径不变。STRATEGY：不适用，旧降级归一化政策单独提交决策11，不在此修改。

## 5. 未完成边界

腾讯源真实业务观察时间及acquisition-end封存尚待采集器接线；这里只保留事实声明且不让其作为执行复合分，不能宣布时点认证完成。独立东财影子采集、同输入五日A2排名差集、10日成分股资金逐日成员版本绑定仍待证据，不能用价格横截面代替。新source模式和资金权重仍需Tony单独批准；旧EM模式中原来可用权重归一化现象已记录，不擅自改动。

## 6. 下一步

合并Hot100消费者防御测试后封存新HEAD全量复审，再完成独立影子入口和采集封存时点合同；不等待本轮评审停止其他隔离工作。
