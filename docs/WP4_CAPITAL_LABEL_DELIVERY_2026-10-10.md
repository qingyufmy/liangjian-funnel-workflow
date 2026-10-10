# WP4 A-LABEL：标签穿透与原分数保留

## 1. 真实原因

Claude 0024/决策11核实逐股资本因子 today=.35、3d=.25、5d=.25、10d=.15，原算法按已观测权重归一化；today 单窗口与四窗口结果共用同名分数。Tony 06:55 只授权增加标签与五日 B-SHADOW，不批准切 B、改权重或候选数量。当前生产 adafe50（完整 SHA 见发布文档）仍 SHADOW/LOCAL_REFERENCE，不将本切片当已上线。

## 2. 代码修改与反例

- `data/capital_source_policy.py`：复用原公式审计，补 label_only、原四窗口权重和逐行纯元数据拷贝；hash/公式不能证明时 DATA_LIMITED，policy_approved=false，不把标签批准等同策略批准。
- `workflow.py`：独立 `CAPITAL_FLOW_WEIGHTING_AUDIT` 留存在新研究包；不改原 CAPITAL_FLOW_SNAPSHOT，不改路由。
- `pipeline/a2_features.py`：仅逐股 capital factor 带 weighting_observation；板块三窗口不套逐股四窗口，不改分数、角色或可用性。
- `pipeline/research/common.py`：同一冻结输入的模型投影带逐行标签和原 input hash；投影 content_hash 仍指原全包，标签是派生元数据，不重写原证据。
- `tests/test_wp4_capital_label_delivery.py`：8 个新反例修复前失败（有效 pytest 命令 exit1）；不存在的 worktree .venv 命令退出1但未跑测试，另行记录，不混入反例计数。修复后61项相关测试通过；追加2个非空送审集的确定性路由等价用例，10项新切片通过。

## 3. 测试与回放

解释器复用 `D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe`，pytest 按当前工作树 `src` 导入。

反例：`python -m pytest tests/test_wp4_capital_label_delivery.py -o addopts='' -q --junitxml=artifacts/wp5-20261010/capital-a-label-before.xml`，exit1，8 failed。

相关：同上加 source_isolation、a2_features、a2_market_data、a2_market_facts，`capital-a-label-after.xml`，exit0，61 passed。

新增路由等价：`capital-a-label-routing.xml`，exit0，10 passed。同输入有非空 review_symbols；标签前后送审/观察集合、状态、路由、首要原因、辨识分及每个因子分数相同。所有原输入 deep equality 与 hash 仍一致。该测试不是行情/模型/实盘回放，不能推定 LLM 看到新信息仍作同一判断。

新 HEAD 全量回执由 `scripts/full_test_baseline.py` 生成，置于本轮 review 包；未用上一 HEAD 3113 的回执冒充新改动通过。

## 4. 四层验收

CODE：局部反例、数值和候选等价通过；新 HEAD 全量以实际回执为准。
REPLAY：固定输入验证；B-SHADOW 连续5交易日未完成，不造历史样本。
OPERATIONS：本切片未发布、无新模型调用/通知/生产库写入。已发布的 adafe50 的安装验收单独留存。
STRATEGY：原算法原阈值；切 B、因子合同变更仍需另批。

## 5. 未完成边界

来源政策与 rotation membership 的独立显式设置（0027设计意见）尚待独立切片，默认迁移不得偷偷改变 LOCAL_REFERENCE 的资金可用性。五日 B 差集尚待同输入研究工具与真实自然日证据；本次不接22:00或A5任务，不应用 B。

审批独立：获批精确发布 adafe50 已完成，新标签提交不得自动 push/deploy。周一计划0、fsync父目录不存在、六日原文件未找到的阻断未被本切片解决。

## 6. 下一步

Claude 复审新标签包；继续拆来源政策与同输入 B-SHADOW 工具，等新的发布/周末研究单独授权。
