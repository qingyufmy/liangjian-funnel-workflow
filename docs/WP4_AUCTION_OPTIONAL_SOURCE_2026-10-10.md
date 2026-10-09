# WP4 可选热榜入口故障隔离

## 1. 真实原因

只读调用盘点 `artifacts/wp5-20261010/WP4_CONSUMER_INVENTORY.md`（e02b6c4基线）确认：`prepare_auction_delta` 采完东财热榜马上抛异常，`project_auction_delta` 再次把热榜当必需项。独立09:26 run-morning晨审不走该研究刷新入口，不能把研究失败误称全部A4失败。此修复不宣称今日生产自然任务已验证。

## 2. 代码修改

只改 `runtime/auction_base.py` 和 `tests/test_auction_base.py`。删除采集后重复热榜可用性门；最终投影区分明确 `available=False` 与伪成功/未知状态。明确不可用时该可选入口记录0项，不消费旧日/部分记录；保留原日期、reason_code、source_attempts和原对象hash，写 `OPTIONAL_SOURCE_UNAVAILABLE / absence_is_not_popularity_evidence`。源故障不等于股票不热门。

`available=True` 仍严格要求当前日、100唯一股票、rank1–100及正确记录结构；可用却不完整、旧日、排名重复、状态未声明仍硬失败。原A1/G0范围、风险事件、日线、公告/宏观原日期、执行发布均不改。当前板块和报价必须就绪；180秒采集预算不改。未替换热榜供应商、未用THS榜冒充东财。

新增反例旧代码4失败/3通过/exit1，保存 `wp4-auction-hot-before.xml`。其中malformed_row曾泄漏AttributeError，现归为明确WorkflowError。两项prepare级测试验证真实调用点不再提前返回：可选热榜失败后仍调用板块/报价；181秒仍拒绝且不落delta。

## 3. 测试与回放

```powershell
$env:PYTHONPATH='src'
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -m pytest tests/test_auction_base.py tests/test_workflow_orchestration_coverage.py tests/test_wp5_auction_preparation.py tests/test_eastmoney_hot.py -o addopts='' -q --junitxml=artifacts/wp5-20261010/wp4-auction-hot-final.xml
git diff --check
```

实际103 passed / exit0，diff-check exit0。固定fixture+Mock调用，不采新行情、模型或生产库。尚未有此新HEAD全量回执，随后统一提交运行；不同切片不相加。

## 4. 四层验收

CODE：103关联项通过，新HEAD全量待回执。REPLAY：本地失败fixture通过，27方向真实影子五日未做。OPERATIONS：未发布，09:26/15:10自然调度仍待证据。STRATEGY：未改任何入场/风险阈值，不适用。

## 5. 未完成边界

这是一个可选入口误阻断修复，不是东财彻底旁路。额外六份板块流仍在收盘组装主路径串行查询；LOCAL_REFERENCE历史补齐仍受空BK组件码条件限制；新的三日THS合同暂未接collector/消费者；源raw响应字节归档、27方向日线与等权历史、180秒现场耗时仍待实施/证据。不能拿这项修复的绿色测试宣称两条链必然READY。

此修改目前仅隔离分支，生产env/数据库/信号/任务不变。发布须Tony单独批准，Claude代码接受不等于授权。

## 6. 下一步

新提交全量验收后送桥接复审，继续补齐THS合同来源字节和非东财板块资金/历史消费者，保持风险事实门不变。
