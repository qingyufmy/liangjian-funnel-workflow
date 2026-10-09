# WP4/WP6 来源隔离与资源测量复审切片

## 1. 真实原因

LOCAL_REFERENCE 的旧 `_research_input` 仍走六项东财板块资金与腾讯失败后的东财回退；热榜消费者只信 available 布尔，可能把部分/旧榜当真实榜外。资源纯合同还没有能产生 Linux 进程观测的显式采样器，流式编码时间成本需要可对照的两条路径。对应 Claude 0018—0021，并非为候选数不足放宽门槛。

## 2. 修改与反例

LOCAL 主关键链隔离东财资金：原反例2失败；today 原始事实独立冻结，缺历史复合分UNKNOWN，EASTMONEY默认不变。Hot100 完整100、日期、时点、hash、证券/rank严验；A2真实上下文、compact与离线范围审计同口径，缺源不推榜外，context反例7失败。新增只读同步资源采样器，原模块21失败；后续错PID/cgroup、完成时间、晚到拒绝、相邻时钟回退先失败后修复。显式 C/STREAM 编码两路径，原新增10失败；特殊float/键/字符串黄金不改变旧异常。

逐文件和命令详见本目录四份 WP4_HOT100_CONSUMER_REPAIR、WP4_CAPITAL_SOURCE_ISOLATION、WP6_EXPLICIT_HASH_ENCODER、WP6_RESOURCE_SAMPLER。纯sampler尚未接workflow，不加入周六发布行为。

## 3. 验证

最后source隔离联合命令316 passed/exit0，`artifacts/wp5-20261010/source-isolation-joint-v1.xml`；资源29新+79相关同一命令108 passed/exit0，`resource-sampler-lifetime-final.xml`。不能相加宣称全量；新HEAD全量由 `scripts/test_all.ps1` 统一执行。

Hot surrogate额外黄金在新代码即通过，原错误已被ValueError父类捕获，无需修改实现；它不是先红后绿缺陷证据。原命令错文件名/fixture自身失误记录保留，不叫生产回归。

首次封存 HEAD `457595ba97ef2d8d8d7c1cfe4b979d5eae3c2aab` 全量实际失败：3113 总项、3101通过、5失败、6跳过、1预期失败，pytest exit1、npm/typecheck exit0，回执 `artifacts/wp5-20261010/full-source-resource-457595b/evidence.json` SHA `fcfc45abce692fc20af384c422f8fc1986ac0b3ddfe3773008baa9c5acdeea88` 原样保留。五项均为 `test_wp5_disclosure_formal_routes.py` 原仅1条记录却声明100的旧fixture；严格consumer拒绝符合预期。仅两处输入替换为既有 `_complete_hot100_fixture`，原route/公告覆盖断言保持不变，不改生产门槛。相关50通过/exit0，`hot100-formal-fixture-final.xml`。新提交须另跑全量，不能用此50项或失败的旧HEAD验收新版本。

## 4. 四层验收

CODE：切片通过，联合新HEAD全量/独立verify后送Claude。REPLAY：固定输入，不是五日来源排名差集或27方向180秒预算。OPERATIONS：未发布、未模型/通知执行、未Linux/VM自然资源采集。STRATEGY：无阈值/数量/限流/旧权重政策应用。

## 5. 未完成边界

资金原始时点双截止正式采集器未接线，acquisition认证False；没有凭请求时刻证明来源有效。独立东财影子采集与排名diff尚待实施。决策11原降级归一化只写提案，Tony未批准。采样同步、固定间隔有遗漏，观测峰值只是下界；cgroup/host不能归因进程，读取span非原子，实际Linux命名空间布局和开销需自然证据。C/STREAM实际运行选择待VM各一次计时。

## 6. 下一步

新提交全量送桥接复审；继续独立工作，但不把Claude接受或测试通过当发布/切源批准。原生产与冻结证据保持不变。
