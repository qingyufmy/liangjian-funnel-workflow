# A3 实际 ResearchPipeline 适配切片

实际采集时刻：2026-10-17 05:14（Asia/Shanghai）。依据统一任务书 WP-A，非整包送审或发布回执。

## 改了什么

新增 `pipeline/team_v2_research.py`，在原 `ResearchPipeline` 构造器显式注入 `team_research_adapter`，A3 阶段分派、现有批处理和最终本地门合并均可实际走新盲审接口。不传适配器时原行为不变。**Workflow 的生产构造点尚未启用该注入**；A1/A2 暂不宣称支持。不能把“隔离中实际 ResearchPipeline 跑通”写成“生产自然研究已切换”。

新路径使用 Claude A3 4.0.1/shared3.0.2 原文，不渲染旧 A3 提示词、不复用旧提示词检查点。原始模型判断仍由 owned journal 精确绑定 source/prompt/config；原模型捕获和原输出不变。原来源时间必须是交易日收盘后，次日通过正式交易日历确定，不按星期猜。晨审入口不在本切片启用范围。

模型看到显式盲技术投影、真实 A2 FOCUS/WATCH 来源、可引用的原事实 ID/hash。模型提名不进入域；原资格、满足列表、门结果和其他会话不送模型。原有价格/策略 provenance 校验、WATCH→PROBE、风险硬门、本地未送模行的完整覆盖仍在最终输出适用，未改阈值/候选数/限额。模型 VETO/DATA_GAP 不变成可执行计划，模型 PASS 也不能推翻引擎 DATA_GAP。

额外反例发现 FOCUS 也可能受原 plan_mode=PROBE 限制，不能仅以 FOCUS 推成 STANDARD；已按原计划/风险事实限制模型风险单位，原例红→绿。异常类型的 strategy_facts 明确源阻断，不抛 AttributeError 后当作模型失败。

每批模型排名保留 `model_priority_rank`，跨批仅由服务器按(批内rank,symbol)确定性穿插为展示序；标 `SERVER_INTERLEAVED_BATCH_LOCAL_DISPLAY_ONLY`，不冒充全域模型排序、不增删计划。旧模型原排名保留在 journal。父预算余量不够承接原已绑定 timeout 时，不发新 HTTP；已有捕获/已确认失败仍可复用，不以重启变更请求绑定。StageAudit 的 provider_attempts/latency 来自实际捕获；传输失败未留完整调用数据时显式 unknown，不伪填一次成功。

## 原件

路径均在 `artifacts/team-v2-20261016/`。

| 回执 | 结果 | SHA256 |
|---|---|---|
| 20261017T050718075263-a3-natural-dispatch-red-receipt.json | 原分派1红/1正对照通过，exit1 | be2c0c3c71d6c801050d9183ad5e5e4c7062dae9bc42b3c09281fb10e77f5eb2 |
| 20261017T051224259758-a3-natural-probe-cap-red-receipt.json | 原FOCUS+PROBE越权反例1红，exit1 | 9e9a719e9f1ccb9b4b39d4be363cc0d1c0def9f0b98e5c6a1716ecb4955efb08 |
| 20261017T051236553284-a3-natural-probe-final-receipt.json | 257关联绿，exit0 | ca15ea740adf56eae7b568cf422433b60ebe97fb233a87809d9a55c589127894 |
| 20261017T051338509014-a3-natural-final-source-delta-receipt.json | 后续源类型保护14项差异绿，exit0 | 0497297adebc7e7e65584eac1770e738e808f0fec79d4c38b023546b435e2c57 |

257范围：新适配/盲审、原pipeline、原检查点两组、A3策略、研究模块迁移。随后只新增异常strategy_facts分支，14项覆盖实际HTTPmock、三批合并、完整本地门覆盖、重复调用、旧cache隔离和预算。本轮数字不相加，不是最终全量/VM。未变代码不重复跑。客户A5/service/publisher、通知模板、生产env/库未改；没有真实模型或新行情请求。

## 四层与下一步

CODE：上述切片通过。REPLAY：隔离反例/真实客户端mock；不是三日真实模型或六日正式回放。OPERATIONS：未启用、未部署。STRATEGY：收益未证。

0262询问 A2 冻结主题分聚合/缺因子/扣分口径，避免自行添加影响FOCUS的公式；仅A2该适配待答复。0259最后收盘bar结算红件仍独立。继续准备 A1/A2 适配及正式冻结输入，不重复既有H1测试、不动其周日安排。
