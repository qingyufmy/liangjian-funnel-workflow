# WP4 数据准备独立审查（0589 / 0591）

审查时间：2026-10-22。范围为本机候选 `codex/postclose-core-20261022`，初始基线 `a4eb2b7`，同时审查主代理正在实施的工作区改动。本报告不代表冻结 HEAD、VM 全量测试、真实模型整链或发布批准。

## 结论

新增边界回归已使初始失败得到修正；当前本机定向组合 47 项通过、0 失败。未执行任何真实 A1、行情或模型请求，也未连接生产。日更 A1 在真实生产入口的调用量不属于 0591 的 24 次 A2/A3/A6/PM/A4 彩排额度，不应借本测试触发。

新增测试位于 `tests/test_postclose_data_contract_0589.py`。主代理拥有并修改核心源码；本审查者只修改该测试和本文。

## 实际发现及修正依据

| 检查点 | 初始行为及影响 | 当前修正 / 回归 |
|---|---|---|
| 空计划集合 | `all([])` 导致空集合为 READY，错误宣称开盘前缀已准备 | 返回 NOT_REQUESTED，保留空集合，不声称通过 |
| 新采合成 K 线 | 只核股票、周期和时间，`SYNTHETIC_QUOTE` 也可写入并 READY | 同时核 MARKET_BAR 与非合成来源；反例不得写入 |
| 缓存合成来源 | MinuteBarStore 的旧主表不持久 `evidence_kind`，读回会使用 MARKET_BAR 默认值 | 不能从默认值反推原生性；另排除实际 `:RISK_ONLY` 来源。测试使用真实风险合成来源格式，不编造可恢复的元数据 |
| 跨股票故障计数 | 下一股票在缓存读取前失败时，`locals().get('present')` 沿用上一只 48 根 | 每只股票独立初始化，故障股票可证明数量为 0 |
| A1 未来数据 | `as_of >= cutoff` 接受未来日或当前研究时点之后的 generation | 同一收盘源日，并受冻结 current 与真实 wall clock 双上界约束 |
| A1 刷新抛异常 | 原异常直接越过内部告警 | 告警并停止下游，旧 generation 不重贴日期、不冒充刷新成功 |
| 源失败日期证明 | 空 failure map 被当作无缺源，但未核实际源日及闭合时刻 | expected_source_date 对照上海时区，未到 15:00/跨日/缺时刻均 DATA_LIMITED |
| 合并缺史时丢证明 | history 缺源重建 source_data_limits，丢原 A1 源失败 SHA 与 as_of | 合并保留旧证据，再添加 history_readiness；最终发布 payload 同时保留两段原事实 |
| 历史来源证明 | READY 只有计数，无逐根内容绑定 | native_rowset_sha256 绑定实际 48 根排序 JSON，保留 source_ids、源日、目标日与 cutoff |

MinuteBar 的 `evidence_kind` 默认实际为 MARKET_BAR；不能因此把全部 legacy 历史一概判为合成，也不能宣称默认值证明了原生来源。旧缓存对任意未标记来源的完整原生性仍取决于原采集路径；本修复明确排除系统已知的风险合成来源，并对新采输入保留逐根哈希。

## 已通过的合同

- 仅目标交易日前一个交易日的 48 个精确 5 分钟闭合时刻可证明前缀完整；47 根、重复、错股票、错周期、错日期、complete=false 均不可写入或 READY。
- 单个 symbol/source-date 重复计划共享一次本机模拟采集；请求 cutoff 为前日 15:00，不用当日早盘数据倒填前缀。
- 原有严格缓存写入事务在冲突时完整回滚，不覆盖冻结价格，不把缓存部分数据与残缺请求拼成 48 根。
- A1 日更调用既有 INCREMENTAL 流程；旧 generation 对象日期不变。刷新失败保留旧状态，但不得继续下游。
- 自然 Team 发布真实分支保留 A1 source_failures SHA/as_of，并在缺史时标 DATA_LIMITED、PROBE、A6 显式审核要求。该分支测试仅将几何函数设为纯 stub，不代表 A3 模型或完整研究通过。
- A6 的 PLAN 事实必须包含实际缺源明细；未审核、空审核、仅审核部分原因、错误引用均不能恢复 eligible。只有逐项原因与准确 PLAN ref 均满足，才保留原有 PROBE 资格，不扩大权限。
- 既有 A6 session MockTransport 回归还覆盖无模型结果、坏 schema、缺 PLAN 事实以及保守 fallback；DATA_LIMITED 计划均 fail-closed，普通计划不被连带禁用。

## 仍须生产入口核实

1. `run_research` 自然 Team 日更后，显式旧 `active_a1_generation_id` 现已拒绝；即使调用方未带 reuse 标志也必须实际消费当日 active generation。`test_natural_close_role_composition.py` 新增真实入口反例，不以 `_ensure_daily_a1` 单测代替入口验证。仍待冻结 HEAD 的全量与实际 A1 刷新验证。
2. 当前测试没有运行真实 A1 模型，不能证明每日完整市场刷新耗时、调用量或实际源覆盖；上线前应单独确认授权和预算，不借 24 次彩排额度扩展 A1 调用。
3. native_rowset_sha256 是内容证据，不等同于 A6 已审或竞价已激活；有 48 根也不等于形态已确认或可以下单。

## 本机执行证据

```powershell
$env:PYTHONPATH='src'
D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -B -m pytest -o addopts='' tests/test_postclose_data_contract_0589.py tests/test_postclose_data_0589.py tests/test_regeneration_data_limits_0498.py -q --tb=short
```

结果：`47 passed in 3.14s`。新增本文件 27 项；既有两份数据准备/显式审核合同一同执行。第一次仅新增边界的运行结果为 `7 failed, 13 passed`，已原样向主代理报告，未使用 xfail 或降低断言掩盖失败。

所有真实行情采集函数均替换为本机 callable；A6 既有测试使用 httpx.MockTransport，不发网络请求。未运行 Workflow 业务构造器、生产任务、VM 命令、真实通知或计划重生成；未提交、桥接或部署。

## 补充：今日 A4 原件的有界纯形态重算

新增 `scripts/replay_a4_originals_0589.py` 仅读取主代理实际 RO 采集的 packet，不实例化任何 Store，不调用模型、行情或通知，不写结果文件。原 packet 为 `artifacts/postclose-core-20261022/a4-originals-0589.zjson`，压缩文件 SHA `8577a6b2607a94ed4dfc1eb3f382968e75bba436b87eadfce3b76bcb798bf561`，解压原字节 1,947,535 B，SHA `da4a1cc82f87b5b0fcc6fa0c4adc92852f07230fd64e87ad4b8cacfe49899fe2`。

范围仅 13:01、13:21、14:25、14:55 四个代表时点、三股各一条，共 12 个真实 monitor event。逐条核对原 event/plan payload 字节 SHA、冻结 1m 选择字节 SHA、按原 execution cutoff 选择后的 `execution_data.input_sha256`、历史 archive 行集 SHA、配置文件 SHA、publication manifest 字节 SHA 及 embedded daily facts SHA。每日事实通过同版本纯 `_validated_small_facts` 恢复 runtime provenance，不使用近似缺省输入。5m/15m 由原冻结 1m 经正式纯聚合函数计算，这是当时执行语义，不冒充新采原生 5m。

实际结果：12 条数值、阈值和来源时刻均无变化；5 条 DATA_LIMITED 改为 WAITING_CONDITION，7 条保持。改变项为 300556 的 13:01/13:21/14:25，002354 的 13:01 和 14:55。600201 四条仍为缺前缀的 DATA_LIMITED。12 条形态 confirmed 均为 false；原件中有效 BUY/ADD 为 0。

这不是全天回放，也不是完整策略、模型、风控、下单或成交验收。原完整 market_context 和 position overlay 未采到，`complete_strategy_count=0`，脚本逐条标明缺项；不得据此宣称全年/全天无漏买或成交链已验证。真实 archive 还显示 002354 种子只有 09-23 的 48 根、300556 为 10-21 的 48 根、600201 为 0；脚本按实际旧源重算，未把旧种子偷偷替换成昨日数据。

边界测试 `tests/test_replay_a4_originals_0589.py` 全部明确标记 CONTROLLED_TEST_ONLY_NOT_PRODUCTION，验证 SHA、缺源、错股票、合成 bar、未来 bar、未闭合 bar、大小上限、完整上下文绑定与零网络。与既有 postclose core 定向联合执行为 21/0；受控 fixture 不作为上述 12 条生产证据。
