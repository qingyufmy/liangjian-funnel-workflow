# A1 根因修复与工程落地实施方案

> 文档版本：1.1
>
> 编制日期：2026-08-29
>
> 适用项目：`liangjian_funnel_workflow`
>
> 方案状态：三轮架构复核完成、待确认、尚未实施
>
> 基线提交：虚拟机 `bd4ec687271fa19fbc7ba510d914510a3bbab86b`
>
> 基线快照：`snapshot-20260828T210944+0800-2e278ff757d1`
>
> 关联文档：`DETERMINISTIC_RESEARCH_PIPELINE_V2_PLAN.md`、`A1_A3_WORKFLOW_REMEDIATION_PLAN_2026-08-28.md`

本文把 2026-08-28 首次全市场 A1 正式运行暴露的问题，转化为可以分批开发、验证、部署和回滚的工程方案。本文不授权连接外部交易，不修改模拟账户，不启动生产研究任务；实施、推送、部署和正式重跑必须分别验收。

---

## 1. 决策摘要

本轮不通过减少股票、缩短历史周期、降低主题覆盖、减少模型或放松事实门来换取运行成功。修复的核心是重新划分数据、确定性代码和 LLM 的权责：

1. **完整事实由数据系统保存。** 全市场 5,563 只证券、G0 质量门后约 4,017 只研究对象、完整历史行情、基本面、公告、PDF、宏观和行业历史继续本地持久化。
2. **确定性结论由代码负责。** 月度行业前 20 排名、基础 `INCLUDE/EXCLUDE/DEFER`、量化因子、主营挂接和全市场筛选不得让模型重复生成。
3. **LLM 只负责不确定的语义研究。** 模型负责政策传导、主题归纳、产业链节点、瓶颈解释、矛盾证据和行业到主题的映射。
4. **研究关键路径只读取就绪事实。** 全市场公告/PDF增量维护从 A1 同步链路中拆出，不再让研究任务等待数小时。
5. **模型输入使用完整事实的确定性投影。** 原始历史不删除；模型接收按同一 `as_of` 计算的充分统计量和可追溯来源，而不是 44 万 token 的原始行重复堆叠。
6. **验收合同只有一个权威来源。** 配置、提示词、运行时说明、JSON Schema、校验器、前端解释必须从同一版本化合同派生。

### 1.1 本次必须解决的根因

| 编号 | 根因 | 当前证据 | 目标状态 |
|---|---|---|---|
| R1 | 月度行业合同冲突 | 服务端要求 20 条；运行时强调 Top10；DeepSeek/Kimi 均返回 10 条 | 20 条基础决策由服务端持有，模型不再复制 |
| R2 | A1 输入过大 | 约 117 万字符、44 万 token | 完整覆盖的紧凑研究包，初期预算不超过 10 万估算 token |
| R3 | 数据同步顺序错误 | 真正进入宏观发现前已耗时约 160 分钟 | 全量维护独立运行；A1 关键路径只做水位检查和小范围刷新 |
| R4 | 业务标准和校验标准不一致 | 配置 8–15 主题、40–80 节点；校验仅 6/12 | 统一为证据感知的同一合同 |
| R5 | `available` 与 PIT 资格混同 | T2 数据被当成宏观支柱 READY | 质量等级参与实时/回放资格和置信度 |
| R6 | 三模型输入不可严格比较 | 本次 DeepSeek 与 Kimi/GLM 的 prompt hash 不同 | 共享基础研究包；lane 覆盖层单独标识 |
| R7 | 语义重试无精确修复信息 | 只返回原因码，重复发送完整大提示词 | 字段级修复、缺失代码清单、有限完整重生成 |
| R8 | 进度和失败诊断不足 | 最终只显示 BLOCKED/未知阶段 | 保存最后业务阶段、合同计数、缺失项和请求规模 |

### 1.2 明确不做的事情

- 不增加全市场性能 Top-N。
- 不把 4,017 只 G0 改成少量测试池后冒充全量结果。
- 不删除 2006 年以来的宏观历史或 82 个行业历史。
- 不用新闻热度替代公告、财报、正式政策和主营证据。
- 不让 LLM 计算技术指标、价格位或确定性量化分。
- 不让成功 lane 的结果填补失败 lane。
- 不用无限重试掩盖合同或输入装配错误。
- 不把 153 份可降级 PDF 失败误判为本次 A1 的直接阻断原因。

---

## 2. 现状基线与成功定义

### 2.1 冻结基线

后续修复必须以同一个冻结基线做回归，避免行情变化掩盖代码影响：

| 项目 | 基线值 |
|---|---:|
| 完整市场目录 | 5,563 |
| G0 研究域 | 4,017 |
| 可模拟交易域 | 3,942 |
| PDF任务数 | 6,454 |
| PDF成功/失败 | 6,301 / 153 |
| 快照文件大小 | 211,019,832 字节 |
| 当前任务状态 | A1 三 lane BLOCKED；A2/A3 未运行 |

旧的 `2026-08-28-close` 检查点只作审计证据。新合同版本生效后不得直接恢复旧 A1 模型结果，因为旧输出不满足新权责和新 hash 合同。

### 2.2 A1 成功的必要条件

A1 只有同时满足以下条件才算完成：

1. 全部 G0 股票都有确定性筛选记录，不因传输批次或模型预算丢失。
2. 月度轮动前 20 个行业均有服务端基础决策和来源。
3. `READY` 月度上下文下，模型给出 8–15 个非重复结构主题和 40–80 个有证据产业链节点。
4. 每个基础 `INCLUDE` 行业均映射到至少一个有效主题；缺少映射时不得静默通过。
5. 模型引用的来源存在、未越过 `as_of`，并携带质量等级。
6. 三个模型使用同一基础研究包 hash；若启用 lane 记忆，必须另存 overlay hash 并在界面标记为非严格同输入。
7. A1 确定性筛选对 4,017 只完成后，模型只复核确定性候选子集。
8. 每个 lane 独立完成、阻断或降级；一个 lane 失败不伪造其他 lane 结果。
9. 所有输出、计数、股票代码、名称、原因和来源落盘并可由前端查看。

### 2.3 当前关键路径的性能下限

虚拟机当前公告请求全局最小间隔为0.5秒。只对4,017只G0执行一次近期公告查询，理论下限已经约为33.5分钟；7日年报缓存需要刷新时再增加约33.5分钟。这还不包含实际网络响应、失败重试、行情和财务同步。

本次还有307份PDF缓存未命中，PDF worker为2，单请求超时30秒，其中153份最终失败。因此约160分钟的数据准备并非前端误判，而是现有“研究时全市场查询”结构的可预期结果。实施W3时必须用阶段计时证明这些工作已离开研究关键路径，不能仅通过提高并发掩盖数据依赖顺序。

### 2.4 虚拟机资源基线

2026-08-29只读核验的生产资源：

| 资源 | 当前值 | 架构影响 |
|---|---:|---|
| CPU | 2 vCPU | 不适合多模型、多PDF和全市场计算同时高并发 |
| 内存 | 3.8 GiB | 211MB JSON对象化后会放大数倍 |
| 可用内存 | 约1.4 GiB | 研究进程必须有RSS门和分阶段释放 |
| Swap | 2.1 GiB，已用约1.6 GiB | 已存在明显换页压力，不能以“未OOM”视为健康 |
| 根分区 | 38GB，剩余约11GB | 必须有引用感知保留和磁盘水位门 |
| 项目storage | 约4.6GB | 其中事实库约3.0GB、快照约1.6GB |
| 事实SQLite | 约2.72GB | 需要批量查询、单写者和维护窗口 |
| 文件描述符上限 | 1024 | 网络/PDF并发和SQLite连接必须有界 |

上述数值是本方案的容量基线，不是临时观察项。生产恢复前必须在同等资源上完成冷启动、热缓存和崩溃恢复测试；不得用开发机性能替代虚拟机验收。

---

## 3. 目标架构

### 3.1 两个运行平面

```text
数据维护平面（长期、增量、可重试）
  同花顺目录 / 行情 / 财务 / 行业成员
  巨潮与北交所公告 / PDF证据
  政策 / 宏观 / 行业活动 / 资讯线索
  ↓
  本地事实库 + 内容寻址文件 + 数据水位 + 失败队列
  ↓
研究运行平面（固定 as_of、可回放）
  全市场质量门 G0
  → 构建紧凑 A1ResearchPacket
  → 三模型宏观/主题/产业链发现
  → 服务端合并月度20行业决策
  → 全部 G0 确定性筛选
  → 候选子集证据补充
  → A1模型复核
  → A2 → A3
```

数据维护平面可以持续数小时，但不能占用每日研究任务的关键路径。研究任务只验证每类事实是否达到规定水位，并从本地版本化事实创建逻辑快照。

### 3.2 A1 内部阶段

```text
A1.0 DATA_READINESS
  验证全市场目录、行情、财务、政策、宏观、行业和公告水位

A1.1 PACKET_BUILD
  从完整快照计算紧凑宏观研究包；不做个股选择

A1.2 MACRO_DISCOVERY
  每个模型一次主题与产业链发现

A1.3 DISCOVERY_JOIN
  服务端把20条基础行业决策与模型映射合并

A1.4 DETERMINISTIC_SCREEN
  对全部G0计算主营、财务、行业、催化、质量与流动性

A1.5 CANDIDATE_ENRICHMENT
  只补候选子集的公告/PDF/主营缺口；已缓存数据直接读取

A1.6 LLM_REVIEW
  按产业节点分批复核候选，不让模型重算全市场硬规则

A1.7 GLOBAL_MERGE
  合并所有批次并校验 A1 ⊆ G0、计数、证据与原因
```

### 3.3 A1股票池形成边界

全4,017只G0在 `DETERMINISTIC_SCREEN` 均产生且只产生一条结果：

- `ACTIVE_CANDIDATE`：主题、主营、财务和证据达到确定性门。
- `MONITOR_DATA_GAP`：方向成立但关键主营、财务或催化证据不足。
- `REJECTED`：明确不满足质量、主题、主营或风险门。

线索池300–800只、ACTIVE研究池100–250只是运行健康和研究容量目标，不是性能配额。少于目标时必须报告是行业覆盖不足、事实缺失还是市场确实无机会；超过目标时保留完整判定，并通过确定性证据排序决定模型代表样本和发布层级，禁止静默删除超出数量的股票。

LLM只复核各产业节点的代表候选和确定性冲突项。一个本地证据完整的候选不能仅因未被发送给模型而被淘汰；模型也不能把缺乏主营证据的股票直接提升为ACTIVE。

---

## 4. 工作包 W1：建立 A1 单一权威合同

### 4.1 新增模块

新增 `src/liangjian_funnel/pipeline/a1_contract.py`，集中定义：

- `A1_CONTRACT_VERSION = "a1-discovery-contract/3.0.0"`
- `A1ResearchPacket`
- `CanonicalMonthlyIndustryDecision`
- `IndustryThemeMapping`
- `A1DiscoveryOutput`
- `A1DiscoveryValidation`
- 主题、节点、来源、计数和状态枚举
- 合同到运行时说明的渲染函数
- 合同到安全前端摘要的投影函数

不得在 YAML、提示词和校验器中分别维护互相独立的数字。

### 4.2 配置只保留策略参数

`config/funnel_config_v2.yaml` 保留：

```yaml
agent_1:
  discovery_contract_version: a1-discovery-contract/3.0.0
  monthly_rotation_decision_top_n: 20
  monthly_theme_target: [8, 15]
  node_count_target: [40, 80]
  minimum_ready_policy_documents: 1
  strict_pit_required_for_replay: true
  evidence_aware_coverage: true
```

删除或废弃会产生双重解释的 `monthly_rotation_top10_required`。Top10 可以保留为报告指标，但不能再改变“前20逐项有基础决策”的完整性合同。

### 4.3 模型输入和输出权责

模型不再返回完整 `monthly_industry_decisions`，只返回映射：

```json
{
  "industry_theme_mappings": [
    {
      "industry_thscode": "881105.TI",
      "mapped_theme_ids": ["theme-resource-cycle"],
      "mapping_status": "MAPPED",
      "supporting_source_refs": ["derived:ths-sector-cycle:..."],
      "contradicting_source_refs": [],
      "data_gaps": [],
      "confidence": 0.82
    }
  ]
}
```

服务端权威决策结构：

```json
{
  "rank": 1,
  "industry_thscode": "881105.TI",
  "industry_name": "煤炭开采加工",
  "base_decision": "INCLUDE",
  "base_reason_codes": ["MONTHLY_RS_PERSISTENT"],
  "base_source_refs": ["derived:ths-sector-cycle:..."],
  "mapped_theme_ids": ["theme-resource-cycle"],
  "mapping_status": "MAPPED",
  "mapping_source": "MODEL",
  "final_decision": "INCLUDE",
  "data_gaps": []
}
```

合并规则：

1. 模型不能把服务端 `EXCLUDE` 改成 `INCLUDE`。
2. `INCLUDE` 未映射时进入字段级修复；仍未映射则该行业最终为 `DEFER_MAPPING_GAP`，该 lane 状态至少降为 `DEGRADED`。
3. `EXCLUDE` 和 `DEFER` 可以没有主题映射，但必须保留服务端原因和来源。
4. 模型返回未知行业代码、重复代码或不存在的主题 ID 时只拒绝对应映射，不污染其他有效行。
5. 最终 20 行由服务端按冻结排名排序，模型不得改变排名。

### 4.4 提示词改造

修改 `prompts/agent_1_macro_chain_v2.txt`：

- 删除让模型复制服务端20条基础决策的要求。
- 删除“前10不得遗漏”和“前20逐项输出”并存的表达。
- 明确输入中 20 条是只读基础决策。
- 明确模型必须为所有基础 `INCLUDE` 行业给出主题映射或 `UNMAPPED` 原因。
- 明确输出只包含 `structural_themes`、`industry_chain_graph`、`taxonomy_links`、`industry_theme_mappings` 和摘要。
- 主题和节点必须引用研究包内的稳定 `source_ref`。
- 不再把通用 A1 公司池 Schema 塞入宏观发现请求。

### 4.5 合同验收测试

新增 `tests/test_a1_contract.py`：

- 配置值、合同值和渲染后的运行时说明完全一致。
- 渲染文本中不存在会改变合同含义的 Top10/Top20冲突。
- 模型缺一条映射时只报告精确行业代码。
- 未知或重复行业代码不能覆盖合法行。
- 20条服务端基础决策始终完整保留。
- 旧输出带 `monthly_industry_decisions` 时只允许通过显式迁移器读取，不参与新合同验证。

---

## 5. 工作包 W2：构建紧凑、完整、可审计的 A1 研究包

### 5.1 新增模块

新增 `src/liangjian_funnel/pipeline/a1_packet.py`，禁止继续由通用 `_prompt_replacements()` 直接向 A1 宏观请求注入原始 `MACRO_ECONOMIC_DATA` 和 `INDUSTRY_ACTIVITY_DATA`。

研究包结构：

```json
{
  "schema_version": "a1-research-packet/1.0.0",
  "contract_version": "a1-discovery-contract/3.0.0",
  "snapshot_id": "...",
  "snapshot_hash": "...",
  "as_of": "...",
  "packet_hash": "...",
  "quality_summary": {},
  "macro_asset_quadrant": {},
  "macro_features": [],
  "policy_dossiers": [],
  "cross_market_leads": [],
  "broker_research_consensus": [],
  "industry_features": [],
  "canonical_monthly_decisions": [],
  "prior_theme_registry": {},
  "source_index": {},
  "coverage": {}
}
```

### 5.2 宏观特征投影

每个宏观序列保留完整的本地原始历史，但研究包只放确定性特征：

- 序列名、单位、来源。
- 最新观察值、观察期、发布时间、抓取时间。
- 1/3/6/12个月变化。
- 同比、环比。
- 3/6/12个月线性斜率。
- 近5年或可用历史分位数。
- 最近拐点、连续上升/下降期数。
- `pit_verified`、`quality_tier`、缺失字段。
- 对应 `source_ref`。

模型需要追溯原始历史时，结果中只能提出 `data_request`，不得让运行时重新把全部历史拼入同一次请求。

### 5.3 行业特征投影

82个行业均必须保留一行，不能随机截断。每行包括：

- 同花顺行业代码和名称。
- 5/10/20/60日收益与相对指数强弱。
- Top10出现次数、持续率和最近进入/退出时间。
- 成交额、换手和持续性特征。
- 可用的行业利润、价格、库存或景气代理。
- 政策和券商共振计数，但不把推荐名单直接作为入选条件。
- 数据质量、缺失项和来源。

完整月度序列仍保存在本地事实库；研究包不得携带每个行业的所有原始月份行。

### 5.4 政策、跨市场与券商研究

- 政策文档继续执行时间窗、机构和主题多样性选择，默认最多60份。
- 每份只保留标题、机构、文号、时间、1200字内确定性摘要、政策阶段、传导标签和来源。
- 跨市场只保留先行资产的最新状态、财报/事件摘要、映射链和时间差。
- 券商月度金股及研究共识作为研究校验和共振特征，不作为硬编码入选名单。
- 所有文本在进入模型前执行提示注入检测和来源白名单检查。

### 5.5 来源索引

取消当前“将所有来源排序后截取前512个”的弱关联方式。每个特征和政策条目直接携带自己可引用的 `source_refs`；`source_index` 只保存本包实际出现的引用及其质量摘要。

### 5.6 大小预算

初始工程预算：

| 项目 | 目标 |
|---|---:|
| A1宏观发现估算输入 | ≤100,000 token |
| 原始快照保留率 | 100%本地保留 |
| 82个行业投影覆盖 | 100% |
| 月度前20行业决策 | 100% |
| 选定正式政策文档 | 100%进入投影，最多60份 |
| 同一基础包跨模型 hash | 完全相同 |

若超过预算，构建器必须返回每个数据块的字符/token占比并失败关闭；不得通过随机切片、股票 Top-N 或静默删除行业来过关。应继续优化特征表达或消除重复字段。

### 5.7 测试

新增 `tests/test_a1_packet.py`：

- 用2026-08-28快照的脱敏结构夹具验证82行业和20决策完整。
- 原始宏观历史变化会改变 packet hash 和派生特征。
- 不同字典顺序不能改变 packet hash。
- 研究包中不出现完整 `series`/`items` 原始历史数组。
- 每个来源引用都存在于 `source_index`。
- 重复数据块不会同时出现在 system 和 user 消息。
- 大小超限时返回 `A1_PACKET_TOO_LARGE` 和分块诊断。

---

## 6. 工作包 W3：拆分全量数据维护与研究关键路径

### 6.1 数据维护任务

扩展现有 `sync-data`，按 namespace 建立可独立续跑的维护阶段：

1. `UNIVERSE_CATALOG_SYNC`
2. `DAILY_MARKET_SYNC`
3. `FINANCIAL_INCREMENTAL_SYNC`
4. `INDUSTRY_MEMBERSHIP_SYNC`
5. `INDUSTRY_HISTORY_SYNC`
6. `ANNOUNCEMENT_INCREMENTAL_SYNC`
7. `PDF_EVIDENCE_QUEUE`
8. `POLICY_SYNC`
9. `OPEN_MACRO_SYNC`
10. `NEWS_CLUE_SYNC`

每个 namespace 有独立游标、水位和失败队列，不能因一个 PDF 失败重新扫描全部市场。

### 6.2 缓存与水位表

在 `LocalFactCache` 的下一 schema migration 中增加：

```sql
data_watermarks(
  namespace,
  source_id,
  cursor,
  max_event_time,
  last_attempt_at,
  last_success_at,
  record_count,
  symbol_count,
  coverage_ratio,
  quality_tier,
  status,
  reason_code,
  version_hash
)

sync_failures(
  namespace,
  item_key,
  reason_code,
  attempts,
  first_failed_at,
  last_failed_at,
  next_retry_at,
  terminal,
  payload_hash
)
```

失败队列只保存稳定标识和安全原因，不保存密钥、认证头或未脱敏响应。

### 6.3 公告与PDF增量策略

当前每只 G0 同时查询近10日公告和450日年报，且近期公告缓存仅6小时。修复后：

- 优先使用公告全局时间游标或分页增量能力；若数据源不支持，则把按股票分片扫描放到维护任务，不放在研究任务中。
- 年报历史只在新披露、版本变化或7日校验水位到期时更新。
- 新公告写入后才创建对应 PDF 任务。
- PDF任务按公告ID幂等；成功结果永久复用，内容哈希变化才重抓。
- 失败按原因分类退避，不让153个失败文档每次全部占满30秒超时。
- A1候选若依赖的关键主营文档缺失，可触发高优先级补抓；它仍是候选级任务，不扫描全市场。

### 6.4 研究数据就绪门

新增 `src/liangjian_funnel/pipeline/data_readiness.py`。研究启动时只进行：

1. 检查全市场目录版本和数量异常。
2. 检查行情、财务、行业、政策、宏观和公告水位。
3. 对当天行情和行业指数做小范围快速刷新。
4. 生成 `DataReadinessReport`。

建议初始资格：

| 数据集 | 收盘研究要求 | 历史回放要求 |
|---|---|---|
| 证券目录/交易状态 | 当日 | 冻结版本 |
| 日行情与成交额 | 当日收盘或明确延迟 | 严格不晚于 as_of |
| 财务和主营 | 最新已披露版本 | 发布时间严格 PIT |
| 行业成员 | 有版本和生效时间 | 版本可回放 |
| 政策 | 最近成功水位，逐文档有发布时间 | 严格 PIT |
| 宏观 | T1/T2均可但必须分级 | 默认只允许T1；T2只能显式降级测试 |
| 公告索引 | 增量水位可证明 | 严格不晚于 as_of |
| PDF正文 | 关键候选缺失时降级/阻断 | 只使用当时可得版本 |

水位不合格时返回具体 namespace 和时间，不进入模型。水位合格但少量非关键文档失败时允许 `READY_DEGRADED`，不得重新执行全量同步。

### 6.5 快照演进

分两步实施：

**阶段一：兼容当前嵌入式快照。** 保留现有完整 JSON 和 hash，先解除大提示词与全量同步阻断。

**阶段二：引入 manifest-backed snapshot v3。** 快照只保存数据集版本、内容 hash、查询水位和事实分片引用；完整事实留在 SQLite/内容寻址文件中。回放按 manifest 读取同一版本，避免每次反序列化约211MB对象。

阶段二不能与P0合同修复混为一个不可回滚的大改动，必须在首次 A1 跑通后单独验收。

---

## 7. 工作包 W4：PIT与证据资格进入决策合同

### 7.1 统一证据质量结构

在 `src/liangjian_funnel/data/quality.py` 扩展：

```json
{
  "quality_tier": "T1_STRICT_PIT | T2_OBSERVATION_DATE_ONLY | T3_CURRENT_ONLY | UNAVAILABLE",
  "observed_at": "...",
  "published_at": "...",
  "fetched_at": "...",
  "pit_verified": true,
  "eligible_for_live": true,
  "eligible_for_replay": true,
  "reason_codes": []
}
```

### 7.2 月度上下文状态规则

`build_monthly_strategy_context()` 不再只看 `available`：

- `READY`：政策、宏观、行业周期核心支柱满足当前运行模式的质量门。
- `READY_DEGRADED`：实时研究允许的T2数据存在，所有影响在置信度和报告中显式记录。
- `BLOCKED_PIT_QUALITY`：回放请求使用了无法证明发布时间的T2/T3数据。
- `BLOCKED_MISSING_PILLAR`：核心支柱不可用。

模型不能把 `READY_DEGRADED` 自行提升为 `READY`。

### 7.3 来源校验

`_a1_discovery_evidence_reasons()` 拆为：

- 引用存在性校验。
- 引用与当前研究包关联校验。
- 发布时间不晚于 `as_of` 校验。
- 运行模式资格校验。
- 事实层级和文本注入校验。

原因码必须能区分不存在、越时、质量不足和来源类型错误。

---

## 8. 工作包 W5：重新组织 A1 模型执行、对比与恢复

### 8.1 共享基础研究包

三个 lane 首先共享：

- 同一个 `snapshot_hash`。
- 同一个 `packet_hash`。
- 同一个 `contract_version`。
- 同一个确定性20行业决策集。
- 同一个共享历史主题注册表。

默认模型比较模式设为 `CONTROLLED`。各模型上一期私有主题记忆仍然保存用于评估，但不进入本期基准输入。

`CONTROLLED` 使用运行开始前已经冻结的同一份 `shared_theme_registry`；没有合法共享版本时使用空注册表，不能临时选取某一个成功lane的私有结果。共享注册表的每个主题保留来源lane、首次/最后出现月份和共识计数，但这些字段只用于延续性研究，不作为本期硬入选条件。

若未来启用 `CONTINUOUS_LANE_MEMORY`：

- `base_packet_hash` 仍一致。
- 私有记忆形成独立 `lane_overlay_hash`。
- 前端明确显示“连续策略对比，非严格同输入”。

### 8.2 顺序执行

保持 DeepSeek → Kimi → GLM 顺序，`research_batch_workers=1`。先通过输入压缩解决时间和内存问题，不贸然恢复三模型并行。

每个 lane 独立：

- discovery checkpoint。
- 模型请求审计。
- 输出和修复记录。
- 后续 A1候选复核。
- A2/A3状态。

一个 lane 模型失败后继续下一个 lane；失败 lane 的 A2/A3 保持上游阻断。

### 8.3 请求大小和输出档位

- 请求前记录字符数、估算输入 token、各数据块占比。
- 输入超过研究包预算时不调用模型。
- 384K → 256K → 128K 保留为模型网关输出容量兼容档位。
- A1宏观发现自身的预期有效 JSON 应远低于上述输出上限；不得通过提高输出上限掩盖重复内容。
- 每次容量降档必须记录档位和触发错误，不能把业务字段静默删掉。

### 8.4 重试分类

| 类型 | 处理方式 |
|---|---|
| 429 | 尊重 `Retry-After`，指数退避和抖动；不改变输入合同 |
| 5xx/断流 | 首次切流式传输，再按有界预算重试 |
| 总时限 | 保存传输审计，lane 阻断；不生成空业务结果 |
| JSON机械问题 | 本地安全修复，记录前后结构差异 |
| 缺失映射 | 发送缺失行业、现有主题和相关证据的最小修复请求 |
| 无效主题/节点 | 最多一次完整语义重生成 |
| 来源越时/不存在 | 不重试模型，直接证据阻断 |

### 8.5 字段级修复协议

修复请求不得重新发送完整宏观历史，只包含：

- 原请求 `packet_hash`、`prompt_hash`、`output_hash`。
- 精确 JSON path 或缺失行业代码。
- 已验证的主题ID、节点ID。
- 与缺失项直接相关的行业特征和来源。
- 允许返回的字段 Schema。

建议限制为：一次完整生成、最多一次完整语义重生成、最多两次字段级修复。超过后明确阻断，避免无限循环。

### 8.6 安全审计产物

持久化：

- HTTP状态、尝试数、流式/非流式。
- 首字节和总耗时。
- 输入字符/token估算和输出字符数。
- JSON顶层字段、主题数、节点数、映射数。
- 缺失代码和原因码。
- 最终结构化响应 hash。

不持久化模型思考正文、密钥、认证头、Cookie或未脱敏网关错误正文。

---

## 9. 工作包 W6：统一业务校验和状态语义

### 9.1 分层校验

A1宏观输出依次经过：

1. JSON和类型校验。
2. 合同版本和 envelope 校验。
3. 主题唯一性与范围校验。
4. 节点唯一性、主题血缘和范围校验。
5. 行业映射完整性校验。
6. 来源和PIT资格校验。
7. 业务覆盖校验。
8. 服务端确定性合并校验。

每层只产生自己的原因码，禁止把一次缺失映射扩展成“主题不足、节点不足、输出缺失”等二次误导原因。

### 9.2 主题和节点目标

当月度上下文为 `READY` 且G0大于500：

- 主题少于8或多于15：阻断。
- 节点少于40或多于80：阻断。
- 禁止通过同义词拆分凑数；使用标准化名称、产业位置和成员重叠检测重复节点。

当上下文为 `READY_DEGRADED`：

- 不降低最低业务目标后伪装成 `VALIDATED`。
- 模型若有完整覆盖可继续；否则状态为 `DEGRADED_COVERAGE`，保存缺口并禁止激活到模拟计划。

### 9.3 建议状态

```text
DATA_NOT_READY
PACKET_BUILDING
PACKET_READY
MODEL_RUNNING
MODEL_REPAIRING
DISCOVERY_VALIDATED
DISCOVERY_DEGRADED
BLOCKED_CONTRACT_MISMATCH
BLOCKED_PACKET_TOO_LARGE
BLOCKED_MODEL_TIMEOUT
BLOCKED_MODEL_TRANSPORT
BLOCKED_EVIDENCE_QUALITY
BLOCKED_DISCOVERY_COVERAGE
DETERMINISTIC_SCREENING
CANDIDATE_ENRICHING
A1_REVIEWING
A1_VALIDATED
NOT_RUN_UPSTREAM_BLOCKED
```

顶层 `status=BLOCKED` 时仍保留 `last_business_phase`，不能把前端阶段覆盖成无法解释的“未知阶段”。

---

## 10. 工作包 W7：进度、前端和运行详情

### 10.1 Python进度合同

扩展 `runtime/progress.py`，新增安全字段：

- `pipeline_version`
- `contract_version`
- `snapshot_id`
- `packet_hash_prefix`
- `last_business_phase`
- `full_universe_count`
- `g0_count`
- `industry_count`
- `monthly_decision_count`
- `packet_estimated_tokens`
- `packet_section_sizes`
- 每 lane 的主题、节点、映射、修复、缺失数量
- `blocking_reason_summary`

宏观发现没有股票输入时，前端显示“主题/节点/行业映射”，不再显示“股票计数未提供”。

### 10.2 Node安全投影

同步更新：

- `server/types.ts`
- `server/files.ts`
- `server/dashboard.ts`
- `server/api.ts`

只暴露白名单统计和稳定原因码，不把原始提示词、模型正文或来源全文返回浏览器。

### 10.3 前端展示

更新：

- `web/src/types.ts`
- `web/src/App.tsx`
- `web/src/styles.css`

进度卡按当前阶段切换指标：

| 阶段 | 主指标 |
|---|---|
| 数据就绪 | namespace 水位与缺失项 |
| 研究包构建 | 行业数、决策数、估算 token |
| 宏观发现 | 主题、节点、行业映射、修复次数 |
| 确定性筛选 | 已处理股票/G0、ACTIVE/观察/淘汰 |
| 候选证据 | 候选数、缓存命中、补抓失败 |
| A1模型复核 | 批次、股票、通过/观察/淘汰 |

失败详情弹窗显示：

- 失败阶段。
- 直接原因。
- 预期值与实际值，例如“行业映射 10/20”。
- 缺失行业代码/名称。
- 是否可续跑。
- 建议动作是“修复合同”“等待数据维护”还是“重试模型”。

---

## 11. 工作包 W8：检查点、兼容和迁移

### 11.1 检查点键

新检查点键必须包含：

- `pipeline_mode`
- `contract_version`
- `snapshot_hash`
- `packet_hash`
- `prompt_hash`
- `model`
- `thinking_variant`
- `stage`
- `batch_symbols_hash`

任一项变化都不能复用旧模型输出。

### 11.2 旧检查点处理

- 不删除旧文件。
- 将旧 active marker 标记为 `INCOMPATIBLE_CONTRACT_VERSION`。
- 前端归档显示，不把它当成本次新任务进度。
- 新管线首次运行从 `PACKET_BUILD` 开始，允许复用同一快照的原始事实，但不复用旧 discovery 输出。

### 11.3 数据库迁移

迁移必须：

- 向前兼容当前 Node 控制台读取。
- 先增加表和可空字段，不破坏现有结果。
- 有 migration version 和幂等测试。
- 在测试库、复制的虚拟机状态库上验证后再部署。
- 不在实施A1合同修复时删除任何旧表或产物。

---

## 12. 工作包 W9：资源、并发与长期运行硬化

W1–W8解决业务合同、输入装配和数据职责，W9负责保证它们能在当前2 vCPU、3.8GiB虚拟机上长期运行。W9未通过前不得恢复正式定时研究。

### 12.1 冷启动与稳态分开验收

定义两个性能场景：

**冷启动/首次初始化：**

- 完整目录和历史事实首次落盘。
- 可以持续数小时，但必须分片、可续跑、可暂停。
- 每完成一个namespace或批次就原子写水位。
- 进程重启后只继续未完成项。
- 冷启动期间研究状态明确为 `DATA_BOOTSTRAP_INCOMPLETE`，不能启动模型。

**稳态每日研究：**

- 只处理新交易日、增量财务、增量公告和新PDF。
- 不全表重扫、不重建所有历史、不重新下载成功PDF。
- 数据维护和研究分别记录耗时、缓存命中和新增记录数。
- 性能结论至少基于连续3次同等虚拟机实测，报告冷/热缓存的P50、P95和峰值RSS，不能用一次幸运运行宣布完成。

### 12.2 进程内存与快照门

当前 `read_text + json.loads` 会同时持有UTF-8文本、Python字典/列表和校验副本。对211MB快照，实际内存不是211MB。要求：

1. 离线Phase 1–3可以暂时读取旧嵌入式快照，但必须采集阶段峰值RSS和swap增长。
2. 正式定时任务恢复前，必须满足以下二选一：
   - manifest-backed snapshot v3已经启用；或
   - 旧嵌入式路径在当前虚拟机实测峰值RSS不超过1.0GiB、系统 `MemAvailable` 不低于512MiB、单次运行swap增长不超过256MiB。
3. 禁止同时保留原始快照、sanitize副本、prompt replacement副本和模型消息四套完整对象。
4. 每个lane结束后释放该lane输出和请求缓冲；执行显式阶段边界，不在全局列表保留模型正文。
5. 研究进程记录 `rss_current_mb`、`rss_peak_mb`、`system_mem_available_mb` 和 `swap_used_mb`。
6. 资源门不满足时返回 `BLOCKED_RESOURCE_PRESSURE` 并保留checkpoint，禁止依赖OOM Killer终止。

### 12.3 SQLite并发一致性

当前事实库和状态库均为WAL，但事实维护和研究读取仍需明确并发合同：

- 数据维护是事实库单写者；研究和Node只读。
- 研究开始时冻结watermark/version hash，并在同一只读事务或不可变版本上读取。
- 维护任务可以继续写下一版本，但不能改变当前研究所引用的事实版本。
- 写入使用短事务和有界批次，禁止在网络请求期间持有SQLite写锁。
- 统一 `busy_timeout` 配置，避免当前不同连接出现5秒/15秒语义漂移。
- WAL checkpoint、`PRAGMA optimize`和必要的增量维护只在维护窗口执行，不能与模型研究争用IO。
- migration、VACUUM或大索引创建必须停研究后单独执行。

必须加入“维护写入与研究读取同时运行”的一致性测试：研究输出的snapshot/packet hash在整个run内保持不变。

### 12.4 防止N+1和全文件解析

manifest-backed阶段不能把当前内存映射改造成每只股票逐次查询数据库。约束：

- 行情、财务、主营、行业成员和风险事实均按symbol集合分块批量查询，建议每批200–500只。
- 查询次数应与namespace和批次数量相关，不能与股票数线性形成4,017次连接。
- 关键SQL加入 `EXPLAIN QUERY PLAN` 回归夹具和索引存在性测试。
- 记录每阶段SQL语句数、读取行数和耗时，超过预算直接告警。
- 4,017只确定性筛选只在内存中的紧凑特征表上单遍运行。

前端现有分页只限制返回50条，但服务端仍会读取和解析完整lane JSON。新结果需要增加持久化 `stage_decisions` 索引表或分片NDJSON+索引：

- 按 `run_id + lane_id + stage + pool + symbol` 查询。
- 服务端分页、搜索和原因筛选在存储层完成。
- 每行保存股票代码、名称、池、分数、原因、证据摘要和详情文件引用。
- Markdown仍是最终人类报告；逐股全量明细放结构化存储，Markdown给出汇总和结果文件位置。
- Node不得为了一个弹窗读取211MB快照或完整4,017条大JSON。

### 12.5 调度优先级与任务重叠

Node当前只有一个进程内 `active` 标记，close任务无Node总超时，且monitor/close可能跨分钟竞争。改造要求：

1. 使用SQLite持久化运行租约和心跳，Node重启后仍能识别存活或可恢复的Python任务。
2. 优先级固定为：`close/morning research > monitor > data maintenance`。
3. 09:26和15:10前设置monitor派发保护窗；已经运行的分钟任务必须在研究时点前有界结束。
4. 数据维护在研究开始时协作暂停，完成当前小批次后释放IO；研究结束后从watermark继续。
5. close和morning同一逻辑日期只能各有一个权威run；重复触发返回现有run ID，不创建第二套模型调用。
6. 研究整体增加wall-clock期限。初始保护上限为90分钟，后续以连续3次实测SLO调整；超时后保存合法checkpoint并返回 `RETRYABLE_DEADLINE`，不能无限挂起。
7. 调度器不能只在原触发分钟内重试BUSY任务；必须保存截止时间，并在保护窗口内按同一dispatch key恢复。
8. SIGTERM先停止领取新批次、刷新checkpoint和释放租约；超过宽限期才允许SIGKILL，并把run标记为 `INTERRUPTED_UNSAFE`。

### 12.6 模型能力与预算单位

不能继续用一个全局100万输入上限代表三个不同模型。新增每模型能力配置：

- `context_limit_tokens`
- `max_output_tokens`
- `supports_stream`
- `thinking_field_variant`
- `timeout_seconds`
- `output_budget_ladder_tokens`
- 最近一次能力探针时间和结果

所有配置字段必须明确单位是token，不允许把字符、字节、token或网关私有单位混用。请求前执行：

`estimated_input + requested_output + safety_margin <= model_context_limit`

中文token估算不得假设固定4字符/token；保留字符数、估算token和提供方返回的实际usage，使用实测误差校正安全系数。384K/256K/128K输出档位只有在模型能力配置证明支持时才能发送；A1宏观Schema另设合理的预期输出范围并在超常膨胀时中止流。

A1提示词实行placeholder白名单：只允许A1研究包、合同和最小通用安全规则。编译测试必须证明未混入A2/A3池Schema、完整快照、重复宏观数据或无绑定placeholder。

### 12.7 磁盘、WAL和保留策略

完整历史必须保留，但不等于永久保留多个重复的200MB嵌入式快照。采用引用感知策略：

- 事实修订、研究结果、合同、manifest和来源hash长期保留。
- 内容相同的事实/PDF/快照分片按hash去重。
- 旧嵌入式快照迁移为manifest并校验可回放后，可压缩归档其重复JSON正文；任何仍被结果或checkpoint引用的对象禁止清理。
- 每日临时文件、失败下载残片和已合并中间文件有明确TTL。
- SQLite WAL设置告警和安全checkpoint，禁止无限增长。
- 磁盘剩余低于20%或8GB时告警；低于10%或4GB时阻断新的重型同步，但仍允许读取、导出和安全清理。
- 每次清理先生成候选清单和引用证明；生产清理是独立授权动作，不由研究任务自动执行。

### 12.8 限流、缓存击穿与来源漂移

- 同一source/item的并发请求合并为一个future，避免缓存击穿。
- 每来源独立令牌桶和并发上限，不能用线程数绕过全局0.5秒节流。
- 429遵守Retry-After且受任务总期限约束；过了期限转失败队列，不跨阶段无限等待。
- 5xx、超时和解析失败分别统计，失败缓存有原因相关TTL。
- 每个适配器记录响应Schema指纹；字段漂移先隔离到 `SOURCE_SCHEMA_DRIFT`，不能把缺字段当0写入。
- 关键数据不使用stale-while-revalidate；非关键T3线索可以显式使用旧版本，但必须显示stale age。

### 12.9 性能和故障注入测试

除现有单元/集成测试外增加：

- 2 vCPU/4GB cgroup或同等虚拟机资源测试。
- 211MB旧快照读取RSS基准。
- manifest v3与旧快照结果等价测试。
- 4,017只批量查询语句数和耗时测试。
- SQLite维护写入+研究读取并发一致性测试。
- 429、5xx、断流、慢首token、JSON截断和超大输出故障注入。
- SIGTERM、Node重启、Python崩溃、租约过期和checkpoint续跑测试。
- 磁盘低水位、WAL膨胀和临时文件残留测试。
- 连续3个交易日等价的soak test，验证内存、文件描述符、线程和磁盘不持续增长。
- Node阶段详情在4,017条结果下的分页、搜索、内存和响应耗时测试。

---

## 13. 文件级改造清单

### 13.1 新增文件

| 文件 | 责任 |
|---|---|
| `src/liangjian_funnel/pipeline/a1_contract.py` | A1单一合同、类型、校验、合并规则 |
| `src/liangjian_funnel/pipeline/a1_packet.py` | 紧凑研究包与大小诊断 |
| `src/liangjian_funnel/pipeline/data_readiness.py` | 数据水位和运行资格 |
| `src/liangjian_funnel/runtime/resource_guard.py` | 虚拟机内存、磁盘和运行资源门 |
| `src/liangjian_funnel/pipeline/result_index.py` | 全量阶段结果索引和存储层分页 |
| `tests/test_a1_contract.py` | 合同一致性和20行业决策测试 |
| `tests/test_a1_packet.py` | 投影完整性、大小和hash测试 |
| `tests/test_data_readiness.py` | 数据水位、降级和PIT资格测试 |
| `tests/test_resource_guard.py` | RSS、磁盘、swap和低水位行为测试 |
| `tests/test_result_index.py` | 4,017条结果分页、搜索和原因过滤测试 |
| `tests/fixtures/a1_packet_20260828_summary.json` | 脱敏、紧凑的生产基线夹具 |

### 13.2 修改文件

| 文件 | 主要改动 |
|---|---|
| `config/funnel_config_v2.yaml` | 删除合同歧义，引用版本化A1合同 |
| `prompts/agent_1_macro_chain_v2.txt` | 改为主题、节点和行业映射输出 |
| `pipeline/monthly_strategy.py` | 服务端20决策、质量感知上下文 |
| `pipeline/research.py` | 调用新合同/研究包；拆分校验和字段修复 |
| `pipeline/prompts.py` | A1专用渲染，不重复注入事实 |
| `pipeline/model_client.py` | 传输诊断、Retry-After、5xx流式切换 |
| `pipeline/research_checkpoint.py` | 新hash和合同版本兼容门 |
| `pipeline/local_fact_cache.py` | 水位、失败队列和迁移 |
| `pipeline/data_sync.py` | namespace增量同步和续跑 |
| `data/quality.py` | T1/T2/T3/PIT统一资格 |
| `workflow.py` | 数据就绪→快照→研究包→A1新阶段顺序 |
| `runtime/progress.py` | 分阶段计数、最后业务阶段和阻断摘要 |
| `runtime/scheduler.py`、`runtime/state.py` | 持久租约、心跳、截止时间和恢复语义 |
| `settings.py` | 每模型能力、资源门和单位明确的预算配置 |
| `server/runner.ts`、`server/scheduler.ts` | 任务优先级、持久运行态、整体期限和优雅终止 |
| `server/types.ts`、`server/files.ts`、`server/dashboard.ts` | 安全读取新进度合同和存储层分页 |
| `web/src/types.ts`、`web/src/App.tsx`、`web/src/styles.css` | 阶段化指标和失败详情 |
| 现有相关测试 | 更新旧合同期望并保留兼容测试 |

`research.py` 当前职责过多。实施时应把新增逻辑放到上述新模块，避免继续扩大单文件；但不在本轮顺手重构无关 A2/A3 代码。

---

## 14. 实施阶段与提交边界

### Phase 0：冻结基线和测试夹具

工作：

- 记录基线快照、配置、提示词和失败原因。
- 生成脱敏A1输入组成统计和82行业/20决策夹具。
- 增加现有行为的失败复现测试。

退出条件：测试能够稳定复现“运行时合同允许Top10、校验要求20”的冲突，以及当前约44万 token 输入。

### Phase 1：单一合同和服务端决策合并

工作：W1全部内容。

退出条件：

- 模型不再输出基础行业决策。
- 服务端20行始终完整。
- 缺失映射只影响对应行。
- 合同文本无Top10/Top20歧义。

### Phase 2：紧凑研究包

工作：W2全部内容。

退出条件：

- 82行业、20决策、政策和宏观支柱完整。
- 输入估算 ≤100K token。
- packet hash稳定。
- 原始快照和事实未删减。

### Phase 3：A1离线三模型验证

工作：

- 使用同一2026-08-28冻结包依次调用三模型。
- 验证主题、节点、映射、来源和字段级修复。
- 不执行A2/A3，不写模拟账户。

退出条件：至少证明每个模型的失败原因准确可解释；不能以一个成功模型代替三个模型。目标是三个 lane 均形成合法 discovery。

### Phase 4：数据维护平面和关键路径拆分

工作：W3、W4和W9中的并发、资源、磁盘及调度硬化。

退出条件：

- 研究任务不执行全市场公告/PDF扫描。
- 维护任务可独立续跑。
- 水位不合格时在模型调用前阻断。
- 153类失败只进入失败队列，不重启全市场同步。
- 研究与维护不争用同一权威版本；close/morning不被monitor长期阻塞。
- 在当前虚拟机达到RSS、swap和磁盘门；生产路径不再依赖完整211MB JSON对象化，或有实测资源证明。

### Phase 5：全量A1冻结回放

工作：

- 同一快照对全部4,017只执行确定性筛选。
- 只对候选子集做证据装配和模型复核。
- 输出逐股结果和完整原因。

退出条件：所有G0逐只有记录；A1池数量由证据阈值自然形成，不使用性能配额。

### Phase 6：状态、前端和恢复

工作：W5–W8剩余内容。

退出条件：前端能准确展示每个阶段、计数、缺失项、模型结果和是否可恢复；旧检查点不污染新运行。

### Phase 7：A1→A2→A3全链验证

工作：

- 使用冻结基线离线验证。
- 再做一次虚拟机正式非交易连接的收盘研究。
- 核对JSON、Markdown、SQLite和前端一致性。

退出条件：三lane结果独立落盘；A2/A3只在各自上游合法时运行；不连接外部交易。

每个Phase单独提交。不得把合同、数据迁移、前端和部署混成一个无法定位回归的大提交。

---

## 15. 测试矩阵

### 15.1 单元测试

- 20行业基础决策生成、合并和顺序。
- 主题/节点计数、去重和血缘。
- 行业映射字段级修复。
- 宏观/行业特征派生。
- packet hash和大小预算。
- T1/T2/T3实时与回放资格。
- 水位和失败队列退避。
- 检查点版本不兼容处理。
- 原因码只指向最早直接失败层。
- 模型预算字段单位和每模型能力门。
- 资源低水位与持久租约状态机。

### 15.2 合同测试

- YAML、Python合同、提示词和运行时文本的一致性。
- 三模型请求 `base_packet_hash` 一致。
- 输出Schema不要求模型重复确定性字段。
- 前端类型与Python进度字段一致。
- 旧版本结果读取不等于新版本通过。
- A1渲染不包含A2/A3 Schema、完整快照或重复placeholder。

### 15.3 集成测试

- 完整数据可用 → A1 discovery validated。
- T2宏观实时 → `READY_DEGRADED`。
- T2宏观历史回放 → `BLOCKED_PIT_QUALITY`。
- 模型返回10个映射 → 只修复剩余缺失映射，不丢20行基础决策。
- GLM超时 → GLM lane阻断，DeepSeek/Kimi结果仍保存。
- 一个PDF失败 → 候选按证据重要性降级，不重新扫描全市场。
- 研究期间数据维护运行 → 冻结as_of不漂移。
- 重启后从合法checkpoint续跑，不重复成功模型请求。
- 维护写入期间研究仍固定读取同一watermark和packet hash。
- 15:09 monitor未结束时15:10 close按优先级获得运行资格且不重复。
- 4,017条结果由存储层分页，Node不解析完整快照。

### 15.4 生产基线回放

必须对 `snapshot-20260828T210944+0800-2e278ff757d1` 记录：

- 原始快照 hash。
- 研究包 hash和token估算。
- 20行业决策。
- 各模型主题、节点、映射、耗时和修复。
- 全4,017只确定性筛选计数。
- A1 ACTIVE/观察/淘汰及逐股原因。
- 与券商月度金股盲测的覆盖报告，但盲测不参与筛选。
- 峰值RSS、swap变化、SQL语句数、磁盘新增量和文件描述符峰值。

### 15.5 性能验收

在虚拟机同等配置、缓存就绪的前提下：

| 稳态阶段 | 初始验收目标 |
|---|---:|
| 数据水位检查和快速刷新 | ≤5分钟 |
| A1研究包构建 | ≤60秒 |
| 单模型宏观发现 | 目标≤4分钟，硬上限保持现有模型总期限 |
| 三模型顺序宏观发现 | 目标≤15分钟 |
| 全4,017只确定性筛选 | ≤2分钟 |
| 缓存就绪的完整A1 | 目标≤30分钟 |

首次冷启动不套用稳态30分钟目标，但必须报告总量、速度、ETA、失败队列、资源峰值和可续跑证据。稳态指标必须在同一虚拟机连续3次运行后同时报告P50/P95；任何一次OOM、任务重复、快照漂移或磁盘低水位均视为失败。性能目标不达标时先分析分阶段指标，禁止通过缩减全市场、减少82行业、减少20决策或降低40节点目标来达标。

---

## 16. 部署、切换与回滚

### 16.1 部署前门禁

- 本地完整 Python 测试通过。
- Node/Vitest测试通过。
- TypeScript构建通过。
- 冻结快照离线回放通过。
- Git工作树仅包含本次范围内修改。
- 虚拟机当前无研究任务运行。
- 备份状态数据库、旧active marker、配置和当前部署提交。
- 虚拟机磁盘、RSS、swap、文件描述符和SQLite/WAL检查通过。
- 同等资源的冷启动/稳态/崩溃恢复测试通过。

### 16.2 功能开关

建议增加：

```text
LIANGJIAN_A1_PIPELINE_MODE=contract_v3
LIANGJIAN_A1_COMPARE_MODE=controlled
LIANGJIAN_A1_PACKET_MAX_TOKENS=100000
LIANGJIAN_RESEARCH_DATA_MODE=readiness_gate
LIANGJIAN_SNAPSHOT_MODE=embedded_v2
```

`embedded_v2` 只允许用于冻结快照离线验证。正式定时任务恢复前必须切换manifest-backed v3，或提供W9规定的当前虚拟机RSS/swap实测证明并经单独确认。

### 16.3 切换顺序

1. 先部署代码但暂停定时研究。
2. 执行数据库migration和只读校验。
3. 运行数据维护任务，达到水位。
4. 用冻结快照执行 A1-only。
5. 检查三模型和前端。
6. 执行完整离线A1-A3。
7. 完成连续3次稳态性能及一次崩溃续跑测试。
8. 最后恢复15:10正式收盘研究和09:26早盘复核。

### 16.4 回滚原则

- 代码可以回滚到部署前提交。
- 数据库只做向前兼容新增，不依赖删除回滚。
- 新结果使用新合同版本目录，旧结果不覆盖。
- 新合同失败时暂停研究任务，但保持数据维护运行。
- 不自动回退到已知存在10/20冲突的旧A1生产路径。
- 不删除已生成事实、快照、模型审计和失败队列。

---

## 17. 风险与缓解

| 风险 | 影响 | 缓解 |
|---|---|---|
| 特征投影遗漏模型真正需要的信息 | 主题质量下降 | 保留完整原始事实；记录data_request；用盲测和冻结回放迭代投影 |
| 40节点要求诱导同义拆分 | 形式达标、语义重复 | 节点标准化、成员重叠和产业位置去重；无证据时降级而非凑数 |
| 免费模型仍有高延迟或超时 | 单lane不稳定 | 顺序隔离、字段修复、完整审计；不让失败lane污染其他lane |
| 数据维护水位长期不足 | 研究频繁阻断 | namespace级失败队列、明确SLA和来源替代；不在研究时全量补抓 |
| PIT字段缺失 | 回放失真 | T2实时可降级、历史默认阻断；逐源补发布时间 |
| 新旧checkpoint混用 | 错误续跑 | 合同、packet、prompt和pipeline版本全部进入checkpoint键 |
| 方案范围过大 | 实施周期失控 | P0先做合同和研究包；manifest snapshot和深层存储重构后置 |
| 3.8GiB内存下快照对象膨胀 | 换页、OOM、极慢 | manifest快照或RSS硬门；阶段释放；禁止对象副本 |
| 11GB剩余磁盘继续下降 | SQLite/WAL/快照写失败 | 引用感知保留、磁盘水位门、独立授权清理 |
| monitor/maintenance与research重叠 | 漏跑、IO争用、重复调用 | 持久租约、优先级、保护窗和协作暂停 |
| 前端分页但服务端全文件解析 | Node内存/64MB上限 | stage_decisions索引和存储层分页 |
| 全局100万token配置掩盖模型差异 | 请求拒绝或预算误判 | 每模型能力、单位合同和实际usage校正 |

---

## 18. 三次方案复核

### 18.1 第一次复核：是否裁剪项目边界

复核结果：方案没有减少市场目录、G0、82个行业、20个行业决策、历史事实或模型数量。模型输入变小来自“原始行→确定性充分统计量”和删除重复装配，不来自随机截断或缩小研究范围。

调整：明确研究包超预算时失败关闭并输出分块诊断，禁止静默截断；明确所有82行业必须保留一行。

### 18.2 第二次复核：是否真正解决最早阻断

复核结果：P0顺序首先处理单一合同、服务端20决策和紧凑研究包，然后才处理存储和前端。这样能用冻结快照快速证明直接阻断已消除，不必先完成大规模数据库重构。

调整：manifest-backed snapshot v3后置；旧检查点不删除但明确不兼容；字段级修复不得重发完整44万token输入。

### 18.3 第三次复核：虚拟机容量和长期运行

复核结果：原方案已覆盖业务合同和数据关键路径，但没有充分量化2 vCPU、3.8GiB内存、已使用swap、11GB剩余磁盘、211MB快照对象化、Node进程内锁和全文件分页的长期风险，因此不能直接称为全面。

调整：新增W9；把冷启动和稳态分开；增加RSS/swap/磁盘/SQL/文件描述符门；正式恢复前要求manifest快照或实测资源证明；增加任务优先级、整体期限、存储层分页、故障注入和连续3次稳态验收。

### 18.4 剩余风险

1. 三个免费模型对结构化金融研究的稳定性必须通过真实同包测试，单元测试不能代替提供方能力。
2. 当前开放宏观数据的发布时间不足，历史回放在补齐PIT前只能阻断或明确降级。
3. A1跑通后，A2资金流、产业瓶颈事实和A3分钟历史仍需按既有方案继续验收；本方案不把A1成功等同于全链成功。
4. 性能SLO是工程验收目标，不是当前已实现结果；必须在虚拟机同等资源上测量。
5. 巨潮是否提供适合全市场的全局增量游标需在实现前做能力探针；若不支持，使用维护分片，不把假设写成已接通能力。
6. manifest-backed snapshot的等价性必须由同一基线结果证明，不能只比较文件大小。

---

## 19. 最终验收清单

只有以下项目全部通过，A1根因修复才可宣布完成：

- [ ] 全市场5,563目录和G0约4,017研究边界未因性能改变。
- [ ] 全部G0逐只有确定性记录。
- [ ] 服务端20行业基础决策完整，模型不再复制。
- [ ] 配置、提示词、运行时合同和校验器无Top10/Top20冲突。
- [ ] 研究包包含82行业、20决策和全部必需宏观/政策支柱。
- [ ] A1输入估算不超过10万token且无随机截断。
- [ ] 三模型基础研究包hash相同。
- [ ] `READY`上下文产生8–15主题、40–80非重复节点。
- [ ] 缺失映射可字段级修复，不能导致已验证内容全部丢失。
- [ ] 429、5xx、超时、JSON和语义错误分别处理。
- [ ] 全市场公告/PDF同步不在A1关键路径。
- [ ] 数据水位和失败队列可独立续跑。
- [ ] PIT质量参与实时与回放资格。
- [ ] 前端显示准确阶段、计数、预期/实际和缺失项。
- [ ] 旧检查点不污染新合同运行。
- [ ] 2026-08-28冻结快照三模型A1回放通过。
- [ ] 虚拟机缓存就绪的完整A1达到性能目标。
- [ ] JSON、Markdown、SQLite和前端结果一致。
- [ ] 未连接任何外部交易或真实账户。
- [ ] 冷启动可暂停、重启和续跑，不重复成功数据任务。
- [ ] 同等虚拟机连续3次稳态测试满足P50/P95和资源门。
- [ ] 研究峰值RSS、swap增长、SQL数量和文件描述符峰值已落盘。
- [ ] 数据维护、monitor、morning和close具有持久租约、优先级和截止时间。
- [ ] Node阶段详情在4,017条结果下使用存储层分页，不读取完整快照。
- [ ] 磁盘水位、WAL和引用感知保留策略已验证。
- [ ] 每模型上下文/输出能力和token单位已由探针及实际usage核对。

本方案确认后，建议严格按 Phase 0 → Phase 1 → Phase 2 推进，并在完成紧凑研究包后先做一次 A1-only 冻结回放。该回放通过前，不恢复正式 A1-A3 定时任务。
