# 工作流完成度与 A2 核心能力改造实施方案

> 文档版本：2.0  
> 编制日期：2026-08-29  
> 适用项目：`liangjian_funnel_workflow`  
> 方案状态：已实施；本地完整回归通过，待虚拟机部署与历史交易日执行验收  
> 当前代码基线：`54b99aec9c09ccb7438cf87463b4b809265f0a06`  
> 关联文档：`A1_ROOT_CAUSE_REMEDIATION_IMPLEMENTATION_PLAN_2026-08-29.md`、`A1_A3_WORKFLOW_REMEDIATION_PLAN_2026-08-28.md`、`DETERMINISTIC_RESEARCH_PIPELINE_V2_PLAN.md`

本文把当前项目剩余的十个问题整理为一套有依赖顺序、可分批上线、可回放、可回滚的实施计划。重点不是继续堆叠提示词或扩大模型请求，而是先修复事实、派生特征、状态和发布边界，再验证 A1→A2→A3→早盘→A4→模拟成交闭环。

本文不授权连接真实交易。部署与历史交易日研究验收必须保持 `SIMULATION_WORKFLOW`，A4 和入离场只允许使用隔离模拟账户。

## 0. 2026-08-29 实施记录

本轮已按依赖顺序完成代码落地：

- Feature Store 升级为 v2 代际模型，增加 `STAGING → VALIDATED → PUBLISHED/FAILED`、原子 active 指针、运行绑定、旧 v1 保留迁移和已发布源代际不可变约束。
- 新增统一 `research-outcome/2.0.0`，Pipeline、Workflow、SQLite、CLI、Node API 和 React 前端使用同一终态；将“已验证无机会”“数据不足”“执行失败”“上游未运行”分开。
- 正式发布由配置的主模型 lane 决定；Kimi、GLM 作为独立对比 lane，不再阻断主结果。
- A2 接入供应商公开资金流事实及点时缓存，并由涨停梯队、龙虎榜/热度、行业概念归属、周期与板块强弱构造梯队、龙头角色和行业链共振特征；成交额、换手率不冒充资金流。
- 新增券商月度金股严格导入与盲测接口；未配置真实数据时明确返回 `NOT_CONFIGURED`，不伪造基准。
- 新增严格十交易日点时回放工具与 JSON/Markdown 报告；旧运行缺少点时身份时明确阻断，不补写、不冒充真实回放。
- 新增隔离 `TEST_ONLY` 非空计划闭环，覆盖早盘激活、A4、模拟买入、T+1 和模拟卖出，不连接外部交易。
- 新增可租约脏实体队列、退避重试、死信、依赖展开、工作日 03:30 增量和周六 03:30 全量重建；全量代际校验通过后才原子发布，周日不执行。
- 新增 GitHub Actions 门禁，覆盖 Python 3.11、Node 20、Python/Node 测试、类型检查、生产构建和 65% 覆盖率下限。

本地验收结果：Python `564 passed`，总覆盖率 `75.58%`；Node `31 passed`；TypeScript 类型检查、Python 编译、前端/服务端生产构建和 `git diff --check` 均通过。真实 10 日效果统计和真实券商金股命中率仍必须等待/导入合规点时数据，不能用测试 fixture 冒充。

---

## 1. 结论与实施顺序

十项工作不能按编号并行推进。正确顺序是：

```text
冻结基线与可恢复备份
  ↓
P1 特征库代际与旧结果隔离（问题 1）
  ↓
P2 统一结果状态 + 数据不足语义 + 主/对比模型解耦（问题 2、4、6）
  ↓
P3 A2 真实事实接入 + 脏实体增量/周重建（问题 3、9）
  ↓
P4 券商月度金股盲测数据（问题 5）
  ↓
P5 至少 10 个交易日点时回放与效果统计（问题 7）
  ↓
P6 非空计划、早盘、A4、模拟入场离场闭环（问题 8）
  ↓
P7 代码拆分、CI、覆盖率、备份和文档收敛（问题 10）
```

这个顺序有三条硬约束：

1. **状态不统一前不能做效果统计。** 否则“无机会”“缺数据”“模型失败”会被混到同一个 0 结果里。
2. **特征代际不隔离前不能相信 A2 回放。** 否则旧派生行可能混入新算法结果。
3. **真实 A2 事实未接入前不能通过放宽阈值制造非空计划。** 非空不等于有效，先证明输入可靠，再验证交易闭环。

### 1.1 十项问题追踪表

| 原编号 | 问题 | 实施阶段 | 主要产物 | 最终验收 |
|---:|---|---|---|---|
| 1 | 特征库下游旧结果残留 | P1 | Feature Store v2、代际绑定、旧表隔离 | 新运行无法读取未绑定代际 |
| 2 | Pipeline/Workflow/CLI/前端状态不一致 | P2 | `research-outcome/2.0.0` 统一合同 | 五端同场景同语义 |
| 3 | A2 缺真实资金、梯队、龙头和行业链数据 | P3 | A2 事实合同与四类日度特征 | 来源、覆盖和抽样重算通过 |
| 4 | 数据不足的零结果与真实无机会混淆 | P2/P3 | 四轴状态、适用域覆盖合同 | `ABSENT` 与 `UNKNOWN` 可稳定区分 |
| 5 | 券商月度金股盲测未配置 | P4 | 月度数据集、manifest、盲测报告 | 不反哺运行且指标可复现 |
| 6 | 主模型发布与可选模型对比耦合 | P2 | primary/shadow 运行边界 | shadow 失败不阻断 primary |
| 7 | 缺少 10 个交易日回放与效果统计 | P5 | 逐日回放与汇总报告 | 10/10 终态、无未来数据 |
| 8 | 缺少非空计划到模拟入离场验收 | P6 | TEST_ONLY 闭环与真实观察路径 | 早盘、A4、T+1、离场、幂等通过 |
| 9 | 脏实体增量和每周完整重建未接通 | P3 | 可租约 dirty worker、周重建/代际切换 | 故障恢复且当前代际不中断 |
| 10 | 代码、CI、覆盖率、备份和文档未收敛 | P7 | 模块拆分与工程门禁 | CI/覆盖/恢复/文档全部通过 |

### 1.2 目标完成定义

项目达到本轮“可运行、可评估、可持续维护”的最低标准，需要同时满足：

- 任意一次运行的原始事实、派生特征、模型输入、模型输出、规则决策和最终状态都由同一 `run_id + as_of + generation_id + contract_hash` 关联。
- Pipeline、Workflow、CLI、Node API 和前端对同一个运行返回同一终态，不再各自推断。
- A2 能证明资金、梯队、龙头、行业链四类数据的来源、时间、覆盖率和计算过程；缺少时失败关闭。
- `A2=0` 至少被拆成“已验证无机会”“事实覆盖不足”“模型失败”“上游未运行”四种不同结果。
- 主模型完成即可决定正式发布；可选模型只做影子对比，不阻断正式结果。
- 10 个真实交易日都能以点时数据重放，且不会引用未来数据。
- 至少一个非空计划完成收盘生成、早盘收紧、A4 触发、模拟成交、T+1 离场和幂等复跑验收。
- 增量更新与每周完整重建都能在失败后恢复，不覆盖当前可用代际。

---

## 2. 当前项目基线与已确认问题

### 2.1 已有能力

当前项目并非从零开始，已经具备以下可复用基础：

- 全市场本地事实缓存、冻结快照、A1→A2→A3 三阶段研究和 A4 盘中观察。
- `FeatureStore` 的确定性阶段结果原子替换，以及行业/概念成员、基本面、市场角色等派生表。
- 研究检查点、结果索引、运行进度、SQLite 状态库和 Node/React 控制台。
- A2 对 `BLOCKED_EVIDENCE_GAP` 与 `VALIDATED_NO_OPPORTUNITY` 已有初步区分。
- 券商金股 CSV/JSON 的严格导入、点时过滤和评估代码已经存在。
- 冻结快照回放脚本和 PaperBroker 状态机已经存在。
- `dirty_entities` 表已经存在，但目前只有标脏/解决接口，没有可运行的消费队列。

### 2.2 当前缺陷证据

| 领域 | 当前事实 | 风险 |
|---|---|---|
| 特征代际 | `feature_store.py` 仍为 schema v1；市场角色和基本面允许多个 `feature_version/source_hash` 共存 | 查询若未限定当前代际，旧结果可能残留到下游 |
| 状态口径 | Python Pipeline、Workflow、CLI、Node 和 `App.tsx` 各自维护状态集合/映射 | 同一运行可被显示为完成、降级或阻断的不同口径 |
| A2 资金 | 当前 `CAPITAL_FLOW_SNAPSHOT` 是未配置占位；A2 样本曾出现资金覆盖 0 | 资金分不可解释，A2 严格门会大量归入数据缺口 |
| A2 梯队/龙头 | 快照有涨停天梯、龙虎榜、热榜，但本地龙头特征主要依赖空的 `FACTOR_SNAPSHOT` | 已采集事实没有被正确转成可消费特征 |
| A2 行业链 | 行业/概念成员覆盖较好，但产业节点与板块强弱、资金共振未形成统一日度投影 | “属于板块”被错误等同于“板块正在共振” |
| A2 零结果 | 代码已有初步分类，但结果文件、CLI、前端仍可能只突出股票数 0 | 用户无法判断是市场无机会还是系统没数据 |
| 模型发布 | CLI 默认把 `lane_1` 视为必要 lane，但完整 Pipeline/Workflow 仍围绕三 lane 汇总 | 对比模型失败仍会影响总状态、耗时和运维判断 |
| 金股盲测 | 评估代码可用，但月度数据文件未配置时返回 `NOT_CONFIGURED` | 无法证明 A1 月度研究覆盖是否合理 |
| 脏实体 | 表中缺少领取、重试、租约、失败队列、依赖展开和消费者 | 标脏后不会自动产生可验证的新投影 |
| 闭环验收 | 非空计划、早盘、A4、普通卖出/减仓和 T+1 尚无统一 E2E 验收证据 | 单元测试通过不等于运行闭环完成 |

### 2.3 不可妥协的业务边界

- 不减少全市场研究范围来换取速度。
- 不让 LLM 对全市场逐股重算确定性因子。
- 不把成交额、换手率、热度榜或涨跌幅命名为“主力资金流”。
- 不把供应商推导的主力资金净流入伪装成交易所原始事实。
- 不把缺数据导致的 0 个候选解释为“市场没有机会”。
- 不用对比模型补齐主模型失败结果，也不自动切换发布模型。
- 不用未来公告、未来成分、未来复权因子或回看后修订数据做历史点时回放。
- 不以人工构造的测试计划冒充真实策略收益。
- 不连接真实交易，所有 A4 和入离场只在隔离的模拟账户中验收。

---

## 3. 目标技术架构

### 3.1 四层数据边界

```text
L0 原始事实层（不可变）
  行情、财务、公告、行业/概念成员、龙虎榜、涨停天梯、融资融券、供应商资金流
  每条包含 source、as_of、event_time、ingested_at、content_hash、quality_tier
        ↓
L1 点时规范层（可重建）
  代码统一、交易日对齐、复权、单位、缺失标记、事实水位、source contract
        ↓
L2 派生特征层（按 generation 发布）
  资金、梯队、龙头、行业链共振、基本面、技术、角色、覆盖率
        ↓
L3 决策与发布层
  A1/A2/A3 决策、LLM 复核、计划、早盘、A4、PaperBroker、评估
```

L0 只能追加或按内容哈希去重；L1/L2 可重新计算；L3 不允许直接读取没有代际标记的旧特征行。

### 3.2 统一运行身份

所有产物必须携带：

```json
{
  "run_id": "2026-08-29-close-...",
  "as_of": "2026-08-29T15:10:00+08:00",
  "trade_date": "2026-08-29",
  "snapshot_id": "snapshot-...",
  "snapshot_hash": "sha256:...",
  "generation_id": "feature-gen-...",
  "feature_contract": "a2-features/2.0.0",
  "pipeline_contract": "research-outcome/2.0.0",
  "code_commit": "...",
  "provider_contract_hash": "sha256:..."
}
```

检查点恢复必须同时匹配这些字段。任一算法、数据源合同、特征代际或提示词变更后，旧检查点只能作为审计证据，不能直接恢复为新结果。

### 3.3 统一状态模型

不再用一个字符串同时表达“是否结束、质量如何、有没有机会、能不能发布”。目标对象为：

```json
{
  "lifecycle_state": "TERMINAL",
  "quality_state": "VALIDATED",
  "opportunity_state": "ABSENT",
  "publication_state": "NOT_APPLICABLE",
  "reason_codes": ["A2_NO_FOCUS_OPPORTUNITY"],
  "counts": {"input": 239, "evaluated": 239, "selected": 0},
  "data_coverage": {"required": 0.98, "actual": 0.995},
  "legacy_status": "VALIDATED_NO_OPPORTUNITY"
}
```

推荐枚举：

| 轴 | 枚举 |
|---|---|
| 生命周期 | `QUEUED / RUNNING / TERMINAL` |
| 质量 | `VALIDATED / DEGRADED / BLOCKED / FAILED / CANCELLED` |
| 机会 | `PRESENT / ABSENT / UNKNOWN / NOT_APPLICABLE` |
| 发布 | `READY / NOT_APPLICABLE / BLOCKED / PUBLISHED` |

旧 `status` 仅作为兼容投影，由一个权威函数生成。Node 和前端只渲染后端合同，不再重新推断业务语义。

---

## 4. 分阶段实施方案

## P0：冻结基线与恢复点

### 实施内容

1. 冻结当前提交、配置哈希、提示词哈希、SQLite schema、最新可复现快照和结果文件。
2. 使用 SQLite online backup API 备份事实库、特征库和运行状态库；对大文件记录大小与 SHA-256。
3. 保存当前 10 个以上交易日的快照可用性清单，标记缺失事实，不做静默补写。
4. 建立 `storage/manifests/baseline-<timestamp>.json`，只记录相对路径和哈希，不写密钥。

### 验收

- 在空的临时目录恢复三类数据库和一份冻结快照，所有哈希一致。
- 当前运行结果能继续只读展示。
- 失败时不影响现有生产目录。

---

## P1：修复特征库下游旧结果残留（问题 1）

### 4.1 根因

当前 `stock_fundamental_features` 和 `stock_market_role_features` 允许同一实体的多个算法版本共存，但读取方没有强制绑定“当前已发布代际”；`feature_store_meta` 只保存 schema 名称。`replace_stage_decisions()` 能原子替换单个 run/lane/stage，但不能解决跨 run 的特征读取污染。

### 4.2 目标设计：Feature Store v2 代际发布

新增以下概念：

- `feature_generations`：一次完整或增量构建的不可变代际。
- `feature_generation_members`：某代际包含哪些实体/分区以及内容哈希。
- `active_feature_generation`：每个特征域当前唯一发布代际。
- `run_feature_bindings`：运行创建时锁定代际，运行中不得漂移。

建议字段：

| 表 | 关键字段 |
|---|---|
| `feature_generations` | `generation_id, domain, as_of, contract_version, algorithm_version, source_manifest_hash, status, created_at, validated_at` |
| `active_feature_generations` | `domain PK, generation_id, activated_at` |
| `run_feature_bindings` | `run_id, domain, generation_id, contract_hash` |
| 各特征表 | 增加 `generation_id, valid_from, valid_to, source_quality, payload_hash` |

### 4.3 读取规则

1. 新运行先在事务中写入 `run_feature_bindings`。
2. 所有特征查询必须显式传 `run_id` 或 `generation_id`；禁止“取最新一行”。
3. 未绑定代际、代际未 `PUBLISHED`、合同不匹配时直接返回 `BLOCKED_FEATURE_GENERATION`。
4. 一次运行从开始到结束只读同一代际，即使后台完成新代际也不切换。
5. 旧表迁移后保留只读审计期，不直接删除；确认两次完整重建与回放通过后再归档。

### 4.4 旧结果清理策略

- 不是直接 `DELETE` 全表，而是先建立 v2 表并双写。
- 运行一次全量重建，验证行数、唯一性、覆盖率、内容哈希和抽样重算。
- 原子切换 active generation。
- 下游只读 v2；旧表标记 `LEGACY_READ_DISABLED`。
- 满足保留期后将旧表导出为压缩审计包，再由单独授权决定是否删除。

### 4.5 代码落点

- `pipeline/feature_store.py`：schema v2、迁移、代际 CRUD、绑定读取。
- `pipeline/deterministic.py`、`pipeline/research.py`：所有特征读取传入 generation。
- `pipeline/research_checkpoint.py`：检查点键加入 generation/contract/code commit。
- `workflow.py`：冻结运行绑定；完成时写入使用的代际。
- `server/files.ts`：展示代际、合同和是否命中旧结果。

### 4.6 验收用例

- 同一股票在 v1/v2 有冲突分数时，新运行只能读取绑定的 v2。
- 后台激活 v3 后，正在运行的 v2 任务结果不变化。
- v3 校验失败时 active generation 仍是 v2。
- 从旧检查点恢复到新合同必须拒绝，原因码稳定。
- 对同一冻结快照重复两次构建，规范化输出哈希一致。

---

## P2：统一完成状态、区分 A2 零结果、解耦主模型与对比模型（问题 2、4、6）

### 5.1 单一状态权威

新增 `pipeline/outcomes.py`，定义 Python 数据类/枚举、状态迁移、兼容映射和 JSON Schema。Pipeline 只产生 `StageOutcome`；Workflow 聚合为 `LaneOutcome/RunOutcome`；CLI、Node、前端只消费序列化结果。

禁止继续在以下位置维护独立“成功状态列表”：

- `research.py` 的局部集合只保留为调用统一模块的兼容层。
- `cli.py` 不再通过字符串集合重新判定成功。
- `server/files.ts` 不再猜测终态。
- `App.tsx` 不再把几十个字符串人工映射成健康/警告。

### 5.2 A2=0 的四种语义

| 场景 | quality | opportunity | publication | 必需证据 |
|---|---|---|---|---|
| 输入充分，全部被规则合理淘汰 | `VALIDATED` | `ABSENT` | `NOT_APPLICABLE` | 输入覆盖达标，239/239 有确定性决策，拒绝原因完整 |
| 必要事实不足，无法判断 | `BLOCKED` 或明确允许的 `DEGRADED` | `UNKNOWN` | `BLOCKED` | 缺失字段、覆盖率、受影响股票、数据源错误 |
| 模型调用/输出失败 | `FAILED` | `UNKNOWN` | `BLOCKED` | provider/model/attempt/retryable/error code |
| A1 未通过，A2 未运行 | `BLOCKED` | `NOT_APPLICABLE` | `BLOCKED` | `UPSTREAM_STAGE_BLOCKED` |

“真实无机会”必须满足：

- A2 输入集合非空。
- 必需事实覆盖率达到配置阈值，并且不是用代理字段冒充。
- 每个输入都有一条确定性决策。
- `evaluated_count == input_count`。
- 所有淘汰项都有可解释原因，原因数量与输入集合一致。
- 模型复核没有失败；或者该阶段合同明确规定零候选时无需调用模型。

### 5.3 主模型与影子对比模型

配置改为显式角色：

```yaml
research_models:
  primary:
    lane_id: lane_primary
    model: deepseek-v4-pro-0813
    publish: true
  comparisons:
    - lane_id: lane_kimi_shadow
      model: moonshotai/kimi-k3-free
    - lane_id: lane_glm_shadow
      model: z-ai/glm-5.3-free
```

运行结构：

- `production_run_id`：只由主 lane 决定发布状态和正式模拟计划。
- `comparison_run_id`：使用相同冻结快照、相同基础确定性特征，在低优先级队列中执行。
- 对比 lane 失败只影响 `comparison_status`，不改变主任务 `publication_state`。
- 第一阶段禁止自动故障切换；主模型失败时正式任务失败关闭，不能让 Kimi/GLM 代替发布。
- 影子 lane 可计算研究质量和未来收益，但不得自动写入正式 A4 计划。

### 5.4 CLI 与前端表现

- CLI 返回固定退出码：`0=主流程终态且发布语义正确`、`2=业务阻断`、`3=技术失败`、`4=配置/合同错误`、`130=取消`。
- 前端顶部显示主任务状态；对比模型放入“模型对比”区域，不占用主流程进度。
- A2 为 0 时必须显示中文原因：“已验证，当前无符合条件机会”或“数据不足，不能判断”，不能只显示 `0只`。
- 每个状态可展开看到输入、覆盖、淘汰原因、数据代际和模型尝试。

### 5.5 验收矩阵

至少构造以下合同测试：主模型成功/影子失败、主模型失败/影子成功、充分数据零机会、资金缺失零结果、模型超时、上游阻断、主动取消、进程崩溃恢复。对每个场景同时断言 Pipeline JSON、Workflow DB、CLI 退出码、Node API 和前端文案一致。

---

## P3：接入 A2 真实资金流、梯队、龙头结构和行业链共振，并打通增量更新（问题 3、9）

### 6.1 A2 的业务职责

A2 不再承担“让 LLM 从一堆股票里自由挑选”的职责，而是对 A1 活跃/观察候选做二次确定性排序：

```text
A1 候选
  → 资金确认
  → 板块梯队与角色识别
  → 产业链节点共振
  → 风险/拥挤度门
  → 确定性 FOCUS/WATCH/REJECT
  → LLM 仅复核矛盾证据、持续性与叙事完整性
```

### 6.2 数据源分级

| 数据 | 首选 | 次选 | 说明 |
|---|---|---|---|
| 行业/概念分类及成员 | 当前同花顺 API | 本地历史版本 | 已有覆盖基础，必须保存历史成分版本 |
| 涨停梯队 | 同花顺涨停天梯 | 本地日线按交易规则重算 | 官方接口提供近 30 个交易日、分板位数据；本地重算用于完整性校验 |
| 龙虎榜/机构净买 | 同花顺龙虎榜 + 交易所公开交易信息 | Tushare `top_inst`（若取得权限） | 龙虎榜只覆盖异动股票，不能代表全市场资金 |
| 融资融券 | 上交所/深交所公开数据 | Tushare `margin_detail` | 交易所事实优先；注意只覆盖融资融券标的 |
| 个股资金流 | 有授权的数据供应商/同花顺派生数据 | Tushare `moneyflow_ths` 或 `moneyflow` | 属于供应商推导因子，必须标记 `VENDOR_DERIVED`，不是交易所原始事实 |
| 热度/关注 | 同花顺热榜 | 资讯热度 | 只能做拥挤/关注辅助，不可当资金主证据 |
| 行业链共振 | 同花顺行业/概念指数日线、成员行情、龙虎榜、融资融券、涨停梯队聚合 | 本地成员股 OHLCV 计算 | 以点时成员集聚合，禁止使用今天成员回算历史 |

官方文档核验：同花顺接口已公开龙虎榜、近 30 个交易日涨停天梯及热榜；Tushare 文档列有 `moneyflow_ths`、`moneyflow`、`margin_detail` 与龙虎榜机构明细，但相关接口需要积分权限。当前配置明确排除了 `tushare_pro`、付费 API，因此接入前必须形成一次配置决策：继续只使用现有同花顺/交易所事实并接受部分资金覆盖，或由用户提供具有相应权限的合规数据源。没有数据权限时系统应显示 `DATA_NOT_CONFIGURED`，不能以代理值填充。

参考官方接口：

- [同花顺 A 股龙虎榜](https://fuyao.aicubes.cn/docs/mcp/tools/get_a_share_special_data_dragon_tiger_list/)
- [同花顺 A 股涨停天梯](https://fuyao.aicubes.cn/docs/mcp/tools/get_a_share_special_data_limit_up_ladder/)
- [同花顺热榜](https://fuyao.aicubes.cn/docs/api-reference/hot-list-data/)
- [Tushare 个股资金流向（THS）](https://tushare.pro/document/2?doc_id=348)
- [Tushare 个股资金流向](https://tushare.pro/document/2?doc_id=170)
- [Tushare 融资融券明细](https://tushare.pro/document/2?doc_id=59)
- [上交所融资融券明细](https://www.sse.com.cn/market/othersdata/margin/detail/index.shtml)

### 6.3 事实合同

每类 A2 原始事实统一包含：

- `symbol/taxonomy_code`
- `trade_date/event_time/as_of`
- `source_id/source_tier/source_ref`
- `fact_type`
- `raw_value/unit/currency`
- `coverage_scope`
- `provider_method`：`OFFICIAL_REPORTED / VENDOR_DERIVED / LOCAL_DERIVED / PROXY`
- `content_hash/ingested_at`
- `available/reason_code`

在计算前先做数据水位检查，不允许一部分股票有真实资金、一部分股票无资金却默认填 0。

#### 6.3.1 适用域与缺失语义

A2 数据不能一律用“全市场行数/股票总数”计算覆盖率。每个数据集必须先定义 `eligible_universe`：

| 数据 | 适用域 | 正常没有记录 | 数据异常 |
|---|---|---|---|
| 供应商个股资金流 | 供应商声明覆盖的当日正常交易股票 | 有记录且净值可为 0 | 适格股票缺行、日期滞后或请求失败 |
| 融资融券 | 当日融资融券标的 | 非两融标的为 `NOT_APPLICABLE` | 两融标的缺行或交易所数据未就绪 |
| 龙虎榜 | 全市场事件扫描 | 未上榜为 `OBSERVED_ABSENT` | 当日榜单未完整取得或日期不匹配 |
| 涨停梯队 | 当日涨停/历史连板事件 | 未涨停为 `OBSERVED_ABSENT` | 天梯窗口缺日、板位截断未声明或接口失败 |
| 热榜 | 接口声明的 Top-N 范围 | 未进入 Top-N 为 `OUTSIDE_PUBLISHED_SCOPE` | 榜单未取得或时间戳失真 |
| 行业链共振 | 当日有效行业/概念及其点时成员 | 无共振是可验证的零值 | 成员版本、成员行情或聚合分母缺失 |

统一可用性枚举为：

- `OBSERVED_VALUE`：取得明确数值，数值可以为 0。
- `OBSERVED_ABSENT`：完整扫描后确认没有事件。
- `NOT_APPLICABLE`：实体不属于数据适用域。
- `OUTSIDE_PUBLISHED_SCOPE`：Top-N 等公开范围之外，不能推断具体排名/数值。
- `NOT_CONFIGURED`：系统没有配置该来源。
- `SOURCE_FAILED`：已配置但本次抓取失败。
- `STALE`：取得的最新日期早于允许水位。

只有前四类可参与“系统已观察”的覆盖口径，且 `OUTSIDE_PUBLISHED_SCOPE` 只能作为弱证据；后三类必须进入数据缺口统计。覆盖率分母使用适用域，A2 整体是否可判断由每个核心因子的独立阈值决定，禁止用总平均覆盖掩盖某一核心因子为 0。

### 6.4 A2 特征定义

#### 资金确认分 `capital_flow_score`

由可用子因子按覆盖自适应，但每个子因子必须保留独立字段：

- 1/3/5 日供应商资金净流入占成交额。
- 大单净流入及连续性。
- 龙虎榜机构净买、机构席位数量、净买占成交额。
- 融资买入额变化、融资余额变化。
- 行业 ETF/行业指数成交额相对强弱。

不允许的替代：单日成交额、换手率、涨幅单独作为 `capital_flow_score`。

#### 梯队分 `tier_score`

- 连板高度、所处梯队、晋级/断板、封板质量、涨停家数/跌停家数。
- 非涨停主线允许通过趋势梯队：行业内 20/60 日相对强度和成交额扩张分位。
- 连板梯队与趋势梯队分别记录，避免只适用于短线题材。

#### 龙头结构分 `leader_score`

每个主题/产业节点内按可解释角色分类：

- `EMOTION_LEADER`：连板高度和市场辨识度。
- `TREND_LEADER`：中期相对强度、创新高、成交持续性。
- `INSTITUTIONAL_CORE`：市值/流动性、机构/融资、盈利与产业暴露。
- `CAPACITY_CORE`：成交容量、行业权重、产业链不可替代性。
- `FOLLOWER/UNCONFIRMED`。

角色由横截面排名产生，不再只因为出现在热榜或龙虎榜就成为龙头。

#### 行业链共振分 `chain_resonance_score`

按 `trade_date + theme_id + node_id` 物化：

- 行业/概念指数 5/20/60 日相对强度。
- 成员上涨比例、创新高比例、成交额扩张比例。
- 涨停/断板/龙虎榜/融资变化的成员覆盖。
- 上游、中游、下游是否同步改善。
- A1 宏观/政策/产业主题是否仍有效。

必须同时保存 node 级聚合和股票对 node 的主营暴露证据，防止仅凭概念成员关系选股。

### 6.5 A2 决策合同

建议初期输出三类，而不是硬凑固定数量：

- `FOCUS`：事实充分，资金/角色/共振至少两类强且无硬风险。
- `WATCH`：产业逻辑成立，但资金或结构尚未确认，或者局部事实缺失但未达到阻断。
- `REJECT`：有确定性反证、拥挤过高、结构破坏或主营暴露不足。

每条结果必须包含：总分、子分、覆盖率、角色、主题/节点、来源、正反证、原因码和是否送 LLM。A2 整体零 `FOCUS` 不代表零 `WATCH`；前端必须同时展示。

### 6.6 脏实体队列 v2

扩展 `dirty_entities`：

| 字段 | 用途 |
|---|---|
| `status` | `PENDING/RUNNING/RETRY/DEAD/RESOLVED` |
| `priority` | 关键交易日/主线实体优先 |
| `attempts/max_attempts` | 有界重试 |
| `next_retry_at` | 指数退避 |
| `lease_owner/lease_expires_at` | 崩溃可恢复领取 |
| `dependency_hash` | 同一依赖变更去重 |
| `last_error_code/last_error_at` | 可观察失败 |
| `created_at/updated_at/resolved_at` | 生命周期 |

实体类型至少包括：`STOCK`、`TAXONOMY`、`THEME`、`CHAIN_NODE`、`MARKET_DAY`、`A2_THEME_METRIC`、`BROKER_GOLD_MONTH`。

### 6.7 增量与完整重建

日常增量：

1. 数据源按交易日批量抓取，写 L0 并比较内容哈希。
2. 只有内容变化才标记实体及依赖为 dirty。
3. worker 原子领取有限批次，计算到新 generation 的 staging 分区。
4. 校验通过后提交实体分区并 resolve；失败进入退避或 DEAD。
5. 不在研究任务中做全市场网络补数；研究只检查数据水位。

每周完整重建：

- 周末创建全新 generation，绝不 truncate 当前代际。
- 对全市场重新计算行业成员、基本面、A2 角色/共振等。
- 验证行数、重复、空值、覆盖、分布漂移、抽样直接重算和历史点时约束。
- 通过后原子切换 active generation；失败继续使用上一个健康代际并告警。
- 至少每月执行一次恢复演练。

本项目单机 VM 不需要引入 Redis。SQLite WAL、单写者、短事务、租约表和文件内容寻址已经足够，能减少额外运维面。

### 6.8 性能目标

- 外部接口按 `trade_date` 批量获取，禁止逐股 N+1。
- 主题/节点日度聚合一次计算，多 lane 共享。
- 研究模型只读取 A2 候选的紧凑投影，不读取 200MB 原始快照。
- 增量日的 A2 特征刷新目标：2 vCPU/4GB VM 上不超过 15 分钟；热缓存研究阶段不超过 5 分钟（不含模型响应）。
- 周重建允许长时间运行，但 RSS、磁盘水位、失败恢复和代际切换必须可观察。

---

## P4：配置券商月度金股盲测（问题 5）

### 7.1 定位

券商金股只用于 A1 研究方向的外部盲测，不进入运行时选股特征，不影响排序，不允许为了命中金股修改当月结果。

### 7.2 数据合同

沿用现有 `evaluation/broker_gold.py` 合同，至少包含：

```text
month, broker, symbol, name, publish_time, source_ref
```

并新增数据集 manifest：

- `schema_version`
- `dataset_month`
- `collected_at`
- `source_count`
- `broker_coverage`
- `record_count`
- `content_hash`
- `collector/method`
- `license_note`

优先配置中信、中金/中投、中泰、华泰等可公开核验的月度名单；来源必须能定位到公开页面、报告或合法保存的原始文件。不得绕过登录、付费墙或版权限制抓取全文，也不得凭记忆补造名单。

### 7.3 盲测时序

1. A1 运行只读取当时事实，不加载金股名单。
2. A1 结果文件完成并写入不可变 hash。
3. 评估任务再加载 `publish_time <= as_of` 的金股数据。
4. 写独立 benchmark 结果，不回写 A1。

### 7.4 指标

- `ACTIVE` 命中率/召回率。
- `ACTIVE + WATCH` 覆盖率。
- 金股在 A1 排名分位。
- 行业、主题、产业节点对齐率。
- 未命中原因分布：G0 排除、行业未纳入、主营证据不足、基本面失败、数据缺失、模型复核拒绝。
- 按券商分层和等权汇总，防止某券商股票数量主导总指标。

### 7.5 验收

- 至少配置当前月及此前 3 个月可核验数据；确实无法获得的月份明确 `NOT_CONFIGURED`。
- 将金股文件故意放入运行前目录，测试证明 A1 输入 hash 不变。
- 同一结果重复评估 hash 一致。
- 前端明确标注“事后盲测，不参与选股”。

---

## P5：至少 10 个交易日点时回放与效果统计（问题 7）

### 8.1 两类回放

1. **合同回放**：冻结事实 + 录制/固定模型输出，验证数据、特征、状态、幂等和流程可复现。
2. **供应商回放**：冻结事实 + 当前模型重新推理，评估模型波动；单独记录 provider、模型版本、prompt hash、请求/响应 hash。

合同回放是 CI 必选；供应商回放是上线前验收，不应因模型随机性要求逐字一致。

### 8.2 点时规则

- 选择至少 10 个连续实际交易日，不能包含休市日。
- 每日只允许读取 `event_time/publish_time <= as_of` 的事实。
- 历史行业/概念成员使用当日版本。
- 财报与公告使用实际披露时间，不使用报告期代替可知时间。
- 复权因子、退市/ST/停牌/涨跌停规则按当日版本。
- 缺少当日事实时报告缺失，不能用今天数据回填后冒充点时数据。

### 8.3 每日统计

| 维度 | 指标 |
|---|---|
| 数据 | 各事实覆盖率、延迟、缺失数、代际、脏实体积压 |
| G0/A1 | 输入数、ACTIVE/WATCH/REJECT、金股盲测、主题/节点覆盖 |
| A2 | FOCUS/WATCH/REJECT、四类核心特征覆盖、角色分布、零结果分类 |
| A3 | 计划数、NO_SETUP 数、技术数据覆盖、风险拒绝 |
| 模型 | 各 lane 延迟、429/5xx、重试、输出合同、成本/Token（若可得） |
| 未来表现 | 1/3/5 日收益、命中率、最大有利/不利波动、相对行业/指数超额 |
| 模拟 | 计划→激活→信号→成交转化、拒单、滑点、费用、T+1、最大回撤 |

### 8.4 评价边界

10 日只用于证明系统在不同市场日能运行、状态可解释、数据点时正确和指标能计算，**不能**证明策略统计显著或高胜率。参数不得在同一 10 日反复调优后再把该区间当作验证集。初步稳定后应扩展到至少 60 个交易日，并划分开发集与样本外集。

### 8.5 验收门

- 10/10 日合同回放达到终态，无未知状态。
- 同一日重复回放确定性部分 hash 一致。
- 任何未来数据注入都会被 PIT 守卫拒绝。
- 每个 0 结果都有明确语义。
- 生成汇总 Markdown + JSON，逐日可追溯到股票级原因。
- 回放默认禁止发布计划；只有显式测试参数且隔离数据库时才允许进入模拟验收。

---

## P6：非空计划、早盘、A4 与模拟入离场验收（问题 8）

### 9.1 两阶段验收

#### 阶段 A：人工构造的隔离合同样例

构造一个完整但明确标记 `TEST_ONLY` 的冻结快照和非空 A3 计划，使用独立 SQLite、输出目录、run namespace 和模拟账户。样例必须具备真实结构：行业/主题、主营证据、资金、角色、技术位、风险预算、来源和点时字段，但数值为测试 fixture，不得进入生产报表。

验证路径：

```text
close A3 计划
→ PENDING_MORNING_REVIEW
→ 09:25 早盘只允许收紧/作废
→ ACTIVE_TODAY
→ A4 在闭合 1m/5m K 线产生有效事件
→ 下一根闭合 Bar 模拟入场
→ 同日卖出被 T+1 拒绝
→ 下一交易日止盈/止损/普通 SELL 或 REDUCE
→ 费用、滑点、持仓和结果归档
```

#### 阶段 B：等待真实非空计划

只有 A1/A2/A3 使用真实点时事实产生非空计划后，才做生产影子观察。真实计划可以因为早盘收紧或 A4 无触发而不成交，这也是有效结果；不能人为放宽阈值强迫成交。

### 9.2 必测场景

- 早盘只能收紧，不得扩大买入区间、仓位或风险预算。
- A4 模型关闭思考模式，且仅处理已激活计划。
- 同一 K 线、同一事件重复执行不重复成交。
- BUY/ADD/SELL/REDUCE/FORCED_RISK_EXIT 都进入相应撮合路径。
- A 股 T+1、涨跌停、停牌、价格无效、计划过期均正确拒绝。
- 成交使用下一根闭合 Bar，保留滑点与费用。
- 崩溃重启后不丢计划、不重复成交。
- 主 lane 与 shadow lane 的模拟账户隔离；第一阶段 shadow 不发布。

### 9.3 验收产物

- 一份端到端事件时间线 JSON。
- 一份人类可读 Markdown，列出状态迁移、计划、信号、成交、拒绝和持仓。
- 每个事件均能关联 `run_id/plan_id/signal_id/order_intent_id/fill_id`。
- 前端可以从 A3 股票弹窗进入计划，再进入 A4 事件和模拟成交详情。

---

## P7：代码拆分、CI、覆盖率、备份与文档收敛（问题 10）

此阶段最后执行，避免在业务合同仍变化时进行大范围重构。

### 10.1 代码拆分

保持现有 `WorkflowApplication` 外观，逐步把职责迁出：

```text
workflow.py
  ├─ application/research_orchestrator.py
  ├─ application/data_readiness.py
  ├─ application/publication_service.py
  ├─ application/morning_review_service.py
  ├─ application/monitor_service.py
  ├─ application/simulation_service.py
  └─ application/replay_service.py

pipeline/research.py
  ├─ stages/a1.py
  ├─ stages/a2.py
  ├─ stages/a3.py
  ├─ outcomes.py
  ├─ validation.py
  └─ model_review.py
```

每次只移动一个边界，先写 characterization tests，再移动代码，避免重构顺带改变策略。

### 10.2 CI 门禁

每次提交至少执行：

- Python 编译与完整 pytest。
- Node TypeScript 类型检查、Vitest、生产构建。
- SQLite migration 升级/降级兼容测试。
- 状态 JSON Schema、快照合同、检查点兼容和结果索引测试。
- 10 日合同回放的缩小 fixture 版本。
- secret scan、依赖漏洞检查、禁止密钥进入日志/产物。
- 关键文件过大告警和循环依赖检查。

外部 API 和真实模型测试放在显式 nightly/手工门，不让普通 PR 因供应商波动随机失败。

### 10.3 覆盖率

- 先建立真实基线，不以一次性补无意义测试追数字。
- 状态迁移、代际切换、A2 因子、PIT 守卫、PaperBroker 关键模块行/分支覆盖率目标不低于 90%。
- 项目整体初期目标 75%，后续只升不降。
- 每一个生产事故必须补回归测试。

### 10.4 备份与恢复

- SQLite 使用 online backup，不复制正在写入的裸 DB。
- 原始事实、快照、输出和 benchmark 使用内容哈希 manifest。
- 保留每日增量、每周完整、每月归档；具体天数按磁盘容量测算后配置。
- 部署前自动创建恢复点；部署不自动删除上一版本。
- 每月至少一次在临时目录做恢复演练并记录 RTO/RPO。

### 10.5 文档收敛

最终只保留以下权威文档入口：

- `README.md`：产品边界和快速开始。
- `DEPLOYMENT.md`：宝塔/Node/Python/调度部署。
- `OPERATIONS_RUNBOOK.md`：运行、阻断、恢复和常见故障。
- `DATA_SOURCES.md`：来源、质量等级、许可、时间语义和降级边界。
- `STATUS_CONTRACT.md`：统一状态与退出码。
- `REPLAY_ACCEPTANCE.md`：点时回放和效果口径。
- `ARCHITECTURE.md`：数据代际、主/影子 lane、模块依赖。

历史方案不直接删除，迁入 `docs/archive/` 并标记是否已实施、被哪份文档替代。

---

## 11. 分阶段交付、工期与发布门

以下是工程量级估算，不包含等待第三方数据权限和真实非空行情机会的时间：

| 阶段 | 工作日估算 | 主要交付 | 发布门 |
|---|---:|---|---|
| P0 | 0.5–1 | 基线 manifest、可恢复备份 | 恢复演练通过 |
| P1 | 3–5 | Feature Store v2、代际绑定、旧结果隔离 | 双写与抽样重算通过 |
| P2 | 4–6 | 统一状态、A2 零语义、主/影子 lane | 五端状态合同矩阵通过 |
| P3 | 7–12 | A2 四类事实/特征、dirty worker、周重建 | 覆盖率与代际切换通过 |
| P4 | 2–4 | 金股数据集、盲测评估 | PIT 与不反哺证明通过 |
| P5 | 4–7 | 10 日回放、统计报告 | 10/10 终态且无未来数据 |
| P6 | 3–5 | TEST_ONLY 闭环、真实观察能力 | 全状态迁移与幂等通过 |
| P7 | 5–10 | 拆分、CI、覆盖率、备份、权威文档 | 全回归和恢复演练通过 |

总量约 28–45 个有效工程日。可通过并行准备数据集与测试 fixture 缩短日历时间，但 P1/P2/P3 的生产合同不能并行乱序合并。

### 11.1 每阶段标准发布流程

1. 本地迁移/单测/集成测试。
2. 使用冻结快照完成离线回放。
3. 建立部署前备份和 commit/tag。
4. 虚拟机只部署代码，不自动启动全量正式任务。
5. 先跑 doctor、migration dry-run、最小合同回放。
6. 再运行一个隔离 shadow 任务。
7. 观察状态、RSS、磁盘、SQLite 锁、网络重试和前端。
8. 满足门禁后才切换正式调度。

### 11.2 回滚边界

- 数据 schema 迁移必须前向兼容至少一个版本。
- active generation 切换是原子指针更新，可回退到上一健康代际。
- 状态合同 v2 在过渡期同时输出 legacy projection，前端可单独回滚。
- 主/影子解耦可通过配置关闭 comparison，不影响主 lane。
- A2 新数据源可逐类关闭，但关闭后必须变为 `DATA_NOT_CONFIGURED/UNKNOWN`，不能静默使用 0。

---

## 12. 总体验收矩阵

| 编号 | 可执行验收 | 通过标准 |
|---|---|---|
| 1 | 新旧特征冲突 fixture + 代际切换 | 下游只读绑定代际；失败重建不污染当前代际 |
| 2 | 核心终态跨五端合同测试 | Pipeline/Workflow/CLI/API/前端语义完全一致 |
| 3 | A2 四类事实覆盖与抽样手工复算 | 来源可追溯、单位正确、角色/共振可重算 |
| 4 | 充分数据零机会 vs 缺资金零结果 | 前者 ABSENT，后者 UNKNOWN/BLOCKED，文案不同 |
| 5 | 至少四个月可核验金股评估 | 运行输入 hash 不受 benchmark 影响，指标可复现 |
| 6 | 主成功影子失败、主失败影子成功 | 正式发布只由主 lane 决定，影子不越权 |
| 7 | 至少 10 个真实交易日点时回放 | 10/10 终态、无未来数据、股票级原因完整 |
| 8 | TEST_ONLY 非空计划全链路 | 早盘、A4、入场、T+1、离场、幂等全部通过 |
| 9 | 脏实体增量 + 周完整重建故障注入 | 可重试、可恢复、原子发布、上一代不中断 |
| 10 | CI、覆盖率、恢复演练、文档链接检查 | 门禁稳定、关键覆盖≥90%、整体≥75%、文档无冲突 |

### 12.1 计划中的标准验证命令

实际实施后，开发机和 CI 至少统一执行以下入口；新增脚本必须包装这些入口，而不是另造一套隐式验收：

```powershell
python -m compileall src tests
python -m pytest
npm run typecheck
npm test
npm run build
```

同时新增项目级命令或脚本：

```text
liangjian-funnel migrate-feature-store --dry-run
liangjian-funnel validate-feature-generation --generation-id <id>
liangjian-funnel replay-research --manifest <10-day-manifest> --no-publish
liangjian-funnel validate-outcome-contract --fixture-dir tests/fixtures/outcomes
liangjian-funnel rebuild-features --mode full --staging-only
liangjian-funnel run-simulation-acceptance --fixture nonempty-plan --test-only
```

这些命令是待实现的公开运维接口。每个命令必须返回稳定退出码、写结构化 JSON，并在失败时保持当前 active generation 和正式模拟账户不变。

---

## 13. 需要外部确认或资源的事项

这些事项不会阻止先实施 P0–P2，但会影响 P3/P4 的最终覆盖：

1. **资金流数据权限选择。** 当前项目配置排除 Tushare Pro 和付费 API；若维持该边界，A2 可以使用同花顺特色数据、交易所融资融券、龙虎榜和本地量价聚合，但“全市场主力资金流”只能保持未配置，不能伪造。若允许使用 Tushare，需要单独提供合法 Token/积分权限并更新数据来源政策。
2. **券商金股原始文件。** 需要公开可核验的月度列表或用户合法持有的文件；系统不会凭模型生成基准答案。
3. **真实非空计划的时间。** 可以先用隔离 fixture 验证技术闭环；真实计划必须等待真实研究结果，不能以降低门槛强制产生。

---

## 14. 两轮复核结论

### 14.1 第一轮：业务逻辑复核

- A1 负责宏观、政策、产业周期和研究池；A2 负责资金、梯队、角色与产业链共振；A3 负责技术位与计划，职责没有越界。
- A2 不再依赖 LLM 对全量股票自由筛选，确定性代码覆盖全部上游候选，LLM 只做软证据复核。
- `0 结果` 被拆成机会缺失、数据未知、模型失败和上游未运行，避免业务误判。
- 金股只做盲测，杜绝把答案喂回 A1。
- TEST_ONLY 非空计划与真实策略结果严格隔离，不会制造虚假策略收益。

### 14.2 第二轮：技术架构与性能复核

- 代际绑定解决旧特征残留，并允许失败时安全保留上一代。
- 主/影子 lane 解耦后，对比模型不会延长正式发布关键路径。
- A2 日度主题聚合和按交易日批量取数避免逐股网络请求。
- SQLite WAL + 单写者 + 租约队列适合当前单机 VM，不引入不必要的 Redis。
- 10 日回放先验证系统正确性，不夸大统计有效性。
- 大规模代码拆分延后到业务合同稳定后，降低边改逻辑边重构的风险。

### 14.3 剩余风险

- 供应商资金流本质是推导数据，不同平台算法可能不一致，必须持续保存源和方法标签。
- 免费/公开数据的历史可回溯范围、频率和稳定性可能不足，10 日回放前需逐日盘点。
- 2 vCPU/约 4GB VM 仍是容量约束；即使算法优化，也必须限制模型并发和大 JSON 常驻内存。
- 10 日样本不足以证明投资有效性，后续仍需更长样本外验证。
- 外部模型具有非确定性；正式决策必须保存完整输入输出哈希和合同结果，不能只保存 Markdown 摘要。

---

## 15. 最终完成清单

- [ ] 基线、数据库和快照已备份并完成恢复演练。
- [ ] Feature Store v2 已上线，所有运行绑定唯一 generation。
- [ ] 旧特征无法被新运行读取，旧检查点合同不匹配时被拒绝。
- [ ] 五端统一状态合同上线，前端不再自行推断。
- [ ] A2 零结果能区分 `ABSENT` 与 `UNKNOWN`。
- [ ] A2 四类核心数据具备来源、覆盖率、点时和派生说明。
- [ ] 主模型独立发布，对比模型失败不阻断主任务。
- [ ] 券商月度金股盲测已配置且不进入运行输入。
- [ ] 至少 10 个交易日点时回放完成并生成统计报告。
- [ ] TEST_ONLY 非空计划完成早盘、A4、入场、T+1 和离场闭环。
- [ ] 脏实体增量消费者和周完整重建已完成故障恢复测试。
- [ ] 关键模块覆盖率不低于 90%，整体不低于 75%。
- [ ] CI、备份、恢复、部署和权威文档全部收敛。

只有以上全部满足，项目才能从“具备主体功能但仍有关键验收缺口”升级为“可持续运行、可解释、可回放、可评估的 A 股研究与模拟工作流”。
