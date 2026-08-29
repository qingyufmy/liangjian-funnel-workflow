# A 股研究工作流评审问题总整改与最终验收方案

> 方案日期：2026-08-29  
> 当前代码基线：`caa46ddde97f85463b2dd5de0fcc1b55d95b5883`  
> 当前部署目录：`/www/wwwroot/Agu/liangjian-funnel-workflow`  
> 方案状态：已进入实施验收；工程本地门已通过，CI、虚拟机和真实业务验收以追踪表及自动报告为准  
> 适用范围：Feature Store、A1/A2/A3、Workflow、CLI、Node 控制面、React 前端、回放、调度、模拟账户、CI、部署与运维  
> 安全边界：只允许研究与隔离模拟，不连接券商，不提交真实订单

## 1. 方案目的

本方案针对 2026-08-29 代码、自动化测试、虚拟机数据库、API、运行进度和 `2026-08-28-close-777fe89a93c0` 实际产物的复核结果，重新定义整改范围和最终验收门。

本轮不再以“新增了模块”“有单元测试”“任务跑到终态”作为完成依据。每项整改必须同时满足：

1. **实现完成**：代码、数据库迁移、配置、前端和文档都已落地。
2. **验证完成**：单元、合同、集成、故障注入和回放测试通过。
3. **上线完成**：指定提交部署到虚拟机，同提交、同配置、同运行用户验证通过。
4. **业务验收完成**：真实点时数据与真实运行产物证明语义和业务结果有效。

任何一层缺失，状态只能写成 `IMPLEMENTED`、`VERIFIED_LOCAL`、`DEPLOYED_UNVERIFIED` 或 `PENDING_BUSINESS_ACCEPTANCE`，禁止写“已完成”。

本文是后续整改的唯一总计划。旧方案保留为历史设计记录；旧验收报告中与本次复核冲突的“已完成”结论不再作为当前完成度依据。

## 2. 当前评审基线

### 2.1 已确认的正向能力

- 全市场目录 5562 只、研究池 4098 只，没有人为裁剪股票池。
- A1 已从逐股大模型分析改为全量确定性筛选、少量 LLM 复核；本次 A1 输出 104 只。
- A1 月度轮动合同已统一为 20 条，修复了此前 top 10 与 top 20 冲突。
- Python 564 项测试、Node 31 项测试、类型检查和生产构建通过。
- Node 控制面健康，运行用户与结果文件所有权已统一为 `www:www`。
- 主模型成功、可选模型失败时，结果状态不再被简单判成三模型全部失败。
- 未连接外部交易，A3 为 0 时没有为了验收强制造单。

### 2.2 阻断最终验收的问题

| ID | 级别 | 问题 | 已确认影响 |
|---|---|---|---|
| R1 | P0 | 历史回放发布代际时切换全局 active 指针 | 历史 generation 曾成为当前 active，破坏时间隔离 |
| R2 | P0 | A2 数据不足被汇总成 `VALIDATED/ABSENT` | “无法判断”被误报成“确认无机会” |
| R3 | P0 | 运行、lane、stage、CLI、API、前端状态没有单一权威 | 失败 lane 被进度文件显示为 `READY_DEGRADED` |
| R4 | P0 | 运行级 opportunity 由任一上游阶段决定 | A1 有研究候选但 A2/A3 为 0 时仍显示“存在机会” |
| R5 | P1 | 主模型与对比模型仅状态解耦，未执行解耦 | Kimi/GLM 仍延迟主模型正式发布约 20 分钟 |
| R6 | P1 | 日涨幅百分位代理被计入真实梯队因子 | A2 数据覆盖被高估，梯队语义不真实 |
| R7 | P1 | Feature Store 全量代际只物化部分特征 | 基本面、角色、主题和产业链投影不完整 |
| R8 | P1 | A1 最终候选丢失行业和名称等元数据 | 104 只中 88 只没有直接保留 `ths_industries` |
| R9 | P1 | 测试总覆盖率通过，但关键编排链路覆盖不足 | `workflow.py` 和回放关键路径缺少强回归保护 |
| R10 | P1 | 存储缺少正式保留、压缩、清理和恢复合同 | 磁盘已使用 80%，事实库约 2.7GB |
| R11 | P2 | 验收报告与实际产物存在口径偏差 | A2 两只候选实际覆盖 0.87，报告写成 0.72 |
| R12 | P2 | 金股、10 日回放、自然非空计划仍缺真实样本 | 只能证明工具存在，不能证明业务有效性 |

## 3. 不可妥协的设计原则

### 3.1 事实、特征、决策、执行四层隔离

```text
事实层 Fact Plane
  原始行情、公告、研报、资金、梯队、行业、概念、宏观、来源时间
        ↓ 不可变事实引用
特征层 Feature Plane
  generation、基本面、角色、梯队、资金、产业链共振、覆盖率
        ↓ run_id + generation_id + contract_hash
决策层 Decision Plane
  A1/A2/A3、LLM 复核、计划、原因、证据、状态
        ↓ 仅隔离模拟
执行层 Simulation Plane
  早盘、A4、模拟入场、T+1、离场、持仓与结果
```

禁止把运行决策写回事实层，禁止历史回放改变当前特征指针，禁止模拟执行反向改变研究结果。

### 3.2 数据不足必须失败关闭

- `0` 是观测值；`null/UNKNOWN` 是不知道；二者不能互换。
- 数据源调用失败不能写成“观察到无事件”。
- 日涨幅、成交额、换手率不能冒充主力资金流或真实梯队。
- 缺少关键事实时可以输出观察池，但不得输出“确认无机会”。
- 每个阶段必须输出非空 `data_coverage`，并标明适用域、分子、分母、时间和来源状态。

### 3.3 状态只有一个权威来源

Python 产生版本化状态合同；CLI、Node 和前端只能校验、传输和展示，禁止各自维护成功状态集合并重新推断业务语义。

### 3.4 主模型优先于模型对比

DeepSeek 主模型是正式研究发布路径；Kimi、GLM 是异步对比任务。对比模型可以失败、超时或延后，但不得延迟主模型结果、计划发布和早盘准备。

### 3.5 完成必须有可复现证据

每个工作包必须产生：

- 代码提交和变更清单；
- 数据库迁移版本；
- 测试报告和覆盖率；
- 回放输入、输出及 SHA-256 manifest；
- 虚拟机提交、进程、API、文件、数据库与资源证据；
- 验收结论和未完成项。

## 4. 目标技术架构

### 4.1 Feature Store：发布与激活彻底分离

当前 `publish_feature_generation()` 同时改变 generation 状态和全局指针，必须拆成三个原子操作：

```python
seal_generation(generation_id, validation_manifest)
bind_run_generation(run_id, generation_id, contract_hash)
activate_generation(generation_id, expected_current_id, activation_reason)
```

目标状态：

```text
STAGING → VALIDATED → SEALED
                    ↘ FAILED

ACTIVE 不是 generation 状态，而是 domain 指针。
```

generation 增加以下元数据：

- `purpose`: `LIVE_FULL | LIVE_INCREMENTAL | HISTORICAL_REPLAY | TEST_FIXTURE`
- `as_of`
- `source_manifest_hash`
- `contract_version`
- `algorithm_version`
- `sealed_at`
- `activation_eligible`
- `validation_manifest_json`

只有 `LIVE_FULL` 和 `LIVE_INCREMENTAL` 可以激活。`HISTORICAL_REPLAY` 只能 seal 和 bind，数据库触发器及服务层同时禁止其写 active 指针。

激活使用 compare-and-swap：

```text
expected_current_id == 实际 active id
新 generation.as_of >= 当前 active.as_of
purpose 允许激活
validation manifest 完整
```

任一条件不满足，事务回滚，旧 active 保持不变。

### 4.2 运行身份和点时绑定

所有研究产物必须携带：

```text
run_id
trade_date
slot
as_of
snapshot_id
snapshot_hash
feature_generation_id
feature_contract_hash
prompt_contract_hash
model_request_id
```

回放读取顺序固定为：

1. 根据 `run_id` 读取绑定 generation；
2. 校验 generation 已 seal；
3. 校验 `as_of` 不晚于研究点时；
4. 校验 snapshot hash、contract hash；
5. 禁止回退到全局 active；
6. 任一项不成立则 `BLOCKED_POINT_IN_TIME_BINDING`。

### 4.3 状态合同 v3

新增单一权威合同：

- `contracts/research_outcome_v3.schema.json`
- `src/liangjian_funnel/pipeline/outcome_contract.py`
- `scripts/export_outcome_contract.py`
- `STATUS_CONTRACT.md`

Python 是状态行为权威，JSON Schema 是跨语言交换权威。TypeScript 类型和状态枚举由脚本生成，CI 执行 `--check`，发现手工漂移立即失败。

运行状态拆成两个互不混用的域：

#### 作业状态 JobLifecycle

```text
QUEUED | RUNNING | SUCCEEDED | FAILED | CANCELLED | STALE
```

#### 业务结果 ResearchOutcome

```text
quality_state:
  VALIDATED | DEGRADED | BLOCKED | FAILED | CANCELLED

data_sufficiency_state:
  SUFFICIENT | PARTIAL | INSUFFICIENT | NOT_APPLICABLE

research_opportunity_state:       # A1 研究候选
  PRESENT | ABSENT | UNKNOWN | NOT_APPLICABLE

focus_opportunity_state:          # A2 确认机会
  PRESENT | ABSENT | UNKNOWN | NOT_APPLICABLE

actionability_state:              # A3 可执行计划
  ACTIONABLE | NO_ACTION | UNKNOWN | NOT_APPLICABLE

publication_state:
  READY | PUBLISHED | BLOCKED | NOT_APPLICABLE
```

不再保留一个含义模糊的 run-level `opportunity_state`。兼容期可输出 v2 projection，但 v2 只能由 v3 确定性转换，不能反向参与业务判断。

### 4.4 进度模型

`workflow_progress.json` 只保存作业进度和嵌入的 canonical outcome，不自行改写 lane 结果。

- `finish()` 只能结束当前作业，不能批量覆盖 lane 状态。
- 每个 lane/stage 在终态时写入完整 outcome v3。
- lane 2/3 失败必须保持失败，不能继承总体 `READY_DEGRADED`。
- Node 只做大小、字段、枚举和敏感信息校验。
- 前端文案由 canonical 轴映射，例如：
  - `SUFFICIENT + ABSENT`：数据充分，当前无符合条件机会；
  - `INSUFFICIENT + UNKNOWN`：数据不足，无法判断是否存在机会；
  - `FAILED`：计算或模型执行失败；
  - `NOT_APPLICABLE`：上游未通过，本阶段未运行。

### 4.5 主模型与对比模型执行边界

正式收盘任务拆成两个可独立持久化的作业：

```text
close-primary
  数据就绪 → A1/A2/A3 DeepSeek → 写正式 run → 发布计划 → 完成

close-comparison(parent_run_id)
  读取相同 snapshot/generation → Kimi → GLM → 写 comparison 结果
```

约束：

- `close-primary` 不实例化可选模型客户端。
- 主模型结果完成后 60 秒内必须在 API/前端可见。
- comparison 通过 `parent_run_id` 关联，不修改主 run 状态和计划。
- comparison 顺序执行，限制内存；失败只更新自己的状态。
- comparison 与下一次 morning/close 冲突时，可暂停或取消，不影响正式工作流。
- Node 使用持久作业租约，重启后可识别已完成主任务和待执行 comparison，禁止重复发布。

## 5. A2 业务与数据整改

### 5.1 A2职责

A2只负责把 A1 研究候选按真实市场确认强度分为：

- `FOCUS`：核心事实充分且达到确定性门槛；
- `WATCH`：逻辑仍成立，但强度不足或非关键事实缺失；
- `DATA_GAP`：关键事实不足，不能判断；
- `REJECT`：数据充分且明确不符合要求。

LLM 只能解释和复核确定性结果，不得填补缺失数字、制造资金流或修改覆盖状态。

### 5.2 A2事实合同

所有 A2 原始事实统一字段：

```json
{
  "dataset": "CAPITAL_FLOW",
  "symbol": "000001.SZ",
  "as_of": "2026-08-28T15:00:00+08:00",
  "published_at": "...",
  "source": "HITHINK|EASTMONEY|EXCHANGE|LOCAL_DERIVED",
  "source_kind": "LICENSED|OFFICIAL|VENDOR_DERIVED|LOCAL_DERIVED",
  "published_scope": "FULL_MARKET|RANKING_TOP_N|EVENT_MEMBERS|SYMBOL",
  "availability_state": "OBSERVED_VALUE|OBSERVED_ABSENT|OUTSIDE_SCOPE|SOURCE_FAILED|NOT_CONFIGURED",
  "value": null,
  "unit": null,
  "source_ref": null,
  "content_hash": "..."
}
```

`OBSERVED_ABSENT` 只在数据源请求成功且明确覆盖该股票的情况下成立。榜单未出现的股票若榜单仅是 Top N，只能是 `OUTSIDE_SCOPE`，不能是零资金流。

### 5.3 四类核心特征

#### 资金确认 `capital_flow`

优先级：

1. 同花顺已授权、可点时验证的资金数据；
2. 交易所官方两融、龙虎榜机构数据，保留原始语义，不冒充主力净流入；
3. 东方财富当前资金榜仅作 `VENDOR_DERIVED` 补充事实；
4. 没有可靠历史源时保持 `NOT_CONFIGURED`，禁止回填当前值到历史日期。

不同语义分别建因子：`main_flow`、`margin_flow`、`institution_lhb_flow`、`etf_flow`，不得合并成无法解释的单一原始字段。最终资金确认分必须保留每个子因子权重和缺失状态。

#### 梯队结构 `tier_structure`

- 真实梯队只来自点时涨停/连板/炸板/晋级事实。
- 当天梯队源成功且覆盖全市场，未入榜股票可以记为 `OBSERVED_ABSENT`，梯队分为 0。
- 数据源失败时全部为 `UNKNOWN`。
- 日涨幅百分位独立命名为 `trend_strength_proxy`，只参与趋势因子，不得提高梯队覆盖率。

#### 龙头结构 `leader_structure`

由可重算特征形成：行业内成交额排名、自由流通市值、相对强度、连续趋势、真实梯队位置、机构/资金确认、主营业务暴露。每项保留分位数、权重和来源。

#### 产业链共振 `chain_resonance`

必须同时验证：

- 股票属于目标主题/产业节点；
- 主题或行业本身的价格、广度、成交额占比在增强；
- 至少一类资金或事件事实支持；
- 所有证据不晚于 `as_of`。

“属于某板块”只能证明 membership，不能直接证明 resonance。

### 5.4 A2数据充分性门

数据充分性按核心因子逐项计算，禁止使用总平均覆盖掩盖某一核心因子为零。

首期门槛：

| 因子 | 适用域 | 最低覆盖 | 不满足后的语义 |
|---|---|---:|---|
| 日线与流动性 | A1输入池 | 95% | `INSUFFICIENT/UNKNOWN` |
| 行业/概念身份 | A1输入池 | 95% | 缺失股票进入 `DATA_GAP` |
| 产业链映射 | A1目标主题股票 | 90% | 不允许判定强共振 |
| 真实梯队数据集状态 | 当日全市场 | 数据源成功且范围可证明 | 不使用趋势代理替代 |
| 资金确认 | A1输入池或来源已声明适用域 | 90% | 不允许整体判定 `ABSENT` |
| 龙头结构 | A1输入池 | 90% | 未覆盖股票进入 `DATA_GAP` |

A2整体状态规则：

```text
关键覆盖全部达标 + FOCUS=0
  → quality=VALIDATED
  → data_sufficiency=SUFFICIENT
  → focus_opportunity=ABSENT

任一关键覆盖未达标 + FOCUS=0
  → quality=DEGRADED 或 BLOCKED
  → data_sufficiency=INSUFFICIENT
  → focus_opportunity=UNKNOWN

关键覆盖达标 + FOCUS>0
  → focus_opportunity=PRESENT
```

本次 102/104 缺资金事实的场景必须落入第二种，不能继续输出 `VALIDATED_NO_OPPORTUNITY`。

## 6. Feature Store完整物化方案

### 6.1 数据域分离

Feature Store拆成两类内容：

#### Maintenance Generation

- 全市场成员；
- 行业/概念映射；
- 主营业务暴露；
- 基本面特征；
- 股票市场角色基础特征；
- 主题、产业链节点和关系；
- 数据覆盖与质量指标。

#### Run-bound Decision Projection

- A1/A2/A3确定性决策；
- 模型复核结果；
- 股票级原因和证据；
- 计划与状态。

`deterministic_stage_decisions=0` 不应阻断周全量 maintenance generation，因为它属于运行域；但 fundamentals、market roles、themes、chain nodes 等被声明为必需的 maintenance 表不能静默为零。

### 6.2 Generation Manifest

每个 generation 必须写入：

```json
{
  "required_tables": {
    "feature_generation_members": {"min_coverage": 0.99},
    "taxonomy_membership_versions": {"min_coverage": 0.95},
    "business_exposure_facts": {"min_coverage": 0.70},
    "stock_fundamental_features": {"min_coverage": 0.85},
    "stock_market_role_features": {"min_coverage": 0.90},
    "theme_registry_versions": {"min_rows": 1},
    "chain_node_versions": {"min_rows": 1}
  },
  "optional_tables": {},
  "invalid_payloads": {},
  "content_hashes": {}
}
```

阈值必须根据合法适用域计算，而不是简单除以全市场。必需表未达标时 generation 不得 seal 和激活：可修复缺口时保持 `STAGING`，确认不可用时进入 `FAILED`，具体缺口写入 validation manifest。

### 6.3 脏实体与重建

脏实体队列必须覆盖：

```text
STOCK
TAXONOMY
THEME
CHAIN_NODE
MARKET_DAY
A2_CAPITAL_FLOW
A2_LADDER
A2_THEME_METRIC
BROKER_GOLD_MONTH
```

规则：

- 日常增量只处理变更实体和依赖展开集合；
- 周全量建立新 generation，不原地覆盖；
- 失败重建保留旧 active；
- 成功激活使用 CAS；
- dirty item 仅在新 generation 激活后标记完成；
- 重启后租约到期可恢复，处理幂等。

## 7. A1结果可解释性与候选目录

建立独立、去重的 `CandidateCatalog`，键为 `run_id + symbol`，保存：

- 股票代码、名称、交易所；
- 同花顺行业、二级行业、概念；
- 主题和产业链节点；
- 基本面摘要、角色、流动性；
- A1入选理由、反证、风险和失效条件；
- 事实引用和 lineage。

A1/A2/A3阶段结果只引用 catalog ID，并保存阶段特有字段，避免三个大 JSON 重复复制全市场元数据。

强制合同：

- approved/watch/reject 每条必须有 symbol；
- symbol 必须能在 catalog 查到代码和名称；
- A1 approved 必须至少有一个行业、主题或明确 `MAPPING_GAP`；
- UI详情由 catalog与阶段结果服务端连接，不依赖模型是否回传名称；
- 任何丢失不静默，计入 `metadata_coverage`。

## 8. 存储、性能与长期运行

### 8.1 当前风险基线

- 根分区约使用 80%，剩余约 7.3GB。
- `market_fact_cache.sqlite3` 约 2.7GB。
- Feature Store 约 693MB。
- 单个冻结快照约 215MB。
- 单个主 lane JSON 约 22MB，决策索引约 14.7MB。

### 8.2 存储水位

| 水位 | 动作 |
|---|---|
| 可用空间 < 25% | 前端告警，禁止无必要全量回放 |
| 可用空间 < 15% 或 < 5GB | 阻止新全量快照和全量重建，允许增量和只读服务 |
| 可用空间 < 10% 或 SQLite 写失败 | 正式研究 fail closed，禁止继续产生大文件 |

### 8.3 保留策略

- 运行结果、manifest和最终MD长期保留。
- 原始大模型响应默认保留30天，之后压缩归档。
- 冻结快照默认保留30个交易日；被回放、金股盲测或人工标记的快照引用保护。
- checkpoint成功完成后保留最近两代；失败checkpoint按7天保留。
- generation至少保留当前active、上一active以及所有被run绑定的代际。
- 删除前生成候选清单、引用检查和预计释放空间；清理命令默认dry-run。
- 不允许通过直接删除SQLite文件或数据库行清理。

### 8.4 SQLite维护

- 每日低峰执行WAL checkpoint和完整性检查。
- 每周记录页数、空闲页、WAL大小和增长率。
- `VACUUM`只在备份成功、空间充足、无运行任务时执行。
- 使用SQLite online backup API生成一致备份，并保存SHA-256。
- 每月至少完成一次恢复到临时目录的自动演练。

### 8.5 性能SLO

以虚拟机2 vCPU/约4GB内存为基线：

| 阶段 | 目标 |
|---|---:|
| 热缓存确定性数据准备 | ≤ 5分钟 |
| 增量特征刷新 | ≤ 15分钟 |
| 全量Feature Store重建 | ≤ 2分钟 |
| DeepSeek主lane完成后API可见 | ≤ 60秒 |
| 主研究端到端 | ≤ 35分钟，供应商故障除外但必须有硬期限 |
| comparison | 后台执行，不影响主SLO |
| 进程峰值RSS | ≤ 1.2GB |
| 单次运行新增swap | ≤ 256MB |

超出SLO必须在结果中记录阶段耗时、请求次数、缓存命中、RSS、swap和文件增长，不能只显示总耗时。

## 9. 分阶段实施计划

## Phase 0：冻结基线和纠正完成度

### 修改内容

1. 将当前验收报告标记为“历史验收，已被本方案复核修正”。
2. 保存当前代码提交、配置hash、active generation、数据库大小、API输出和最新运行manifest。
3. 使用SQLite online backup备份事实库、Feature Store和运行状态库。
4. 建立 `REMEDIATION_TRACEABILITY.md`，逐项跟踪 R1-R12。

### 退出门

- 备份可在临时目录打开并通过 `integrity_check`。
- 当前 active pointer、运行绑定和文件hash可复现。
- 不启动新的正式A1-A3。

## Phase 1：修复历史回放和generation激活边界

### 代码落点

- `pipeline/feature_store.py`
- `pipeline/feature_rebuild.py`
- `pipeline/research.py`
- 新增数据库迁移和激活审计表
- `scripts/replay_frozen_research.py`

### 必须实现

- seal/bind/activate API分离；
- purpose与activation eligibility；
- active pointer CAS和as_of单调校验；
- 历史回放永不调用activate；
- 旧 `publish_feature_generation()` 删除业务调用或变成显式兼容封装并记录弃用；
- 激活审计记录 actor、reason、previous、new、as_of和hash。

现有数据迁移必须显式处理：

- 当前 active generation 保持原指针，不因迁移自动切换；
- 已发布的 full/incremental generation 迁移为 `SEALED + activation_eligible=true`；
- 根据 `contract_version`、metadata和snapshot绑定识别历史回放 generation，迁移为 `SEALED + purpose=HISTORICAL_REPLAY + activation_eligible=false`；
- 无法可靠识别purpose的旧generation默认不可激活，等待人工审计；
- 迁移前后对generation数量、active指针、run绑定和内容hash做逐项对账。

### 验收门

1. 当前active为G2，回放历史G1后active仍为G2。
2. 历史run绑定G1且读取G1结果。
3. 尝试激活`HISTORICAL_REPLAY`必须数据库和服务层双重拒绝。
4. 两个worker并发激活时只有CAS成功者生效。
5. 激活失败时dirty队列不被错误清空。

## Phase 2：状态合同v3和前端统一

### 代码落点

- 新增 `contracts/research_outcome_v3.schema.json`
- 新增 `pipeline/outcome_contract.py`
- 修改 `pipeline/outcomes.py`
- 修改 `runtime/progress.py`
- 修改 `workflow.py`、`cli.py`
- 生成 `server/generated/outcome-contract.ts`
- 生成 `web/src/generated/outcome-contract.ts`
- 修改Node API和React展示
- 新增 `STATUS_CONTRACT.md`

### 必须实现

- 作业状态与业务状态分离；
- A1研究机会、A2焦点机会、A3可执行性分离；
- `finish()`不覆盖lane/stage；
- legacy projection只由v3生成；
- Node/frontend禁止自行从股票数推断状态；
- 所有未知枚举fail closed并显示合同版本错误。

### 验收矩阵

至少覆盖：

1. A1有候选、A2充分但无机会、A3无动作；
2. A1有候选、A2数据不足；
3. A2有焦点、A3有计划；
4. 主模型失败；
5. 主成功、Kimi/GLM失败；
6. 主失败、对比成功；
7. 上游阻断；
8. 任务取消；
9. 进度过期；
10. 旧v2结果只读兼容。

每个场景必须证明Pipeline、Workflow结果文件、CLI、API、进度文件和前端文案一致。

## Phase 3：A2真实数据和零结果语义

### 代码落点

- `data/a2_market.py`
- `pipeline/a2_features.py`
- `pipeline/deterministic.py`
- `pipeline/data_readiness.py`
- `facts/contracts.py`
- A2结果索引与前端详情

### 必须实现

- 统一A2事实合同和适用域；
- 梯队与趋势代理拆分；
- 资金各语义拆分；
- 逐因子覆盖门；
- DATA_GAP池；
- A2 overall outcome按充分性决定；
- 20只股票人工抽样重算工具；
- 数据源健康和字段漂移检测。

任何新的资金或梯队来源进入正式A2前，必须经过数据源准入：

1. 连续至少20个交易日影子采集，不参与决策；
2. 记录成功率、全市场覆盖、发布时间延迟、字段漂移、重复率和历史可回放性；
3. 随机抽样不少于50只，与供应商页面或第二来源核对方向和单位；
4. 明确授权范围、限流、缓存与降级规则；
5. 达到成功率≥99%、关键字段覆盖≥95%、无未来数据后才可标记`PRODUCTION_ELIGIBLE`；
6. 未通过准入的来源只能显示在诊断面板，不能提高A2数据充分性。

### 验收门

- 资金源失败场景必为`INSUFFICIENT/UNKNOWN`。
- 真实覆盖充分且零焦点场景才为`SUFFICIENT/ABSENT`。
- 日涨幅代理不计入梯队覆盖率。
- Top N榜单外股票不被记为资金流0。
- 20只抽样的因子、权重、总分与人工计算一致。
- 本次104只历史产物重放后，不再把102只缺资金的情况判为确认无机会。

## Phase 4：主模型发布与对比任务真正解耦

### 代码落点

- `workflow.py`
- `pipeline/research.py`
- `runtime/state.py`
- `server/runner.ts`
- `server/scheduler.ts`
- 前端模型对比区域

### 必须实现

- 独立 primary/comparison 命令和作业记录；
- 主结果写入和计划发布不等待comparison；
- comparison关联parent run；
- Node重启续跑和幂等租约；
- comparison超时、失败、取消不改变primary；
- comparison结果晚到时只刷新对比区域。

### 验收门

- 注入Kimi/GLM各10分钟超时，DeepSeek结果仍在完成后60秒内可见。
- primary只发布一次计划。
- Node在primary完成、comparison未完成时重启，不重复primary。
- comparison取消后morning/close可正常调度。

## Phase 5：Feature Store完整物化和A1元数据

### 必须实现

- maintenance/run-bound域分离；
- generation manifest和必需表阈值；
- fundamentals、market roles、themes、chain nodes正式物化；
- CandidateCatalog；
- A1/A2/A3结果引用catalog；
- 前端详情稳定显示代码、名称、行业、主题、原因、证据与风险。

### 验收门

- 全量generation所有必需表达到阈值。
- 随机抽样50只与冻结快照直接重算一致。
- A1 approved 104只代码/名称覆盖100%。
- 行业/主题覆盖100%，无法映射的必须有明确`MAPPING_GAP`。
- 重复结果文件总体积显著下降，接口分页不加载22MB完整JSON。

## Phase 6：存储治理、备份恢复和资源门

### 必须实现

- 存储水位守卫；
- 引用感知保留；
- dry-run清理报告；
- SQLite备份、校验和临时恢复；
- WAL和数据库增长监控；
- 前端显示磁盘趋势和预计可用天数。

### 验收门

- 在复制目录演练清理，所有run绑定generation仍可读取。
- 恢复后的API能读取最新运行和历史回放。
- 人工注入低磁盘水位时全量任务fail closed，Node仍可只读服务。
- 连续三次全量运行无数据库锁死、无不可控swap增长。

## Phase 7：CI、覆盖率和全链业务验收

### CI门禁

- 整体行覆盖率不低于80%。
- 以下关键模块行和分支覆盖率不低于90%：
  - generation激活与run绑定；
  - outcome合同和状态聚合；
  - A2覆盖与零结果判断；
  - PIT守卫；
  - primary/comparison发布；
  - PaperBroker关键状态迁移。
- JSON Schema和生成TypeScript必须无diff。
- Python、Node、类型检查、生产构建、文档链接、迁移测试全部通过。

### 新增关键测试文件

- `tests/test_feature_generation_activation.py`
- `tests/test_historical_replay_isolation.py`
- `tests/test_outcome_contract_v3.py`
- `tests/test_a2_data_sufficiency.py`
- `tests/test_a2_tier_semantics.py`
- `tests/test_primary_publication_isolation.py`
- `tests/test_feature_generation_manifest.py`
- `tests/test_candidate_catalog.py`
- `tests/test_storage_retention.py`
- `test/server/outcome-contract.test.ts`
- `test/server/primary-comparison.test.ts`

### 真实业务验收

1. 至少10个独立交易日点时回放，10/10有明确终态。
2. 每日记录A1/A2/A3池、四类A2覆盖、状态、耗时、资源和模型失败。
3. 配置至少四个月合法券商月度金股，只用于盲测，不反哺选股。
4. 等待至少一个自然非空A3计划，完成早盘、A4、模拟入场、T+1和离场。
5. 10日只证明系统稳定；策略效果结论至少扩展到60个交易日并划分开发集和样本外集。

### 预计工程量与依赖

| 阶段 | 预计工程工作日 | 前置依赖 | 是否可与其他阶段并行 |
|---|---:|---|---|
| Phase 0 | 0.5–1 | 无 | 否，必须先完成 |
| Phase 1 | 2–3 | Phase 0 | 与业务数据调研可并行 |
| Phase 2 | 3–4 | Phase 1接口确定 | 不与状态相关前端改动并行 |
| Phase 3 | 4–6 | Phase 2状态合同 | 数据源20日影子采集单独计时 |
| Phase 4 | 2–3 | Phase 2 | 可与Phase 3后半段并行开发，验收需串行 |
| Phase 5 | 3–4 | Phase 1、Phase 3特征合同 | 否 |
| Phase 6 | 2–3 | Phase 1、Phase 5 | 可先实现只读水位监控 |
| Phase 7 | 3–5 | Phase 1–6 | 真实10日和自然计划等待时间单独计算 |

预计纯工程量约17.5–26个工作日；20日数据源准入、10个真实回放日和自然非空计划属于客观观察窗口，不能通过压缩开发时间或构造数据冒充完成。

## 10. 发布与迁移方案

### 10.1 提交边界

每个Phase独立提交，不允许把P0状态修复、A2策略改动和存储清理混在一个不可回滚提交中。

推荐提交序列：

```text
fix(feature-store): separate seal bind and activate
feat(outcomes): introduce generated outcome contract v3
fix(a2): enforce factor-specific sufficiency semantics
refactor(research): split primary and comparison jobs
feat(features): materialize required feature domains
feat(storage): add retention and recovery guards
test(acceptance): enforce critical workflow gates
docs(acceptance): publish verified remediation evidence
```

### 10.2 数据库迁移

- 迁移只新增表、列、索引和触发器，不删除旧数据。
- 部署前在事实库和Feature Store副本上执行。
- 旧v2 run继续只读，不回写v3。
- active pointer迁移前后记录hash和generation。
- 新代码首次启动只校验迁移，不自动激活新generation。

### 10.3 虚拟机切换

1. 等待当前任务自然结束；若无任务则获取调度租约。
2. 备份代码、配置、SQLite和active pointer。
3. 拉取指定提交，安装依赖，执行只读迁移检查。
4. 使用`www:www`执行迁移和重建。
5. 启动Node，检查健康和只读API。
6. 先运行隔离fixture，再运行一个历史点时回放。
7. 验证历史回放没有改变active。
8. 再运行正式close-primary。
9. 主结果发布后启动comparison。
10. 全部门禁通过后恢复定时调度。

### 10.4 回滚

- 代码回滚到部署前提交。
- 数据库使用向前兼容迁移，不执行破坏性down migration。
- active pointer只能通过审计API恢复到备份记录的generation，禁止手工SQL。
- v3前端可回退显示v2只读结果，但新运行不得重新写v2作为权威。
- A2新数据源可以关闭；关闭后必须显示`NOT_CONFIGURED/UNKNOWN`。
- comparison可以整体关闭，不影响primary。

## 11. 总体验收矩阵

| 问题 | 关键验证 | 最终通过标准 |
|---|---|---|
| R1 历史代际污染 | G2 active时回放G1 | active始终为G2，run绑定G1 |
| R2 A2零结果误判 | 缺资金与充分数据两组fixture | 分别为UNKNOWN与ABSENT |
| R3 状态漂移 | 六端合同矩阵 | Pipeline/Workflow/CLI/progress/API/UI完全一致 |
| R4 opportunity歧义 | A1>0、A2=0、A3=0 | 研究机会有，焦点机会无，计划无 |
| R5 发布耦合 | shadow超时注入 | primary发布不等待shadow |
| R6 梯队代理 | 无梯队、有日涨幅场景 | trend有值，tier不伪装可用 |
| R7 特征物化 | full generation manifest | 必需表覆盖达标才可激活 |
| R8 元数据缺失 | A1全池校验 | 代码名称100%，映射缺口显式 |
| R9 测试不足 | per-module coverage gate | 关键模块行/分支≥90% |
| R10 存储风险 | 清理和恢复演练 | 引用不丢、可恢复、低水位关闭写入 |
| R11 报告偏差 | 自动生成验收数据 | 报告数值来自manifest，不手抄 |
| R12 业务样本 | 10日、金股、自然计划 | 分别标记真实完成，不以fixture代替 |

## 12. 防止再次“实现不完整”的治理门

### 12.1 完成状态词典

| 状态 | 含义 |
|---|---|
| `PLANNED` | 只有方案 |
| `IMPLEMENTED` | 代码已完成，尚未充分验证 |
| `VERIFIED_LOCAL` | 本地自动化验证通过 |
| `VERIFIED_CI` | CI和合同门通过 |
| `DEPLOYED_UNVERIFIED` | 已部署但未完成真实运行验收 |
| `VERIFIED_VM` | 虚拟机同提交验收通过 |
| `PENDING_BUSINESS_ACCEPTANCE` | 技术完成，等待真实样本 |
| `BUSINESS_ACCEPTED` | 真实数据和真实流程验收完成 |

只有`BUSINESS_ACCEPTED`可以在最终报告中写“已完成”。工具、接口、fixture或单日样本不能升级为业务完成。

### 12.2 每个PR必须回答

1. 修改解决哪个R编号？
2. 根因是什么，为什么不是表面修补？
3. 数据和状态合同是否变化？
4. 如何证明没有未来数据和跨代际污染？
5. 失败、重启、并发、超时、磁盘不足时如何处理？
6. 有哪些单元、合同、集成、回放和VM证据？
7. 如何回滚？
8. 尚未完成什么？

缺少任一答案不得合并。

### 12.3 验收报告自动生成

最终验收报告的股票数、覆盖率、耗时、generation、提交、模型状态和资源指标必须从manifest/API/SQLite自动生成。人工只撰写解释和结论，禁止手工复制数值，避免再次出现0.72与0.87等口径偏差。

## 13. 两轮方案复核

### 13.1 第一轮：业务逻辑复核

复核问题：方案是否仍符合“宏观和产业周期定方向，A2从板块中识别核心股票，A3形成技术计划”的核心目标？

结论：符合。A1职责保持不变；A2没有被缩减成单纯量价排名，而是明确资金、梯队、龙头和产业链共振四类事实；A3仍只对A2已确认候选生成计划。数据不足不会被放宽阈值绕过，也不会为了产生计划裁剪全市场或强制造单。

本轮调整：新增A2 `DATA_GAP`池、逐因子充分性门和三层机会状态，避免A1研究候选被误报成可执行机会。

### 13.2 第二轮：技术架构、性能和长期运行复核

复核问题：方案是否会重复出现大快照、多模型阻塞、SQLite污染、进度漂移和磁盘失控？

结论：关键风险均有明确边界。Feature generation发布与激活分离；主模型与对比模型成为独立作业；状态通过生成合同统一；CandidateCatalog减少重复JSON；存储水位、引用保留和恢复演练成为发布门。

本轮调整：增加active pointer CAS与as_of单调性、comparison持久租约、generation必需表manifest、每模块覆盖率门和验收报告自动生成。

## 14. 剩余外部风险

1. 同花顺或第三方接口的授权范围、字段和稳定性可能变化，必须通过source manifest和字段漂移检测管理。
2. 免费数据源不能保证历史全市场主力资金流；没有合法可靠来源时，A2应保持降级，而不是用代理数据补齐。
3. 模型结构化输出仍可能超时或违反合同；主路径解耦可以控制影响，但不能消除供应商风险。
4. 10日回放只能证明工程稳定，不能证明投资胜率；策略有效性需要更长样本外周期。
5. 当前磁盘余量已进入警戒区，正式实施前必须先完成一致备份和dry-run清理评估，但删除任何数据仍需单独授权。

## 15. 最终完成定义

本项目本轮整改只有同时满足以下条件才算完成：

- 历史回放连续三次不改变active generation；
- A2缺数据与真实无机会在全部六端语义一致；
- A1/A2/A3机会层级不再混淆；
- DeepSeek主结果发布不等待Kimi/GLM；
- A2真实梯队与趋势代理彻底分离；
- Feature Store必需域完整物化并通过manifest；
- A1股票代码、名称、行业、主题、原因和证据可完整查看；
- 关键模块行/分支覆盖率达到90%，整体达到80%；
- 虚拟机连续三次运行满足资源和状态门；
- 10个真实交易日回放完成；
- 金股盲测数据合法配置并完成至少四个月评估；
- 至少一个自然非空计划完成早盘、A4和隔离模拟闭环；
- 备份恢复演练成功；
- 最终报告全部数据由系统自动生成，未完成项不再被写成已完成。

在这些条件完成前，系统只能用于研究和隔离模拟，不得接入真实交易，也不得宣称策略或工作流已经完成最终业务验收。
