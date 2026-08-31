# 特征维护内存峰值修复与生产验收方案

> 文档版本：1.0  
> 编制日期：2026-08-31  
> 适用项目：`liangjian_funnel_workflow`  
> 方案性质：设计、实施与本地复审记录；代码已在本地修改，尚未提交、推送、部署或完成生产数据验收  
> 安全边界：不改变 A1/A2/A3/A4 选股规则，不缩减全市场股票池，不连接真实交易，不自动删除历史快照或特征代际

## 1. 结论

本次 OOM 的主因是维护程序把多份 200MB 以上的单体 JSON 快照逐份完整解析、重新序列化计算哈希，并同时保留在 Python 对象列表中。4GB 虚拟机只是放大了问题；单纯扩容不能根治。

修复采用“先止血、再改读取边界、最后做批量写入和生产验收”的顺序：

1. 在读取任何快照前完成维护日历、互斥锁和增量脏队列判断；没有工作时直接 `NOOP_NO_DIRTY`。
2. 定时维护不再解析历史单体 JSON，而是消费现有 Feature Store 中随冻结快照生成的、不可激活的 `LIVE_SOURCE` 源代际。
3. 源代际在快照仍位于内存时按固定小批次写入；维护时使用 SQLite `INSERT ... SELECT` 在代际间复制，禁止重新构造全市场 Python 对象。
4. 保留当前 Feature Store 的代际、严格 CAS 和 active 指针模型；本轮不引入 Redis、消息队列、Parquet、分布式任务或新的数据库服务。
5. 在 4GB 虚拟机上用不少于当期生产 G0 数量（当前基线为 4,017 只）的真实规模源代际完成峰值内存、失败恢复、数据等价和发布安全验收。

目标不是让任务“勉强不死”，而是保证：空队列近似零成本、增量只复制受影响实体、全量内存有界、失败不污染 active 代际、同一输入的业务结果不变化。

## 2. 当前生产证据与根因

### 2.1 生产证据

2026-08-31 03:30 的实际运行证据：

| 项目 | 观测值 |
|---|---:|
| 虚拟机物理内存 | 3.8GiB |
| 被杀 Python 匿名 RSS | 约 2.62GiB |
| 被杀 Python 虚拟内存 | 约 2.88GiB |
| 当时结果 | Linux OOM Killer 杀死进程，Node 记录 `exit=null` |
| 顶层冻结快照 | 7 份 |
| 顶层冻结快照总大小 | 1.37GiB |
| 大于 100MiB 的快照 | 6 份 |
| 最新快照 | 261.5MiB |
| 特征库 | 约 1,002MiB |
| 当前脏实体 | 0 |

最新 261.5MiB 快照主要内容：

| 命名空间 | 文件占用 |
|---|---:|
| `DISCLOSURE_EVENTS` | 86.2MiB |
| `COMPANY_FUNDAMENTALS` | 54.8MiB |
| `A2_FACTOR_SNAPSHOT` | 33.6MiB |
| `RECENT_DAILY_BARS` | 28.1MiB |
| `THS_CONCEPT_MEMBERSHIP` | 12.0MiB |
| `RISK_EVENTS` | 10.1MiB |
| `CAPITAL_FLOW_SNAPSHOT` | 9.4MiB |

### 2.2 直接代码根因

1. `feature_maintenance._load_snapshot()` 使用 `path.read_text()` 和 `json.loads()`，同时保留原始文本与完整 Python 对象树。
2. 同一函数为校验 `snapshot_hash` 又执行全量规范化 JSON 序列化，制造第二个大对象峰值。
3. `load_latest_verified_snapshot()` 遍历所有候选文件，把每一份有效快照追加到 `valid` 列表，直到全部解析完成才选择 `as_of` 最大者。
4. `run_feature_maintenance()` 在判断周日、脏队列和实际工作量之前先加载快照。因此当前脏队列为 0，仍然走进最重的内存路径。
5. `SnapshotFeatureBuilder._write_bulk()` 一次构造全市场 `members`；`record_feature_generation_members()` 又一次性构造全部序列化 `rows`。
6. 测试制品只有 3 只股票，没有覆盖多份 200MB 快照、4,000 只股票和内存预算。

### 2.3 为什么文件缓存没有解决

磁盘缓存只改变“数据从哪里来”，没有改变“程序如何读取”。当前代码仍把磁盘文件完整展开到内存。Redis 也不会解决此问题：如果仍然一次取回全部值，只会增加一套服务和网络序列化成本。

## 3. 修复原则与不可变边界

### 3.1 修复原则

- **先判断是否有工作，再读取数据。** 空队列不得打开大快照。
- **只读取本次需要的实体。** 增量批次不得物化全市场对象树。
- **不可变源与可重建投影分离。** 研究 JSON 快照继续用于审计/回放；维护读取同一 Feature Store 内不可激活的轻量 `LIVE_SOURCE` 源代际。
- **写入批次有界。** 任意 Python 容器不得随全市场规模线性无限增长。
- **失败关闭。** 源代际缺失、损坏、版本不匹配或水位不足时不回退到旧快照冒充最新数据。
- **代际发布原子。** staging 构建失败时 active generation 不变；显式 CAS 冲突不得被自动放宽。
- **业务等价。** 修复只改变数据装载和写入方式，不改变 A1/A2/A3 因子、阈值、排序和模型提示词。

### 3.2 本轮明确不做

- 不引入 Redis、Kafka、RabbitMQ、Celery、Airflow 或独立微服务。
- 不把 SQLite 改成 PostgreSQL/MySQL。
- 不把全市场股票池裁剪成抽样池。
- 不重写 Feature Store 代际模型为复杂的多层 overlay。
- 不自动删除任何被运行、active、previous 或回放引用的快照/代际。
- 不通过放宽 A2/A3 门槛制造非空计划。
- 不把扩容虚拟机作为验收通过条件。

## 4. 目标调用链

### 4.1 工作日增量

```text
03:30 调度
  → 维护开关、日期和跨进程租约检查
  → 原子领取最多 N 个 dirty_entities
      → 无任务：NOOP_NO_DIRTY，结束，不读取快照
      → 有任务：获得 ClaimedDirtyBatch
  → 选择最新 SEALED、验证通过的 LIVE_SOURCE 源代际
  → 校验源代际身份、完整性、市场日期和 dirty 版本包含关系
  → 从 active generation 克隆维护范围投影到 staging
  → 使用 INSERT ... SELECT 从源代际替换 claimed/expanded 实体
  → 覆盖率与内容哈希验证
  → seal → 严格 CAS activate
  → 完成 dirty 租约
```

### 4.2 周六全量

```text
03:30 调度
  → 维护开关、日期和跨进程租约检查
  → 选择最新 SEALED、验证通过的 LIVE_SOURCE 源代际
  → 使用 SQLite INSERT ... SELECT 复制维护范围表到全新 staging generation
  → 全量计数、覆盖、哈希、SQLite quick_check
  → seal → 严格 CAS activate
```

### 4.3 周日

周日必须在访问快照和 Feature Store 大表之前直接返回：

```json
{"status":"NOOP","reason_code":"NON_MAINTENANCE_DAY"}
```

## 5. Feature Store 源代际设计

### 5.1 复用现有存储，不新增第二套制品库

项目现有 `research_feature_store.sqlite3` 已具备：

- `feature_generations` 的 staging/validated/sealed 生命周期；
- `feature_generation_members` 的 `snapshot-inputs` 分区；
- 基本面、行业/概念、主营事实等维护范围表；
- `source_manifest_hash`、validation manifest、严格 CAS 和 active/previous 指针；
- 基于 generation 的引用与保留计划。

生产证据显示 `LIVE_FULL` 代际已经能够保存 4,017 条 `snapshot-inputs`；当前 `RUN_SNAPSHOT` 代际成员数为 0。缺口是“新鲜快照没有生成可供维护复制的源代际”，不是缺少另一种数据库。

不直接把完整输入写进现有 `RUN_SNAPSHOT`：它与研究运行永久绑定并受引用保护，如果每天增加数千条大 payload，会把短期维护输入变成长期审计负担。独立的 `LIVE_SOURCE` 仍位于同一个 Feature Store，但不绑定研究 run，允许在不影响审计结果的前提下只保留最新、previous 和在建引用。

因此新增 purpose：

```text
LIVE_SOURCE
```

它具有以下约束：

- 来源是一个已冻结的 live 研究快照；
- `activation_eligible=false`，永远不能成为 active research generation；
- 只保存维护需要的 `snapshot-inputs`、基本面、分类成员和主营事实，不保存公告全文、新闻正文和 LLM 原文；
- 完成分批物化与验证后 seal；只有 SEALED 且 validation READY 才能被维护读取；
- historical replay、TEST_ONLY 和普通 `RUN_SNAPSHOT` 不作为 live maintenance 输入。

### 5.2 源代际最小验证合同

源代际的 metadata/validation manifest 至少包含：

```json
{
  "schema_version": "live-source-generation/1.0.0",
  "snapshot_id": "snapshot-...",
  "snapshot_hash": "...",
  "as_of": "2026-08-31T15:10:00+08:00",
  "market_trade_date": "2026-08-31",
  "g0_count": 4017,
  "member_root_hash": "...",
  "namespace_contract": [
    "g0_candidates",
    "RECENT_DAILY_BARS",
    "COMPANY_FUNDAMENTALS",
    "FACTOR_SNAPSHOT",
    "A2_FACTOR_SNAPSHOT",
    "LIQUIDITY_SNAPSHOT",
    "TRADABILITY_FLAGS",
    "THS_INDUSTRY_MEMBERSHIP",
    "THS_CONCEPT_MEMBERSHIP",
    "MAIN_BUSINESS_EVIDENCE"
  ],
  "namespace_freshness": {
    "RECENT_DAILY_BARS": {
      "expected_trade_date": "2026-08-31",
      "fresh_count": 4000,
      "explained_stale_count": 17,
      "unexplained_stale_count": 0
    }
  },
  "source_versions": {},
  "status": "READY"
}
```

`member_root_hash` 由按 symbol 排序的 `(symbol, content_hash, row_count)` 迭代计算，不重新序列化全市场对象。

### 5.3 生成与选择规则

- `Workflow.prepare_snapshot()` 写出研究快照时已经持有 `data`；此时按 25～200 只一批投影到一个 STAGING `LIVE_SOURCE`，不得从 JSON 文件反向读取。
- 同一 `snapshot_hash` 的 `LIVE_SOURCE` 创建必须幂等；已经存在 SEALED/READY 源代际时直接复用，STAGING/FAILED 只能按明确恢复规则处理，不能重复堆积完整副本。
- 每批只构造当前批次 payload/rows，提交后立即释放引用。
- 全量计数、symbol 集合、成员根哈希、基本面/分类/主营覆盖和 `PRAGMA quick_check` 通过后才 seal。
- live maintenance 只选择 `purpose=LIVE_SOURCE`、`status=SEALED`、validation READY、`as_of <= maintenance_at` 的源代际。
- 同一最大 `as_of` 出现不同 `source_manifest_hash` 时返回 `FEATURE_SOURCE_GENERATION_AMBIGUOUS`，不按创建时间任意选择。
- `as_of` 不能替代市场新鲜度。日线必须覆盖 `market_trade_date`；停牌等合法旧日期必须由同一时点的可交易标志逐只解释。
- dirty item 已携带 `dependency_hash/source_version` 时，源代际必须证明包含该版本；无法证明则 `RETRY_WAITING_FOR_SOURCE_GENERATION`。
- scheduled maintenance 禁止回退到解析旧 JSON；没有合格源代际时返回 `FEATURE_SOURCE_GENERATION_MISSING`。

### 5.4 失败边界

源代际物化失败时：

- 已完成的研究快照仍可用于当次 A1/A2/A3 和历史审计；
- 源代际标记 FAILED 或保留为可识别的未完成 staging，绝不 seal；
- 维护能力标记为 `BLOCKED_SOURCE_GENERATION`；
- 当前 active live generation 保持不变；
- 不把源代际失败误报为“市场无机会”。

源代际结构完整但行情新鲜度失败时同样不能 READY。本次 2026-08-30 快照的 4,017 只 `RECENT_DAILY_BARS` 全部停在 2026-08-27，就是必须被该合同阻断的真实样例。

## 6. 代码修改范围

### 6.1 `feature_maintenance.py`

1. 把日期/模式判断移到任何快照读取之前。
2. 增量任务先原子领取脏实体；无任务立即返回。
3. 删除 scheduled 路径中的 `load_latest_verified_snapshot()` 全目录 eager 扫描。
4. 用轻量 SQL 查询选择最新合格 `LIVE_SOURCE`，只读取 generation metadata 和 validation manifest。
5. 增量通过 generation id 和 claimed entities 调用数据库内复制，不再创建 `SnapshotFeatureBuilder` 或读取 `snapshot.data`。
6. 任何领取后的失败都必须进入现有 retry/dead-letter 语义；不得让租约永久停留在 LEASED。

### 6.2 `feature_rebuild.py`

1. 将增量的“领取工作”和“构建执行”边界显式化，使用窄对象 `ClaimedDirtyBatch`。
2. 领取发生在昂贵输入打开之前。
3. 领取后源代际缺失、损坏或版本不足时，把项目置为 `RETRY`，原因码稳定且不泄漏路径。
4. 保留严格 `activate_generation(expected_current_id=...)`；CAS 冲突不重试激活，只释放/重排 dirty 项。
5. 全量仍创建独立 staging generation，不原地修改 active。

### 6.3 `feature_store.py`

1. `record_feature_generation_members()` 增加有界批次写入入口，单批默认 50，允许配置 25～200，用于生成 `LIVE_SOURCE`。
2. 单批内部只保存该批的序列化 rows；提交后释放引用。
3. 基本面、业务事实和分类成员采用同样的批次上限。
4. 扩展 purpose 合同支持不可激活的 `LIVE_SOURCE`，并增加最新合格源代际查询。
5. 增加按实体从 source generation 复制到 staging generation 的 SQL 接口；全量和增量均按有界 symbol 批次执行 `INSERT ... SELECT`，避免单个超长写事务和巨型 rollback/WAL 峰值。
6. 不修改现有 active/previous/run binding 的读取规则。
7. 不修改兼容 wrapper 与显式 CAS 的严格性边界。

### 6.4 `storage_governance.py`

1. 将最新两个 SEALED/READY `LIVE_SOURCE` 和被 staging target metadata 引用的 source generation 标记为保留。
2. 其他未引用源代际只进入现有 advisory retention plan，不在维护任务中自动删除。
3. 增加按当前平均源代际大小计算的 7/14 日磁盘增长预测；达到 watermark 时阻断新全量，不删除 active/previous/run-bound 数据。

### 6.5 `workflow.py`

1. 在冻结研究快照时同步创建 STAGING `LIVE_SOURCE` 源代际。
2. 源代际使用现有内存中的 `data` 按 symbol 分批投影，不复制整个 `data`，也不重新读取 JSON。
3. 研究快照与源代际分别记录结果；源代际失败不得伪装成研究快照成功。
4. 进度增加 `FEATURE_SOURCE_GENERATION` 阶段和完成/阻断原因。

### 6.6 Node/前端

- Node 子进程被信号杀死时记录 `SIGNAL_KILLED` 和 signal，不再只显示 `exit=null`。
- 前端分别显示：`NOOP_NO_DIRTY`、`BLOCKED_SOURCE_GENERATION`、`FAILED_RESOURCE`、`PUBLISHED`。
- 不展示内存中的业务原文，只展示阶段、计数、峰值 RSS、源代际身份和稳定原因码。

## 7. 内存、批次和性能预算

预算以 4GB 虚拟机为硬约束，不以扩容为通过条件：

| 场景 | 目标峰值 RSS | 目标耗时 | 数据读取 |
|---|---:|---:|---|
| 周日 NOOP | 峰值 <200MiB，且相对 CLI 启动基线增量 <30MiB | <1s | 不打开大文件 |
| 工作日空脏队列 | 峰值 <220MiB，且相对 CLI 启动基线增量 <50MiB | <2s | 只读队列/元数据 |
| 100 个脏实体增量 | <350MiB | <10min | SQL 复制受影响实体及必要共享数据 |
| 不少于当期生产 G0 数量的全量 | <700MiB | <60min | SQLite `INSERT ... SELECT` |
| 快照生成并物化 `LIVE_SOURCE` | 相对现有快照生成基线增量 <200MiB，生产绝对峰值 <1.8GiB | 额外耗时 <10min | 只序列化当前批次 |

这些值是验收上限，不参与业务筛选。若实际生产数据使单股票 payload 异常大，应按单实体大小拒绝并进入数据质量诊断，不允许扩大全局批次掩盖问题。

只在阶段边界记录 RSS：`PRECHECK`、`SOURCE_SELECT`、`BUILD`、`VALIDATE`、`PUBLISH`。本轮不增加常驻采样线程或独立监控服务。

## 8. 边界条件与失败语义

| 场景 | 必须行为 | 禁止行为 |
|---|---|---|
| 脏队列为空 | `NOOP_NO_DIRTY`，不打开快照、不查询大 payload 表 | 为了“验证”扫描全部历史快照 |
| 新 dirty 在领取后到达 | 保持 PENDING，下一批处理 | 合并进已冻结批次导致输入漂移 |
| dirty 版本晚于源代际水位 | `RETRY_WAITING_FOR_SOURCE_GENERATION` | 使用旧源代际发布新代际 |
| dirty 已领取但源代际读取失败 | 当前 claimed batch 进入 RETRY/租约可回收 | 留在永久 LEASED |
| 最新源代际缺失 | 阻断维护并保留 active | 回退旧快照冒充最新 |
| 最新源代际日线日期落后 | `FEATURE_SOURCE_MARKET_DATA_STALE` | 用较新的 `as_of` 掩盖较旧的行情日期 |
| 同一 as_of 有不同 live hash | `FEATURE_SOURCE_GENERATION_AMBIGUOUS` | 依赖 created_at 任意选一个 |
| 候选源属于历史回放/测试 | 从 live source 候选中排除 | 激活到生产域 |
| 源代际未 seal 或 validation 不完整 | `FEATURE_SOURCE_GENERATION_INVALID` | 尝试修补后继续发布 |
| SQLite quick_check 失败 | 阻断并告警 | 激活部分结果 |
| 写源代际时磁盘满 | 源代际 FAILED/staging 可识别，研究快照不回滚 | 删除旧 active/历史快照腾空间 |
| 构建中进程被杀 | active 不变；租约到期后可重领；staging 标记/识别为孤儿 | 将 staging 当作完成 |
| 两个维护进程并发 | durable lease 只允许一个进入构建；CAS 提供第二层保护 | 仅依赖 Node 内存锁 |
| CAS 冲突 | 当前构建不激活，记录冲突并重排 dirty | 刷新 expected id 后偷偷覆盖胜者 |
| CAS 成功后、dirty 完成前进程被杀 | active 保持新代际；dirty 到期后幂等重算并核对是否已包含 | 回滚已经成功的 active 指针或直接丢弃 dirty |
| 全量某一股票损坏 | 整代不激活，明确坏实体 | 跳过股票后宣称全量成功 |
| 增量某一股票损坏 | 该批失败/重试，active 不变 | 发布缺少该股票的增量代际 |
| Node 重启 | 从数据库租约和代际状态恢复 | 依靠内存进度推断完成 |
| 旧 JSON 没有源代际 | scheduled 路径明确阻断 | 在 4GB VM 上隐式 eager 转换 |
| active generation 缺失 | `FEATURE_ACTIVE_GENERATION_MISSING` | 将增量静默升级为全量重建 |
| 单股票 payload 异常大 | 隔离该实体并给出稳定数据质量原因，整批不发布 | 自动扩大批次/内存上限 |

## 9. 迁移与部署顺序

### P0：安全止血

1. 增加独立 `LIANGJIAN_FEATURE_MAINTENANCE_ENABLED` 开关，默认开启，只控制 03:30 特征维护，不影响早盘、收盘研究和 A4。
2. 周日和空脏队列前置返回。
3. scheduled 路径禁止扫描全部历史 JSON；缺少 READY `LIVE_SOURCE` 时可诊断失败，不允许 OOM。
4. Node 正确记录 signal-killed。

验收：在当前 7 份/1.37GiB 快照仍存在的情况下，空脏队列维护 <2 秒结束，峰值 RSS <220MiB、相对启动基线增量 <50MiB，并由文件访问探针证明未打开任何历史大 JSON。

P0 与 P1 必须作为同一发布批次交付，或在首份 SEALED/READY `LIVE_SOURCE` 生成前保持特征维护开关关闭，避免出现“旧 eager 路径已禁用但新源代际尚不可用”的部署空窗。

### P1：物化 LIVE_SOURCE 源代际

1. 扩展现有 Feature Store purpose/validation 合同，不新增数据库文件。
2. 新冻结快照写研究 JSON 的同时分批写 `LIVE_SOURCE`。
3. 使用新鲜且日线完整的生产快照生成第一份 SEALED/READY 源代际。
4. 对源代际与同一内存 `data` 做全市场内容哈希、行数和抽样字段一致性验证。
5. 明确验证 `market_trade_date`、日线最新日期分布以及停牌/无交易日解释集合，不能只验证 `as_of`。

不在生产 VM 上把现有 261.5MiB JSON eager 转换为源代际。旧历史快照继续作为审计文件；本轮不新增通用历史转换器，确有单次回放需要时另行评估并授权。

### P2：维护切换为代际内 SQL 复制

1. 增量只复制 claimed/expanded entities。
2. 全量使用 generation table `INSERT ... SELECT`。
3. 数据库内复制到 staging generation，完成后统一校验、seal、CAS activate。
4. 前端显示真实阶段与内存指标。

### P3：生产验收与观察

1. 手动执行一次空队列增量。
2. 构造隔离测试库中的 1、100 个 dirty 实体，不污染生产 active。
3. 使用不少于当期生产 G0 数量的 `LIVE_SOURCE` 执行一次全量 staging 验证。
4. 在确认 validation、quick_check、内容等价和内存预算后才允许激活。
5. 连续观察 5 个维护窗口；任一窗口超预算即回滚维护开关，不影响 A1-A4 调度。

## 10. 数据等价与业务不变验收

对同一个 `snapshot_id/snapshot_hash`，旧实现和新实现必须比较：

- G0 symbol 集合完全一致；
- 每只股票 feature member `content_hash` 一致；
- 日线 bar count 和最后交易日一致；
- 基本面、A2 因子、流动性、可交易标志一致；
- 行业/概念成员集合一致；
- 主营业务事实集合一致；
- generation validation manifest 的覆盖项一致；
- 至少一次冻结研究回放证明 A1/A2/A3 的确定性阶段输入/输出哈希一致；这只是兼容性检查，不替代 10 个交易日策略效果验收。

允许变化的只有：源代际 id、批次号、维护耗时、RSS、staging generation id 和运行时间戳。

## 11. 测试方案

### 11.1 单元测试

- 周日返回前不得调用 snapshot loader 或查询 Feature Store 大 payload 表。
- 空 dirty 返回前只允许轻量队列/租约查询，不得选择或复制源代际。
- 多份历史 JSON 存在时 scheduled 路径不打开它们。
- `LIVE_SOURCE` 必须按 `STAGING → VALIDATED → SEALED` 发布，STAGING/FAILED 不可被选择。
- source generation metadata/validation/snapshot 身份任一不一致均失败关闭。
- live/replay/test purpose 隔离；未来 as_of、相同 as_of 冲突 hash 均失败关闭。
- `as_of` 新但全市场日线旧的真实结构 fixture 必须返回 `FEATURE_SOURCE_MARKET_DATA_STALE`。
- 合法停牌股票可以保留旧 bar，但必须由同一时点 tradability 逐只解释。
- dirty 水位高于 source generation 水位进入 retry。
- `LIVE_SOURCE` 物化每批不得超过配置上限。
- 源代际投影对中文、空字段、超长公告引用、北交所代码保持一致。
- CAS 冲突不覆盖 active。
- 失败后 dirty 租约可回收，staging 不可读取为 active。

### 11.2 集成测试

- 不少于当期生产 G0 数量的符号，以及代表性日线/基本面/A2/分类/主营 payload。
- 同时存在多份历史 JSON 路径；使用文件访问 spy 或稀疏占位文件证明 scheduled maintenance 不读取它们，不在普通 CI 中制造 1.4GiB 测试垃圾。
- 增量 1/100 个实体；全量覆盖当期生产 G0 数量。
- 进程在 BUILD、VALIDATE、ACTIVATE 前被终止后的恢复。
- 进程在 CAS 成功、dirty complete 之前被终止后的幂等恢复。
- 磁盘不足、数据库锁、损坏 source validation、损坏 SQLite、并发发布。
- Feature Store `PRAGMA quick_check`、active/previous/run binding 完整性。

### 11.3 内存回归

在独立子进程中运行，Linux CI 使用标准库 `resource` 或 `/usr/bin/time -v` 记录最大 RSS；不引入 `psutil`。CI 使用缩小但结构等价的数据集验证“维护内存不随历史 JSON 份数增长”，生产 VM 使用不少于当期生产 G0 数量的 `LIVE_SOURCE` 验证绝对预算。

## 12. 回滚与恢复

- 新维护路径上线前保留旧 active generation，不迁移或原地改写。
- 新 `LIVE_SOURCE` 是不可激活的附加代际；回滚代码时可以保留，旧代码不会把它设为 active。
- 如维护异常，关闭 `LIANGJIAN_FEATURE_MAINTENANCE_ENABLED`，不关闭早盘、收盘和 A4。
- 回滚代码后不得重新启用旧 eager 维护路径；应保持特征维护关闭，直到修复版恢复。
- staging/临时文件只通过引用扫描计划列出，不能在回滚脚本中递归删除。
- active generation、previous generation、run binding 和研究快照在回滚前后均执行只读完整性检查。

## 13. 完成定义

以下条件全部满足才算修复完成：

1. 空 dirty 生产维护不读取任何大快照，<2 秒结束。
2. 不少于当期生产 G0 数量的全量构建峰值 RSS <700MiB，且不触发 swap 激增/OOM。
3. 增量只复制 claimed/expanded 实体，100 只峰值 RSS <350MiB。
4. 最新 SEALED/READY `LIVE_SOURCE` 与研究快照身份一致，日线数据新鲜度满足目标交易日。
5. 全量和增量失败均不改变 active generation。
6. 数据等价检查全部通过，A1/A2/A3 确定性结果无业务漂移。
7. Node、CLI、SQLite 状态和前端显示同一维护终态与原因码。
8. 本地测试、Linux CI、生产 VM 验收全部通过。
9. 连续 5 个维护窗口没有 OOM、孤儿租约、错误激活或任务重叠。
10. `LIVE_SOURCE` 物化没有把 OOM 从 03:30 转移到快照生成阶段，且 14 日磁盘增长预测不触发阻断水位。

## 14. 首轮复审记录

> 本节保留第一轮当时的审查轨迹；其中“独立 SQLite 制品”结论已在第三轮被现有 Feature Store `LIVE_SOURCE` 方案替代，不代表最终实施选择。

### 14.1 复审重点

- 是否为了修一个本地 OOM 引入了不必要的服务、存储系统或分布式架构。
- 验收指标是否脱离当前 Python/Node 真实基线。
- 测试本身是否制造无意义的大文件、长耗时或 CI 不稳定。
- 是否把当前 4,017 只错误固化为永久业务常量。
- 是否把工程兼容回放扩大成重复的策略效果工程。
- 空队列、无 active generation、制品缺失和 dirty 版本不匹配时是否可能读旧数据或错误发布。

### 14.2 发现与调整

1. **保留 SQLite 制品，拒绝 Redis/消息队列/Parquet。** SQLite 是现有依赖，按 symbol 查询即可解决问题；引入新的常驻服务属于过度设计。
2. **保留现有 Feature Store 代际 clone/CAS。** 本轮不实现 copy-on-write overlay。代际复制主要是磁盘和 I/O 成本，不是本次 2.62GiB OOM 的直接根因；先增加指标，后续以数据决定是否优化。
3. **调整内存预算。** 初稿 `<100/120MiB` 低于生产 Python 导入基线，已改为绝对上限加相对基线增量，避免得到无法稳定复现的假失败。
4. **删除 CI 大文件要求。** 普通 CI 不创建 7 份 200MiB 文件，改用访问 spy/稀疏文件证明历史 JSON 未被打开；真实绝对 RSS 只在 Linux 专项和生产 VM 验收。
5. **移除固定股票数合同。** 4,017 只仅作为当前证据，验收规模改为不少于当期生产 G0 数量，避免新股、退市和 G0 规则变化引发错误阻断。
6. **限制水位设计。** 不建设通用数据水位平台，仅使用 dirty 已有 `dependency_hash/source_version` 与制品身份证明包含关系；无法证明就重试等待新制品。
7. **限制回放范围。** 本修复只要求一次冻结回放证明工程等价；10 日策略效果仍属于独立业务验收，不能为了内存修复重复建设。

### 14.3 首轮结论

方案没有必要引入外部缓存或分布式组件。最小且足够的技术变化为：SQLite 按股票制品、空队列前置、领取后按需读取、批次写入、严格代际发布和有限可观测性。保留自动删除、代际 overlay、通用水位平台和常驻内存监控在线外，避免扩大项目边界。

## 15. 第二轮复审记录

> 本节保留第二轮当时的边界审查轨迹；manifest/独立制品相关实现已由第三轮源代际方案吸收，日期、purpose、租约和 CAS 结论继续有效。

### 15.1 复审重点

- 空队列、多快照、损坏快照、并发快照和回放快照是否会走错输入。
- dirty 领取、制品选择、staging 构建、CAS 激活、dirty 完成各断点是否可恢复。
- 日期、数据新鲜度和运行 purpose 是否可能被混淆。
- 磁盘满、SQLite 锁、进程被杀、单实体异常大是否会污染 active。
- 部署顺序是否会制造维护不可用空窗。
- 新增的 freshness、purpose、租约和 manifest 是否可以用现有 SQLite/状态合同完成，是否无意演变成通用数据平台或后台守护服务。

### 15.2 发现与调整

1. **补充市场日期合同。** 初稿只有 `as_of`，无法阻止“8月30日快照包含8月27日日线”。已增加 `market_trade_date` 和命名空间新鲜度；停牌旧 bar 必须逐只解释。
2. **禁止静默选择旧制品。** 最新 live 制品缺失、损坏或不新鲜时必须阻断；不能回退旧快照后仍标记成功。
3. **隔离 live/replay/test。** manifest 增加 purpose；历史回放和 TEST_ONLY 制品不得进入 live active generation。
4. **处理同点时冲突。** 同一最大 `as_of` 出现不同 hash 时阻断，不使用文件 mtime 决胜，避免并发/重放生成的不确定性。
5. **补齐领取后的失败恢复。** claimed batch 在制品打开、验证或构建失败时进入 retry；进程崩溃依靠租约到期重领，不能永久 LEASED。
6. **补齐 CAS 后崩溃边界。** active 已成功切换但 dirty 尚未 complete 时允许幂等重算并核对包含关系，不回滚有效 active，也不直接丢弃 dirty。
7. **不自动增量转全量。** active generation 缺失时明确阻断。自动全量可能扩大资源消耗并掩盖状态损坏，属于危险的“自愈”。
8. **避免部署空窗。** P0 与 P1 同批发布，或在首份 READY 制品前关闭独立维护开关；早盘、收盘、A4 不受影响。
9. **保证 SQLite 制品可独立读取。** 发布前关闭连接/checkpoint WAL，manifest 最后写入，避免只有主 DB 文件而数据仍留在 `-wal`。
10. **再次删除过度设计入口。** 新鲜度只保存在制品 manifest，不建设独立水位服务；失败由现有调度、dirty retry 和前端状态呈现，不新增常驻 retry daemon；旧快照不自动修复或自动转换。

### 15.3 第二轮结论

调整后，方案覆盖了本次 OOM 的直接根因，也覆盖输入身份、行情新鲜度、并发选择、租约、CAS、磁盘和崩溃恢复边界。新增内容都直接服务于“不会再次 OOM、不会读错数据、不会错误激活”三项目标；没有扩展到策略优化、数据源重构、自动交易、通用数据平台或自动历史清理。

## 16. 第三轮复审记录

### 16.1 复审重点

- 独立 SQLite 制品是否与现有 Feature Store 代际和 `feature_generation_members` 重复。
- 把输入写入现有库是否会破坏 run binding、active 指针或严格 CAS。
- 数据库内复制能否同时解决维护内存、增量边界和全量构建问题。
- 新方案是否引入不必要的文件、manifest、reader、历史转换器或清理服务。
- 源代际失败、并发、日期不新鲜、磁盘满和进程中断时是否仍然失败关闭。

### 16.2 新证据

生产 Feature Store 的实际行数：

| generation purpose | generation | snapshot-input members |
|---|---|---:|
| `RUN_SNAPSHOT` | `run-7c59ed081f06-...` | 0 |
| `LIVE_FULL` | `feature-full-614bf4...` | 4,017 |
| `LIVE_FULL` | `feature-full-01c25...` | 4,017 |
| `LIVE_FULL` | `feature-full-8b2e4...` | 4,017 |

这证明现有 Feature Store 已经具备保存维护输入的表、索引、内容哈希和代际生命周期；当前问题是最新 live snapshot 没有物化维护输入，而不是缺少新的存储介质。

### 16.3 发现与调整

1. **删除独立制品数据库和 manifest。** 原方案会复制现有 `feature_generation_members`、validation manifest 和 SQLite 生命周期，属于过度设计。
2. **复用现有 Feature Store，新增 `LIVE_SOURCE` purpose。** 该代际不可激活，只作为 live maintenance 的已验证输入源；历史回放、TEST_ONLY 和普通 run generation 均被排除。
3. **维护阶段改用 SQL 复制。** 全量通过 generation table `INSERT ... SELECT`；增量只替换 claimed/expanded entities。维护进程不再解析或反序列化股票 payload。
4. **保留分批序列化，但只发生一次。** 冻结快照的 `data` 尚在内存时按 25～200 只写 `LIVE_SOURCE`，避免从 261.5MiB JSON 反向构建。
5. **删除通用历史转换器。** 生产旧 JSON 不自动转换；只有新鲜 live 快照产生合格源代际。确需历史转换时另行评估，避免把 OOM 修复扩展成迁移平台。
6. **保留第二轮边界合同。** `market_trade_date`、逐只停牌解释、同点时 hash 冲突、dirty 版本包含关系、租约恢复和严格 CAS 都移入源代际 validation。
7. **降低维护内存预算。** 数据库内复制不应达到原 sidecar reader 的内存目标；全量上限收紧到 700MiB，100 dirty 增量收紧到 350MiB。
8. **明确存储增长边界。** `LIVE_SOURCE` 不绑定历史 run，不写 LLM/公告全文；仅保留最新、previous 和被构建引用的源代际。删除动作仍使用现有引用计划，首版不增加自动清理守护程序。

### 16.4 过度设计与无用项复核

最终方案没有新增数据库文件、Redis、队列、对象存储、Parquet、常驻 reader/retry 服务或通用水位平台。新增内容限定为一个 Feature Store purpose、批次物化函数、源代际选择/验证函数、按实体 SQL 复制和现有前端状态投影，均直接对应已证实的缺口。

### 16.5 边界复核结论

- 空 dirty 不触碰大 payload；周日更早返回。
- 没有合格 `LIVE_SOURCE` 时阻断，不读取旧 JSON，也不回退历史代际。
- `LIVE_SOURCE` 永远不可激活；LIVE target 仍经 staging、validation、seal 和严格 CAS。
- 源代际写入或 SQL 复制失败不会改变 active；进程中断由 staging 状态和 dirty 租约恢复。
- 日线旧、purpose 错、同 as_of 多 hash、dirty 版本未包含、active 缺失均有独立失败语义。
- 数据库存储压力通过现有引用治理评估，不在本轮引入自动删除。

### 16.6 第三轮仍需实测的风险

1. **成本转移风险。** `LIVE_SOURCE` 在快照 `data` 尚驻留内存时生成，理论上内存有界，但必须证明没有把 03:30 OOM 转移到收盘/盘前快照阶段。
2. **SQLite 写锁风险。** 源代际物化和维护 SQL copy 都会产生写锁；必须在真实库副本上验证批次事务不会阻塞 Node 状态查询或研究结果持久化。
3. **磁盘增长风险。** 当前库已约 1GiB。即使不自动删除，也必须先得到单个 `LIVE_SOURCE` 的实际大小和 7/14 日增长预测；水位不安全时不得上线每日物化。
4. **当前数据不可直接复用。** 现有最新快照日线停在 2026-08-27，不能为了验证新架构转成 READY 源代际；必须先生成行情新鲜度合格的新快照。
5. **数据库内复制性能未知。** `INSERT ... SELECT` 明显降低 Python RSS，但真实耗时、WAL/rollback 大小和磁盘写放大仍需 VM 实测，不能用单元测试推定生产通过。

这些风险不需要新组件解决，但都是实施验收的 NO-GO 条件。任一项超预算时保持维护开关关闭，A1-A4 继续按现有独立调度运行。

### 16.7 第三轮结论

`LIVE_SOURCE + 数据库内代际复制` 比独立 SQLite 制品更小、更符合现有架构，并消除了重复存储合同。该结论替代前两轮关于独立制品文件的实现选择，前两轮保留的日期、失败恢复和验收边界继续有效。方案仍有明确的 VM 实测门槛，但不需要再增加组件来覆盖这些风险。

## 17. 本地实施与复审记录

> 实施日期：2026-08-31  
> 当前边界：本地代码与自动化测试已经完成；尚未提交、推送或部署虚拟机，生产 RSS、WAL、磁盘增长和连续 5 个维护窗口仍待实机验收。

### 17.1 已实施内容

1. 新增不可激活的 `LIVE_SOURCE` purpose、生命周期迁移、严格 READY 选择和同点时冲突阻断。
2. 快照冻结后直接使用现有 `data` 按 25～200 只分批写入源代际；scheduled maintenance 不再调用历史 JSON loader。
3. 增量先领取 dirty；空队列返回 `NOOP_NO_DIRTY`，有任务才选择源代际。领取后的缺源、版本不匹配和 active 缺失均返回 retry。
4. 全量和增量使用 SQLite 内复制；全量按股票 keyset 分批提交，Python 不反序列化 payload。
5. 源版本/依赖水位按股票绑定，不使用全局版本集合，避免相同版本号跨股票误通过。
6. 最新源不合格时阻断，不允许回退较旧 READY 源冒充最新输入。
7. 增加 host-local 互斥锁、独立维护开关、磁盘水位前置、RSS/磁盘进度指标和稳定原因码。
8. Node 对信号终止记录 `SIGNAL_KILLED:<signal>`；前端展示源代际、NOOP、资源失败和维护阶段。
9. 引用治理保护最新两个 READY 源以及在建 target 引用的源；7/14 日增长只做评估，不自动删除。

### 17.2 实施后第一遍复审

- 删除了“概念/主营必须覆盖全部 G0”的错误假设。概念成员和主营事实允许合法部分覆盖，但 source/target 四类维护表行数必须精确一致；股票成员和基本面仍要求全覆盖。
- 将周六磁盘水位检查移动到 Feature Store 初始化之前；水位不足不创建或迁移数据库。
- 将全量复制由单个大事务改成固定股票批次，避免把 Python OOM 转移成巨型 rollback/WAL 事务。
- 保留旧 JSON loader 仅供显式历史校验/回放，scheduled 入口没有兼容回退。

### 17.3 实施后第二遍复审

- 修复“旧 READY 源掩盖更新但失败的源”：严格按最新 `as_of` 检查，最新源 STAGING、FAILED、行情陈旧或身份冲突都会阻断。
- 修复“全局 source_version 同名误匹配”：兼容性改为 `entity_id → source_versions/dependency_hashes`，不能用另一只股票的同名版本证明当前股票已更新。
- 增加进程互斥，避免宝塔、Node 调度和人工 CLI 同时复制大代际；锁文件损坏时只在保守陈旧窗口后回收，不能删除刚创建但尚未写完的锁。
- 复核没有加入 Redis、消息队列、Parquet、第二数据库、自动历史转换器、自动清理守护进程或策略阈值变化。

### 17.4 当前验证结果与未完成项

- Python 全量测试通过；新增 4,017 股票结构等价测试通过。
- Node 测试、TypeScript 类型检查和生产构建通过。
- `git diff --check` 无补丁格式错误。
- 尚未完成 Linux 4GB 虚拟机真实 payload 的峰值 RSS、SQLite WAL/锁等待、磁盘 7/14 日增长、进程强杀恢复和连续 5 个窗口验收；这些仍是上线维护开关前的 NO-GO 条件。
