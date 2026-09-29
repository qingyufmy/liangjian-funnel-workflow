# A2 资讯证据旁路：首轮实施与验收

## 范围

实现既有免费资讯链路的旁路证据切片，不新增付费采集、不复制 AIHOT 代码、不修改板块排名、选股阈值和执行策略。本次没有提交、推送或部署，保留工作树中原有 A1 内存和 A3 修复。

## 已落地

- `facts/hithink.py` 的公共事实投影保留接收时间；`market_aggregates.py` 的资讯聚合保留接收/抓取时间、内容哈希及可疑文本标记，并记录数量截断。
- 新增 `pipeline/a2_news_context.py`：只使用截止时刻前已接收的有效证据；未知时刻、未来数据、时序错误、旧闻、缺引用和可疑文本分别计数。相同标题、摘要和当地发布日期保守归并，内容变更保留为独立记录；不声称完成语义事件聚类或撤回关系识别。
- 原始出处逐条保留，转载数量不是独立确认数量。股票关联只认既有源的股票字段，并明确标记为线索，不声称已验证主营受益。
- 逐候选保存现有板块、排名、量化结论及理由；无新闻不淘汰，有新闻不覆盖原拒绝，也不纳入池外股票。
- `ResearchPipeline._persist_gate` 在 A2 本地角色阶段写入 `outputs/research/a2_news_shadow/<run>/<lane>.json`；报告有输入哈希和自身哈希。不进入现有 LLM 输入。旁路写盘失败记录明确日志，不阻断 A2。
- 工作流合并优先使用事实 ID，避免相同内容哈希吞掉不同事实引用，累计被截断数量。
- 新增 `scripts/audit_a2_news_shadow.py`，离线读冻结文件，无网络、模型和生产库访问，拒绝覆盖已有输出。可传冻结量化决策数组生成逐股票对照。

## 需求—测试—证据

| 需求 | 测试/证据 |
|---|---|
| 不把转载当多份确认，修订不吞并 | `test_reposts_do_not_mean_independent_confirmation_and_revision_stays_separate` |
| 防止事后补到的数据进入历史判断 | `test_unusable_evidence_has_explicit_reason` |
| 新闻不更改量化结果和资格 | `test_news_does_not_change_gate_or_admit_outside_symbols` |
| 数量守恒和显式截断 | `test_count_reconciliation_and_bounded_projection` |
| 保留接收时间，原模型投影不增加字段 | `test_heat_preserves_clocks_but_model_projection_stays_unchanged` |
| 接入真实 A2 落盘入口 | `test_real_gate_persistence_writes_sidecar_without_feature_store` |
| 旁路写盘失败隔离 | `test_sidecar_disk_failure_does_not_block_a2` |
| 合并不丢独立引用 | `test_workflow_merge_retains_separate_fact_references_and_omissions` |

## 实际验证

Python：`D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe`，`PYTHONPATH=src`。

1. `python -m pytest tests/test_a2_news_context.py tests/test_market_aggregates.py tests/test_open_news_fact_normalization.py tests/test_pipeline_research.py tests/test_a2_role_logic.py tests/test_a2_data_sufficiency.py -o addopts='' -q --tb=short`
   首次 174 项通过，退出 0；随后增加摘要屏蔽反例，最终结果见迭代状态。
2. `python scripts/audit_a2_news_shadow.py --snapshot artifacts/readonly-snapshot-20260928.json --output artifacts/a2-news-shadow-20260929-frozen-0928.json`
   退出 0。真实旧快照输入 120 条，120 条均首要缺项为 `EVIDENCE_REFERENCE_MISSING`，合格事件 0。未重采、未补造字段、未改写快照。本次未传量化决策，不能据此声称完成逐股票对照或全市场覆盖验收。

旧快照缺项是原聚合投影的信息损失，不能误解为当日没有资讯。新投影修复只能改善后续生成的证据，不能还原过去接收时间。

## 未完成与下一步

### 第四轮：分页断点与修订账本

新增 `data/news_journal.py`，只用于隔离旁路 SQLite，不修改生产事实库。原始页、观察到的内容版本和游标在同一事务中写入。抓取失败、字段缺失、没有可靠后续游标、游标循环和页数预算耗尽均明确为未完成；失败页不推进游标。并发使用旧游标时冲突退出。同一页重放幂等，跨次恢复的游标循环也会阻断。

同一来源同一资料 ID 的标题/摘要/发布时间/URL/股票关联变化产生不同修订哈希。按实际接收时间查询可还原当时版本；A→B→A 不丢最后一次变化。同一接收时刻出现冲突版本明确标记；资料不在下一页中不能认定被撤回。这里是资料修订，不是已经完成“合同取消”等事件关系语义识别。

探针接入隔离账本并输出 `page-coverage.json`。对已冻结的真实两源结果重放至 `artifacts/a2-news-journal-20260929/`，保持原始截止时间，仍得到 30 条有效资讯。现有源适配器没有输出经过验证的下一页/结束契约，因此真实样本覆盖状态明确为 `complete=false / LIVE_CURSOR_CONTRACT_NOT_VERIFIED`，没有用短页或 HTTP 成功伪装全量完成。通用分页器通过注入适配器的反例测试，不等于两个公网端点的历史遍历已验收。

实际命令：

```
python -m pytest tests/test_news_journal.py tests/test_a2_news_receipts.py tests/test_a2_news_review.py tests/test_a2_news_context.py tests/test_open_news.py tests/test_open_news_fact_normalization.py tests/test_market_aggregates.py tests/test_pipeline_research.py -o addopts='' -q --tb=short
python -m pytest tests/test_news_journal.py -o addopts='' -q --tb=short
python scripts/probe_a2_news_lineage.py --frozen-dir artifacts/a2-news-live-20260929 --output-dir artifacts/a2-news-journal-20260929
git diff --check
```

退出码均 0；选定回归 191 项通过，补跨次循环及其幂等反例后 8 项账本测试通过。没有新增网络请求、模型请求或生产部署。下一步仍需核验供应方实际分页游标语义与同秒边界，再把真实分页适配接到此账本；不能猜游标后宣称历史覆盖完成。

### 第三轮：免费源实测、映射反例与持久回执

2026-09-29 16:24:11 +08:00，从本地只读访问财联社和东方财富免费源，各限 20 条、单次尝试、10 秒超时。两源均 OK，各 20 条，规范化去重后 30 条事实；进入旁路的 30 条均保留引用与时间，排除数为 0。不是全日或全市场完整率证明。

产物在 `artifacts/a2-news-live-20260929/`，含源适配结果、事实清单、旁路报告和模型输入；不保存凭证，不写生产。新增 `scripts/probe_a2_news_lineage.py` 可采集，也可通过 `--frozen-dir` 离线重放，输出目录必须为新目录。

实测反例：金螳螂装修合同摘要提到客户华泰证券，命中“证券”词。不能将它解释为证券行业催化。已新增 `mention_scope` 区分标题提及和摘要背景，所有命中继续明确 `theme_catalyst_confirmed=false`；尚未实现可靠的主体/客户语义识别，因此不自动删除背景词，也不将其晋升正式方向。

修复后对同一事实文件离线重放至 `artifacts/a2-news-live-20260929-replayed/`，仍为 30 条有效事件，其中 7 条有题材命中：6 条标题提及、1 条摘要背景。采集截止时间未变化，未再次访问网络；这不是映射精确率或召回率统计。

新增 `pipeline/a2_news_receipts.py`。复用现有模型客户端与原子 JSON 写盘工具，独立旁路目录通过独占目录创建认领逻辑请求，身份绑定供应方哈希、模型、输入、提示词与调用限制。成功响应先落盘再校验；校验失败仍保留输出；已保存响应复用；超时/崩溃留下占用，不自动重发。原 `review_with_client` 也必须提供回执目录，不能绕过回执。

边界：当前是本地逻辑请求去重，不是供应商计费恰好一次保证；现有客户端内部重试仍按其自身规则。没有新增每日费用账本或自动恢复未知请求，故不启用自然调度，不执行真实付费调用。响应结构和引用通过仍不保证语义真实。

实际命令与结果：

```
python scripts/probe_a2_news_lineage.py --output-dir artifacts/a2-news-live-20260929
python scripts/probe_a2_news_lineage.py --frozen-dir artifacts/a2-news-live-20260929 --output-dir artifacts/a2-news-live-20260929-replayed
python -m pytest tests/test_a2_news_receipts.py tests/test_a2_news_review.py tests/test_a2_news_context.py tests/test_open_news_fact_normalization.py tests/test_market_aggregates.py tests/test_pipeline_research.py tests/test_a2_role_logic.py tests/test_a2_data_sufficiency.py -o addopts='' -q --tb=short
python -m pytest tests/test_a2_news_receipts.py tests/test_a2_news_review.py tests/test_a2_news_context.py -o addopts='' -q --tb=short
git diff --check
```

全部退出 0。大范围回归 192 项通过；收紧供应方身份及统一回执入口后再跑 31 项针对性测试通过。新增反例覆盖客户名误关联、超时不重发、失败响应保留、成功回执复用及损坏回执拒绝。

下一步：从多时段冻结样本建立人工标注集，评估主题召回与误关联；补来源完整遍历和事件修订链。真实模型旁路验证仍需单独授权调用及预算，不以现有单页样本或合成模型响应代替验收。生产未部署，正式排名和策略不变。

### 第二轮推进：题材关联与模型审核入口

新增 `pipeline/a2_news_review.py`：复用 `rotation_themes_v1.yaml` 的校验、有效期和主题语义；报告冻结完整映射配置及哈希。关键词只标记文字相关，不标记受益，英文缩写采用词边界，避免 AI 命中 RAID。

关联原 `MAIN_BUSINESS_EVIDENCE` 中同股票、包含命中词且带引用/内容哈希/发布时间/接收时间的片段。缺任何关键字段则保留缺口，不从新闻推导主营；即使存在文字交集，也不声称已证实经济收益。

新增有界模型投影：最多 12 个事件、系统提示与输入合计不超过 18000 字符，显式记录省略数；绑定事件、报告和输入哈希。A2 自然调用路径只写 `<lane>.review-input.json`，不自动调用模型。只读审计脚本的新版输出为 `{report, review_input}`，旧产物未覆盖。

新增严格响应校验：事件逐个覆盖、不得新增/重复/漏项、引用必须属于本事件、不得新增 action 等字段、不确定性不能为空。`citation_contract_valid` 与 `semantic_truth_verified` 分开，结构通过不是内容真实。

可注入的 `review_with_client` 复用现有客户端接口，固定 deepseek-v4-pro、90 秒、3000 输出 token；缺少显式调用许可直接拒绝。本轮仅使用 FakeClient，不进行真实模型请求。该入口尚未接入持久化调用回执、费用预算及不确定结果恢复，因此禁止接入自动调度；下阶段应复用项目已有回执设施后再开展授权模型验证。

新增离线工具：`scripts/validate_a2_news_review.py --input <review-input.json> --response <response.json> --output <new-audit.json>`，不调用网络、不覆盖原响应。

实际执行：

```
python -m pytest tests/test_a2_news_review.py tests/test_a2_news_context.py tests/test_open_news_fact_normalization.py tests/test_market_aggregates.py tests/test_pipeline_research.py tests/test_a2_role_logic.py tests/test_a2_data_sufficiency.py -o addopts='' -q --tb=short
python scripts/audit_a2_news_shadow.py --snapshot artifacts/readonly-snapshot-20260928.json --output artifacts/a2-news-shadow-20260929-mapped-0928.json
git diff --check
```

退出码均为 0；187 项测试通过（14.41 秒）。新增端到端测试使用实际 OpenNewsItem/FetchResult 合成样本，经规范化、公共事实投影、新闻聚合、题材关联、模型输入及引用校验，不是实网采集或真实模型验收。真实旧快照仍是 120 条引用缺项、有效事件 0，不能把这个结果视为业务增益通过。

当前是可运行的首轮证据旁路，不是完整资讯情绪系统。尚未完成：分页续点/资料修订持久库、市场新闻到主营与题材的可靠映射、跨文语义事件及否认/撤回关联、传播速度、模型带引用分析、前端对照表、影子多日增益和生产验收。

下一步先获取按新投影生成的资讯样本，验证从免费源解析到 A2 旁路的完整血缘，再建立跨行业人工标注集。只有在证据完整且误关联率可接受后，才将有界事件摘要接入模型旁路分析；不直接影响前五板块和正式资格。旧快照已缺失的字段不补写。

CODE：针对性测试通过。REPLAY：真实旧快照缺项审计完成，不是情绪增益通过。OPERATIONS：未部署。STRATEGY：门槛不变，未证明收益或漏选改善。
