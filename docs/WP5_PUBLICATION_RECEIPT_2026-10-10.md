# WP5 规范化 publication 纯回执合同

基线 `e02b6c46a43a9cadbb43cddaab7a2d62714092cb`。完整读 canonical `outbox/0009-reply.md` / `0011-reply.md`；两文件实际同内容、同 SHA256 `7f6dedf603935fa53bc12ff277209b7b1292be97dc07684f81fde1988ba8075a`，是一份 covers 0009/0011 的答复，不伪称两次独立评审。

本切片仅新增 `runtime/publication_receipt.py`、对应测试、本文。正常构造纯 builder/validator，不改 workflow/state/research/scheduler/现有 reader 或 ledger，不提交，不读 DB/缓存/Settings，不做文件读取/写入、网络、模型或通知。pipeline 的既有纯 `route_execution_permission` 仅被调用核验原 authority 变更；不重新复制策略/筛选候选。

## 什么被证明，什么没有

`LOCAL_RECEIPT_BOUND` / `evidence_complete=True` 表示输入原对象、实际给定字节、原对象成员关系、变更轨迹、已知规范化和 snapshot 配方在此纯函数中相互一致。不是 PUBLISHED execution、原文件 acquisition 认证、live provider、历史回填、整个 publication batch/合法empty 或交易资格。`implementation_status=IMPLEMENTATION_PARTIAL`、`eligibility_released=False`、`publication_execution_authenticated=False`、`acquisition_provenance_authenticated=False` 恒定。缺项、错误或未支持形状保留 DATA_LIMITED；validator 的 valid=True 只指回执相对同组原输入的完整性，DATA_LIMITED 回执也可 valid=True，但 evidence_complete=False。

规范从下一次真实调用捕获开始适用，不重构9-30或10-08历史，不凭现有 DB 字节 pin 补造 normalized/post 对象。此模块未接 publisher，也不消除已存在 reader 的 AUDIT_OVERLAY_BINDING_UNPROVEN / PLAN_AUDIT_PAYLOAD_BINDING_UNPROVEN，后续由root独占 workflow 编辑权评估接线。

## 公开接口与原对象边界

```python
BoundObject(value: Mapping, sha256: str)
AuthorityProof(value: BoundObject, source: BoundObject,
               selector: tuple[str | int, ...], origin: str)
SnapshotProof(snapshot: BoundObject, file_bytes: bytes | None,
              file_sha256: str | None, parent_snapshot_hash: str | None,
              hash_recipe: BoundObject | None, recipe_kind: str)
FieldChange(stage, field, old_present, new_present, old, new,
            basis, authority_sha256=None)
PublicationInputs(run_id, lane_id, plan_id, a3_audit, raw_plan,
    raw_plan_location, normalized_payload, submitted_payload,
    published_payload, changes, authority, snapshot_chain,
    target_trade_date, server_expires_at, published_at, publication_mode,
    minimum_trade_date=None)
PublicationReceiptBuilder().build(inputs, *, calendar) -> dict
validate_publication_receipt(receipt, inputs, *, calendar) -> dict
```

BoundObject.sha256 是 canonical JSON **对象** SHA，不是 HTTP body/文件字节 SHA。所有原对象 hash 构建前与 seal 前复算；frozen dataclass 不意味着 nested Mapping 不会被别的线程改，晚变更为 OBJECT_HASH_MISMATCH，不封完整回执。builder 不修改任何原 nested input。SnapshotProof 的字节 SHA 另算，并严格解析字节确认实际文件对象与原 envelope 对象一致（拒绝重复键/非有限值）；没有 bytes 就 SNAPSHOT_FILE_PROOF_MISSING，不能自己 serialize 缺失历史对象然后宣称是历史文件。

`a3_audit` 应是实际 A3 StageAudit.as_dict 对象：同lane、A3、可发布状态，output_hash 复算；raw_plan 的 pool/index 必须是该 output 真正成员且对象 hash 一致；plan_id 按实际 run/lane/logical-or-raw-hash 生成关系对账。submitted_payload 是 publish_plan_batch 前实际最终对象，published_payload 是该调用真实返回行 payload 原对象；必须同对象内容 hash。不能把 pre 对象复制成 post 来冒充实际 DB 返回；纯函数本身不认证调用发生，所以 execution_authenticated 始终 False。

## 实际调用处应捕获什么（尚未接线）

| 现有源码 | 必须来自该调用的捕获值 |
| --- | --- |
| `workflow.py:6561 _publish_plans` lane循环 | 同lane原A3 StageAudit对象、真实stage snapshot/原reference；不用其他run lane替代 |
| `:6614` pool/raw循环 | 原 raw 对象 + pool/index，不能从DB payload去逆推raw |
| `:6620 _plan_payload(raw)` | 返回后立即封存 normalized 原副本；后续top-level改写前记录旧/新 presence/value |
| run/TREND追加 | source_run_id、TREND_MA5 的 trend-ma5/2；basis SOURCE_RUN/TREND_RULE |
| `authority = upstream_permissions.get(symbol) or permission_context` | 实际选择的原 authority row 与冻结来源对象 hash；A2 audit output 的真实pool/index，或实际 stage snapshot data 中 A2_BOTTLENECK_CONTEXT[symbol] |
| scoped_permission四字段赋值 | 每项 old/new + A2 authority hash；复用现有 pure route_execution_permission，不依据model permission当authority |
| batch提交/`store.publish_plan_batch`返回后 | prebatch最终payload、真实post payload、真实expires_at；PUBLISHED run/lifecycle/batch成功证明属于后续publisher hook，不由本纯对象模块造证 |
| `_plan_expiry`调用参数 | 实际 published_at、slot/mode、minimum_trade_date、明确server target和实际returned expires_at，不凭 ID/created date补目标日 |
| `research/common.py _enrich_stage_snapshot`、a2/a3 context builders | 每次真实父/子snapshot、实际hash preimage、对应真实已持久化文件字节/完整SHA；现有未保存的overlay必须missing proof，不调用旧文件“最新版本”补链 |

实际 `_plan_payload` 保留 **raw，再规范化 symbol、trigger_low/high、stop_level、no_chase、confirmation_bars、action。`_publish_plans` 追加 source_run_id / trend版本，可依据 authority 改 execution_permission/research_only_reason、添加 a2_execution_permission/a2_research_only_reason。state.publish_plan_batch 只序列化原payload且同ID内容冲突拒绝，没有中间 normalized 子对象。这些已核对，无其他payload几何改写分支；morning tightening只筛选/替换父状态，不是本切片支持的CLOSE模式。

## 字段白名单、安全投影与缺口

NORMALIZE 仅上述7字段；PUBLISH 仅 source_run_id、trend_entry_rule_version、4权限字段。两次实际 diff 必须与明确 FieldChange 集合一一相等：遗漏、重复、不一致旧/新 presence/value 或不明 basis 均 gap，不自动按diff生成“完整变更证书”。不支持字段变化单列 MODIFICATION_FIELD_UNSUPPORTED，并只输出字段名 hash/值 hash；不扩展策略规则。

原raw、audit、authority、snapshot全文只用于hash/核验，不进入回执正文。必要几何有限数值、合法symbol、已知permission枚举、当前run ID及固定版本可输出old/new；research_only_reason/a2_research_only_reason字符串只输出hash，未知旧值/文本同样只hash，不泄露任意模型文本。原模型文字由调用方原文件保留且对象hash绑定，不在本模块另存大payload。

A2_AUDIT_ROW 必须原A2 audit同lane、output_hash正确、selector位于原4类pool且symbol一致、snapshot ID在已给链内。SNAPSHOT_A2_CONTEXT 则通过原snapshot对象hash与 `(data,A2_BOTTLENECK_CONTEXT,symbol)` selector 绑定：真实context row可能没有symbol属性，不猜补。每项权限变更还必须带对应authority对象hash；缺来源、hash或membership为显式gap。

## Snapshot 已知配方（不把ID集合当链）

范围明确为 RESEARCH_BASE_TO_A3_STAGE，非所有 provider/raw-FIS 传输血缘认证。root research文件的 data canonical hash须等内部 snapshot_hash；每个子文件须有真实 bytes/hash、完整原envelope对象hash、父内部hash、实际原hash recipe对象hash。再对原父→子 data 变化复算已知**结构转换**，不是重新计算策略。

| 配方 | 真实hash preimage与data变更 |
| --- | --- |
| BASE_RESEARCH_DATA | workflow base snapshotHash=canonical(data)，原标准research envelope文件 |
| STAGE_OVERLAY | common `{base_snapshot_hash,stage:A2/A3,overlay}`，parent data merge实际overlay |
| A2_BOTTLENECK_CONTEXT | a2 `{base_snapshot_hash,stage,context}`，加入该context |
| A3_CANDIDATE_CONTEXT | a3 `{base_snapshot_hash,stage,origins}`，加入原origins/sorted scope |
| A3_DETERMINISTIC_CONTEXT | a3 `{base_snapshot_hash,stage,context}`，加入该context；按现有代码只对FACTOR_SNAPSHOT的timeframes和technical_summary.timeframes保留monthly/weekly/daily，其他data不变 |

文件字节与对象、recipe内部hash、parent hash、ID的hash短后缀、as_of与实际发布时点、全部未改数据均核对。仅file SHA+parent ID或仅known字符串标签不能证明链；源字节缺失、不明recipe、recipe外额外字段或未解释data变化均 DATA_LIMITED。不支持 raw Pydantic FIS 的“无data research envelope”假转换，不能凭其64位hash冒充此stage链；原FIS与research投影来源验证是独立证据。

日期仍显式 target+真实交易日历+returned server expiry 对齐；只支持CLOSE。允许真实既有同目标日>=15:00 horizon（包括原15:30），但payload plan_expiry若有必须一致，不自动修旧日期。这里记录现有行为，不放宽旧reader的保守15:00支持范围。过期/未到下限/目标冲突/无显式target为 DATA_LIMITED，输入原日期不变。

## 测试和剩余集成门

先写反例后实现：`publication-receipt-before.xml` 是缺模块collection error/exit1，不说全部用例逐条红。首版23pass；按实际source shape补查A2 context无row.symbol、跨lane authority、重复键文件、future snapshot四项先4fail/exit1，修复后通过；`publication-receipt-original-mutation-before.xml` 另1fail/exit1证明seal前原对象变化能被旧实现漏掉，再修复。所有fixture明确是本地原对象/字节形状，并非实际生产发布/历史回放。

```powershell
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -m pytest tests/test_wp5_publication_receipt.py tests/test_wp5_auction_publication_export.py tests/test_a2_role_logic.py tests/test_runtime_calendar.py --junitxml=artifacts/wp5-20261010/publication-receipt-final-v2.xml
git diff --check
```

本纯builder对完整原对象/文件执行hash和JSON验证，真实大snapshot的CPU/峰值内存/调度预算尚未证实，也不是可抢占hard deadline。自然publisher接线前须真实来源捕获、去重/流式存储设计、原stage链持久化、batch/lifecycle成功引用、安全写盘不阻塞发布的预算/失败反例；不能直接将多个数百MB envelope按每plan重复捕获并以小fixture测试宣称生产预算通过。这里没有默认network fallback、文件补采、Settings路径或任何生产接线。由root后续唯一workflow编辑者评估这些边界。

最终 final-v2 slice：131 passed（47新增receipt + 72既有reader + 10既有A2 role + 2既有calendar）/exit0；不是新HEAD全量或自然运行发布。旧 final.xml 对应前一版，不充当最终源码回执。三个新增文件完成后冻结交回，由root单独评审/显式提交/全量测试；本子任务没有提交。
