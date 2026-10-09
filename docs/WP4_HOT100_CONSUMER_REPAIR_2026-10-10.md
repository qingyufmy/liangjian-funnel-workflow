# WP4 Hot100 消费者严验修复

日期：2026-10-10。工作树 `D:/dev_A股/liangjian_wp5_hotfix_20261009`，修改起点 HEAD `ae2d098b5eeb3a369e23d080fd006b099d7962fc`。本切片已冻结；未提交、未运行全量。

## 结论与边界

Hot100 缺源、残留、部分、旧日、未来时点或未验证 hash 不能生成真实榜外标签。只有当日完整 100 个唯一证券、顺序 rank 1–100、明确版本/来源/PIT、带时区观察时间不晚于决策时间、原 records canonical hash 一致的全榜，才可证明目标未命中。真实榜外也不证明股票整体弱势。

新增纯观察函数 `data.hot100_observation.observe_hot100(value, *, decision_as_of)`，不读取缓存、Settings、RuntimeStore、网络或 DB。返回 COMPLETE / UNAVAILABLE / INVALID_SOURCE / DATA_LIMITED；非 COMPLETE 的 records 一律为空，membership 一律 UNKNOWN。错误时不填榜、不改日期、不降级成合法情绪入口。hash 是 records canonical JSON SHA256，不是 envelope hash，也不认证第三方来源真实性。

完整读取 canonical outbox `0021-reply.md`（SHA256 `4318eca85dae783362a18b17b4c0e9a1045ea4157e51473504003b09b368c24c`）及旧 `WP4_HOT100_ABSENCE_CONSUMER_AUDIT.md`。旧 collector `data/eastmoney_hot.py` 的 canonical records 算法保持不变；其 `_load_cache` 绑定文件读取，不能作为无缓存纯消费者验证入口。本模块按相同 hash 合同严验本次提供的冻结对象，没有调用 collector。

## 实际消费者链与七项反例

| 旧审计项 | 本轮实际验证路径与结果 |
|---|---|
| 1 竞价缺榜仍可独立趋势 | `project_auction_delta` → `screen_a2`，False 残留被清空、无 NOT_IN、独立趋势仍合格；boards/quotes 缺失仍抛原 WorkflowError。未修改 auction projector。 |
| 2 完整榜外与缺源区分 | `screen_a2` 正常构造，完整同日榜缺目标保留原 daily member NOT_IN；False 空/残留、None/无 key 不产生该标签，无情绪资格。独立趋势 score/factors/资格保持相同。 |
| 3 True 伪完整防御 | 空、99 行、重复证券/名次、错 count、旧日、未来、naive time、缺/错 hash、假投影声明均不可完整或证明 NOT_IN。 |
| 4 leader 不污染与硬门 | 正式纯 `evaluate_a4_plan` 对相同 leader 合同加 False 空热榜，输出不变；明确断梯仍不得 BUY_SIGNAL。leader 本来不读热榜，没有新增热榜依赖。 |
| 5 健康投影真实可达 | gate → 正常 FrozenInputSnapshot → `_with_a2_bottleneck_context` → `_project_prompt_value` compact；独立健康字段不受 3 reason 截断。实际 OUTSIDE_ROTATION、gate item、canonical A2 同样保留 server 健康，模型伪造 available=True 被原 context 覆盖；不改变分区、资格或门槛。 |
| 6 残留与离线范围归因 | `_with_daily_emotion_overlay`、prompt 投影、`audit_projected_batch` 均调用同一严验观察。False 残留/99/旧日/错 hash/缺来源日期不产生有效 HOT100 source_set；完整100正向对照可保留来源。离线脚本用已存在的显式 aware snapshot.as_of，缺来源时钟是 DATA_LIMITED；缺整个 audit 决策时间仍按原总体输入合同拒绝，不能借残留恢复。未改 prefilter/scope 语义。 |
| 7 提示词约束 | 三个真实 A2 prompt 明确“热榜不可用≠榜外≠热度低”、读取独立 source 健康、禁止热度负推断，趋势独立且情绪硬门不松。只验证实际模板文字，不宣称模型实际输出已通过。 |

TOP10_PLUS_BATCH_MATCHES 只从本函数已经验证的完整原榜投影；11 行 top10+batch 示例仍可携带 full_snapshot_validated=True 与原100行 hash，但不能把该投影作为下一次完整源输入。输入自行声称 full_snapshot_validated=True 没有效力。source_health 白名单不投影任意来源文本/URL。旧冻结回放没有版本/hash/时钟只能显式缺证，未为历史源补证。

现有 tiny fixtures 过去声明 record_count=100 实际1/2行，已补成真实100、原fixture业务日期与记录 hash，再沿原业务断言；没有放松策略断言。A1 disposition 与 outside_active 数量改100/99是新增真实填充行的计数，不是生产改数。故意 True 空榜的旧趋势独立测试保持空榜，仍通过。修正仅四个已授权 fixture 文件。

## 测试证据

Python：`D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe`，纯本地 fixture；未构造 Settings、RuntimeStore、WorkflowApplication，未调用网络、DB、模型、通知或生产。

| 回执 | exit | 结果与解释 |
|---|---:|---|
| `hot100-consumer-before.xml` | 1 | 28 failed / 3 passed，先于消费者源码实现。首次完整榜外正向测试错误检查 global reason；实际标签在 daily_a1_member_reason_codes，保留旧失败并纠正测试目标，不能称该项是旧业务缺陷。 |
| `hot100-consumer-v1.xml` | 1 | 1 failed / 30 passed，同一正向断言位置问题。 |
| `hot100-consumer-v2.xml` | 1 | 1 failed / 38 passed，补真实旧fixtures后仍为上述测试目标问题。 |
| `hot100-consumer-v3.xml` | 0 | 39 passed，按真实 daily member reason 验证。 |
| `hot100-consumer-context-before.xml` | 1 | 7 failed / 4 passed / 28 deselected；真实上下文丢健康2项、离线未验来源5项；先反例后最小字段拷贝/同函数严验。 |
| `hot100-consumer-final.xml` | 0 | 47 passed。 |
| `hot100-consumer-final-v2.xml` | 0 | 最终49 passed，1.23s；39项新模块测试（参数展开后41）+8个已有纯fixture相关用例。 |

最终实际命令（XML 路径相对工作树）：

```powershell
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -m pytest tests/test_wp4_hot100_consumers.py tests/test_deterministic_pipeline_v2.py::test_a2_dual_core_pool_keeps_hot100_emotion_and_selected_board_trend_together tests/test_deterministic_pipeline_v2.py::test_a2_daily_emotion_overlay_risk_and_trade_boundaries_stay_closed tests/test_deterministic_pipeline_v2.py::test_a2_full_market_top5_promotes_partial_trend_for_complete_llm_review tests/test_a1_registry.py::test_daily_emotion_overlay_only_annotates_active_a1_without_mutating_sealed_payload tests/test_a1_registry.py::test_daily_emotion_overlay_disposes_every_hot_row_and_rejects_hard_risk tests/test_a2_a3_evidence_handoff.py::test_overlay_uses_frozen_theme_not_popularity_label_and_preserves_monthly tests/test_a2_a3_evidence_handoff.py::test_missing_membership_does_not_infer_from_stock_name tests/test_pipeline_research.py::test_a2_hot100_projection_keeps_top10_and_batch_match_with_full_validation --junitxml=artifacts/wp5-20261010/hot100-consumer-final-v2.xml
```

源码/模板合同：已实现。局部 TEST：上述命令 exit0。原冻结 REPLAY/模型：未执行，不宣称历史模型没有负推断。OPERATIONS/自然交易日：未执行，无部署/生产验收。workflow 及资本源政策为其他操作者所有，本轮未编辑；PIT artifacts 保持冻结。

最终前后 hash 与测试命令见同目录 artifacts `hot100-consumer-manifest.json`；before 是起点 HEAD git blob 原字节 SHA256，after 是 Windows 工作树实际文件字节 SHA256，可能存在换行差异。全部证据 XML 保留，不以中间失败冒充业务通过。
