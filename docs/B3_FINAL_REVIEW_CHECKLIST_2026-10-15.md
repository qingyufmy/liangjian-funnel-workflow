# B3 最终候选核对入口

实际核对：2026-10-15 18:29 Asia/Shanghai。依据0177及Tony追加裁定。以下是已实现范围，不是最终HEAD验收或发布批准；最终版本、全量、VM差异、回退树及包SHA由封包回执填写，不沿用旧版本数字。

| 必须件 | 实现与反例入口 | 当前边界 |
| --- | --- | --- |
| 09:25竞价证据 | `runtime/opening_evidence.py`；`test_b3_opening_evidence.py`；Node `b3-opening-evidence.test.ts` | 独立job/CLI，09:25:00–30一次，只取证；原供应商恰09:25:00且按时到达才claim，不称交易所认证；不改晨审 |
| 发布ATR证据 | `runtime/publication_indicators.py`；workflow `_publish_plans`；`test_b3_publication_atr_evidence.py` | 未来发布写`shadow_inputs.atr14_evidence`，全部可用PIT缓存Wilder种子、不截短；上市全历史完整性NOT_CLAIMED；不提升缺factor状态，不改现有14条计划 |
| 簇标签 | `runtime/publication_projection.py:card_plan_binding`、`review/observability.py`；publication_projection反例 | 内部卡证据+A5标签，不降级、不改变客户正文、不加信号cap；UNKNOWN不并成共享暴露簇。信号级≤2留B4 |
| A5健康/异常分离 | `review/business_audit.py`；`test_b3_operational_semantics.py` | 原operational_evidence保留，稳定及正常等待不误报DATA_SOURCE；未知不当健康，持续UNCONFIRMED仍保留事件 |
| A5预算预警 | `review/observability.py:timing_summary`；`test_b3_a5_observability.py` | 父时长严格>50s记MONITOR_MINUTE_NEAR_BUDGET，保留既有>45s预警；不改47s全局预算/58s父预算，不发客户通知 |
| durable当轮读取 | workflow `_effective_events_for_identities`；`test_b3_durable_event_scope.py` | 空身份跳过，否则只查当轮身份最小到最大时间范围，四元组精确匹配；不是全局所有历史读取都已限域 |
| 决策标签解耦 | `evaluation/decision_capture.py`及research/common；`test_b3_outcome_capture.py` | 紧凑run/lane/stage/snapshot绑定，focus/watch/A3资格分开；不开feature_store、不补历史；77关联绿不代替最终HEAD全量 |
| V2最后接线 | `test_b3_v2_runtime_policy.py`及workflow `monitor_once` | Tony18:32明确要求现在推进后已接线：新live context和engine显式同一policy2；旧冻结缺policy仍1，保留5m/520/龙头/持仓保护。红例转绿，关联67项通过、退出0；最终HEAD/VM待封包 |
| P-STRICT/plan_ids | publication_archive/projection、原始recipe捕获与reader；publication_end_to_end反例 | 真实生产者→发布→严格reader；卡日期用DB列，plan_ids总览/分页准确；旧原件不回填 |

## 性能预期不得夸大

13:30栈所指workflow当轮durable发布读取属于本次修复。13:01栈所指`rebuild_effective_markdown`全历史重建**未改**，MonitorEngine其他既有历史读取也不因此成为“只读当前分钟”。六日1426范围0差异证明此读取变换等价，不证明明日startup时延已经改善，更不证明56.4s不会再出现。B4继续对历史Markdown重建单独取证与设计；明日以真实phase_timings验效果。

## 发布硬门

单一候选直接父4296925；HEAD绑定全量、最后改动的VM差异与实际源码SHA、前向回退树等于429、最终包SHA齐全后送审。最终候选Claude ACCEPT才push/deploy.sh；发布前无活动研究/outcomes且时段允许。原14条计划SHA `5a920936c0810a05f57bfb9729fc87ed878e828617067f69c4218d3b82dfedfb`逐行不变，不恢复研究。

CODE以最终回执为准；REPLAY沿用六日已封存V0零差异和0173敏感性；OPERATIONS待实际发布及自然运行；STRATEGY严格收益NULL，不宣称盈利或每日2–5信号已达成。
