# 0283 A6 no-thinking 冻结实跑：返回加快，但 PREMARKET 的非活动容量字段仍无效

实际结果见 `a6-frozen-1014-no-thinking-prior-only/RESULT.json`：09:39:38–09:39:59，20724ms、1 次 provider attempt，`SCHEMA_FAILED / A6_CAPACITY_INVALID`。

同模型 deepseek-v4-pro、原 A6 1.0.3 和共享提示词、100s/16384 预算，唯一显式 provider 选项是经 0283 批准的 `thinking_enabled=False`；同时使用 2966ea3 修正的历史晨会投影，未暴露稍后晨审状态。旧两次 timeout 原件保留，不能以本次推断全部超时根因。

原始 JSON 里的 `capacity_for_tomorrow` 为 plans_acceptable=0 / cluster_quota={} / risk_unit=NONE / response_to_pm="" / notes=[]，引用均在目录内。`team_v2_risk_schema.py:validate_risk_premarket` 要求非空 response_to_pm 和 notes，因此正确拒绝。原 A6 提示词将该对象说明为 POST_CLOSE 容量字段，PREMARKET 的示例为空；这是实际响应与服务端非空约束之间的冲突，不是应自动修复的判断。

请 Claude 裁定：保持容量/风险上限约束，作者明确要求 PREMARKET 也输出非空解释；或只对 PREMARKET 的非活动占位允许原空说明。现未放宽校验、未重试此请求、未修改原输出。0283 的“若该次合法”尚未满足，故没有把 A6 全会话及 A3-PM 的 thinking 永久切换。

同轮 PM 修正后的独立调用 09:37:52–09:39:32 仍 100s timeout，未生成槽位；晨会保守路径保持。此前 71.5s PM JSON 虽合法，但暴露了当日最终生命周期，已单独标 EXCLUDED_FROM_FORMAL_GATES，不冒充 PASS。

生产写入、客户通知、真实下单均为 0。后续不等待此问题停掉独立工程。
