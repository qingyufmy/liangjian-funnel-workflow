# 0271 方法反馈增量

实际验证：2026-10-17 07:35–07:36 Asia/Shanghai。仅本机实现及隔离 HTTP mock，无真实模型、VM 操作或生产写入。

按 `0271-reply`：每个角色／会话／判断类别／证据类别集合桶，同时满足 10 个成熟交易日、15 个成熟判断、5 只不同股票，才输出方法统计。T+1 与 T+3 原始价格、标签可用时刻、原响应及目录哈希仍须全部成立，不能向其他桶借样本。输出移除平均收益，只保留匿名命中率、样本数、日期范围、证明 hash 和方法分类。

A5 原文升级为 Claude 2.0.4，精确字节校验：

- `agent_5_daily_reviewer_v2.txt` 7383B，SHA `a64a8b367f34fbd1854063b5d36cc947eb62d81d13a7ba142a045d37c6dd4ad7`。
- README 6681B，SHA `39368196be3ec7360ae91eea0ced72a417522c0271d86ffaa305c6783f19ea9d`。

方法条目固定 `method_id/stance/note/evidence_ids/opposing_refs`。`LOOSEN` 或格式非法走原 A5 降级；域外 ID 仍保留判断原件但不回灌。只有成熟 ID、方法证明引用以及 `TIGHTEN/KEEP` 才回灌；`INSUFFICIENT` 不注入，自由文本 `note` 永不复制。相同 ID 的冲突立场不按数组顺序选择。

反例包括：9 日／14 样本／4 股票／单股票多样本、跨判断类别借样本、迟到标签、篡改证明、未知 ID、INSUFFICIENT、LOOSEN，以及实际 HTTP mock → A5 记账 → 研究读取。所有数据为显式单测夹具，不能充真实成熟样本。

增量初轮 69/0；加跨桶反例与非法类型边界后，最终关联 160/0。两轮重叠不相加。最终回执：

`artifacts/team-v2-20261016/20261017T073542010865-feedback-0271-final-related-receipt.json`

SHA `33c726ea09182d4c14fc37675212080cf6c889f0bc7d0de3168c17e23e9c18b4`。

CODE：本增量通过；REPLAY：正式六日／五日未通过；OPERATIONS：未部署；STRATEGY：无成熟真实样本、收益未证。0270 已由 0271 解决，不再等待该问题。继续完整回放输入适配及真实 Workflow 组合接线。
