# A6 冻结晨会两次真实超时：请求裁定推理传输选项

实际证据均在本机隔离、当前 Ark/deepseek-v4-pro 路由，max_attempts=1。生产写入/权限/订单/客户通知均为 0。

1. `a6-frozen-1014-real-current`：源码 `98f27cc`；原 10-14 A3 和原发布 5 条计划严格绑定到 10-15 规划会话；实际 09:11:38–09:13:19。输入 460,121 字符，100s 到期，`MODEL_WALL_CLOCK_TIMEOUT`，1 次 HTTP attempt，没有完整响应。
2. 排查发现晨会把包含完整 A3 审计的 payload_json 同时放进 PLANS 与证据目录。`5058558` 改为结构化规划事实：保留原几何、交接、证伪、退出规则及原行/payload SHA；原始行仍完整封存。41 项关联测试通过，原 5 行与 5 个引擎信封逐项相等。新输入 33,175 字符（约少 92.79%）；**这不是已证实的超时根因**。
3. `a6-frozen-1014-compact-real`：新源码、新输入、新调用，实际 09:15:59–09:17:39，同样 100s `MODEL_WALL_CLOCK_TIMEOUT`、1 attempt、没有完整响应。原大输入失败没有改判或覆盖。两个过程均已结束，不自动重试，不加预算。

源码进一步发现：`pipeline/model_client.py:147` 默认 `thinking_enabled=True`，Ark 分支 `:212–214` 对 A2/A4 关闭 thinking，对 A6 默认开启 thinking + reasoning_effort=low。A4 本轮正确源域的 2 计划请求 19.427s 返回合法输出；不能据此断言 thinking 是 A6 超时的唯一原因。未保存或打印隐藏推理/第三方响应正文。

建议裁定：是否批准**先仅在 A6 PREMARKET 隔离冻结干跑**显式 `thinking_enabled=False`，保留同一模型、角色正文、schema、所有限额和 100s/120s 边界？用一次新调用验证；有效后再决定是否接入 A6 晨会及 ≤20s 异常会话。当前没有改该选项，也没有动生产模型设置。

如果不接受关闭 hidden thinking，请指定替代口径；不建议扩大晨会预算或把超时当 PASS。周六进度仍记 A6 真实模型门未过，其他工作继续。

证据：

- 大输入 `PREPARED.json` SHA `e6fceebadbc3df33ffd0e5aac51929c2971bba9ad36ccb9453e3746e2fd7d7b6`。
- 大输入 `RESULT.json` SHA `2dd0e02f1a52189fc4e6a86858a0fe77d6719f40c94c11ecc98fd71c30892011`。
- 大输入 `PROVIDER_FAILURE.json` SHA `59fdcc74b3ab30b5537abfb3e744fd136733185bae3083255b9c96ff08d25e7f`。
- 紧凑输入路径 `artifacts/team-v2-20261016/a6-frozen-1014-compact-real/`；实际 hash 另附本轮 status，不用大输入回执顶替。
