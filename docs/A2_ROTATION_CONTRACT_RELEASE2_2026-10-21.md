# Release-2：A2 轮动理由契约（0505 B）

范围仅 `SELECTED_BOARD:*` 的原始 `rotation_reviews[].reason_codes`：schema 提前要求 1–4 个非空、不重复字符串。轮动域、选股、权限和后验阈值未放宽，服务端不代造理由。最后一条 user 合同与逐字段反馈使用同一冻结方向投影。

实际旧后验没有正向理由码枚举：其真实域为非空字符串，同时拒绝“整份理由都来自既有 11 个入场门代码”的情况。`ENTRY_GATE_ONLY_REASON_CODES` 只是从旧后验原样提取的共享排除集，不是新加的正向白名单。本实现如实给出 `positive_enum: null`，正枚举若要新增需 Claude 明确裁决。

## 原件与反例

- 既有隔离根：`/var/tmp/team-v2-core-20261021-avh2z6l2/whole-chain-3c563dd-0498`。
- 最后真实原答 call：`A2:CLOSE_REVIEW:7635cf4b137179b879fa62d5a4115c2ecfaaddbfa29578de153f766bdd796167`。
- 原始 output JSON SHA：`e31feddb5fa22e251e66bc4e92a3d445304c76c59b934a48320e64773d9398e9`。原始 `validated=1` 不改写。
- 本地小原件保存 exact `output_json` 字符串及 SHA；另保存原后验输出的 `focus_pool/rotation_reviews` 精确投影及父 body SHA，不搬大审计。
- 新 schema 对原答提前拒绝：`A2_ROTATION_REASON_CODES_INVALID:SELECTED_BOARD:AI_COMPUTE_INFRASTRUCTURE`，反馈精确指向 `rotation_reviews[0].reason_codes`，actual 为 `[]`。
- 旧后验同方向出现 `REASON_CODES_REQUIRED` 后会跳过该行，因未加入 explained 集合连带出现 `FOCUS_COVERAGE_MISSING`。只在显式受控模型纠正分支将该行理由改成既有测试示例 `LOW_CONVICTION`，两错一同消失。此为受控正例，不是新模型答复，也不改原件。
- 真正缺行仍给缺失行位置、精确方向、成员和现有理由约束；短解释反馈使用旧后验的 10 字符要求，不改变阈值。

验证为零模型、零行情、零生产写入。最终新 HEAD 仍须 VM 自身全量、同 HEAD 40 项及授权 12 次研究段门，不能用本地单测替代。
