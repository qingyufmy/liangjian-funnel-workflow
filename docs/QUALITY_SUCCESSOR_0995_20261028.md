# 0995 最小后继 H

原 H `8e8ae8eaa0c206900627c73b776be545e4b9cb74` 的真实质量为三例通过、三例失败、A5发送前阻断；原七次HTTP、原回复、validated和失败审计均保留，不冒新版已通过。

按 Claude 2026-10-28 16:25 的0995裁定：

- A6提示词文件原本就与6ea相同。本次撤回 risk_session 新增的 `CLAUDE_0937_LJ099` runtime输出指令，恢复6ea请求提示路径。服务端容量白名单、独立effective证明及原source-gap指令保留。
- A4只删除形状合法的catalog外技术引用，审计保留完整原文/my_read与 `UNKNOWN_REFERENCE_ITEM_DROPPED`。已知项仍按原权威status回填；缺必需UNMET、身份、重复项、动作和结构错误仍拒绝，原动作不变。
- PM仅将自由文本note确定性截到40字符，`NOTE_TRUNCATED`审计保留完整原文和截后文本。原reply不变，plan_ref、decision、size_tier、reason_code、数值、资格、分配等仍按原校验。
- A5计长域由0995明确为全部messages.content之和，不只是facts。已有服务端250000门保持；OPS实测适配器按该域执行，同时记录完整messages/body/wire长度，不能覆盖旧binding或重置七次累计。

新增回放使用此次A4_1406和PM真实失败回复、完整目录及原输入域；fixture同时保留原output hash和来源RESULT SHA。受影响117项本机检查通过（run083050276226），含真实客户端流式入口、原回复不改写、动作不升级、必需项/身份/结构错误负例及原valid A6六主六备合同。首轮run082953911010的七个测试失败保留：真实14:06源没有UNMET而负夹具未造出缺失必需项、PM fixture先构造闭包导致patch无效，以及五条旧unknown-item必拒断言与0995新白名单冲突。修夹具和明确授权合同断言，未删除身份/结构/必需项门。

此文记录代码变更和局部回放，不代表后继H full/runtime、真实质量、FINAL或部署通过。后继H必须完成受影响与full三项；新实测最多六次、今日总计最多十三次，无自动重试循环。Supervisor/lifecycle/deploy原字节及其他门保持。
