# 同日规则 producer 与外围配置检查点

实际采集：2026-10-17 01:12 Asia/Shanghai。当前源码 HEAD `0a5035b5898e697b4a1b43a142a16fc16cfa8571`，规则 producer 源提交 `f635e2833051281ca2875b8212705e4caed02fba`。未 push/deploy。

## 两个真实接线阻断（已问，不擅自放宽）

桥接 0245：既有 HithinkClient 只读探测两个既有接口、各一页 5 行。证券目录只有 name/list_date/asset_type 等，缺严格原 PIT wrapper 必需的 board/security_type/security_status/is_st/limit_regime；价格页有 high/last/prev/volume，但 data.timestamp 是周五业务时间，不能冒充周六新行情。完整请求结构/原 HTTP bytes 在 `artifacts/team-v2-20261016/20261017T005817424196-identity-contract-probe.json` 及同 prefix 的 -0.bin/-1.bin。未进桥接原件或凭证。正式身份合同待 Claude 裁定，不能以“无 ST 名称且有成交”填 NORMAL。

桥接 0246：原严格 `_day_clock` 的 PLANNED_DUE=10-16，周一 10-19 请求前就返回 RULE_REVALIDATION_PLAN_WINDOW_EXCEEDED。保留旧 reviewed-through、旧日期常量和三个正式 URL/raw pins，询问有限的同日复核计划续期入口；未获答复前，周一规则不声称就绪。

## 完成的独立工程

1. `team_v2_rule_source.prepare_daily_rule_source`：08:33 引擎晨会内，调用既有三份正式正文预检，10 秒预算；仅写独立 Team/rules，成功原 receipt O_EXCL 固化，重启复用并重验；失败回执保留，中断 claim 不静默重取。未获新版本批准就阻断，不把旧 archive 当今日 HTTP 到达。缺 archive 不发请求。晨会 COMPLETE 仅指 PLANNING，显式带 rule_preflight.DATA_LIMITED，不授予交易许可。
2. 按 0242 把唯一外围权威段放在 `risk_limits.yaml.external_adjustment`，强校验 schema、类型和有限值；删去 deployment yaml 的重复政策。纳指≥1.5% AND A50≥1% AND VIX非NULL且≤20 才 +10pp、封顶95；收缩 OR 优先、缺源/过期 −10pp；VIX单独不可用为NULL，不扩张。只改变已批准的使用率基线，不放大单票/簇/交易风险限额，不产生买单。
3. 真实晨会测试证明 TREND 70–90 经 +10pp 变为80–95；yaml禁用扩张仍保留收缩和缺源保护。最小既有供应商隔夜源尚未接，现有宏观缓存仍非隔夜七项，自然缺源会继续保守，不拿合成夹具说实网已接通。

## 原始测试证据

均在 `artifacts/team-v2-20261016/`，content SHA 绑定，非最终 Team 全量/VM。

- `20261017T010350586771-daily-rule-source-first`：新 producer 2 个正向用10-19真实失败，发现旧计划窗口；未改旧核心，正向改为原窗口内10-16，另保留10-19阻断反例。
- `20261017T010444949311-daily-rule-source-qualified`：137 通过、1 源字节 pin 失败。实际是本 checkout 的 Windows CRLF，不是策略逻辑差异。两旧文件按已写死原 LF SHA 核对后只做行尾格式恢复，git 语义 diff 为0，未改常量/测试 pin。
- `20261017T010638498023-daily-rule-producer-and-original-pins-final`：9 通过、1 同上 pin 失败，原件保留。随后 `20261017T010711168969-daily-rule-producer-original-pins-qualified`：10 通过/exit0，原 LF pins 恢复；未重跑已证未变的旧137项。
- `20261017T010755342304-daily-rule-source-morning-wiring-final`：新增测试夹具缺 previous_trading_day 导致1失败；改用已有完整晨会 calendar 夹具后 `20261017T010823681795-daily-rule-source-morning-wiring-qualified`：57通过/exit0，含旧DB逐字节不变、预检仅一次、CONTROL/外围回归。receipt SHA `d1f4730b0ab7a774bad435d66cc61f31b672ccd48a0cf169cd0955b7e9e9d368`。
- 外围 `20261017T011055654293-external-0242-expansion-first`：104通过/exit0。receipt SHA `9109c0fec7c4703f1890e4001ad547a36fe9343b1506c6a33a9df8573ffe29e1`。与57/10重叠，不相加。

原批准四文件从旧 wp5 的固定 archive 只读复制到本 worktree 的同路径供真实正文解析反例；不伪装今日到达，不改原 archive，后续最终封包需携带固定四文件和 hash。

## 四层与下一步

CODE：本切片通过，但周一日期窗/身份源为明确门。REPLAY：仅隔离、真实旧正文+注入时钟和 HTTP mock，非六日模型回放。OPERATIONS：未部署、生产库未读写、服务未重启、未发通知；仅本机两小页既有只读源探测。STRATEGY：无收益结论。

优先消费 0245/0246 的实际答复，再推进身份/G5真实计数与日期窗接线。未答复时继续 A6 异常/其余角色/A5/调度等独立工作，不重复已经通过的撮合或 H-1 测试。最终真实模型干跑、六日/五日回放、全量/VM/回退与总候选评审未完成；H-1 f79 周日白天独立安装→部署→内部唯一probe安排不变。
