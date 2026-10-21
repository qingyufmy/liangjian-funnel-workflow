# 10-21 核心候选：开发进度，不是发布通过

范围依0466/0467/0470/0471/0474。生产目前仍是fa2b9b9，今日14:41的RECORD保护不在此候选开发阶段解除。客户不补更正卡。新候选及回退各自VM全量、隔离生产入口整链、Claude最终ACCEPT全部欠缺时禁止上线。

## 已实现及真实反例

- WP1：旧客户与Team共用`liangjian_paper_broker.sellability.sellable_qty`，各自账户adapter只提供自己的dated lots；所有有效退出前过门，原子paper admission/matching二次强制T+1。当前新审计统一T1_LOCKED，历史码只兼容读取。
- 原件`tests/fixtures/t1_002270_20261021_1451.json`保留完整原payload字符串，SHA956bb09c531e29f5f884264e926a2033efb5a95f789fd61340684feb6b3197cf。退出门→持久事件→publisher回归为0有效减仓/0减仓卡/1风险待办与中性风险通知。非生产发送。
- 次日自然monitor重评、期限09:35，缺首bar仅记风险审计，之后新闭合bar可完成；不把旧退出搬到次日撮合。Team测试09:30缺证→09:31新价重评，昨日lot可卖正例；不冒称明早已运行。
- WP6-1：共享`clamp_entry_quantity`按risk、20%单票、PROBE10%名义、现金、板块及原上游cap取最小；持仓与预留BUY均占名义空间。STANDARD不加10%门；002270原价位18.80/stop18.66/NAV100万示例从20%10600股进一步压到5300股。不得追改今天已有10500股。
- 数量审计包含raw_risk_qty/cap20_qty/probe10_qty/final_qty/binding_limits；有效daily Wilder ATR原件仅用于STOP_TOO_TIGHT复核标记，不新增拒绝门。缺ATR不填造数值。
- WP2-1：A5旧guard只查metrics，原11:37答复引用A6事实中的15000被误拒。新guard从原样送入facts递归采集有限数值和来源路径，拒绝未知数字；不手工列预算白名单、不改变原坏记录。原回复hash78e2c5980cd3ae367ef02d40dbca812b4a421d5cdfd31863f4b701f7d333142f；原风险预算路径`$.a6.rows.risk_meetings[0].result_json.envelopes[0].risk_budget`。
- WP2-3：A6异常错误不再写/reset晨会risk_day_state；只丢弃该会话未通过处置，保留既有目标限制及真正engine/morning hard halt。下一新闭合tick按引擎触发重新评估，失败范围/恢复条件留审计；不把本会话格式错经外层monitor升级为账户持久RECORD。
- A5紧凑格式与A6完整输出契约放在最后一条RUNTIME_INPUT请求，重算input_hash；仍保留system+user两条消息及原角色源码hash，未放宽原schema。14:41旧原回复仍按原八错拒绝，不能改写它冒充新通过。
- WP2-2：内部发送保留HTTP状态/provider码/消息摘要hash及有限错误类别，不记录secret/供应商原文。历史拒绝不每分钟反复告警；仍保留未确认总数，任务完成不冒称通道健康。真实拒绝根因与一次内部实发门尚待验证。

## 测试与未过的门

开发测试阶段：先前316项定向、后续T1原件/次日27项、WP1+WP6合并127项、A5/A6相关133项、内部37项均有本地通过；是有重叠的不同轮次，不能相加冒充全量。增加新的真实边界测试后仍需重验。

剩余：新候选commit及VM自身全量、回退VM自身全量（串行）；真实A5一次、A6异常一次待预算答复、A2一次依0474；生产入口隔离整链及安装后验收；内部实际拒绝根因与单次真实投递；最终批准。WP2-4 A2原始轮动域RECUR、R11 context字节、9→7发布前差额2、CNINFO52分钟根因另交。没有把非核心待交项写成已完成。
