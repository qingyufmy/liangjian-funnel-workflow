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
# 17:11 追加核验（尚未发布）

## 17:26 新失败及处置（不冒称真实门已过）

- A5单次真实验证在VM上180.109s超时，遥测为`MODEL_WALL_CLOCK_TIMEOUT`，provider_attempts1，输入106986字符/115977字节，token_usage未报、无原回复。不能从这份遥测证明是prompt计算慢还是服务端排队；未追加模型调用，0479已申请下一次预算。
- A5增加可逆输入投影：数据库中规范JSON字符串无损解码，重复长事实用共享引用；全部原字段/数值/原引用保留、逐对象逆变换hash验证。校验仍用未投影完整facts；不是删原件或人工白名单。相关54项定向通过，VM实输入缩减量及真实验证仍待。
- VM回退fa2首full失败13项，原红回执保留；其中8项资源前置门因/tmp仅3.26GiB（原门要求4GiB）、3项验收待足额盘复测、1项glob目录顺序误取repair、1项CRLF原件被Git默认转换。没有修改4GiB/10%门。
- 回退候选`f301e203a0a7489a77a158f3ab7f75a63ed9dca1`相对fa2仅`.gitattributes`精确保存已有原件CRLF、该原件字节及测试glob选择；生产源码逐文件相同。自身full使用独立/var/tmp TMPDIR在VM串行重跑，不冒充fa2原HEAD通过。

- 内部配置由 Tony 更新后，17:00:51 单次新验证 SENT/HTTP200/code0；不重发旧19001失败项。
- A1 active pointer 仍是 `a1-incremental-20261016T180003192023Z-824999b48c32`；按0475须刷新，不能以旧周更口径结案。现有同日快照 selected1578/research3577，不能直接用于要求全研究域的 A1 snapshot reuse。
- A2 同一事实域共7个方向，最终有效FOCUS覆盖4个，正确 NO_FOCUS 集仅液冷/先进封装/航运3个；原回复4行多了 `SELECTED_BOARD:AI_CPO`。该行说明自身已承认300852为FOCUS。不是大小写或别名，更不是外域ID；服务端只删除矛盾行，绝不改ID。新增删除审计rule_id/raw-row/normalized-null；最后user请求由同一direction_members生成条件式目录，首轮即可见，校验不变。定向65通过。
- R11：A2真实请求286074字符/估算81547 tokens；输入持久上下文1417759字节（含原始值和传输投影），不把它说成小于1MB；收盘summary67833字节。投影从426957缩为278015字符，65.1%，尚未达到历史60%目标。
- selected9到geometry7少的2只是002313、603236，既有 `A3_EMOTION_RESEARCH_ONLY_NO_ENTRY` 隔离，不是几何静默丢失。
- 精确phase timing更正：EARLY_DISCOVERY_DAILY_SYNC1995.38s、COMPANY_FACT_SYNC419.30s、CNINFO_SYNC790.70s、PDF14.82s；此前把15:25的阶段累计3103s称为CNINFO单阶段不准确。总4046s仍成立，未完成增量优化不称达标。
- VM独立回退HEAD fa2b9b9自身full已启动，使用独立源码/Node依赖副本和核SHA的规则原件；未改生产。
