# 10-21 核心候选：开发进度，不是发布通过

范围依0466/0467/0470/0471/0474/0475。生产目前仍是fa2b9b9，今日14:41的RECORD保护不在此候选开发阶段解除。客户不补更正卡。新候选/回退自身VM全量、隔离生产入口整链及Claude最终ACCEPT缺任何一项都不发布；下方早期进度由最新追加核验覆盖。

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

## 18:03 最新裁定与追加候选范围

- 已全文读0481（SHA96bf19c79b0236b9240a06d525f894fe713710b37a363b9875801d6116db0db7）/0482（SHA2b660cfa702eb3fc2a865edc041f3c438896f45dbd506deb1c1f2ca98e67b4e7）。0482覆盖A5旧速度目标，数值正确性为核心且已获接受，耗时/深度质量不阻塞T+1发布。
- 846d52a VM own-full6450PASS/0FAIL/9skip/1xfail，6460总，release_qualified，SHA cb58377945ca3406dac28abb20df5b5a82bd4db29ce80491e3bdb2624511e0b2。A6单次真实新契约READY/validated1/17331ms，actual17:51:26→43，未触账户RECORD；0483已交。A2单次冻结请求边缘验证原回复validated1/86.72s/provider1，非生产postpolicy/整链，不夸大覆盖。
- 新增生产入口`run-close --regenerate-from <source_run_id>`：只允许本日15:10后、已发布READY原run和准确旧pending集合、已封存当日15点后A1。使用该新A1的原snapshot_id/hash，复用字段SHA清单写run；禁止用旧日/未来快照冒充本日。跨日再启动拒绝，不改时钟。
- 重生成正常调用实际`run_research`/自然Team选择；A2最后原校验history非空且最后一轮0issues才允许发布，归一化阶段绿不能代替。几何后有效batch非空才原子替换。RuntimeStore同事务比较准确旧pending集合，任一并发变化/身份冲突整批回滚；旧payload不改，独立supersession审计记`post-fix regeneration`，旧status为SUPERSEDED。失败/空可执行batch保留旧3条，杜绝混批。
- A5仅自身配置`config/team_v2_a5.yaml`：600秒，午间12:50前结束；客户端仅该角色使用600，A4/A6/PM预算不动。A5流式观测记录packet/TTFT/provider usage，未报告不估算。撤销速写限制，最后user绑定六项深度复盘要求；严格数字域保留。
- A5每计划保留原plan/冻结原件、每种观察条件状态的全部样本计数及首末证据、原生命周期、重放摘要；Team账户完整快照与旧客户账户分列。聚合标多轮采样不称持续；无源MFE/MAE/R不计算。不是删原件，原4.83MB完整facts仍原地冻结。
- 追加定向117项中116PASS，1个旧220k速度上限断言与0482冲突；仅改为精确逆变换/完整因果事实断言后，该项独立PASS。另75及64相关定向已过（重叠不相加）。追加源码需新HEAD VM full，不沿用846绿件。
- A1老入口的周更/月更规则解释了10-16之后未自动刷新；按0481/0482新日更时效要求，今晚必须显式增量刷新，全域门不绕。自动调度日更化尚未修改，列下一闭环项；不得把“原设计如此”当现行R20已通过。

## 17:42 候选首VM full及T+1新边界

- 候选87c54b1 full：6459总/6434PASS/15FAIL/9skip/1xfail，SHA44c7d05ec5bfbe56b3173850708a9371cfae40d1816f50e960d58e12d3e5312a，红件保留。回退f301 full6402PASS/0FAIL/9skip/1xfail已终态合格（bc3ff3e0...，完整SHA见桥接0480）。
- 真实新边界：旧账户次日缓存sellable_qty仍为0时，原adapter把已释放dated lot再压成0，误抑制昨日仓风控退出。现删除“缓存上限”依赖，单一lot函数只依本账户日期/剩余数量；现金/预约/成交量仍由现有broker事务检查。新增“昨日lot+缓存0→可卖”“今日lot+缓存乐观→仍锁”双向测试。
- 其余对齐：真实旧仓fixture取代无日期总仓stub；次日必须新评估事件才能成交，旧昨日退出不自动重放；PROBE10%数量期望15000→10000；A6两次无效advisory不能当全账户halt；A2重建输入包括新末端目录契约、仍逐消息hash相等。
- 对应183项本地开发轮180PASS/3FAIL，最后3项是在该轮载入后已修的测试代码；随后独立23项重跑全部PASS。均不是新HEAD VM full证明。下一新HEAD仍须完整VM own-full。
- WP2-2真实SENT已关闭发送故障；A5真实180s超时、A6真实预算/整链/最终批准仍未过。A2只做零调用组件预检，授权单次仍保留整链使用，不消费额外模型预算。

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
# 0488 补充（10-21 18:48，替代原 A2 post-policy 责任分配）

模型原回复按自己的 `focus_decisions ∩ frozen direction members` 校验，不能把同题材非成员算作覆盖。真实模型错误必须在最多三轮内自行纠正，投影删除不替代 provider validated=1。服务端后续策略降级导致原有重点成员全部消失时，只补 `SERVER_POLICY_DEMOTED` 说明；rule_id、降级符号、原/后 SHA 与调用身份冻结，focus 选择和最终权限不变。其它漏方向/外域/格式错误不自动补。

0487 实际原件投影回归区分两类：医药 603127/300199 被服务端降级，补医药；AI_COMPUTE 的 603236 不是方向成员，仍欠说明且不得由服务端补。先前 9d 整链终态 BLOCKED，A2/A3 各一实际HTTP、A6/PM/A4均0，预算前拒绝两次额外语义尝试；原件保留。0488批准新HEAD A2≤3，其他各1。

A5按0488以公共策略定义+引用ID进一步无损去重，字段缺失/null/原数值与全文均须逆向一致；不提高250k门。最终HEAD自身VM全量、同HEAD新整链和Claude最终ACCEPT仍缺，不能借9d 6470/0。

## 20:50 0498 / 0501 授权窄修（替代上段对应旧限制）

- 0494三份原始provider JSON按原字节/hash保留；纠错反馈逐行标明symbol字段和合法同代码标识，不在unchanged/server-demoted列表回显非法证券。
- 0498只对合法目录唯一六位代码规范交易所后缀，逐字段`SYMBOL_SUFFIX_CANONICAL`审计；未知/歧义/外域/重复/其它refs错误仍拒绝。原journal严格校验结果不改。
- 原第三轮先前在错误后缀处短路，规范后暴露多余NO_FOCUS。0501批准先后缀→删除已由本模型合法focus覆盖方向的矛盾说明→完整严格校验。删除留`CONTRADICTORY_NO_FOCUS_DROPPED` raw→null；不增改focus、不补真正缺失方向、不制造模型判断。三份真实原件最终有效校验分别FAIL/PASS/PASS，首份其它refs错误仍拒绝。
- 每次冻结`a2-canonicalization:<call>`，分别报告strict原样与canonical通过率；重生成与OPS守卫只认同最后call的0498/0501白名单审计及成功postpolicy，不沿用其它归一化绿。
- 仅显式重生成从原snapshot_manifest.source_failures派生DATA_LIMITED/缺源明细/PROBE上限；A3选中后几何未发布也独立留小审计。A6末端合同要求逐项缺源审查和原PLAN引用；缺审/坏审/缺晨会只阻断对应缺源计划，不能被conservative fallback或新价刷新绕过。自然研究/正常无缺源计划不变，不改原快照或默认删除证券。
- 0498发布口径：新HEAD自身VM全量+确定性40、前向回退自身VM全量及一次同HEAD整链；A2仍失败时证明旧3条保留与无异常扩散，核心仍可申请发布，但不正式重生成。最终ACCEPT与实际VM安装验收尚待，非上线记录。

