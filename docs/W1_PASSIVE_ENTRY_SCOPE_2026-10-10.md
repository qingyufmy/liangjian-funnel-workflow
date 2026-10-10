# W1 全集原基线与新增仓研究子集（0053）

## 1. 结论与范围

仅改 `runtime/shadow_session.py` 的影子消费路径。全集为指定lane的当前ACTIVE rows与同分钟原monitor事件引用rows的并集，不删终态事件、不按updated_at回填历史状态。原策略、生产monitor、adc1de89影子引擎、G-B resolver及参数不改；不发布、不启动服务、不操作生产。此切片不是新PIT bridge接线或自然交易日验收。

## 2. 全集passive基线合同

先验证原atomic monitor/latest完成fence、observability hash、精确分钟/cutoff/snapshot、全部事件与计划row的身份、事件key/UUID以及原outer action/reason等式。全集任何冲突仍整轮DATA_LIMITED，不能因事件属于终态或持仓而跳过。源状态SQLite为显式mode=ro事务，没有RuntimeStore构造。

原row主键与event payload/key/UUID构成计划身份链。旧publisher没有要求payload重复plan_id，因此缺重复字段只记MISSING；若该字段存在则必须精确一致，不补作历史证据。row.updated_at仅用于拒绝原事件以后发生的revision，不用来推断过去ACTIVE。

所有已验证原outer baseline投影写入minute summary，并在调用引擎前进入session receipt，包含原action、first_cause、event_id、event_sha256与字段缺失状态。`source_scope=CALLER_SUPPLIED_NOT_REEVALUATED`，不声称重新计算生产基线。首笔BUY后已有持仓，仍保存该分钟原BUY；终态缺inner/bars时不虚构重放，原事件与完成fence仍必须成立。风险处理仍归生产路径，此处只封存原证据。

## 3. 确定性entry cohort与严格等式

规则版本`shadow-entry-scope/1`：当前status=ACTIVE_TODAY、当前有效期覆盖dispatch及真实观察时钟、无已记录有效PLAN_INVALIDATED或payload invalidation、同`paper:<lane>`账户该symbol无正持仓、原计划/事件身份链已验证。只给该子集调用现有evaluate_tentative；无entry时不调用策略，仅存全集passive minute。

排除原因TERMINAL、INACTIVE_STATUS、HELD、OUTSIDE_VALIDITY、IDENTITY_UNPROVEN逐计划列出，可重叠；原因计数之和不等于去重排除计划数。全集身份失败不会伪装成单计划可旁路：回执全数/entry数保持null，保留失败census和gap。当前holding仅属于同paper账户同symbol；不相关symbol或其他账户的holding不能整轮阻断。

入参子集必须与规则重算集合严格相等，重复、遗漏、额外或对象内容不一致均拒绝；引擎结果也不得产生子集外plan信号。ACTIVE但entry bar/inner/frozen market不足仍整轮DATA_LIMITED，不把缺bars从entry集合悄悄删掉。

## 4. 回执与PIT边界

新session回执`shadow-session-receipt/2`保留旧字段，新增full_plan_count、active_plan_count、entry_plan_count、excluded_plan_count、entry_plan_ids、exclusion_counts、逐计划entry_admission、passive_baseline_coverage_count/status与source bindings。entry_plan_count是结构性研究scope，不等于PIT合格交易数。逐日可累积entry_plan_count / active_plan_count的plan-minute口径；原终态baseline也计入全集，但不混入ACTIVE分母。全轮拒绝不能当零entry或通过分钟。

缺provider/UNKNOWN的实际BUY/ADD研究结果只存DATA_LIMITED audit，variant_action=null，evaluation_variant_action保留真实计算动作，data_block=true；不提交这些key的engine首次触发状态，未来证据补齐不能因为先前审计而丢首触发。独立ledger仍按status/action判断触发，不相信event_kind声明。minute first_trigger_count仍是引擎tentative原值，明确basis=ENGINE_TENTATIVE_BEFORE_PIT_NOT_DURABLE_LEDGER，不能解释成真实ledger首笔数。

既有显式注入provider协议仍是READY+原bytes+seal成功，不新增身份认证或新W2规则验证。默认自动接线UNWIRED；它不满足新bridge业务验收。writer状态机fixture使用明确mock provider，仅测提交/时钟/失败，不当来源证明。预算仍总≤5秒与同分钟fence，未扩大队列或线程；不确定writer结果仍暂停session。未来正式PIT接线应由独立批准切片完成。

## 5. 反例与验证

独立artifact目录`artifacts/wp5-20261010/w1-passive-entry-scope-0053`保留before及修复过程中失败XML，不覆盖旧证据。原新增9例native exit1（6 failed、3 passed）：终态被丢、相关/无关holding整轮阻断、无PIT仍OK BUY和子集合同缺失。后续补同账户隔离、当前ACTIVE不能恢复terminal、revision拒绝、额外engine信号、真实独立ledger零entry原BUY、显式payload身份冲突/缺字段记录等。

实际Python为`D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe`，PYTHONPATH明确当前worktree/src。最终命令、原生退出码、JUnit计数与所有文件SHA记录在独立freeze.json。测试全部临时SQLite、固定bars、确定性时钟，未调用provider/网络/模型或生产。

## 6. 未完成边界

没有自然日完整覆盖率、真实来源PIT完成、收益/成交或生产启动证明。原完成文件不含每event原始内容SHA，当前合同核验它所拥有的原outer字段与UUID/key，再绑定当前只读event完整row SHA和payload字节SHA；不声称这个hash为producer额外签发的认证。bar缺失的passive事件不被升级成策略可算。G-B封存结果和source版本不被本session改动冒充。
