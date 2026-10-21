# /team 事件与原件映射

本表是只读展示契约，不授权执行研究、行情采集、交易或补写事件。14 个明确列举类型全部保留；任务书中的“13 类”文字差异已交 0439 请 Claude 确认。没有源就不发该类型。

| UI 类型 | 已核实的原件或持久表 | 展示规则 / 当前缺口 |
|---|---|---|
| clock | 对应原事件的 captured_at / observed_at / as_of | 历史时钟只代表正在查看的记录时刻，不冒充实时行情 |
| task | workflow_runs / workflow_stages；model_calls；trader_sessions | 调度计划不等于实际完成；provider_attempts=0 不能称模型主动观望 |
| role | 上述任务的已知角色及结果 | 姿态是 UI 映射，不是新增决策。工作/说话不产生交易 |
| say | model_judgments 的可展示结构化结论 | 不展示原始提示、客户正文、模型私有推理；缺原件不造发言 |
| relay | 已有显式 source_call_id / meeting_id 关联 | 时间相近不等于任务交接，当前 fixture 不补造 |
| focus / conds | monitor_events，execution_plans；原 confirmation_results | 逐项 available/met、闭合时点、阈值和原观测；不硬凑七条件 |
| account | Team paper daily_nav / 当轮冻结 snapshot.account | 只显示源净值/现金/盈亏；旧 workflow 虚拟账本不得混入 |
| plans | execution_plans + 原 publication_facts / source row SHA | 保留原 33 与后续激活状态，INVALIDATED 不进入可执行列表 |
| positions | Team paper_lots / paper_orders / paper_fills | 无原件为缺源，已证实空列表才显示空；初次同步不是新成交 |
| risk | risk_day_state、risk_meetings、pm_decisions、control_record_latch | 区分单股与全账户；只读显示，不能解锁或切 CONTROL |
| theme | 尚无可证 theme_intraday_5m 表 | 缺源，不创建表或生成题材排名 |
| alert | team_notifications 的允许展示元数据及锁记录 | 不含 webhook、接收地址、客户正文、完整 provider 错误或环境 |
| market | 原市场上下文 / 冻结快照；持续行情来源尚待核实 | 缺源不绘制指数走势，不额外请求行情 |

## 本机 10-20 回放

只读三份已经存在的本机原件，`scripts/load_team_fixture.mjs` 在内存中核对原字节 SHA 后白名单投影，不复制 MB 级审计。

- morning：`artifacts/team-v2-iteration-20261020/frozen/morning.json`，SHA `8b869dac2e94ece67b0f83d070b9706b00e399bd5503669a16085aea0c279362`。
- models：同目录 `models.json`，SHA `d898f7e4bd22afb924acf1eab4be4f78c71fb1bd23b4b82b18cf43c946960f11`。
- latest：`artifacts/a4-monitor-20261020/LATEST.json`，SHA `8cae46bd9f84adcd5f790a3bade9ebdcae9016bca8b46dc2361f4270be9b3441`。

根目录通过 `TEAM_UI_LOCAL_EVIDENCE_ROOT` 显式传入，只供 loopback 开发服务读取；不是生产根目录参数。所有事件含 source/id/SHA/at。行情条件保留原 available/met 值，题材、市场、角色原话缺源不补。Team session 的分钟时刻取原 decision_id 的本地日期分钟编码，明确是记录编码，不给它编造毫秒耗时。

覆盖：33 原计划、32 ACTIVE_TODAY / 1 INVALIDATED、27 Team 时点、32 条条件原件、13:23 RECORD 锁及原内部通知计数。账户只证明 08:33 原 snapshot，末条零持仓只证明最后计数。**不是完整全天旧客户 A4 流水**；PM 早晨失败在汇总中可见但此包缺其模型原件，不生成对应模型耗时。没有日/周收益证据就显示缺失。

开发 mock：GET/HEAD `/api/ui/replay`、`/api/ui/snapshot`、`/api/ui/events`，必须 `trade_date=2026-10-20`。返回明确 `LOCAL_HISTORICAL_FIXTURE`，其他日期 404，写方法 405；SSE 带事件 id、明确 fixture-end，客户端读完关闭而非伪装直播。这些端点目前仅在本机预览服务，**不等于生产 Node 真实接口已完成**。

任务书§4九个GET路径也由 `scripts/team_mock_api.mjs` 按同签名提供：day/tasks、feed、plans、fills；role/today、stats；review/scoreboard、lessons；settings。day限定10-20，role/today在mock中也明确历史数据模式，不冒充今日。limit 1–500，offset有界，days≤20，未知角色/坏参数拒绝；source缺失返回null而不是[]或0。设置写申请路由不开放，本次无POST。生产接口将复用同一展示约定，但当前并未集成生产服务。

生产只读接口还需按现有 Node 服务集成、默认关闭 `/team` 开关、SQLite mode=ro + query_only、有界读取、零写入/模型/通知证据及 Claude 审查；盘中不为前端连接 VM。届时原件不全的类型继续缺源，不能用本机历史 fixture 代替生产值。
