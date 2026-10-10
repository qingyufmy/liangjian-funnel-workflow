# W3 A5 只读完成观察器

当前组件仅读取显式原件与现有 SQLite `mode=ro`。不构造 RuntimeStore/App/Settings，不请求行情、模型或 Node API，不发通知，不写报告或生产库。真实 timer/Node DTO原封存接线未完成。完整唯一关联也只输出 `DATA_LIMITED` + `candidate`，等待桥接0049裁定时段关联语义；不会给协调器伪造 `SUCCEEDED`。

## 来源与时间

`inspect_a5_completion` 接受原 ledger 行、批准 JSON 原字节、原 `recentJobRuns` DTO 字节以及同run原stdout。要求当日 `a5-close/run-a5-close`、真实 aware `finishedAt`、exit0且无signal/reason、唯一非skip `DISPATCHED`、固定16:00 slot，以及该进程时间区间内唯一 accepted POST_CLOSE 行。失败/运行中的第二父进程也计入歧义，不用“只挑成功者”掩盖并发。

批准报告须是 `{facts, report}`，与原 ledger 两份JSON完整对账；按正式事实口径重算去掉顶层 `input_hash` 的 canonical SHA。`output_hash` 始终标为 `model_output_hash`，与批准JSON原字节SHA区分。DEGRADED是质量标签而非任务失败。不会用lease.completed_at、文件mtime、报告cutoff、observer当前时钟补父进程结束时间。16:45之后首次读取不能追认截止前可用。

`read_a5_observation` 仅读现有DB（不存在则不创建），短事务rollback后关闭；不使用immutable=1或checkpoint。批准JSON路径必须resolve在显式root内，拒绝symlink越界。原打开file的fstat前后一致、字节长度一致后留原SHA。SQLite行数/累计JSON字节在载入正文前限制，读与对账共享2秒**接受截止**；真实阻塞文件系统不声称可强制中断。超限迟到结果不接受。原源对象不变，无猜“最新”修订。

现有公开stdout不含review_id/input_hash，因此唯一时段关联明确标为 `UNIQUE_NONOVERLAPPING_INTERVAL`，不是强身份因果性。现本地09/30归档缺完整三件，同日原件不够则缺项；不与09/29批准报告拼接。审计证据见 artifacts/w3-observer-20261010/A5_OBSERVER_READ_CONTRACT.md。

## 需求—测试—证据

| 要求 | 测试 | 证据 |
|---|---|---|
| 质量与任务分开、真实父完成钟 | unique_degraded_completion | observer-final-v2.xml |
| exit0/空调度不冒充复盘完成 | skipped_python_dispatch、noop_or_non_original_success | 同上 |
| 原报告与账本及facts hash一致 | ledger_and_approved_json、wrong_input_hash | 同上 |
| 同日、deadline、来源唯一、不猜关联 | late_observation、overlapping_parents | 同上 |
| mode=ro、路径边界、源字节不变 | actual_ro_read、missing_db、report_path_escape | 同上 |
| JSON载入前预算、总接受期限 | content_budget、inspection_finishing_after_read_budget | 两份 before XML 与 final-v2 XML |

实际命令（PowerShell）：`$env:PYTHONPATH='D:/dev_A股/liangjian_wp5_hotfix_20261009/src'; & 'D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe' -m pytest tests/test_w3_shadow_a5_observer.py tests/test_w3_shadow_close_schedule.py -o addopts= -q --junitxml=artifacts/w3-observer-20261010/observer-final-v2.xml`。

真实结果：37 passed，native exit0，XML SHA `983c5c47d6ef30ad04e10bfdb66e3175c0a4814038914ab6f4ad2f0b46cd20f8`。缺模块红期exit1；content-budget和总预算反例各实际1 failed后修绿，原XML保留。早期observer-first/final切片分别35/36pass，但36命令后附带不相关missing-file查询使整条shell exit1；最终独立37命令exit0才作为验收。

以上37项及源码SHA `868194b458b2bb54a920adbb16efce97535e3c5b733bcf43ac0f99121bc8c505`、测试SHA `41920aceec61163f3f973af6c71cced5f5f5208bcdbd5733c9fc8559fb586138` 对应桥接0050的原固定包，原包不覆盖、不改SHA。

## 13:40 时间与截止口径反例补充

进一步核查真实 Node logger 使用 `timestamp` 保存stdout到达钟。新增六个反例揭示：原观察器只检查dispatch在同日，未要求位于父进程实际区间；未验证stdout原到达钟；原facts虽与账本JSON相同且hash可重算，事实 `cutoff_at` 仍可与账本声称的15:00冲突。

修复仅收紧本地源检查：每条匹配stdout需原aware到达钟且在父进程区间；dispatch在该区间并不晚于唯一accepted row创建；原facts cutoff等于accepted ledger cutoff。不得因缺原到达钟自行用当前读钟或文件mtime补足。

命令同上，XML改为 `observer-clock-before.xml`（6 failed、23 passed、native exit1）与 `observer-clock-final.xml`（29新+14协调器=43 passed、native exit0）。两份原XML保留。最终XML SHA `a596cbaefa417fb8eecf52d046bbba882eb35d4ebd29287f77e123021833005f`。

当前源码SHA `9a56575c490ec0d3862cf6debaaa3e41340aaa8842831baec6144f788b446946`，测试SHA `bb12d10dd0dd0f75f056ce423fc2d23a6cb7ca65c5a599a6860de69233c0abed`。它们不属于0050旧包或aa68a78全量回执，需送新的增量预审。interval关联仍候选，不放行SUCCEEDED。

## 0049 评审后影子专用接线

Claude 0049 已裁定唯一时段关联可用于独立影子日报完成观察。完整来源现在只输出 `A5_COMPLETED_OBSERVED`，保留 `correlation_basis=UNIQUE_NONOVERLAPPING_INTERVAL`、原 accepted row id、父进程 startedAt..finishedAt、观察时间及批准JSON原字节SHA；不输出或接收 `SUCCEEDED`。它不是 A5 内容、通知送达、强身份或生产业务成功证明。强绑定 run→review_id 属于后续 runner 工程，不在本轮中补造。

`ShadowCloseCoordinator` 只接受上述影子观察，在16:45后首次读取或任一来源/时间歧义下使用原草稿并标 `A5_NOT_COMPLETE_OR_AMBIGUOUS`；报告 writer收到全部关联依据。没有接真实 timer，也没有修改 A5 调度。旧0050/0052固定包和XML不覆盖。

新增真实反例 `interval-policy-before.xml`：3 failed、native exit1（旧状态不能释放影子日报、旧协调器错误接受SUCCEEDED）。修复后 `interval-policy-final-v1.xml`：46 passed、native exit0。增加区间关联歧义与采样器生命周期隔离合同后的联合切片 `artifacts/wp5-20261010/claude-0048-0049-components-final.xml`：101 passed、native exit0，SHA `0b493d660197fa4f4411d27324b2a827bc9330b62e3543507d01c5f2d04db05b`。这是联合切片，不与aa68全量数字相加。

CODE：新组件合同切片通过；不属于aa68a78原全量回执。REPLAY：真实历史归档只有部分证据，0 qualified observations。OPERATIONS：UNWIRED/UNDEPLOYED，不宣称A5正常运行。STRATEGY：不涉及参数或收益。
