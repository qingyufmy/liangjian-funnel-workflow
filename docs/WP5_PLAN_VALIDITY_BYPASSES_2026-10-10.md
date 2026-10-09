# WP5：计划日期旁路小修

本地 CODE / 相关集成切片验证完成，未提交、部署或修改生产。依据 canonical bridge 0007 的 WP5任务4日期子项；A4信封仍为设计，不在本次实现范围。

## 结论

真实入口反例证实旧 `activate_plan` 可将过期、翌日目标、NULL/naive失效日计划变成 ACTIVE；旧 `publish_plan_batch` 可直接发布同样不合法的 ACTIVE_TODAY，并退役已有active。旧active的幂等返回也先于日期验证，能返回已过期行。本次仅补这两个入口的共同aware日期合同，不改交易策略、监控、风险或工作流。

历史9-28的5条异常仍是独立事实：激活时间9-28 09:32、失效日9-25 15:00。旧激活/发布入口没有充分血缘，因此**不能认定是本次查证的旁路造成**，也不修写那5行或其历史输入。

## 行为合同

- `plan_validity.plan_time` 只解析明确、aware的时间并规范到Asia/Shanghai，不填NULL，不推断交易日。`activation_reason`复用该解析，既有expiry/target-day判断不变。新增 `active_plan_reason`检查有效起点同session且不晚于操作时钟。
- `activate_plan(valid_from=...)`：显式时钟继续用于原有paper/replay/操作路径，不新要求等于当前wall；expiry必须不早于该时钟、同session，payload.target_trade_date若有必须一致。显式历史时钟是可复现state合同，**不是生产授权绕过**，它不能改写任何已过期active的原始有效区间。
- `activate_plan(valid_from=None)`：保留row原start，以 `_now` 检查当前session；原start缺失/naive、未来未生效或原expiry过期均拒绝。不把旧expiry/start重标为今天。
- ACTIVE幂等先验证原row；原 `valid_from` 不采用新传入值，不写updated_at、不重标日期。到次日或expiry后再次调用即拒绝，不能以已ACTIVE逃过检查。
- `publish_plan_batch`直接ACTIVE必须有aware start/expiry且构成同target日有效区间。以显式start验证区间，允许正式09:26发布、09:32才生效；`list_active_plans`仍按原时段生效，不提前执行。新active日期（含UTC字符串）规范为上海offset，避免现有SQL字符串区间比较误判。
- 批发布先规范/校验全部传入active日期；事务内对全部已有/重复plan ID验证内容和原区间，然后才开始任何retirement、parent invalidation或INSERT。旧已有行不能用传入的新expiry/start绕过。反例使用SQLite trace确认日期失败前没有INSERT/UPDATE/DELETE，不只检查回滚后状态。
- 翌日PENDING展示和发布不变；批激活原有逻辑不改。持仓独立保护不调用新entry日期门；expired entry源计划仍可供已有持仓风险与类型化退出使用。低层 `create_execution_plan`（fixture/import构造入口）不在此次ownership范围，未加新门；此修复不宣称所有底层写入口均被禁止构造legacy行。

## 文件与夹具

源码仅：

- `src/liangjian_funnel/runtime/plan_validity.py`
- `src/liangjian_funnel/runtime/state.py` 的activate_plan/_transition_plan/publish_plan_batch必要段

新反例：`tests/test_wp5_plan_validity_bypasses.py`（25项参数化测试）。覆盖过期/未来session/target、NULL/naive、省略start、原区间幂等、完整批原子性、事务前DML检查、UTC规范化、同日未来起点、pending展示、expired状态转换。

仅补3个旧fixture文件的合法日期，未删测试或放宽任何原断言：

- `tests/test_runtime_state.py`：幂等计划原NULL起点/expiry补固定08-24 session；SQL默认at的旧断言用相同固定 `_now`；批冲突/替换fixture补合法start，使测试继续到原身份冲突/替换业务边界。
- `tests/test_runtime_monitor.py`：同一08-24分钟fixture补当日15:00 expiry、显式09:31 activation；模型次数、确认、去重、数据门、markdown、风险退出断言全部保留。
- `tests/test_a3_a4_observability.py`：使用原bars的08-31 observation时钟激活，expiry为同日15:00；原安全字段/策略观测断言保留。

未改 workflow.py、monitor.py、strategies.py、预算/配置。其他操作者的 DEPLOYMENT.md、docs/iteration/ITERATION_STATE.json、test_wp5_daily_incremental.py 等修改未回退、未纳入本次ownership。

## 实际验证与退出码

Python固定 `D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe`；工作目录 `D:/dev_A股/liangjian_wp5_hotfix_20261009`；PYTHONPATH指本worktree/src。无生产数据库/缓存/网络/模型/通知调用；测试使用临时SQLite和固定fixture。每次命令末尾保存 `$LASTEXITCODE`、输出并 `exit $taskExit`，下表是Python实际退出码。

| 顺序 | 命令选择 | 实际结果 | Python退出码 |
| --- | --- | --- | --- |
| 1 先反例 | 新test文件，旧源码 | 19失败/2通过；非法计划未抛异常、幂等绕过、UTC未规范 | 1 |
| 2 初修 | 新test文件 | 20通过/1失败；新UTC断言误将09:32的minute32带入期望15:00，修fixture期望为15:00 | 1 |
| 3 集成首次 | 下列8文件（当时新21项） | 85通过/15失败，全部旧fixture缺expiry/start，非策略/风险约束变化；已先向主代理报告 | 1 |
| 4 新SQL反例 | 新test文件增加4项 | 24通过/1失败；通用激活保留UTC expiry字符串导致SQL不识别原等价有效区间 | 1 |
| 5 修后集成 | 下列8文件（新25项） | 104通过 | 0 |
| 6 扩充实际风险切片 | 下列8文件+portfolio_risk_lifecycle | 115通过，19.34秒 | 0 |
| 7 独立编排/审查 | iteration/test_a4_orchestration + test_llm_review_contract | 19通过，3.02秒 | 0 |

最终完整相关命令：

```powershell
$env:PYTHONPATH='D:/dev_A股/liangjian_wp5_hotfix_20261009/src'
& 'D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe' -m pytest -o addopts='' tests/test_wp5_plan_validity_bypasses.py tests/test_plan_session_contract.py tests/test_runtime_state.py tests/test_runtime_monitor.py tests/test_a3_a4_observability.py tests/test_a4_runtime_repair.py tests/test_workflow_integration.py tests/test_workflow_premarket.py tests/iteration/test_portfolio_risk_lifecycle.py -q --tb=short
$taskExit=$LASTEXITCODE
Write-Output "PYTEST_EXIT=$taskExit"
exit $taskExit
```

```powershell
$env:PYTHONPATH='D:/dev_A股/liangjian_wp5_hotfix_20261009/src'
& 'D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe' -m pytest -o addopts='' tests/iteration/test_a4_orchestration.py tests/iteration/test_llm_review_contract.py -q --tb=short
$taskExit=$LASTEXITCODE
Write-Output "PYTEST_EXIT=$taskExit"
exit $taskExit
```

先失败命令相同Python/PYTHONPATH，选择 `-m pytest tests/test_wp5_plan_validity_bypasses.py -q`，实际打印 `PYTEST_EXIT=1`。仓库默认addopts=-q，后两正式切片用 `-o addopts=''`保留准确通过计数。`git diff --check -- <本次tracked文件>`退出0；workflow/monitor/strategy diff numstat为空。

最后验证源码实际字节SHA256：plan_validity=`b827277f5092674b068a811de9143f59a3fb89b75905cbb48c5ac608e117755a`；state=`635de2c7a9737f6a212dd7f9e2463b46be4c98b336e66f3457ae7921351800d9`；新test=`83634364881f70bce804d1feedbdebf8a7f1303cc2d0efedf82e4351673ddf84`。未跑全量测试，不将134项相关切片称全仓验收；未验证生产自然调度、旧历史入口归因或真实留证信封。
