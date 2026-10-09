# WP5 0006–0008复审后的确认与证据

## 1. 真实原因

Claude已读HEAD f0c20b7/2479b2d的评审包并接受本地代码合同及200股传输等价，但请求进一步确认发布门禁、机械拆分证据及短历史三根安全。复审接受不是发布授权。本轮没有push/deploy/生产env修改，仍以虚拟机47f6ef03b042c0f9a207400c72d2ecffff338244为运行状态。

## 2. 修改

`DEPLOYMENT.md`增加exact HEAD全量回执＋三个原日志传输步骤、两个`LIANGJIAN_TEST_EVIDENCE*`环境变量、PROJECT_ROOT从deploy脚本目录解析说明。测试门禁和脚本保护不放宽。

`tests/test_wp5_daily_incremental.py::test_complete_one_or_two_bar_receipt_never_selects_three_bar_overlap`分别验证源成功回执只有1/2根仍FULL_REFRESH/HISTORY_SHORT_BOOTSTRAP，不走`rows[2]`。生产函数已有`len(rows)<3`首行guard，本轮没有改该业务条件。初版测试额外猜测分页只会调用1次，真实pager会追加空页，因此2 failed/14 passed exit1；只修无关分页次数断言，保留FULL请求范围、安全及原因断言，16 passed exit0。这不是旧代码IndexError已复现。

## 3. 真实验证

VM以www/uid1001，仅在自有自动清理的临时目录运行源码绑定校验器，不运行deploy、CLI研究、RuntimeStore、网络源或通知。2479b2d正确回执验证通过，错误HEAD（实际生产47f6ef0）明确报`TEST_EVIDENCE_HEAD_MISMATCH`，前后生产HEAD不变。

命令：`artifacts/wp5-20261010/verify_test_gate_vm_readonly.ps1 -OutputPath artifacts/wp5-20261010/vm-test-gate-readonly-2479b2d.json`，exit0；证据SHA `e28381dbb07e9c33ff21f1c4c895939404d60296c4bde90268d5349fdd4d7bf1`。

重新运行严格原机械拆分manifest对当前源码校验exit1：唯一`ResearchPipeline._persist_gate`变更及新`audit_disclosure_scope` import；不隐瞒或改manifest让它通过。独立follow-up读取提交7402e7f的真实四模块AST，239定义、97导入关系与原manifest相等；当前238定义未变，唯一后加WP5公告范围审计和CANDIDATE_DOMAIN fail-closed逻辑单列精确patch。

命令：`python -B artifacts/wp5-20261010/verify_relocation_current.py artifacts/wp5-20261010/research-relocation-current-2479b2d.json`，exit0；证据SHA `dcdbb279f2eee1f87d173389e1daf88c4c43a6fbba091c8b6809f6b36010f52a`。这是拆分等价＋明确后续业务差异，不是当前全部AST等价。

短历史命令：`pytest tests/test_wp5_daily_incremental.py -o addopts='' -q --junitxml=artifacts/wp5-20261010/daily-short-guard-1-2-final-v2.xml`，exit0、16 passed。该切片不与2632通过的2479b2d全量相加；新源reader完成后再生成最终新HEAD回执。

## 4. 四层

- CODE：确认切片通过；2479b2d已有2632 passed/6 skipped/1 xfail独立全量，当前新增未提交reader须新HEAD全量，不冒用旧回执。
- REPLAY：200股传输等价已获接受；复权真实语义、竞价历史pending仍待证据。旧47行不升级。
- OPERATIONS：没有发布，自然五交易日未验收。
- STRATEGY：未改阈值，没有新收益结论。

## 5. 边界

0008最新范围切换最低条件将10-08旧原派生输入缺失样本保持DATA_LIMITED/missing=0；仍须10-12自然影子COVERED、P1-A闭环，再由Tony逐次批准切换。原因子样本合同未接A3；本地shadow竞价合同未接22:00/07:00。只读计划导出的日期与overlay边界已提交桥接0009，不等待答复去做可独立反例。

## 6. 下一步

封存真实publication/positions只读reader及反例，再完善共享绝对截止源adapter；A4决策留证信封按0007两层coverage合同单独推进，不补历史判断。
