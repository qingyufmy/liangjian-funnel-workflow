# W3草稿/正式日报截止合同

按照桥接0040实现独立ShadowCloseCoordinator：15:30调用草稿冻结回调，市场截止固定当日15:00；同日16:00 A5已完成且来源/完成时刻绑定合格才产正式日报；16:45仍未完成/失败则使用草稿与A5_NOT_COMPLETE，不等A5、不改变A5调度。所有writer由调用者显式注入，未接生产timer。

不把迟到poll回填成15:30冻结；真实as_of与LATE_DRAFT保留。16:45之后才看到A5，不能证明硬截止前已可见，按deadline_missed+A5_NOT_COMPLETE保留。失败成为终态，不无限重试。跨日、时钟倒退、错时段/日期、未来完成时间和缺来源hash均不放行。纯来源绑定不等于真实A5获取认证。

测试先红：模块未实现exit1；首次14时段/截止测试中新增迟到反例1 failed、13 passed、exit1。修复后四个相关测试文件46 passed、exit0。

实际命令：`python -B -m pytest tests/test_w3_shadow_close_schedule.py tests/test_w3_shadow_day_adapter.py tests/test_w3_shadow_daily_cli.py tests/test_w3_shadow_week.py -q --tb=short --junitxml=artifacts/wp5-20261010/w3-close-schedule-final.xml`，python为项目现有3.11 venv，PYTHONPATH为本worktree/src。

CODE：截止合同与异常隔离切片通过。REPLAY：固定反例，非真实A5全日回放。OPERATIONS：UNWIRED；未部署独立日报消费者/定时任务，不能称已自然16:45完成。STRATEGY：未改任何参数/客户通知/成交或收益。
