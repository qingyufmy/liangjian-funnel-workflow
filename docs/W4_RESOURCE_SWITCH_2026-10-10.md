# W4 收盘资源采样独立关闭开关

按Claude0048，新增 `Settings.close_resource_sampling_enabled`，默认True，显式环境变量 `LIANGJIAN_CLOSE_RESOURCE_SAMPLING_ENABLED=false` 可关闭。未修改生产env或部署。开关关闭时不构造观察器、不建线程、不读写资源文件，原研究返回对象、异常和deadline对象直接保留。它不是研究、风险或数据资格开关，也不进入模型输入。

反例：`sampler-switch-before-v2.xml` 2 failed、native1。更早before.xml也exit1，但env测试使用了错误关键字，不能当作该项代码反例。修复后第一次相关回归42pass/1fail：在Windows繁忙线程调度下，同一采样同时晚于研究结束和观测预算，只记录了前者；改为独立记录两个超界事实，迟到样本仍不进入证据，没有放宽预算。

`sampler-switch-final-v2.xml` 43 passed/native0；扩展constructor/start/stop六个故障隔离合同后与W3/Settings联合执行 `claude-0048-0049-components-final.xml` 101 passed/native0。此切片不覆盖整个新HEAD；没有修改A4动作、阈值、报价或确认层。

真实命令：设置本worktree PYTHONPATH，用既有`.venv/Scripts/python.exe -B -m pytest tests/test_w3_shadow_a5_observer.py tests/test_w3_shadow_close_schedule.py tests/test_w4_resource_sampling.py tests/test_settings.py -o addopts= -q --junitxml=artifacts/wp5-20261010/claude-0048-0049-components-final.xml`，native exit0。关闭开关与反例清单见上述测试，不需要读生产库。

CODE：合同切片通过，待新HEAD全量。REPLAY：不改策略路径，无新策略回放声明。OPERATIONS：未发布、未改生产env。STRATEGY：不适用。
