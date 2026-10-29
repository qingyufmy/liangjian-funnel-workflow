# A4 外部 watchdog 无窗口维护约束

运行入口为 `scripts/watch_a4_runtime.py`。主 SSH 的无窗口参数之外，已校验并导入的历史 pin 模块内部 `ssh-keygen -F` 与 `ssh -G` 也必须无窗口。

维护来源：`_WindowlessPinSubprocess` 仅绑定导入模块的局部 `subprocess` 引用；对 `check_output` 的原 `creationflags` 做 `CREATE_NO_WINDOW` 位或。不得覆盖已有标志、修改历史 pin 源码/SHA/指纹/严格主机校验，或改成全局 monkey patch。

更新前运行 `python scripts/check_watchdog_windowless.py`。它实际执行两个本机校验入口，不连接远端、不读取业务数据库、不发模型请求。安装/重建脚本先运行该检查，失败时在注册任务前停止，保留现有任务。正常全量测试同时覆盖 `tests/test_watchdog_windowless.py` 的两个实际入口、原标志保留及 SHA 错误拒绝。

2026-10-29 本次整合没有运行安装脚本，没有重启、暂停或重跑任务，没有修改监控频率、业务查询、通知或交易行为。当前主工作树文件已有其他未提交改动，未覆盖或整体提交这些改动；后续更新必须从本维护提交保留此适配器和检查入口。

原自然观察证据由修复会话保存于 `D:/dev_codex/docs/console-flash-fix-20261029.md` 与 `D:/dev_codex/.local-runtime/console-flash-observer/result.json`；其 20:26–20:28 观察结论仅适用于当时窗口。本维护回归不再次启动观察器或业务任务。
