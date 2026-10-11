# 0072 影子分钟库连接修复

基线 f6e114e。仅修改独立影子 Source/Session/CLI，不改生产分钟写端、A4策略、数据库权限、调度、模型、客户通知或计划。

- 每0.5秒尝试打开 `mode=ro`，实际触碰schema以验证WAL可读，随即rollback。不采用immutable、写PRAGMA、checkpoint或复制生产库。
- 成功后保持同一连接；每轮读取独立BEGIN/ROLLBACK，空闲时不保持读事务。
- SQLite读故障关闭连接，标记本分钟DATA_LIMITED/MINUTE_DB_WAL_NOT_PRESENT；仍继续重连，不补已完成或失败的历史分钟。
- 曾就绪后连续120秒无法重新就绪，返回SHADOW_MINUTE_SOURCE_LOST，CLI退出3。120秒由单调时钟计量，不是跨过两个分钟标签；成功重连清空连续失败时钟。
- 初始尚未就绪不冒充故障恢复成功；周一09:31首次写入至09:35的自然READ_OK仍需单独验收。
- CLI所有退出路径finally释放source连接。轮询最大0.5秒；现有安装模板使用0.5秒，无单位修改。

反例及虚拟机隔离fixture使用新建测试库，不连接生产库。VM实跑使用真实候选CLI与ShadowSession；测试IPC仅提供读数观察与同线程关闭SQLite句柄的故障注入，不能算自然盘中验收或策略收益证据。

发布前提供新HEAD全量回执、VM实跑、前向回退树=f6e114e及回退全量。发布、影子启用须Tony另行批准。本文件不是发布批准。
