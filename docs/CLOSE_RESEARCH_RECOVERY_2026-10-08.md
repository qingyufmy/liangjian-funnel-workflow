# 10月8日收盘研究超时修复与恢复

## 已核实根因

生产自然 close 已使用 sealed A1，不是隐式重跑月度 A1。15:10启动，3975只全市场日线更新耗时约2123秒，1561只官方公告定位耗时约2471秒，PDF约119秒；冻结快照约118秒。16:39:55保存恢复标记，16:40被Node的5400秒总预算终止。资源测量只是statvfs、proc读取，并非递归磁盘扫描；特征维护实际DISABLED。原FEATURE_SOURCE_GENERATION标签使排障方向误导，本次改为真实SNAPSHOT_READY，未增加任何运行超时。

## 最小实现

1. 公告缓存必须同时验证内容哈希、股票、查询区间、关键词身份和非未来接收时间；TTL未过不代表昨日查询覆盖今日。
2. 历史450日定位结果与完整近期全公告结果覆盖连续区间时，生成显式union投影。不重新模拟供应商的模糊关键词检索；保留原历史结果、全部近期公告、来源日期和两份输入哈希，交给既有事实/PDF相关性选择。结果source_id明确为official_disclosure_incremental_projection，不冒充新HTTP请求。
3. 日期有缺口、错股票、过期超过既有45日边界、结果不完整、文档修订冲突或缓存哈希损坏，仍执行原完整官方检索。近期风险检索不能借历史缓存跨日跳过。既有7日业务TTL、45日明确降级回退、策略阈值、情绪执行权限不放宽。
4. 旧官方router没有保留search_keyword，只允许固定的四个历史语义缓存键提供关键词身份；显式关键词冲突仍拒绝，任意新键不享受这种推断。

## 反例、真实对比与命令

- 最终相关7文件切片：`python -m pytest tests/test_disclosure_incremental.py tests/test_workflow_fact_result_cache.py tests/test_disclosure_router.py tests/test_cninfo_fact_normalization.py tests/test_workflow_integration.py tests/test_workflow_orchestration_coverage.py tests/test_pipeline_data_sync.py -o addopts='' -q --tb=short`，退出0，88通过（5.50秒）。缺失tests/test_a1_active_downstream.py的初次扩大命令退出1、未运行测试，随后按真实存在文件更正。
- `python scripts/audit_disclosure_incremental_readonly.py --day 2026-10-08 --output artifacts/diagnosis-20261008/disclosure-incremental-readonly-v3.json`：退出0。只读1619组完整历史/近期/当日全量检索三元组，845组可合成：473组文档ID和内容哈希完全相同，372组包含全量检索的全部文档并额外保留1049条近期公告；缺失/修订差异0，缓存哈希错误0。774组严格拒绝合成，走原完整检索。另3356个缓存标的缺三元组，不冒充验证通过。
- v1探针发现旧router缺关键词元数据，全部拒绝合成；v2曾错误使用标题子串模拟供应商检索，真实对比519组漏文档，退出1。候选未部署，已修正为明确union；v2失败证据完整保留。不能只凭单测通过发布数据优化。
- 真实对比是事后缓存一致性，不证明旧判断时已收到后来资料，也不证明未来所有收盘研究均能在90分钟内完成。
- `git diff --check`退出0。

## 10月9日准备：真实恢复结果

17:27:55经主机/地址/HEAD/无并发校验，正式恢复单元liangjian-close-recovery-20261008-1730.service启动现有scripts/run_close_with_a2_diagnostics.py。固定复用当日哈希5c1c660d0b7c3435cf49d569a6c7bd42aec48b8e6951c171149fe23383ad3b1e快照与有效sealed A1；不重新采集，不重跑A1，不传非法历史snapshot_id，不补历史信号。

17:35:19正常退出0；run_id=2026-10-08-close-a2-audit-172755，READY_DEGRADED，研究436秒，峰值内存1.5G。使用火山方舟deepseek-v4-pro，A2覆盖审核通过。A1运行视图1451只，A2评估1451/聚焦4/A3输入9。A3有5只技术研究合格（核心3与观察2），但正式发布0：全部来自情绪研究路径，A2记录BLOCKED/A2_EMOTION_CYCLE_NO_NEW_ENTRY，未取得趋势轮动核心资格；不能用A3技术研究合格解除上游权限。股票为600026、600663、603906、600310、601975。这不是任务仍超时，也不等于明日已经具备可执行计划。

下一步核对数据不足/轮动入口与18:00自然A1维护，次日竞价刷新需据新交易日事实重新研究。不得通过调高数量、删除情绪限制或激活10月8日过期计划制造明日就绪。

## 分层验收

- CODE：88项相关反例/回归通过，待部署后核验实际导入模块。
- REPLAY：845组真实公告缓存对比无遗漏；774组继续严格回退，不做因果性或收益声明。
- OPERATIONS：正式恢复已完成，明日可执行计划仍为0。发布及新缓存逻辑自然调度另行记录，不能称全项目稳定。
- STRATEGY：三策略、情绪权限、T+1与风险阈值未改。本次不证明新增买点或策略收益有效。
