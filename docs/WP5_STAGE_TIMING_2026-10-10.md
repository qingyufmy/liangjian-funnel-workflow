# WP5 任务 4 阶段计时与预算展示（2026-10-10）

## 结论与边界

CODE 通过：阶段多次访问、失败重进、同 run 恢复、并发模型阶段和父任务实际预算已接入；本轮归档身份修复后 110 项相关 Python 测试通过、diff --check exit=0。未改控制台源码，沿用上一 slice 的 67 项控制台测试及 typecheck/build:web exit=0 证据，本轮未重跑这些命令。真实应用使用的后端文件投影、overview DTO 与执行进度 React 组件已连接，既有组件 SSR 验证展示了准确数值与失联标签。

REPLAY：仅确定性本地计时夹具与相关既有工作流回归，不是自然收盘研究。OPERATIONS：未部署、未生产执行、未浏览器访问生产页面；生产任务/浏览器视觉/自然日运行仍待证据。STRATEGY：不适用；未改候选、策略阈值、公告门、请求限流、父任务总时限、模型或通知。

## 真实调用链与源码范围

`server/runner.ts timeoutForJob → childEnvironment` 注入 child-only 的 `LIANGJIAN_PARENT_JOB_BUDGET_MS`、`LIANGJIAN_PARENT_JOB_STARTED_MS`；值来自本次已计算的父任务 timeout 与 startedAt，不读取或修改 .env、不更改定时器。close 原有绝对 `LIANGJIAN_PARENT_CLOSE_DEADLINE_MS` 继续按原值传递；NaN/Infinity/非正预算、missing close budget、安全整数溢出均拒绝，不能靠清除 deadline 延长时限。

`runtime/progress.py WorkflowProgress → state/workflow_progress.json timing → server/files.ts ProjectFiles.workflowProgress/normalizeWorkflowTiming → server/dashboard.ts overview.workflowProgress → web/src/App.tsx WorkflowProgressPanel → WorkflowTimingTable` 是实际应用链，非独立设计预览。旧 progress 没有 timing 时明确显示预算未知，不推断 90 分钟或 research_close_deadline_seconds 为分母。

必要 workflow.py 改动仅阶段切换与回执：数据同步、A1 维护、普通/竞价/比较研究在结果整理阶段切到 PERSIST；成功回执添加 stage_timing，不更改研究输入、算法或业务发布逻辑。A1 维护另有 `<maintenance_run_id>-a1-maintenance.json` 回执。WorkflowProgress 每次原子写进度时还写同目录 `run_timing/<safe-prefix>-<original-id-sha256-16>.json` 安全计时回执，所以新任务覆盖展示文件后，新合同下之前失联任务的最后一次 RUNNING 仍可复查；异常/中断不需要冒充 finish 才能留证。

新增测试：tests/test_wp5_stage_timing.py、test/server/wp5-stage-timing.test.ts、test/web/wp5-stage-timing.test.ts；并扩展既有 test/server/wp5-deadline.test.ts。新增可见组件 web/src/WorkflowTimingTable.tsx，沿用执行进度面板样式并增加表格的基础窄屏横向滚动。server/types.ts 与 web/src/types.ts 定义对应小型 DTO。

## 计时合同

- `PHASE` 是进度观察者的串行阶段访问窗口，不是 CPU 时间，也不独立证明所有后台工作都串行。相同 phase 的持续更新不重复开访问，离开再进入新建访问；阶段切换标为 LEFT_PHASE，不冒称阶段成功。
- `RESEARCH_STAGE` 按 lane 单独记录，多模型可重叠。失败后的 RUNNING 为新访问；重复 terminal callback 不制造重复访问。COMPLETED 但批次未满不能提前结束累计阶段。阶段离开与真正完成分开标记。
- `python_elapsed_seconds` 是已观测的 Python 调用墙钟累计；`run_wall_elapsed_seconds` 从原 run 起点计算，包含恢复间隔；`parent_elapsed_seconds` 从本次 Node 父任务启动时间计算。三者不混同，模型并发阶段耗时不能加总为总墙钟。
- 同 run/job 恢复只导入有界的固定 token/数字/日期计时合同，不导入原模型内容。旧 RUNNING 在旧 updated_at 处标 INTERRUPTED，不把失联到恢复之间的时间补为执行时长。不同 run 重新开始记录，旧 run 回执保留。
- totals 同时保留累计耗时/访问次数与本次 invocation 耗时。预算占比使用本次耗时除以本次真实父预算；不能把旧尝试累计时间除以重启后的新预算。visit 还保留其调用起点、预算和父起点供反查。
- 没有有效父预算/起点时为 UNKNOWN，不猜默认数字。比例可以超过 100%，不通过截断掩盖超时。Node 原超时与 close deadline 仍是执行边界；这些展示计数不授予执行权限。
- 最多保留最近 384 条访问明细，较早明细移出时 totals 的累计耗时和次数仍保留，visits_dropped_count 明示。固定 phase/lane/status 白名单，不复制凭证、任意模型文本、响应或未知字段。
- 进程被杀没有 finish：回执保持最后一次 RUNNING 和记录截止时间；服务 heartbeat timeout 呈 STALE，但不虚增耗时或把 visit 升格为 COMPLETED。UI 显示“失联前记录，未确认结束”。正常任务 finish 中仍未有明确 stage 完成证据的访问标 RUN_ENDED；失败 finish 标 INTERRUPTED，均不表示阶段成功。迟到回调不会重开已 finish 的计时。

## 反例与验证命令

先行 8 个计时反例全部失败（旧代码无 timing），见 artifacts/wp5-20261009/stage-timing-before.xml。实现后补充时钟回拨、finish 后迟到回调、384 条边界不丢累计时长、恢复后本次预算口径和未知文本白名单反例。

本轮逐 run 归档反例：`a:b` / `a/b` 和共同 190 字符前缀、仅末尾不同的两个原始 ID，在旧标点替换/截长实现中均只留下一个文件；两项失败见 stage-timing-collision-before-v2.xml。修复保留最长 64 字符安全前缀，附原始完整 run_id 的 SHA256 前 16 位；状态与回执同时保留完整 64 位 run_id_sha256，新状态恢复还核对该身份。两项修复后测试确认各有两个独立文件、身份匹配、各自 7 秒记录不覆盖。较短前缀给 Windows 原子写临时文件留空间。旧无哈希回执未迁移、未删除，也不宣称旧记录已隔离或重新验证。

Python（PYTHONPATH=src）：

    python -B -m pytest tests/test_wp5_stage_timing.py tests/test_runtime_progress.py tests/test_workflow_pdf_progress.py tests/test_workflow_orchestration_coverage.py tests/test_wp5_pipeline_workflow_integration.py tests/test_workflow_integration.py tests/test_workflow_premarket.py tests/test_workflow_replay_snapshot.py tests/test_a1_registry.py --junitxml=artifacts/wp5-20261009/stage-timing-backend-final-v4.xml

110 passed，exit=0。含两项归档碰撞反例、本次 timing、原进度秘密过滤/原子写、公告原文进度、真实 workflow 编排与父 deadline、研究/竞价/快照回放/A1 注册回归。

控制台：

    npm test -- test/server/wp5-stage-timing.test.ts test/server/wp5-deadline.test.ts test/web/wp5-stage-timing.test.ts test/server/server.test.ts --reporter=junit --outputFile=artifacts/wp5-20261009/stage-timing-console-final-v4.xml
    npm run typecheck
    npm run build:web
    git diff --check

上一 slice 67 tests、0 failures；四条命令当时均 exit=0。本轮仅重新执行 Python 回归和 diff --check；控制台证据来自未改动源码的上一 slice。DTO 测试覆盖有界安全投影、实际预算重新计算、旧/无效分母未知、heartbeat 不外推、坏 JSON 恢复；UI 测试用安全 DTO SSR 渲染真实已挂载组件，验证 30% 父预算、10% 本次阶段占比、访问次数和未完成标签。这不是浏览器交互/布局或自然运行通过。

## 本地证据

- artifacts/wp5-20261009/stage-timing-example.json：上一 slice 由实际 WorkflowProgress 生成的确定性历史夹具，声明 NOT_NATURAL_RUN；不是新归档文件名身份合同的证明。Python 观测墙钟 35 秒、父墙钟 55 秒、预算 100 秒；DATA_SYNC 两次访问共 15 秒，两个 A2 lane 各 10 秒，不相加成研究总墙钟。
- artifacts/wp5-20261009/stage-timing-validation.json：本轮验证时 HEAD、源文件/新测试/最终 XML/反例 XML/历史夹具 SHA256、110 项本轮 Python 与 67 项未改控制台上一 slice 证据来源、命令 exit。未 commit/push，保留其他代理改动。

覆盖范围是现有 WorkflowProgress 长任务与成功 workflow 回执；没有采用该 writer 的其他模块不能据此宣称已具备同一阶段合同，独立后台预取也没有新增工作线程级独立计时。没有重启生产、改生产 env、访问生产库、模型或通知。
