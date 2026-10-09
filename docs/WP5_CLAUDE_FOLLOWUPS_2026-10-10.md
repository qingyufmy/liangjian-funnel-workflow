# WP5 发布规范化和夜间截止跟进

依据 canonical bridge `0017-reply.md`。仅隔离分支，未 push/部署、未接线发布回执或夜间任务。

## 发布规范化

删除 `publication_receipt._payloads` 内复写的 symbol/trigger/stop/no_chase/confirmation/action 规范化。函数内延迟导入并调用生产纯函数 `workflow._plan_payload`；不构造 `WorkflowApplication`、Settings 或 RuntimeStore，不读取库或模型。延迟导入避免模块级相互依赖。现有严格哈希/权限/修改集合审核保持不变。

反例先行：spy 证明原版本没有调用生产规范化；修复后真实函数被调用。15 个符号格式/无效符号与策略组合同时核对生产规范化输出，零/负/缺价格保持生产既有结果。该合同不是计划资格门，不能用此测试宣称无效价格可执行。

源码 AST 测试允许的唯一 workflow 导入严格限定为 `_plan_payload`；应用、设置、数据库、网络和发布入口仍禁止。

## 夜间截止

`run_maintenance` 新增可注入 `clock`，默认仍为 `time.monotonic`。父循环、共享 gate、观察耗时、报告耗时使用同一时钟，不改变预算或依赖超时。

确定性反例：第一个采集实际返回成功对象但时钟已超过共享截止，必须记录 `DEADLINE_EXCEEDED`；第二个标的必须 `MAINTENANCE_BUDGET_EXHAUSTED` 且从未调用采集。原挂起工人持有槽并触发下一次 BACKPRESSURE 的回归保留；删除 Windows `elapsed < 0.5` 的测量断言，不替换为更大的容差。真实 I/O 尾延迟移至未授权 VM 基准和自然运行 OPERATIONS，不能由此宣称写盘预算已验收。

## 实际命令与证据

环境：共享 `.venv/Scripts/python.exe`、`PYTHONPATH=src`。

1. 两个新增反例精确节点 pytest：exit 1，2 failed，`artifacts/wp5-20261010/review-followup-before.xml`。
2. 三文件切片首轮：88 passed / 1 failed（旧 AST 禁止任何 workflow 导入）；`review-followup-final.xml` 原样保留，未以后续 shell 命令的 exit 0 冒充测试通过。
3. 修正 AST 精确允许纯函数后89 passed，exit0，新回执 `review-followup-final-v2.xml`。
4. 与资金双截止合同/原市场缓存联合：136 passed，exit0，`review-cutoff-joint-final.xml`。不是与前三次计数相加。
5. 补齐无效预期供应商不能自认证的五个反例后，最终联合141 passed，exit0，`review-cutoff-joint-final-v2.xml`；136项旧回执保留，不相加。

CODE 为局部切片；REPLAY 为本地反例；OPERATIONS 未接线未部署；STRATEGY 不适用。原 0.594 秒全量失败回执保留，真实时延根因并未凭这一改动精确定位。
