# 最终验收状态

当前仅执行 S00，禁止标记全项目通过。

| 维度 | 状态 | 说明 |
| --- | --- | --- |
| CODE | S00_CODE_ACCEPTED | 基线通过；离线入口 53 tests passed，正常退出 0，注入失败退出 1 |
| REPLAY | NOT_RUN | 未执行冻结历史数据回放 |
| OPERATIONS | PENDING_PRODUCTION_EVIDENCE | 未在目标虚拟机影子运行，未修改生产 |
| STRATEGY | INSUFFICIENT_EVIDENCE | 未做策略参数或增益结论 |
| DEPLOYMENT | APPROVAL_REQUIRED | 本分支未 push、未合并、未部署 |
