# 迭代决策记录

## D-S00-01 隔离优先

所有开发在 `codex/implementation-s00-s11-20260920` 工作树完成。测试只使用临时 SQLite；离线 fixture 移除模型和通知凭证并阻断 socket。未授权前不部署、不 push、不触碰生产状态。

## D-S00-02 一个事实存储，不造平行平台

后续复用 RuntimeStore、A1 registry、分钟缓存、RiskGovernor、回放设施和当前前端投影。需要的新字段以兼容扩展或派生投影实现。

## D-S00-03 分级验收

代码通过不等于回放、运维和策略有效。`CODE`、`REPLAY`、`OPERATIONS`、`STRATEGY` 分开记录；缺少生产证据保持 `PENDING_PRODUCTION_EVIDENCE`。

## D-S00-04 不通过放宽策略制造改善

本迭代不修改策略阈值、不扩大候选数量、不降低数据完整性要求。easy-stock 只能作设计参考，新源默认旁路且无授权不启用。
