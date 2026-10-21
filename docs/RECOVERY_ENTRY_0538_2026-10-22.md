# 0538/0541 恢复入口及供应商拒绝留证

状态：仅本机候选。07:02 一次获批诊断 HTTP 200 已交 0542；今日盘前不部署、不重生成，原 002354/300556/600201 三计划照常。生产仍以既有已验 041726b 为基线，不能用本文件声称已安装新代码。

## 显式恢复，不重置旧运行

仅 `run-close --regenerate-from <source_run>` 可加：

```
--recovery-attempt 2
--recovery-from <prior_regeneration_run_id>
--recovery-terminal <absolute_path_to_prior_terminal_JSON>
```

`N` 是大于等于 2 的整数，必须恰好接前一 attempt。旧 binding 必须证明相同 source_run、交易日历源日/目标日、真实时钟、原 CLI 入口；终态须原 HEAD 相同、非零整数 exit，完成时刻在旧启动之后且不晚于当前。成功、运行中、缺原件、跨交易日、错前驱均拒绝。终态是运维原件，入口不生成或修补它。

新 run_id 为 `regen-<source_day>-<new_HEAD>-r<N>`，末尾 `-r2` 保证落入既有 progress 文件名 64 字符约束，避免 `-attempt-2` 在第 65 字符被截断后与其他 attempt 混用。新 binding 独占创建，增加 `recovers`、`recovery_attempt`、`recovery_terminal_sha256`、`recovery_authority=0538`；发布 manifest 同样记录。

不删除或改写旧 binding/marker/claim/终态，不 SQL 重置，不换 HEAD 冒额度。原 A1 新鲜度、最近收盘日→下个交易日、06:30 启动/发布截止、原批次完整匹配、严格 A2/A3、原子整批替换及失败保留原批次全部不变。自然 `run-close` 未增加任何恢复行为。

## 收到拒绝先留证，再走原失败/重试逻辑

`OpenAICompatibleModelClient` 仅对实际收到的 HTTP 429/5xx，先持久化完整脱敏错误正文、error.type/code/message、原 body SHA、retry-after、所有限流/配额/request-id 头及调用绑定。文件独占写入 `workflow_output_dir/provider_rejections/<上海交易日期>/`，不存请求 messages/Authorization，不复制生产库。

脱敏覆盖已知密钥、敏感字段、认证 URL、请求回显。文件写入/fsync/SHA 复核失败，保留原供应商失败码并附安全 `provider_evidence_failure`，停止该次继续重试，不假报保存成功。正常 200 路径不写此类文件；没有新增模型调用、供应商切换、超时放宽或交易权限变更。

注意：旧 0529 运维 transport 在 ModelClient 前截取 429，因此本候选不等于旧守卫已被修正。后续真正启用恢复 OPS 时必须用同一留证 helper，另按已批 0534/0538 绑定真实额度（60/120/240/480、每 call 4、全程 12、HTTP 20、长 retry-after），保留旧守卫原件，不能原样运行旧全局一次 marker 包装。

## 已做及待做

- 本机 7 个相关测试文件 150 项通过、0 失败，耗时 8.51 秒；包含真实 CLI 转发、独占绑定、旧行不变、日期门、拒绝留证与现有模型传输回归。全部请求为 MockTransport，不是 VM own-full。
- 15:05 后随统一批次执行新 HEAD 虚拟机 own-full/同 HEAD 40/回退可用性及既有发布验收，并经 Claude 最终批准；本机绿不代替发布门。
- 本次没有执行恢复，没有发布新计划，没有改生产文件、DB、服务或配置。15:10 自然研究的真实结果另按当时生产版本与原件评估，不提前称通过。
