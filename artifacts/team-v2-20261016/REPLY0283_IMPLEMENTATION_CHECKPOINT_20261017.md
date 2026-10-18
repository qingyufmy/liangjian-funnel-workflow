# 0283 已实施；带仓回退和 A5 新版干跑有新证据，CONTROL 总门未通过

证据截至 2026-10-17 09:53 Asia/Shanghai。生产没有部署、重启、业务写入、研究重跑或客户通知。

## CODE

- `b85aa77`：双跑执行器。首遍实际决策捕获，第二遍 journal 只读、禁止新 HTTP；同一 paper 路径重建独立账户并留存两遍账本/曲线原件。合成 BUY→T+1 SELL 与资金曲线逐字节相同；相关 51 绿。它只是组件，不拿合成数据充六日回放。
- `f1d4d6c`：0283 五个作者文件逐字节 SHA 匹配，全文已读后复制、重新封存 manifest；A2 主题 coverage=scope 全键；非哨兵多余 search scope 只从派生判断剥离并记 WARN，原输出不动。A5 输入同时给标准层和原细分码。234 项关联测试通过，不是最终候选全量。
- `1994718`：带仓 record 保持在 CONTROL 自己的 paper.db：匹配前撤未成交 BUY；既有 SELL 与 1m 硬止损继续，无模型/新 BUY/迁库；沿用同一 packet 和 47s 预算。15:01 即使晚切 record 也先撤 BUY，再更新持仓估值；15:30 settle 不停。A5 给 RECORD_FALLBACK_ACTIVE 和起始时刻。80 项关联测试通过。
- `2966ea3`：发现历史晨会干跑暴露了当日最终 status/valid_from，旧两次 A6 和首次 PM 输入不合格，单独加 SOURCE_DOMAIN_REVIEW，不改旧回执/输出；历史投影去掉未来状态，缺值不捏造为 PENDING，实时源投影不变。42 项关联绿。
- `0f9602e`：真实晨会入口 PLANS_TODAY 原来未注入，红测读到 NOT_AVAILABLE；自然入口和冻结干跑已接线，46 项关联绿。测试重叠不相加。

关键回执原件 SHA：

- 双跑 `20261017T093021741888-replay-two-pass-related-receipt.json`：b9272bc92dc0b0aa5d7087d795777c276ade5f53fa7b594851ed503d3e9278ea。
- 0283 格式/域 `20261017T094233092467-reply0283-final-format-and-domain-receipt.json`：ee74d0dc6f94406127d0d868cc1fcfcb18ad603c29635fc126770797e11a27c9。
- 带仓/收盘/A5 `20261017T094936369341-held-record-close-a5-final-receipt.json`：0109475b0f23c9067c377b2374398ea1b0edf8f50c4ed64d2fea49d9feeebda4。
- PM 占位符 `20261017T095131768262-pm-today-placeholder-green-receipt.json`：0f03879b688d7f74818896bcc0216c40cd34815f6ea1fc333fd2ec57dd87cc69。

## 真实模型调用

新增 5 请求的进程均已退出；总计 14 请求、9 完整响应、5 provider 失败；4 个响应 JSON 合法，但扣除原源不合格者，仅 2 个源合格且 schema 合法。不能说三日六角色门通过。

- A5 2.0.5，10-14 冻结事实，新调用 PASS / 148666ms / 1 attempt，180s 内；FIELD_DIFF 原件有旧硬编码标签，另附 METADATA_REVIEW 说明实际版本，原文件不覆盖。
- A2 3.0.2 新调用 31513ms，ICE_POINT 非八阶段、方向自造、目录外 refs，正确 BLOCKED，问题 0285。
- A6 按 0283 关 thinking 后 20724ms 返回，但 PREMARKET 非活动容量字段空说明，A6_CAPACITY_INVALID，问题 0284。未满足“该次合法”条件，**未**把 A6 全会话或 A3-PM 永久切换。
- 修正原源后的 PM 仍 100s timeout，无槽位；旧 71489ms JSON 虽合法但含未来生命周期，排除，不拿来报 PASS。PLANS_TODAY 修复后的新 HEAD 真实重测尚待执行，不假称已自然通过。

六角色各会话 p50/p95/预算/失败及源排除清单均在 `20261017T095215265247-REAL_DRYRUN_SUMMARY.json`，SHA 89478c416e1a72419edaaf4e3756ed58e3d8d4777bdb8af5b8b6cb0de5d42144。统计包含非法和排除响应；N=0 是未观察，不是 0 秒。108 个本轮范围文件按两份已知 provider 凭证原字节扫描，0 命中；凭证只经内存，不声称全机未知密钥审计。

## REPLAY / OPERATIONS / STRATEGY

- 0277 A：六日源 21542 / 21338 可绑定 / 204 缺项已完成不重跑；完整六日 A4 合法率≥99%、零越权、双曲线及分歧/拒绝分布 **未执行完成**，不能上线 CONTROL。
- 0277 B：首日 PROBE/≤2、报价 DATA_BLOCK>20% 或护栏异常 record 锁存已实现；本轮补齐带仓保护，周一自然观察仍待证据。
- 0277 C：原 MarketQuote 独立归档已有实现，未自然运行验收。
- H1 最终 f79 和已完成全量/VM 不重跑；周日白天按独立批准安装告警配置→deploy.sh→唯一内部测试。Team 回退必须以已上线 H1 为基线。
- 不声称收益、胜率或策略有效。下一步继续其他角色真实输入和六日执行器；0284/0285 仅阻断相关作者契约，不阻断独立工程。
