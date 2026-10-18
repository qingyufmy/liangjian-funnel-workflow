# 收盘完成 → PM → A6 自然调度边

实际本地检查：2026-10-17 03:51:57 Asia/Shanghai。

## 接线

- 新 `run-team-eod` 在 CLI 创建旧 WorkflowApplication/RuntimeStore 之前分派，仅访问既有独立 paper/team 账户；缺账户或晨会来源则 BLOCKED，不为制造成功建空账户。非交易日和 15:10 前 NOOP。
- 账户/持仓、只读生产原持仓计划、当日真实 paper 拒绝、原风险事件、最近真实市场状态先冻结入独立 Team DB。复跑用第一次上下文，不把后来持仓/行情变为第一次输入。没有行情供应商调用或重新研究。
- 既有 morning 风险上限、盘中 cap、故障日熔断只取更紧；A3-PM 隔夜去留 → A6 审核沿用已有严格 schema。两次 HTTP 各50s，Node 同一任务父120s；只是次日建议，不提交订单或激活计划。
- Node `team-eod` 使用独立 review 执行槽，**同日成功 close 完成后**触发，不和原 close 同槽、也不再触发研究。BUSY 时每≥5s可续接，业务失败不自动重调模型。
- Node close 完成写独立 `state/team_v2/eod-handoff/YYYY-MM-DD.json`，保留原 run_id/真实开始结束/exit code，O_EXCL 不覆盖。Node 重启读取同日真实完成原件后仅续接 EOD；EOD 的请求/模型 journal 幂等，不重复研究或已捕获模型调用。读取最大8193字节，过大/损坏/错日不凭标签认定成功。

## 实际测试

Python `eod-natural-command-first`：2 个失败（合成响应仍引用旧 fixture 目录 C1/M1）；严格校验正确拒绝。改正 mock 的引用为本次真实服务端 EOD:ACCOUNT/EOD:MARKET，未放松 schema。

`20261017T034801695655-eod-natural-command-actual-catalog`：**82通过，exit0**，含原持仓/空持仓、真实客户端mock、拒绝/失败恢复、CLI禁止旧构造器、A5及结算切片。原生产计划数据库字节保持不变。

Node 初轮定向10绿；全 Node 初轮1个旧预期失败（新增授权 team-eod 任务使原任务序列增加一项），旧预期同步为真实新序列。

`20261017T035020461224-eod-node-final-regression`：**全部20文件、158通过、exit0**；随后仅有界 reader 变化，`eod-handoff-bounded-reader-final-delta` **5通过、exit0**。Server 与 frontend TypeScript 最终均exit0。切片重叠不加总，不当作最终 Team 源 HEAD 全量。

## 发布门进度（本次时刻，不冒充周六12:00回执）

| 门 | 事实 |
|---|---|
| H1 独立 CODE/VM/回退 | 原最终f79 ACCEPT，未重复测试；周日白天部署未执行 |
| 核心交易员/风控/模拟盘 | 定向链已打通；本次A6 191绿、EOD82绿与Node158绿 |
| A5新角色/卡/队列 | 111绿，旧客户显示适配与逐事件ID问题0256待裁定，内部真实 sender 未完成 |
| A1/A2/A3研究新角色 | 提示词原件已定、部分校验器完成，完整自然接线仍未完成 |
| 六日正式新路径回放 | 未完成；旧21285零差异不是TeamV2六日门 |
| 三日真实模型干跑 | 未开始，真实调用0；没有拿HTTP mock当真实干跑 |
| 五日情绪开关轨迹 | 未完成 |
| 最终HEAD全量/VM/带H1生产回退 | 最终集成后执行；不是复用旧H1/B3回执 |
| G3/G4盘中雷达/龙头 | 尚未正式完成，按任务书可分开发布，不冒称启用 |

CODE：本次切片通过。REPLAY：正式门待。OPERATIONS：无生产写入/部署/真实模型/通知。STRATEGY：收益未证。
