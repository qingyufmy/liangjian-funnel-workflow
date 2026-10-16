# PM→A6 收盘会话接线回执

实际记录：2026-10-16 21:56 Asia/Shanghai。源码 `276844abf73d886da6dcb82b0fbd9537569bbb8c`。

新增 `run_team_eod`：在同日15:10后，使用 Claude 原字节角色提示词，经既有真实客户端依次调用 PM_POST_CLOSE 和 A6 POST_CLOSE。当前验证使用本地 HTTP MockTransport，不是供应商真实干跑，不访问生产。

PM 对冻结持仓恰好逐只判断；A6 对 PM 原意只能同意或收紧。重复/缺失/域外股票、伪造 PM stance、反向放宽、域外风险事件或漏事件、额外股数字段、无引用、超上限容量均阻断。PM 的原始提案独立进入 `pm_decisions`，不被 A6 覆盖；已审隔夜建议与真实 `pm_override` 同事务提交。中途插入故障整笔回滚，重启读取原模型响应继续，不重复调用。

会议绑定账户、日期、持仓/事件原值、目录、预算、两个调用身份与共享/角色提示词 SHA；模型消息也绑定账户和完整会议摘要。同一调用不能被另一账户挪用。调用结果已捕获或已确认 FAILED 后，最终提交失败可恢复；在途未知状态不伪装失败或成功。PM 失败不调 A6；A6 失败保留 PM 提案为 UNREVIEWED，最终 overnight_actions=[]，不伪造 HOLD、减仓或明日订单。原始模型 JSON 仍在独立模型日志中。

## 原件

均位于 `artifacts/team-v2-20261016/`：

- `20261016T214746914226-eod-schema-session-red`：新函数未实现的 collection exit2，不称业务红测。
- `20261016T214911592595-eod-first-check`：测试夹具错误调用不存在的 `_client`，保留，不称业务缺陷。
- `20261016T214920962876-eod-second-check`：真实入口拒绝缺 THEME_REVIEWS 输入；补齐测试源，不修改角色输入契约。
- `20261016T215445515363-eod-pm-original-red`：PM原提案未入 `pm_decisions` 的持久化反例实际失败，修复后通过；原件未重写。
- `20261016T215500294081-eod-affected-final-green`：156 passed、0 failed、exit0；32项新EOD与124项关联验证，重叠集合不与旧回执相加。回执 SHA `012026ba3b24fdca673552f0682e7f23886ff64e45815588b07c4a33030fad1e`。命令在提交前运行，按源码内容SHA绑定，不冒称最终Team发布HEAD全量。

CODE：上述会话及持久化局部通过。REPLAY：合成HTTP/独立SQLite反例通过；六日回放与三日真实模型门未完成。OPERATIONS：无生产改动、未部署；生产源获取、close尾部CLI/Node接线及父预算仍待整包验证。STRATEGY：不据此声称收益或策略有效。

剩余 A6 INTRADAY_EXCEPTION 会话、PM槽位到交易员实际执行约束、生产源与调度继续推进；0227晨会时序和0230分类语义待答复，但不阻塞独立工作。H1最终0224评审/0223告警来源未变，不重复既有全量/VM。
