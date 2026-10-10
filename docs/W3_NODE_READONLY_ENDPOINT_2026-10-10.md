# W3 Node原始任务DTO只读入口（0062）

这是决策14包内单列的生产Node源码变更，尚未部署；不改变任务、调度、策略、模型、数据库或旧路由。

新增 `/api/shadow/job-runs-readonly`，沿用已有dashboardAuth。GET以实际socket peer为准，仅接受127.0.0.1、::1、::ffff:127.0.0.1，其他来源403；不采信Host/X-Forwarded-For。非GET返回405/Allow:GET，已有Bearer未满足时仍沿用401。Cache-Control:no-store。

handler只读取 `runner.recentRuns(1000)` 并原样序列化，不过滤、排序、改写JobRunRecord；额外返回node.pid、createApp时一次保留的原浮点 `Date.now()-process.uptime()*1000`，及本次响应的observedAt。启动估计只作Node进程身份，不作任何任务完成时间或交易所时钟。历史被淘汰、Node重启或原件缺失必须由消费者标记证据不足，不能从空列表推断完成。

实际Node反例：新增路由未实现时16项全部失败/native1；实现后16项全部通过/native0，typecheck native0。覆盖真实本机HTTP读取、稳定进程出生值、DTO不变、认证、POST/PUT/PATCH/DELETE/HEAD/OPTIONS拒绝、非loopback和伪造转发头、三类loopback地址。spy证明ProjectFiles.status、dashboard.overview、runner.run和child_process.spawn调用次数均0。

原件：artifacts/weekend-closeout-20261010/node-dto-before.json 和 node-dto-after.json。具体源哈希、前后路由表、最终统一HEAD全量另封存。Python客户端独立每分钟取样、禁proxy/redirect、2秒接受预算、原body与SHA、重启判定及16:45兜底由相应W3切片验收，不把本路由单测当客户端或生产通过。
