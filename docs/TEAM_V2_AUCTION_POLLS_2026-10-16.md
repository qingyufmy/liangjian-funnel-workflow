# G-6 四固定时点取证接线

实际记录：2026-10-16 22:24 Asia/Shanghai。源码 `48835bc04aac4735c6b1d51449c88fdec3acc64e`。

Node按显式Asia/Shanghai时钟，09:25:03/08/15/24分别形成唯一调度key；各有2秒启动容差，错过不补调。Python CLI使用同一固定slot规则，各slot独立 O_EXCL claim 和内容hash原件；进程重启或同slot重复tick不会重复请求。旧 `once.json`、原capture和失败证据不覆盖。每个slot保留原30s父预算、review槽；在途任务不重叠，阻塞时错过的slot仍是缺证据，不排迟到队列、不声称完成四次。

只采既有腾讯批量源，不研究/激活/重锚、不改供应商限流或客户通知。只有原供应商09:25:00时标、身份/交易/价格均合法且09:25:30前实际收到的观测才能claim；非交易所认证。晚到仍记录真实received_at且不claim。晚一次成功不修改早一次失败或把09:26报价换标。

## 原件与结果

位置 `artifacts/team-v2-20261016/`：

- `20261016T222135755064-opening-fixed-slots-red`：新函数缺失的ImportError，保留，不冒充业务语义红测。
- `20261016T222219766640-opening-fixed-slots-node`：仅3项指定文件实际被选中，其余两个不存在的测试路径没有执行；不当关联全量，后续使用真实路径补齐。
- `20261016T222339723336-opening-fourpoll-final-py`：29 passed、exit0，receipt SHA `852193bf16f4f286fc182e242153a373faadec2e9b0a6627cd7af85cd6b25c6c`。
- `20261016T222339852159-opening-fourpoll-final-node`：6真实文件69 passed、exit0，receipt SHA `2d3ed8aff71ffa94fa374222ace0761db71dbc25110b4c74180408b02ba83734`。
- `20261016T222339784358-opening-fourpoll-tsc`：server tsc exit0，receipt SHA `d147138c588987540d80c3ea6fa1bb6015633d6f36ad6b3772c24f82c1751b8e`。

覆盖四slot顺序/重复、显式UTC→上海时钟、错过不补、在途不重叠、晚到拒claim、先失败后成功原件不变、CLI不构造WorkflowApplication、原DB字节不变和既有调度/58s/H1/结算关联。集合重叠不与旧运行相加；提交前按内容SHA绑定，非最终发布HEAD全量。

CODE局部通过；REPLAY仅本地假时钟/HTTP替身，没有新行情采集；OPERATIONS未部署、VM Python启动是否满足slot容差仍待最小闲时验证，不能把单元通过当自然09:25证据；STRATEGY不赋予执行或收益结论。自然验收需逐股票核实际当日批准计划池和原价覆盖，不能把固定14写成真实分母，更不能用health代替。

下一步仍为生产源/Node晨会与close接线、三日真实干跑和六日回放。0233已交PM执行闭环，H1不重复测试；0223/0224/0227/0230/0232截至本记录未有新答复。客户/生产env/计划行无改动，未push或deploy。
