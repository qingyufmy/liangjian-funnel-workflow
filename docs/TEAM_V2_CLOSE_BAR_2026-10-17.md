# Team V2 尾盘消费切片（0263）

采集/验证时刻：2026-10-17 05:35–05:39 Asia/Shanghai。源码 `5c3a32248b62174de724127de809299e420a3b98`。仅本机隔离，未访问生产、未真实行情/模型/通知。

## 改了什么

新增 `run-team-close-bar`，Node 15:01 触发，review 槽60秒；忙槽可在15:05前恢复，业务失败不自动重跑。最后15:00 monitor与58秒父预算不动。

仅拥有的 paper 挂单/持仓：原冻结15:00完整分钟包优先，否则15:01–15:05按原A4腾讯/TDX路径正常采集一次。逐股先持久化claim；中断不重采。保留原时标/全窗口原数据/原计划行/packet SHA，经原交易性合同后才调用既有 `on_bar`。不创建意图，不调用A4/A6，不重锚。15:30只复用同一原包，按broker原幂等规则跳过。

缺包/错误时标/篡改/缺原所有者/超窗均不伪造收盘价，NAV保留DATA_LIMITED，独立内部outbox投递 `CLOSE_BAR_MISSING`；不直接发送HTTP，不碰客户。

## 红绿与回执

- 新边界初红：8失败，exit1；`20261017T053537171819-close-bar-red-receipt.json`，SHA `354da8981430cf71d4aa67fe0b253f4b43dfb070cde4b216ead61136d11633f3`。
- 最终关联Python：79通过，exit0；`20261017T053820089179-close-bar-integration-final-receipt.json`，SHA `7f4cb0ee700fb3c1f2f5dceed9793c3ace43f77117302487a9e0a02c43b62ca8`。
- Node当前全套：165通过，exit0；`20261017T053858668596-close-bar-node-final-receipt.json`，SHA `0a974764d0c4422eaa7ab6f479f691e47325061305c7d70848133932fa7a3f57`。
- server tsc：exit0；`20261017T053901664660-close-bar-server-ts-receipt.json`，SHA `3aa7248a6ef9008b0e0942dbec9cc98d14e838d7a615695de3cd6d67bdfcf98c`。

回执均在 `artifacts/team-v2-20261016/`，日志、初轮夹具目录错误与Node断言遗漏原15:00 monitor的失败保留，不充业务红测。计数不相加，不充最终候选全量。

原0259红件不覆盖。新增真实协调器+HTTP mock交易员→拥有的模拟成交→原末轮14:59→新15:00尾盘→结算COMPLETE；缺包反例保持DATA_LIMITED且内部告警。两例须进入正式六日Team回放，尚未声称已完成。

四层：CODE本机关联通过；REPLAY合成反例通过/正式六日未过；OPERATIONS未部署未VM；STRATEGY收益未证。H1仍按独立周日发布门；本切片不是总候选发布批准。A2评分口径0263已明确，接下来实施。
