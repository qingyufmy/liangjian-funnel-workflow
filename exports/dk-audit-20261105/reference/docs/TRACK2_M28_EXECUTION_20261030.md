# 轨道二 M28 v0 收口

本件依据看板 `liangjian#M28` 与 Tony 15:57/本轮续接指令。仅影子、全部权限关闭；不是发布或业务 ACCEPT。

- BEAR_RISK 的身份标注为 `a6_prior.market_regime.BEAR_RISK`，保留原答，不能据此等价到 PANIC / RISK_OFF_RETREAT。跨来源权限仍未定义。
- 剩余62个源日格标 `UNCOVERED_NO_ORIGINAL`。273个已列对象投影相等、7份文件 SHA 稳定；PIT 缺失仍 DATA_BLOCK。历史原件不再追索或重算补造。
- 周趋势采用已闭合完整周：UP 为周 close>MA10、MA5>MA10、MA10−MA10[lag2]>0；DOWN 全部反向，否则 RANGE。MA20只输出，不参与该分类。当周partial不加入均线，趋势另列last_complete_at/trend_as_of。
- 日线与DK只用腾讯：新增日线 raw day parser，空复权字段、不用 qfqday、不走个股映射；指数名称、代码、供应商时刻和闭合收盘逐项核实。未知量单位不使用，不把估算额当成交额。
- `track2_collect_tencent.py` 只写本机独立 artifacts，保留原响应字节、规范化输入、PIT与 SHA、DK/日线影子事实。新台账绑定退休DK1台账SHA并继承腾讯已用1/20、东财1/5（仅记历史，不再调用），MKT新增源30/日。
- 新增传输必须经过真实ModelClient→httpx.stream的零网络预检，发送前记账、上一流终态后满1秒；无重试/重定向。采集前及每次发送前复核T1看板短租约，任一新T1消息即撤销租约并暂停待核，不阻塞轨道一。源/业务失败保留原件并停止，不能新建run重置累计。
- TH1实现遵循M28，行业/概念分榜；六项百分位等权、HS300基准、10CM/20CM分计、龙头与二波字段。源码/受控验证和真实历史结果分列，尚无原件时不报告10-28/29验收通过。

浏览器只用内置浏览器或连接插件；本次连接host返回不可用，后续用用户明确允许的本机headless Playwright，不使用桌面Computer Use。前轮UI候选两项修复仍须三宽/F7/D8复验。

下游：新增模块仅被本机离线工具和定向测试消费；A1–A6/PM、收盘链、闭市续跑、MinuteBarStore、CAS/risk、订单、通知和调度不接线。H/R full、runtime40/10/10、三批、两日D5、D8与Claude FINAL均另列，未完成就不发布。恢复链在途不批量取数或跑full。
