# PM配置到模拟执行的闭环

实际记录：2026-10-16 22:20 Asia/Shanghai；源码 `99546d230c4a9f0dd5710e8bd9ff8775600c694a`。

修复真实缺口：原 A4→引擎→paper 链即使没有PM晨会分配也能买入。反例 `20261016T221456301764-pm-allocation-missing-red` 在旧源码真实失败（已经 dispatched）。现已接通：实际PM客户端通过严格schema后封存计划symbol/簇绑定；交易员从同一账户/同日 `pm_decisions` 读取明确会议的分配；无会议或DEGRADED没有开仓许可。

PM槽位、单位、批次进入模型执行上下文hash及paper原子admission绑定。只有已分槽且有allocation的计划能买入；主/备共享一个槽，未成交BUY挂单即占槽，部分成交和持仓继续占槽，取消原入场挂单后才可给备选。检查在 BEGIN IMMEDIATE 内读新鲜账户状态，不靠调用前缓存。PM只能把STANDARD收为PROBE、减少批次，不能扩大原A6上限或改变价格；使用率target仍为组合目标，不偷改成新增硬阈值或强制买满。

新检查不依赖客户通道，不写原 execution_plans。退出、撤单与1m硬风险不以PM配置作为放行前提。最初关联运行发现4项问题：两项PM原因掩盖硬亏损/回撤锁存，已调整顺序让硬熔断继续优先并持久化；一项仅持仓次日退出被旧晨会日期拦住，已移除该依赖；另一项测试在部分成交后仍传旧mark时间，改成实际当前标记，未削弱新鲜度门。

## 验证

原件在 `artifacts/team-v2-20261016/`。

- 缺PM真实红测：`20261016T221456301764-pm-allocation-missing-red`。
- 首轮关联4失败：`20261016T221658021445-pm-allocation-first-green`（名称含green但实际exit1，按实际结果，不当通过）。
- 修复后40项通过：`20261016T221738100937-pm-allocation-associated-green`。
- 最终受影响集：`20261016T221841145778-pm-allocation-final-affected`，179 passed、0 failed、exit0，回执SHA `83e27d8b948efca50032740b106519fbe0084808407bf8b8711c033ddd7fac3b`。
- 包含真实既有客户端HTTP mock的 PM→持久化→A4客户端→引擎→paper订单，原始两个模型各只调用一次；另有主备占槽、取消后释放、PROBE/单批次、账户日期、缺会议、原退出/硬熔断关联反例。

各测试集合重叠不相加；命令在提交前跑，按源文件SHA绑定，不冒称最终HEAD全量。CODE局部通过；REPLAY仅合成本地mock与SQLite，六日/三日真实模型门待完成；OPERATIONS未部署，生产源/Node总体仍待接线；STRATEGY收益未证。生产、客户、限额yaml和提示词正文均未改。H1仍等0223/0224，其他0227/0230/0232问题无新答复，不重复测试旧包。
